/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.services.abfs.client;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakePathClient;
import com.azure.storage.file.datalake.models.AccessControlType;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathAccessControl;
import com.azure.storage.file.datalake.models.PathAccessControlEntry;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.RolePermissions;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.services.abfs.RangerABFSConstants;
import org.apache.ranger.services.abfs.client.ABFSIdentityResolver.ResolvedIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.LinkedHashSet;

/**
 * Translates Ranger ABFS policies into Azure Data Lake Storage Gen2 (ADLS Gen2)
 * POSIX-style ACLs and pushes them to the storage account.
 *
 * <h3>Access-type to POSIX mapping (file- vs directory-aware)</h3>
 * ADLS Gen2 assigns different meaning to r/w/x on files vs directories, so each
 * node receives bits appropriate to its type:
 * <pre>
 *   access   directory        file
 *   ------   ---------        ----
 *   read     r-x              r--   (read + navigate/list the tree; read file bytes)
 *   list     r-x              ---   (enumerate + traverse a directory; no file read)
 *   write    -wx              rw-   (create children in a dir / write+append a file)
 *   delete   rwx              ---   (delete/rename children; tree delete needs rwx)
 * </pre>
 * A directory always receives execute (x) when any access is granted, because a
 * directory must be traversable to be usable. Execute is never set on files (it
 * is meaningless there per ADLS). The read/list distinction shows only on files:
 * {@code read} allows reading file contents ({@code r--}) while {@code list}
 * grants directory listing without file read.
 *
 * <h3>Ancestor traverse</h3>
 * For a policy on {@code /a/b/c}, each principal is also granted traverse-only
 * ({@code --x}) access on every ancestor directory ({@code /}, {@code /a},
 * {@code /a/b}) so ACL-only principals can reach the target. Ancestor entries
 * are add-only: existing (possibly stronger) entries are never downgraded and
 * are never removed, including on policy delete.
 *
 * <h3>Recursion &amp; inheritance</h3>
 * <ul>
 *   <li>Access ACLs are applied to the policy target and, when the policy
 *       resource is recursive, to every existing child (per-node file/dir bits).</li>
 *   <li>Default ACLs are applied to directories only, so future children inherit them.</li>
 *   <li>Only Ranger-managed named entries are added/updated/removed; entries
 *       configured outside Ranger are preserved.</li>
 * </ul>
 */
public class ABFSAclSyncService {

    private static final Logger LOG = LoggerFactory.getLogger(ABFSAclSyncService.class);

    private static final String ACTION_DELETE = "delete";

    /**
     * Entry point invoked on every Ranger ABFS policy create/update/delete.
     *
     * @param policy    the policy being created / updated / deleted
     * @param action    one of create/update/delete (case-insensitive)
     * @param oldPolicy pre-change snapshot (null for create); used to remove
     *                  stale Ranger-managed ACL entries precisely
     * @param configs   the ABFS service configs
     */
    public void syncPolicy(RangerPolicy policy, String action, RangerPolicy oldPolicy, Map<String, String> configs) {
        if (policy == null) {
            return;
        }
        LOG.debug("==> ABFSAclSyncService.syncPolicy action={}, policyId={}", action, policy.getId());

        final boolean isDelete = StringUtils.equalsIgnoreCase(ACTION_DELETE, action);
        final ABFSIdentityResolver resolver = new ABFSIdentityResolver(configs);
        final boolean defaultAclInheritance = isDefaultAclInheritanceEnabled(configs);

        // On delete, the policy itself describes the entries Ranger must remove.
        final RangerPolicy desiredPolicy  = isDelete ? null : policy;
        final RangerPolicy previousPolicy = isDelete ? policy : oldPolicy;

        final List<DesiredGrant> desiredGrants  = buildDesiredGrants(desiredPolicy, resolver);
        final List<DesiredGrant> previousGrants = buildDesiredGrants(previousPolicy, resolver);

        Set<ABFSPathRef> pathRefs = getPolicyPathRefs(policy, configs);

        for (ABFSPathRef pathRef : pathRefs) {
            try {
                DataLakeFileSystemClient fsClient =
                        ABFSClientConnectionMgr.getFileSystemClient(configs, pathRef.getContainer());

                // 1) Grant traverse-only (--x) on ancestors so the target is reachable.
                //    Add-only; skipped entirely on delete so siblings are never broken.
                if (!isDelete && !desiredGrants.isEmpty()) {
                    applyAncestorTraverse(fsClient, pathRef, desiredGrants);
                }

                // 2) Apply access (+ default) ACLs to the target and, if recursive, its children.
                applyTarget(fsClient, pathRef, desiredGrants, previousGrants, defaultAclInheritance);
            } catch (Exception e) {
                LOG.error("ABFSAclSyncService.syncPolicy failed for container={}, path={}: {}",
                        pathRef.getContainer(), pathRef.getRelativePath(), e.getMessage(), e);
                throw new RuntimeException("ABFS ACL sync failed for path '" + pathRef.getRelativePath()
                        + "' in container '" + pathRef.getContainer() + "': " + e.getMessage(), e);
            }
        }

        LOG.debug("<== ABFSAclSyncService.syncPolicy action={}, policyId={}", action, policy.getId());
    }

    // ------------------------------------------------------------------
    // Target + children application
    // ------------------------------------------------------------------

    private void applyTarget(DataLakeFileSystemClient fsClient, ABFSPathRef pathRef,
                             List<DesiredGrant> desiredGrants, List<DesiredGrant> previousGrants,
                             boolean defaultAclInheritance) {
        if (desiredGrants.isEmpty() && previousGrants.isEmpty()) {
            return;
        }

        String path = stripLeadingSlash(pathRef.getRelativePath());

        // The policy target (relativepath) is always treated as a directory.
        applyNode(fsClient, path, true, desiredGrants, previousGrants, defaultAclInheritance);

        if (!pathRef.isRecursive()) {
            return;
        }

        // Walk each existing child and apply file- vs directory-specific POSIX bits.
        int count = 0;
        for (PathItem item : listChildren(fsClient, path)) {
            applyNode(fsClient, item.getName(), item.isDirectory(),
                    desiredGrants, previousGrants, defaultAclInheritance);
            count++;
        }
        LOG.info("ABFS per-node ACL applied to {} children under '{}'", count, path);
    }

    /**
     * Applies the Ranger-managed access (and, for directories, default) ACL
     * entries to a single node, preserving entries Ranger does not manage.
     */
    private void applyNode(DataLakeFileSystemClient fsClient, String path, boolean isDirectory,
                           List<DesiredGrant> desiredGrants, List<DesiredGrant> previousGrants,
                           boolean defaultAclInheritance) {
        List<PathAccessControlEntry> desired  =
                entriesForNode(desiredGrants, isDirectory, defaultAclInheritance);
        List<PathAccessControlEntry> previous =
                entriesForNode(previousGrants, isDirectory, defaultAclInheritance);

        if (desired.isEmpty() && previous.isEmpty()) {
            return;
        }

        try {
            DataLakePathClient client = isDirectory ? dirClientFor(fsClient, path) : fsClient.getFileClient(path);
            PathAccessControl current = client.getAccessControl();
            List<PathAccessControlEntry> merged =
                    mergeAclEntries(current.getAccessControlList(), desired, previous);
            client.setAccessControlList(merged, current.getGroup(), current.getOwner());
            LOG.debug("ABFS setAccessControlList on '{}' (dir={}): {} total entries ({} Ranger-managed)",
                    path, isDirectory, merged.size(), desired.size());
        } catch (Exception e) {
            LOG.warn("ABFS failed to apply ACL on '{}' (dir={}): {}", path, isDirectory, e.getMessage());
        }
    }

    private Iterable<PathItem> listChildren(DataLakeFileSystemClient fsClient, String path) {
        ListPathsOptions options = new ListPathsOptions()
                .setRecursive(true)
                .setMaxResults(RangerABFSConstants.ABFS_LIST_MAX_RESULTS);
        if (StringUtils.isNotEmpty(path)) {
            options.setPath(path);
        }
        return fsClient.listPaths(options, null);
    }

    // ------------------------------------------------------------------
    // Ancestor traverse
    // ------------------------------------------------------------------

    private void applyAncestorTraverse(DataLakeFileSystemClient fsClient, ABFSPathRef pathRef,
                                       List<DesiredGrant> desiredGrants) {
        String path = stripLeadingSlash(pathRef.getRelativePath());
        if (StringUtils.isEmpty(path)) {
            // Target is the container root; it has no ancestors.
            return;
        }
        List<PathAccessControlEntry> traverse = traverseEntries(desiredGrants);
        if (traverse.isEmpty()) {
            return;
        }
        for (String ancestor : ancestorPaths(path)) {
            try {
                DataLakeDirectoryClient client = dirClientFor(fsClient, ancestor);
                PathAccessControl current = client.getAccessControl();
                List<PathAccessControlEntry> merged =
                        mergeTraverseEntries(current.getAccessControlList(), traverse);
                if (merged == null) {
                    continue; // nothing new to add; leave the ancestor untouched
                }
                client.setAccessControlList(merged, current.getGroup(), current.getOwner());
                LOG.debug("ABFS ancestor traverse (--x) applied on '{}'", ancestor.isEmpty() ? "/" : ancestor);
            } catch (Exception e) {
                LOG.warn("ABFS failed to grant ancestor traverse on '{}': {}",
                        ancestor.isEmpty() ? "/" : ancestor, e.getMessage());
            }
        }
    }

    /**
     * Add-only merge for ancestor traverse: keeps every existing entry as-is and
     * only adds a {@code --x} entry for a principal that has no access entry yet.
     * Returns {@code null} when there is nothing to add (so the caller can skip
     * the write entirely).
     */
    private List<PathAccessControlEntry> mergeTraverseEntries(List<PathAccessControlEntry> existing,
                                                              List<PathAccessControlEntry> traverse) {
        Set<String> existingKeys = new LinkedHashSet<>();
        List<PathAccessControlEntry> merged = new ArrayList<>();
        if (existing != null) {
            for (PathAccessControlEntry e : existing) {
                existingKeys.add(toAclKey(e));
                merged.add(e);
            }
        }
        boolean added = false;
        for (PathAccessControlEntry e : traverse) {
            if (!existingKeys.contains(toAclKey(e))) {
                merged.add(e);
                added = true;
            }
        }
        return added ? merged : null;
    }

    private List<PathAccessControlEntry> traverseEntries(List<DesiredGrant> grants) {
        Map<String, PathAccessControlEntry> byKey = new LinkedHashMap<>();
        RolePermissions traverseOnly = perms(false, false, true);
        for (DesiredGrant grant : grants) {
            PathAccessControlEntry entry = aclEntry(grant.identity, traverseOnly, false);
            byKey.put(toAclKey(entry), entry);
        }
        return new ArrayList<>(byKey.values());
    }

    private static List<String> ancestorPaths(String path) {
        List<String> ancestors = new ArrayList<>();
        ancestors.add(""); // container root
        String[] parts = path.split("/");
        StringBuilder cumulative = new StringBuilder();
        // Every part except the last (the target itself) is an ancestor directory.
        for (int i = 0; i < parts.length - 1; i++) {
            if (StringUtils.isBlank(parts[i])) {
                continue;
            }
            if (cumulative.length() > 0) {
                cumulative.append('/');
            }
            cumulative.append(parts[i]);
            ancestors.add(cumulative.toString());
        }
        return ancestors;
    }

    // ------------------------------------------------------------------
    // Desired grants (identity + access flags)
    // ------------------------------------------------------------------

    private List<DesiredGrant> buildDesiredGrants(RangerPolicy policy, ABFSIdentityResolver resolver) {
        Map<String, DesiredGrant> byKey = new LinkedHashMap<>();
        if (policy == null || policy.getPolicyItems() == null) {
            return new ArrayList<>();
        }
        for (RangerPolicyItem item : policy.getPolicyItems()) {
            if (item == null || item.getAccesses() == null || item.getAccesses().isEmpty()) {
                continue;
            }
            AccessFlags flags = toAccessFlags(item.getAccesses());
            if (!flags.any()) {
                continue;
            }
            if (item.getUsers() != null) {
                for (String user : item.getUsers()) {
                    mergeGrant(byKey, resolver.resolveUser(user), flags);
                }
            }
            if (item.getGroups() != null) {
                for (String group : item.getGroups()) {
                    mergeGrant(byKey, resolver.resolveGroup(group), flags);
                }
            }
        }
        return new ArrayList<>(byKey.values());
    }

    private void mergeGrant(Map<String, DesiredGrant> byKey, ResolvedIdentity identity, AccessFlags flags) {
        if (identity == null) {
            return;
        }
        String key = identity.getType() + ":" + identity.getObjectId();
        DesiredGrant existing = byKey.get(key);
        if (existing == null) {
            byKey.put(key, new DesiredGrant(identity, flags.copy()));
        } else {
            existing.flags.mergeFrom(flags);
        }
    }

    private static AccessFlags toAccessFlags(List<RangerPolicyItemAccess> accesses) {
        AccessFlags flags = new AccessFlags();
        for (RangerPolicyItemAccess access : accesses) {
            if (access == null || Boolean.FALSE.equals(access.getIsAllowed())) {
                continue;
            }
            String type = access.getType();
            if (type == null) {
                continue;
            }
            switch (type) {
                case RangerABFSConstants.ACCESS_TYPE_READ:
                    flags.read = true;
                    break;
                case RangerABFSConstants.ACCESS_TYPE_LIST:
                    flags.list = true;
                    break;
                case RangerABFSConstants.ACCESS_TYPE_WRITE:
                    flags.write = true;
                    break;
                case RangerABFSConstants.ACCESS_TYPE_DELETE:
                    flags.delete = true;
                    break;
                default:
                    break;
            }
        }
        return flags;
    }

    // ------------------------------------------------------------------
    // POSIX bit mapping (file- vs directory-aware)
    // ------------------------------------------------------------------

    /**
     * Directory permissions. A directory needs execute whenever any access is
     * granted (it must be traversable to be usable). Read is granted so the
     * directory is listable when the principal can read its contents (read),
     * enumerate it (list), or recursively delete it (delete). Write is granted
     * for write or delete.
     *
     * <p>Example: {@code read} on {@code /finance} recursively yields {@code r-x}
     * on {@code /finance} and every sub-directory, and {@code r--} on files, so a
     * user can navigate and read the whole tree.</p>
     */
    private static RolePermissions directoryPermissions(AccessFlags f) {
        if (!f.any()) {
            return null;
        }
        boolean r = f.read || f.list || f.delete;
        boolean w = f.write || f.delete;
        boolean x = true;
        return perms(r, w, x);
    }

    /**
     * File permissions. Execute is meaningless on files, so it is never set.
     * A file is readable for read; write grants read+write ({@code rw-}) since
     * appending/updating a file requires read as well. list/delete grant nothing
     * on the file itself (deletion is controlled by the parent directory).
     */
    private static RolePermissions filePermissions(AccessFlags f) {
        boolean w = f.write;
        boolean r = f.read || f.write;
        if (!r && !w) {
            return null;
        }
        return perms(r, w, false);
    }

    private static RolePermissions perms(boolean r, boolean w, boolean x) {
        RolePermissions p = new RolePermissions();
        p.setReadPermission(r);
        p.setWritePermission(w);
        p.setExecutePermission(x);
        return p;
    }

    /**
     * Builds the Ranger-managed named ACL entries for a node of the given type.
     * Default-scope entries are only produced for directories.
     */
    private List<PathAccessControlEntry> entriesForNode(List<DesiredGrant> grants, boolean isDirectory,
                                                        boolean defaultAclInheritance) {
        Map<String, PathAccessControlEntry> byKey = new LinkedHashMap<>();
        for (DesiredGrant grant : grants) {
            RolePermissions accessPerms = isDirectory
                    ? directoryPermissions(grant.flags)
                    : filePermissions(grant.flags);
            if (accessPerms != null) {
                PathAccessControlEntry entry = aclEntry(grant.identity, accessPerms, false);
                byKey.put(toAclKey(entry), entry);
            }
            if (isDirectory && defaultAclInheritance) {
                RolePermissions defaultPerms = directoryPermissions(grant.flags);
                if (defaultPerms != null) {
                    PathAccessControlEntry entry = aclEntry(grant.identity, defaultPerms, true);
                    byKey.put(toAclKey(entry), entry);
                }
            }
        }
        return new ArrayList<>(byKey.values());
    }

    private PathAccessControlEntry aclEntry(ResolvedIdentity identity, RolePermissions perms, boolean isDefaultScope) {
        PathAccessControlEntry entry = new PathAccessControlEntry();
        entry.setDefaultScope(isDefaultScope);
        entry.setAccessControlType(identity.getType() == ABFSIdentityResolver.ResolvedIdentityType.USER
                ? AccessControlType.USER : AccessControlType.GROUP);
        entry.setEntityId(identity.getObjectId());
        entry.setPermissions(perms);
        return entry;
    }

    // ------------------------------------------------------------------
    // ACL merge helpers
    // ------------------------------------------------------------------

    private List<PathAccessControlEntry> mergeAclEntries(List<PathAccessControlEntry> existing,
                                                         List<PathAccessControlEntry> desired,
                                                         List<PathAccessControlEntry> previous) {
        Map<String, PathAccessControlEntry> byKey = new LinkedHashMap<>();
        if (existing != null) {
            for (PathAccessControlEntry e : existing) {
                byKey.put(toAclKey(e), e);
            }
        }
        // Drop entries Ranger previously managed (preserve everything else).
        if (previous != null) {
            for (PathAccessControlEntry e : previous) {
                byKey.remove(toAclKey(e));
            }
        }
        // Layer in the currently desired Ranger entries.
        if (desired != null) {
            for (PathAccessControlEntry e : desired) {
                byKey.put(toAclKey(e), e);
            }
        }
        return new ArrayList<>(byKey.values());
    }

    // ------------------------------------------------------------------
    // Path / resource helpers
    // ------------------------------------------------------------------

    private static DataLakeDirectoryClient dirClientFor(DataLakeFileSystemClient fsClient, String path) {
        // An empty path denotes the container root; the SDK addresses it via "/".
        return fsClient.getDirectoryClient(StringUtils.isEmpty(path) ? "/" : path);
    }

    private Set<ABFSPathRef> getPolicyPathRefs(RangerPolicy policy, Map<String, String> configs) {
        Set<ABFSPathRef> refs = new LinkedHashSet<>();
        String defaultContainer = configs.get(RangerABFSConstants.DEFAULT_CONTAINER);

        List<String> containers = getResourceValues(policy, RangerABFSConstants.CONTAINER);
        List<String> relPaths   = getResourceValues(policy, RangerABFSConstants.RELATIVE_PATH);
        boolean recursive       = isResourceRecursive(policy, RangerABFSConstants.RELATIVE_PATH)
                && isRecursiveAclSyncEnabled(configs);

        if (relPaths.isEmpty()) {
            relPaths = new ArrayList<>();
            relPaths.add("/");
        }

        for (String container : containers) {
            String resolvedContainer = StringUtils.startsWith(container, "*") ? defaultContainer : container;
            if (StringUtils.isBlank(resolvedContainer)) {
                continue;
            }
            for (String relPath : relPaths) {
                refs.add(new ABFSPathRef(resolvedContainer, relPath, recursive));
            }
        }
        return refs;
    }

    private static List<String> getResourceValues(RangerPolicy policy, String resourceName) {
        List<String> values = new ArrayList<>();
        if (policy != null && policy.getResources() != null) {
            RangerPolicyResource resource = policy.getResources().get(resourceName);
            if (resource != null && resource.getValues() != null) {
                for (String value : resource.getValues()) {
                    if (StringUtils.isNotBlank(value)) {
                        values.add(value);
                    }
                }
            }
        }
        return values;
    }

    private static boolean isResourceRecursive(RangerPolicy policy, String resourceName) {
        if (policy != null && policy.getResources() != null) {
            RangerPolicyResource resource = policy.getResources().get(resourceName);
            if (resource != null && resource.getIsRecursive() != null) {
                return resource.getIsRecursive();
            }
        }
        return false;
    }

    private boolean isRecursiveAclSyncEnabled(Map<String, String> configs) {
        String value = configs.get(RangerABFSConstants.RECURSIVE_ACL_SYNC_ENABLED);
        return StringUtils.isBlank(value) || Boolean.parseBoolean(value);
    }

    private boolean isDefaultAclInheritanceEnabled(Map<String, String> configs) {
        String value = configs.get(RangerABFSConstants.DEFAULT_ACL_INHERITANCE_ENABLED);
        return StringUtils.isBlank(value) || Boolean.parseBoolean(value);
    }

    private static String toAclKey(PathAccessControlEntry entry) {
        return (entry.isInDefaultScope() ? "default" : "access") + ":"
                + entry.getAccessControlType() + ":"
                + entry.getEntityId();
    }

    /**
     * Converts a Ranger relative path into an ADLS directory path. The container
     * root ('/' or '*') maps to an empty string, which the SDK treats as root.
     */
    private static String stripLeadingSlash(String relativePath) {
        if (StringUtils.isBlank(relativePath) || "/".equals(relativePath) || "*".equals(relativePath)) {
            return "";
        }
        String normalized = relativePath;
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        // Trailing wildcard means "this directory and below"; the directory itself is the anchor.
        if (normalized.endsWith("/*")) {
            normalized = normalized.substring(0, normalized.length() - 2);
        }
        return normalized;
    }

    // ------------------------------------------------------------------
    // Small value types
    // ------------------------------------------------------------------

    /** Mutable set of granted Ranger access types for a single principal. */
    private static final class AccessFlags {
        private boolean read;
        private boolean list;
        private boolean write;
        private boolean delete;

        private boolean any() {
            return read || list || write || delete;
        }

        private AccessFlags copy() {
            AccessFlags c = new AccessFlags();
            c.read = read;
            c.list = list;
            c.write = write;
            c.delete = delete;
            return c;
        }

        private void mergeFrom(AccessFlags other) {
            read   |= other.read;
            list   |= other.list;
            write  |= other.write;
            delete |= other.delete;
        }
    }

    /** A resolved principal together with the access it should be granted. */
    private static final class DesiredGrant {
        private final ResolvedIdentity identity;
        private final AccessFlags flags;

        private DesiredGrant(ResolvedIdentity identity, AccessFlags flags) {
            this.identity = identity;
            this.flags    = flags;
        }
    }

    /** Immutable (container, relativePath, recursive) tuple identifying an ACL target. */
    static final class ABFSPathRef {
        private final String  container;
        private final String  relativePath;
        private final boolean recursive;

        ABFSPathRef(String container, String relativePath, boolean recursive) {
            this.container    = container;
            this.relativePath = relativePath;
            this.recursive    = recursive;
        }

        public String getContainer() {
            return container;
        }

        public String getRelativePath() {
            return relativePath;
        }

        public boolean isRecursive() {
            return recursive;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ABFSPathRef that = (ABFSPathRef) o;
            return recursive == that.recursive
                    && Objects.equals(container, that.container)
                    && Objects.equals(relativePath, that.relativePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(container, relativePath, recursive);
        }
    }
}
