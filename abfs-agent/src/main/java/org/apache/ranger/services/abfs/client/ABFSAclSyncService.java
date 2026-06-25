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
import com.azure.storage.file.datalake.models.PathAccessControl;
import com.azure.storage.file.datalake.models.PathAccessControlEntry;
import com.azure.storage.file.datalake.models.PathRemoveAccessControlEntry;
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
 * <h3>Access-type to POSIX mapping</h3>
 * <pre>
 *   read   -&gt; r-- (execute added on directories for traversal)
 *   list   -&gt; r-x
 *   write  -&gt; -w- (execute added on directories for traversal)
 *   delete -&gt; -wx (parent directory needs w + x)
 * </pre>
 *
 * <h3>Recursion &amp; inheritance</h3>
 * <ul>
 *   <li>Access ACLs are applied to the policy target and, when the policy
 *       resource is recursive, to all existing children.</li>
 *   <li>Default ACLs are applied to directories so future children inherit them.</li>
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

        Set<ABFSPathRef> pathRefs = getPolicyPathRefs(policy, configs);

        for (ABFSPathRef pathRef : pathRefs) {
            try {
                DataLakeFileSystemClient fsClient =
                        ABFSClientConnectionMgr.getFileSystemClient(configs, pathRef.getContainer());

                // 1) access ACLs (apply to target + existing children when recursive)
                applyAclRecursively(fsClient, pathRef, desiredPolicy, previousPolicy, resolver, false);

                // 2) default ACLs on directories so future children inherit
                if (defaultAclInheritance) {
                    applyAclRecursively(fsClient, pathRef, desiredPolicy, previousPolicy, resolver, true);
                }
            } catch (Exception e) {
                LOG.error("ABFSAclSyncService.syncPolicy failed for container={}, path={}: {}",
                        pathRef.getContainer(), pathRef.getRelativePath(), e.getMessage(), e);
                throw new RuntimeException("ABFS ACL sync failed for path '" + pathRef.getRelativePath()
                        + "' in container '" + pathRef.getContainer() + "': " + e.getMessage(), e);
            }
        }

        LOG.debug("<== ABFSAclSyncService.syncPolicy action={}, policyId={}", action, policy.getId());
    }

    private void applyAclRecursively(DataLakeFileSystemClient fsClient, ABFSPathRef pathRef,
                                     RangerPolicy desiredPolicy, RangerPolicy previousPolicy,
                                     ABFSIdentityResolver resolver, boolean isDefaultScope) {
        List<PathAccessControlEntry> desired  =
                buildDesiredAclEntries(desiredPolicy, resolver, isDefaultScope);
        List<PathAccessControlEntry> previous =
                buildDesiredAclEntries(previousPolicy, resolver, isDefaultScope);

        if (desired.isEmpty() && previous.isEmpty()) {
            return;
        }

        String path = stripLeadingSlash(pathRef.getRelativePath());
        DataLakeDirectoryClient dirClient = fsClient.getDirectoryClient(path);

        boolean recursive = pathRef.isRecursive();

        if (recursive) {
            // Recursive merge: remove stale Ranger entries, then add/update desired ones.
            // Manual entries on each item are preserved because we never replace the full ACL.
            List<PathRemoveAccessControlEntry> removeEntries = toRemoveEntries(previous, desired);
            if (!removeEntries.isEmpty()) {
                LOG.info("ABFS removeAccessControlRecursive on '{}' (defaultScope={}): {} entries",
                        path, isDefaultScope, removeEntries.size());
                dirClient.removeAccessControlRecursive(removeEntries);
            }
            if (!desired.isEmpty()) {
                LOG.info("ABFS updateAccessControlRecursive on '{}' (defaultScope={}): {} entries",
                        path, isDefaultScope, desired.size());
                dirClient.updateAccessControlRecursive(desired);
            }
        } else {
            applyAcl(dirClient, desired, previous);
        }
    }

    /**
     * Single-path apply: reads the current ACL, removes entries Ranger previously
     * managed, layers in the desired entries, and writes the merged ACL back.
     * Manually configured entries are preserved.
     */
    private void applyAcl(DataLakePathClient pathClient, List<PathAccessControlEntry> desired,
                          List<PathAccessControlEntry> previous) {
        PathAccessControl current = pathClient.getAccessControl();
        List<PathAccessControlEntry> existing = current.getAccessControlList();

        List<PathAccessControlEntry> merged = mergeAclEntries(existing, desired, previous);

        pathClient.setAccessControlList(merged, current.getGroup(), current.getOwner());
        LOG.info("ABFS setAccessControlList applied: {} total entries ({} Ranger-managed)",
                merged.size(), desired.size());
    }

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

    /**
     * Builds the named USER/GROUP ACL entries implied by a policy's allow items.
     * Base owner/group/other entries are never produced here.
     */
    private List<PathAccessControlEntry> buildDesiredAclEntries(RangerPolicy policy,
                                                                ABFSIdentityResolver resolver, boolean isDefaultScope) {
        Map<String, PathAccessControlEntry> byKey = new LinkedHashMap<>();
        if (policy == null || policy.getPolicyItems() == null) {
            return new ArrayList<>();
        }

        for (RangerPolicyItem item : policy.getPolicyItems()) {
            if (item == null || item.getAccesses() == null || item.getAccesses().isEmpty()) {
                continue;
            }
            RolePermissions perms = toRolePermissions(item.getAccesses());
            if (perms == null) {
                continue;
            }

            if (item.getUsers() != null) {
                for (String user : item.getUsers()) {
                    ResolvedIdentity id = resolver.resolveUser(user);
                    addAclEntry(byKey, id, perms, isDefaultScope);
                }
            }
            if (item.getGroups() != null) {
                for (String group : item.getGroups()) {
                    ResolvedIdentity id = resolver.resolveGroup(group);
                    addAclEntry(byKey, id, perms, isDefaultScope);
                }
            }
        }
        return new ArrayList<>(byKey.values());
    }

    private void addAclEntry(Map<String, PathAccessControlEntry> byKey, ResolvedIdentity identity,
                             RolePermissions perms, boolean isDefaultScope) {
        if (identity == null) {
            return;
        }
        PathAccessControlEntry entry = new PathAccessControlEntry();
        entry.setDefaultScope(isDefaultScope);
        entry.setAccessControlType(identity.getType() == ABFSIdentityResolver.ResolvedIdentityType.USER
                ? AccessControlType.USER : AccessControlType.GROUP);
        entry.setEntityId(identity.getObjectId());
        entry.setPermissions(perms);
        byKey.put(toAclKey(entry), entry);
    }

    /**
     * Maps a set of Ranger access types to POSIX r/w/x permissions. Execute is
     * always added when read or write is granted, since ADLS requires execute on
     * directories to traverse into them.
     */
    private RolePermissions toRolePermissions(List<RangerPolicyItemAccess> accesses) {
        boolean read = false;
        boolean write = false;
        boolean execute = false;

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
                    read = true;
                    break;
                case RangerABFSConstants.ACCESS_TYPE_LIST:
                    read = true;
                    execute = true;
                    break;
                case RangerABFSConstants.ACCESS_TYPE_WRITE:
                    write = true;
                    break;
                case RangerABFSConstants.ACCESS_TYPE_DELETE:
                    write = true;
                    execute = true;
                    break;
                default:
                    break;
            }
        }

        if (!read && !write && !execute) {
            return null;
        }
        // Traversal requirement: directories need execute whenever read/write is granted.
        if (read || write) {
            execute = true;
        }

        RolePermissions perms = new RolePermissions();
        perms.setReadPermission(read);
        perms.setWritePermission(write);
        perms.setExecutePermission(execute);
        return perms;
    }

    private List<PathRemoveAccessControlEntry> toRemoveEntries(List<PathAccessControlEntry> previous,
                                                              List<PathAccessControlEntry> desired) {
        Set<String> desiredKeys = new LinkedHashSet<>();
        if (desired != null) {
            for (PathAccessControlEntry e : desired) {
                desiredKeys.add(toAclKey(e));
            }
        }
        List<PathRemoveAccessControlEntry> removeEntries = new ArrayList<>();
        if (previous == null) {
            return removeEntries;
        }
        Set<String> seen = new LinkedHashSet<>();
        for (PathAccessControlEntry e : previous) {
            String key = toAclKey(e);
            // Skip entries that the new desired set will overwrite anyway, and de-dup.
            if (desiredKeys.contains(key) || !seen.add(key)) {
                continue;
            }
            PathRemoveAccessControlEntry remove = new PathRemoveAccessControlEntry();
            remove.setDefaultScope(e.isInDefaultScope());
            remove.setAccessControlType(e.getAccessControlType());
            remove.setEntityId(e.getEntityId());
            removeEntries.add(remove);
        }
        return removeEntries;
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
