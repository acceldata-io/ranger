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

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakePathClient;
import com.azure.storage.file.datalake.models.AccessControlType;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathAccessControl;
import com.azure.storage.file.datalake.models.PathAccessControlEntry;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.RolePermissions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.services.abfs.RangerABFSConstants;
import org.apache.ranger.services.abfs.client.ABFSIdentityResolver.ResolvedIdentity;
import org.apache.ranger.services.abfs.client.ABFSIdentityResolver.ResolvedIdentityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ABFSAclSyncService {
    private static final Logger LOG = LoggerFactory.getLogger(ABFSAclSyncService.class);
    private static final String ACTION_DELETE = "delete";

    public void syncPolicy(RangerPolicy policy, String action, RangerPolicy oldPolicy, Map<String, String> configs) {
        LOG.info("==> ABFSAclSyncService.syncPolicy() policyId={}, action={}", policy != null ? policy.getId() : null, action);

        if (policy == null || policy.getResources() == null) {
            return;
        }

        ABFSIdentityResolver identityResolver = new ABFSIdentityResolver(configs);

        Set<ABFSPathRef> affectedPaths = new HashSet<>();
        affectedPaths.addAll(getPolicyPathRefs(policy, configs));
        if (oldPolicy != null) {
            affectedPaths.addAll(getPolicyPathRefs(oldPolicy, configs));
        }

        boolean deleteAction = ACTION_DELETE.equalsIgnoreCase(action);

        for (ABFSPathRef pathRef : affectedPaths) {
            DataLakeFileSystemClient fileSystemClient = ABFSClientConnectionMgr.getFileSystemClient(configs,
                    pathRef.getContainer());
            List<PathAccessControlEntry> staleAclEntries = oldPolicy != null
                    ? buildDesiredAclEntries(oldPolicy, pathRef, identityResolver, false)
                    : new ArrayList<>();
            List<PathAccessControlEntry> desiredAclEntries = deleteAction
                    ? new ArrayList<>()
                    : buildDesiredAclEntries(policy, pathRef, identityResolver, false);
            applyAcl(fileSystemClient.getDirectoryClient(stripLeadingSlash(pathRef.getRelativePath())),
                    desiredAclEntries, staleAclEntries);

            if (pathRef.isRecursive() && isRecursiveAclSyncEnabled(configs)) {
                applyAclRecursively(fileSystemClient, pathRef, policy, oldPolicy, identityResolver, deleteAction);
            }
        }

        LOG.info("<== ABFSAclSyncService.syncPolicy() policyId={}, affectedPaths={}",
                policy.getId(), affectedPaths.size());
    }

    private void applyAclRecursively(DataLakeFileSystemClient fileSystemClient, ABFSPathRef rootPath,
                                     RangerPolicy policy, RangerPolicy oldPolicy,
                                     ABFSIdentityResolver identityResolver, boolean deleteAction) {
        String normalizedRootPath = stripLeadingSlash(rootPath.getRelativePath());
        ListPathsOptions options = new ListPathsOptions()
                .setPath(normalizedRootPath)
                .setRecursive(true)
                .setMaxResults(RangerABFSConstants.ABFS_LIST_MAX_RESULTS);

        for (PathItem pathItem : fileSystemClient.listPaths(options, null)) {
            String pathName = pathItem.getName();
            if (StringUtils.isBlank(pathName)) {
                continue;
            }

            DataLakePathClient pathClient = Boolean.TRUE.equals(pathItem.isDirectory())
                    ? fileSystemClient.getDirectoryClient(pathName)
                    : fileSystemClient.getFileClient(pathName);
            List<PathAccessControlEntry> staleAclEntries = oldPolicy != null
                    ? buildDesiredAclEntries(oldPolicy, rootPath, identityResolver, Boolean.TRUE.equals(pathItem.isDirectory()))
                    : new ArrayList<>();
            List<PathAccessControlEntry> desiredAclEntries =
                    deleteAction ? new ArrayList<>()
                            : buildDesiredAclEntries(policy, rootPath, identityResolver, Boolean.TRUE.equals(pathItem.isDirectory()));
            applyAcl(pathClient, desiredAclEntries, staleAclEntries);
        }
    }

    private void applyAcl(DataLakePathClient pathClient, List<PathAccessControlEntry> desiredAclEntries,
                          List<PathAccessControlEntry> staleAclEntries) {
        if (CollectionUtils.isEmpty(desiredAclEntries) && CollectionUtils.isEmpty(staleAclEntries)) {
            return;
        }

        PathAccessControl currentAccessControl = pathClient.getAccessControl();
        List<PathAccessControlEntry> mergedEntries = mergeAclEntries(
                currentAccessControl.getAccessControlList(), desiredAclEntries, staleAclEntries);

        pathClient.setAccessControlList(mergedEntries, currentAccessControl.getGroup(), currentAccessControl.getOwner());
    }

    private List<PathAccessControlEntry> mergeAclEntries(List<PathAccessControlEntry> currentEntries,
                                                        List<PathAccessControlEntry> desiredEntries,
                                                        List<PathAccessControlEntry> staleEntries) {
        Map<String, PathAccessControlEntry> byKey = new HashMap<>();

        if (CollectionUtils.isNotEmpty(currentEntries)) {
            for (PathAccessControlEntry entry : currentEntries) {
                byKey.put(toAclKey(entry), entry);
            }
        }

        if (CollectionUtils.isNotEmpty(staleEntries)) {
            for (PathAccessControlEntry entry : staleEntries) {
                byKey.remove(toAclKey(entry));
            }
        }

        for (PathAccessControlEntry entry : desiredEntries) {
            byKey.put(toAclKey(entry), entry);
        }

        return new ArrayList<>(byKey.values());
    }

    private List<PathAccessControlEntry> buildDesiredAclEntries(RangerPolicy policy, ABFSPathRef pathRef,
                                                               ABFSIdentityResolver identityResolver,
                                                               boolean includeDefaultAcl) {
        List<PathAccessControlEntry> ret = new ArrayList<>();

        if (policy == null || CollectionUtils.isEmpty(policy.getPolicyItems())) {
            return ret;
        }

        boolean addDefaultAcl = includeDefaultAcl || pathRef.isRecursive();
        for (RangerPolicyItem item : policy.getPolicyItems()) {
            RolePermissions permissions = toRolePermissions(item.getAccesses());
            if (permissions == null) {
                continue;
            }

            if (CollectionUtils.isNotEmpty(item.getUsers())) {
                for (String user : item.getUsers()) {
                    ResolvedIdentity identity = identityResolver.resolveUser(user);
                    addAclEntry(ret, identity, permissions, false);
                    if (addDefaultAcl) {
                        addAclEntry(ret, identity, permissions, true);
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(item.getGroups())) {
                for (String group : item.getGroups()) {
                    ResolvedIdentity identity = identityResolver.resolveGroup(group);
                    addAclEntry(ret, identity, permissions, false);
                    if (addDefaultAcl) {
                        addAclEntry(ret, identity, permissions, true);
                    }
                }
            }
        }

        return ret;
    }

    private void addAclEntry(List<PathAccessControlEntry> entries, ResolvedIdentity identity,
                             RolePermissions permissions, boolean defaultScope) {
        if (identity == null) {
            return;
        }

        AccessControlType accessControlType = identity.getType() == ResolvedIdentityType.GROUP
                ? AccessControlType.GROUP
                : AccessControlType.USER;

        entries.add(new PathAccessControlEntry()
                .setAccessControlType(accessControlType)
                .setDefaultScope(defaultScope)
                .setEntityId(identity.getObjectId())
                .setPermissions(permissions));
    }

    private RolePermissions toRolePermissions(List<RangerPolicyItemAccess> accesses) {
        if (CollectionUtils.isEmpty(accesses)) {
            return null;
        }

        RolePermissions permissions = new RolePermissions();
        boolean hasPermission = false;

        for (RangerPolicyItemAccess access : accesses) {
            if (access == null || Boolean.FALSE.equals(access.getIsAllowed())) {
                continue;
            }

            String type = access.getType();
            if (RangerABFSConstants.ACCESS_TYPE_READ.equals(type)) {
                permissions.setReadPermission(true);
                hasPermission = true;
            } else if (RangerABFSConstants.ACCESS_TYPE_LIST.equals(type)) {
                permissions.setReadPermission(true).setExecutePermission(true);
                hasPermission = true;
            } else if (RangerABFSConstants.ACCESS_TYPE_WRITE.equals(type)) {
                permissions.setWritePermission(true).setExecutePermission(true);
                hasPermission = true;
            } else if (RangerABFSConstants.ACCESS_TYPE_DELETE.equals(type)
                    || RangerABFSConstants.ACCESS_TYPE_RENAME.equals(type)
                    || RangerABFSConstants.ACCESS_TYPE_SET_ACL.equals(type)) {
                permissions.setWritePermission(true).setExecutePermission(true);
                hasPermission = true;
            } else if (RangerABFSConstants.ACCESS_TYPE_EXECUTE.equals(type)) {
                permissions.setExecutePermission(true);
                hasPermission = true;
            }
        }

        return hasPermission ? permissions : null;
    }

    private Set<ABFSPathRef> getPolicyPathRefs(RangerPolicy policy, Map<String, String> configs) {
        Set<ABFSPathRef> ret = new HashSet<>();
        if (policy == null || policy.getResources() == null) {
            return ret;
        }

        List<String> containers = getResourceValues(policy, RangerABFSConstants.CONTAINER);
        List<String> paths = getResourceValues(policy, RangerABFSConstants.RELATIVE_PATH);
        boolean recursive = isResourceRecursive(policy, RangerABFSConstants.RELATIVE_PATH);

        if (containers.isEmpty()) {
            containers.add(configs.get(RangerABFSConstants.DEFAULT_CONTAINER));
        }
        if (paths.isEmpty()) {
            paths.add("/");
        }

        for (String container : containers) {
            if (StringUtils.isBlank(container) || "*".equals(container)) {
                container = configs.get(RangerABFSConstants.DEFAULT_CONTAINER);
            }
            if (StringUtils.isBlank(container)) {
                continue;
            }

            for (String path : paths) {
                ret.add(new ABFSPathRef(container, StringUtils.defaultIfBlank(path, "/"), recursive));
            }
        }

        return ret;
    }

    private static List<String> getResourceValues(RangerPolicy policy, String resourceName) {
        List<String> ret = new ArrayList<>();
        RangerPolicyResource resource = policy.getResources().get(resourceName);
        if (resource != null && CollectionUtils.isNotEmpty(resource.getValues())) {
            ret.addAll(resource.getValues());
        }
        return ret;
    }

    private static boolean isResourceRecursive(RangerPolicy policy, String resourceName) {
        RangerPolicyResource resource = policy.getResources().get(resourceName);
        return resource != null && Boolean.TRUE.equals(resource.getIsRecursive());
    }

    private boolean isRecursiveAclSyncEnabled(Map<String, String> configs) {
        return Boolean.parseBoolean(StringUtils.defaultIfBlank(
                configs.get(RangerABFSConstants.RECURSIVE_ACL_SYNC_ENABLED), "true"));
    }

    private static String toAclKey(PathAccessControlEntry entry) {
        return entry.isInDefaultScope() + ":" + entry.getAccessControlType() + ":"
                + StringUtils.defaultString(entry.getEntityId());
    }

    private static String stripLeadingSlash(String value) {
        String ret = StringUtils.defaultIfBlank(value, "/");
        while (ret.startsWith("/") && ret.length() > 1) {
            ret = ret.substring(1);
        }
        return "/".equals(ret) ? "" : ret;
    }

    private static class ABFSPathRef {
        private final String container;
        private final String relativePath;
        private final boolean recursive;

        ABFSPathRef(String container, String relativePath, boolean recursive) {
            this.container = container;
            this.relativePath = relativePath;
            this.recursive = recursive;
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
            if (!(o instanceof ABFSPathRef)) {
                return false;
            }
            ABFSPathRef that = (ABFSPathRef) o;
            return recursive == that.recursive
                    && StringUtils.equals(container, that.container)
                    && StringUtils.equals(relativePath, that.relativePath);
        }

        @Override
        public int hashCode() {
            int result = container != null ? container.hashCode() : 0;
            result = 31 * result + (relativePath != null ? relativePath.hashCode() : 0);
            result = 31 * result + (recursive ? 1 : 0);
            return result;
        }
    }
}
