/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.acceldata.mlflow.ranger;

import io.acceldata.mlflow.ranger.MlflowRoleSpec.ResourceGrantKey;
import io.acceldata.mlflow.ranger.MlflowRoleSpec.WorkspaceRoleKey;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerRole.RoleMember;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Translates Apache Ranger {@link ServicePolicies}, {@link RangerUserStore}, and
 * {@link RangerRoles} into MLflow 3.16.0's native RBAC model ({@link MlflowRoleSpec}).
 *
 * <h2>How Ranger concepts map onto MLflow 3.16.0 native RBAC</h2>
 * <ul>
 *   <li><b>Ranger Role</b> {@code R} in workspace {@code W} &rarr; MLflow Role
 *       {@code <prefix>role_<R>} in workspace {@code W}. Assigned users are the
 *       union of direct user members and users belonging to any group member of
 *       {@code R} (including transitive nested Ranger roles).</li>
 *   <li><b>Ranger Group</b> {@code G} in workspace {@code W} &rarr; MLflow Role
 *       {@code <prefix>group_<G>} in workspace {@code W}. Assigned users are
 *       resolved from {@link RangerUserStore#getUserGroupMapping()}.</li>
 *   <li><b>Ranger User</b> {@code U} in workspace {@code W} &rarr; MLflow Role
 *       {@code <prefix>user_<U>} in workspace {@code W} (assigned to {@code U}).
 *       Note: we intentionally use {@code <prefix>user_<U>} rather than MLflow's
 *       internal {@code __user_<id>__} synthetic role because MLflow reserves the
 *       {@code __user_} prefix and rejects role CRUD on it.</li>
 *   <li><b>Access types</b> {@code read}, {@code use}, {@code edit}, {@code manage}
 *       map to MLflow permission tiers {@code READ} (1), {@code USE} (2),
 *       {@code EDIT} (3), {@code MANAGE} (4). Multiple grants on the same
 *       {@code (role, resource_type, resource_pattern)} are merged using
 *       {@link #maxPermission(String, String)}, matching
 *       {@code mlflow.server.auth.permissions.max_permission}.</li>
 *   <li><b>Workspace-level grants</b> ({@code resource_type="workspace"}):
 *       MLflow's {@code _validate_permission_for_resource_type} only accepts
 *       {@code USE} or {@code MANAGE} on {@code ("workspace", "*")}. Consistent
 *       with MLflow's {@code e5f6a7b8c9d0} RBAC migration:
 *       <ul>
 *         <li>{@code read} or {@code use} on {@code workspace} &rarr;
 *             {@code ("workspace", "*", "USE")}</li>
 *         <li>{@code edit} on {@code workspace} &rarr;
 *             {@code ("workspace", "*", "USE")} plus {@code (T, "*", "EDIT")}
 *             across all 8 concrete resource types</li>
 *         <li>{@code manage} on {@code workspace} &rarr;
 *             {@code ("workspace", "*", "MANAGE")}</li>
 *       </ul>
 *   </li>
 *   <li><b>Concrete resource grants</b>: Every resource grant in MLflow requires
 *       the user to have workspace presence when workspaces are enabled. And when
 *       a policy grants access to a concrete resource (e.g. {@code experiment=12})
 *       without an explicit workspace grant, the converter emits the concrete
 *       {@code ("experiment", "12", PERM)} row on the target workspace role.</li>
 * </ul>
 */
public final class RbacConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RbacConverter.class);

    public static final String RESOURCE_WORKSPACE                = "workspace";
    public static final String RESOURCE_EXPERIMENT               = "experiment";
    public static final String RESOURCE_REGISTERED_MODEL         = "registered_model";
    public static final String RESOURCE_PROMPT                   = "prompt";
    public static final String RESOURCE_SCORER                   = "scorer";
    public static final String RESOURCE_GATEWAY_SECRET           = "gateway_secret";
    public static final String RESOURCE_GATEWAY_ENDPOINT         = "gateway_endpoint";
    public static final String RESOURCE_GATEWAY_MODEL_DEFINITION = "gateway_model_definition";
    public static final String RESOURCE_MCP_SERVER               = "mcp_server";

    /**
     * All concrete (non-workspace) resource types in MLflow 3.16.0
     * ({@code mlflow/server/auth/permissions.py}).
     */
    public static final List<String> CONCRETE_RESOURCE_TYPES = Collections.unmodifiableList(Arrays.asList(
            RESOURCE_EXPERIMENT,
            RESOURCE_REGISTERED_MODEL,
            RESOURCE_PROMPT,
            RESOURCE_SCORER,
            RESOURCE_GATEWAY_SECRET,
            RESOURCE_GATEWAY_ENDPOINT,
            RESOURCE_GATEWAY_MODEL_DEFINITION,
            RESOURCE_MCP_SERVER
    ));

    public static final String PERM_READ   = "READ";
    public static final String PERM_USE    = "USE";
    public static final String PERM_EDIT   = "EDIT";
    public static final String PERM_MANAGE = "MANAGE";

    public static final String WILDCARD = "*";

    private static final Map<String, Integer> PERMISSION_PRIORITY;
    static {
        Map<String, Integer> m = new HashMap<>();
        m.put(PERM_READ,   1);
        m.put(PERM_USE,    2);
        m.put(PERM_EDIT,   3);
        m.put(PERM_MANAGE, 4);
        PERMISSION_PRIORITY = Collections.unmodifiableMap(m);
    }

    private final String managedRolePrefix;
    private final String defaultWorkspace;

    public RbacConverter() {
        this(AgentConfig.DEFAULT_MANAGED_ROLE_PREFIX, AgentConfig.DEFAULT_WORKSPACE_NAME);
    }

    public RbacConverter(String managedRolePrefix, String defaultWorkspace) {
        this.managedRolePrefix = Objects.requireNonNull(managedRolePrefix, "managedRolePrefix");
        this.defaultWorkspace  = Objects.requireNonNull(defaultWorkspace, "defaultWorkspace");
        if (managedRolePrefix.startsWith(AgentConfig.MLFLOW_SYNTHETIC_ROLE_PREFIX)) {
            throw new IllegalArgumentException(
                    "managedRolePrefix must not start with reserved prefix '"
                    + AgentConfig.MLFLOW_SYNTHETIC_ROLE_PREFIX + "'");
        }
    }

    /**
     * Converts Ranger policies, user-store group mappings, and role definitions
     * into a deterministic map of {@link WorkspaceRoleKey} &rarr; {@link MlflowRoleSpec}.
     */
    public Map<WorkspaceRoleKey, MlflowRoleSpec> convert(ServicePolicies servicePolicies,
                                                         RangerUserStore userStore,
                                                         RangerRoles rangerRoles,
                                                         Collection<String> knownWorkspaces) {
        List<RangerPolicy> policies = (servicePolicies == null || servicePolicies.getPolicies() == null)
                ? Collections.emptyList()
                : servicePolicies.getPolicies();

        Set<RangerRole> roleDefs = collectRoleDefinitions(servicePolicies, rangerRoles);
        Map<String, Set<String>> groupToUsers = invertUserGroupMapping(userStore);
        Map<String, Set<String>> roleToUsers  = flattenRolesToUsers(roleDefs, groupToUsers);

        Set<String> expandedWorkspaces = new LinkedHashSet<>();
        expandedWorkspaces.add(defaultWorkspace);
        if (knownWorkspaces != null) {
            for (String ws : knownWorkspaces) {
                if (ws != null && !ws.isBlank()) {
                    expandedWorkspaces.add(ws.trim());
                }
            }
        }

        Map<WorkspaceRoleKey, RoleAccumulator> accumulators = new TreeMap<>();
        int processed = 0;
        int skipped   = 0;

        for (RangerPolicy policy : policies) {
            if (!isEligible(policy)) {
                skipped++;
                continue;
            }

            List<String> targetWorkspaces = resolveTargetWorkspaces(policy, expandedWorkspaces);
            List<ResourceTarget> resourceTargets = extractResourceTargets(policy);
            if (targetWorkspaces.isEmpty() || resourceTargets.isEmpty()) {
                skipped++;
                continue;
            }

            for (RangerPolicyItem item : policy.getPolicyItems()) {
                String highestPerm = highestPermissionFromItem(item);
                if (highestPerm == null) {
                    continue;
                }

                List<PrincipalTarget> principals = extractPrincipals(item, groupToUsers, roleToUsers);
                if (principals.isEmpty()) {
                    continue;
                }

                for (String workspace : targetWorkspaces) {
                    for (PrincipalTarget principal : principals) {
                        WorkspaceRoleKey roleKey = new WorkspaceRoleKey(workspace, principal.mlflowRoleName);
                        RoleAccumulator acc = accumulators.computeIfAbsent(
                                roleKey,
                                k -> new RoleAccumulator(workspace, principal.mlflowRoleName, principal.description));
                        acc.assignedUsers.addAll(principal.users);

                        for (ResourceTarget target : resourceTargets) {
                            applyResourceGrant(acc, target.resourceType, target.resourcePattern, highestPerm);
                        }
                    }
                }
            }
            processed++;
        }

        Map<WorkspaceRoleKey, MlflowRoleSpec> result = new LinkedHashMap<>();
        for (Map.Entry<WorkspaceRoleKey, RoleAccumulator> entry : accumulators.entrySet()) {
            RoleAccumulator acc = entry.getValue();
            if (acc.permissions.isEmpty()) {
                continue;
            }
            result.put(entry.getKey(), new MlflowRoleSpec(
                    acc.workspace,
                    acc.roleName,
                    acc.description,
                    acc.permissions,
                    acc.assignedUsers));
        }

        LOG.info("Converted Ranger policies to MLflow RBAC: policiesProcessed={}, policiesSkipped={}, mlflowRoles={}",
                processed, skipped, result.size());
        return result;
    }

    /**
     * Convenience overload when all policies target configured/explicit workspaces.
     */
    public Map<WorkspaceRoleKey, MlflowRoleSpec> convert(List<RangerPolicy> policies,
                                                         RangerUserStore userStore,
                                                         RangerRoles rangerRoles) {
        ServicePolicies sp = new ServicePolicies();
        sp.setPolicies(policies);
        return convert(sp, userStore, rangerRoles, Collections.singleton(defaultWorkspace));
    }

    public boolean isManagedRoleName(String roleName) {
        return roleName != null
                && !roleName.startsWith(AgentConfig.MLFLOW_SYNTHETIC_ROLE_PREFIX)
                && roleName.startsWith(managedRolePrefix);
    }

    public static boolean isSyntheticRoleName(String roleName) {
        return roleName != null && roleName.startsWith(AgentConfig.MLFLOW_SYNTHETIC_ROLE_PREFIX);
    }

    public String roleNameForUser(String username) {
        return managedRolePrefix + "user_" + sanitizeIdentifier(username);
    }

    public String roleNameForGroup(String groupName) {
        return managedRolePrefix + "group_" + sanitizeIdentifier(groupName);
    }

    public String roleNameForRangerRole(String rangerRoleName) {
        return managedRolePrefix + "role_" + sanitizeIdentifier(rangerRoleName);
    }

    static String sanitizeIdentifier(String raw) {
        if (raw == null) {
            return "unknown";
        }
        return raw.trim().replaceAll("[^a-zA-Z0-9._@-]", "_");
    }

    /**
     * Mirrors {@code mlflow.server.auth.permissions.max_permission(a, b)}.
     */
    public static String maxPermission(String a, String b) {
        if (a == null) return b;
        if (b == null) return a;
        int prioA = PERMISSION_PRIORITY.getOrDefault(a, 0);
        int prioB = PERMISSION_PRIORITY.getOrDefault(b, 0);
        return prioA >= prioB ? a : b;
    }

    // -----------------------------------------------------------------------
    // Grant application & MLflow 3.16.0 constraint enforcement
    // -----------------------------------------------------------------------

    private static void applyResourceGrant(RoleAccumulator acc,
                                           String resourceType,
                                           String resourcePattern,
                                           String permission) {
        if (RESOURCE_WORKSPACE.equals(resourceType)) {
            // MLflow 3.16.0 requires resource_pattern="*" for resource_type="workspace"
            // and only allows USE or MANAGE in WORKSPACE_GRANTABLE_PERMISSIONS.
            if (PERM_MANAGE.equals(permission)) {
                acc.mergeGrant(RESOURCE_WORKSPACE, WILDCARD, PERM_MANAGE);
            } else if (PERM_EDIT.equals(permission)) {
                // Mirror MLflow migration e5f6a7b8c9d0: workspace EDIT fans out to
                // ("workspace", "*", "USE") + (concrete_type, "*", "EDIT").
                acc.mergeGrant(RESOURCE_WORKSPACE, WILDCARD, PERM_USE);
                for (String concreteType : CONCRETE_RESOURCE_TYPES) {
                    acc.mergeGrant(concreteType, WILDCARD, PERM_EDIT);
                }
            } else {
                // READ or USE on workspace -> ("workspace", "*", "USE")
                acc.mergeGrant(RESOURCE_WORKSPACE, WILDCARD, PERM_USE);
            }
            return;
        }

        if (!isSupportedPattern(resourcePattern)) {
            LOG.warn("Skipping unsupported partial wildcard pattern '{}' for resource_type '{}' "
                    + "(MLflow 3.16.0 native RBAC supports exact resource IDs or '*')",
                    resourcePattern, resourceType);
            return;
        }

        acc.mergeGrant(resourceType, resourcePattern, permission);
    }

    static boolean isSupportedPattern(String pattern) {
        if (pattern == null || pattern.isBlank()) {
            return false;
        }
        String trimmed = pattern.trim();
        if (WILDCARD.equals(trimmed)) {
            return true;
        }
        return trimmed.indexOf('*') < 0 && trimmed.indexOf('?') < 0;
    }

    // -----------------------------------------------------------------------
    // Policy inspection helpers
    // -----------------------------------------------------------------------

    private static boolean isEligible(RangerPolicy policy) {
        if (policy == null) {
            return false;
        }
        if (Boolean.FALSE.equals(policy.getIsEnabled())) {
            return false;
        }
        Integer policyType = policy.getPolicyType();
        if (policyType != null && policyType != RangerPolicy.POLICY_TYPE_ACCESS) {
            return false;
        }
        return policy.getPolicyItems() != null && !policy.getPolicyItems().isEmpty();
    }

    private List<String> resolveTargetWorkspaces(RangerPolicy policy, Set<String> knownWorkspaces) {
        Map<String, RangerPolicyResource> resources = policy.getResources();
        if (resources == null) {
            return Collections.emptyList();
        }
        RangerPolicyResource wsRes = resources.get(RESOURCE_WORKSPACE);
        if (wsRes == null || wsRes.getValues() == null || wsRes.getValues().isEmpty()) {
            return Collections.singletonList(defaultWorkspace);
        }
        if (Boolean.TRUE.equals(wsRes.getIsExcludes())) {
            LOG.warn("Policy '{}' (id={}) uses isExcludes on workspace; skipping",
                    policy.getName(), policy.getId());
            return Collections.emptyList();
        }

        Set<String> resolved = new LinkedHashSet<>();
        for (String raw : wsRes.getValues()) {
            if (raw == null || raw.isBlank()) {
                continue;
            }
            String trimmed = raw.trim();
            if (WILDCARD.equals(trimmed)) {
                resolved.addAll(knownWorkspaces);
            } else if (isSupportedPattern(trimmed)) {
                resolved.add(trimmed);
            } else {
                LOG.warn("Policy '{}' (id={}) has partial wildcard workspace '{}'; skipping value",
                        policy.getName(), policy.getId(), trimmed);
            }
        }
        return new ArrayList<>(resolved);
    }

    private static List<ResourceTarget> extractResourceTargets(RangerPolicy policy) {
        Map<String, RangerPolicyResource> resources = policy.getResources();
        if (resources == null || resources.isEmpty()) {
            return Collections.emptyList();
        }

        List<ResourceTarget> concreteTargets = new ArrayList<>();
        for (String concreteType : CONCRETE_RESOURCE_TYPES) {
            RangerPolicyResource res = resources.get(concreteType);
            if (res == null || res.getValues() == null || res.getValues().isEmpty()) {
                continue;
            }
            if (Boolean.TRUE.equals(res.getIsExcludes())) {
                LOG.warn("Policy '{}' (id={}) uses isExcludes on '{}'; skipping resource",
                        policy.getName(), policy.getId(), concreteType);
                continue;
            }
            for (String val : res.getValues()) {
                if (val != null && !val.isBlank()) {
                    concreteTargets.add(new ResourceTarget(concreteType, val.trim()));
                }
            }
        }

        if (!concreteTargets.isEmpty()) {
            return concreteTargets;
        }

        // If no concrete child resource is specified, the policy targets the workspace itself.
        RangerPolicyResource wsRes = resources.get(RESOURCE_WORKSPACE);
        if (wsRes != null && wsRes.getValues() != null && !wsRes.getValues().isEmpty()) {
            return Collections.singletonList(new ResourceTarget(RESOURCE_WORKSPACE, WILDCARD));
        }

        return Collections.emptyList();
    }

    private static String highestPermissionFromItem(RangerPolicyItem item) {
        if (item == null || item.getAccesses() == null) {
            return null;
        }
        String best = null;
        for (RangerPolicyItemAccess access : item.getAccesses()) {
            if (access == null || Boolean.FALSE.equals(access.getIsAllowed())) {
                continue;
            }
            String mapped = mapAccessTypeToPermission(access.getType());
            if (mapped != null) {
                best = maxPermission(best, mapped);
            }
        }
        return best;
    }

    static String mapAccessTypeToPermission(String accessType) {
        if (accessType == null) {
            return null;
        }
        switch (accessType.trim().toLowerCase()) {
            case "read":
                return PERM_READ;
            case "use":
                return PERM_USE;
            case "edit":
            case "update":
            case "write":
                return PERM_EDIT;
            case "manage":
            case "admin":
                return PERM_MANAGE;
            default:
                LOG.warn("Ignoring unknown Ranger access type '{}' for MLflow", accessType);
                return null;
        }
    }

    private List<PrincipalTarget> extractPrincipals(RangerPolicyItem item,
                                                    Map<String, Set<String>> groupToUsers,
                                                    Map<String, Set<String>> roleToUsers) {
        List<PrincipalTarget> targets = new ArrayList<>();

        if (item.getUsers() != null) {
            for (String user : item.getUsers()) {
                if (user == null || user.isBlank() || "{USER}".equalsIgnoreCase(user.trim()) || "{OWNER}".equalsIgnoreCase(user.trim())) {
                    continue;
                }
                String u = user.trim();
                targets.add(new PrincipalTarget(
                        roleNameForUser(u),
                        "Ranger-managed role for user '" + u + "'",
                        Collections.singleton(u)));
            }
        }

        if (item.getGroups() != null) {
            for (String group : item.getGroups()) {
                if (group == null || group.isBlank()) {
                    continue;
                }
                String g = group.trim();
                Set<String> members = groupToUsers.getOrDefault(g, Collections.emptySet());
                targets.add(new PrincipalTarget(
                        roleNameForGroup(g),
                        "Ranger-managed role for group '" + g + "'",
                        members));
            }
        }

        if (item.getRoles() != null) {
            for (String role : item.getRoles()) {
                if (role == null || role.isBlank()) {
                    continue;
                }
                String r = role.trim();
                Set<String> members = roleToUsers.getOrDefault(r, Collections.emptySet());
                targets.add(new PrincipalTarget(
                        roleNameForRangerRole(r),
                        "Ranger-managed role for Ranger role '" + r + "'",
                        members));
            }
        }

        return targets;
    }

    // -----------------------------------------------------------------------
    // UserStore (group -> users) and RangerRoles (role -> users) resolution
    // -----------------------------------------------------------------------

    static Map<String, Set<String>> invertUserGroupMapping(RangerUserStore userStore) {
        if (userStore == null || userStore.getUserGroupMapping() == null) {
            return Collections.emptyMap();
        }
        Map<String, Set<String>> groupToUsers = new HashMap<>();
        Set<String> allUsers = new TreeSet<>();

        for (Map.Entry<String, Set<String>> entry : userStore.getUserGroupMapping().entrySet()) {
            String user = entry.getKey();
            if (user == null || user.isBlank()) {
                continue;
            }
            String trimmedUser = user.trim();
            allUsers.add(trimmedUser);
            Set<String> groups = entry.getValue();
            if (groups == null) {
                continue;
            }
            for (String group : groups) {
                if (group != null && !group.isBlank()) {
                    groupToUsers.computeIfAbsent(group.trim(), k -> new TreeSet<>()).add(trimmedUser);
                }
            }
        }

        if (userStore.getUserAttrMapping() != null) {
            for (String user : userStore.getUserAttrMapping().keySet()) {
                if (user != null && !user.isBlank()) {
                    allUsers.add(user.trim());
                }
            }
        }

        if (!allUsers.isEmpty()) {
            groupToUsers.computeIfAbsent("public", k -> new TreeSet<>()).addAll(allUsers);
        }

        return groupToUsers;
    }

    static Set<RangerRole> collectRoleDefinitions(ServicePolicies servicePolicies, RangerRoles rangerRoles) {
        Set<RangerRole> all = new HashSet<>();
        if (rangerRoles != null && rangerRoles.getRangerRoles() != null) {
            all.addAll(rangerRoles.getRangerRoles());
        }
        return all;
    }

    static Map<String, Set<String>> flattenRolesToUsers(Set<RangerRole> roleDefs,
                                                        Map<String, Set<String>> groupToUsers) {
        if (roleDefs == null || roleDefs.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, RangerRole> byName = new HashMap<>();
        for (RangerRole r : roleDefs) {
            if (r != null && r.getName() != null && !r.getName().isBlank()) {
                byName.put(r.getName().trim(), r);
            }
        }

        Map<String, Set<String>> resolved = new HashMap<>();
        for (String roleName : byName.keySet()) {
            Set<String> users = new TreeSet<>();
            resolveRoleMembersRecursive(roleName, byName, groupToUsers, users, new HashSet<>());
            resolved.put(roleName, users);
        }
        return resolved;
    }

    private static void resolveRoleMembersRecursive(String roleName,
                                                    Map<String, RangerRole> byName,
                                                    Map<String, Set<String>> groupToUsers,
                                                    Set<String> outUsers,
                                                    Set<String> visitedRoles) {
        if (!visitedRoles.add(roleName)) {
            return;
        }
        RangerRole role = byName.get(roleName);
        if (role == null) {
            return;
        }
        if (role.getUsers() != null) {
            for (RoleMember u : role.getUsers()) {
                if (u != null && u.getName() != null && !u.getName().isBlank()) {
                    outUsers.add(u.getName().trim());
                }
            }
        }
        if (role.getGroups() != null) {
            for (RoleMember g : role.getGroups()) {
                if (g != null && g.getName() != null && !g.getName().isBlank()) {
                    Set<String> gUsers = groupToUsers.get(g.getName().trim());
                    if (gUsers != null) {
                        outUsers.addAll(gUsers);
                    }
                }
            }
        }
        if (role.getRoles() != null) {
            for (RoleMember childRole : role.getRoles()) {
                if (childRole != null && childRole.getName() != null && !childRole.getName().isBlank()) {
                    resolveRoleMembersRecursive(childRole.getName().trim(), byName, groupToUsers, outUsers, visitedRoles);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Internal value types
    // -----------------------------------------------------------------------

    private static final class ResourceTarget {
        final String resourceType;
        final String resourcePattern;

        ResourceTarget(String resourceType, String resourcePattern) {
            this.resourceType    = resourceType;
            this.resourcePattern = resourcePattern;
        }
    }

    private static final class PrincipalTarget {
        final String mlflowRoleName;
        final String description;
        final Set<String> users;

        PrincipalTarget(String mlflowRoleName, String description, Set<String> users) {
            this.mlflowRoleName = mlflowRoleName;
            this.description    = description;
            this.users          = users;
        }
    }

    private static final class RoleAccumulator {
        final String workspace;
        final String roleName;
        final String description;
        final Map<ResourceGrantKey, String> permissions = new TreeMap<>();
        final Set<String> assignedUsers = new TreeSet<>();

        RoleAccumulator(String workspace, String roleName, String description) {
            this.workspace   = workspace;
            this.roleName    = roleName;
            this.description = description;
        }

        void mergeGrant(String resourceType, String resourcePattern, String permission) {
            ResourceGrantKey key = new ResourceGrantKey(resourceType, resourcePattern);
            String existing = permissions.get(key);
            permissions.put(key, maxPermission(existing, permission));
        }
    }
}
