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

import io.acceldata.mlflow.ranger.MlflowRbacClient.MlflowRolePermissionRecord;
import io.acceldata.mlflow.ranger.MlflowRbacClient.MlflowRoleRecord;
import io.acceldata.mlflow.ranger.MlflowRbacClient.MlflowUserRecord;
import io.acceldata.mlflow.ranger.MlflowRoleSpec.ResourceGrantKey;
import io.acceldata.mlflow.ranger.MlflowRoleSpec.WorkspaceRoleKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Diff-based reconciler that converges MLflow 3.16.0's native RBAC database
 * ({@code roles}, {@code role_permissions}, and {@code user_role_assignments})
 * onto the desired state computed by {@link RbacConverter}.
 *
 * <h2>Reconciliation algorithm</h2>
 * <ol>
 *   <li>Fetch current MLflow roles ({@code GET /api/3.0/mlflow/roles/list})
 *       and users with role assignments ({@code GET /api/2.0/mlflow/users/list}).</li>
 *   <li>Ensure any missing MLflow users referenced by desired role assignments exist
 *       when {@code mlflow.auto.create.users=true}.</li>
 *   <li>For every desired {@link MlflowRoleSpec}:
 *     <ul>
 *       <li>Create the role if absent ({@code POST /api/3.0/mlflow/roles/create}).</li>
 *       <li>Diff its {@code role_permissions}: add missing grants, update changed
 *           permission tiers in-place, and remove stale grants.</li>
 *       <li>Diff its {@code user_role_assignments}: assign newly added users and
 *           unassign removed users.</li>
 *     </ul>
 *   </li>
 *   <li>Clean up obsolete roles according to {@link UnmanagedDoctrine}:
 *     <ul>
 *       <li>{@link UnmanagedDoctrine#LENIENT}: Delete only Ranger-managed roles
 *           (matching {@code mlflow.managed.role.prefix}) that are no longer in the
 *           desired map.</li>
 *       <li>{@link UnmanagedDoctrine#STRICT}: Delete all non-synthetic roles not in
 *           the desired map, and strip any residual grants from MLflow's reserved
 *           {@code __user_<id>__} synthetic roles so Ranger is the sole source of
 *           truth.</li>
 *     </ul>
 *   </li>
 * </ol>
 */
public class MlflowRbacReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(MlflowRbacReconciler.class);

    private final MlflowRbacClient  client;
    private final String            managedRolePrefix;
    private final UnmanagedDoctrine doctrine;
    private final boolean           autoCreateUsers;

    public MlflowRbacReconciler(MlflowRbacClient client, AgentConfig config) {
        this(client,
                config.managedRolePrefix(),
                config.doctrine(),
                config.autoCreateUsers());
    }

    public MlflowRbacReconciler(MlflowRbacClient client,
                                String managedRolePrefix,
                                UnmanagedDoctrine doctrine,
                                boolean autoCreateUsers) {
        this.client            = Objects.requireNonNull(client, "client");
        this.managedRolePrefix = Objects.requireNonNull(managedRolePrefix, "managedRolePrefix");
        this.doctrine          = Objects.requireNonNull(doctrine, "doctrine");
        this.autoCreateUsers   = autoCreateUsers;
    }

    public ReconcileStats reconcile(Map<WorkspaceRoleKey, MlflowRoleSpec> desiredRoles) throws IOException {
        Objects.requireNonNull(desiredRoles, "desiredRoles");

        List<MlflowRoleRecord> existingRolesList = client.listAllRoles();
        List<MlflowUserRecord> existingUsersList = client.listUsersWithRoles();

        Map<WorkspaceRoleKey, MlflowRoleRecord> existingRoles = new HashMap<>();
        for (MlflowRoleRecord r : existingRolesList) {
            if (r.workspace() != null && r.name() != null) {
                existingRoles.put(new WorkspaceRoleKey(r.workspace(), r.name()), r);
            }
        }

        Map<String, MlflowUserRecord> usersByName = new HashMap<>();
        Map<Integer, Set<String>> currentUsersByRoleId = new HashMap<>();
        for (MlflowUserRecord u : existingUsersList) {
            usersByName.put(u.username(), u);
            for (Integer roleId : u.assignedRoleIds()) {
                currentUsersByRoleId.computeIfAbsent(roleId, k -> new TreeSet<>()).add(u.username());
            }
        }

        int rolesCreated       = 0;
        int rolesDeleted       = 0;
        int permissionsAdded   = 0;
        int permissionsUpdated = 0;
        int permissionsRemoved = 0;
        int usersAssigned      = 0;
        int usersUnassigned    = 0;
        int usersCreated       = 0;

        // 1. Reconcile all desired Ranger-managed roles
        for (Map.Entry<WorkspaceRoleKey, MlflowRoleSpec> entry : desiredRoles.entrySet()) {
            WorkspaceRoleKey key = entry.getKey();
            MlflowRoleSpec spec  = entry.getValue();

            MlflowRoleRecord liveRole = existingRoles.get(key);
            if (liveRole == null) {
                liveRole = client.createRole(spec.workspace(), spec.roleName(), spec.description());
                existingRoles.put(key, liveRole);
                rolesCreated++;
            }

            int roleId = liveRole.id();

            // Reconcile role_permissions for this role
            Map<ResourceGrantKey, MlflowRolePermissionRecord> livePerms = new HashMap<>();
            for (MlflowRolePermissionRecord p : liveRole.permissions()) {
                livePerms.put(new ResourceGrantKey(p.resourceType(), p.resourcePattern()), p);
            }

            for (Map.Entry<ResourceGrantKey, String> desiredPerm : spec.permissions().entrySet()) {
                ResourceGrantKey grantKey = desiredPerm.getKey();
                String targetPerm         = desiredPerm.getValue();
                MlflowRolePermissionRecord currentPerm = livePerms.get(grantKey);

                if (currentPerm == null) {
                    client.addRolePermission(
                            roleId,
                            grantKey.resourceType(),
                            grantKey.resourcePattern(),
                            targetPerm);
                    permissionsAdded++;
                } else if (!targetPerm.equals(currentPerm.permission())) {
                    client.updateRolePermission(currentPerm.id(), targetPerm);
                    permissionsUpdated++;
                }
            }

            for (Map.Entry<ResourceGrantKey, MlflowRolePermissionRecord> livePerm : livePerms.entrySet()) {
                if (!spec.permissions().containsKey(livePerm.getKey())) {
                    client.removeRolePermission(livePerm.getValue().id());
                    permissionsRemoved++;
                }
            }

            // Reconcile user_role_assignments for this role
            Set<String> currentlyAssigned = currentUsersByRoleId.getOrDefault(roleId, new TreeSet<>());
            Set<String> targetUsers       = new HashSet<>();

            for (String desiredUser : spec.assignedUsers()) {
                MlflowUserRecord userRecord = usersByName.get(desiredUser);
                if (userRecord == null) {
                    if (autoCreateUsers) {
                        userRecord = client.createUserWithRandomPassword(desiredUser);
                        usersByName.put(desiredUser, userRecord);
                        usersCreated++;
                    } else {
                        LOG.debug("Skipping role assignment for user '{}' (not registered in MLflow and mlflow.auto.create.users=false)",
                                desiredUser);
                        continue;
                    }
                }
                targetUsers.add(desiredUser);
                if (!currentlyAssigned.contains(desiredUser)) {
                    client.assignRole(desiredUser, roleId);
                    usersAssigned++;
                }
            }

            for (String assignedUser : currentlyAssigned) {
                if (!targetUsers.contains(assignedUser)) {
                    client.unassignRole(assignedUser, roleId);
                    usersUnassigned++;
                }
            }
        }

        // 2. Clean up stale / unmanaged roles according to doctrine
        for (Map.Entry<WorkspaceRoleKey, MlflowRoleRecord> entry : existingRoles.entrySet()) {
            WorkspaceRoleKey key  = entry.getKey();
            MlflowRoleRecord role = entry.getValue();

            if (desiredRoles.containsKey(key)) {
                continue;
            }

            if (RbacConverter.isSyntheticRoleName(role.name())) {
                // MLflow reserves `__user_<id>__` roles and forbids deleting them via
                // DELETE /api/3.0/mlflow/roles/delete. In STRICT mode, strip their
                // direct resource permissions so they cannot bypass Ranger policies.
                if (doctrine == UnmanagedDoctrine.STRICT && !role.permissions().isEmpty()) {
                    for (MlflowRolePermissionRecord p : role.permissions()) {
                        client.removeRolePermission(p.id());
                        permissionsRemoved++;
                    }
                }
                continue;
            }

            boolean isManagedByRanger = role.name().startsWith(managedRolePrefix);
            if (isManagedByRanger || doctrine == UnmanagedDoctrine.STRICT) {
                client.deleteRole(role.id());
                rolesDeleted++;
            }
        }

        ReconcileStats stats = new ReconcileStats(
                rolesCreated,
                rolesDeleted,
                permissionsAdded,
                permissionsUpdated,
                permissionsRemoved,
                usersAssigned,
                usersUnassigned,
                usersCreated);

        if (stats.hasChanges()) {
            LOG.info("MLflow RBAC reconciliation applied changes: {}", stats);
        } else {
            LOG.debug("MLflow RBAC already in sync with Ranger policies");
        }
        return stats;
    }

    public static final class ReconcileStats {
        private final int rolesCreated;
        private final int rolesDeleted;
        private final int permissionsAdded;
        private final int permissionsUpdated;
        private final int permissionsRemoved;
        private final int usersAssigned;
        private final int usersUnassigned;
        private final int usersCreated;

        public ReconcileStats(int rolesCreated,
                              int rolesDeleted,
                              int permissionsAdded,
                              int permissionsUpdated,
                              int permissionsRemoved,
                              int usersAssigned,
                              int usersUnassigned,
                              int usersCreated) {
            this.rolesCreated       = rolesCreated;
            this.rolesDeleted       = rolesDeleted;
            this.permissionsAdded   = permissionsAdded;
            this.permissionsUpdated = permissionsUpdated;
            this.permissionsRemoved = permissionsRemoved;
            this.usersAssigned      = usersAssigned;
            this.usersUnassigned    = usersUnassigned;
            this.usersCreated       = usersCreated;
        }

        public int rolesCreated()       { return rolesCreated; }
        public int rolesDeleted()       { return rolesDeleted; }
        public int permissionsAdded()   { return permissionsAdded; }
        public int permissionsUpdated() { return permissionsUpdated; }
        public int permissionsRemoved() { return permissionsRemoved; }
        public int usersAssigned()      { return usersAssigned; }
        public int usersUnassigned()    { return usersUnassigned; }
        public int usersCreated()       { return usersCreated; }

        public boolean hasChanges() {
            return rolesCreated > 0
                    || rolesDeleted > 0
                    || permissionsAdded > 0
                    || permissionsUpdated > 0
                    || permissionsRemoved > 0
                    || usersAssigned > 0
                    || usersUnassigned > 0
                    || usersCreated > 0;
        }

        @Override
        public String toString() {
            return "ReconcileStats{"
                    + "rolesCreated=" + rolesCreated
                    + ", rolesDeleted=" + rolesDeleted
                    + ", permissionsAdded=" + permissionsAdded
                    + ", permissionsUpdated=" + permissionsUpdated
                    + ", permissionsRemoved=" + permissionsRemoved
                    + ", usersAssigned=" + usersAssigned
                    + ", usersUnassigned=" + usersUnassigned
                    + ", usersCreated=" + usersCreated
                    + '}';
        }
    }
}
