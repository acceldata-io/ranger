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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Immutable specification of a desired MLflow 3.16.0 native RBAC role within a workspace.
 *
 * <p>Mirrors the three SQLAlchemy tables in {@code mlflow/server/auth/db/models.py}:
 * <ul>
 *   <li>{@code SqlRole}: {@code (workspace, name, description)}</li>
 *   <li>{@code SqlRolePermission}: {@code (role_id, resource_type, resource_pattern, permission)}
 *       with {@code UniqueConstraint("role_id", "resource_type", "resource_pattern")}</li>
 *   <li>{@code SqlUserRoleAssignment}: {@code (user_id, role_id)}</li>
 * </ul>
 */
public final class MlflowRoleSpec {

    private final String workspace;
    private final String roleName;
    private final String description;
    private final Map<ResourceGrantKey, String> permissions;
    private final Set<String> assignedUsers;

    public MlflowRoleSpec(String workspace,
                          String roleName,
                          String description,
                          Map<ResourceGrantKey, String> permissions,
                          Set<String> assignedUsers) {
        this.workspace     = Objects.requireNonNull(workspace, "workspace");
        this.roleName      = Objects.requireNonNull(roleName, "roleName");
        this.description   = description;
        this.permissions   = Collections.unmodifiableMap(new LinkedHashMap<>(
                Objects.requireNonNull(permissions, "permissions")));
        this.assignedUsers = Collections.unmodifiableSet(new TreeSet<>(
                Objects.requireNonNull(assignedUsers, "assignedUsers")));
    }

    public String workspace()                          { return workspace; }
    public String roleName()                           { return roleName; }
    public String description()                        { return description; }
    public Map<ResourceGrantKey, String> permissions() { return permissions; }
    public Set<String> assignedUsers()                 { return assignedUsers; }

    public WorkspaceRoleKey key() {
        return new WorkspaceRoleKey(workspace, roleName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MlflowRoleSpec)) return false;
        MlflowRoleSpec that = (MlflowRoleSpec) o;
        return Objects.equals(workspace, that.workspace)
                && Objects.equals(roleName, that.roleName)
                && Objects.equals(description, that.description)
                && Objects.equals(permissions, that.permissions)
                && Objects.equals(assignedUsers, that.assignedUsers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workspace, roleName, description, permissions, assignedUsers);
    }

    @Override
    public String toString() {
        return "MlflowRoleSpec{"
                + "workspace='" + workspace + '\''
                + ", roleName='" + roleName + '\''
                + ", permissions=" + permissions
                + ", assignedUsers=" + assignedUsers
                + '}';
    }

    /**
     * Composite key identifying a role in MLflow's {@code roles} table
     * ({@code UniqueConstraint("workspace", "name")}).
     */
    public static final class WorkspaceRoleKey implements Comparable<WorkspaceRoleKey> {
        private final String workspace;
        private final String roleName;

        public WorkspaceRoleKey(String workspace, String roleName) {
            this.workspace = Objects.requireNonNull(workspace, "workspace");
            this.roleName  = Objects.requireNonNull(roleName, "roleName");
        }

        public String workspace() { return workspace; }
        public String roleName()  { return roleName; }

        @Override
        public int compareTo(WorkspaceRoleKey other) {
            int cmp = this.workspace.compareTo(other.workspace);
            return cmp != 0 ? cmp : this.roleName.compareTo(other.roleName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof WorkspaceRoleKey)) return false;
            WorkspaceRoleKey that = (WorkspaceRoleKey) o;
            return Objects.equals(workspace, that.workspace)
                    && Objects.equals(roleName, that.roleName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workspace, roleName);
        }

        @Override
        public String toString() {
            return workspace + ":" + roleName;
        }
    }

    /**
     * Composite key identifying a permission row on a role in MLflow's
     * {@code role_permissions} table
     * ({@code UniqueConstraint("role_id", "resource_type", "resource_pattern")}).
     */
    public static final class ResourceGrantKey implements Comparable<ResourceGrantKey> {
        private final String resourceType;
        private final String resourcePattern;

        public ResourceGrantKey(String resourceType, String resourcePattern) {
            this.resourceType    = Objects.requireNonNull(resourceType, "resourceType");
            this.resourcePattern = Objects.requireNonNull(resourcePattern, "resourcePattern");
        }

        public String resourceType()    { return resourceType; }
        public String resourcePattern() { return resourcePattern; }

        @Override
        public int compareTo(ResourceGrantKey other) {
            int cmp = this.resourceType.compareTo(other.resourceType);
            return cmp != 0 ? cmp : this.resourcePattern.compareTo(other.resourcePattern);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ResourceGrantKey)) return false;
            ResourceGrantKey that = (ResourceGrantKey) o;
            return Objects.equals(resourceType, that.resourceType)
                    && Objects.equals(resourcePattern, that.resourcePattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resourceType, resourcePattern);
        }

        @Override
        public String toString() {
            return resourceType + ":" + resourcePattern;
        }
    }
}
