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
import io.acceldata.mlflow.ranger.MlflowRbacReconciler.ReconcileStats;
import io.acceldata.mlflow.ranger.MlflowRoleSpec.ResourceGrantKey;
import io.acceldata.mlflow.ranger.MlflowRoleSpec.WorkspaceRoleKey;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MlflowRbacReconcilerTest {

    @Test
    void createsMissingRolesDiffsPermissionsAndAssignsUsers() throws Exception {
        MlflowRbacClient client = mock(MlflowRbacClient.class);

        // Existing role ranger_user_alice has READ on experiment:10 and obsolete experiment:99
        MlflowRoleRecord existingAliceRole = new MlflowRoleRecord(
                101,
                "default",
                "ranger_user_alice",
                "desc",
                Arrays.asList(
                        new MlflowRolePermissionRecord(1, 101, "experiment", "10", "READ"),
                        new MlflowRolePermissionRecord(2, 101, "experiment", "99", "READ")));

        // Stale Ranger role that should be deleted in LENIENT mode
        MlflowRoleRecord staleRangerRole = new MlflowRoleRecord(
                102,
                "default",
                "ranger_group_old_team",
                "desc",
                Collections.emptyList());

        // Custom operator role that MUST be preserved in LENIENT mode
        MlflowRoleRecord customOperatorRole = new MlflowRoleRecord(
                103,
                "default",
                "custom_operator_role",
                "desc",
                Collections.emptyList());

        // Reserved synthetic MLflow role __user_5__
        MlflowRoleRecord syntheticRole = new MlflowRoleRecord(
                104,
                "default",
                "__user_5__",
                null,
                Collections.singletonList(
                        new MlflowRolePermissionRecord(3, 104, "experiment", "10", "MANAGE")));

        when(client.listAllRoles()).thenReturn(Arrays.asList(
                existingAliceRole, staleRangerRole, customOperatorRole, syntheticRole));

        when(client.listUsersWithRoles()).thenReturn(Arrays.asList(
                new MlflowUserRecord(5, "alice", false, Collections.emptySet()),
                new MlflowUserRecord(6, "bob", false, Collections.singleton(101))));

        // Desired: ranger_user_alice has EDIT on experiment:10, USE on prompt:rag_v1, assigned to alice only
        Map<ResourceGrantKey, String> desiredPerms = new LinkedHashMap<>();
        desiredPerms.put(new ResourceGrantKey("experiment", "10"), "EDIT");
        desiredPerms.put(new ResourceGrantKey("prompt", "rag_v1"), "USE");

        MlflowRoleSpec desiredAliceSpec = new MlflowRoleSpec(
                "default",
                "ranger_user_alice",
                "Ranger-managed role for user 'alice'",
                desiredPerms,
                Collections.singleton("alice"));

        Map<WorkspaceRoleKey, MlflowRoleSpec> desiredMap = Collections.singletonMap(
                desiredAliceSpec.key(), desiredAliceSpec);

        MlflowRbacReconciler reconciler = new MlflowRbacReconciler(
                client, "ranger_", UnmanagedDoctrine.LENIENT, false);

        ReconcileStats stats = reconciler.reconcile(desiredMap);

        // experiment:10 updated from READ -> EDIT
        verify(client).updateRolePermission(1, "EDIT");
        // prompt:rag_v1 added
        verify(client).addRolePermission(101, "prompt", "rag_v1", "USE");
        // experiment:99 removed
        verify(client).removeRolePermission(2);
        // alice assigned, bob unassigned
        verify(client).assignRole("alice", 101);
        verify(client).unassignRole("bob", 101);
        // stale ranger role deleted, custom operator role and synthetic role untouched
        verify(client).deleteRole(102);
        verify(client, never()).deleteRole(103);
        verify(client, never()).deleteRole(104);
        verify(client, never()).removeRolePermission(3);

        assertThat(stats.permissionsUpdated()).isEqualTo(1);
        assertThat(stats.permissionsAdded()).isEqualTo(1);
        assertThat(stats.permissionsRemoved()).isEqualTo(1);
        assertThat(stats.usersAssigned()).isEqualTo(1);
        assertThat(stats.usersUnassigned()).isEqualTo(1);
        assertThat(stats.rolesDeleted()).isEqualTo(1);
    }

    @Test
    void strictDoctrineDeletesNonRangerRolesAndScrubsSyntheticRolePermissions() throws Exception {
        MlflowRbacClient client = mock(MlflowRbacClient.class);

        MlflowRoleRecord customOperatorRole = new MlflowRoleRecord(
                201,
                "default",
                "custom_operator_role",
                "desc",
                Collections.emptyList());

        MlflowRoleRecord syntheticRole = new MlflowRoleRecord(
                202,
                "default",
                "__user_7__",
                null,
                Collections.singletonList(
                        new MlflowRolePermissionRecord(99, 202, "registered_model", "m1", "MANAGE")));

        when(client.listAllRoles()).thenReturn(Arrays.asList(customOperatorRole, syntheticRole));
        when(client.listUsersWithRoles()).thenReturn(Collections.emptyList());

        MlflowRbacReconciler reconciler = new MlflowRbacReconciler(
                client, "ranger_", UnmanagedDoctrine.STRICT, false);

        ReconcileStats stats = reconciler.reconcile(Collections.emptyMap());

        // Custom role deleted in STRICT mode
        verify(client).deleteRole(201);
        // Synthetic role is NOT deleted (reserved in MLflow), but its direct grant IS removed
        verify(client, never()).deleteRole(202);
        verify(client).removeRolePermission(99);

        assertThat(stats.rolesDeleted()).isEqualTo(1);
        assertThat(stats.permissionsRemoved()).isEqualTo(1);
    }

    @Test
    void autoCreatesMissingMlflowUsersWhenConfigured() throws Exception {
        MlflowRbacClient client = mock(MlflowRbacClient.class);

        when(client.listAllRoles()).thenReturn(Collections.emptyList());
        when(client.listUsersWithRoles()).thenReturn(Collections.emptyList());
        when(client.createRole("default", "ranger_user_newbie", "desc"))
                .thenReturn(new MlflowRoleRecord(301, "default", "ranger_user_newbie", "desc", Collections.emptyList()));
        when(client.createUserWithRandomPassword("newbie"))
                .thenReturn(new MlflowUserRecord(42, "newbie", false, Set.of()));

        MlflowRoleSpec spec = new MlflowRoleSpec(
                "default",
                "ranger_user_newbie",
                "desc",
                Collections.singletonMap(new ResourceGrantKey("workspace", "*"), "USE"),
                Collections.singleton("newbie"));

        MlflowRbacReconciler reconciler = new MlflowRbacReconciler(
                client, "ranger_", UnmanagedDoctrine.LENIENT, true);

        ReconcileStats stats = reconciler.reconcile(Collections.singletonMap(spec.key(), spec));

        verify(client).createUserWithRandomPassword("newbie");
        verify(client).assignRole("newbie", 301);
        assertThat(stats.usersCreated()).isEqualTo(1);
        assertThat(stats.usersAssigned()).isEqualTo(1);
    }
}
