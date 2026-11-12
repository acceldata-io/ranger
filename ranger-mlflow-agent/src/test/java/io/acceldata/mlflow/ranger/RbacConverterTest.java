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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RbacConverterTest {

    private final RbacConverter converter = new RbacConverter("ranger_", "default");

    @Test
    void convertsUserGroupAndRoleGrantsIntoMlflowRoles() {
        RangerPolicy policy = new RangerPolicy();
        policy.setId(10L);
        policy.setName("exp-12-edit");
        policy.setIsEnabled(true);

        Map<String, RangerPolicyResource> resources = new HashMap<>();
        resources.put("workspace", new RangerPolicyResource("default"));
        resources.put("experiment", new RangerPolicyResource(Arrays.asList("12", "15"), false, false));
        policy.setResources(resources);

        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(Collections.singletonList("alice"));
        item.setGroups(Collections.singletonList("ml_engineers"));
        item.setRoles(Collections.singletonList("model_stewards"));
        item.setAccesses(Arrays.asList(
                new RangerPolicyItemAccess("read", true),
                new RangerPolicyItemAccess("edit", true)));
        policy.setPolicyItems(Collections.singletonList(item));

        RangerUserStore userStore = new RangerUserStore();
        Map<String, Set<String>> userGroupMap = new HashMap<>();
        userGroupMap.put("bob", new HashSet<>(Collections.singletonList("ml_engineers")));
        userGroupMap.put("carol", new HashSet<>(Arrays.asList("ml_engineers", "analytics")));
        userGroupMap.put("dave", new HashSet<>(Collections.singletonList("steward_group")));
        userStore.setUserGroupMapping(userGroupMap);

        RangerRole role = new RangerRole();
        role.setName("model_stewards");
        role.setUsers(Collections.singletonList(new RoleMember("erin", false)));
        role.setGroups(Collections.singletonList(new RoleMember("steward_group", false)));
        RangerRoles rangerRoles = new RangerRoles();
        rangerRoles.setRangerRoles(Collections.singleton(role));

        Map<WorkspaceRoleKey, MlflowRoleSpec> result = converter.convert(
                Collections.singletonList(policy), userStore, rangerRoles);

        assertThat(result).hasSize(3);

        MlflowRoleSpec userRole = result.get(new WorkspaceRoleKey("default", "ranger_user_alice"));
        assertThat(userRole).isNotNull();
        assertThat(userRole.assignedUsers()).containsExactly("alice");
        assertThat(userRole.permissions())
                .containsEntry(new ResourceGrantKey("experiment", "12"), "EDIT")
                .containsEntry(new ResourceGrantKey("experiment", "15"), "EDIT");

        MlflowRoleSpec groupRole = result.get(new WorkspaceRoleKey("default", "ranger_group_ml_engineers"));
        assertThat(groupRole).isNotNull();
        assertThat(groupRole.assignedUsers()).containsExactly("bob", "carol");
        assertThat(groupRole.permissions())
                .containsEntry(new ResourceGrantKey("experiment", "12"), "EDIT");

        MlflowRoleSpec rangerRoleSpec = result.get(new WorkspaceRoleKey("default", "ranger_role_model_stewards"));
        assertThat(rangerRoleSpec).isNotNull();
        assertThat(rangerRoleSpec.assignedUsers()).containsExactly("dave", "erin");
        assertThat(rangerRoleSpec.permissions())
                .containsEntry(new ResourceGrantKey("experiment", "12"), "EDIT");
    }

    @Test
    void mergesDuplicateGrantsWithMaxPermissionPriority() {
        RangerPolicy readPolicy = buildSingleResourcePolicy(
                1L, "default", "registered_model", "fraud_detector", "alice", "read");
        RangerPolicy managePolicy = buildSingleResourcePolicy(
                2L, "default", "registered_model", "fraud_detector", "alice", "manage");

        Map<WorkspaceRoleKey, MlflowRoleSpec> result = converter.convert(
                Arrays.asList(readPolicy, managePolicy), null, null);

        MlflowRoleSpec spec = result.get(new WorkspaceRoleKey("default", "ranger_user_alice"));
        assertThat(spec).isNotNull();
        assertThat(spec.permissions())
                .containsEntry(new ResourceGrantKey("registered_model", "fraud_detector"), "MANAGE");
    }

    @Test
    void workspaceGrantsRespectMlflow316TwoTierRulesAndEditFanOut() {
        RangerPolicy readWs = buildWorkspaceOnlyPolicy(1L, "default", "alice", "read");
        RangerPolicy editWs = buildWorkspaceOnlyPolicy(2L, "default", "bob", "edit");
        RangerPolicy manageWs = buildWorkspaceOnlyPolicy(3L, "default", "carol", "manage");

        Map<WorkspaceRoleKey, MlflowRoleSpec> result = converter.convert(
                Arrays.asList(readWs, editWs, manageWs), null, null);

        // READ on workspace -> ("workspace", "*", "USE")
        MlflowRoleSpec aliceSpec = result.get(new WorkspaceRoleKey("default", "ranger_user_alice"));
        assertThat(aliceSpec.permissions())
                .containsOnly(Map.entry(new ResourceGrantKey("workspace", "*"), "USE"));

        // EDIT on workspace -> ("workspace", "*", "USE") + (concrete_type, "*", "EDIT")
        MlflowRoleSpec bobSpec = result.get(new WorkspaceRoleKey("default", "ranger_user_bob"));
        assertThat(bobSpec.permissions())
                .containsEntry(new ResourceGrantKey("workspace", "*"), "USE")
                .containsEntry(new ResourceGrantKey("experiment", "*"), "EDIT")
                .containsEntry(new ResourceGrantKey("registered_model", "*"), "EDIT")
                .containsEntry(new ResourceGrantKey("prompt", "*"), "EDIT")
                .containsEntry(new ResourceGrantKey("scorer", "*"), "EDIT")
                .containsEntry(new ResourceGrantKey("gateway_secret", "*"), "EDIT")
                .containsEntry(new ResourceGrantKey("gateway_endpoint", "*"), "EDIT")
                .containsEntry(new ResourceGrantKey("gateway_model_definition", "*"), "EDIT")
                .containsEntry(new ResourceGrantKey("mcp_server", "*"), "EDIT");

        // MANAGE on workspace -> ("workspace", "*", "MANAGE")
        MlflowRoleSpec carolSpec = result.get(new WorkspaceRoleKey("default", "ranger_user_carol"));
        assertThat(carolSpec.permissions())
                .containsOnly(Map.entry(new ResourceGrantKey("workspace", "*"), "MANAGE"));
    }

    @Test
    void wildcardWorkspaceExpandsAcrossAllKnownMlflowWorkspaces() {
        RangerPolicy policy = buildSingleResourcePolicy(
                1L, "*", "prompt", "customer_support_prompt", "alice", "use");
        ServicePolicies sp = new ServicePolicies();
        sp.setPolicies(Collections.singletonList(policy));

        Map<WorkspaceRoleKey, MlflowRoleSpec> result = converter.convert(
                sp, null, null, Arrays.asList("default", "research", "production"));

        assertThat(result.keySet()).containsExactly(
                new WorkspaceRoleKey("default", "ranger_user_alice"),
                new WorkspaceRoleKey("production", "ranger_user_alice"),
                new WorkspaceRoleKey("research", "ranger_user_alice"));
    }

    @Test
    void skipsPartialWildcardsAndDisabledPolicies() {
        RangerPolicy partialWildcard = buildSingleResourcePolicy(
                1L, "default", "registered_model", "fraud_*", "alice", "read");
        RangerPolicy disabled = buildSingleResourcePolicy(
                2L, "default", "registered_model", "exact_model", "alice", "read");
        disabled.setIsEnabled(false);

        Map<WorkspaceRoleKey, MlflowRoleSpec> result = converter.convert(
                Arrays.asList(partialWildcard, disabled), null, null);
        assertThat(result).isEmpty();
    }

    @Test
    void rejectsReservedMlflowSyntheticPrefix() {
        assertThatThrownBy(() -> new RbacConverter("__user_", "default"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("__user_");
    }

    private static RangerPolicy buildSingleResourcePolicy(long id,
                                                          String workspace,
                                                          String resourceType,
                                                          String resourcePattern,
                                                          String user,
                                                          String accessType) {
        RangerPolicy policy = new RangerPolicy();
        policy.setId(id);
        policy.setName("policy-" + id);
        policy.setIsEnabled(true);

        Map<String, RangerPolicyResource> resources = new HashMap<>();
        resources.put("workspace", new RangerPolicyResource(workspace));
        resources.put(resourceType, new RangerPolicyResource(resourcePattern));
        policy.setResources(resources);

        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(Collections.singletonList(user));
        item.setAccesses(Collections.singletonList(new RangerPolicyItemAccess(accessType, true)));
        policy.setPolicyItems(Collections.singletonList(item));
        return policy;
    }

    private static RangerPolicy buildWorkspaceOnlyPolicy(long id,
                                                         String workspace,
                                                         String user,
                                                         String accessType) {
        RangerPolicy policy = new RangerPolicy();
        policy.setId(id);
        policy.setName("ws-policy-" + id);
        policy.setIsEnabled(true);

        Map<String, RangerPolicyResource> resources = new HashMap<>();
        resources.put("workspace", new RangerPolicyResource(workspace));
        policy.setResources(resources);

        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(Collections.singletonList(user));
        item.setAccesses(Collections.singletonList(new RangerPolicyItemAccess(accessType, true)));
        policy.setPolicyItems(Collections.singletonList(item));
        return policy;
    }
}
