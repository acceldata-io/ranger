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

import io.acceldata.mlflow.ranger.MlflowRbacReconciler.ReconcileStats;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PolicySyncServiceTest {

    @Test
    void advancesPolicyVersionAfterSuccessfulReconciliationAndHandlesServiceIdReset() throws Exception {
        RangerAdminClient adminClient = mock(RangerAdminClient.class);
        MlflowRbacClient mlflowClient = mock(MlflowRbacClient.class);
        MlflowRbacReconciler reconciler = mock(MlflowRbacReconciler.class);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

        when(mlflowClient.listWorkspaces()).thenReturn(Set.of("default"));
        when(reconciler.reconcile(anyMap())).thenReturn(
                new ReconcileStats(1, 0, 1, 0, 0, 1, 0, 0));

        ServicePolicies v1 = new ServicePolicies();
        v1.setServiceId(100L);
        v1.setPolicyVersion(5L);
        RangerPolicy policy = new RangerPolicy();
        policy.setId(1L);
        policy.setName("p1");
        policy.setIsEnabled(true);
        Map<String, RangerPolicyResource> res = new HashMap<>();
        res.put("workspace", new RangerPolicyResource("default"));
        res.put("experiment", new RangerPolicyResource("1"));
        policy.setResources(res);
        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(Collections.singletonList("alice"));
        item.setAccesses(Collections.singletonList(new RangerPolicyItemAccess("read", true)));
        policy.setPolicyItems(Collections.singletonList(item));
        v1.setPolicies(Collections.singletonList(policy));

        when(adminClient.getServicePoliciesIfUpdated(-1L, 0L)).thenReturn(v1);

        PolicySyncService sync = new PolicySyncService(
                testConfig(),
                adminClient,
                new RbacConverter("ranger_", "default"),
                mlflowClient,
                reconciler,
                scheduler);

        // Cycle 1: downloads v1 (version 5, serviceId 100) and reconciles
        sync.runOneCycle();
        assertThat(sync.isReady()).isTrue();
        assertThat(sync.lastKnownPolicyVersion()).isEqualTo(5L);
        assertThat(sync.successCount()).isEqualTo(1L);
        verify(reconciler, times(1)).reconcile(anyMap());

        // Cycle 2: serviceId changes to 200 (service recreated in Ranger) -> resets version to -1
        ServicePolicies recreated = new ServicePolicies();
        recreated.setServiceId(200L);
        recreated.setPolicyVersion(1L);
        when(adminClient.getServicePoliciesIfUpdated(5L, sync.lastSuccessAtMs())).thenReturn(recreated);

        sync.runOneCycle();
        assertThat(sync.lastKnownPolicyVersion()).isEqualTo(-1L);
    }

    private static AgentConfig testConfig() {
        Properties props = new Properties();
        props.setProperty("ranger.admin.url", "http://ranger:6080");
        props.setProperty("ranger.service.name", "odp_mlflow");
        props.setProperty("mlflow.url", "http://mlflow:5000");
        props.setProperty("mlflow.admin.username", "admin");
        props.setProperty("mlflow.admin.password", "password123456");
        props.setProperty("sync.force.resync.ms", "0");
        return AgentConfig.fromProperties(props, k -> null);
    }
}
