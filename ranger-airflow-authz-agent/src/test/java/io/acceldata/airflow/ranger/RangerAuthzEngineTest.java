/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.airflow.ranger;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RangerAuthzEngineTest {

    @Test
    @DisplayName("in-memory plugin allows alice trigger on etl_sales and denies bob")
    void allowAndDeny() throws Exception {
        RangerAuthzEngine engine = new RangerAuthzEngine(inMemoryPlugin());
        try {
            assertThat(engine.isReady()).isTrue();
            assertThat(engine.policyVersion()).isEqualTo(118L);

            RangerAccessResult allow = engine.evaluate(request("alice", "etl_sales", "trigger"));
            assertThat(allow.getIsAllowed()).isTrue();
            assertThat(allow.getPolicyId()).isEqualTo(47L);

            RangerAccessResult deny = engine.evaluate(request("bob", "etl_sales", "trigger"));
            assertThat(deny.getIsAllowed()).isFalse();
        } finally {
            engine.close();
        }
    }

    private static RangerAccessRequestImpl request(String user, String dag, String access) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue("dag", dag);
        RangerAccessRequestImpl req = new RangerAccessRequestImpl(
                resource, access, user, Collections.emptySet(), null);
        req.setClientIPAddress("10.4.2.19");
        req.setRequestData("/api/v2/dags/" + dag + "/dagRuns");
        req.setClusterName("odp-dev");
        return req;
    }

    private static RangerBasePlugin inMemoryPlugin() throws Exception {
        RangerServiceDef def;
        try (InputStream in = RangerServiceDef.class.getResourceAsStream(
                "/service-defs/ranger-servicedef-airflow.json")) {
            assertThat(in).as("airflow service-def on classpath").isNotNull();
            def = JsonUtils.jsonToObject(new InputStreamReader(in, StandardCharsets.UTF_8), RangerServiceDef.class);
        }
        def.setMarkerAccessTypes(ServiceDefUtil.getMarkerAccessTypes(def.getAccessTypes()));

        RangerPolicy policy = new RangerPolicy();
        policy.setId(47L);
        policy.setName("alice-trigger-etl-sales");
        policy.setService("odp_airflow");
        policy.setIsEnabled(true);
        policy.setIsAuditEnabled(true);
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        Map<String, RangerPolicyResource> resources = new HashMap<>();
        resources.put("dag", new RangerPolicyResource("etl_sales"));
        policy.setResources(resources);
        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(Collections.singletonList("alice"));
        item.setAccesses(Collections.singletonList(new RangerPolicyItemAccess("trigger", true)));
        policy.setPolicyItems(Collections.singletonList(item));

        ServicePolicies policies = new ServicePolicies();
        policies.setServiceName("odp_airflow");
        policies.setServiceDef(def);
        policies.setPolicyVersion(118L);
        policies.setPolicies(Collections.singletonList(policy));

        RangerPolicyEngineOptions options = new RangerPolicyEngineOptions();
        options.disablePolicyRefresher = true;
        options.disableTagRetriever = true;
        options.disableUserStoreRetriever = true;
        options.disableGdsInfoRetriever = true;

        RangerPluginConfig pluginConfig = new RangerPluginConfig(
                "airflow", "odp_airflow", "airflow", "odp-dev", "on-prem", options);
        return new RangerBasePlugin(pluginConfig, policies, null, null, null, null);
    }
}
