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
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
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
import java.util.Arrays;
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

    @Test
    @DisplayName("omitted key means any: a prefix-scoped grant allows the class-level read")
    void anyResourceAllowedByPrefixPolicy() throws Exception {
        RangerAuthzEngine engine = new RangerAuthzEngine(inMemoryPlugin());
        try {
            // No dag key at all -- "may alice read any DAG?". She holds read on
            // etl_* only, and per the contract that MUST be allowed.
            RangerAccessRequestImpl any = keylessRequest("alice", "read");
            any.setResourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);
            assertThat(engine.evaluate(any).getIsAllowed())
                    .as("alice holds read on etl_*, so an any-resource read must be allowed")
                    .isTrue();

            // bob holds nothing, so the same question must still deny.
            RangerAccessRequestImpl bobAny = keylessRequest("bob", "read");
            bobAny.setResourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);
            assertThat(engine.evaluate(bobAny).getIsAllowed()).isFalse();
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("regression: the default SELF scope denies the any-resource read")
    void anyResourceDeniedUnderDefaultScope() throws Exception {
        RangerAuthzEngine engine = new RangerAuthzEngine(inMemoryPlugin());
        try {
            // Documents why AuthzServer widens the scope. A keyless resource can
            // only produce MatchType.DESCENDANT against dag=etl_*, and under the
            // default SELF scope RangerDefaultPolicyEvaluator counts that as no
            // match. If this assertion ever flips, Ranger's matching semantics
            // changed and the widening in buildRequest should be revisited.
            assertThat(engine.evaluate(keylessRequest("alice", "read")).getIsAllowed()).isFalse();
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("GAP-18: a keyless check must not be satisfied by another resource type's policy")
    void keylessCheckIsScopedToItsResourceType() throws Exception {
        RangerAuthzEngine engine = new RangerAuthzEngine(inMemoryPlugin());
        try {
            // carol holds read on connection=* and nothing else.
            assertThat(engine.evaluate(typedKeylessRequest("carol", "connection", "read")).getIsAllowed())
                    .as("carol holds connection read")
                    .isTrue();
            assertThat(engine.evaluate(typedKeylessRequest("carol", "variable", "read")).getIsAllowed())
                    .as("a connection grant must NOT satisfy a variable check")
                    .isFalse();
            assertThat(engine.evaluate(typedKeylessRequest("carol", "dag", "read")).getIsAllowed())
                    .as("a connection grant must NOT satisfy a dag check")
                    .isFalse();

            // alice holds read on dag=etl_* -- the prefix case that the
            // widened matching scope exists to serve must still pass.
            assertThat(engine.evaluate(typedKeylessRequest("alice", "dag", "read")).getIsAllowed())
                    .as("a prefix-scoped dag grant must still satisfy the class-level dag read")
                    .isTrue();
            assertThat(engine.evaluate(typedKeylessRequest("alice", "variable", "read")).getIsAllowed())
                    .as("a dag grant must NOT satisfy a variable check")
                    .isFalse();
        } finally {
            engine.close();
        }
    }

    /**
     * Mirrors the request {@code AuthzServer.buildAccessRequest} builds for a
     * check with no key: the resource type is present so the policy engine can
     * tell the nine parallel hierarchies apart, and the empty value is read as
     * "any resource of that type" via SELF_OR_PREFIX.
     */
    private static RangerAccessRequestImpl typedKeylessRequest(String user, String type, String access) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(type, "");
        RangerAccessRequestImpl req = new RangerAccessRequestImpl(
                resource, access, user, Collections.emptySet(), null);
        req.setResourceElementMatchingScopes(Collections.singletonMap(
                type, RangerAccessRequest.ResourceElementMatchingScope.SELF_OR_PREFIX));
        req.setResourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);
        req.setClientIPAddress("10.4.2.19");
        req.setRequestData("/api/v2/" + type);
        req.setClusterName("odp-dev");
        return req;
    }

    private static RangerAccessRequestImpl keylessRequest(String user, String access) {
        RangerAccessRequestImpl req = new RangerAccessRequestImpl(
                new RangerAccessResourceImpl(), access, user, Collections.emptySet(), null);
        req.setClientIPAddress("10.4.2.19");
        req.setRequestData("/api/v2/dags");
        req.setClusterName("odp-dev");
        return req;
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

        // Prefix-scoped read. This is the policy shape that makes the "any"
        // question meaningful: alice can read some DAGs but not all of them.
        RangerPolicy prefixPolicy = new RangerPolicy();
        prefixPolicy.setId(48L);
        prefixPolicy.setName("alice-read-etl-prefix");
        prefixPolicy.setService("odp_airflow");
        prefixPolicy.setIsEnabled(true);
        prefixPolicy.setIsAuditEnabled(true);
        prefixPolicy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        Map<String, RangerPolicyResource> prefixResources = new HashMap<>();
        prefixResources.put("dag", new RangerPolicyResource("etl_*"));
        prefixPolicy.setResources(prefixResources);
        RangerPolicyItem prefixItem = new RangerPolicyItem();
        prefixItem.setUsers(Collections.singletonList("alice"));
        prefixItem.setAccesses(Collections.singletonList(new RangerPolicyItemAccess("read", true)));
        prefixPolicy.setPolicyItems(Collections.singletonList(prefixItem));

        // A grant on a DIFFERENT top-level resource. The nine Airflow resources
        // are parallel level-10 hierarchies, so this is the policy that must
        // never satisfy a dag or variable question (GAP-18).
        RangerPolicy connectionPolicy = new RangerPolicy();
        connectionPolicy.setId(49L);
        connectionPolicy.setName("carol-read-all-connections");
        connectionPolicy.setService("odp_airflow");
        connectionPolicy.setIsEnabled(true);
        connectionPolicy.setIsAuditEnabled(true);
        connectionPolicy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        Map<String, RangerPolicyResource> connectionResources = new HashMap<>();
        connectionResources.put("connection", new RangerPolicyResource("*"));
        connectionPolicy.setResources(connectionResources);
        RangerPolicyItem connectionItem = new RangerPolicyItem();
        connectionItem.setUsers(Collections.singletonList("carol"));
        connectionItem.setAccesses(Collections.singletonList(new RangerPolicyItemAccess("read", true)));
        connectionPolicy.setPolicyItems(Collections.singletonList(connectionItem));

        ServicePolicies policies = new ServicePolicies();
        policies.setServiceName("odp_airflow");
        policies.setServiceDef(def);
        policies.setPolicyVersion(118L);
        policies.setPolicies(Arrays.asList(policy, prefixPolicy, connectionPolicy));

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
