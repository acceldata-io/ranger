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

package org.apache.ranger.biz;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.GroupInfo;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.UserInfo;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.rest.RangerAccessCheckRequest;
import org.apache.ranger.rest.RangerAccessCheckResponse;
import org.apache.ranger.rest.ServiceREST;
import org.junit.Before;
import org.junit.Test;

public class TestServiceRESTAccessCheck {
    private RangerServiceDef      mlflowServiceDef;
    private RangerPolicyAdminImpl policyAdmin;
    private RangerUserStore       userStore;
    private List<AuthzAuditEvent> capturedAudits;
    private ServiceREST           serviceREST;

    @Before
    public void setUp() throws Exception {
        InputStream in = getClass().getResourceAsStream("/service-defs/ranger-servicedef-mlflow.json");
        assertNotNull("ranger-servicedef-mlflow.json must exist", in);
        try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            RangerServiceDef raw = JsonUtils.jsonToObject(reader, RangerServiceDef.class);
            raw.setId(205L);
            mlflowServiceDef = ServiceDefUtil.normalize(raw);
        }

        // Policy 1: workspace='finance' -> 'use' for group 'ml-engineers'
        RangerPolicy wsUsePolicy = new RangerPolicy();
        wsUsePolicy.setId(1L);
        wsUsePolicy.setService("prod_mlflow");
        wsUsePolicy.setName("finance-workspace-use");
        wsUsePolicy.setIsEnabled(true);
        wsUsePolicy.setIsAuditEnabled(true);
        Map<String, RangerPolicyResource> wsRes = new HashMap<>();
        wsRes.put("workspace", new RangerPolicyResource("finance"));
        wsUsePolicy.setResources(wsRes);
        RangerPolicyItem wsItem = new RangerPolicyItem();
        wsItem.setGroups(Collections.singletonList("ml-engineers"));
        wsItem.setAccesses(Collections.singletonList(new RangerPolicyItemAccess("use", true)));
        wsUsePolicy.setPolicyItems(Collections.singletonList(wsItem));

        // Policy 2: workspace='finance', experiment='/prod/*' -> 'edit' for group 'ml-engineers'
        RangerPolicy expEditPolicy = new RangerPolicy();
        expEditPolicy.setId(2L);
        expEditPolicy.setService("prod_mlflow");
        expEditPolicy.setName("finance-prod-experiments-edit");
        expEditPolicy.setIsEnabled(true);
        expEditPolicy.setIsAuditEnabled(true);
        Map<String, RangerPolicyResource> expRes = new HashMap<>();
        expRes.put("workspace", new RangerPolicyResource("finance"));
        expRes.put("experiment", new RangerPolicyResource("/prod/*"));
        expEditPolicy.setResources(expRes);
        RangerPolicyItem expItem = new RangerPolicyItem();
        expItem.setGroups(Collections.singletonList("ml-engineers"));
        expItem.setAccesses(Collections.singletonList(new RangerPolicyItemAccess("edit", true)));
        expEditPolicy.setPolicyItems(Collections.singletonList(expItem));

        // Policy 3: explicit DENY of 'read' on '/prod/secret-*'
        RangerPolicy expDenyPolicy = new RangerPolicy();
        expDenyPolicy.setId(3L);
        expDenyPolicy.setService("prod_mlflow");
        expDenyPolicy.setName("deny-secret-experiments");
        expDenyPolicy.setIsEnabled(true);
        expDenyPolicy.setIsAuditEnabled(true);
        Map<String, RangerPolicyResource> denyRes = new HashMap<>();
        denyRes.put("workspace", new RangerPolicyResource("finance"));
        denyRes.put("experiment", new RangerPolicyResource("/prod/secret-*"));
        expDenyPolicy.setResources(denyRes);
        RangerPolicyItem denyItem = new RangerPolicyItem();
        denyItem.setGroups(Collections.singletonList("ml-engineers"));
        denyItem.setAccesses(Collections.singletonList(new RangerPolicyItemAccess("read", true)));
        expDenyPolicy.setDenyPolicyItems(Collections.singletonList(denyItem));

        ServicePolicies sp = new ServicePolicies();
        sp.setServiceName("prod_mlflow");
        sp.setServiceDef(mlflowServiceDef);
        sp.setPolicyVersion(42L);
        sp.setPolicies(new ArrayList<>(Arrays.asList(wsUsePolicy, expEditPolicy, expDenyPolicy)));

        RangerPolicyEngineOptions opts = new RangerPolicyEngineOptions();
        opts.disablePolicyRefresher = true;
        RangerPluginContext pluginContext = new RangerPluginContext(
                new RangerPluginConfig("mlflow", null, "prod_mlflow", "cl1", "on-prem", opts));
        policyAdmin = new RangerPolicyAdminImpl(sp, pluginContext, null);

        Map<String, Set<String>> userGroupMap = new HashMap<>();
        userGroupMap.put("alice", new HashSet<>(Collections.singletonList("ml-engineers")));
        userStore = new RangerUserStore(7L, Collections.<UserInfo>emptySet(), Collections.<GroupInfo>emptySet(), userGroupMap);

        capturedAudits = new ArrayList<>();
        RangerAdminAccessAuditLogger auditLogger = new RangerAdminAccessAuditLogger();
        auditLogger.setTestSink(event -> capturedAudits.add(event));

        serviceREST = new ServiceREST() {
            {
                // accessAuditLogger is protected; inject the in-memory logger for the test
                this.accessAuditLogger = auditLogger;
            }

            @Override
            protected void authorizeAccessCheckCaller(String serviceName, HttpServletRequest request) {
                // authorization is covered by the REST layer; this test targets evaluation logic
            }

            @Override
            protected RangerPolicyAdmin getAccessCheckPolicyAdmin(String serviceName) {
                return policyAdmin;
            }

            @Override
            protected RangerUserStore resolveAccessCheckUserStore() {
                return userStore;
            }

            @Override
            protected long resolveAccessCheckUserStoreVersion() {
                return 7L;
            }
        };
    }

    @Test
    public void testSingleEvaluateResolvesGroupsImpliedGrantsAndAudit() {
        Map<String, String> res = new HashMap<>();
        res.put("workspace", "finance");
        res.put("experiment", "/prod/churn-v2");

        RangerAccessCheckRequest.Item item = new RangerAccessCheckRequest.Item();
        item.setId("only");
        item.setResource(res);

        RangerAccessCheckRequest req = new RangerAccessCheckRequest();
        req.setUser("alice");
        req.setAccessType("use"); // satisfied by 'edit' via impliedGrants
        req.setAction("POST /api/2.0/mlflow/runs/create");
        req.setClientIPAddress("10.10.1.25");
        req.setItems(Collections.singletonList(item));

        RangerAccessCheckResponse resp = serviceREST.checkAccess("prod_mlflow", req, null);
        assertEquals(42L, resp.getPolicyVersion());
        assertEquals(1, resp.getResults().size());

        RangerAccessCheckResponse.Result result = resp.getResults().get(0);
        assertEquals("only", result.getId());
        assertTrue(result.isAllowed());
        assertEquals(2L, result.getPolicyId());
        assertTrue(result.isAudited());
        assertEquals(1, capturedAudits.size());

        AuthzAuditEvent evt = capturedAudits.get(0);
        assertEquals("alice", evt.getUser());
        assertEquals("prod_mlflow", evt.getRepositoryName());
        assertEquals("experiment", evt.getResourceType());
        assertEquals("use", evt.getAccessType());
        assertEquals("POST /api/2.0/mlflow/runs/create", evt.getAction());
        assertEquals("10.10.1.25", evt.getClientIP());
        assertEquals(1, evt.getAccessResult());

        // UI capability probe with audit=false suppresses audit emission
        capturedAudits.clear();
        req.setAudit(Boolean.FALSE);
        RangerAccessCheckResponse silent = serviceREST.checkAccess("prod_mlflow", req, null);
        assertTrue(silent.getResults().get(0).isAllowed());
        assertFalse(silent.getResults().get(0).isAudited());
        assertTrue(capturedAudits.isEmpty());

        // Explicit DENY blocks /prod/secret-salaries even though /prod/* grants edit
        RangerAccessCheckRequest.Item secret = new RangerAccessCheckRequest.Item();
        Map<String, String> secretRes = new HashMap<>();
        secretRes.put("workspace", "finance");
        secretRes.put("experiment", "/prod/secret-salaries");
        secret.setResource(secretRes);
        req.setItems(Collections.singletonList(secret));
        req.setAccessType("read");
        req.setAudit(Boolean.TRUE);

        RangerAccessCheckResponse denied = serviceREST.checkAccess("prod_mlflow", req, null);
        assertFalse(denied.getResults().get(0).isAllowed());
        assertEquals(3L, denied.getResults().get(0).getPolicyId());
        assertEquals(1, capturedAudits.size());
        assertEquals(0, capturedAudits.get(0).getAccessResult());
    }

    @Test
    public void testBatchEvaluateKeepsOrderAndEmitsSingleSummaryAudit() {
        List<RangerAccessCheckRequest.Item> items = new ArrayList<>();
        items.add(item("10", "finance", "/prod/fraud-v1"));
        items.add(item("11", "finance", "/prod/secret-roadmap"));
        items.add(item("12", "hr", "/prod/attrition"));

        RangerAccessCheckRequest req = new RangerAccessCheckRequest();
        req.setUser("alice");
        req.setAccessType("read");
        req.setAction("search_experiments");
        req.setItems(items);

        RangerAccessCheckResponse resp = serviceREST.checkAccess("prod_mlflow", req, null);
        assertEquals(42L, resp.getPolicyVersion());
        assertEquals(3, resp.getResults().size());
        assertEquals("10", resp.getResults().get(0).getId());
        assertEquals("11", resp.getResults().get(1).getId());
        assertEquals("12", resp.getResults().get(2).getId());
        assertTrue(resp.getResults().get(0).isAllowed());
        assertFalse(resp.getResults().get(1).isAllowed());
        assertFalse(resp.getResults().get(2).isAllowed());

        // Exactly ONE summary audit emitted for the entire batch
        assertEquals(1, capturedAudits.size());
        assertEquals("evaluated=3,allowed=1", capturedAudits.get(0).getRequestData());
    }

    @Test
    public void testVersionReturnsPolicyRoleAndUserStoreVersions() {
        Map<String, Object> ver = serviceREST.getAccessPolicyVersion("prod_mlflow", null);
        assertEquals("prod_mlflow", ver.get("serviceName"));
        assertEquals(42L, ver.get("policyVersion"));
        assertEquals(7L, ver.get("userStoreVersion"));
    }

    private static RangerAccessCheckRequest.Item item(String id, String workspace, String experiment) {
        Map<String, String> resource = new HashMap<>();
        resource.put("workspace", workspace);
        resource.put("experiment", experiment);

        RangerAccessCheckRequest.Item item = new RangerAccessCheckRequest.Item();
        item.setId(id);
        item.setResource(resource);
        return item;
    }
}
