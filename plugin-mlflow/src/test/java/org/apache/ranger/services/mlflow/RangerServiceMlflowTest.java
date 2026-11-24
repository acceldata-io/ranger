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

package org.apache.ranger.services.mlflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Test;

public class RangerServiceMlflowTest {

    private RangerServiceDef loadMlflowServiceDef() throws Exception {
        InputStream in = getClass().getResourceAsStream("/service-defs/ranger-servicedef-mlflow.json");
        assertNotNull(in, "ranger-servicedef-mlflow.json must be on classpath");
        try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            RangerServiceDef def = JsonUtils.jsonToObject(reader, RangerServiceDef.class);
            assertNotNull(def);
            def.setId(205L);
            return ServiceDefUtil.normalize(def);
        }
    }

    @Test
    public void testServiceDefSchemaAndDefaultPoliciesSeedLookupUser() throws Exception {
        assertTrue(EmbeddedServiceDefsUtil.DEFAULT_BOOTSTRAP_SERVICEDEF_LIST.contains("mlflow"));
        RangerServiceDef def = loadMlflowServiceDef();
        assertEquals("mlflow", def.getName());
        assertEquals(9, def.getResources().size());
        assertEquals(4, def.getAccessTypes().size());

        RangerService svc = new RangerService();
        svc.setName("dev_mlflow");
        svc.setType("mlflow");
        Map<String, String> configs = new HashMap<String, String>();
        configs.put(RangerMlflowConstants.CONFIG_AUTH_TYPE, "kerberos");
        configs.put(RangerMlflowConstants.CONFIG_KERBEROS_PRINCIPAL, "mlflow/mlflow-host.odp.local@ODP.LOCAL");
        svc.setConfigs(configs);

        RangerServiceMlflow rangerService = new RangerServiceMlflow();
        rangerService.init(def, svc);

        List<RangerPolicy> defaults = rangerService.getDefaultRangerPolicies();
        assertNotNull(defaults);
        assertFalse(defaults.isEmpty());

        for (RangerPolicy policy : defaults) {
            boolean foundLookupRead = false;
            for (RangerPolicyItem item : policy.getPolicyItems()) {
                if (item.getUsers() != null && item.getUsers().contains("mlflow")) {
                    for (RangerPolicyItemAccess acc : item.getAccesses()) {
                        if ("read".equals(acc.getType()) && Boolean.TRUE.equals(acc.getIsAllowed())) {
                            foundLookupRead = true;
                        }
                    }
                }
            }
            assertTrue(foundLookupRead, "Expected lookup user 'mlflow' to have read on default policy: " + policy.getName());
        }
    }

    @Test
    public void testPolicyEngineImpliedGrantsWildcardsAndExplicitDeny() throws Exception {
        RangerServiceDef def = loadMlflowServiceDef();

        RangerPolicy expPolicy = new RangerPolicy();
        expPolicy.setId(101L);
        expPolicy.setService("dev_mlflow");
        expPolicy.setName("finance-prod-experiments");
        expPolicy.setIsEnabled(true);
        expPolicy.setIsAuditEnabled(true);
        Map<String, RangerPolicyResource> resMap = new HashMap<String, RangerPolicyResource>();
        resMap.put("workspace", new RangerPolicyResource("finance"));
        resMap.put("experiment", new RangerPolicyResource("/prod/*"));
        expPolicy.setResources(resMap);

        RangerPolicyItem allowEdit = new RangerPolicyItem();
        allowEdit.setGroups(Collections.singletonList("ml-engineers"));
        allowEdit.setAccesses(Collections.singletonList(new RangerPolicyItemAccess("edit", true)));
        expPolicy.setPolicyItems(Collections.singletonList(allowEdit));

        RangerPolicy denyRestricted = new RangerPolicy();
        denyRestricted.setId(102L);
        denyRestricted.setService("dev_mlflow");
        denyRestricted.setName("deny-restricted-experiments");
        denyRestricted.setIsEnabled(true);
        denyRestricted.setIsAuditEnabled(true);
        Map<String, RangerPolicyResource> denyRes = new HashMap<String, RangerPolicyResource>();
        denyRes.put("workspace", new RangerPolicyResource("finance"));
        denyRes.put("experiment", new RangerPolicyResource("/prod/restricted-*"));
        denyRestricted.setResources(denyRes);

        RangerPolicyItem denyItem = new RangerPolicyItem();
        denyItem.setGroups(Collections.singletonList("ml-engineers"));
        denyItem.setAccesses(Collections.singletonList(new RangerPolicyItemAccess("read", true)));
        denyRestricted.setDenyPolicyItems(Collections.singletonList(denyItem));

        RangerPolicyItem denyException = new RangerPolicyItem();
        denyException.setGroups(Collections.singletonList("fraud-leads"));
        denyException.setAccesses(Collections.singletonList(new RangerPolicyItemAccess("read", true)));
        denyRestricted.setDenyExceptions(Collections.singletonList(denyException));

        ServicePolicies sp = new ServicePolicies();
        sp.setServiceName("dev_mlflow");
        sp.setServiceDef(def);
        sp.setPolicyVersion(1L);
        sp.setPolicies(new ArrayList<RangerPolicy>(Arrays.asList(expPolicy, denyRestricted)));

        RangerPolicyEngineOptions options = new RangerPolicyEngineOptions();
        RangerPluginContext pluginContext = new RangerPluginContext(
                new RangerPluginConfig("mlflow", null, "dev_mlflow", "cl1", "on-prem", options));
        RangerPolicyEngine engine = new RangerPolicyEngineImpl(sp, pluginContext, null);

        RangerAccessResourceImpl fraudExp = new RangerAccessResourceImpl();
        fraudExp.setServiceDef(def);
        fraudExp.setValue("workspace", "finance");
        fraudExp.setValue("experiment", "/prod/fraud-detector");

        assertTrue(evaluate(engine, fraudExp, "alice", Collections.singleton("ml-engineers"), "read").getIsAllowed());
        assertTrue(evaluate(engine, fraudExp, "alice", Collections.singleton("ml-engineers"), "use").getIsAllowed());
        assertTrue(evaluate(engine, fraudExp, "alice", Collections.singleton("ml-engineers"), "edit").getIsAllowed());
        assertFalse(evaluate(engine, fraudExp, "alice", Collections.singleton("ml-engineers"), "manage").getIsAllowed());

        RangerAccessResourceImpl restrictedExp = new RangerAccessResourceImpl();
        restrictedExp.setServiceDef(def);
        restrictedExp.setValue("workspace", "finance");
        restrictedExp.setValue("experiment", "/prod/restricted-pci");

        assertFalse(evaluate(engine, restrictedExp, "alice", Collections.singleton("ml-engineers"), "read").getIsAllowed());
        assertTrue(evaluate(
                engine,
                restrictedExp,
                "carol",
                new HashSet<String>(Arrays.asList("ml-engineers", "fraud-leads")),
                "edit").getIsAllowed());
    }

    private RangerAccessResult evaluate(RangerPolicyEngine engine,
                                        RangerAccessResourceImpl resource,
                                        String user,
                                        Set<String> groups,
                                        String accessType) {
        RangerAccessRequestImpl req = new RangerAccessRequestImpl(resource, accessType, user, groups, null);
        req.setAccessTime(new Date());
        return engine.evaluatePolicies(req, RangerPolicy.POLICY_TYPE_ACCESS, null);
    }
}
