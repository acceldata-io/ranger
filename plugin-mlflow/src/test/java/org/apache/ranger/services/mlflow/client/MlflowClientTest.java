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

package org.apache.ranger.services.mlflow.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.mlflow.RangerMlflowConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MlflowClientTest {

    @BeforeEach
    public void resetCache() {
        MlflowConnectionMgr.clearCache();
    }

    @Test
    public void testMissingRequiredConfigAcrossAuthModes() {
        Map<String, String> cfg = new HashMap<String, String>();
        assertEquals(RangerMlflowConstants.CONFIG_MLFLOW_URL, MlflowClient.missingRequiredConfig(cfg));

        cfg.put(RangerMlflowConstants.CONFIG_MLFLOW_URL, "https://mlflow-1:5000, https://mlflow-2:5000/");
        assertEquals(RangerMlflowConstants.CONFIG_USERNAME, MlflowClient.missingRequiredConfig(cfg));

        cfg.put(RangerMlflowConstants.CONFIG_USERNAME, "mlflow");
        assertEquals(RangerMlflowConstants.CONFIG_PASSWORD, MlflowClient.missingRequiredConfig(cfg));

        cfg.put(RangerMlflowConstants.CONFIG_PASSWORD, "secret");
        assertNull(MlflowClient.missingRequiredConfig(cfg));

        // Kerberos mode requires principal + keytab instead of username/password
        Map<String, String> krbCfg = new HashMap<String, String>();
        krbCfg.put(RangerMlflowConstants.CONFIG_MLFLOW_URL, "https://mlflow-1:5000");
        krbCfg.put(RangerMlflowConstants.CONFIG_AUTH_TYPE, "kerberos");
        assertEquals(RangerMlflowConstants.CONFIG_KERBEROS_PRINCIPAL, MlflowClient.missingRequiredConfig(krbCfg));
        krbCfg.put(RangerMlflowConstants.CONFIG_KERBEROS_PRINCIPAL, "mlflow/host@EXAMPLE.COM");
        assertEquals(RangerMlflowConstants.CONFIG_KERBEROS_KEYTAB, MlflowClient.missingRequiredConfig(krbCfg));
        krbCfg.put(RangerMlflowConstants.CONFIG_KERBEROS_KEYTAB, "/etc/security/keytabs/mlflow.service.keytab");
        assertNull(MlflowClient.missingRequiredConfig(krbCfg));

        // Knox mode accepts either Bearer token or username/password
        Map<String, String> knoxCfg = new HashMap<String, String>();
        knoxCfg.put(RangerMlflowConstants.CONFIG_MLFLOW_URL, "https://knox:8443/gateway/default/mlflow");
        knoxCfg.put(RangerMlflowConstants.CONFIG_AUTH_TYPE, "knox");
        knoxCfg.put(RangerMlflowConstants.CONFIG_KNOX_TOKEN, "eyJhbGciOiJSUzI1NiJ9.token");
        assertNull(MlflowClient.missingRequiredConfig(knoxCfg));
    }

    @Test
    public void testBuildAuthorizationHeaderBasicKnoxAndKerberos() throws Exception {
        Map<String, String> basicCfg = new HashMap<String, String>();
        basicCfg.put(RangerMlflowConstants.CONFIG_MLFLOW_URL, "http://mlflow:5000");
        basicCfg.put(RangerMlflowConstants.CONFIG_USERNAME, "rangerlookup");
        basicCfg.put(RangerMlflowConstants.CONFIG_PASSWORD, "pass123");
        MlflowClient basicClient = new MlflowClient("mlflow_dev", basicCfg);
        String expectedBasic = "Basic " + Base64.getEncoder().encodeToString(
                "rangerlookup:pass123".getBytes(StandardCharsets.UTF_8));
        assertEquals(expectedBasic, basicClient.buildAuthorizationHeader(URI.create("http://mlflow:5000/version")));

        Map<String, String> knoxCfg = new HashMap<String, String>();
        knoxCfg.put(RangerMlflowConstants.CONFIG_MLFLOW_URL, "https://knox:8443/gateway/default/mlflow");
        knoxCfg.put(RangerMlflowConstants.CONFIG_AUTH_TYPE, "knox");
        knoxCfg.put(RangerMlflowConstants.CONFIG_KNOX_TOKEN, "jwt-token-xyz");
        MlflowClient knoxClient = new MlflowClient("mlflow_knox", knoxCfg);
        assertEquals("Bearer jwt-token-xyz",
                knoxClient.buildAuthorizationHeader(URI.create("https://knox:8443/gateway/default/mlflow")));

        Map<String, String> krbCfg = new HashMap<String, String>();
        krbCfg.put(RangerMlflowConstants.CONFIG_MLFLOW_URL, "https://mlflow:5000");
        krbCfg.put(RangerMlflowConstants.CONFIG_AUTH_TYPE, "kerberos");
        MlflowClient krbClient = new MlflowClient("mlflow_krb", krbCfg) {
            @Override
            protected String generateSpnegoToken(URI targetUri) {
                return "spnego-token-for-" + targetUri.getHost();
            }
        };
        assertEquals("Negotiate spnego-token-for-mlflow",
                krbClient.buildAuthorizationHeader(URI.create("https://mlflow:5000/api/2.0/mlflow/experiments/search")));
    }

    @Test
    public void testMultiUrlFailoverAndResourceLookups() {
        Map<String, String> cfg = new HashMap<String, String>();
        cfg.put(RangerMlflowConstants.CONFIG_MLFLOW_URL, "http://down-host:5000, http://active-host:5000/");
        cfg.put(RangerMlflowConstants.CONFIG_USERNAME, "rangerlookup");
        cfg.put(RangerMlflowConstants.CONFIG_PASSWORD, "secret");

        final AtomicInteger downCalls = new AtomicInteger(0);
        final Map<String, String> seenWorkspaces = new HashMap<String, String>();

        MlflowClient client = new MlflowClient("mlflow_ha", cfg) {
            @Override
            protected HttpResult sendRequest(String method, String fullUrl, String jsonBody, String workspace) {
                if (fullUrl.startsWith("http://down-host:5000")) {
                    downCalls.incrementAndGet();
                    throw new RuntimeException("Connection refused");
                }
                seenWorkspaces.put(fullUrl, workspace);
                if (fullUrl.endsWith(RangerMlflowConstants.REST_PATH_WORKSPACES)) {
                    return new HttpResult(200, "{\"workspaces\":[{\"name\":\"default\"},{\"name\":\"finance\"}]}");
                }
                if (fullUrl.endsWith(RangerMlflowConstants.REST_PATH_EXPERIMENTS_SEARCH)) {
                    return new HttpResult(200, "{\"experiments\":["
                            + "{\"experiment_id\":\"10\",\"name\":\"/prod/fraud\"},"
                            + "{\"experiment_id\":\"11\",\"name\":\"/dev/sandbox\"}]}");
                }
                if (fullUrl.contains(RangerMlflowConstants.REST_PATH_REGISTERED_MODELS_SEARCH)) {
                    return new HttpResult(200, "{\"registered_models\":["
                            + "{\"name\":\"credit-risk-v1\",\"tags\":[]},"
                            + "{\"name\":\"triage-system-prompt\",\"tags\":[{\"key\":\"mlflow.prompt.is_prompt\",\"value\":\"true\"}]}"
                            + "]}");
                }
                if (fullUrl.endsWith(RangerMlflowConstants.REST_PATH_SCORERS_LIST)) {
                    return new HttpResult(200, "{\"scorers\":[{\"experiment_id\":\"10\",\"scorer_name\":\"toxicity\"}]}");
                }
                if (fullUrl.endsWith(RangerMlflowConstants.REST_PATH_GATEWAY_ENDPOINTS)) {
                    return new HttpResult(200, "{\"endpoints\":[{\"name\":\"gpt4-prod\"}]}");
                }
                if (fullUrl.endsWith(RangerMlflowConstants.REST_PATH_GATEWAY_SECRETS)) {
                    return new HttpResult(200, "{\"secrets\":[{\"secret_name\":\"openai-api-key\"}]}");
                }
                if (fullUrl.endsWith(RangerMlflowConstants.REST_PATH_GATEWAY_MODEL_DEFINITIONS)) {
                    return new HttpResult(200, "{\"model_definitions\":[{\"name\":\"gpt4o-def\"}]}");
                }
                if (fullUrl.contains(RangerMlflowConstants.REST_PATH_MCP_SERVERS)) {
                    return new HttpResult(200, "{\"mcp_servers\":[{\"name\":\"analytics/sql-tools\"}]}");
                }
                return new HttpResult(404, "");
            }
        };

        assertEquals(Arrays.asList("default", "finance"),
                client.lookupResources("workspace", null, "", null));
        assertTrue(downCalls.get() > 0);

        assertEquals(Collections.singletonList("/prod/fraud"),
                client.lookupResources("experiment", "finance", "/prod", null));
        assertEquals("finance",
                seenWorkspaces.get("http://active-host:5000" + RangerMlflowConstants.REST_PATH_EXPERIMENTS_SEARCH));

        assertEquals(Collections.singletonList("credit-risk-v1"),
                client.lookupResources("registered_model", "finance", "", null));
        assertEquals(Collections.singletonList("triage-system-prompt"),
                client.lookupResources("prompt", "finance", "", null));

        assertEquals(Collections.singletonList("/prod/fraud/toxicity"),
                client.lookupResources("scorer", "finance", "", null));

        assertEquals(Collections.singletonList("gpt4-prod"),
                client.lookupResources("gateway_endpoint", "finance", "", null));
        assertEquals(Collections.singletonList("openai-api-key"),
                client.lookupResources("gateway_secret", "finance", "", null));
        assertEquals(Collections.singletonList("gpt4o-def"),
                client.lookupResources("gateway_model_definition", "finance", "", null));
        assertEquals(Collections.singletonList("analytics/sql-tools"),
                client.lookupResources("mcp_server", "finance", "", null));
    }

    @Test
    public void testSelectedWorkspaceExtractionSkipsWildcards() {
        Map<String, List<String>> resources = new HashMap<String, List<String>>();
        resources.put("workspace", Collections.singletonList("finance"));
        assertEquals("finance", MlflowResourceMgr.resolveSelectedWorkspace(resources));

        resources.put("workspace", Collections.singletonList("fin*"));
        assertNull(MlflowResourceMgr.resolveSelectedWorkspace(resources));

        resources.put("workspace", Arrays.asList("finance", "marketing"));
        assertNull(MlflowResourceMgr.resolveSelectedWorkspace(resources));

        ResourceLookupContext ctx = new ResourceLookupContext();
        ctx.setResourceName("workspace");
        ctx.setUserInput("def");
        assertEquals(Collections.singletonList("default"),
                MlflowResourceMgr.getMlflowResources("mlflow_offline", Collections.<String, String>emptyMap(), ctx));
    }
}
