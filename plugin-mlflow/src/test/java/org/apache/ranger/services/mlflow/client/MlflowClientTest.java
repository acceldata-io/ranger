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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.mlflow.RangerMlflowConstants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MlflowClientTest {

    @Test
    public void testParseExperimentsJson() throws Exception {
        String json = "{\"experiments\": ["
                + "{\"experiment_id\": \"0\", \"name\": \"Default\"},"
                + "{\"experiment_id\": \"12\", \"name\": \"fraud_detection\"}"
                + "], \"next_page_token\": \"tok_2\"}";

        MlflowClient.PageResult page = MlflowClient.parseExperimentIds(json);
        assertEquals(Arrays.asList("0", "12"), page.ids);
        assertEquals("tok_2", page.nextPageToken);
    }

    @Test
    public void testParseRegisteredModelsVsPrompts() throws Exception {
        String json = "{\"registered_models\": ["
                + "{\"name\": \"credit_risk_model\", \"tags\": [{\"key\": \"stage\", \"value\": \"prod\"}]},"
                + "{\"name\": \"rag_system_prompt\", \"tags\": [{\"key\": \"mlflow.prompt.is_prompt\", \"value\": \"true\"}]},"
                + "{\"name\": \"churn_classifier\"}"
                + "]}";

        MlflowClient.PageResult models = MlflowClient.parseRegisteredModelsOrPrompts(json, false);
        assertEquals(Arrays.asList("credit_risk_model", "churn_classifier"), models.ids);
        assertNull(models.nextPageToken);

        MlflowClient.PageResult prompts = MlflowClient.parseRegisteredModelsOrPrompts(json, true);
        assertEquals(Collections.singletonList("rag_system_prompt"), prompts.ids);
    }

    @Test
    public void testParseScorersMatchesMlflowScorerPatternEncoding() throws Exception {
        String json = "{\"scorers\": ["
                + "{\"experiment_id\": \"7\", \"scorer_name\": \"correctness\"},"
                + "{\"experiment_id\": 15, \"scorer_name\": \"safety/v2 check\"}"
                + "]}";

        List<String> patterns = MlflowClient.parseScorerPatterns(json);
        assertEquals(Arrays.asList("7/correctness", "15/safety%2Fv2%20check"), patterns);
    }

    @Test
    public void testParseGatewayAndMcpResources() throws Exception {
        String secretsJson = "{\"secrets\": [{\"secret_id\": \"sec-1\", \"secret_name\": \"openai\"}]}";
        assertEquals(Collections.singletonList("sec-1"),
                MlflowClient.parseSimpleArrayIds(
                        secretsJson,
                        RangerMlflowConstants.JSON_SECRETS,
                        RangerMlflowConstants.JSON_SECRET_ID));

        String endpointsJson = "{\"endpoints\": [{\"endpoint_id\": \"ep-9\", \"name\": \"chat-prod\"}]}";
        assertEquals(Collections.singletonList("ep-9"),
                MlflowClient.parseSimpleArrayIds(
                        endpointsJson,
                        RangerMlflowConstants.JSON_ENDPOINTS,
                        RangerMlflowConstants.JSON_ENDPOINT_ID));

        String mcpJson = "{\"mcp_servers\": [{\"name\": \"internal/sql-agent\"}]}";
        assertEquals(Collections.singletonList("internal/sql-agent"),
                MlflowClient.parseSimplePagedIds(
                        mcpJson,
                        RangerMlflowConstants.JSON_MCP_SERVERS,
                        RangerMlflowConstants.JSON_NAME).ids);
    }

    @Test
    public void testFilterMatchesAndWildcardPrefixNormalization() {
        List<String> candidates = Arrays.asList("fraud_v1", "fraud_v2", "churn_v1");

        assertEquals(Arrays.asList("fraud_v1", "fraud_v2"),
                MlflowClient.filterMatches(candidates, "fraud_*", null, false));

        assertEquals(Collections.singletonList("fraud_v2"),
                MlflowClient.filterMatches(candidates, "fraud_", Collections.singletonList("fraud_v1"), false));

        assertEquals(candidates,
                MlflowClient.filterMatches(candidates, "*", null, false));
    }

    @Test
    public void testMissingConfigValidationAndResourceMgrGuards() {
        Map<String, Object> result = MlflowClient.connectionTest(
                "mlflow_dev", Collections.<String, String>emptyMap());
        assertEquals(Boolean.FALSE, result.get("connectivityStatus"));

        Map<String, String> validConfigs = new HashMap<String, String>();
        validConfigs.put(RangerMlflowConstants.CONFIG_MLFLOW_URL, "http://mlflow:5000");
        assertNull(MlflowClient.missingRequiredConfig(validConfigs));

        ResourceLookupContext ctx = new ResourceLookupContext();
        ctx.setResourceName("unknown_resource");
        ctx.setUserInput("");
        assertTrue(MlflowResourceMgr.getMlflowResources("mlflow_dev", validConfigs, ctx).isEmpty());
    }

    @Test
    public void testResolveSelectedWorkspace() {
        assertEquals("default", MlflowResourceMgr.resolveSelectedWorkspace(null));

        Map<String, List<String>> mapWithWildcard = new HashMap<String, List<String>>();
        mapWithWildcard.put(RangerMlflowConstants.RESOURCE_WORKSPACE, Collections.singletonList("*"));
        assertEquals("default", MlflowResourceMgr.resolveSelectedWorkspace(mapWithWildcard));

        Map<String, List<String>> mapWithSpecific = new HashMap<String, List<String>>();
        mapWithSpecific.put(RangerMlflowConstants.RESOURCE_WORKSPACE, Arrays.asList("*", "research"));
        assertEquals("research", MlflowResourceMgr.resolveSelectedWorkspace(mapWithSpecific));
        assertFalse(MlflowClient.matchesPrefix("research", "prod", false));
    }
}
