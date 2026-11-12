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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.services.mlflow.RangerMlflowConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * HTTP client used by Ranger Admin to validate connectivity and populate
 * MLflow 3.16.0 policy resource dropdowns.
 *
 * <p>Authenticates against MLflow's {@code basic-auth} app using HTTP Basic
 * Authentication, and queries MLflow's REST APIs ({@code /api/2.0/mlflow/*}
 * and {@code /api/3.0/mlflow/*}) with an optional {@code X-MLFLOW-WORKSPACE}
 * header to scope resource lookups to the workspace selected in the policy form.
 */
public class MlflowClient extends BaseClient {

    private static final Logger LOG = LoggerFactory.getLogger(MlflowClient.class);

    private static final String EXPECTED_MIME_TYPE = "application/json";

    private static final int  CONNECT_TIMEOUT_MS = 5_000;
    private static final int  READ_TIMEOUT_MS    = 10_000;
    private static final long LOOKUP_BUDGET_MS   = 10_000L;

    private static final String ERR_TAIL =
            " You can still save the repository and start creating policies, but you "
            + "would not be able to use autocomplete for resource names. "
            + "Check ranger_admin.log for more info.";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String url;
    private final String userName;
    private final String password;

    public MlflowClient(String serviceName, Map<String, String> configs) {
        super(serviceName, configs, "mlflow-client");

        this.url      = configs == null ? null : configs.get(RangerMlflowConstants.CONFIG_MLFLOW_URL);
        this.userName = configs == null ? null : configs.get(RangerMlflowConstants.CONFIG_USERNAME);
        this.password = configs == null ? null : configs.get(RangerMlflowConstants.CONFIG_PASSWORD);

        if (StringUtils.isBlank(this.url)) {
            LOG.error("No value found for configuration '{}'. MLflow resource lookup will fail.",
                    RangerMlflowConstants.CONFIG_MLFLOW_URL);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("MlflowClient built with url=[{}], user=[{}]", this.url, this.userName);
        }
    }

    public List<String> getWorkspaceList(String matching, List<String> existing) {
        Client client = createHttpClient();
        try {
            String endpoint = normalizedBaseUrl() + RangerMlflowConstants.REST_PATH_WORKSPACES;
            ClientResponse response = null;
            try {
                response = applyAuth(client.resource(endpoint), null).get(ClientResponse.class);
                if (response != null && response.getStatus() == 200) {
                    List<String> workspaces = parseWorkspaceNames(response.getEntity(String.class));
                    if (!workspaces.contains(RangerMlflowConstants.DEFAULT_WORKSPACE)) {
                        workspaces.add(0, RangerMlflowConstants.DEFAULT_WORKSPACE);
                    }
                    return filterMatches(workspaces, matching, existing, false);
                }
                // When MLFLOW_ENABLE_WORKSPACES is false, MLflow returns 400/404 on
                // /api/3.0/mlflow/workspaces (@_disable_if_workspaces_disabled).
                // Fall back to the built-in "default" workspace.
                return filterMatches(
                        Collections.singletonList(RangerMlflowConstants.DEFAULT_WORKSPACE),
                        matching, existing, false);
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        } catch (Throwable t) {
            LOG.warn("Workspace listing fell back to '{}': {}",
                    RangerMlflowConstants.DEFAULT_WORKSPACE, t.getMessage());
            return filterMatches(
                    Collections.singletonList(RangerMlflowConstants.DEFAULT_WORKSPACE),
                    matching, existing, false);
        } finally {
            client.destroy();
        }
    }

    public List<String> getExperimentList(String workspace, String matching, List<String> existing) {
        List<String> ids = fetchPaginated(
                RangerMlflowConstants.REST_PATH_EXPERIMENTS_SEARCH,
                workspace,
                "max_results",
                new PageParser() {
                    @Override
                    public PageResult parse(String json) throws Exception {
                        return parseExperimentIds(json);
                    }
                });
        return filterMatches(ids, matching, existing, false);
    }

    public List<String> getRegisteredModelList(String workspace,
                                               String matching,
                                               List<String> existing,
                                               final boolean promptsOnly) {
        List<String> names = fetchPaginated(
                RangerMlflowConstants.REST_PATH_REGISTERED_MODELS,
                workspace,
                "max_results",
                new PageParser() {
                    @Override
                    public PageResult parse(String json) throws Exception {
                        return parseRegisteredModelsOrPrompts(json, promptsOnly);
                    }
                });
        return filterMatches(names, matching, existing, false);
    }

    public List<String> getScorerList(String workspace, String matching, List<String> existing) {
        String body = fetchSingleJson(RangerMlflowConstants.REST_PATH_SCORERS_LIST, workspace);
        try {
            return filterMatches(parseScorerPatterns(body), matching, existing, false);
        } catch (Exception e) {
            throw wrapException("Unable to parse MLflow scorers response", e);
        }
    }

    public List<String> getGatewaySecretList(String workspace, String matching, List<String> existing) {
        String body = fetchSingleJson(RangerMlflowConstants.REST_PATH_GATEWAY_SECRETS_LIST, workspace);
        try {
            return filterMatches(
                    parseSimpleArrayIds(body,
                            RangerMlflowConstants.JSON_SECRETS,
                            RangerMlflowConstants.JSON_SECRET_ID),
                    matching, existing, false);
        } catch (Exception e) {
            throw wrapException("Unable to parse MLflow gateway secrets response", e);
        }
    }

    public List<String> getGatewayEndpointList(String workspace, String matching, List<String> existing) {
        String body = fetchSingleJson(RangerMlflowConstants.REST_PATH_GATEWAY_ENDPOINTS_LIST, workspace);
        try {
            return filterMatches(
                    parseSimpleArrayIds(body,
                            RangerMlflowConstants.JSON_ENDPOINTS,
                            RangerMlflowConstants.JSON_ENDPOINT_ID),
                    matching, existing, false);
        } catch (Exception e) {
            throw wrapException("Unable to parse MLflow gateway endpoints response", e);
        }
    }

    public List<String> getGatewayModelDefinitionList(String workspace, String matching, List<String> existing) {
        String body = fetchSingleJson(RangerMlflowConstants.REST_PATH_GATEWAY_MODEL_DEFS, workspace);
        try {
            return filterMatches(
                    parseSimpleArrayIds(body,
                            RangerMlflowConstants.JSON_MODEL_DEFINITIONS,
                            RangerMlflowConstants.JSON_MODEL_DEFINITION_ID),
                    matching, existing, false);
        } catch (Exception e) {
            throw wrapException("Unable to parse MLflow gateway model definitions response", e);
        }
    }

    public List<String> getMcpServerList(String workspace, String matching, List<String> existing) {
        List<String> names = fetchPaginated(
                RangerMlflowConstants.REST_PATH_MCP_SERVERS,
                workspace,
                "max_results",
                new PageParser() {
                    @Override
                    public PageResult parse(String json) throws Exception {
                        return parseSimplePagedIds(
                                json,
                                RangerMlflowConstants.JSON_MCP_SERVERS,
                                RangerMlflowConstants.JSON_NAME);
                    }
                });
        return filterMatches(names, matching, existing, false);
    }

    public void verifyConnection() {
        Client client = createHttpClient();
        ClientResponse response = null;
        String endpoint = normalizedBaseUrl() + RangerMlflowConstants.REST_PATH_EXPERIMENTS_SEARCH;
        try {
            WebResource resource = client.resource(endpoint)
                    .queryParam("max_results", "1");
            response = applyAuth(resource, null).get(ClientResponse.class);
            int status = response == null ? -1 : response.getStatus();
            if (status != 200) {
                String body = response == null ? "" : safeReadEntity(response);
                String msg = "Unexpected response from MLflow URL [" + endpoint + "]: status=" + status;
                LOG.error("{} body=[{}]", msg, body);
                HadoopException he = new HadoopException(msg);
                he.generateResponseDataMap(false, msg, msg + ERR_TAIL, null, null);
                throw he;
            }
        } finally {
            if (response != null) {
                response.close();
            }
            client.destroy();
        }
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        Map<String, Object> responseData = new HashMap<String, Object>();
        String missing = missingRequiredConfig(configs);
        if (missing != null) {
            String failureMsg = "MLflow service is missing '" + missing + "'.";
            BaseClient.generateResponseDataMap(false, failureMsg, failureMsg + ERR_TAIL, null, null, responseData);
            return responseData;
        }

        boolean connectivityStatus = false;
        Throwable failure = null;

        MlflowClient client = MlflowConnectionMgr.getMlflowClient(serviceName, configs);
        try {
            client.verifyConnection();
            connectivityStatus = true;
        } catch (Throwable t) {
            failure = t;
            LOG.error("MLflow connection test failed for service [{}]", serviceName, t);
        }

        if (connectivityStatus) {
            String successMsg = "ConnectionTest Successful";
            BaseClient.generateResponseDataMap(true, successMsg, successMsg, null, null, responseData);
        } else {
            String failureMsg = "Unable to connect to MLflow Tracking Server using the given parameters.";
            String message = BaseClient.getMessage(failure);
            BaseClient.generateResponseDataMap(false,
                    (message == null || message.isEmpty()) ? failureMsg : message,
                    failureMsg + ERR_TAIL, null, null, responseData);
        }

        return responseData;
    }

    static String missingRequiredConfig(Map<String, String> configs) {
        if (configs == null || StringUtils.isBlank(configs.get(RangerMlflowConstants.CONFIG_MLFLOW_URL))) {
            return RangerMlflowConstants.CONFIG_MLFLOW_URL;
        }
        return null;
    }

    public static List<String> filterMatches(List<String> values,
                                             String userInput,
                                             List<String> existing,
                                             boolean ignoreCase) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<String>(values.size());
        for (String value : values) {
            if (StringUtils.isBlank(value)) {
                continue;
            }
            if (existing != null && existing.contains(value)) {
                continue;
            }
            if (matchesPrefix(value, userInput, ignoreCase)) {
                result.add(value);
            }
        }
        return result;
    }

    static String normalizeUserInput(String userInput) {
        if (StringUtils.isBlank(userInput)) {
            return "";
        }
        String trimmed = userInput.trim();
        if ("*".equals(trimmed) || "%".equals(trimmed)) {
            return "";
        }
        int star = trimmed.indexOf('*');
        int question = trimmed.indexOf('?');
        int wildcard = star < 0 ? question : (question < 0 ? star : Math.min(star, question));
        return wildcard < 0 ? trimmed : trimmed.substring(0, wildcard);
    }

    static boolean matchesPrefix(String value, String userInput, boolean ignoreCase) {
        String prefix = normalizeUserInput(userInput);
        if (StringUtils.isBlank(prefix)) {
            return true;
        }
        if (ignoreCase) {
            return value.regionMatches(true, 0, prefix, 0, prefix.length());
        }
        return value.startsWith(prefix);
    }

    // -----------------------------------------------------------------------
    // JSON response parsers (package-private for unit tests)
    // -----------------------------------------------------------------------

    static List<String> parseWorkspaceNames(String json) throws Exception {
        return parseSimpleArrayIds(
                json,
                RangerMlflowConstants.JSON_WORKSPACES,
                RangerMlflowConstants.JSON_NAME);
    }

    static PageResult parseExperimentIds(String json) throws Exception {
        return parseSimplePagedIds(
                json,
                RangerMlflowConstants.JSON_EXPERIMENTS,
                RangerMlflowConstants.JSON_EXPERIMENT_ID);
    }

    /**
     * Parses {@code /api/2.0/mlflow/registered-models/search} and partitions
     * entities into prompts vs registered models using MLflow 3.16.0's
     * {@code mlflow.prompt.is_prompt} tag convention.
     */
    static PageResult parseRegisteredModelsOrPrompts(String json, boolean promptsOnly) throws Exception {
        JsonNode root = MAPPER.readTree(json);
        if (root == null) {
            return new PageResult(Collections.<String>emptyList(), null);
        }
        JsonNode array = root.get(RangerMlflowConstants.JSON_REGISTERED_MODELS);
        List<String> ids = new ArrayList<String>();
        if (array != null && array.isArray()) {
            for (JsonNode item : array) {
                String name = text(item, RangerMlflowConstants.JSON_NAME);
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                boolean isPrompt = hasPromptTag(item.get(RangerMlflowConstants.JSON_TAGS));
                if (promptsOnly == isPrompt) {
                    ids.add(name);
                }
            }
        }
        String nextToken = text(root, RangerMlflowConstants.JSON_NEXT_PAGE_TOKEN);
        return new PageResult(ids, nextToken);
    }

    private static boolean hasPromptTag(JsonNode tagsNode) {
        if (tagsNode == null || !tagsNode.isArray()) {
            return false;
        }
        for (JsonNode tag : tagsNode) {
            String key = text(tag, RangerMlflowConstants.JSON_KEY);
            String val = text(tag, RangerMlflowConstants.JSON_VALUE);
            if (RangerMlflowConstants.PROMPT_TAG_KEY.equals(key)
                    && "true".equalsIgnoreCase(val)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Formats scorer grants as {@code <experiment_id>/<url_encoded_scorer_name>},
     * matching {@code SqlAlchemyStore._scorer_pattern} in MLflow 3.16.0.
     */
    static List<String> parseScorerPatterns(String json) throws Exception {
        JsonNode root = MAPPER.readTree(json);
        if (root == null) {
            return Collections.emptyList();
        }
        JsonNode array = root.get(RangerMlflowConstants.JSON_SCORERS);
        if (array == null || !array.isArray()) {
            return Collections.emptyList();
        }
        List<String> patterns = new ArrayList<String>();
        for (JsonNode item : array) {
            String expId = scalarAsString(item, RangerMlflowConstants.JSON_EXPERIMENT_ID);
            String scorerName = text(item, RangerMlflowConstants.JSON_SCORER_NAME);
            if (StringUtils.isBlank(scorerName)) {
                scorerName = text(item, RangerMlflowConstants.JSON_NAME);
            }
            if (StringUtils.isNotBlank(expId) && StringUtils.isNotBlank(scorerName)) {
                patterns.add(formatScorerPattern(expId, scorerName));
            }
        }
        return patterns;
    }

    public static String formatScorerPattern(String experimentId, String scorerName) {
        String encoded = URLEncoder.encode(scorerName, StandardCharsets.UTF_8)
                .replace("+", "%20");
        return experimentId + "/" + encoded;
    }

    static List<String> parseSimpleArrayIds(String json, String arrayField, String idField) throws Exception {
        return parseSimplePagedIds(json, arrayField, idField).ids;
    }

    static PageResult parseSimplePagedIds(String json, String arrayField, String idField) throws Exception {
        JsonNode root = MAPPER.readTree(json);
        if (root == null) {
            return new PageResult(Collections.<String>emptyList(), null);
        }
        JsonNode array = root.get(arrayField);
        List<String> ids = new ArrayList<String>();
        if (array != null && array.isArray()) {
            for (JsonNode item : array) {
                String id = scalarAsString(item, idField);
                if (StringUtils.isNotBlank(id)) {
                    ids.add(id);
                }
            }
        }
        String nextToken = text(root, RangerMlflowConstants.JSON_NEXT_PAGE_TOKEN);
        return new PageResult(ids, nextToken);
    }

    static final class PageResult {
        final List<String> ids;
        final String nextPageToken;

        PageResult(List<String> ids, String nextPageToken) {
            this.ids = ids;
            this.nextPageToken = nextPageToken;
        }
    }

    private interface PageParser {
        PageResult parse(String json) throws Exception;
    }

    // -----------------------------------------------------------------------
    // HTTP transport helpers
    // -----------------------------------------------------------------------

    private List<String> fetchPaginated(String path,
                                        String workspace,
                                        String maxResultsParam,
                                        PageParser parser) {
        if (StringUtils.isBlank(url)) {
            return Collections.emptyList();
        }

        Client client = createHttpClient();
        String endpoint = normalizedBaseUrl() + path;
        List<String> all = new ArrayList<String>();
        String pageToken = null;
        long deadline = System.currentTimeMillis() + LOOKUP_BUDGET_MS;

        try {
            for (int page = 0; page < RangerMlflowConstants.MAX_PAGES; page++) {
                if (System.currentTimeMillis() > deadline) {
                    LOG.warn("MLflow lookup for {} exceeded its {} ms budget after {} item(s); "
                            + "returning partial list", path, LOOKUP_BUDGET_MS, all.size());
                    break;
                }

                WebResource resource = client.resource(endpoint)
                        .queryParam(maxResultsParam, String.valueOf(RangerMlflowConstants.PAGE_LIMIT));
                if (StringUtils.isNotBlank(pageToken)) {
                    resource = resource.queryParam("page_token", pageToken);
                }

                ClientResponse response = null;
                try {
                    response = applyAuth(resource, workspace).get(ClientResponse.class);
                    if (response == null || response.getStatus() != 200) {
                        int status = response == null ? -1 : response.getStatus();
                        String body = response == null ? "" : safeReadEntity(response);
                        String msg = "Unexpected response from MLflow URL [" + endpoint + "]: status=" + status;
                        LOG.error("{} body=[{}]", msg, body);
                        HadoopException he = new HadoopException(msg);
                        he.generateResponseDataMap(false, msg, msg + ERR_TAIL, null, null);
                        throw he;
                    }

                    PageResult parsed = parser.parse(response.getEntity(String.class));
                    all.addAll(parsed.ids);
                    pageToken = parsed.nextPageToken;
                    if (StringUtils.isBlank(pageToken)) {
                        break;
                    }
                } finally {
                    if (response != null) {
                        response.close();
                    }
                }
            }
            return all;
        } catch (HadoopException he) {
            throw he;
        } catch (Throwable t) {
            throw wrapException("Exception while fetching MLflow resources from [" + endpoint + "]", t);
        } finally {
            client.destroy();
        }
    }

    private String fetchSingleJson(String path, String workspace) {
        if (StringUtils.isBlank(url)) {
            return "{}";
        }
        Client client = createHttpClient();
        String endpoint = normalizedBaseUrl() + path;
        ClientResponse response = null;
        try {
            WebResource resource = client.resource(endpoint);
            response = applyAuth(resource, workspace).get(ClientResponse.class);
            if (response == null || response.getStatus() != 200) {
                int status = response == null ? -1 : response.getStatus();
                String body = response == null ? "" : safeReadEntity(response);
                String msg = "Unexpected response from MLflow URL [" + endpoint + "]: status=" + status;
                LOG.error("{} body=[{}]", msg, body);
                HadoopException he = new HadoopException(msg);
                he.generateResponseDataMap(false, msg, msg + ERR_TAIL, null, null);
                throw he;
            }
            return response.getEntity(String.class);
        } catch (HadoopException he) {
            throw he;
        } catch (Throwable t) {
            throw wrapException("Exception while fetching MLflow resources from [" + endpoint + "]", t);
        } finally {
            if (response != null) {
                response.close();
            }
            client.destroy();
        }
    }

    private WebResource.Builder applyAuth(WebResource resource, String workspace) {
        WebResource.Builder builder = resource.accept(EXPECTED_MIME_TYPE);
        if (StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password)) {
            String rawCreds = userName + ":" + decryptedPassword();
            String encoded = Base64.getEncoder().encodeToString(rawCreds.getBytes(StandardCharsets.UTF_8));
            builder = builder.header("Authorization", "Basic " + encoded);
        }
        if (StringUtils.isNotBlank(workspace)) {
            builder = builder.header(RangerMlflowConstants.WORKSPACE_HEADER, workspace);
        }
        return builder;
    }

    private Client createHttpClient() {
        Client client = Client.create();
        client.setConnectTimeout(CONNECT_TIMEOUT_MS);
        client.setReadTimeout(READ_TIMEOUT_MS);
        return client;
    }

    private String normalizedBaseUrl() {
        return url == null ? "" : url.trim().replaceAll("/+$", "");
    }

    private String decryptedPassword() {
        try {
            String decrypted = PasswordUtils.getDecryptPassword(password);
            return decrypted == null ? password : decrypted;
        } catch (Throwable t) {
            LOG.warn("Password decryption failed; using the configured string as-is");
            return password;
        }
    }

    private HadoopException wrapException(String msg, Throwable t) {
        LOG.error(msg, t);
        HadoopException he = new HadoopException(msg, t);
        he.generateResponseDataMap(false, BaseClient.getMessage(t), msg + ERR_TAIL, null, null);
        return he;
    }

    private static String text(JsonNode node, String field) {
        if (node == null || field == null) {
            return null;
        }
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual()) {
            return null;
        }
        String text = value.asText();
        return StringUtils.isBlank(text) ? null : text;
    }

    private static String scalarAsString(JsonNode node, String field) {
        if (node == null || field == null) {
            return null;
        }
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || value.isContainerNode()) {
            return null;
        }
        String text = value.asText();
        return StringUtils.isBlank(text) ? null : text;
    }

    private static String safeReadEntity(ClientResponse response) {
        try {
            return response.getEntity(String.class);
        } catch (Throwable t) {
            return "";
        }
    }
}
