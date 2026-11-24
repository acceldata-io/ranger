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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.services.mlflow.RangerMlflowConstants;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Multi-auth HTTP client used by Ranger Admin to validate connectivity and
 * autocomplete MLflow 3.16.0+ resources across HTTP Basic/LDAP, Kerberos/SPNEGO,
 * and Apache Knox deployments.
 */
public class MlflowClient extends BaseClient {

    private static final Logger LOG = LoggerFactory.getLogger(MlflowClient.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SPNEGO_OID = "1.3.6.1.5.5.2";

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS    = 10_000;

    private static final String ERR_TAIL =
            " You can still save the repository and start creating policies, but you "
            + "would not be able to use autocomplete for resource names. "
            + "Check ranger_admin.log for more info.";

    private final List<String> baseUrls;
    private final String authType;
    private final String userName;
    private final String password;
    private final String kerberosPrincipal;
    private final String kerberosKeytab;
    private final String knoxToken;

    public MlflowClient(String serviceName, Map<String, String> configs) {
        super(serviceName, withEffectiveLoginUser(configs), "mlflow-client");

        this.baseUrls = parseBaseUrls(configs == null ? null : configs.get(RangerMlflowConstants.CONFIG_MLFLOW_URL));
        String configuredAuth = configs == null ? null : configs.get(RangerMlflowConstants.CONFIG_AUTH_TYPE);
        this.authType = StringUtils.isBlank(configuredAuth)
                ? RangerMlflowConstants.AUTH_TYPE_BASIC
                : configuredAuth.trim().toLowerCase(Locale.ROOT);
        this.userName = configs == null ? null : configs.get(RangerMlflowConstants.CONFIG_USERNAME);
        this.password = configs == null ? null : configs.get(RangerMlflowConstants.CONFIG_PASSWORD);
        this.kerberosPrincipal = configs == null ? null : configs.get(RangerMlflowConstants.CONFIG_KERBEROS_PRINCIPAL);
        this.kerberosKeytab = configs == null ? null : configs.get(RangerMlflowConstants.CONFIG_KERBEROS_KEYTAB);
        this.knoxToken = configs == null ? null : configs.get(RangerMlflowConstants.CONFIG_KNOX_TOKEN);
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        Map<String, Object> responseData = new HashMap<String, Object>();
        String missing = missingRequiredConfig(configs);
        if (missing != null) {
            String failureMsg = "MLflow service is missing required configuration '" + missing + "'.";
            BaseClient.generateResponseDataMap(false, failureMsg, failureMsg + ERR_TAIL, null, null, responseData);
            return responseData;
        }

        MlflowClient client = MlflowConnectionMgr.getMlflowClient(serviceName, configs);
        try {
            client.checkConnectivity();
            String successMsg = "ConnectionTest Successful";
            BaseClient.generateResponseDataMap(true, successMsg, successMsg, null, null, responseData);
        } catch (Throwable t) {
            LOG.error("MLflow connection test failed for service [{}]", serviceName, t);
            String failureMsg = "Unable to connect to MLflow server using the provided configuration.";
            String detail = BaseClient.getMessage(t);
            BaseClient.generateResponseDataMap(false,
                    StringUtils.isBlank(detail) ? failureMsg : detail,
                    failureMsg + ERR_TAIL,
                    null,
                    null,
                    responseData);
        }
        return responseData;
    }

    public static String missingRequiredConfig(Map<String, String> configs) {
        if (configs == null || parseBaseUrls(configs.get(RangerMlflowConstants.CONFIG_MLFLOW_URL)).isEmpty()) {
            return RangerMlflowConstants.CONFIG_MLFLOW_URL;
        }
        String mode = configs.get(RangerMlflowConstants.CONFIG_AUTH_TYPE);
        String normalizedMode = StringUtils.isBlank(mode)
                ? RangerMlflowConstants.AUTH_TYPE_BASIC
                : mode.trim().toLowerCase(Locale.ROOT);

        if (RangerMlflowConstants.AUTH_TYPE_KERBEROS.equals(normalizedMode)) {
            if (StringUtils.isBlank(configs.get(RangerMlflowConstants.CONFIG_KERBEROS_PRINCIPAL))) {
                return RangerMlflowConstants.CONFIG_KERBEROS_PRINCIPAL;
            }
            if (StringUtils.isBlank(configs.get(RangerMlflowConstants.CONFIG_KERBEROS_KEYTAB))) {
                return RangerMlflowConstants.CONFIG_KERBEROS_KEYTAB;
            }
            return null;
        }

        if (RangerMlflowConstants.AUTH_TYPE_KNOX.equals(normalizedMode)) {
            if (StringUtils.isNotBlank(configs.get(RangerMlflowConstants.CONFIG_KNOX_TOKEN))) {
                return null;
            }
        }

        if (StringUtils.isBlank(configs.get(RangerMlflowConstants.CONFIG_USERNAME))) {
            return RangerMlflowConstants.CONFIG_USERNAME;
        }
        if (StringUtils.isBlank(configs.get(RangerMlflowConstants.CONFIG_PASSWORD))) {
            return RangerMlflowConstants.CONFIG_PASSWORD;
        }
        return null;
    }

    public void checkConnectivity() {
        String body = "{\"max_results\": 1}";
        executeWithFailover("POST", RangerMlflowConstants.REST_PATH_EXPERIMENTS_SEARCH, body, null, false);
    }

    public List<String> lookupResources(String resourceName,
                                        String workspace,
                                        String matching,
                                        List<String> existing) {
        try {
            List<String> candidates;
            if (RangerMlflowConstants.RESOURCE_WORKSPACE.equals(resourceName)) {
                candidates = fetchWorkspaces();
            } else if (RangerMlflowConstants.RESOURCE_EXPERIMENT.equals(resourceName)) {
                candidates = fetchExperiments(workspace);
            } else if (RangerMlflowConstants.RESOURCE_REGISTERED_MODEL.equals(resourceName)) {
                candidates = fetchRegisteredModelsOrPrompts(workspace, false);
            } else if (RangerMlflowConstants.RESOURCE_PROMPT.equals(resourceName)) {
                candidates = fetchRegisteredModelsOrPrompts(workspace, true);
            } else if (RangerMlflowConstants.RESOURCE_SCORER.equals(resourceName)) {
                candidates = fetchScorers(workspace);
            } else if (RangerMlflowConstants.RESOURCE_GATEWAY_ENDPOINT.equals(resourceName)) {
                candidates = fetchJsonArrayField(
                        RangerMlflowConstants.REST_PATH_GATEWAY_ENDPOINTS, "endpoints", "name", workspace);
            } else if (RangerMlflowConstants.RESOURCE_GATEWAY_SECRET.equals(resourceName)) {
                candidates = fetchJsonArrayField(
                        RangerMlflowConstants.REST_PATH_GATEWAY_SECRETS, "secrets", "secret_name", workspace);
            } else if (RangerMlflowConstants.RESOURCE_GATEWAY_MODEL_DEFINITION.equals(resourceName)) {
                candidates = fetchJsonArrayField(
                        RangerMlflowConstants.REST_PATH_GATEWAY_MODEL_DEFINITIONS, "model_definitions", "name", workspace);
            } else if (RangerMlflowConstants.RESOURCE_MCP_SERVER.equals(resourceName)) {
                candidates = fetchJsonArrayField(
                        RangerMlflowConstants.REST_PATH_MCP_SERVERS + "?max_results=" + RangerMlflowConstants.PAGE_LIMIT,
                        "mcp_servers", "name", workspace);
            } else {
                candidates = Collections.emptyList();
            }
            return filterMatches(candidates, matching, existing, false);
        } catch (HadoopException he) {
            throw he;
        } catch (Throwable t) {
            String msg = "Unable to retrieve MLflow " + resourceName + " resources.";
            LOG.error(msg, t);
            HadoopException hdpException = new HadoopException(msg, t);
            hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msg + ERR_TAIL, null, null);
            throw hdpException;
        }
    }

    List<String> fetchWorkspaces() throws IOException {
        HttpResult res = executeWithFailover("GET", RangerMlflowConstants.REST_PATH_WORKSPACES, null, null, true);
        if (res.statusCode == 404 || StringUtils.isBlank(res.body)) {
            return Collections.singletonList(RangerMlflowConstants.DEFAULT_WORKSPACE);
        }
        List<String> names = extractFieldFromArray(res.body, "workspaces", "name");
        if (names.isEmpty()) {
            return Collections.singletonList(RangerMlflowConstants.DEFAULT_WORKSPACE);
        }
        return names;
    }

    List<String> fetchExperiments(String workspace) throws IOException {
        String payload = "{\"max_results\": " + RangerMlflowConstants.PAGE_LIMIT + "}";
        HttpResult res = executeWithFailover(
                "POST", RangerMlflowConstants.REST_PATH_EXPERIMENTS_SEARCH, payload, workspace, false);
        return extractFieldFromArray(res.body, "experiments", "name");
    }

    Map<String, String> fetchExperimentIdToNameMap(String workspace) throws IOException {
        String payload = "{\"max_results\": " + RangerMlflowConstants.PAGE_LIMIT + "}";
        HttpResult res = executeWithFailover(
                "POST", RangerMlflowConstants.REST_PATH_EXPERIMENTS_SEARCH, payload, workspace, false);
        Map<String, String> idToName = new HashMap<String, String>();
        if (StringUtils.isBlank(res.body)) {
            return idToName;
        }
        JsonNode root = MAPPER.readTree(res.body);
        JsonNode arr = root.get("experiments");
        if (arr != null && arr.isArray()) {
            for (JsonNode exp : arr) {
                JsonNode idNode = exp.get("experiment_id");
                JsonNode nameNode = exp.get("name");
                if (idNode != null && nameNode != null
                        && StringUtils.isNotBlank(idNode.asText())
                        && StringUtils.isNotBlank(nameNode.asText())) {
                    idToName.put(idNode.asText(), nameNode.asText());
                }
            }
        }
        return idToName;
    }

    List<String> fetchRegisteredModelsOrPrompts(String workspace, boolean promptsOnly) throws IOException {
        String path = RangerMlflowConstants.REST_PATH_REGISTERED_MODELS_SEARCH
                + "?max_results=" + RangerMlflowConstants.PAGE_LIMIT;
        HttpResult res = executeWithFailover("GET", path, null, workspace, false);
        if (StringUtils.isBlank(res.body)) {
            return Collections.emptyList();
        }
        JsonNode root = MAPPER.readTree(res.body);
        JsonNode models = root.get("registered_models");
        if (models == null || !models.isArray()) {
            return Collections.emptyList();
        }
        Set<String> results = new LinkedHashSet<String>();
        for (JsonNode rm : models) {
            JsonNode nameNode = rm.get("name");
            if (nameNode == null || StringUtils.isBlank(nameNode.asText())) {
                continue;
            }
            boolean isPrompt = isPromptEntity(rm);
            if (promptsOnly == isPrompt) {
                results.add(nameNode.asText());
            }
        }
        return new ArrayList<String>(results);
    }

    List<String> fetchScorers(String workspace) throws IOException {
        HttpResult res = executeWithFailover("GET", RangerMlflowConstants.REST_PATH_SCORERS_LIST, null, workspace, true);
        if (res.statusCode == 404 || StringUtils.isBlank(res.body)) {
            return Collections.emptyList();
        }
        Map<String, String> expIdToName = fetchExperimentIdToNameMap(workspace);
        JsonNode root = MAPPER.readTree(res.body);
        JsonNode scorers = root.get("scorers");
        if (scorers == null || !scorers.isArray()) {
            return Collections.emptyList();
        }
        Set<String> results = new LinkedHashSet<String>();
        for (JsonNode scorer : scorers) {
            JsonNode expIdNode = scorer.get("experiment_id");
            JsonNode scorerNameNode = scorer.has("scorer_name") ? scorer.get("scorer_name") : scorer.get("name");
            if (scorerNameNode == null || StringUtils.isBlank(scorerNameNode.asText())) {
                continue;
            }
            String expKey = expIdNode == null ? "" : expIdNode.asText();
            String expName = expIdToName.containsKey(expKey) ? expIdToName.get(expKey) : expKey;
            if (StringUtils.isNotBlank(expName)) {
                results.add(expName + "/" + scorerNameNode.asText());
            } else {
                results.add(scorerNameNode.asText());
            }
        }
        return new ArrayList<String>(results);
    }

    List<String> fetchJsonArrayField(String path,
                                     String arrayField,
                                     String nameField,
                                     String workspace) throws IOException {
        HttpResult res = executeWithFailover("GET", path, null, workspace, true);
        if (res.statusCode == 404 || StringUtils.isBlank(res.body)) {
            return Collections.emptyList();
        }
        return extractFieldFromArray(res.body, arrayField, nameField);
    }

    static List<String> extractFieldFromArray(String json, String arrayField, String itemField) throws IOException {
        if (StringUtils.isBlank(json)) {
            return Collections.emptyList();
        }
        JsonNode root = MAPPER.readTree(json);
        JsonNode arr = root.get(arrayField);
        if (arr == null || !arr.isArray()) {
            return Collections.emptyList();
        }
        Set<String> values = new LinkedHashSet<String>();
        for (JsonNode item : arr) {
            JsonNode field = item.get(itemField);
            if (field != null && field.isTextual() && StringUtils.isNotBlank(field.asText())) {
                values.add(field.asText());
            }
        }
        return new ArrayList<String>(values);
    }

    static boolean isPromptEntity(JsonNode registeredModelNode) {
        JsonNode tags = registeredModelNode.get("tags");
        if (tags == null || !tags.isArray()) {
            return false;
        }
        for (JsonNode tag : tags) {
            JsonNode key = tag.get("key");
            JsonNode val = tag.get("value");
            if (key != null
                    && RangerMlflowConstants.PROMPT_TAG_KEY.equals(key.asText())
                    && val != null
                    && "true".equalsIgnoreCase(val.asText())) {
                return true;
            }
        }
        return false;
    }

    public static List<String> filterMatches(List<String> candidates,
                                             String matching,
                                             List<String> existing,
                                             boolean ignoreCase) {
        if (candidates == null || candidates.isEmpty()) {
            return Collections.emptyList();
        }
        String needle = matching == null ? "" : matching.trim();
        if ("*".equals(needle)) {
            needle = "";
        }
        String loweredNeedle = needle.toLowerCase(Locale.ROOT);
        List<String> out = new ArrayList<String>();
        for (String item : candidates) {
            if (StringUtils.isBlank(item)) {
                continue;
            }
            if (existing != null && existing.contains(item)) {
                continue;
            }
            if (needle.isEmpty()) {
                out.add(item);
            } else if (ignoreCase) {
                if (item.toLowerCase(Locale.ROOT).contains(loweredNeedle)) {
                    out.add(item);
                }
            } else if (item.contains(needle) || item.toLowerCase(Locale.ROOT).startsWith(loweredNeedle)) {
                out.add(item);
            }
        }
        return out;
    }

    static List<String> parseBaseUrls(String rawUrls) {
        if (StringUtils.isBlank(rawUrls)) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<String>();
        for (String part : rawUrls.split(",")) {
            String trimmed = part == null ? "" : part.trim().replaceAll("/+$", "");
            if (StringUtils.isNotBlank(trimmed)) {
                result.add(trimmed);
            }
        }
        return result;
    }

    private static Map<String, String> withEffectiveLoginUser(Map<String, String> configs) {
        Map<String, String> copy = new HashMap<String, String>();
        if (configs != null) {
            copy.putAll(configs);
        }
        if (StringUtils.isBlank(copy.get(RangerMlflowConstants.CONFIG_USERNAME))) {
            String principal = copy.get(RangerMlflowConstants.CONFIG_KERBEROS_PRINCIPAL);
            copy.put(RangerMlflowConstants.CONFIG_USERNAME,
                    StringUtils.isNotBlank(principal) ? principal.trim() : "mlflow-lookup");
        }
        return copy;
    }

    protected HttpResult executeWithFailover(String method,
                                             String path,
                                             String jsonBody,
                                             String workspace,
                                             boolean allow404) {
        if (baseUrls.isEmpty()) {
            throw new IllegalStateException("No MLflow URL configured in '"
                    + RangerMlflowConstants.CONFIG_MLFLOW_URL + "'");
        }
        RuntimeException lastError = null;
        for (String baseUrl : baseUrls) {
            String fullUrl = baseUrl + path;
            try {
                HttpResult result = sendRequest(method, fullUrl, jsonBody, workspace);
                if (result.statusCode >= 200 && result.statusCode < 300) {
                    return result;
                }
                if (allow404 && result.statusCode == 404) {
                    return result;
                }
                throw new IllegalStateException(
                        "HTTP " + result.statusCode + " from MLflow endpoint [" + fullUrl + "]");
            } catch (Exception e) {
                LOG.warn("MLflow request [{} {}] failed: {}", method, fullUrl, e.getMessage());
                lastError = (e instanceof RuntimeException)
                        ? (RuntimeException) e
                        : new RuntimeException(e.getMessage(), e);
            }
        }
        throw lastError != null ? lastError : new RuntimeException("All MLflow URLs failed");
    }

    protected HttpResult sendRequest(String method,
                                     String fullUrl,
                                     String jsonBody,
                                     String workspace) throws Exception {
        URL urlObj = new URL(fullUrl);
        HttpURLConnection conn = openConnection(urlObj);
        try {
            conn.setRequestMethod(method);
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Authorization", buildAuthorizationHeader(urlObj.toURI()));
            if (StringUtils.isNotBlank(workspace)) {
                conn.setRequestProperty(RangerMlflowConstants.HEADER_WORKSPACE, workspace);
            }
            if (jsonBody != null) {
                byte[] payloadBytes = jsonBody.getBytes(StandardCharsets.UTF_8);
                conn.setDoOutput(true);
                conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(payloadBytes);
                }
            }
            int status = conn.getResponseCode();
            InputStream stream = (status >= 200 && status < 400) ? conn.getInputStream() : conn.getErrorStream();
            String responseBody = readAll(stream);
            return new HttpResult(status, responseBody);
        } finally {
            conn.disconnect();
        }
    }

    protected HttpURLConnection openConnection(URL url) throws IOException {
        return (HttpURLConnection) url.openConnection();
    }

    String buildAuthorizationHeader(final URI targetUri) throws Exception {
        if (RangerMlflowConstants.AUTH_TYPE_KERBEROS.equals(authType)) {
            return "Negotiate " + generateSpnegoToken(targetUri);
        }
        if (RangerMlflowConstants.AUTH_TYPE_KNOX.equals(authType) && StringUtils.isNotBlank(knoxToken)) {
            return "Bearer " + decryptSecret(knoxToken).trim();
        }
        String clearPassword = decryptSecret(password);
        String raw = StringUtils.defaultString(userName) + ":" + StringUtils.defaultString(clearPassword);
        return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    protected String generateSpnegoToken(final URI targetUri) throws Exception {
        final String host = targetUri.getHost();
        if (StringUtils.isBlank(host)) {
            throw new IllegalArgumentException("Cannot determine target hostname for SPNEGO from URI: " + targetUri);
        }
        Subject subject = SecureClientLogin.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);
        return Subject.doAs(subject, new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {
                GSSManager manager = GSSManager.getInstance();
                Oid spnegoOid = new Oid(SPNEGO_OID);
                GSSName serverName = manager.createName(
                        "HTTP@" + host, GSSName.NT_HOSTBASED_SERVICE, spnegoOid);
                GSSContext gssContext = manager.createContext(
                        serverName, spnegoOid, null, GSSContext.DEFAULT_LIFETIME);
                try {
                    gssContext.requestMutualAuth(false);
                    byte[] token = gssContext.initSecContext(new byte[0], 0, 0);
                    if (token == null) {
                        throw new IllegalStateException("GSSContext returned empty SPNEGO token for HTTP@" + host);
                    }
                    return Base64.getEncoder().encodeToString(token);
                } finally {
                    gssContext.dispose();
                }
            }
        });
    }

    static String decryptSecret(String secret) {
        if (StringUtils.isBlank(secret)) {
            return "";
        }
        try {
            String decrypted = PasswordUtils.decryptPassword(secret);
            return decrypted != null ? decrypted : secret;
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Secret is not encrypted with Ranger master key; using raw value");
            }
            return secret;
        }
    }

    private static String readAll(InputStream stream) throws IOException {
        if (stream == null) {
            return "";
        }
        try (InputStream in = stream; ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) != -1) {
                bos.write(buf, 0, n);
            }
            return new String(bos.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    public static final class HttpResult {
        public final int statusCode;
        public final String body;

        public HttpResult(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body;
        }
    }
}
