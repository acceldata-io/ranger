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

package org.apache.ranger.services.airflow.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.services.airflow.RangerAirflowConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * HTTP client used by Ranger Admin to populate Airflow policy dropdowns.
 *
 * <p>Authenticates against Airflow 3 with {@code POST /auth/token}, then lists
 * resources from {@code /api/v2/*}. Same shape as {@code YuniKornClient}:
 * Jersey GET, short timeouts, prefix-filter in process, exclude values already
 * selected in the policy form.
 */
public class AirflowClient extends BaseClient {

    private static final Logger LOG = LoggerFactory.getLogger(AirflowClient.class);

    private static final String EXPECTED_MIME_TYPE = "application/json";

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS    = 10_000;

    private static final String ERR_TAIL =
            " You can still save the repository and start creating policies, but you "
            + "would not be able to use autocomplete for resource names. "
            + "Check ranger_admin.log for more info.";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String url;
    private final String userName;
    private final String password;

    public AirflowClient(String serviceName, Map<String, String> configs) {
        super(serviceName, configs, "airflow-client");

        this.url      = configs == null ? null : configs.get(RangerAirflowConstants.CONFIG_AIRFLOW_URL);
        this.userName = configs == null ? null : configs.get(RangerAirflowConstants.CONFIG_USERNAME);
        this.password = configs == null ? null : configs.get(RangerAirflowConstants.CONFIG_PASSWORD);

        if (StringUtils.isBlank(this.url)) {
            LOG.error("No value found for configuration '{}'. Airflow resource lookup will fail.",
                    RangerAirflowConstants.CONFIG_AIRFLOW_URL);
        }
        if (StringUtils.isBlank(this.userName)) {
            LOG.error("No value found for configuration '{}'. Airflow resource lookup will fail.",
                    RangerAirflowConstants.CONFIG_USERNAME);
        }
        if (StringUtils.isBlank(this.password)) {
            LOG.error("No value found for configuration '{}'. Airflow resource lookup will fail.",
                    RangerAirflowConstants.CONFIG_PASSWORD);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("AirflowClient built with url=[{}], user=[{}]", this.url, this.userName);
        }
    }

    public List<String> getDagList(String matching, List<String> existing) {
        return listAndFilter(RangerAirflowConstants.REST_PATH_DAGS,
                RangerAirflowConstants.JSON_DAGS,
                RangerAirflowConstants.JSON_DAG_ID,
                matching, existing, false);
    }

    public List<String> getConnectionList(String matching, List<String> existing) {
        return listAndFilter(RangerAirflowConstants.REST_PATH_CONNECTIONS,
                RangerAirflowConstants.JSON_CONNECTIONS,
                RangerAirflowConstants.JSON_CONNECTION_ID,
                matching, existing, false);
    }

    public List<String> getVariableList(String matching, List<String> existing) {
        return listAndFilter(RangerAirflowConstants.REST_PATH_VARIABLES,
                RangerAirflowConstants.JSON_VARIABLES,
                RangerAirflowConstants.JSON_VARIABLE_KEY,
                matching, existing, false);
    }

    public List<String> getPoolList(String matching, List<String> existing) {
        return listAndFilter(RangerAirflowConstants.REST_PATH_POOLS,
                RangerAirflowConstants.JSON_POOLS,
                RangerAirflowConstants.JSON_POOL_NAME,
                matching, existing, false);
    }

    /**
     * Prefix-match helper used by HTTP lookups and the static view list.
     * {@code ignoreCase} follows the service-def matcher for that resource.
     */
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

    /**
     * Policy fields accept {@code *} / {@code etl_*}. Treat those as "all" or a
     * prefix; a literal startsWith on the wildcard string matches nothing.
     */
    static String normalizeUserInput(String userInput) {
        if (StringUtils.isBlank(userInput)) {
            return "";
        }
        String trimmed = userInput.trim();
        if ("*".equals(trimmed) || "%".equals(trimmed)) {
            return "";
        }
        int wildcard = firstWildcard(trimmed);
        if (wildcard < 0) {
            return trimmed;
        }
        return trimmed.substring(0, wildcard);
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

    private static int firstWildcard(String value) {
        int star = value.indexOf('*');
        int question = value.indexOf('?');
        if (star < 0) {
            return question;
        }
        if (question < 0) {
            return star;
        }
        return Math.min(star, question);
    }

    static List<String> collectIds(String json, String arrayField, String idField) throws Exception {
        return collectPage(json, arrayField, idField).ids;
    }

    /**
     * Parsed ids plus the raw array length. Pagination must use {@code rawCount}:
     * a full page with a blank id would otherwise look short and drop later pages.
     */
    static CollectionPage collectPage(String json, String arrayField, String idField) throws Exception {
        JsonNode root = MAPPER.readTree(json);
        JsonNode array = root == null ? null : root.get(arrayField);
        List<String> ids = new ArrayList<String>();
        if (array == null || !array.isArray()) {
            return new CollectionPage(ids, 0);
        }
        for (JsonNode item : array) {
            String id = text(item, idField);
            if (id == null && RangerAirflowConstants.JSON_POOL_NAME.equals(idField)) {
                id = text(item, RangerAirflowConstants.JSON_POOL_ID);
            }
            if (StringUtils.isNotBlank(id)) {
                ids.add(id);
            }
        }
        return new CollectionPage(ids, array.size());
    }

    static final class CollectionPage {
        final List<String> ids;
        final int rawCount;

        CollectionPage(List<String> ids, int rawCount) {
            this.ids = ids;
            this.rawCount = rawCount;
        }
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        Map<String, Object> responseData = new HashMap<String, Object>();
        String missing = missingRequiredConfig(configs);
        if (missing != null) {
            String failureMsg = "Airflow service is missing '" + missing + "'.";
            BaseClient.generateResponseDataMap(false, failureMsg, failureMsg + ERR_TAIL, null, null, responseData);
            return responseData;
        }

        boolean connectivityStatus = false;
        Throwable failure = null;

        AirflowClient client = AirflowConnectionMgr.getAirflowClient(serviceName, configs);
        try {
            List<String> dags = client.getDagList("", null);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Connection test retrieved {} Airflow DAG(s)", dags == null ? 0 : dags.size());
            }
            connectivityStatus = true;
        } catch (Throwable t) {
            failure = t;
            LOG.error("Airflow connection test failed for service [{}]", serviceName, t);
        }

        if (connectivityStatus) {
            String successMsg = "ConnectionTest Successful";
            BaseClient.generateResponseDataMap(true, successMsg, successMsg, null, null, responseData);
        } else {
            String failureMsg = "Unable to list Airflow DAGs using the given parameters.";
            String message = BaseClient.getMessage(failure);
            BaseClient.generateResponseDataMap(false,
                    (message == null || message.isEmpty()) ? failureMsg : message,
                    failureMsg + ERR_TAIL, null, null, responseData);
        }

        return responseData;
    }

    static String missingRequiredConfig(Map<String, String> configs) {
        if (configs == null || StringUtils.isBlank(configs.get(RangerAirflowConstants.CONFIG_AIRFLOW_URL))) {
            return RangerAirflowConstants.CONFIG_AIRFLOW_URL;
        }
        if (StringUtils.isBlank(configs.get(RangerAirflowConstants.CONFIG_USERNAME))) {
            return RangerAirflowConstants.CONFIG_USERNAME;
        }
        if (StringUtils.isBlank(configs.get(RangerAirflowConstants.CONFIG_PASSWORD))) {
            return RangerAirflowConstants.CONFIG_PASSWORD;
        }
        return null;
    }

    private List<String> listAndFilter(String path,
                                       String arrayField,
                                       String idField,
                                       String matching,
                                       List<String> existing,
                                       boolean ignoreCase) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting Airflow {} list matching=[{}]", path, matching);
        }

        try {
            List<String> all = fetchCollection(path, arrayField, idField);
            return filterMatches(all, matching, existing, ignoreCase);
        } catch (HadoopException he) {
            throw he;
        } catch (Throwable t) {
            String msg = "Unable to retrieve Airflow resources from [" + url + path + "]";
            LOG.error(msg, t);
            HadoopException hdpException = new HadoopException(msg, t);
            hdpException.generateResponseDataMap(false,
                    BaseClient.getMessage(t), msg + ERR_TAIL, null, null);
            throw hdpException;
        }
    }

    private List<String> fetchCollection(String path, String arrayField, String idField) {
        if (StringUtils.isBlank(url)) {
            return Collections.emptyList();
        }

        Client client = Client.create();
        client.setConnectTimeout(CONNECT_TIMEOUT_MS);
        client.setReadTimeout(READ_TIMEOUT_MS);

        try {
            String token = fetchAccessToken(client);
            String base = url.trim().replaceAll("/+$", "");
            List<String> all = new ArrayList<String>();

            for (int page = 0; page < RangerAirflowConstants.MAX_PAGES; page++) {
                int offset = page * RangerAirflowConstants.PAGE_LIMIT;
                String endpoint = base + path;
                WebResource resource = client.resource(endpoint)
                        .queryParam("limit", String.valueOf(RangerAirflowConstants.PAGE_LIMIT))
                        .queryParam("offset", String.valueOf(offset));
                if (RangerAirflowConstants.REST_PATH_DAGS.equals(path)) {
                    resource = resource.queryParam("exclude_stale", "false");
                }

                ClientResponse response = null;
                try {
                    response = resource
                            .header("Authorization", "Bearer " + token)
                            .accept(EXPECTED_MIME_TYPE)
                            .get(ClientResponse.class);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("GET {} offset={} -> status {}",
                                endpoint, offset, response == null ? "null" : response.getStatus());
                    }

                    if (response == null || response.getStatus() != 200) {
                        int status = response == null ? -1 : response.getStatus();
                        String body = response == null ? "" : safeReadEntity(response);
                        String msg = "Unexpected response from Airflow URL [" + endpoint + "]: status=" + status;
                        LOG.error("{} body=[{}]", msg, body);
                        HadoopException hdpException = new HadoopException(msg);
                        hdpException.generateResponseDataMap(false, msg, msg + ERR_TAIL, null, null);
                        throw hdpException;
                    }

                    CollectionPage pageIds = collectPage(response.getEntity(String.class), arrayField, idField);
                    all.addAll(pageIds.ids);
                    if (pageIds.rawCount < RangerAirflowConstants.PAGE_LIMIT) {
                        break;
                    }
                    if (page == RangerAirflowConstants.MAX_PAGES - 1) {
                        LOG.warn("Airflow lookup for {} hit the {}-page cap ({} names); later entries will not autocomplete",
                                path,
                                RangerAirflowConstants.MAX_PAGES,
                                RangerAirflowConstants.MAX_PAGES * RangerAirflowConstants.PAGE_LIMIT);
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
            String msg = "Exception while fetching Airflow resources from [" + url + path + "]";
            LOG.error(msg, t);
            HadoopException hdpException = new HadoopException(msg, t);
            hdpException.generateResponseDataMap(false,
                    BaseClient.getMessage(t), msg + ERR_TAIL, null, null);
            throw hdpException;
        } finally {
            client.destroy();
        }
    }

    private String fetchAccessToken(Client client) {
        if (StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
            String msg = "Airflow username/password are required for resource lookup.";
            HadoopException hdpException = new HadoopException(msg);
            hdpException.generateResponseDataMap(false, msg, msg + ERR_TAIL, null, null);
            throw hdpException;
        }

        String endpoint = url.trim().replaceAll("/+$", "") + RangerAirflowConstants.REST_PATH_TOKEN;
        ClientResponse response = null;
        try {
            Map<String, String> payload = new HashMap<String, String>();
            payload.put("username", userName);
            payload.put("password", decryptedPassword());
            String body = MAPPER.writeValueAsString(payload);

            WebResource resource = client.resource(endpoint);
            response = resource
                    .type(EXPECTED_MIME_TYPE)
                    .accept(EXPECTED_MIME_TYPE)
                    .post(ClientResponse.class, body);

            int status = response == null ? -1 : response.getStatus();
            if (response == null || (status != 200 && status != 201)) {
                String respBody = response == null ? "" : safeReadEntity(response);
                String msg = "Airflow token request failed at [" + endpoint + "]: status=" + status;
                LOG.error("{} body=[{}]", msg, respBody);
                HadoopException hdpException = new HadoopException(msg);
                hdpException.generateResponseDataMap(false, msg, msg + ERR_TAIL, null, null);
                throw hdpException;
            }

            JsonNode root = MAPPER.readTree(response.getEntity(String.class));
            String token = root == null ? null : text(root, RangerAirflowConstants.JSON_ACCESS_TOKEN);
            if (StringUtils.isBlank(token)) {
                String msg = "Airflow token response from [" + endpoint + "] did not contain access_token.";
                HadoopException hdpException = new HadoopException(msg);
                hdpException.generateResponseDataMap(false, msg, msg + ERR_TAIL, null, null);
                throw hdpException;
            }
            return token;
        } catch (HadoopException he) {
            throw he;
        } catch (Throwable t) {
            String msg = "Exception while requesting an Airflow access token from [" + endpoint + "]";
            LOG.error(msg, t);
            HadoopException hdpException = new HadoopException(msg, t);
            hdpException.generateResponseDataMap(false,
                    BaseClient.getMessage(t), msg + ERR_TAIL, null, null);
            throw hdpException;
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private String decryptedPassword() {
        try {
            String decrypted = PasswordUtils.getDecryptPassword(password);
            return decrypted == null ? password : decrypted;
        } catch (Throwable t) {
            LOG.debug("Password decryption failed; using the configured string as-is");
            return password;
        }
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

    private static String safeReadEntity(ClientResponse response) {
        try {
            return response.getEntity(String.class);
        } catch (Throwable t) {
            return "";
        }
    }
}
