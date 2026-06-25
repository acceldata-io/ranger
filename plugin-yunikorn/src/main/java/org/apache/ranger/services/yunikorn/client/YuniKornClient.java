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

package org.apache.ranger.services.yunikorn.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.services.yunikorn.RangerYuniKornConstants;
import org.apache.ranger.services.yunikorn.client.json.model.YuniKornQueueResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * Client for the YuniKorn read-only REST API.
 *
 * <p>Used by Ranger Admin to populate queue dropdowns when an administrator
 * authors a policy against the {@code yunikorn} service type. Performs a
 * single GET to {@code /ws/v1/partition/{partition}/queues}, parses the
 * resulting queue tree, and returns a flat list of fully-qualified queue
 * paths (e.g. {@code root.research.nlp}).
 *
 * <p>YuniKorn's REST API has no authentication upstream, so this client
 * sends plain HTTP requests with no credentials. If a customer fronts
 * YuniKorn with an Ingress that requires auth, that's handled at the
 * network layer, not here.
 */
public class YuniKornClient extends BaseClient {

    private static final Logger LOG = LoggerFactory.getLogger(YuniKornClient.class);

    private static final String EXPECTED_MIME_TYPE = "application/json";

    // This lookup runs on a Ranger Admin thread (queue autocomplete / Test
    // Connection). Without timeouts, a hung YuniKorn endpoint would block that
    // thread indefinitely and could starve Ranger Admin. Keep them short since
    // this only powers UI autocomplete.
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS    = 10_000;

    private static final String ERR_TAIL =
            " You can still save the repository and start creating policies, but you "
            + "would not be able to use autocomplete for queue names. "
            + "Check ranger_admin.log for more info.";

    private final String url;
    private final String partition;

    public YuniKornClient(String serviceName, Map<String, String> configs) {
        super(serviceName, configs, "yunikorn-client");

        String configuredUrl       = configs == null ? null : configs.get(RangerYuniKornConstants.CONFIG_YUNIKORN_URL);
        String configuredPartition = configs == null ? null : configs.get(RangerYuniKornConstants.CONFIG_YUNIKORN_PARTITION);

        this.url       = configuredUrl;
        this.partition = (configuredPartition == null || configuredPartition.trim().isEmpty())
                ? RangerYuniKornConstants.DEFAULT_PARTITION
                : configuredPartition.trim();

        if (this.url == null || this.url.trim().isEmpty()) {
            LOG.error("No value found for configuration '{}'. YuniKorn resource lookup will fail.",
                    RangerYuniKornConstants.CONFIG_YUNIKORN_URL);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("YuniKornClient built with url=[{}], partition=[{}]", this.url, this.partition);
        }
    }

    /**
     * Returns the list of queue paths matching the given prefix, excluding any
     * paths already present in {@code existingQueueList}.
     */
    public List<String> getQueueList(final String queueNameMatching,
                                     final List<String> existingQueueList) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting YuniKorn queue list for queueNameMatching=[{}]", queueNameMatching);
        }

        try {
            return fetchQueuePaths(queueNameMatching, existingQueueList);
        } catch (HadoopException he) {
            throw he;
        } catch (Throwable t) {
            String msg = "Unable to retrieve YuniKorn queue list from [" + url + "]";
            LOG.error(msg, t);
            HadoopException hdpException = new HadoopException(msg, t);
            hdpException.generateResponseDataMap(false,
                    BaseClient.getMessage(t), msg + ERR_TAIL, null, null);
            throw hdpException;
        }
    }

    private List<String> fetchQueuePaths(String queueNameMatching, List<String> existingQueueList) {
        if (url == null || url.trim().isEmpty()) {
            return Collections.emptyList();
        }

        String endpoint = url.trim().replaceAll("/+$", "")
                + String.format(RangerYuniKornConstants.REST_PATH_QUEUES_FMT, partition);

        Client       client   = Client.create();
        client.setConnectTimeout(CONNECT_TIMEOUT_MS);
        client.setReadTimeout(READ_TIMEOUT_MS);
        ClientResponse response = null;

        try {
            WebResource resource = client.resource(endpoint);
            response = resource.accept(EXPECTED_MIME_TYPE).get(ClientResponse.class);

            if (LOG.isDebugEnabled()) {
                LOG.debug("GET {} -> status {}", endpoint, response == null ? "null" : response.getStatus());
            }

            if (response == null || response.getStatus() != 200) {
                int status = response == null ? -1 : response.getStatus();
                String body = response == null ? "" : safeReadEntity(response);
                String msg = "Unexpected response from YuniKorn URL [" + endpoint + "]: status=" + status;
                LOG.error("{} body=[{}]", msg, body);
                HadoopException hdpException = new HadoopException(msg);
                hdpException.generateResponseDataMap(false, msg, msg + ERR_TAIL, null, null);
                throw hdpException;
            }

            String json = response.getEntity(String.class);
            ObjectMapper mapper = new ObjectMapper();
            YuniKornQueueResponse root = mapper.readValue(json, YuniKornQueueResponse.class);

            List<String> allPaths = new ArrayList<>();
            if (root != null) {
                root.collectQueuePaths(allPaths);
            }

            List<String> result = new ArrayList<>(allPaths.size());
            for (String path : allPaths) {
                if (existingQueueList != null && existingQueueList.contains(path)) {
                    continue;
                }
                if (queueNameMatching == null
                        || queueNameMatching.isEmpty()
                        || path.startsWith(queueNameMatching)) {
                    result.add(path);
                }
            }
            return result;

        } catch (HadoopException he) {
            throw he;
        } catch (Throwable t) {
            String msg = "Exception while fetching YuniKorn queue list from [" + endpoint + "]";
            LOG.error(msg, t);
            HadoopException hdpException = new HadoopException(msg, t);
            hdpException.generateResponseDataMap(false,
                    BaseClient.getMessage(t), msg + ERR_TAIL, null, null);
            throw hdpException;
        } finally {
            if (response != null) {
                response.close();
            }
            client.destroy();
        }
    }

    private static String safeReadEntity(ClientResponse response) {
        try {
            return response.getEntity(String.class);
        } catch (Throwable t) {
            return "";
        }
    }

    /**
     * Connection-test entry called from {@code RangerServiceYuniKorn.validateConfig()}.
     * Treats a successful queue fetch (any non-empty result, or even an empty
     * one provided no exception was thrown) as connectivity proof.
     */
    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        Map<String, Object> responseData = new HashMap<>();
        boolean connectivityStatus = false;
        Throwable failure = null;

        YuniKornClient client = YuniKornConnectionMgr.getYuniKornClient(serviceName, configs);
        try {
            List<String> queues = client.getQueueList("", null);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Connection test retrieved {} YuniKorn queue(s)", queues == null ? 0 : queues.size());
            }
            connectivityStatus = true;
        } catch (Throwable t) {
            failure = t;
            LOG.error("YuniKorn connection test failed for service [{}]", serviceName, t);
        }

        if (connectivityStatus) {
            String successMsg = "ConnectionTest Successful";
            BaseClient.generateResponseDataMap(true, successMsg, successMsg, null, null, responseData);
        } else {
            String failureMsg = "Unable to retrieve YuniKorn queues using the given parameters.";
            // Surface the real cause (walks the exception's cause chain) so the
            // admin sees e.g. "Connection refused" rather than only generic text.
            String message = BaseClient.getMessage(failure);
            BaseClient.generateResponseDataMap(false,
                    (message == null || message.isEmpty()) ? failureMsg : message,
                    failureMsg + ERR_TAIL, null, null, responseData);
        }

        return responseData;
    }
}
