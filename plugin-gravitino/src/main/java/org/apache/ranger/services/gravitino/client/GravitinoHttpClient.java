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
package org.apache.ranger.services.gravitino.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.services.gravitino.client.auth.GravitinoAuth;
import org.apache.ranger.services.gravitino.client.auth.GravitinoAuthFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

/**
 * GravitinoHttpClient - HTTP client implementation for Xstore API.
 * 
 * This client communicates with the Xstore REST API to lookup resources
 * for policy configuration in Ranger Admin UI.
 */
public class GravitinoHttpClient extends BaseClient implements GravitinoClient {
    private static final Logger LOG = LoggerFactory.getLogger(GravitinoHttpClient.class);
    
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int CONNECT_TIMEOUT_MS = 10000;
    private static final int READ_TIMEOUT_MS = 30000;
    private static final String KEY_XSTORE_URL = "xstore.url";
    // TODO: remove gravitino.url fallback once legacy configs are dropped.
    private static final String KEY_GRAVITINO_URL = "gravitino.url";

    private final GravitinoAuth auth;
    
    public GravitinoHttpClient(String serviceName, Map<String, String> connectionConfig) {
        super(serviceName, connectionConfig);
        this.auth = GravitinoAuthFactory.create(serviceName, connectionConfig);
    }
    
    /**
     * Test connection to Xstore server.
     */
    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        Map<String, Object> resp = new HashMap<>();

        try {
            GravitinoHttpClient client = new GravitinoHttpClient(serviceName, configs);
            Properties p = client.getConfigHolder().getRangerSection();
            String baseUrl = resolveBaseUrl(p);
            
            if (baseUrl == null || baseUrl.isEmpty()) {
                BaseClient.generateResponseDataMap(
                        false,
                        "Missing xstore.url",
                        "Missing xstore.url (or gravitino.url)",
                        null,
                        KEY_XSTORE_URL,
                        resp);
                return resp;
            }

            URL url = new URL(baseUrl + "/api/metalakes");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            conn.setRequestProperty("Content-Type", "application/json");
            GravitinoAuthFactory.create(serviceName, configs).apply(conn);

            int code = conn.getResponseCode();
            if (code >= 200 && code < 300) {
                BaseClient.generateResponseDataMap(true, "Connection test successful",
                        "Connection test successful", null, null, resp);
            } else {
                BaseClient.generateResponseDataMap(false, "Connection test failed (HTTP " + code + ")",
                        "Connection test failed (HTTP " + code + ")", null, null, resp);
            }

            return resp;
        } catch (Exception e) {
            LOG.error("Connection test failed", e);
            HadoopException hE = new HadoopException(e.getMessage());
            hE.setStackTrace(e.getStackTrace());
            hE.generateResponseDataMap(false, BaseClient.getMessage(e),
                    "Unable to connect to Xstore", null, null);
            throw hE;
        }
    }
    
    @Override
    public List<String> listMetalakes(String prefix) throws Exception {
        Properties p = getConfigHolder().getRangerSection();
        String baseUrl = resolveBaseUrl(p);

        URL url = new URL(baseUrl + "/api/metalakes");
        return executeAndParseIdentifiers(url, "identifiers", prefix);
    }

    @Override
    public List<String> listCatalogs(String metalake, String prefix) throws Exception {
        if (metalake == null || metalake.isEmpty()) {
            return Collections.emptyList();
        }
        Properties p = getConfigHolder().getRangerSection();
        String baseUrl = resolveBaseUrl(p);

        URL url = new URL(baseUrl + "/api/metalakes/" + metalake + "/catalogs");
        return executeAndParseIdentifiers(url, "identifiers", prefix);
    }
    
    @Override
    public List<String> listSchemas(String metalake, String catalog, String prefix) throws Exception {
        if (metalake == null || metalake.isEmpty() || catalog == null || catalog.isEmpty()) {
            return Collections.emptyList();
        }
        Properties p = getConfigHolder().getRangerSection();
        String baseUrl = resolveBaseUrl(p);

        URL url = new URL(baseUrl + "/api/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas");
        return executeAndParseIdentifiers(url, "identifiers", prefix);
    }
    
    @Override
    public List<String> listTables(String metalake, String catalog, String schema, String prefix) throws Exception {
        if (metalake == null || catalog == null || schema == null ||
                metalake.isEmpty() || catalog.isEmpty() || schema.isEmpty()) {
            return Collections.emptyList();
        }
        Properties p = getConfigHolder().getRangerSection();
        String baseUrl = resolveBaseUrl(p);

        URL url = new URL(baseUrl + "/api/metalakes/" + metalake + "/catalogs/" + catalog + 
                "/schemas/" + schema + "/tables");
        return executeAndParseIdentifiers(url, "identifiers", prefix);
    }
    
    @Override
    public List<String> listTopics(String metalake, String catalog, String schema, String prefix) throws Exception {
        if (metalake == null || catalog == null || schema == null ||
                metalake.isEmpty() || catalog.isEmpty() || schema.isEmpty()) {
            return Collections.emptyList();
        }
        Properties p = getConfigHolder().getRangerSection();
        String baseUrl = resolveBaseUrl(p);

        URL url = new URL(baseUrl + "/api/metalakes/" + metalake + "/catalogs/" + catalog + 
                "/schemas/" + schema + "/topics");
        return executeAndParseIdentifiers(url, "identifiers", prefix);
    }
    
    @Override
    public List<String> listFilesets(String metalake, String catalog, String schema, String prefix) throws Exception {
        if (metalake == null || catalog == null || schema == null ||
                metalake.isEmpty() || catalog.isEmpty() || schema.isEmpty()) {
            return Collections.emptyList();
        }
        Properties p = getConfigHolder().getRangerSection();
        String baseUrl = resolveBaseUrl(p);

        URL url = new URL(baseUrl + "/api/metalakes/" + metalake + "/catalogs/" + catalog + 
                "/schemas/" + schema + "/filesets");
        return executeAndParseIdentifiers(url, "identifiers", prefix);
    }
    
    @Override
    public List<String> listModels(String metalake, String catalog, String schema, String prefix) throws Exception {
        if (metalake == null || catalog == null || schema == null ||
                metalake.isEmpty() || catalog.isEmpty() || schema.isEmpty()) {
            return Collections.emptyList();
        }
        Properties p = getConfigHolder().getRangerSection();
        String baseUrl = resolveBaseUrl(p);

        URL url = new URL(baseUrl + "/api/metalakes/" + metalake + "/catalogs/" + catalog + 
                "/schemas/" + schema + "/models");
        return executeAndParseIdentifiers(url, "identifiers", prefix);
    }
    
    @Override
    public List<String> listModelVersions(String metalake, String catalog, String schema, 
            String model, String prefix) throws Exception {
        if (metalake == null || catalog == null || schema == null || model == null ||
                metalake.isEmpty() || catalog.isEmpty() || schema.isEmpty() || model.isEmpty()) {
            return Collections.emptyList();
        }
        Properties p = getConfigHolder().getRangerSection();
        String baseUrl = resolveBaseUrl(p);

        URL url = new URL(baseUrl + "/api/metalakes/" + metalake + "/catalogs/" + catalog + 
                "/schemas/" + schema + "/models/" + model + "/versions");
        return executeAndParseVersions(url, prefix);
    }

    private static String resolveBaseUrl(Properties p) {
        String baseUrl = trimToNull(p.getProperty(KEY_XSTORE_URL));
        if (baseUrl == null) {
            baseUrl = trimToNull(p.getProperty(KEY_GRAVITINO_URL));
        }
        return baseUrl;
    }

    private static String trimToNull(String v) {
        if (v == null) {
            return null;
        }
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
    
    /**
     * Execute HTTP request and parse identifiers from response.
     */
    private List<String> executeAndParseIdentifiers(URL url, String arrayField, String prefix) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");

        auth.apply(conn);
        
        int code = conn.getResponseCode();
        if (code < 200 || code >= 300) {
            LOG.warn("Request to {} failed with status {}", url, code);
            return Collections.emptyList();
        }
        
        List<String> names = new ArrayList<>();
        try (InputStream in = conn.getInputStream()) {
            JsonNode root = MAPPER.readTree(in);
            JsonNode arrayNode = root.get(arrayField);
            
            if (arrayNode != null && arrayNode.isArray()) {
                for (JsonNode node : arrayNode) {
                    String name = extractName(node);
                    if (name != null && matchesPrefix(name, prefix)) {
                        names.add(name);
                    }
                }
            }
        }
        
        LOG.debug("Listed {} items from {} (prefix: {})", names.size(), url, prefix);
        return names;
    }
    
    /**
     * Parse version list from response.
     */
    private List<String> executeAndParseVersions(URL url, String prefix) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");

        auth.apply(conn);
        
        int code = conn.getResponseCode();
        if (code < 200 || code >= 300) {
            LOG.warn("Request to {} failed with status {}", url, code);
            return Collections.emptyList();
        }
        
        List<String> versions = new ArrayList<>();
        try (InputStream in = conn.getInputStream()) {
            JsonNode root = MAPPER.readTree(in);
            JsonNode arrayNode = root.get("versions");
            
            if (arrayNode != null && arrayNode.isArray()) {
                for (JsonNode node : arrayNode) {
                    String version = node.asText();
                    if (version != null && matchesPrefix(version, prefix)) {
                        versions.add(version);
                    }
                }
            }
        }
        
        return versions;
    }
    
    /**
     * Extract name from a JSON node (handles both string and object formats).
     */
    private String extractName(JsonNode node) {
        if (node.isTextual()) {
            return node.asText();
        } else if (node.isObject()) {
            // Handle NameIdentifier format: {"namespace": [...], "name": "..."}
            JsonNode nameNode = node.get("name");
            if (nameNode != null && nameNode.isTextual()) {
                return nameNode.asText();
            }
        }
        return null;
    }
    
    /**
     * Check if name matches prefix (case-insensitive).
     */
    private boolean matchesPrefix(String name, String prefix) {
        if (prefix == null || prefix.isEmpty() || prefix.equals("*")) {
            return true;
        }
        return name.toLowerCase().startsWith(prefix.toLowerCase());
    }
}
