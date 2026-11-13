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
package org.apache.ranger.services.pinot.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Calls the Pinot controller REST API for Ranger Admin: the connection test and the database / table lookup that
 * fills policy resources. Controllers are tried in order until one answers. Authentication: none, Basic (user name
 * and password) or OAuth 2.0 client credentials (a token is fetched per client instance).
 *
 * Never puts a password, a client secret or a token into an exception message or a log line.
 */
public class PinotAdminClient {
    public static final String CONFIG_CONTROLLER_URL       = "controller.url";
    public static final String CONFIG_AUTHENTICATION_TYPE  = "authentication.type";
    public static final String CONFIG_USERNAME             = "username";
    public static final String CONFIG_PASSWORD             = "password";
    public static final String CONFIG_OAUTH2_TOKEN_URL     = "oauth2.token.url";
    public static final String CONFIG_OAUTH2_CLIENT_ID     = "oauth2.client.id";
    public static final String CONFIG_OAUTH2_CLIENT_SECRET = "oauth2.client.secret";
    public static final String CONFIG_OAUTH2_SCOPE         = "oauth2.scope";

    static final int  TIMEOUT_MS  = 10_000;
    static final int  MAX_ANSWER  = 4 * 1024 * 1024;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<String> controllers;
    private final String       authType;
    private final String       username;
    private final String       password;
    private final String       tokenUrl;
    private final String       clientId;
    private final String       clientSecret;
    private final String       scope;
    private       String       authorization;

    public PinotAdminClient(Map<String, String> configs) {
        String urls = StringUtils.trimToEmpty(configs.get(CONFIG_CONTROLLER_URL));

        if (urls.isEmpty()) {
            throw new IllegalArgumentException("'" + CONFIG_CONTROLLER_URL + "' is required");
        }

        List<String> list = new ArrayList<>();

        for (String url : urls.split(",")) {
            url = StringUtils.stripEnd(url.trim(), "/");

            if (!url.isEmpty()) {
                if (!url.startsWith("http://") && !url.startsWith("https://")) {
                    throw new IllegalArgumentException("'" + CONFIG_CONTROLLER_URL + "' must start with http:// or https://: " + url);
                }

                list.add(url);
            }
        }

        this.controllers  = Collections.unmodifiableList(list);
        this.authType     = StringUtils.defaultIfBlank(configs.get(CONFIG_AUTHENTICATION_TYPE), "basic").trim().toLowerCase();
        this.username     = StringUtils.trimToNull(configs.get(CONFIG_USERNAME));
        this.password     = configs.get(CONFIG_PASSWORD);
        this.tokenUrl     = StringUtils.trimToNull(configs.get(CONFIG_OAUTH2_TOKEN_URL));
        this.clientId     = StringUtils.trimToNull(configs.get(CONFIG_OAUTH2_CLIENT_ID));
        this.clientSecret = configs.get(CONFIG_OAUTH2_CLIENT_SECRET);
        this.scope        = StringUtils.trimToNull(configs.get(CONFIG_OAUTH2_SCOPE));

        switch (authType) {
            case "none":
                break;
            case "basic":
                if (username == null) {
                    throw new IllegalArgumentException("'" + CONFIG_USERNAME + "' is required for Basic authentication");
                }
                break;
            case "oauth2":
                if (tokenUrl == null || clientId == null || StringUtils.isEmpty(clientSecret)) {
                    throw new IllegalArgumentException("'" + CONFIG_OAUTH2_TOKEN_URL + "', '" + CONFIG_OAUTH2_CLIENT_ID + "' and '" + CONFIG_OAUTH2_CLIENT_SECRET + "' are required for OAuth 2.0");
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown '" + CONFIG_AUTHENTICATION_TYPE + "' " + authType + "; expected none, basic or oauth2");
        }
    }

    /** Checks that a controller is up (GET /health) and that the configured credentials may list tables. */
    public void testConnection() throws IOException {
        get("/health", null);
        get("/tables", null);
    }

    /** The cluster's databases (GET /databases). */
    public List<String> getDatabases() throws IOException {
        return strings(MAPPER.readTree(get("/databases", null)));
    }

    /** The raw names of a database's tables (GET /tables with the database header). */
    public List<String> getTables(String database) throws IOException {
        JsonNode tables = MAPPER.readTree(get("/tables", database)).get("tables");

        return tables == null ? Collections.<String>emptyList() : strings(tables);
    }

    String get(String path, String database) throws IOException {
        IOException last = null;

        for (String controller : controllers) {
            try {
                return call(controller + path, database);
            } catch (PinotHttpException e) {
                throw e; // the controller answered: another controller answers the same
            } catch (IOException e) {
                last = e;
            }
        }

        throw last != null ? last : new IOException("No controller configured");
    }

    private String call(String url, String database) throws IOException {
        HttpURLConnection conn = open(url);

        conn.setRequestMethod("GET");
        // JSON for the lookups, but /health answers text/plain: an Accept without a fallback gets 406
        conn.setRequestProperty("Accept", "application/json, text/plain;q=0.9, */*;q=0.8");

        if (database != null) {
            conn.setRequestProperty("database", database);
        }

        String auth = authorization();

        if (auth != null) {
            conn.setRequestProperty("Authorization", auth);
        }

        try {
            int status = conn.getResponseCode();

            if (status / 100 != 2) {
                throw new PinotHttpException(status, "GET " + url + " answered " + status);
            }

            return read(conn.getInputStream());
        } finally {
            conn.disconnect();
        }
    }

    private synchronized String authorization() throws IOException {
        switch (authType) {
            case "basic":
                String pair = username + ":" + StringUtils.defaultString(password);
                return "Basic " + Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8));
            case "oauth2":
                if (authorization == null) {
                    authorization = "Bearer " + fetchToken();
                }
                return authorization;
            default:
                return null;
        }
    }

    private String fetchToken() throws IOException {
        HttpURLConnection conn = open(tokenUrl);
        String form = "grant_type=client_credentials" + (scope != null ? "&scope=" + URLEncoder.encode(scope, "UTF-8") : "");
        String pair = URLEncoder.encode(clientId, "UTF-8") + ":" + URLEncoder.encode(clientSecret, "UTF-8");

        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8)));

        try {
            try (OutputStream out = conn.getOutputStream()) {
                out.write(form.getBytes(StandardCharsets.UTF_8));
            }

            int status = conn.getResponseCode();

            if (status / 100 != 2) {
                // Do not include the body: some providers echo the request
                throw new PinotHttpException(status, "Token endpoint answered " + status);
            }

            JsonNode token = MAPPER.readTree(read(conn.getInputStream())).get("access_token");

            if (token == null || !token.isTextual() || token.asText().isEmpty()) {
                throw new IOException("Token endpoint answer has no access_token");
            }

            return token.asText();
        } finally {
            conn.disconnect();
        }
    }

    private static HttpURLConnection open(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

        conn.setConnectTimeout(TIMEOUT_MS);
        conn.setReadTimeout(TIMEOUT_MS);
        conn.setInstanceFollowRedirects(false);

        return conn;
    }

    private static String read(InputStream in) throws IOException {
        try (InputStream is = in) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int n;

            while ((n = is.read(buf)) > 0) {
                out.write(buf, 0, n);

                if (out.size() > MAX_ANSWER) {
                    throw new IOException("Answer larger than " + MAX_ANSWER + " bytes");
                }
            }

            return new String(out.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private static List<String> strings(JsonNode node) throws IOException {
        if (node == null || !node.isArray()) {
            throw new IOException("Unexpected answer from the Pinot controller: expected a JSON array");
        }

        List<String> ret = new ArrayList<>();

        for (JsonNode n : node) {
            if (n.isTextual()) {
                ret.add(n.asText());
            }
        }

        return ret;
    }

    /** The controller (or token endpoint) answered with a non-2xx status. */
    public static class PinotHttpException extends IOException {
        private final int status;

        public PinotHttpException(int status, String message) {
            super(message);

            this.status = status;
        }

        public int getStatus() {
            return status;
        }
    }
}
