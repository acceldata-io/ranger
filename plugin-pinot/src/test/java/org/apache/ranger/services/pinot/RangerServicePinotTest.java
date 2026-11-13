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
package org.apache.ranger.services.pinot;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test Connection, resource lookup and default policies of the Pinot service, against a stand-in controller. */
public class RangerServicePinotTest {
    private static final String BASIC  = "Basic " + Base64.getEncoder().encodeToString("ranger:secret".getBytes(StandardCharsets.UTF_8));
    private static final String BEARER = "Bearer tok-1";

    private HttpServer   server;
    private String       base;
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    private final List<String> databaseHeaders = new CopyOnWriteArrayList<>();

    @Before
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);

        // Like Pinot: /health produces text/plain and refuses (406) a request that accepts only JSON
        server.createContext("/health", exchange -> {
            String accept = String.valueOf(exchange.getRequestHeaders().getFirst("Accept"));

            if (accept.contains("text/plain") || accept.contains("*/*") || "null".equals(accept)) {
                answer(exchange, 200, "OK");
            } else {
                answer(exchange, 406, "Not Acceptable");
            }
        });
        server.createContext("/databases", exchange -> {
            if (authorized(exchange)) {
                answer(exchange, 200, "[\"default\",\"sales\",\"hr\"]");
            }
        });
        server.createContext("/tables", exchange -> {
            if (authorized(exchange)) {
                String db = exchange.getRequestHeaders().getFirst("database");

                databaseHeaders.add(String.valueOf(db));

                if ("sales".equals(db)) {
                    answer(exchange, 200, "{\"tables\":[\"orders\",\"order_items\",\"returns\"]}");
                } else if ("hr".equals(db)) {
                    answer(exchange, 200, "{\"tables\":[\"staff\"]}");
                } else {
                    answer(exchange, 200, "{\"tables\":[\"airlineStats\",\"orders\"]}");
                }
            }
        });
        server.createContext("/oauth/token", exchange -> {
            String expected = "Basic " + Base64.getEncoder().encodeToString("ranger-lookup:csecret".getBytes(StandardCharsets.UTF_8));

            if (expected.equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                answer(exchange, 200, "{\"access_token\":\"tok-1\",\"expires_in\":300}");
            } else {
                answer(exchange, 401, "{\"error\":\"invalid_client\"}");
            }
        });
        server.start();

        base = "http://localhost:" + server.getAddress().getPort();
    }

    @After
    public void stop() {
        server.stop(0);
    }

    @Test
    public void testConnectionWithBasicCredentials() throws Exception {
        assertEquals(Boolean.TRUE, service(basic("secret")).validateConfig().get("connectivityStatus"));
        assertEquals("credentials on the table listing (health is public)", Collections.singletonList(BASIC), authorizations);
    }

    @Test
    public void testConnectionFailsWithWrongPasswordWithoutLeakingIt() throws Exception {
        Map<String, Object> ret = service(basic("wrong-pw")).validateConfig();

        assertEquals(Boolean.FALSE, ret.get("connectivityStatus"));
        assertTrue(String.valueOf(ret.get("message")).contains("401"));
        assertFalse(String.valueOf(ret).contains("wrong-pw"));
    }

    @Test
    public void testConnectionFailsWhenNoControllerAnswers() throws Exception {
        Map<String, String> configs = basic("secret");

        configs.put("controller.url", "http://localhost:1");

        assertEquals(Boolean.FALSE, service(configs).validateConfig().get("connectivityStatus"));
    }

    @Test
    public void secondControllerIsTriedWhenTheFirstIsDown() throws Exception {
        Map<String, String> configs = basic("secret");

        configs.put("controller.url", "http://localhost:1, " + base + "/");

        assertEquals(Boolean.TRUE, service(configs).validateConfig().get("connectivityStatus"));
    }

    @Test
    public void testConnectionWithOAuthClientCredentials() throws Exception {
        Map<String, String> configs = new HashMap<>();

        configs.put("controller.url", base);
        configs.put("authentication.type", "oauth2");
        configs.put("oauth2.token.url", base + "/oauth/token");
        configs.put("oauth2.client.id", "ranger-lookup");
        configs.put("oauth2.client.secret", "csecret");

        assertEquals(Boolean.TRUE, service(configs).validateConfig().get("connectivityStatus"));
        assertEquals(Collections.singletonList(BEARER), authorizations);

        configs.put("oauth2.client.secret", "bad");
        assertEquals(Boolean.FALSE, service(configs).validateConfig().get("connectivityStatus"));
    }

    @Test
    public void lookupDatabases() throws Exception {
        RangerServicePinot service = service(basic("secret"));

        assertEquals(Arrays.asList("default", "sales", "hr"), service.lookupResource(lookup("database", "", Collections.<String, List<String>>emptyMap())));
        assertEquals(Collections.singletonList("sales"), service.lookupResource(lookup("database", "sa", Collections.<String, List<String>>emptyMap())));
        assertEquals("already selected values are not offered", Arrays.asList("default", "hr"), service.lookupResource(lookup("database", "", Collections.singletonMap("database", Collections.singletonList("sales")))));
    }

    @Test
    public void lookupTablesOfTheSelectedDatabases() throws Exception {
        RangerServicePinot        service  = service(basic("secret"));
        Map<String, List<String>> selected = new HashMap<>();

        selected.put("database", Collections.singletonList("sales"));
        assertEquals(Arrays.asList("orders", "order_items"), service.lookupResource(lookup("table", "ord", selected)));
        assertEquals(Collections.singletonList("sales"), databaseHeaders);

        selected.put("database", Collections.singletonList("*"));
        assertEquals(Arrays.asList("airlineStats", "orders", "order_items", "returns", "staff"), service.lookupResource(lookup("table", "", selected)));

        assertEquals("no database selected: default", Arrays.asList("airlineStats", "orders"), service.lookupResource(lookup("table", "", Collections.<String, List<String>>emptyMap())));
        assertEquals("cluster has no lookup", Collections.emptyList(), service.lookupResource(lookup("cluster", "", Collections.<String, List<String>>emptyMap())));
    }

    @Test
    public void defaultPoliciesGrantEverythingToTheLookupAndDefaultUsers() throws Exception {
        Map<String, String> configs = basic("secret");

        configs.put("default.policy.users", "pinot-admin,svc-pinot");
        configs.put("default.policy.groups", "pinot-admins");

        List<RangerPolicy> policies = service(configs).getDefaultRangerPolicies();
        List<String>       names    = new ArrayList<>();

        for (RangerPolicy policy : policies) {
            names.add(policy.getName());

            RangerPolicy.RangerPolicyItem item = policy.getPolicyItems().get(0);

            assertTrue(item.getUsers().containsAll(Arrays.asList("ranger", "pinot-admin", "svc-pinot")));
            assertEquals(Collections.singletonList("pinot-admins"), item.getGroups());
            assertTrue(item.getDelegateAdmin());
            assertEquals("every access type of the service", 15, item.getAccesses().size());
        }

        Collections.sort(names);
        assertEquals("one policy per resource hierarchy", Arrays.asList("all - cluster", "all - database", "all - database, table"), names);
    }

    @Test(expected = IllegalArgumentException.class)
    public void basicNeedsAUserName() {
        Map<String, String> configs = new HashMap<>();

        configs.put("controller.url", base);
        configs.put("authentication.type", "basic");
        new org.apache.ranger.services.pinot.client.PinotAdminClient(configs);
    }

    // ---------------------------------------------------------------------------------------------------------------

    private boolean authorized(HttpExchange exchange) throws IOException {
        String auth = exchange.getRequestHeaders().getFirst("Authorization");

        authorizations.add(String.valueOf(auth));

        if (BASIC.equals(auth) || BEARER.equals(auth)) {
            return true;
        }

        answer(exchange, 401, "{\"code\":401,\"error\":\"HTTP 401 Unauthorized\"}");

        return false;
    }

    private static void answer(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);

        exchange.sendResponseHeaders(status, bytes.length);

        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private Map<String, String> basic(String password) {
        Map<String, String> configs = new HashMap<>();

        configs.put("controller.url", base);
        configs.put("authentication.type", "basic");
        configs.put("username", "ranger");
        configs.put("password", password);

        return configs;
    }

    private static RangerServicePinot service(Map<String, String> configs) {
        RangerService service = new RangerService();

        service.setName("pinotdev");
        service.setType("pinot");
        service.setConfigs(configs);

        RangerServicePinot ret = new RangerServicePinot();

        ret.init(serviceDef(), service);

        return ret;
    }

    static RangerServiceDef serviceDef() {
        return JsonUtils.jsonToObject(new InputStreamReader(RangerServicePinotTest.class.getResourceAsStream("/service-defs/ranger-servicedef-pinot.json"), StandardCharsets.UTF_8), RangerServiceDef.class);
    }

    private static ResourceLookupContext lookup(String resource, String input, Map<String, List<String>> selected) {
        ResourceLookupContext context = new ResourceLookupContext();

        context.setResourceName(resource);
        context.setUserInput(input);
        context.setResources(selected);

        return context;
    }
}
