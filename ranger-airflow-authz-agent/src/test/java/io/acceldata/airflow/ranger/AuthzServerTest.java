/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.airflow.ranger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class AuthzServerTest {

    private static final String TOKEN = "0123456789abcdef0123456789abcdef";
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path tmp;

    private FakeEngine engine;
    private AuthzServer server;

    @BeforeEach
    void setUp() throws Exception {
        engine = new FakeEngine();
        AgentConfig config = AgentConfig.load(writeProps());
        server = new AuthzServer(config, engine);
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    @DisplayName("health is unauthenticated; info and authorize are not")
    void sharedSecretBoundary() throws Exception {
        assertThat(get("/v1/health", null, 200).path("status").asText()).isEqualTo("ok");
        get("/v1/info", null, 401);
        get("/v1/info", "Bearer wrong-token-that-is-32-bytes!!", 401);
        JsonNode info = get("/v1/info", "Bearer " + TOKEN, 200);
        assertThat(info.path("contract_version").asText()).isEqualTo("v1");
        assertThat(info.path("ranger_service").asText()).isEqualTo("odp_airflow");
    }

    @Test
    @DisplayName("ready is 503 until the engine has policies")
    void readyGate() throws Exception {
        engine.ready = false;
        engine.reason = "policies not loaded";
        JsonNode body = get("/v1/ready", null, 503);
        assertThat(body.path("ready").asBoolean()).isFalse();
        engine.ready = true;
        assertThat(get("/v1/ready", null, 200).path("ready").asBoolean()).isTrue();
    }

    @Test
    @DisplayName("authorize returns a per-check decision and threads audit context")
    void authorizeDagTrigger() throws Exception {
        String payload = "{"
                + "\"user\":\"alice@CORP.EXAMPLE\","
                + "\"context\":{\"client_ip\":\"10.4.2.19\",\"request_uri\":\"/api/v2/dags/etl_sales/dagRuns\",\"request_id\":\"c3f1a8e2\"},"
                + "\"checks\":[{\"id\":\"0\",\"resource_type\":\"dag\",\"method\":\"POST\",\"access_entity\":\"RUN\",\"key\":\"etl_sales\"}]"
                + "}";
        JsonNode body = post("/v1/authorize", "Bearer " + TOKEN, payload, 200);
        assertThat(body.path("policy_version").asLong()).isEqualTo(118L);
        JsonNode d = body.path("decisions").get(0);
        assertThat(d.path("id").asText()).isEqualTo("0");
        assertThat(d.path("allowed").asBoolean()).isTrue();
        assertThat(d.path("policy_id").asLong()).isEqualTo(47L);

        RangerAccessRequest seen = engine.lastRequest.get();
        assertThat(seen).isNotNull();
        assertThat(seen.getUser()).isEqualTo("alice");
        assertThat(seen.getAccessType()).isEqualTo("trigger");
        assertThat(seen.getResource().getValue("dag")).isEqualTo("etl_sales");
        assertThat(seen.getClientIPAddress()).isEqualTo("10.4.2.19");
        assertThat(seen.getRequestData()).isEqualTo("/api/v2/dags/etl_sales/dagRuns");
        assertThat(seen.getSessionId()).isEqualTo("c3f1a8e2");
        assertThat(seen.getClusterName()).isEqualTo("odp-dev");
        assertThat(seen.getUserGroups()).isEmpty();
    }

    @Test
    @DisplayName("unmapped combination is a deny, not a 500")
    void unmappedDenies() throws Exception {
        String payload = "{"
                + "\"user\":\"alice\","
                + "\"context\":{\"client_ip\":\"10.4.2.19\",\"request_uri\":\"/api/v2/config\"},"
                + "\"checks\":[{\"id\":\"1\",\"resource_type\":\"config\",\"method\":\"POST\",\"key\":\"smtp\"}]"
                + "}";
        JsonNode d = post("/v1/authorize", "Bearer " + TOKEN, payload, 200).path("decisions").get(0);
        assertThat(d.path("allowed").asBoolean()).isFalse();
        assertThat(d.path("reason").asText()).isEqualTo("unmapped_access");
        RangerAccessRequest audited = engine.lastRequest.get();
        assertThat(audited).as("§6: unmapped_access denials go through evaluate so they audit").isNotNull();
        assertThat(audited.getUser()).isEqualTo("alice");
        assertThat(audited.getAccessType()).isEqualTo("unmapped_access");
        assertThat(audited.getResource().getValue("config")).isEqualTo("smtp");
        assertThat(audited.getClientIPAddress()).isEqualTo("10.4.2.19");
        assertThat(audited.getRequestData()).isEqualTo("/api/v2/config");
    }

    @Test
    @DisplayName("unknown resource_type is 422")
    void unknownType422() throws Exception {
        String payload = "{"
                + "\"user\":\"alice\","
                + "\"context\":{\"client_ip\":\"10.4.2.19\",\"request_uri\":\"/x\"},"
                + "\"checks\":[{\"id\":\"1\",\"resource_type\":\"nope\",\"method\":\"GET\"}]"
                + "}";
        post("/v1/authorize", "Bearer " + TOKEN, payload, 422);
    }

    @Test
    @DisplayName("omitted key does not set a Ranger resource value")
    void omittedKeyMeansAny() throws Exception {
        engine.allow = false;
        engine.policyId = -1;
        String payload = "{"
                + "\"user\":\"alice\","
                + "\"context\":{\"client_ip\":\"10.4.2.19\",\"request_uri\":\"/api/v2/connections\"},"
                + "\"checks\":[{\"id\":\"2\",\"resource_type\":\"connection\",\"method\":\"GET\"}]"
                + "}";
        JsonNode d = post("/v1/authorize", "Bearer " + TOKEN, payload, 200).path("decisions").get(0);
        assertThat(d.path("allowed").asBoolean()).isFalse();
        assertThat(d.path("reason").asText()).isEqualTo("no_matching_policy");
        RangerAccessRequest seen = engine.lastRequest.get();
        assertThat(seen.getResource().getValue("connection")).isNull();
        assertThat(seen.getResourceMatchingScope())
                .as("an omitted key must widen the matching scope, or a prefix-scoped "
                        + "policy yields MatchType.DESCENDANT and the engine denies")
                .isEqualTo(RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);
    }

    @Test
    @DisplayName("a specific key keeps the default SELF matching scope")
    void specificKeyUsesSelfScope() throws Exception {
        String payload = "{"
                + "\"user\":\"alice\","
                + "\"context\":{\"client_ip\":\"10.4.2.19\",\"request_uri\":\"/api/v2/dags/etl_sales\"},"
                + "\"checks\":[{\"id\":\"0\",\"resource_type\":\"dag\",\"method\":\"GET\",\"key\":\"etl_sales\"}]"
                + "}";
        post("/v1/authorize", "Bearer " + TOKEN, payload, 200);
        RangerAccessRequest seen = engine.lastRequest.get();
        assertThat(seen.getResource().getValue("dag")).isEqualTo("etl_sales");
        assertThat(seen.getResourceMatchingScope())
                .isEqualTo(RangerAccessRequest.ResourceMatchingScope.SELF);
    }

    @Test
    @DisplayName("422 on unknown vocabulary evaluates nothing, so no audit records are written")
    void unknownVocabularyEvaluatesNothing() throws Exception {
        String payload = "{"
                + "\"user\":\"alice\","
                + "\"context\":{\"client_ip\":\"10.4.2.19\",\"request_uri\":\"/api/v2/dags\"},"
                + "\"checks\":["
                + "{\"id\":\"0\",\"resource_type\":\"dag\",\"method\":\"GET\",\"key\":\"etl_sales\"},"
                + "{\"id\":\"1\",\"resource_type\":\"dag\",\"method\":\"GET\",\"key\":\"etl_costs\"},"
                + "{\"id\":\"2\",\"resource_type\":\"sandwich\",\"method\":\"GET\"}"
                + "]}";
        JsonNode err = post("/v1/authorize", "Bearer " + TOKEN, payload, 422);
        assertThat(err.path("error").asText()).isEqualTo("unknown_vocabulary");
        assertThat(engine.evaluations.get())
                .as("checks before the bad one must not reach the engine, or they "
                        + "leave audit rows for a request that returned no decisions")
                .isZero();
    }

    @Test
    @DisplayName("filter is 501 while unimplemented, and info says so up front")
    void filterNotImplemented() throws Exception {
        JsonNode err = post("/v1/filter", "Bearer " + TOKEN, "{}", 501);
        assertThat(err.path("error").asText()).isEqualTo("not_implemented");

        JsonNode info = get("/v1/info", "Bearer " + TOKEN, 200);
        assertThat(info.path("capabilities").isArray()).isTrue();
        assertThat(info.path("capabilities").toString()).isEqualTo("[\"authorize\"]");
        assertThat(info.path("user_store_version").asLong()).isEqualTo(9L);
    }

    @Test
    @DisplayName("info omits user_store_version when no user store has been downloaded")
    void infoOmitsUserStoreVersionWhenAbsent() throws Exception {
        engine.userStoreVersion = -1L;
        JsonNode info = get("/v1/info", "Bearer " + TOKEN, 200);
        assertThat(info.has("user_store_version")).isFalse();
    }

    private Path writeProps() throws Exception {
        Path token = tmp.resolve("token");
        Files.writeString(token, TOKEN);
        Files.setPosixFilePermissions(token, Set.of(PosixFilePermission.OWNER_READ));
        Path props = tmp.resolve("agent.properties");
        Files.writeString(props, ""
                + "authz.agent.bind.address=127.0.0.1\n"
                + "authz.agent.port=0\n"
                + "authz.agent.token.file=" + token.toAbsolutePath() + "\n"
                + "authz.agent.cluster.name=odp-dev\n");
        return props;
    }

    private String base() {
        return "http://127.0.0.1:" + server.boundPort();
    }

    @Test
    @DisplayName("filter returns the permitted subset and audits the call exactly once")
    void filterReturnsSubsetAndAuditsOnce() throws Exception {
        engine.allowedKeys = Set.of("etl_0001", "etl_0002");

        String body = "{\"user\":\"spark@CORP.EXAMPLE\","
                + "\"context\":{\"client_ip\":\"10.4.2.19\",\"request_uri\":\"/api/v2/dags\"},"
                + "\"resource_type\":\"dag\",\"method\":\"GET\","
                + "\"keys\":[\"etl_0001\",\"finance_0001\",\"etl_0002\",\"mktg_0001\"]}";
        JsonNode out = post("/v1/filter", "Bearer " + TOKEN, body, 200);

        assertThat(out.path("allowed_keys")).hasSize(2);
        assertThat(out.path("allowed_keys").toString()).contains("etl_0001").contains("etl_0002");
        assertThat(out.path("evaluated").asInt()).isEqualTo(4);
        assertThat(out.path("policy_version").asLong()).isEqualTo(118L);

        // One evaluation per key, none of them audited individually...
        assertThat(engine.unauditedEvaluations.get()).isEqualTo(4);
        assertThat(engine.evaluations.get()).isZero();
        // ...and exactly one summary record for the whole call.
        assertThat(engine.auditSummaries.get()).isEqualTo(1);
        assertThat(engine.lastSummaryAllowed.get()).isTrue();
        assertThat(engine.lastSummary.get()).isEqualTo("filter: /api/v2/dags evaluated=4 allowed=2");
    }

    @Test
    @DisplayName("filter denying every key still audits once, as a denial")
    void filterDenyingEverythingAuditsOnce() throws Exception {
        engine.allowedKeys = Set.of();

        String body = "{\"user\":\"spark\","
                + "\"context\":{\"client_ip\":\"10.4.2.19\",\"request_uri\":\"/api/v2/dags\"},"
                + "\"resource_type\":\"dag\",\"method\":\"GET\",\"keys\":[\"etl_0001\",\"etl_0002\"]}";
        JsonNode out = post("/v1/filter", "Bearer " + TOKEN, body, 200);

        assertThat(out.path("allowed_keys")).isEmpty();
        assertThat(engine.auditSummaries.get()).isEqualTo(1);
        assertThat(engine.lastSummaryAllowed.get()).isFalse();
    }

    @Test
    @DisplayName("filter rejects unknown vocabulary with 422 and writes no audit record")
    void filterUnknownVocabulary() throws Exception {
        String body = "{\"user\":\"spark\","
                + "\"context\":{\"client_ip\":\"10.4.2.19\",\"request_uri\":\"/api/v2/x\"},"
                + "\"resource_type\":\"teapot\",\"method\":\"GET\",\"keys\":[\"a\"]}";
        post("/v1/filter", "Bearer " + TOKEN, body, 422);

        assertThat(engine.unauditedEvaluations.get()).isZero();
        assertThat(engine.auditSummaries.get()).isZero();
    }

    @Test
    @DisplayName("filter requires the shared secret and a non-empty key list")
    void filterInputGuards() throws Exception {
        String body = "{\"user\":\"spark\","
                + "\"context\":{\"client_ip\":\"10.4.2.19\",\"request_uri\":\"/api/v2/dags\"},"
                + "\"resource_type\":\"dag\",\"method\":\"GET\",\"keys\":[]}";
        post("/v1/filter", null, body, 401);
        post("/v1/filter", "Bearer " + TOKEN, body, 400);
        assertThat(engine.auditSummaries.get()).isZero();
    }

    @Test
    @DisplayName("whoami shows the normalized name and the groups Ranger resolved")
    void whoamiReportsIdentity() throws Exception {
        engine.groupsByUser = java.util.Map.of("alice", Set.of("data_eng", "all_staff"));

        JsonNode out = get("/v1/whoami?principal=alice%40CORP.EXAMPLE", "Bearer " + TOKEN, 200);
        assertThat(out.path("principal").asText()).isEqualTo("alice@CORP.EXAMPLE");
        assertThat(out.path("normalized").asText()).isEqualTo("alice");
        assertThat(out.path("groups").toString()).contains("data_eng").contains("all_staff");
        assertThat(out.path("user_store_version").asLong()).isEqualTo(9L);

        // An LDAP DN normalizes the same way, and an unknown user resolves to
        // no groups rather than an error -- that is the common diagnosis.
        JsonNode dn = get("/v1/whoami?principal=uid%3Dbob%2Cou%3Dpeople%2Cdc%3Dcorp",
                "Bearer " + TOKEN, 200);
        assertThat(dn.path("normalized").asText()).isEqualTo("bob");
        assertThat(dn.path("groups")).isEmpty();
    }

    @Test
    @DisplayName("whoami needs the shared secret and a principal")
    void whoamiGuards() throws Exception {
        get("/v1/whoami?principal=alice", null, 401);
        get("/v1/whoami", "Bearer " + TOKEN, 400);
    }

    @Test
    @DisplayName("info advertises the filter capability")
    void infoAdvertisesFilter() throws Exception {
        JsonNode info = get("/v1/info", "Bearer " + TOKEN, 200);
        assertThat(info.path("capabilities").toString()).contains("authorize").contains("filter");
    }

    private JsonNode get(String path, String auth, int expected) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URI(base() + path).toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        if (auth != null) {
            conn.setRequestProperty("Authorization", auth);
        }
        return read(conn, expected);
    }

    private JsonNode post(String path, String auth, String body, int expected) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URI(base() + path).toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Authorization", auth);
        conn.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));
        return read(conn, expected);
    }

    private static JsonNode read(HttpURLConnection conn, int expected) throws Exception {
        int code = conn.getResponseCode();
        assertThat(code).isEqualTo(expected);
        InputStream in = code < 400 ? conn.getInputStream() : conn.getErrorStream();
        String text = in == null ? "{}" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        return text.isBlank() ? JSON.readTree("{}") : JSON.readTree(text);
    }

    static final class FakeEngine implements AuthzEngine {
        volatile boolean ready = true;
        volatile String reason = "policies not loaded";
        volatile boolean allow = true;
        volatile long policyId = 47;
        volatile long userStoreVersion = 9L;
        /** When non-null, only these resource values are permitted. */
        volatile Set<String> allowedKeys = null;
        final AtomicReference<RangerAccessRequest> lastRequest = new AtomicReference<>();
        final AtomicInteger evaluations = new AtomicInteger();
        final AtomicInteger unauditedEvaluations = new AtomicInteger();
        final AtomicInteger auditSummaries = new AtomicInteger();
        final AtomicReference<String> lastSummary = new AtomicReference<>();
        final AtomicReference<Boolean> lastSummaryAllowed = new AtomicReference<>();

        @Override public boolean isReady() { return ready; }
        @Override public String notReadyReason() { return reason; }
        @Override public long policyVersion() { return 118L; }
        @Override public String serviceName() { return "odp_airflow"; }
        @Override public String clusterName() { return "odp-dev"; }
        @Override public Integer serviceDefVersion() { return 3; }
        @Override public long userStoreVersion() { return userStoreVersion; }
        @Override public void close() {}

        volatile java.util.Map<String, Set<String>> groupsByUser = java.util.Map.of();

        @Override
        public Set<String> resolvedGroups(String user) {
            return groupsByUser.getOrDefault(user, Set.of());
        }

        @Override
        public RangerAccessResult evaluate(RangerAccessRequest request) {
            evaluations.incrementAndGet();
            return decide(request);
        }

        @Override
        public RangerAccessResult evaluateNoAudit(RangerAccessRequest request) {
            unauditedEvaluations.incrementAndGet();
            return decide(request);
        }

        @Override
        public void auditFilterSummary(RangerAccessRequest request, RangerAccessResult result,
                                       boolean allowed, String requestData) {
            auditSummaries.incrementAndGet();
            lastSummary.set(requestData);
            lastSummaryAllowed.set(allowed);
        }

        private RangerAccessResult decide(RangerAccessRequest request) {
            lastRequest.set(request);
            RangerAccessResult result = new RangerAccessResult(
                    RangerPolicy.POLICY_TYPE_ACCESS, "odp_airflow", null, request);
            boolean permitted = allowedKeys == null ? allow : allowedKeys.contains(keyOf(request));
            result.setIsAllowed(permitted);
            result.setIsAccessDetermined(true);
            if (permitted && policyId > 0) {
                result.setPolicyId(policyId);
            }
            return result;
        }

        private static String keyOf(RangerAccessRequest request) {
            if (request == null || request.getResource() == null) {
                return null;
            }
            java.util.Map<String, Object> values = request.getResource().getAsMap();
            if (values == null || values.isEmpty()) {
                return null;
            }
            Object first = values.values().iterator().next();
            return first == null ? null : first.toString();
        }
    }
}
