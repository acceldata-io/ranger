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
        assertThat(engine.lastRequest.get()).isNull();
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
        final AtomicReference<RangerAccessRequest> lastRequest = new AtomicReference<>();

        @Override public boolean isReady() { return ready; }
        @Override public String notReadyReason() { return reason; }
        @Override public long policyVersion() { return 118L; }
        @Override public String serviceName() { return "odp_airflow"; }
        @Override public Integer serviceDefVersion() { return 3; }
        @Override public void close() {}

        @Override
        public RangerAccessResult evaluate(RangerAccessRequest request) {
            lastRequest.set(request);
            RangerAccessResult result = new RangerAccessResult(
                    RangerPolicy.POLICY_TYPE_ACCESS, "odp_airflow", null, request);
            result.setIsAllowed(allow);
            result.setIsAccessDetermined(true);
            if (policyId > 0) {
                result.setPolicyId(policyId);
            }
            return result;
        }
    }
}
