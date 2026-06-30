/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.yunikorn.ranger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link HealthServer}.
 *
 * <p>We boot a real HTTP server on an ephemeral port and hit it with
 * {@link HttpURLConnection}. The {@link PolicySyncService} dependency is
 * replaced with a tiny stub that lets each test control what the server's
 * handlers see (ready vs not ready, counter values, etc).
 */
class HealthServerTest {

    private StubSyncService stub;
    private HealthServer    server;

    @BeforeEach
    void setUp() throws IOException {
        stub = new StubSyncService();
        server = new HealthServer(/* ephemeral port */ 0, stub);
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
    }

    private String baseUrl() {
        return "http://127.0.0.1:" + server.boundPort();
    }

    private static String get(String url, int expectedStatus) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URI(url).toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        int code = conn.getResponseCode();
        assertThat(code).isEqualTo(expectedStatus);
        try (InputStream in = code < 400 ? conn.getInputStream() : conn.getErrorStream()) {
            return in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    // -----------------------------------------------------------------------

    @Test
    @DisplayName("/health/live returns 200 OK")
    void liveAlwaysOk() throws Exception {
        String body = get(baseUrl() + "/health/live", 200);
        assertThat(body).isEqualTo("OK\n");
    }

    @Test
    @DisplayName("/health/ready returns 503 before first successful cycle")
    void readyReturns503BeforeFirstSuccess() throws Exception {
        stub.ready.set(false);
        String body = get(baseUrl() + "/health/ready", 503);
        assertThat(body).isEqualTo("NOT_READY\n");
    }

    @Test
    @DisplayName("/health/ready returns 200 after sync becomes ready")
    void readyReturns200WhenReady() throws Exception {
        stub.ready.set(true);
        String body = get(baseUrl() + "/health/ready", 200);
        assertThat(body).isEqualTo("READY\n");
    }

    @Test
    @DisplayName("/metrics emits Prometheus-formatted counters and gauges")
    void metricsEmitPrometheusFormat() throws Exception {
        stub.cycleCount.set(42);
        stub.successCount.set(40);
        stub.failureCount.set(2);
        stub.lastKnownVersion.set(123);
        stub.lastSuccessAtMs.set(1_700_000_000_000L);
        stub.running.set(true);

        String body = get(baseUrl() + "/metrics", 200);

        // Each metric must include its HELP and TYPE lines.
        assertThat(body)
                .contains("# TYPE ranger_yunikorn_agent_sync_cycles_total counter")
                .contains("ranger_yunikorn_agent_sync_cycles_total 42")
                .contains("# TYPE ranger_yunikorn_agent_sync_success_total counter")
                .contains("ranger_yunikorn_agent_sync_success_total 40")
                .contains("# TYPE ranger_yunikorn_agent_sync_failure_total counter")
                .contains("ranger_yunikorn_agent_sync_failure_total 2")
                .contains("# TYPE ranger_yunikorn_agent_last_known_policy_version gauge")
                .contains("ranger_yunikorn_agent_last_known_policy_version 123")
                .contains("ranger_yunikorn_agent_last_success_timestamp_ms 1700000000000")
                .contains("ranger_yunikorn_agent_running 1");
    }

    @Test
    @DisplayName("/metrics gauges reflect 'not running' state")
    void metricsRunningGaugeWhenStopped() throws Exception {
        stub.running.set(false);
        String body = get(baseUrl() + "/metrics", 200);
        assertThat(body).contains("ranger_yunikorn_agent_running 0");
    }

    @Test
    @DisplayName("start() is idempotent")
    void startIdempotent() throws Exception {
        // Calling start a second time should not throw or rebind.
        int firstPort = server.boundPort();
        server.start();
        assertThat(server.boundPort()).isEqualTo(firstPort);
    }

    @Test
    @DisplayName("stop() then start() rebinds (close-and-restart cycle)")
    void stopThenStartAgain() throws Exception {
        server.stop();
        server = new HealthServer(0, stub);   // new instance for clarity
        server.start();
        // Should be live again
        assertThat(get(baseUrl() + "/health/live", 200)).isEqualTo("OK\n");
    }

    @Test
    @DisplayName("constructor rejects out-of-range port")
    void rejectInvalidPort() {
        assertThatThrownBy(() -> new HealthServer(-1, stub))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HealthServer(70_000, stub))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------
    // Stub for PolicySyncService — we extend the real class but don't call
    // start()/stop(). Atomic fields let tests poke values from outside.
    //
    // Why subclass instead of mock? Mockito's inline mock-maker on Java 17
    // can't redefine concrete classes that share lineage with JDK system
    // types (we hit this for ConfigMapWriter too); subclassing sidesteps
    // the issue cleanly and produces simpler tests.
    // -----------------------------------------------------------------------

    private static final class StubSyncService extends PolicySyncService {

        final AtomicBoolean ready              = new AtomicBoolean(false);
        final AtomicBoolean running            = new AtomicBoolean(false);
        final AtomicLong    cycleCount         = new AtomicLong(0);
        final AtomicLong    successCount       = new AtomicLong(0);
        final AtomicLong    failureCount       = new AtomicLong(0);
        final AtomicLong    lastKnownVersion   = new AtomicLong(-1);
        final AtomicLong    lastSuccessAtMs    = new AtomicLong(-1);

        StubSyncService() {
            super(stubConfig(),
                  org.mockito.Mockito.mock(org.apache.ranger.admin.client.RangerAdminClient.class),
                  new AclConverter(),
                  new StubConfigMapWriter(),
                  new java.util.concurrent.ScheduledThreadPoolExecutor(1));
        }

        @Override public boolean isReady()           { return ready.get(); }
        @Override public boolean isRunning()         { return running.get(); }
        @Override public long    cycleCount()        { return cycleCount.get(); }
        @Override public long    successCount()      { return successCount.get(); }
        @Override public long    failureCount()      { return failureCount.get(); }
        @Override public long    lastKnownVersion()  { return lastKnownVersion.get(); }
        @Override public long    lastSuccessAtMs()   { return lastSuccessAtMs.get(); }

        private static AgentConfig stubConfig() {
            java.util.Properties p = new java.util.Properties();
            p.setProperty("ranger.admin.url",    "https://example:6182");
            p.setProperty("ranger.service.name", "stub");
            return AgentConfig.fromProperties(p, key -> null);
        }
    }

    /** ConfigMapWriter the stub never actually exercises. */
    private static final class StubConfigMapWriter extends ConfigMapWriter {
        StubConfigMapWriter() {
            super(new io.fabric8.kubernetes.client.KubernetesClientBuilder()
                          .withConfig(new io.fabric8.kubernetes.client.ConfigBuilder()
                                  .withMasterUrl("http://localhost:1")
                                  .build())
                          .build(),
                  new AclSplicer(),
                  StubSyncService.stubConfig());
        }
    }
}
