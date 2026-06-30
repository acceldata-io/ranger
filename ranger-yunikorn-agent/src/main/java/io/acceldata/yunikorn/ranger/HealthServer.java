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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Minimal HTTP server exposing what Kubernetes (and Prometheus) expect from a
 * production-grade pod:
 *
 * <ul>
 *   <li>{@code GET /health/live}  — liveness probe. 200 if the JVM is up.
 *       Used by K8s to decide whether to restart the pod. We always return
 *       200 once started; if this endpoint stops responding, the JVM is
 *       wedged and a restart is the right move.</li>
 *
 *   <li>{@code GET /health/ready} — readiness probe. 200 only after the
 *       sync service has completed at least one successful cycle. Until
 *       then, K8s holds traffic / dependents back.</li>
 *
 *   <li>{@code GET /metrics}      — Prometheus text-format scrape endpoint.
 *       Reports cycle count, success/failure counters, last-success
 *       timestamp, and lastKnownVersion.</li>
 * </ul>
 *
 * <p>Uses {@link HttpServer} from the JDK so we don't pull in another HTTP
 * dependency. Single-threaded executor — these endpoints are infrequent
 * and trivially cheap, no need for a thread pool.
 *
 * <h2>Threading</h2>
 * Safe. Health checks read atomic counters from {@link PolicySyncService}.
 * Start/stop are idempotent.
 */
public class HealthServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HealthServer.class);

    private static final String CT_TEXT_PLAIN =
            "text/plain; version=0.0.4; charset=utf-8";

    private final int port;
    private final PolicySyncService syncService;
    private HttpServer httpServer;

    public HealthServer(int port, PolicySyncService syncService) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        this.port = port;
        this.syncService = Objects.requireNonNull(syncService, "syncService");
    }

    /**
     * Start the server. Binds to {@code 0.0.0.0:port}.
     *
     * @throws IOException if the bind fails (port already in use, etc.)
     */
    public synchronized void start() throws IOException {
        if (httpServer != null) {
            LOG.warn("HealthServer.start() called but already started");
            return;
        }
        httpServer = HttpServer.create(new InetSocketAddress(port), /* backlog = */ 0);
        httpServer.createContext("/health/live",  this::handleLive);
        httpServer.createContext("/health/ready", this::handleReady);
        httpServer.createContext("/metrics",      this::handleMetrics);
        // Default executor is fine for our traffic — these endpoints are
        // hit rarely and never block.
        httpServer.start();
        LOG.info("HealthServer listening on :{}", httpServer.getAddress().getPort());
    }

    /** Idempotent. Stops the HTTP server with a 0s grace period. */
    public synchronized void stop() {
        if (httpServer == null) return;
        LOG.info("HealthServer stopping");
        httpServer.stop(0);
        httpServer = null;
    }

    @Override
    public void close() {
        stop();
    }

    /** The actual port we bound to (useful in tests where port=0). */
    public int boundPort() {
        return httpServer == null ? -1 : httpServer.getAddress().getPort();
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    private void handleLive(HttpExchange ex) throws IOException {
        // The JVM is responding to the request, so by definition liveness
        // is OK. K8s uses this to decide pod restarts, not data freshness.
        respond(ex, 200, "OK\n");
    }

    private void handleReady(HttpExchange ex) throws IOException {
        if (syncService.isReady()) {
            respond(ex, 200, "READY\n");
        } else {
            respond(ex, 503, "NOT_READY\n");
        }
    }

    private void handleMetrics(HttpExchange ex) throws IOException {
        StringBuilder sb = new StringBuilder(512);

        appendCounter(sb, "ranger_yunikorn_agent_sync_cycles_total",
                "Number of policy sync cycles started",
                syncService.cycleCount());
        appendCounter(sb, "ranger_yunikorn_agent_sync_success_total",
                "Number of successful sync cycles",
                syncService.successCount());
        appendCounter(sb, "ranger_yunikorn_agent_sync_failure_total",
                "Number of failed sync cycles",
                syncService.failureCount());

        appendGauge(sb, "ranger_yunikorn_agent_last_known_policy_version",
                "Highest Ranger policy version successfully applied " +
                        "(-1 means none yet)",
                syncService.lastKnownVersion());
        appendGauge(sb, "ranger_yunikorn_agent_last_success_timestamp_ms",
                "Wall-clock time of the last successful sync cycle " +
                        "(-1 means none yet)",
                syncService.lastSuccessAtMs());
        appendGauge(sb, "ranger_yunikorn_agent_running",
                "1 if the sync service is running, 0 otherwise",
                syncService.isRunning() ? 1 : 0);

        respond(ex, 200, sb.toString());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static void appendCounter(StringBuilder sb, String name,
                                      String help, long value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" counter\n");
        sb.append(name).append(' ').append(value).append('\n');
    }

    private static void appendGauge(StringBuilder sb, String name,
                                    String help, long value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" gauge\n");
        sb.append(name).append(' ').append(value).append('\n');
    }

    private static void respond(HttpExchange ex, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", CT_TEXT_PLAIN);
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
