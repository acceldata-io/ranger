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

package io.acceldata.mlflow.ranger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.Executors;

/**
 * Lightweight HTTP server exposing {@code /healthz}, {@code /readyz}, and {@code /status}
 * for liveness/readiness probes and operational diagnostics.
 */
public final class HealthServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HealthServer.class);

    private final int               port;
    private final PolicySyncService syncService;
    private HttpServer              server;

    public HealthServer(int port, PolicySyncService syncService) {
        this.port        = port;
        this.syncService = Objects.requireNonNull(syncService, "syncService");
    }

    public synchronized void start() throws IOException {
        if (server != null) {
            return;
        }
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "mlflow-agent-health");
            t.setDaemon(true);
            return t;
        }));
        server.createContext("/healthz", this::handleHealth);
        server.createContext("/readyz",  this::handleReady);
        server.createContext("/status",  this::handleStatus);
        server.start();
        LOG.info("HealthServer listening on port {}", actualPort());
    }

    public synchronized int actualPort() {
        return server == null ? port : server.getAddress().getPort();
    }

    public synchronized void stop() {
        if (server == null) {
            return;
        }
        server.stop(1);
        server = null;
        LOG.info("HealthServer stopped");
    }

    @Override
    public void close() {
        stop();
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            respond(exchange, 405, "{\"error\":\"method_not_allowed\"}");
            return;
        }
        respond(exchange, 200, "{\"status\":\"UP\"}");
    }

    private void handleReady(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            respond(exchange, 405, "{\"error\":\"method_not_allowed\"}");
            return;
        }
        if (syncService.isReady()) {
            respond(exchange, 200, "{\"ready\":true}");
        } else {
            respond(exchange, 503, "{\"ready\":false,\"reason\":\"initial_sync_pending\"}");
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            respond(exchange, 405, "{\"error\":\"method_not_allowed\"}");
            return;
        }
        String json = "{"
                + "\"ready\":" + syncService.isReady()
                + ",\"cycles\":" + syncService.cycleCount()
                + ",\"successes\":" + syncService.successCount()
                + ",\"failures\":" + syncService.failureCount()
                + ",\"policyVersion\":" + syncService.lastKnownPolicyVersion()
                + ",\"userStoreVersion\":" + syncService.lastKnownUserStoreVersion()
                + ",\"roleVersion\":" + syncService.lastKnownRoleVersion()
                + ",\"lastSuccessAtMs\":" + syncService.lastSuccessAtMs()
                + "}";
        respond(exchange, 200, json);
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        } finally {
            exchange.close();
        }
    }
}
