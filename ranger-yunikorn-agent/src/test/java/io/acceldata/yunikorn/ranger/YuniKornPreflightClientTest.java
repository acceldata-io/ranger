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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises {@link YuniKornPreflightClient} against a tiny in-process
 * {@link HttpServer} standing in for YuniKorn's {@code /ws/v1/validate-conf}
 * endpoint. No external dependency, full control over status + body.
 */
class YuniKornPreflightClientTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) server.stop(0);
    }

    // -----------------------------------------------------------------------

    @Test
    @DisplayName("allowed=true → validate() returns and the YAML body is POSTed verbatim")
    void allowedPasses() throws Exception {
        AtomicReference<String> received = new AtomicReference<>();
        AtomicReference<String> method   = new AtomicReference<>();
        String base = startServer(200, "{\"allowed\":true,\"reason\":\"\"}", received, method);

        YuniKornPreflightClient client = new YuniKornPreflightClient(base);
        client.validate("partitions:\n  - name: default\n");

        assertThat(method.get()).isEqualTo("POST");
        assertThat(received.get()).isEqualTo("partitions:\n  - name: default\n");
    }

    @Test
    @DisplayName("allowed=false → PreflightException carrying YuniKorn's reason")
    void rejectedThrowsWithReason() throws Exception {
        String base = startServer(200,
                "{\"allowed\":false,\"reason\":\"undefined queue in placement rule\"}");

        YuniKornPreflightClient client = new YuniKornPreflightClient(base);

        assertThatThrownBy(() -> client.validate("partitions: []"))
                .isInstanceOf(PreflightException.class)
                .hasMessageContaining("undefined queue in placement rule");
    }

    @Test
    @DisplayName("Capitalised JSON fields (Go-style) still parse")
    void caseInsensitiveJson() throws Exception {
        String base = startServer(200, "{\"Allowed\":false,\"Reason\":\"capitalised\"}");

        YuniKornPreflightClient client = new YuniKornPreflightClient(base);

        assertThatThrownBy(() -> client.validate("x"))
                .isInstanceOf(PreflightException.class)
                .hasMessageContaining("capitalised");
    }

    @Test
    @DisplayName("Non-200 status is fail-closed")
    void non200FailsClosed() throws Exception {
        String base = startServer(404, "not found");

        YuniKornPreflightClient client = new YuniKornPreflightClient(base);

        assertThatThrownBy(() -> client.validate("x"))
                .isInstanceOf(PreflightException.class)
                .hasMessageContaining("404");
    }

    @Test
    @DisplayName("Unreachable endpoint is fail-closed")
    void unreachableFailsClosed() throws Exception {
        // Grab a port, then immediately free it so nothing is listening.
        int deadPort;
        try (ServerSocket s = new ServerSocket(0)) {
            deadPort = s.getLocalPort();
        }
        YuniKornPreflightClient client =
                new YuniKornPreflightClient("http://127.0.0.1:" + deadPort, 500, 500);

        assertThatThrownBy(() -> client.validate("x"))
                .isInstanceOf(PreflightException.class)
                .hasMessageContaining("validate-conf");
    }

    @Test
    @DisplayName("Trailing slashes on the rest URL are trimmed before appending the path")
    void trailingSlashTrimmed() throws Exception {
        AtomicReference<String> received = new AtomicReference<>();
        AtomicReference<String> method   = new AtomicReference<>();
        String base = startServer(200, "{\"allowed\":true}", received, method);

        // Add trailing slashes; the client must still hit /ws/v1/validate-conf.
        new YuniKornPreflightClient(base + "//").validate("ok");

        assertThat(received.get()).isEqualTo("ok");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private String startServer(int status, String body) throws IOException {
        return startServer(status, body, new AtomicReference<>(), new AtomicReference<>());
    }

    /** Starts a server that only answers /ws/v1/validate-conf; returns its base URL. */
    private String startServer(int status, String body,
                               AtomicReference<String> bodySink,
                               AtomicReference<String> methodSink) throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/ws/v1/validate-conf", ex -> {
            methodSink.set(ex.getRequestMethod());
            bodySink.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] out = body.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(status, out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
            }
        });
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }
}
