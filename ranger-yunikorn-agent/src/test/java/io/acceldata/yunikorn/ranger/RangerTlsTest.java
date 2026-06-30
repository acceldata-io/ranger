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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Focused on the cases that guard real behaviour: that an unconfigured agent
 * leaves TLS on JVM defaults (protecting every non-TLS deployment), that the
 * new code never disturbs a plain-HTTP connection, and that a configured
 * truststore actually installs a custom socket factory.
 */
class RangerTlsTest {

    private static Function<String, String> emptyEnv() {
        return key -> null;
    }

    private static Function<String, String> envWith(String... kv) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i], kv[i + 1]);
        }
        return map::get;
    }

    private static Properties baseProps() {
        Properties p = new Properties();
        p.setProperty("ranger.admin.url",    "https://ranger.example.com:6182");
        p.setProperty("ranger.service.name", "test-yunikorn");
        return p;
    }

    /** Write an empty (but structurally valid) JKS store to disk and return its path. */
    private static Path writeEmptyJks(Path dir, String name, String password) throws Exception {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, password.toCharArray());     // initialise an empty store
        Path out = dir.resolve(name);
        try (OutputStream os = Files.newOutputStream(out)) {
            ks.store(os, password.toCharArray());
        }
        return out;
    }

    private static HttpsURLConnection httpsConn() throws Exception {
        return (HttpsURLConnection) URI.create("https://ranger.example.com:6182").toURL().openConnection();
    }

    @Test
    @DisplayName("no TLS config → forConfig returns null (every non-TLS deployment uses JVM defaults)")
    void nullWhenUnconfigured() {
        AgentConfig c = AgentConfig.fromProperties(baseProps(), emptyEnv());
        assertThat(RangerTls.forConfig(c)).isNull();
    }

    @Test
    @DisplayName("apply on a plain-HTTP connection is a no-op")
    void applyOnHttpIsNoop() throws Exception {
        Properties p = baseProps();
        p.setProperty("ranger.ssl.insecure", "true");     // any config that yields a non-null RangerTls
        RangerTls tls = RangerTls.forConfig(AgentConfig.fromProperties(p, emptyEnv()));
        assertThat(tls).isNotNull();

        HttpURLConnection httpConn =
                (HttpURLConnection) URI.create("http://ranger.example.com:6080").toURL().openConnection();
        assertThatCode(() -> tls.apply(httpConn)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("valid truststore → custom socket factory, default hostname verification")
    void truststoreLoads(@TempDir Path dir) throws Exception {
        Path ts = writeEmptyJks(dir, "truststore.jks", "changeit");
        Properties p = baseProps();
        p.setProperty("ranger.ssl.truststore.path", ts.toString());

        RangerTls tls = RangerTls.forConfig(AgentConfig.fromProperties(p,
                envWith("RANGER_SSL_TRUSTSTORE_PASSWORD", "changeit")));
        assertThat(tls).isNotNull();

        HttpsURLConnection conn = httpsConn();
        HostnameVerifier before = conn.getHostnameVerifier();
        tls.apply(conn);

        assertThat(conn.getSSLSocketFactory())
                .isNotSameAs(HttpsURLConnection.getDefaultSSLSocketFactory());
        // Non-insecure: hostname verifier is left at the connection default.
        assertThat(conn.getHostnameVerifier()).isSameAs(before);
    }
}
