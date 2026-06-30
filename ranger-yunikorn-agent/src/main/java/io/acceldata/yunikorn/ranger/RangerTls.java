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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * TLS for HTTPS connections to Ranger Admin.
 *
 * <p>ODP Ranger Admin runs behind TLS with a certificate signed by an internal
 * CA the JRE does not trust by default, so a plain HTTPS connection fails with
 * {@code PKIX path building failed}. Point the agent at a truststore that
 * trusts Ranger's certificate and that goes away.
 *
 * <p>{@link #forConfig} returns {@code null} when nothing needs customizing
 * (no truststore and not insecure) — callers then use the JVM defaults, which
 * is correct for plain HTTP or a Ranger cert already trusted by the JRE.
 *
 * <ul>
 *   <li><b>truststore</b> ({@code ranger.ssl.truststore.path}) — the cert/CA
 *       chain the agent trusts for Ranger's server certificate.</li>
 *   <li><b>insecure</b> ({@code ranger.ssl.insecure=true}) — trust any cert and
 *       skip the hostname check, like {@code curl -k}. DEVELOPMENT ONLY.</li>
 * </ul>
 */
public final class RangerTls {

    private static final Logger LOG = LoggerFactory.getLogger(RangerTls.class);

    private final SSLSocketFactory socketFactory;
    /** Non-null only in insecure mode; null = default hostname verification. */
    private final HostnameVerifier hostnameVerifier;

    private RangerTls(SSLSocketFactory socketFactory, HostnameVerifier hostnameVerifier) {
        this.socketFactory    = socketFactory;
        this.hostnameVerifier = hostnameVerifier;
    }

    /**
     * @return TLS settings, or {@code null} when the JVM defaults suffice.
     * @throws IllegalArgumentException if the truststore file is unreadable
     * @throws IllegalStateException    if the truststore can't be loaded
     */
    static RangerTls forConfig(AgentConfig config) {
        if (!config.rangerSslInsecure() && config.rangerSslTruststorePath() == null) {
            return null;     // JVM default TLS (or plain HTTP) — nothing to do
        }
        try {
            TrustManager[]   trustManagers;
            HostnameVerifier verifier;
            if (config.rangerSslInsecure()) {
                LOG.warn("ranger.ssl.insecure=true — TLS certificate and hostname verification are " +
                        "DISABLED for Ranger Admin connections. Development only; configure " +
                        "ranger.ssl.truststore.path for production.");
                trustManagers = TRUST_ALL;
                verifier      = (hostname, session) -> true;   // accept any hostname
            } else {
                trustManagers = trustManagersFor(config);
                verifier      = null;                          // default verification
            }

            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, trustManagers, new SecureRandom());

            LOG.info("Ranger Admin TLS configured (truststore={}, insecure={})",
                    config.rangerSslTruststorePath() == null
                            ? "(JVM default)" : config.rangerSslTruststorePath(),
                    config.rangerSslInsecure());
            return new RangerTls(ctx.getSocketFactory(), verifier);
        } catch (RuntimeException e) {
            throw e;     // already a clear IllegalArgument from trustManagersFor
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to initialise TLS for Ranger Admin: " + e.getMessage() +
                    ". Check ranger.ssl.truststore.path, RANGER_SSL_TRUSTSTORE_PASSWORD, and " +
                    "ranger.ssl.truststore.type (JKS vs PKCS12).", e);
        }
    }

    /** Apply to a connection if it is HTTPS; a no-op on plain HTTP. */
    void apply(HttpURLConnection conn) {
        if (conn instanceof HttpsURLConnection) {
            HttpsURLConnection https = (HttpsURLConnection) conn;
            https.setSSLSocketFactory(socketFactory);
            if (hostnameVerifier != null) {
                https.setHostnameVerifier(hostnameVerifier);
            }
        }
    }

    /** Null-safe convenience: apply {@code tls} to {@code conn} if non-null. */
    static void applyIfPresent(HttpURLConnection conn, RangerTls tls) {
        if (tls != null) {
            tls.apply(conn);
        }
    }

    private static TrustManager[] trustManagersFor(AgentConfig config) throws Exception {
        String path = config.rangerSslTruststorePath();
        Path p = Path.of(path);
        if (!Files.isReadable(p)) {
            throw new IllegalArgumentException(
                    "Truststore not readable at '" + path + "' — verify the path and that the " +
                    "Secret/volume is mounted into the pod.");
        }
        String password = config.rangerSslTruststorePassword();
        KeyStore ts = KeyStore.getInstance(config.rangerSslTruststoreType());
        try (InputStream in = Files.newInputStream(p)) {
            ts.load(in, password == null ? null : password.toCharArray());
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        return tmf.getTrustManagers();
    }

    /** Accept-any-certificate trust manager. Insecure mode only. */
    private static final TrustManager[] TRUST_ALL = {
        new X509TrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] chain, String authType) { }
            @Override public void checkServerTrusted(X509Certificate[] chain, String authType) { }
            @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        }
    };
}
