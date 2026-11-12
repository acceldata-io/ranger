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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * TLS customization for HTTPS connections to Ranger Admin and MLflow.
 */
final class RangerTls {

    private final SSLSocketFactory socketFactory;
    private final HostnameVerifier hostnameVerifier;

    private RangerTls(SSLSocketFactory socketFactory, HostnameVerifier hostnameVerifier) {
        this.socketFactory    = socketFactory;
        this.hostnameVerifier = hostnameVerifier;
    }

    static RangerTls forConfig(AgentConfig config) {
        if (config == null) {
            return null;
        }
        if (config.rangerSslInsecure()) {
            return buildInsecure();
        }
        if (config.rangerSslTruststorePath() != null) {
            return buildFromTruststore(
                    config.rangerSslTruststorePath(),
                    config.rangerSslTruststoreType(),
                    config.rangerSslTruststorePassword());
        }
        return null;
    }

    static void applyIfPresent(HttpURLConnection conn, RangerTls tls) {
        if (tls == null || !(conn instanceof HttpsURLConnection)) {
            return;
        }
        HttpsURLConnection https = (HttpsURLConnection) conn;
        if (tls.socketFactory != null) {
            https.setSSLSocketFactory(tls.socketFactory);
        }
        if (tls.hostnameVerifier != null) {
            https.setHostnameVerifier(tls.hostnameVerifier);
        }
    }

    private static RangerTls buildInsecure() {
        try {
            TrustManager[] trustAll = new TrustManager[] {
                new X509TrustManager() {
                    @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                    @Override public void checkClientTrusted(X509Certificate[] c, String a) { }
                    @Override public void checkServerTrusted(X509Certificate[] c, String a) { }
                }
            };
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, trustAll, new SecureRandom());
            return new RangerTls(ctx.getSocketFactory(), (hostname, session) -> true);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize insecure SSLContext", e);
        }
    }

    private static RangerTls buildFromTruststore(String path, String type, String password) {
        try {
            KeyStore ks = KeyStore.getInstance(type == null ? "JKS" : type);
            char[] pwd = password == null ? null : password.toCharArray();
            try (InputStream in = new FileInputStream(path)) {
                ks.load(in, pwd);
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, tmf.getTrustManagers(), new SecureRandom());
            return new RangerTls(ctx.getSocketFactory(), null);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load TLS truststore from " + path, e);
        }
    }
}
