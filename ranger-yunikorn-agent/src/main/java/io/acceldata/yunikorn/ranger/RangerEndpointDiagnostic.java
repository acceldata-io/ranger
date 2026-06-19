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

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * One-shot startup diagnostic that prints the exact URL the agent will hit
 * on Ranger Admin, plus the response status and a curl-equivalent command
 * the operator can paste into a shell to reproduce.
 *
 * <p>This is a deliberate redundant call (the {@link PolicySyncService} will
 * make the same request a moment later via {@link
 * org.apache.ranger.admin.client.RangerAdminRESTClient}). Its only purpose
 * is to make a normally-opaque interaction visible: what URL? what status?
 * what's in the response body? Without this, "Unauthenticated access not
 * allowed" is all you ever see, with no way to distinguish "wrong URL" from
 * "wrong creds" from "wrong service-type" from a Ranger-side rule.
 *
 * <p>If the diagnostic itself fails (network down, 401, 404), the agent
 * still continues — the failure is logged but never fatal. A real sync
 * cycle will retry shortly.
 *
 * <p>Skipped when no credentials are configured (the agent's main path
 * also still works in that mode; we just don't add the diagnostic noise).
 */
public final class RangerEndpointDiagnostic {

    private static final Logger LOG = LoggerFactory.getLogger(RangerEndpointDiagnostic.class);

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS    = 10_000;

    /**
     * Build the same URL the Ranger client will use, log it, attempt one
     * GET, log the result. Never throws — diagnostic best-effort.
     */
    public static void run(AgentConfig config) {
        try {
            String url = buildPolicyDownloadUrl(config);
            LOG.info("=========================================================================");
            LOG.info("Ranger endpoint diagnostic (one-shot at startup)");
            LOG.info("  URL: {}", url);
            LOG.info("  curl-equivalent:");
            LOG.info("    {}", curlEquivalent(url, config));
            LOG.info("=========================================================================");

            HttpURLConnection conn = (HttpURLConnection) new URI(url).toURL().openConnection();
            // Same TLS material the real sync client uses, so an SSL-enabled
            // Ranger is reachable here too (and a handshake failure surfaces
            // as a clear TLS error rather than being mistaken for auth).
            RangerTls.applyIfPresent(conn, RangerTls.forConfig(config));
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            conn.setRequestProperty("Accept", "application/json");
            if (config.hasRangerCredentials()) {
                conn.setRequestProperty("Authorization", basicAuthHeader(config));
            }

            int status = conn.getResponseCode();
            String body = readBody(conn, status);
            LOG.info("Diagnostic response: status={}, bodyLen={}", status, body.length());
            if (status >= 400) {
                LOG.warn("Diagnostic body (truncated to 1000 chars): {}",
                        body.length() > 1000 ? body.substring(0, 1000) + "..." : body);
            } else {
                LOG.info("Diagnostic OK — first 200 chars of body: {}",
                        body.length() > 200 ? body.substring(0, 200) + "..." : body);
            }
        } catch (Throwable t) {
            LOG.warn("Ranger endpoint diagnostic failed (non-fatal). Sync will still attempt.", t);
        }
    }

    /**
     * Replicates the URL shape used by RangerAdminRESTClient when the agent
     * is configured with credentials: the {@code /service/plugins/secure/...}
     * path, which is the one Ranger's auth filter accepts non-Kerberos
     * credentials on.
     *
     * <p>When no credentials are configured, the diagnostic still uses
     * {@code /secure/} for symmetry with what the agent actually sends —
     * if Ranger rejects on {@code /secure/} without auth, the diagnostic
     * surfaces the same rejection.
     *
     * <p>Path: /service/plugins/secure/policies/download/{serviceName}
     * <p>Query: ?lastKnownVersion=-1&lastActivationTime=0&pluginId={type}@{host}-{name}
     */
    static String buildPolicyDownloadUrl(AgentConfig config) {
        String base = config.rangerAdminUrl().replaceAll("/+$", "");
        String path = "/service/plugins/secure/policies/download/" +
                URLEncoder.encode(config.rangerServiceName(), StandardCharsets.UTF_8);

        String pluginId = pluginId(config);
        String query =
                "lastKnownVersion=-1" +
                "&lastActivationTime=0" +
                "&pluginId=" + URLEncoder.encode(pluginId, StandardCharsets.UTF_8);

        return base + path + "?" + query;
    }

    /**
     * Approximation of the pluginId Ranger plugins use:
     *   {serviceType}@{hostname}-{serviceName}
     */
    static String pluginId(AgentConfig config) {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Throwable t) {
            host = "unknown";
        }
        return config.rangerServiceType() + "@" + host + "-" + config.rangerServiceName();
    }

    private static String curlEquivalent(String url, AgentConfig config) {
        // -k only when the agent itself is in insecure mode; otherwise the
        // operator should reproduce with the real CA (curl --cacert <pem>).
        StringBuilder sb = new StringBuilder(config.rangerSslInsecure() ? "curl -k -v" : "curl -v");
        if (config.hasRangerCredentials()) {
            // Don't print the actual password — show the env var
            sb.append(" -u \"$RANGER_PRINCIPAL:$RANGER_CREDENTIAL\"");
        }
        sb.append(" \"").append(url).append("\"");
        return sb.toString();
    }

    private static String basicAuthHeader(AgentConfig config) {
        String creds = config.rangerUser() + ":" + config.rangerPassword();
        return "Basic " + Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8));
    }

    private static String readBody(HttpURLConnection conn, int status) {
        try (java.io.InputStream is = (status >= 400)
                ? conn.getErrorStream()
                : conn.getInputStream()) {
            if (is == null) return "";
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Throwable t) {
            return "<failed to read body: " + t.getMessage() + ">";
        }
    }

    private RangerEndpointDiagnostic() {
        // utility
    }
}
