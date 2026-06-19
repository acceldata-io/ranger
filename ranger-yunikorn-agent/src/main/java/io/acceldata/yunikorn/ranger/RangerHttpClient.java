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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * Direct HTTP client to Ranger Admin's policy-download endpoint, bypassing
 * Ranger's own {@code RangerAdminRESTClient}.
 *
 * <p><b>Why we don't use the official client:</b> the version we're linked
 * against insists on hitting {@code /service/plugins/policies/download/...}
 * with Hadoop UGI in {@code SIMPLE} mode, which Ranger Admin's
 * {@code RangerSecurityContextFormationFilter} rejects with the misleading
 * 400 "Unauthenticated access not allowed" — even when our basic-auth
 * headers are correctly attached. The {@code /service/plugins/secure/...}
 * variant accepts basic auth happily, but the official client only switches
 * onto that path when UGI thinks Kerberos is in use, and getting it there
 * consistently across Ranger versions has proven brittle.
 *
 * <p>This class skips all that. It builds the URL ourselves, sends
 * {@code Authorization: Basic} ourselves, parses the {@code ServicePolicies}
 * JSON response with Jackson. ~80 lines of HTTP plus ~20 lines of JSON
 * registration to handle the few non-trivial Ranger types that show up in
 * the response.
 *
 * <h2>What we don't implement</h2>
 * The {@link RangerAdminClient} interface is wide — roles, tags, grant/revoke,
 * user-store, GDS — but the agent only ever calls
 * {@link #getServicePoliciesIfUpdated(long, long)}. Every other method
 * throws {@link UnsupportedOperationException}. If we ever need them, we
 * add them then.
 *
 * <h2>Trade-offs vs. the official client</h2>
 * <ul>
 *   <li><b>No delta protocol.</b> Each call returns the full policy set;
 *       we don't get incremental updates. Fine for v1 — policies are tiny.</li>
 *   <li><b>No client-side cache.</b> The official client persists a local
 *       JSON cache. We rely on the in-memory {@code lastKnownVersion}
 *       check in {@link PolicySyncService}. Restart loses nothing the
 *       next sync wouldn't refetch.</li>
 *   <li><b>No HA URL list.</b> One Ranger Admin URL only. If the customer
 *       runs Ranger HA we'd extend the URL field to a list.</li>
 * </ul>
 */
public class RangerHttpClient implements RangerAdminClient {

    private static final Logger LOG = LoggerFactory.getLogger(RangerHttpClient.class);

    static final int DEFAULT_CONNECT_TIMEOUT_MS = 30_000;
    static final int DEFAULT_READ_TIMEOUT_MS    = 30_000;

    private final String  rangerAdminUrl;
    private final String  rangerServiceName;
    private final String  rangerServiceType;
    private final String  basicAuthHeader;        // non-null in BASIC mode with creds
    private final SpnegoAuthenticator spnegoAuthenticator;  // non-null in KERBEROS mode
    private final String  hostname;
    private final ObjectMapper mapper;
    /** TLS settings for HTTPS Ranger Admin; null when JVM defaults suffice (or plain HTTP). */
    private final RangerTls tls;

    public RangerHttpClient(AgentConfig config) {
        this.rangerAdminUrl    = config.rangerAdminUrl().replaceAll("/+$", "");
        this.rangerServiceName = config.rangerServiceName();
        this.rangerServiceType = config.rangerServiceType();
        this.hostname          = resolveHostname();
        this.mapper            = configuredMapper();
        this.tls               = RangerTls.forConfig(config);

        if (config.authMode() == AgentConfig.AuthMode.KERBEROS) {
            this.basicAuthHeader = null;
            try {
                this.spnegoAuthenticator = new SpnegoAuthenticator();
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to initialise SPNEGO authenticator. Verify: " +
                        "kerberos.principal exists in the KDC, kerberos.keytab.path is " +
                        "readable, java.security.auth.login.config points at the JAAS " +
                        "file the agent generates at startup, and krb5.conf has a valid " +
                        "default_realm.", e);
            }
            LOG.info("RangerHttpClient configured for serviceType='{}' with SPNEGO/Kerberos " +
                            "(principal='{}'); URL: {}/service/plugins/secure/policies/download/{}",
                    rangerServiceType, config.kerberosPrincipal(),
                    rangerAdminUrl, rangerServiceName);
        } else if (config.hasRangerCredentials()) {
            this.basicAuthHeader = buildBasicAuthHeader(config.rangerUser(), config.rangerPassword());
            this.spnegoAuthenticator = null;
            LOG.info("RangerHttpClient configured for serviceType='{}' with basic auth (user='{}'); " +
                    "URL: {}/service/plugins/secure/policies/download/{}",
                    rangerServiceType, config.rangerUser(), rangerAdminUrl, rangerServiceName);
        } else {
            this.basicAuthHeader = null;
            this.spnegoAuthenticator = null;
            LOG.warn("RangerHttpClient configured for serviceType='{}' with NO authentication.",
                    rangerServiceType);
        }
    }

    // -----------------------------------------------------------------------
    // The one method that matters
    // -----------------------------------------------------------------------

    @Override
    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion,
                                                       long lastActivationTimeInMillis)
            throws Exception {
        String url = buildUrl(lastKnownVersion, lastActivationTimeInMillis);

        URI uri = URI.create(url);
        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        // Apply TLS material before the handshake (no-op on plain HTTP).
        RangerTls.applyIfPresent(conn, tls);
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(DEFAULT_READ_TIMEOUT_MS);
        conn.setRequestProperty("Accept", "application/json");

        if (basicAuthHeader != null) {
            conn.setRequestProperty("Authorization", basicAuthHeader);
        } else if (spnegoAuthenticator != null) {
            // Build a fresh SPNEGO token for this request. Service tickets
            // are short-lived but the GSS layer caches them inside the
            // Subject, so subsequent calls reuse the cached ticket until
            // it expires.
            String header;
            try {
                header = spnegoAuthenticator.buildAuthorizationHeader(uri);
            } catch (Exception e) {
                throw new IOException("Failed to build SPNEGO Authorization header for " + url, e);
            }
            conn.setRequestProperty("Authorization", header);
        }
        // Else: no auth (development / network-protected Ranger)

        int status;
        try {
            status = conn.getResponseCode();
        } catch (IOException e) {
            throw new IOException("Failed to connect to Ranger Admin at " + url, e);
        }

        // 304 Not Modified — Ranger uses 304 to mean "no policy version
        // change since lastKnownVersion." Returning null tells PolicySync
        // to skip this cycle, exactly like the official client does.
        if (status == 304) {
            LOG.debug("Ranger returned 304 Not Modified — no policy changes since version {}",
                    lastKnownVersion);
            return null;
        }

        if (status != 200) {
            String body = readBody(conn, status);
            throw new IOException("Ranger Admin returned " + status + " from " + url +
                    " — body: " + truncate(body, 500));
        }

        try (InputStream in = conn.getInputStream()) {
            return mapper.readValue(in, ServicePolicies.class);
        }
    }

    String buildUrl(long lastKnownVersion, long lastActivationTimeInMillis) {
        String pluginId = rangerServiceType + "@" + hostname + "-" + rangerServiceName;
        return rangerAdminUrl
                + "/service/plugins/secure/policies/download/"
                + URLEncoder.encode(rangerServiceName, StandardCharsets.UTF_8)
                + "?lastKnownVersion=" + lastKnownVersion
                + "&lastActivationTime=" + lastActivationTimeInMillis
                + "&pluginId=" + URLEncoder.encode(pluginId, StandardCharsets.UTF_8);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static String buildBasicAuthHeader(String user, String pass) {
        String creds = user + ":" + pass;
        return "Basic " + Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8));
    }

    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Throwable t) {
            return "unknown";
        }
    }

    /**
     * Jackson mapper configured to:
     * <ul>
     *   <li>ignore unknown JSON properties (Ranger's response includes a
     *       large number of nested fields we don't model)</li>
     *   <li>tolerate {@code null} for primitive fields some Ranger
     *       versions return as null instead of omitting</li>
     * </ul>
     */
    private static ObjectMapper configuredMapper() {
        ObjectMapper m = new ObjectMapper();
        m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        m.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        // Defensive: register a mixin that lets ServicePolicies tolerate
        // Ranger response variations across versions.
        m.addMixIn(ServicePolicies.class, IgnoreUnknownMixin.class);
        return m;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private abstract static class IgnoreUnknownMixin { }

    private static String readBody(HttpURLConnection conn, int status) {
        try (InputStream in = (status >= 400) ? conn.getErrorStream() : conn.getInputStream()) {
            if (in == null) return "";
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Throwable t) {
            return "<failed to read body: " + t.getMessage() + ">";
        }
    }

    private static String truncate(String s, int max) {
        return (s == null || s.length() <= max) ? s : s.substring(0, max) + "...";
    }

    // -----------------------------------------------------------------------
    // Interface methods we don't use. Throwing rather than no-op so any
    // accidental future caller gets an obvious error.
    // -----------------------------------------------------------------------

    @Override public void init(String s, String s1, String s2, Configuration c) { /* nothing to do */ }

    @Override public RangerRoles    getRolesIfUpdated(long a, long b)            { return null; }
    @Override public ServiceTags    getServiceTagsIfUpdated(long a, long b)      { return null; }
    @Override public RangerUserStore getUserStoreIfUpdated(long a, long b)       { return null; }
    @Override public ServiceGdsInfo getGdsInfoIfUpdated(long a, long b)          { return null; }

    @Override public RangerRole createRole(RangerRole r)        { throw unsupported("createRole"); }
    @Override public void       dropRole(String s, String t)    { throw unsupported("dropRole"); }
    @Override public List<String> getAllRoles(String s)         { throw unsupported("getAllRoles"); }
    @Override public List<String> getUserRoles(String s)        { throw unsupported("getUserRoles"); }
    @Override public RangerRole getRole(String s, String t)     { throw unsupported("getRole"); }
    @Override public void       grantRole(GrantRevokeRoleRequest r)  { throw unsupported("grantRole"); }
    @Override public void       revokeRole(GrantRevokeRoleRequest r) { throw unsupported("revokeRole"); }
    @Override public void       grantAccess(GrantRevokeRequest r)    { throw unsupported("grantAccess"); }
    @Override public void       revokeAccess(GrantRevokeRequest r)   { throw unsupported("revokeAccess"); }
    @Override public List<String> getTagTypes(String s)              { throw unsupported("getTagTypes"); }

    private static UnsupportedOperationException unsupported(String op) {
        return new UnsupportedOperationException(
                "RangerHttpClient does not implement '" + op + "'. " +
                "If the agent grew a need for it, switch to the official " +
                "RangerAdminRESTClient or extend RangerHttpClient.");
    }
}
