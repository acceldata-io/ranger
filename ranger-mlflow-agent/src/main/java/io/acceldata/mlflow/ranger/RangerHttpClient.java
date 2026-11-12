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
import org.apache.ranger.plugin.util.ServiceRMSMappings;
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
 * HTTP client to Ranger Admin that downloads:
 * <ul>
 *   <li>{@link ServicePolicies} from {@code /service/plugins/secure/policies/download/{service}}</li>
 *   <li>{@link RangerUserStore} from {@code /service/xusers/secure/download/{service}} (for group membership)</li>
 *   <li>{@link RangerRoles} from {@code /service/roles/secure/download/{service}} (for Ranger role definitions)</li>
 * </ul>
 *
 * <p>Because MLflow 3.16.0's native RBAC models access via {@code Role}s and
 * {@code UserRoleAssignment}s (without a native group table), downloading both
 * {@code ServicePolicies} and {@code RangerUserStore}/{@code RangerRoles} lets
 * {@link RbacConverter} materialize Ranger groups and Ranger roles as first-class
 * MLflow roles and bind their member users automatically.
 */
public class RangerHttpClient implements RangerAdminClient {

    private static final Logger LOG = LoggerFactory.getLogger(RangerHttpClient.class);

    static final int DEFAULT_CONNECT_TIMEOUT_MS = 30_000;
    static final int DEFAULT_READ_TIMEOUT_MS    = 30_000;

    private final String              rangerAdminUrl;
    private final String              rangerServiceName;
    private final String              rangerServiceType;
    private final String              basicAuthHeader;
    private final SpnegoAuthenticator spnegoAuthenticator;
    private final String              hostname;
    private final ObjectMapper        mapper;
    private final RangerTls           tls;

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
                throw new IllegalStateException("Failed to initialize SPNEGO authenticator", e);
            }
        } else if (config.hasRangerCredentials()) {
            this.basicAuthHeader     = buildBasicAuthHeader(config.rangerUser(), config.rangerPassword());
            this.spnegoAuthenticator = null;
        } else {
            this.basicAuthHeader     = null;
            this.spnegoAuthenticator = null;
        }
    }

    @Override
    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion,
                                                       long lastActivationTimeInMillis) throws Exception {
        String url = buildPolicyDownloadUrl(lastKnownVersion, lastActivationTimeInMillis);
        return fetchIfUpdated(url, lastKnownVersion, ServicePolicies.class);
    }

    @Override
    public RangerUserStore getUserStoreIfUpdated(long lastKnownUserStoreVersion,
                                                 long lastActivationTimeInMillis) throws Exception {
        String url = buildUserStoreDownloadUrl(lastKnownUserStoreVersion, lastActivationTimeInMillis);
        return fetchIfUpdated(url, lastKnownUserStoreVersion, RangerUserStore.class);
    }

    @Override
    public RangerRoles getRolesIfUpdated(long lastKnownRoleVersion,
                                         long lastActivationTimeInMillis) throws Exception {
        String url = buildRolesDownloadUrl(lastKnownRoleVersion, lastActivationTimeInMillis);
        return fetchIfUpdated(url, lastKnownRoleVersion, RangerRoles.class);
    }

    String buildPolicyDownloadUrl(long lastKnownVersion, long lastActivationTimeInMillis) {
        return rangerAdminUrl
                + "/service/plugins/secure/policies/download/"
                + URLEncoder.encode(rangerServiceName, StandardCharsets.UTF_8)
                + "?lastKnownVersion=" + lastKnownVersion
                + "&lastActivationTime=" + lastActivationTimeInMillis
                + "&pluginId=" + URLEncoder.encode(pluginId(), StandardCharsets.UTF_8);
    }

    String buildUserStoreDownloadUrl(long lastKnownUserStoreVersion, long lastActivationTimeInMillis) {
        return rangerAdminUrl
                + "/service/xusers/secure/download/"
                + URLEncoder.encode(rangerServiceName, StandardCharsets.UTF_8)
                + "?lastKnownUserStoreVersion=" + lastKnownUserStoreVersion
                + "&lastActivationTime=" + lastActivationTimeInMillis
                + "&pluginId=" + URLEncoder.encode(pluginId(), StandardCharsets.UTF_8);
    }

    String buildRolesDownloadUrl(long lastKnownRoleVersion, long lastActivationTimeInMillis) {
        return rangerAdminUrl
                + "/service/roles/secure/download/"
                + URLEncoder.encode(rangerServiceName, StandardCharsets.UTF_8)
                + "?lastKnownRoleVersion=" + lastKnownRoleVersion
                + "&lastActivationTime=" + lastActivationTimeInMillis
                + "&pluginId=" + URLEncoder.encode(pluginId(), StandardCharsets.UTF_8);
    }

    private String pluginId() {
        return rangerServiceType + "@" + hostname + "-" + rangerServiceName;
    }

    private <T> T fetchIfUpdated(String url, long lastKnownVersion, Class<T> responseType) throws Exception {
        URI uri = URI.create(url);
        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        try {
            RangerTls.applyIfPresent(conn, tls);
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(DEFAULT_READ_TIMEOUT_MS);
            conn.setRequestProperty("Accept", "application/json");

            if (basicAuthHeader != null) {
                conn.setRequestProperty("Authorization", basicAuthHeader);
            } else if (spnegoAuthenticator != null) {
                conn.setRequestProperty("Authorization", spnegoAuthenticator.buildAuthorizationHeader(uri));
            }

            int status = conn.getResponseCode();
            if (status == 304) {
                LOG.debug("Ranger returned 304 Not Modified for {} (version={})", responseType.getSimpleName(), lastKnownVersion);
                return null;
            }
            if (status != 200) {
                String body = readBody(conn, status);
                throw new IOException("Ranger Admin returned " + status + " from " + url
                        + " — body: " + truncate(body, 500));
            }
            try (InputStream in = conn.getInputStream()) {
                return mapper.readValue(in, responseType);
            }
        } finally {
            conn.disconnect();
        }
    }

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

    private static ObjectMapper configuredMapper() {
        ObjectMapper m = new ObjectMapper();
        m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        m.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        m.addMixIn(ServicePolicies.class, IgnoreUnknownMixin.class);
        m.addMixIn(RangerUserStore.class, IgnoreUnknownMixin.class);
        m.addMixIn(RangerRoles.class, IgnoreUnknownMixin.class);
        return m;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private abstract static class IgnoreUnknownMixin { }

    private static String readBody(HttpURLConnection conn, int status) {
        try (InputStream in = (status >= 400) ? conn.getErrorStream() : conn.getInputStream()) {
            if (in == null) {
                return "";
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Throwable t) {
            return "<failed to read body: " + t.getMessage() + ">";
        }
    }

    private static String truncate(String s, int max) {
        return (s == null || s.length() <= max) ? s : s.substring(0, max) + "...";
    }

    @Override public void init(String s, String s1, String s2, Configuration c) { }
    @Override public ServiceTags getServiceTagsIfUpdated(long a, long b)        { return null; }
    @Override public ServiceGdsInfo getGdsInfoIfUpdated(long a, long b)         { return null; }
    @Override public ServiceRMSMappings getRMSMappings(String s, Long v)        { return null; }

    @Override public RangerRole createRole(RangerRole r)               { throw unsupported("createRole"); }
    @Override public void dropRole(String s, String t)                 { throw unsupported("dropRole"); }
    @Override public List<String> getAllRoles(String s)                { throw unsupported("getAllRoles"); }
    @Override public List<String> getUserRoles(String s)               { throw unsupported("getUserRoles"); }
    @Override public RangerRole getRole(String s, String t)            { throw unsupported("getRole"); }
    @Override public void grantRole(GrantRevokeRoleRequest r)          { throw unsupported("grantRole"); }
    @Override public void revokeRole(GrantRevokeRoleRequest r)         { throw unsupported("revokeRole"); }
    @Override public void grantAccess(GrantRevokeRequest r)            { throw unsupported("grantAccess"); }
    @Override public void revokeAccess(GrantRevokeRequest r)           { throw unsupported("revokeAccess"); }
    @Override public List<String> getTagTypes(String s)                { throw unsupported("getTagTypes"); }

    private static UnsupportedOperationException unsupported(String op) {
        return new UnsupportedOperationException("RangerHttpClient does not implement '" + op + "'");
    }
}
