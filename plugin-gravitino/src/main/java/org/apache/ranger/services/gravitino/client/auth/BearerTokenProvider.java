package org.apache.ranger.services.gravitino.client.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class BearerTokenProvider {
    private static final class TokenResponse {
        final String accessToken;
        final long expiresAtMs;

        TokenResponse(String accessToken, long expiresAtMs) {
            this.accessToken = accessToken;
            this.expiresAtMs = expiresAtMs;
        }
    }

    private static final class CachedToken {
        final String accessToken;
        final long expiresAtMs;

        CachedToken(String accessToken, long expiresAtMs) {
            this.accessToken = accessToken;
            this.expiresAtMs = expiresAtMs;
        }
    }
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // cache: per service+tokenUrl+clientId
    private static final ConcurrentHashMap<Key, CachedToken> CACHE = new ConcurrentHashMap<>();

    // defaults
    private static final long DEFAULT_EXPIRES_IN_SEC = 300;   // 5m fallback if token response lacks expires_in
    private static final long DEFAULT_SKEW_MS = 30_000;       // refresh 30s early
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 5000;
    private static final int DEFAULT_READ_TIMEOUT_MS = 5000;

    private BearerTokenProvider() {}

    /** Returns full header value: "Bearer <token>" */
    public static String getBearerHeader(String serviceName, Map<String, String> configs) {
        // TODO: remove gravitino.auth.* fallbacks once legacy configs are dropped.
        String tokenUrlRaw = trimToNull(firstNonBlank(
                configs.get("xstore.auth.token.url"),
                configs.get("gravitino.auth.token.url")
        ));

        // Local testing escape hatch: skip auth entirely
        if (tokenUrlRaw == null || "none".equalsIgnoreCase(tokenUrlRaw) || "noauth".equalsIgnoreCase(tokenUrlRaw)) {
            return null;
        }

        String tokenUrl = must(configs, "xstore.auth.token.url", "gravitino.auth.token.url");
        String clientId = must(configs, "xstore.auth.client.id", "gravitino.auth.client.id");
        String clientSecret = must(configs, "xstore.auth.client.secret", "gravitino.auth.client.secret");
        String scope = trimToNull(firstNonBlank(
                configs.get("xstore.auth.scope"),
                configs.get("gravitino.auth.scope")
        ));

        long skewMs = parseLong(firstNonBlank(
                configs.get("xstore.auth.token.cache.skew.ms"),
                configs.get("gravitino.auth.token.cache.skew.ms")
        ), DEFAULT_SKEW_MS);
        int connectTimeoutMs = (int) parseLong(firstNonBlank(
                configs.get("xstore.auth.token.connect.timeout.ms"),
                configs.get("gravitino.auth.token.connect.timeout.ms")
        ), DEFAULT_CONNECT_TIMEOUT_MS);
        int readTimeoutMs = (int) parseLong(firstNonBlank(
                configs.get("xstore.auth.token.read.timeout.ms"),
                configs.get("gravitino.auth.token.read.timeout.ms")
        ), DEFAULT_READ_TIMEOUT_MS);

        Key key = new Key(serviceName, tokenUrl, clientId);

        long now = System.currentTimeMillis();
        CachedToken cached = CACHE.get(key);
        if (cached != null && cached.expiresAtMs - skewMs > now) {
            return "Bearer " + cached.accessToken;
        }

        // Prevent token stampede
        synchronized (key.lock) {
            cached = CACHE.get(key);
            now = System.currentTimeMillis();
            if (cached != null && cached.expiresAtMs - skewMs > now) {
                return "Bearer " + cached.accessToken;
            }

            TokenResponse fresh = fetchClientCredentialsToken(tokenUrl, clientId, clientSecret, scope, connectTimeoutMs, readTimeoutMs);
            CACHE.put(key, new CachedToken(fresh.accessToken, fresh.expiresAtMs));
            return "Bearer " + fresh.accessToken;
        }
    }

    private static TokenResponse fetchClientCredentialsToken(
            String tokenUrl,
            String clientId,
            String clientSecret,
            String scope,
            int connectTimeoutMs,
            int readTimeoutMs
    ) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(tokenUrl).openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(connectTimeoutMs);
            conn.setReadTimeout(readTimeoutMs);
            conn.setDoOutput(true);

            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            conn.setRequestProperty("Accept", "application/json");

            // simplest: client_id/client_secret in form body
            String body = "grant_type=client_credentials"
                    + "&client_id=" + enc(clientId)
                    + "&client_secret=" + enc(clientSecret)
                    + (scope != null ? "&scope=" + enc(scope) : "");

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }

            int code = conn.getResponseCode();
            try (InputStream in = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream()) {
                if (code < 200 || code >= 300) {
                    String err = (in != null) ? slurp(in) : "";
                    throw new RuntimeException("Token endpoint HTTP " + code + ": " + err);
                }

                JsonNode root = MAPPER.readTree(in);
                String accessToken = text(root, "access_token");
                if (accessToken == null || accessToken.isEmpty()) {
                    throw new RuntimeException("Token response missing access_token");
                }

                long expiresInSec = root.path("expires_in").asLong(DEFAULT_EXPIRES_IN_SEC);
                long expiresAtMs = System.currentTimeMillis() + (expiresInSec * 1000L);

                return new TokenResponse(accessToken, expiresAtMs);
            }
        } catch (Exception e) {
            HadoopException he = new HadoopException("Unable to fetch bearer token", e);
            he.generateResponseDataMap(false, BaseClient.getMessage(e), "Unable to fetch bearer token", null, "xstore.auth.token.url");
            throw he;
        }
    }

    private static String enc(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String slurp(InputStream in) throws IOException {
        if (in == null) return "";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) >= 0) {
            out.write(buf, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v != null && v.isTextual()) ? v.asText() : null;
    }

    private static String must(Map<String, String> configs, String key, String fallbackKey) {
        String v = trimToNull(configs.get(key));
        if (v == null) {
            v = trimToNull(configs.get(fallbackKey));
        }
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing config: " + key + " (or " + fallbackKey + ")");
        }
        return v.trim();
    }

    private static String firstNonBlank(String a, String b) {
        String ta = trimToNull(a);
        return ta != null ? ta : trimToNull(b);
    }

    private static String trimToNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static long parseLong(String v, long def) {
        if (v == null) return def;
        try { return Long.parseLong(v.trim()); } catch (Exception e) { return def; }
    }

    private static final class Key {
        final String serviceName;
        final String tokenUrl;
        final String clientId;
        final Object lock = new Object();

        Key(String serviceName, String tokenUrl, String clientId) {
            this.serviceName = serviceName;
            this.tokenUrl = tokenUrl;
            this.clientId = clientId;
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key)) return false;
            Key k = (Key) o;
            return Objects.equals(serviceName, k.serviceName)
                    && Objects.equals(tokenUrl, k.tokenUrl)
                    && Objects.equals(clientId, k.clientId);
        }
        @Override public int hashCode() {
            return Objects.hash(serviceName, tokenUrl, clientId);
        }
    }
}