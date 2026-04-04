/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.services.gravitino.client.auth;

import java.util.Locale;
import java.util.Map;

/**
 * Creates a {@link GravitinoAuth} implementation based on configuration.
 *
 * Supported:
 * - xstore.auth.type = bearer | basic | aksk | none
 * - gravitino.auth.type = bearer | basic | aksk | none
 *
 * Boot-time override (highest precedence):
 * - if env var RANGER_GRAVITINO_AUTH_TYPE is set to aksk (or alias), AccessKeySecretKeyAuth is used
 * - if env var RANGER_GRAVITINO_FORCE_AKSK is truthy, AccessKeySecretKeyAuth is used
 * - if both access/secret env vars are present, AccessKeySecretKeyAuth is used
 *
 * Backward compatibility:
 * - if auth.type is missing and auth.token.url is set (and not "none"/"noauth"), bearer auth is used
 */
public final class GravitinoAuthFactory {
    // type selector
    // TODO: remove gravitino.auth.* fallbacks once legacy configs are dropped.
    public static final String KEY_AUTH_TYPE = "gravitino.auth.type";
    public static final String KEY_AUTH_TYPE_XSTORE = "xstore.auth.type";

    // bearer (oauth client credentials) - keys owned by BearerTokenProvider
    public static final String KEY_TOKEN_URL = "gravitino.auth.token.url";
    public static final String KEY_TOKEN_URL_XSTORE = "xstore.auth.token.url";

    // basic
    public static final String KEY_BASIC_USERNAME = "gravitino.auth.basic.username";
    public static final String KEY_BASIC_PASSWORD = "gravitino.auth.basic.password";
    public static final String KEY_BASIC_USERNAME_XSTORE = "xstore.auth.basic.username";
    public static final String KEY_BASIC_PASSWORD_XSTORE = "xstore.auth.basic.password";
    // backward-compat / UI-friendly aliases (already present in current service-def)
    public static final String KEY_BASIC_USERNAME_ALIAS = "username";
    public static final String KEY_BASIC_PASSWORD_ALIAS = "password";
    public static final String DEFAULT_BASIC_USERNAME = "admin";
    public static final String DEFAULT_BASIC_PASSWORD = "password";

    // boot-time override (env)
    public static final String ENV_AUTH_TYPE_OVERRIDE = "RANGER_GRAVITINO_AUTH_TYPE";
    public static final String ENV_FORCE_AKSK = "RANGER_GRAVITINO_FORCE_AKSK";

    private GravitinoAuthFactory() {}

    public static GravitinoAuth create(String serviceName, Map<String, String> configs) {
        // Highest precedence: allow platform/helm to enforce auth mode at boot-time,
        // regardless of service configs set in Ranger.
        if (shouldForceAccessKeySecretKeyAuthFromEnv()) {
            return new AccessKeySecretKeyAuth();
        }

        String type = trimToNull(firstNonBlank(configs.get(KEY_AUTH_TYPE_XSTORE), configs.get(KEY_AUTH_TYPE)));
        if (type == null) {
            // legacy behavior: if token url is configured, assume bearer auth
            String tokenUrl = trimToNull(firstNonBlank(configs.get(KEY_TOKEN_URL_XSTORE), configs.get(KEY_TOKEN_URL)));
            if (tokenUrl != null && !isNoAuthValue(tokenUrl)) {
                return new BearerTokenAuth(serviceName, configs);
            }

            // weak heuristic: if basic username is set, assume basic auth
            String user = firstNonBlank(
                    configs.get(KEY_BASIC_USERNAME_XSTORE),
                    configs.get(KEY_BASIC_USERNAME),
                    configs.get(KEY_BASIC_USERNAME_ALIAS));
            if (user != null) {
                String pass = firstNonBlank(
                        configs.get(KEY_BASIC_PASSWORD_XSTORE),
                        configs.get(KEY_BASIC_PASSWORD),
                        configs.get(KEY_BASIC_PASSWORD_ALIAS));
                return new BasicAuth(user, pass != null ? pass : DEFAULT_BASIC_PASSWORD);
            }

            return new NoAuth();
        }

        String t = type.toLowerCase(Locale.ROOT).trim();
        switch (t) {
            case "bearer":
            case "token":
            case "oauth":
            case "oauth2":
                return new BearerTokenAuth(serviceName, configs);
            case "basic":
                String user = firstNonBlank(
                        configs.get(KEY_BASIC_USERNAME_XSTORE),
                        configs.get(KEY_BASIC_USERNAME),
                        configs.get(KEY_BASIC_USERNAME_ALIAS));
                String pass = firstNonBlank(
                        configs.get(KEY_BASIC_PASSWORD_XSTORE),
                        configs.get(KEY_BASIC_PASSWORD),
                        configs.get(KEY_BASIC_PASSWORD_ALIAS));
                return new BasicAuth(
                        user != null ? user : DEFAULT_BASIC_USERNAME,
                        pass != null ? pass : DEFAULT_BASIC_PASSWORD
                );
            case "aksk":
            case "accesskey":
            case "access_key":
            case "accesskeysecretkey":
            case "access_key_secret_key":
                return new AccessKeySecretKeyAuth();
            case "none":
            case "noauth":
                return new NoAuth();
            default:
                // safest default: don't send credentials unexpectedly
                return new NoAuth();
        }
    }

    private static boolean isNoAuthValue(String v) {
        String t = v == null ? null : v.trim().toLowerCase(Locale.ROOT);
        return t == null || t.isEmpty() || "none".equals(t) || "noauth".equals(t);
    }

    private static String trimToNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static String firstNonBlank(String a, String b) {
        return firstNonBlank(a, b, null);
    }

    private static String firstNonBlank(String a, String b, String c) {
        String ta = trimToNull(a);
        if (ta != null) {
            return ta;
        }
        String tb = trimToNull(b);
        if (tb != null) {
            return tb;
        }
        return trimToNull(c);
    }

    private static boolean shouldForceAccessKeySecretKeyAuthFromEnv() {
        String overrideType = trimToNull(System.getenv(ENV_AUTH_TYPE_OVERRIDE));
        if (overrideType != null) {
            String t = overrideType.toLowerCase(Locale.ROOT).trim();
            if (isAkskType(t)) {
                return true;
            }
        }

        if (isTruthy(System.getenv(ENV_FORCE_AKSK))) {
            return true;
        }

        // If accessKey/secretKey are provided via env, prefer them automatically.
        // This enables Helm secrets to configure auth without touching Ranger service configs.
        String accessKey = trimToNull(firstNonBlank(
                System.getenv(AccessKeySecretKeyAuth.ENV_ACCESS_KEY),
                System.getenv(AccessKeySecretKeyAuth.ENV_ACCESS_KEY_ALT)
        ));
        String secretKey = trimToNull(firstNonBlank(
                System.getenv(AccessKeySecretKeyAuth.ENV_SECRET_KEY),
                System.getenv(AccessKeySecretKeyAuth.ENV_SECRET_KEY_ALT)
        ));

        return accessKey != null && secretKey != null;
    }

    private static boolean isAkskType(String t) {
        switch (t) {
            case "aksk":
            case "accesskey":
            case "access_key":
            case "accesskeysecretkey":
            case "access_key_secret_key":
                return true;
            default:
                return false;
        }
    }

    private static boolean isTruthy(String v) {
        String t = trimToNull(v);
        if (t == null) {
            return false;
        }

        switch (t.toLowerCase(Locale.ROOT)) {
            case "1":
            case "true":
            case "yes":
            case "y":
            case "on":
                return true;
            default:
                return false;
        }
    }
}

