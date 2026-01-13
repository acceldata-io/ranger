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
 * - gravitino.auth.type = bearer | basic | none
 *
 * Backward compatibility:
 * - if gravitino.auth.type is missing and gravitino.auth.token.url is set (and not "none"/"noauth"), bearer auth is used
 */
public final class GravitinoAuthFactory {
    // type selector
    public static final String KEY_AUTH_TYPE = "gravitino.auth.type";

    // bearer (oauth client credentials) - keys owned by BearerTokenProvider
    public static final String KEY_TOKEN_URL = "gravitino.auth.token.url";

    // basic
    public static final String KEY_BASIC_USERNAME = "gravitino.auth.basic.username";
    public static final String KEY_BASIC_PASSWORD = "gravitino.auth.basic.password";
    // backward-compat / UI-friendly aliases (already present in current service-def)
    public static final String KEY_BASIC_USERNAME_ALIAS = "username";
    public static final String KEY_BASIC_PASSWORD_ALIAS = "password";
    public static final String DEFAULT_BASIC_USERNAME = "admin";
    public static final String DEFAULT_BASIC_PASSWORD = "password";

    private GravitinoAuthFactory() {}

    public static GravitinoAuth create(String serviceName, Map<String, String> configs) {
        String type = trimToNull(configs.get(KEY_AUTH_TYPE));
        if (type == null) {
            // legacy behavior: if token url is configured, assume bearer auth
            String tokenUrl = trimToNull(configs.get(KEY_TOKEN_URL));
            if (tokenUrl != null && !isNoAuthValue(tokenUrl)) {
                return new BearerTokenAuth(serviceName, configs);
            }

            // weak heuristic: if basic username is set, assume basic auth
            String user = firstNonBlank(configs.get(KEY_BASIC_USERNAME), configs.get(KEY_BASIC_USERNAME_ALIAS));
            if (user != null) {
                String pass = firstNonBlank(configs.get(KEY_BASIC_PASSWORD), configs.get(KEY_BASIC_PASSWORD_ALIAS));
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
                String user = firstNonBlank(configs.get(KEY_BASIC_USERNAME), configs.get(KEY_BASIC_USERNAME_ALIAS));
                String pass = firstNonBlank(configs.get(KEY_BASIC_PASSWORD), configs.get(KEY_BASIC_PASSWORD_ALIAS));
                return new BasicAuth(
                        user != null ? user : DEFAULT_BASIC_USERNAME,
                        pass != null ? pass : DEFAULT_BASIC_PASSWORD
                );
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
        String ta = trimToNull(a);
        return ta != null ? ta : trimToNull(b);
    }
}

