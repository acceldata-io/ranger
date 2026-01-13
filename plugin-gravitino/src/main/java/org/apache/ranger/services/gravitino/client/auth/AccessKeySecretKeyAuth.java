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

import java.net.HttpURLConnection;

/**
 * Custom header auth derived from accessKey/secretKey env vars.
 *
 * Header format (matches curl usage):
 * - accessKey: &lt;access-key-value&gt;
 * - secretKey: &lt;secret-key-value&gt;
 */
public final class AccessKeySecretKeyAuth implements GravitinoAuth {
    /**
     * Primary env var names (recommended for Helm charts).
     */
    public static final String ENV_ACCESS_KEY = "RANGER_GRAVITINO_ACCESS_KEY";
    public static final String ENV_SECRET_KEY = "RANGER_GRAVITINO_SECRET_KEY";

    /**
     * Backward/alternate env var names.
     */
    public static final String ENV_ACCESS_KEY_ALT = "GRAVITINO_ACCESS_KEY";
    public static final String ENV_SECRET_KEY_ALT = "GRAVITINO_SECRET_KEY";

    @Override
    public void apply(HttpURLConnection conn) {
        String accessKey = firstNonBlank(System.getenv(ENV_ACCESS_KEY), System.getenv(ENV_ACCESS_KEY_ALT));
        if (accessKey == null) {
            return;
        }

        String secretKey = firstNonBlank(System.getenv(ENV_SECRET_KEY), System.getenv(ENV_SECRET_KEY_ALT));
        if (secretKey == null) {
            return;
        }

        // Match header names used by the target service.
        conn.setRequestProperty("accessKey", accessKey);
        conn.setRequestProperty("secretKey", secretKey);
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
}

