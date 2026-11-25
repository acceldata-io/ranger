/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.plugin.auth.oauth2;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Test stand-in for Pinot's ServiceTokens (same entry points): the tests copy it into a jar of its own, like Pinot's
 * token-source jars next to the plug-in. A token for source java with the right secret, else Pinot-like errors.
 */
public final class ServiceTokens {
    private ServiceTokens() {
    }

    public static String fetch(Map<String, String> settings) {
        if (!"java".equals(settings.get("source"))) {
            throw new IllegalArgumentException("Token source " + settings.get("source") + ": unsupported source");
        }

        // Like Pinot's HTTP client in a JVM whose SSL defaults are broken: set only in the test's own JVM
        if (Boolean.getBoolean("test.broken.ssl.defaults")) {
            throw new IllegalStateException(new java.io.IOException(new java.security.NoSuchAlgorithmException("Error constructing implementation (algorithm: Default)")));
        }

        if (!"com.example.IdmTokenSource".equals(settings.get("class")) || !"s3cret".equals(settings.get("client.secret"))) {
            throw new IllegalStateException("Token source java: token endpoint answered 401");
        }

        return "tok-1";
    }

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();

        properties.load(new InputStreamReader(System.in, StandardCharsets.UTF_8));

        Map<String, String> settings = new HashMap<>();

        properties.stringPropertyNames().forEach(name -> settings.put(name, properties.getProperty(name)));

        try {
            System.out.println(fetch(settings));
        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
