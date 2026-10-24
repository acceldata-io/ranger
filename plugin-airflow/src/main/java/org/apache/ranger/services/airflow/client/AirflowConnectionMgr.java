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

package org.apache.ranger.services.airflow.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.services.airflow.RangerAirflowConstants;

/**
 * Per-service client cache so autocomplete reuses a JWT instead of minting
 * one on every keystroke. A change to url / username / password replaces
 * the cached client.
 */
public final class AirflowConnectionMgr {

    private static final ConcurrentMap<String, CachedClient> CACHE =
            new ConcurrentHashMap<String, CachedClient>();

    public static AirflowClient getAirflowClient(String serviceName, Map<String, String> configs) {
        String key = serviceName == null ? "" : serviceName;
        String fingerprint = fingerprint(configs);
        CachedClient cached = CACHE.get(key);
        if (cached != null && fingerprint.equals(cached.fingerprint)) {
            return cached.client;
        }
        AirflowClient created = new AirflowClient(serviceName, configs);
        CACHE.put(key, new CachedClient(fingerprint, created));
        return created;
    }

    static void clearCache() {
        CACHE.clear();
    }

    private static String fingerprint(Map<String, String> configs) {
        if (configs == null) {
            return "";
        }
        return StringUtils.defaultString(configs.get(RangerAirflowConstants.CONFIG_AIRFLOW_URL))
                + '\0'
                + StringUtils.defaultString(configs.get(RangerAirflowConstants.CONFIG_USERNAME))
                + '\0'
                + StringUtils.defaultString(configs.get(RangerAirflowConstants.CONFIG_PASSWORD));
    }

    private static final class CachedClient {
        final String fingerprint;
        final AirflowClient client;

        CachedClient(String fingerprint, AirflowClient client) {
            this.fingerprint = fingerprint;
            this.client = client;
        }
    }

    private AirflowConnectionMgr() {
        // utility class
    }
}
