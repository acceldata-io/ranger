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

package org.apache.ranger.services.mlflow.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.services.mlflow.RangerMlflowConstants;

/**
 * Per-service client cache for MLflow resource lookup and connection validation.
 * Rebuilds the cached client whenever {@code mlflow.url}, {@code username}, or
 * {@code password} changes.
 */
public final class MlflowConnectionMgr {

    private static final ConcurrentMap<String, CachedClient> CACHE =
            new ConcurrentHashMap<String, CachedClient>();

    public static MlflowClient getMlflowClient(String serviceName, Map<String, String> configs) {
        String key = serviceName == null ? "" : serviceName;
        String fingerprint = fingerprint(configs);
        CachedClient cached = CACHE.get(key);
        if (cached != null && fingerprint.equals(cached.fingerprint)) {
            return cached.client;
        }
        MlflowClient created = new MlflowClient(serviceName, configs);
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
        return StringUtils.defaultString(configs.get(RangerMlflowConstants.CONFIG_MLFLOW_URL))
                + '\0'
                + StringUtils.defaultString(configs.get(RangerMlflowConstants.CONFIG_USERNAME))
                + '\0'
                + StringUtils.defaultString(configs.get(RangerMlflowConstants.CONFIG_PASSWORD));
    }

    private static final class CachedClient {
        final String fingerprint;
        final MlflowClient client;

        CachedClient(String fingerprint, MlflowClient client) {
            this.fingerprint = fingerprint;
            this.client = client;
        }
    }

    private MlflowConnectionMgr() {
        // utility class
    }
}
