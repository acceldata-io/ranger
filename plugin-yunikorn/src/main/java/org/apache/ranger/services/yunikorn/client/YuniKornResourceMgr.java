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

package org.apache.ranger.services.yunikorn.client;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.yunikorn.RangerYuniKornConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class YuniKornResourceMgr {

    private static final Logger LOG = LoggerFactory.getLogger(YuniKornResourceMgr.class);

    /**
     * Connectivity check invoked by {@code RangerServiceYuniKorn.validateConfig()}
     * when an admin saves the YuniKorn service form.
     */
    public static Map<String, Object> validateConfig(String serviceName,
                                                     Map<String, String> configs) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> YuniKornResourceMgr.validateConfig serviceName=[{}]", serviceName);
        }
        try {
            return YuniKornClient.connectionTest(serviceName, configs);
        } catch (Exception e) {
            LOG.error("<== YuniKornResourceMgr.validateConfig failed", e);
            throw e;
        }
    }

    /**
     * Resource-lookup entry invoked by {@code RangerServiceYuniKorn.lookupResource()}
     * when an admin types into the queue field in the policy editor.
     */
    public static List<String> getYuniKornResources(String serviceName,
                                                    Map<String, String> configs,
                                                    ResourceLookupContext context) {
        if (configs == null || configs.isEmpty()) {
            LOG.error("YuniKorn service [{}] has empty configuration; cannot lookup queues", serviceName);
            return Collections.emptyList();
        }

        String userInput = context.getUserInput();
        Map<String, List<String>> resourceMap = context.getResources();

        List<String> existing = null;
        if (resourceMap != null && resourceMap.get(RangerYuniKornConstants.RESOURCE_QUEUE) != null) {
            existing = resourceMap.get(RangerYuniKornConstants.RESOURCE_QUEUE);
        }

        YuniKornClient client = YuniKornConnectionMgr.getYuniKornClient(serviceName, configs);
        if (client == null) {
            return Collections.emptyList();
        }

        synchronized (client) {
            return client.getQueueList(userInput, existing);
        }
    }

    private YuniKornResourceMgr() {
        // utility class
    }
}
