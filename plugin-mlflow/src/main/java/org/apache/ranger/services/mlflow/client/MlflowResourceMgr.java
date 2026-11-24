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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.mlflow.RangerMlflowConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MlflowResourceMgr {

    private static final Logger LOG = LoggerFactory.getLogger(MlflowResourceMgr.class);

    public static Map<String, Object> validateConfig(String serviceName,
                                                     Map<String, String> configs) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> MlflowResourceMgr.validateConfig serviceName=[{}]", serviceName);
        }
        try {
            return MlflowClient.connectionTest(serviceName, configs);
        } catch (Exception e) {
            LOG.error("<== MlflowResourceMgr.validateConfig failed", e);
            throw e;
        }
    }

    public static List<String> getMlflowResources(String serviceName,
                                                  Map<String, String> configs,
                                                  ResourceLookupContext context) {
        if (context == null || StringUtils.isBlank(context.getResourceName())) {
            return Collections.emptyList();
        }

        String resourceName = context.getResourceName();
        String userInput = context.getUserInput();
        Map<String, List<String>> resourceMap = context.getResources();
        List<String> existing = resourceMap == null ? null : resourceMap.get(resourceName);

        String missing = MlflowClient.missingRequiredConfig(configs);
        if (missing != null) {
            if (RangerMlflowConstants.RESOURCE_WORKSPACE.equals(resourceName)) {
                return MlflowClient.filterMatches(
                        Collections.singletonList(RangerMlflowConstants.DEFAULT_WORKSPACE),
                        userInput,
                        existing,
                        false);
            }
            LOG.warn("MLflow service [{}] is missing '{}'; cannot lookup {}",
                    serviceName, missing, resourceName);
            return Collections.emptyList();
        }

        String workspace = resolveSelectedWorkspace(resourceMap);
        MlflowClient client = MlflowConnectionMgr.getMlflowClient(serviceName, configs);
        return client.lookupResources(resourceName, workspace, userInput, existing);
    }

    static String resolveSelectedWorkspace(Map<String, List<String>> resourceMap) {
        if (resourceMap == null) {
            return null;
        }
        List<String> workspaces = resourceMap.get(RangerMlflowConstants.RESOURCE_WORKSPACE);
        if (workspaces == null || workspaces.size() != 1) {
            return null;
        }
        String candidate = workspaces.get(0);
        if (StringUtils.isBlank(candidate) || candidate.contains("*") || candidate.contains("?")) {
            return null;
        }
        return candidate.trim();
    }

    private MlflowResourceMgr() {
        // utility class
    }
}
