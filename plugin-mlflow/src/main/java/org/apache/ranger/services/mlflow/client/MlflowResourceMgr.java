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
        if (context == null) {
            return Collections.emptyList();
        }

        String resourceName = context.getResourceName();
        String userInput = context.getUserInput();
        Map<String, List<String>> resourceMap = context.getResources();
        List<String> existing = resourceMap == null ? null : resourceMap.get(resourceName);

        if (!RangerMlflowConstants.ALL_LOOKUP_RESOURCES.contains(resourceName)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No MLflow lookup supported for resourceName=[{}]", resourceName);
            }
            return Collections.emptyList();
        }

        String missing = MlflowClient.missingRequiredConfig(configs);
        if (missing != null) {
            LOG.error("MLflow service [{}] is missing '{}'; cannot lookup {}",
                    serviceName, missing, resourceName);
            return Collections.emptyList();
        }

        String workspace = resolveSelectedWorkspace(resourceMap);
        MlflowClient client = MlflowConnectionMgr.getMlflowClient(serviceName, configs);

        switch (resourceName) {
            case RangerMlflowConstants.RESOURCE_WORKSPACE:
                return client.getWorkspaceList(userInput, existing);
            case RangerMlflowConstants.RESOURCE_EXPERIMENT:
                return client.getExperimentList(workspace, userInput, existing);
            case RangerMlflowConstants.RESOURCE_REGISTERED_MODEL:
                return client.getRegisteredModelList(workspace, userInput, existing, false);
            case RangerMlflowConstants.RESOURCE_PROMPT:
                return client.getRegisteredModelList(workspace, userInput, existing, true);
            case RangerMlflowConstants.RESOURCE_SCORER:
                return client.getScorerList(workspace, userInput, existing);
            case RangerMlflowConstants.RESOURCE_GATEWAY_SECRET:
                return client.getGatewaySecretList(workspace, userInput, existing);
            case RangerMlflowConstants.RESOURCE_GATEWAY_ENDPOINT:
                return client.getGatewayEndpointList(workspace, userInput, existing);
            case RangerMlflowConstants.RESOURCE_GATEWAY_MODEL_DEFINITION:
                return client.getGatewayModelDefinitionList(workspace, userInput, existing);
            case RangerMlflowConstants.RESOURCE_MCP_SERVER:
                return client.getMcpServerList(workspace, userInput, existing);
            default:
                return Collections.emptyList();
        }
    }

    static String resolveSelectedWorkspace(Map<String, List<String>> resourceMap) {
        if (resourceMap == null) {
            return RangerMlflowConstants.DEFAULT_WORKSPACE;
        }
        List<String> wsValues = resourceMap.get(RangerMlflowConstants.RESOURCE_WORKSPACE);
        if (wsValues == null || wsValues.isEmpty()) {
            return RangerMlflowConstants.DEFAULT_WORKSPACE;
        }
        for (String val : wsValues) {
            if (StringUtils.isNotBlank(val) && !RangerMlflowConstants.WILDCARD.equals(val.trim())) {
                return val.trim();
            }
        }
        return RangerMlflowConstants.DEFAULT_WORKSPACE;
    }

    private MlflowResourceMgr() {
        // utility class
    }
}
