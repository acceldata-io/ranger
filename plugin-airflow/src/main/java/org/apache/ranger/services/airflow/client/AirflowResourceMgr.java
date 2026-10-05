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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.airflow.RangerAirflowConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AirflowResourceMgr {

    private static final Logger LOG = LoggerFactory.getLogger(AirflowResourceMgr.class);

    public static Map<String, Object> validateConfig(String serviceName,
                                                     Map<String, String> configs) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AirflowResourceMgr.validateConfig serviceName=[{}]", serviceName);
        }
        try {
            return AirflowClient.connectionTest(serviceName, configs);
        } catch (Exception e) {
            LOG.error("<== AirflowResourceMgr.validateConfig failed", e);
            throw e;
        }
    }

    /**
     * Resource-lookup entry invoked by {@code RangerServiceAirflow.lookupResource()}
     * when an admin types into a policy resource field.
     */
    public static List<String> getAirflowResources(String serviceName,
                                                   Map<String, String> configs,
                                                   ResourceLookupContext context) {
        if (context == null) {
            return Collections.emptyList();
        }

        String resourceName = context.getResourceName();
        String userInput = context.getUserInput();
        Map<String, List<String>> resourceMap = context.getResources();
        List<String> existing = resourceMap == null ? null : resourceMap.get(resourceName);

        if (RangerAirflowConstants.RESOURCE_VIEW.equals(resourceName)) {
            return AirflowClient.filterMatches(
                    RangerAirflowConstants.BUILTIN_VIEWS, userInput, existing, true);
        }

        if (!isHttpLookupResource(resourceName)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No Airflow lookup for resourceName=[{}]", resourceName);
            }
            return Collections.emptyList();
        }

        String missing = AirflowClient.missingRequiredConfig(configs);
        if (missing != null) {
            LOG.error("Airflow service [{}] is missing '{}'; cannot lookup {}",
                    serviceName, missing, resourceName);
            return Collections.emptyList();
        }

        AirflowClient client = AirflowConnectionMgr.getAirflowClient(serviceName, configs);
        if (RangerAirflowConstants.RESOURCE_DAG.equals(resourceName)) {
            return client.getDagList(userInput, existing);
        }
        if (RangerAirflowConstants.RESOURCE_CONNECTION.equals(resourceName)) {
            return client.getConnectionList(userInput, existing);
        }
        if (RangerAirflowConstants.RESOURCE_VARIABLE.equals(resourceName)) {
            return client.getVariableList(userInput, existing);
        }
        return client.getPoolList(userInput, existing);
    }

    private static boolean isHttpLookupResource(String resourceName) {
        return RangerAirflowConstants.RESOURCE_DAG.equals(resourceName)
                || RangerAirflowConstants.RESOURCE_CONNECTION.equals(resourceName)
                || RangerAirflowConstants.RESOURCE_VARIABLE.equals(resourceName)
                || RangerAirflowConstants.RESOURCE_POOL.equals(resourceName);
    }

    private AirflowResourceMgr() {
        // utility class
    }
}
