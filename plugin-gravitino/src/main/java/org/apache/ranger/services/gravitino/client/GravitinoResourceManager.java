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
package org.apache.ranger.services.gravitino.client;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * GravitinoResourceManager - Handles resource lookup for Ranger Admin UI.
 * 
 * This class provides autocomplete functionality for Gravitino resources
 * when creating policies in the Ranger Admin UI.
 */
public class GravitinoResourceManager {
    private static final Logger LOG = LoggerFactory.getLogger(GravitinoResourceManager.class);
    
    // Resource names matching the service definition
    public static final String RESOURCE_METALAKE = "metalake";
    public static final String RESOURCE_CATALOG = "catalog";
    public static final String RESOURCE_SCHEMA = "schema";
    public static final String RESOURCE_TABLE = "table";
    public static final String RESOURCE_TOPIC = "topic";
    public static final String RESOURCE_FILESET = "fileset";
    public static final String RESOURCE_MODEL = "model";
    public static final String RESOURCE_MODEL_VERSION = "model-version";
    
    private static final int LOOKUP_TIMEOUT_SECONDS = 10;

    /**
     * Get Gravitino resources for the Ranger Admin UI autocomplete.
     * 
     * @param serviceName Name of the Ranger service
     * @param serviceType Type of service (gravitino)
     * @param configs Service configuration
     * @param context Resource lookup context from the UI
     * @return List of matching resource names
     * @throws Exception if lookup fails
     */
    public static List<String> getGravitinoResources(
            String serviceName,
            String serviceType,
            Map<String, String> configs,
            ResourceLookupContext context
    ) throws Exception {
        
        if (context == null) {
            return Collections.emptyList();
        }
        
        String userInput = context.getUserInput();
        String resourceName = context.getResourceName();
        Map<String, List<String>> resourceMap = context.getResources();
        
        LOG.debug("Resource lookup: resourceName={}, userInput={}, resources={}", 
                resourceName, userInput, resourceMap);
        
        // Extract parent resources from the UI context
        String metalake = getFirstValue(resourceMap, RESOURCE_METALAKE);
        String catalog = getFirstValue(resourceMap, RESOURCE_CATALOG);
        String schema = getFirstValue(resourceMap, RESOURCE_SCHEMA);
        String model = getFirstValue(resourceMap, RESOURCE_MODEL);

        final GravitinoClient client = GravitinoConnectionManager.getClient(serviceName, serviceType, configs);
        final String needle = (userInput == null || userInput.isEmpty()) ? "" : userInput;

        Callable<List<String>> task = null;

        switch (resourceName.toLowerCase()) {
            case RESOURCE_METALAKE:
                task = () -> client.listMetalakes(needle);
                break;
                
            case RESOURCE_CATALOG:
                if (metalake != null && !metalake.isEmpty()) {
                    final String m = metalake;
                    task = () -> client.listCatalogs(m, needle);
                }
                break;
                
            case RESOURCE_SCHEMA:
                if (metalake != null && catalog != null && !metalake.isEmpty() && !catalog.isEmpty()) {
                    final String m = metalake;
                    final String c = catalog;
                    task = () -> client.listSchemas(m, c, needle);
                }
                break;
                
            case RESOURCE_TABLE:
                if (metalake != null && catalog != null && schema != null &&
                        !metalake.isEmpty() && !catalog.isEmpty() && !schema.isEmpty()) {
                    final String m = metalake;
                    final String c = catalog;
                    final String s = schema;
                    task = () -> client.listTables(m, c, s, needle);
                }
                break;
                
            case RESOURCE_TOPIC:
                if (metalake != null && catalog != null && schema != null &&
                        !metalake.isEmpty() && !catalog.isEmpty() && !schema.isEmpty()) {
                    final String m = metalake;
                    final String c = catalog;
                    final String s = schema;
                    task = () -> client.listTopics(m, c, s, needle);
                }
                break;
                
            case RESOURCE_FILESET:
                if (metalake != null && catalog != null && schema != null &&
                        !metalake.isEmpty() && !catalog.isEmpty() && !schema.isEmpty()) {
                    final String m = metalake;
                    final String c = catalog;
                    final String s = schema;
                    task = () -> client.listFilesets(m, c, s, needle);
                }
                break;
                
            case RESOURCE_MODEL:
                if (metalake != null && catalog != null && schema != null &&
                        !metalake.isEmpty() && !catalog.isEmpty() && !schema.isEmpty()) {
                    final String m = metalake;
                    final String c = catalog;
                    final String s = schema;
                    task = () -> client.listModels(m, c, s, needle);
                }
                break;
                
            case RESOURCE_MODEL_VERSION:
                if (metalake != null && catalog != null && schema != null && model != null &&
                        !metalake.isEmpty() && !catalog.isEmpty() && !schema.isEmpty() && !model.isEmpty()) {
                    final String m = metalake;
                    final String c = catalog;
                    final String s = schema;
                    final String md = model;
                    task = () -> client.listModelVersions(m, c, s, md, needle);
                }
                break;
                
            default:
                LOG.warn("Unknown resource type: {}", resourceName);
                return Collections.emptyList();
        }

        if (task == null) {
            LOG.debug("Task is null - missing parent resources for {}", resourceName);
            return Collections.emptyList();
        }

        try {
            List<String> result = TimedEventUtil.timedTask(task, LOOKUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            LOG.debug("Resource lookup returned {} items for {}", 
                    result != null ? result.size() : 0, resourceName);
            return result != null ? result : Collections.emptyList();
        } catch (Exception e) {
            LOG.error("Resource lookup failed for {}", resourceName, e);
            throw e;
        }
    }
    
    /**
     * Get the first value from a list in the resource map.
     */
    private static String getFirstValue(Map<String, List<String>> resourceMap, String key) {
        if (resourceMap == null) {
            return null;
        }
        List<String> values = resourceMap.get(key);
        if (values != null && !values.isEmpty()) {
            return values.get(0);
        }
        return null;
    }
}
