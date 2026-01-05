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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * GravitinoConnectionManager - Manages connections to Gravitino servers.
 * 
 * This class provides connection pooling and caching for Gravitino clients
 * to improve performance and reduce connection overhead.
 */
public class GravitinoConnectionManager {
    private static final Logger LOG = LoggerFactory.getLogger(GravitinoConnectionManager.class);
    
    // Cache clients by service name to avoid creating new clients for each request
    private static final ConcurrentHashMap<String, GravitinoClient> clientCache = new ConcurrentHashMap<>();
    
    private GravitinoConnectionManager() {
        // Utility class - no instantiation
    }
    
    /**
     * Get a Gravitino client for the specified service.
     * 
     * @param serviceName Name of the Ranger service
     * @param serviceType Type of service (gravitino)
     * @param configs Service configuration
     * @return A GravitinoClient instance
     * @throws Exception if client creation fails
     */
    public static GravitinoClient getClient(
            String serviceName,
            String serviceType,
            Map<String, String> configs) throws Exception {
        
        LOG.debug("Getting Gravitino client for service: {}", serviceName);
        
        // For simplicity, create a new client each time to ensure fresh configs
        // In production, you might want to cache clients with proper invalidation
        GravitinoClient client = new GravitinoHttpClient(serviceName, configs);
        
        LOG.debug("Created new Gravitino HTTP client for service: {}", serviceName);
        return client;
    }
    
    /**
     * Get a cached client or create a new one.
     * 
     * @param serviceName Name of the Ranger service
     * @param serviceType Type of service (gravitino)
     * @param configs Service configuration
     * @return A cached or new GravitinoClient instance
     * @throws Exception if client creation fails
     */
    public static GravitinoClient getCachedClient(
            String serviceName,
            String serviceType,
            Map<String, String> configs) throws Exception {
        
        return clientCache.computeIfAbsent(serviceName, name -> {
            try {
                LOG.info("Creating cached Gravitino client for service: {}", name);
                return new GravitinoHttpClient(name, configs);
            } catch (Exception e) {
                LOG.error("Failed to create Gravitino client for service: {}", name, e);
                throw new RuntimeException("Failed to create Gravitino client", e);
            }
        });
    }
    
    /**
     * Invalidate a cached client.
     * 
     * @param serviceName Name of the Ranger service
     */
    public static void invalidateClient(String serviceName) {
        GravitinoClient removed = clientCache.remove(serviceName);
        if (removed != null) {
            LOG.info("Invalidated cached Gravitino client for service: {}", serviceName);
        }
    }
    
    /**
     * Clear all cached clients.
     */
    public static void clearCache() {
        LOG.info("Clearing all cached Gravitino clients");
        clientCache.clear();
    }
}
