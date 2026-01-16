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
package org.apache.ranger.services.gravitino;

import org.apache.ranger.plugin.client.HadoopConfigHolder;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.gravitino.client.GravitinoHttpClient;
import org.apache.ranger.services.gravitino.client.GravitinoResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * RangerServiceGravitino - Ranger service implementation for Gravitino.
 * 
 * This class provides:
 * 1. Connection validation to Gravitino server
 * 2. Resource lookup for policy autocomplete in Ranger Admin UI
 * 3. Default policies for new Gravitino services
 */
public class RangerServiceGravitino extends RangerBaseService {
    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceGravitino.class);
    
    // Resource names
    public static final String RESOURCE_METALAKE = "metalake";
    public static final String RESOURCE_CATALOG = "catalog";
    public static final String RESOURCE_SCHEMA = "schema";
    public static final String RESOURCE_TABLE = "table";
    public static final String RESOURCE_TOPIC = "topic";
    public static final String RESOURCE_FILESET = "fileset";
    public static final String RESOURCE_MODEL = "model";
    
    // Access types
    public static final String ACCESS_CREATE_CATALOG = "create_catalog";
    public static final String ACCESS_USE_CATALOG = "use_catalog";
    public static final String ACCESS_CREATE_SCHEMA = "create_schema";
    public static final String ACCESS_USE_SCHEMA = "use_schema";
    public static final String ACCESS_SELECT_TABLE = "select_table";
    public static final String ACCESS_MODIFY_TABLE = "modify_table";
    public static final String ACCESS_MANAGE_GRANTS = "manage_grants";
    
    public static final String WILDCARD = "*";
    
    public RangerServiceGravitino() {
        super();
    }
    
    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
        LOG.info("Initializing RangerServiceGravitino for service: {}", service.getName());
    }

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret = new HashMap<>();
        String serviceName = getServiceName();
        
        LOG.debug("==> RangerServiceGravitino.validateConfig() Service: {}", serviceName);

        if (configs != null) {
            // Ensure password field exists (can be null for OAuth auth)
            if (!configs.containsKey(HadoopConfigHolder.RANGER_LOGIN_PASSWORD)) {
                configs.put(HadoopConfigHolder.RANGER_LOGIN_PASSWORD, null);
            }

            try {
                ret = GravitinoHttpClient.connectionTest(serviceName, configs);
            } catch (HadoopException he) {
                LOG.error("Connection test failed for service: {}", serviceName, he);
                throw he;
            }
        }
        
        LOG.debug("<== RangerServiceGravitino.validateConfig() Response: {}", ret);
        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<>();
        String serviceName = getServiceName();
        String serviceType = getServiceType();
        Map<String, String> configs = getConfigs();
        
        LOG.debug("==> RangerServiceGravitino.lookupResource() Context: {}", context);

        if (configs != null && !configs.containsKey(HadoopConfigHolder.RANGER_LOGIN_PASSWORD)) {
            configs.put(HadoopConfigHolder.RANGER_LOGIN_PASSWORD, null);
        }

        try {
            ret = GravitinoResourceManager.getGravitinoResources(serviceName, serviceType, configs, context);
        } catch (Exception e) {
            LOG.error("Resource lookup failed for service: {}", serviceName, e);
            throw e;
        }
        
        LOG.debug("<== RangerServiceGravitino.lookupResource() Response size: {}", ret.size());
        return ret;
    }
    
    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        LOG.debug("==> RangerServiceGravitino.getDefaultRangerPolicies()");
        
        List<RangerPolicy> ret = super.getDefaultRangerPolicies();
        
        // Add lookup user access to all-metalakes policy
        for (RangerPolicy defaultPolicy : ret) {
            if (defaultPolicy.getName().contains("all") && lookUpUser != null && !lookUpUser.isEmpty()) {
                RangerPolicyItem lookupItem = new RangerPolicyItem();
                List<RangerPolicyItemAccess> lookupAccesses = new ArrayList<>();
                lookupAccesses.add(new RangerPolicyItemAccess(ACCESS_USE_CATALOG));
                lookupAccesses.add(new RangerPolicyItemAccess(ACCESS_USE_SCHEMA));
                lookupAccesses.add(new RangerPolicyItemAccess(ACCESS_SELECT_TABLE));
                lookupItem.setUsers(Collections.singletonList(lookUpUser));
                lookupItem.setAccesses(lookupAccesses);
                lookupItem.setDelegateAdmin(false);
                defaultPolicy.addPolicyItem(lookupItem);
            }
        }
        
        LOG.debug("<== RangerServiceGravitino.getDefaultRangerPolicies() count: {}", ret.size());
        return ret;
    }
}
