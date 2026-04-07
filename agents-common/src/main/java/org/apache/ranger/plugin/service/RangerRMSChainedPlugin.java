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

package org.apache.ranger.plugin.service;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerChainedPluginConfig;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.util.RangerRMSMappingCache;
import org.apache.ranger.plugin.util.RangerRMSMappingCache.MappingEntry;
import org.apache.ranger.plugin.util.ServiceRMSMappings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RangerRMSChainedPlugin is the base class for RMS-enabled chained plugins.
 * It provides functionality to:
 * 1. Download and cache resource mappings from RMS
 * 2. Lookup Hive resources from storage paths
 * 3. Evaluate Hive policies for storage access requests
 */
public abstract class RangerRMSChainedPlugin extends RangerChainedPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(RangerRMSChainedPlugin.class);

    protected static final String HIVE_ACCESS_TYPE_SELECT = "select";
    protected static final String HIVE_ACCESS_TYPE_UPDATE = "update";
    protected static final String HIVE_ACCESS_TYPE_ALTER = "alter";
    protected static final String HIVE_ACCESS_TYPE_CREATE = "create";
    protected static final String HIVE_ACCESS_TYPE_DROP = "drop";
    protected static final String HIVE_ACCESS_TYPE_ALL = "all";
    protected static final String HIVE_ACCESS_TYPE_ANY = "_any";

    protected static final String STORAGE_ACCESS_TYPE_READ = "read";
    protected static final String STORAGE_ACCESS_TYPE_WRITE = "write";
    protected static final String STORAGE_ACCESS_TYPE_EXECUTE = "execute";

    private final RangerRMSMappingCache mappingCache;
    private final boolean authorizeOnlyWithChainedPlugin;
    private final Map<String, List<String>> accessTypeMapping;
    private final Map<String, List<String>> dbAccessTypeMapping;
    private final Set<String> privilegedUsers;
    private final Set<String> serviceUsers;

    protected RangerRMSChainedPlugin(RangerBasePlugin rootPlugin, String serviceType, String serviceName) {
        super(rootPlugin, serviceType, serviceName);

        this.mappingCache = new RangerRMSMappingCache(rootPlugin.getServiceName());

        RangerPluginConfig config = rootPlugin.getPluginContext().getConfig();
        String propertyPrefix = config.getPropertyPrefix();

        this.authorizeOnlyWithChainedPlugin = config.getBoolean(
            propertyPrefix + ".mapping.hive.authorize.with.only.chained.policies", true);

        this.accessTypeMapping = loadAccessTypeMapping(config, propertyPrefix, false);
        this.dbAccessTypeMapping = loadAccessTypeMapping(config, propertyPrefix, true);

        this.privilegedUsers = loadUserSet(config, propertyPrefix + ".privileged.user.names",
            "admin,dpprofiler,hue,beacon,hive,impala");
        this.serviceUsers = loadUserSet(config, propertyPrefix + ".service.names",
            "hive,impala");

        LOG.info("RangerRMSChainedPlugin: authorizeOnlyWithChainedPlugin={}", authorizeOnlyWithChainedPlugin);
    }

    private Map<String, List<String>> loadAccessTypeMapping(RangerPluginConfig config, String prefix, boolean isDbLevel) {
        Map<String, List<String>> ret = new HashMap<>();

        String mappingPrefix = prefix + (isDbLevel ? ".db.accesstype.mapping." : ".accesstype.mapping.");

        String readMapping = config.get(mappingPrefix + "read", HIVE_ACCESS_TYPE_SELECT);
        String writeMapping = config.get(mappingPrefix + "write", HIVE_ACCESS_TYPE_UPDATE + "," + HIVE_ACCESS_TYPE_ALTER);
        String executeMapping = config.get(mappingPrefix + "execute", HIVE_ACCESS_TYPE_ANY);

        ret.put(STORAGE_ACCESS_TYPE_READ, parseAccessTypes(readMapping));
        ret.put(STORAGE_ACCESS_TYPE_WRITE, parseAccessTypes(writeMapping));
        ret.put(STORAGE_ACCESS_TYPE_EXECUTE, parseAccessTypes(executeMapping));

        if (isDbLevel) {
            ret.put(STORAGE_ACCESS_TYPE_READ, parseAccessTypes(config.get(mappingPrefix + "read", HIVE_ACCESS_TYPE_ANY)));
            ret.put(STORAGE_ACCESS_TYPE_WRITE, parseAccessTypes(config.get(mappingPrefix + "write",
                HIVE_ACCESS_TYPE_CREATE + "," + HIVE_ACCESS_TYPE_DROP + "," + HIVE_ACCESS_TYPE_ALTER)));
        }

        return ret;
    }

    private List<String> parseAccessTypes(String accessTypes) {
        List<String> ret = new ArrayList<>();
        if (StringUtils.isNotBlank(accessTypes)) {
            for (String accessType : accessTypes.split(",")) {
                if (StringUtils.isNotBlank(accessType)) {
                    ret.add(accessType.trim());
                }
            }
        }
        return ret;
    }

    private Set<String> loadUserSet(RangerPluginConfig config, String property, String defaultValue) {
        Set<String> ret = new HashSet<>();
        String value = config.get(property, defaultValue);
        if (StringUtils.isNotBlank(value)) {
            for (String user : value.split(",")) {
                if (StringUtils.isNotBlank(user)) {
                    ret.add(user.trim().toLowerCase());
                }
            }
        }
        return ret;
    }

    @Override
    protected RangerBasePlugin buildChainedPlugin(String serviceType, String serviceName, String appId) {
        RangerPluginConfig rootConfig = rootPlugin.getPluginContext().getConfig();
        RangerChainedPluginConfig chainedConfig = new RangerChainedPluginConfig(serviceType, serviceName, appId, rootConfig);
        return new RangerBasePlugin(chainedConfig);
    }

    @Override
    public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
        LOG.debug("==> RangerRMSChainedPlugin.isAccessAllowed({})", request);

        RangerAccessResult ret = null;

        if (request == null) {
            LOG.debug("<== RangerRMSChainedPlugin.isAccessAllowed(): null request");
            return null;
        }

        if (isPrivilegedUser(request.getUser())) {
            LOG.debug("Skipping chained evaluation for privileged user: {}", request.getUser());
            return null;
        }

        if (skipAccessCheckIfAlreadyDetermined && isAccessDetermined(request)) {
            LOG.debug("Skipping chained evaluation as access is already determined");
            return null;
        }

        String storagePath = extractStoragePath(request);
        if (StringUtils.isBlank(storagePath)) {
            LOG.debug("No storage path found in request");
            return null;
        }

        MappingEntry mapping = mappingCache.findMappingForPath(storagePath);
        if (mapping == null) {
            LOG.debug("No RMS mapping found for path: {}", storagePath);
            return authorizeOnlyWithChainedPlugin ? createDeniedResult(request) : null;
        }

        ret = evaluateHiveAccess(request, mapping);

        LOG.debug("<== RangerRMSChainedPlugin.isAccessAllowed(): result={}", ret);
        return ret;
    }

    @Override
    public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests) {
        List<RangerAccessResult> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(requests)) {
            for (RangerAccessRequest request : requests) {
                RangerAccessResult result = isAccessAllowed(request);
                ret.add(result);
            }
        }

        return ret;
    }

    @Override
    public RangerResourceACLs getResourceACLs(RangerAccessRequest request) {
        return getResourceACLs(request, null);
    }

    @Override
    public RangerResourceACLs getResourceACLs(RangerAccessRequest request, Integer policyType) {
        LOG.debug("==> RangerRMSChainedPlugin.getResourceACLs()");

        RangerResourceACLs ret = null;

        if (request != null) {
            String storagePath = extractStoragePath(request);
            MappingEntry mapping = storagePath != null ? mappingCache.findMappingForPath(storagePath) : null;

            if (mapping != null) {
                RangerAccessRequestImpl hiveRequest = createHiveRequest(request, mapping);
                if (hiveRequest != null && plugin != null) {
                    ret = plugin.getResourceACLs(hiveRequest, policyType);
                }
            }
        }

        LOG.debug("<== RangerRMSChainedPlugin.getResourceACLs(): {}", ret);
        return ret;
    }

    @Override
    public boolean isAuthorizeOnlyWithChainedPlugin() {
        return authorizeOnlyWithChainedPlugin;
    }

    @Override
    public RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request) {
        LOG.debug("==> RangerRMSChainedPlugin.evalDataMaskPolicies()");

        RangerAccessResult ret = null;

        String storagePath = extractStoragePath(request);
        MappingEntry mapping = storagePath != null ? mappingCache.findMappingForPath(storagePath) : null;

        if (mapping != null) {
            RangerAccessRequestImpl hiveRequest = createHiveRequest(request, mapping);
            if (hiveRequest != null && plugin != null) {
                ret = plugin.evalDataMaskPolicies(hiveRequest, null);
            }
        }

        LOG.debug("<== RangerRMSChainedPlugin.evalDataMaskPolicies(): {}", ret);
        return ret;
    }

    @Override
    public RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request) {
        LOG.debug("==> RangerRMSChainedPlugin.evalRowFilterPolicies()");

        RangerAccessResult ret = null;

        String storagePath = extractStoragePath(request);
        MappingEntry mapping = storagePath != null ? mappingCache.findMappingForPath(storagePath) : null;

        if (mapping != null) {
            RangerAccessRequestImpl hiveRequest = createHiveRequest(request, mapping);
            if (hiveRequest != null && plugin != null) {
                ret = plugin.evalRowFilterPolicies(hiveRequest, null);
            }
        }

        LOG.debug("<== RangerRMSChainedPlugin.evalRowFilterPolicies(): {}", ret);
        return ret;
    }

    /**
     * Update the mapping cache with new mappings.
     */
    public void updateMappings(ServiceRMSMappings rmsMappings) {
        if (rmsMappings != null) {
            mappingCache.update(rmsMappings);
        }
    }

    /**
     * Get the current mapping version.
     */
    public Long getMappingVersion() {
        return mappingCache.getMappingVersion();
    }

    /**
     * Extract storage path from the request. Override in subclasses for specific resource types.
     */
    protected abstract String extractStoragePath(RangerAccessRequest request);

    /**
     * Check if a user is privileged (should skip RMS evaluation).
     */
    protected boolean isPrivilegedUser(String user) {
        if (StringUtils.isBlank(user)) {
            return false;
        }
        return privilegedUsers.contains(user.toLowerCase()) || serviceUsers.contains(user.toLowerCase());
    }

    /**
     * Check if access is already determined.
     */
    protected boolean isAccessDetermined(RangerAccessRequest request) {
        Object isAccessDetermined = request.getContext() != null ?
            request.getContext().get("isAccessDetermined") : null;
        return Boolean.TRUE.equals(isAccessDetermined);
    }

    /**
     * Evaluate Hive access for a storage request.
     */
    protected RangerAccessResult evaluateHiveAccess(RangerAccessRequest request, MappingEntry mapping) {
        LOG.debug("==> evaluateHiveAccess(user={}, database={}, table={})",
                  request.getUser(), mapping.getHiveDatabaseName(), mapping.getHiveTableName());

        RangerAccessResult ret = null;

        RangerAccessResult maskResult = evalDataMaskPolicies(request);
        if (maskResult != null && maskResult.isMaskEnabled()) {
            LOG.info("Denying access due to masking policy on table: {}.{}",
                     mapping.getHiveDatabaseName(), mapping.getHiveTableName());
            ret = createDeniedResult(request);
            ret.setReason("Masking policy exists for the mapped Hive resource");
            return ret;
        }

        RangerAccessResult rowFilterResult = evalRowFilterPolicies(request);
        if (rowFilterResult != null && rowFilterResult.isRowFilterEnabled()) {
            LOG.info("Denying access due to row-filter policy on table: {}.{}",
                     mapping.getHiveDatabaseName(), mapping.getHiveTableName());
            ret = createDeniedResult(request);
            ret.setReason("Row-filter policy exists for the mapped Hive resource");
            return ret;
        }

        List<String> hiveAccessTypes = mapStorageAccessToHive(request.getAccessType(),
            StringUtils.isBlank(mapping.getHiveTableName()));

        if (CollectionUtils.isEmpty(hiveAccessTypes)) {
            return null;
        }

        for (String hiveAccessType : hiveAccessTypes) {
            RangerAccessRequestImpl hiveRequest = createHiveRequest(request, mapping);
            if (hiveRequest != null) {
                hiveRequest.setAccessType(hiveAccessType);

                if (plugin != null) {
                    RangerAccessResult hiveResult = plugin.isAccessAllowed(hiveRequest);
                    if (hiveResult != null && hiveResult.getIsAllowed()) {
                        ret = hiveResult;
                        break;
                    } else if (hiveResult != null && hiveResult.getIsAccessDetermined()) {
                        ret = hiveResult;
                    }
                }
            }
        }

        if (ret == null && authorizeOnlyWithChainedPlugin) {
            ret = createDeniedResult(request);
        }

        LOG.debug("<== evaluateHiveAccess(): isAllowed={}", ret != null ? ret.getIsAllowed() : null);
        return ret;
    }

    /**
     * Create a Hive access request from a storage request.
     */
    protected RangerAccessRequestImpl createHiveRequest(RangerAccessRequest storageRequest, MappingEntry mapping) {
        RangerAccessResourceImpl hiveResource = new RangerAccessResourceImpl();
        hiveResource.setServiceDef(plugin.getServiceDef());
        hiveResource.setValue("database", mapping.getHiveDatabaseName());

        String tableName = mapping.getHiveTableName();
        if (StringUtils.isNotBlank(tableName)) {
            hiveResource.setValue("table", tableName);
            hiveResource.setValue("column", "*");
        }

        RangerAccessRequestImpl ret = new RangerAccessRequestImpl();
        ret.setResource(hiveResource);
        ret.setUser(storageRequest.getUser());
        ret.setUserGroups(storageRequest.getUserGroups());
        ret.setUserRoles(storageRequest.getUserRoles());
        ret.setAccessType(storageRequest.getAccessType());
        ret.setAction(storageRequest.getAction());
        ret.setClientIPAddress(storageRequest.getClientIPAddress());
        ret.setClientType(storageRequest.getClientType());
        ret.setAccessTime(storageRequest.getAccessTime());
        ret.setClusterName(storageRequest.getClusterName());

        return ret;
    }

    /**
     * Map storage access type to Hive access types.
     */
    protected List<String> mapStorageAccessToHive(String storageAccessType, boolean isDatabaseLevel) {
        Map<String, List<String>> mapping = isDatabaseLevel ? dbAccessTypeMapping : accessTypeMapping;
        List<String> ret = mapping.get(storageAccessType);
        return ret != null ? ret : new ArrayList<>();
    }

    /**
     * Create a denied access result.
     */
    protected RangerAccessResult createDeniedResult(RangerAccessRequest request) {
        RangerAccessResult ret = new RangerAccessResult(
            RangerPolicy.POLICY_TYPE_ACCESS, serviceName, plugin.getServiceDef(), request);
        ret.setIsAllowed(false);
        ret.setIsAccessDetermined(true);
        return ret;
    }
}
