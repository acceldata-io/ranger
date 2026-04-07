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

package org.apache.ranger.biz;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXRMSMappingProviderDao;
import org.apache.ranger.db.XXRMSNotificationDao;
import org.apache.ranger.db.XXRMSResourceMappingDao;
import org.apache.ranger.db.XXRMSServiceResourceDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.entity.XXRMSMappingProvider;
import org.apache.ranger.entity.XXRMSNotification;
import org.apache.ranger.entity.XXRMSResourceMapping;
import org.apache.ranger.entity.XXRMSServiceResource;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.util.ServiceRMSMappings;
import org.apache.ranger.plugin.util.ServiceRMSMappings.RMSResourceMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RMSMgr handles all Resource Mapping Server (RMS) business logic.
 * It manages mappings between high-level resources (Hive tables/databases)
 * and low-level resources (HDFS paths, S3 locations, Ozone keys).
 */
@Component
public class RMSMgr {
    private static final Logger LOG = LoggerFactory.getLogger(RMSMgr.class);

    private static final String RMS_MAPPING_PROVIDER_NAME = "HMS";
    private static final String SERVICE_TYPE_HIVE = "hive";
    private static final String SERVICE_TYPE_HDFS = "hdfs";
    private static final String SERVICE_TYPE_OZONE = "ozone";
    private static final String SERVICE_TYPE_S3 = "s3";

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    GUIDUtil guidUtil;

    @Autowired
    ServiceDBStore svcStore;

    private XXRMSMappingProviderDao mappingProviderDao;
    private XXRMSNotificationDao notificationDao;
    private XXRMSServiceResourceDao serviceResourceDao;
    private XXRMSResourceMappingDao resourceMappingDao;
    private XXServiceDao serviceDao;

    @PostConstruct
    public void init() {
        LOG.info("==> RMSMgr.init()");
        mappingProviderDao = daoMgr.getXXRMSMappingProvider();
        notificationDao = daoMgr.getXXRMSNotification();
        serviceResourceDao = daoMgr.getXXRMSServiceResource();
        resourceMappingDao = daoMgr.getXXRMSResourceMapping();
        serviceDao = daoMgr.getXXService();
        LOG.info("<== RMSMgr.init()");
    }

    /**
     * Get resource mappings for a service (for plugin download).
     */
    @Transactional(readOnly = true)
    public ServiceRMSMappings getServiceMappings(String serviceName, Long lastKnownVersion) {
        LOG.debug("==> RMSMgr.getServiceMappings(serviceName={}, lastKnownVersion={})", serviceName, lastKnownVersion);

        ServiceRMSMappings ret = null;

        XXService xxService = serviceDao.findByName(serviceName);
        if (xxService == null) {
            LOG.error("Service not found: {}", serviceName);
            return null;
        }

        XXRMSMappingProvider mappingProvider = getMappingProvider();
        Long currentVersion = mappingProvider != null ? mappingProvider.getLastKnownVersion() : 0L;

        if (lastKnownVersion != null && lastKnownVersion.equals(currentVersion)) {
            ret = new ServiceRMSMappings(serviceName, null, currentVersion);
            ret.setLastKnownVersion(lastKnownVersion);
            LOG.debug("No changes since version {}", lastKnownVersion);
        } else {
            ret = buildServiceMappings(xxService, currentVersion);
        }

        LOG.debug("<== RMSMgr.getServiceMappings(serviceName={}, lastKnownVersion={}): mappingVersion={}",
                  serviceName, lastKnownVersion, ret != null ? ret.getMappingVersion() : null);

        return ret;
    }

    /**
     * Build complete mappings for a service.
     */
    private ServiceRMSMappings buildServiceMappings(XXService xxService, Long mappingVersion) {
        ServiceRMSMappings ret = new ServiceRMSMappings();
        ret.setServiceName(xxService.getName());
        ret.setMappingVersion(mappingVersion);

        String serviceType = getServiceType(xxService);
        if (StringUtils.isBlank(serviceType)) {
            return ret;
        }

        List<Object[]> mappings = resourceMappingDao.getResourceMappings();
        if (CollectionUtils.isEmpty(mappings)) {
            return ret;
        }

        Map<Long, XXRMSServiceResource> resourceCache = new HashMap<>();

        for (Object[] mapping : mappings) {
            Long hlResourceId = (Long) mapping[0];
            Long llResourceId = (Long) mapping[1];

            XXRMSServiceResource hlResource = getOrLoadResource(hlResourceId, resourceCache);
            XXRMSServiceResource llResource = getOrLoadResource(llResourceId, resourceCache);

            if (hlResource == null || llResource == null) {
                continue;
            }

            XXService llService = serviceDao.getById(llResource.getServiceId());
            if (llService == null || !llService.getId().equals(xxService.getId())) {
                continue;
            }

            XXService hlService = serviceDao.getById(hlResource.getServiceId());
            if (hlService != null) {
                ret.setHlServiceName(hlService.getName());
            }

            RMSResourceMapping rmsMapping = new RMSResourceMapping();
            rmsMapping.setHlResourceGuid(hlResource.getGuid());
            rmsMapping.setLlResourceGuid(llResource.getGuid());
            rmsMapping.setHlResourceElements(parseResourceElements(hlResource.getServiceResourceElements()));
            rmsMapping.setLlResourceElements(parseResourceElements(llResource.getServiceResourceElements()));

            ret.addResourceMapping(rmsMapping);

            RangerServiceResource hlSvcResource = toRangerServiceResource(hlResource, hlService);
            RangerServiceResource llSvcResource = toRangerServiceResource(llResource, llService);

            ret.addServiceResource(hlSvcResource);
            ret.addServiceResource(llSvcResource);
        }

        return ret;
    }

    private XXRMSServiceResource getOrLoadResource(Long resourceId, Map<Long, XXRMSServiceResource> cache) {
        XXRMSServiceResource ret = cache.get(resourceId);
        if (ret == null) {
            ret = serviceResourceDao.getById(resourceId);
            if (ret != null) {
                cache.put(resourceId, ret);
            }
        }
        return ret;
    }

    private String getServiceType(XXService xxService) {
        if (xxService == null || xxService.getType() == null) {
            return null;
        }
        return svcStore.getServiceDefByIdForRMS(xxService.getType());
    }

    private RangerServiceResource toRangerServiceResource(XXRMSServiceResource xxResource, XXService xxService) {
        RangerServiceResource ret = new RangerServiceResource();
        ret.setId(xxResource.getId());
        ret.setGuid(xxResource.getGuid());
        ret.setServiceName(xxService != null ? xxService.getName() : null);
        ret.setResourceElements(parseResourceElements(xxResource.getServiceResourceElements()));
        ret.setResourceSignature(xxResource.getResourceSignature());
        return ret;
    }

    @SuppressWarnings("unchecked")
    private Map<String, RangerPolicy.RangerPolicyResource> parseResourceElements(String resourceElementsText) {
        if (StringUtils.isBlank(resourceElementsText)) {
            return Collections.emptyMap();
        }
        try {
            return JsonUtils.jsonToObject(resourceElementsText, Map.class);
        } catch (Exception e) {
            LOG.error("Failed to parse resource elements: {}", resourceElementsText, e);
            return Collections.emptyMap();
        }
    }

    /**
     * Create or update a resource mapping.
     */
    @Transactional
    public void createOrUpdateMapping(String hlServiceName, Map<String, RangerPolicy.RangerPolicyResource> hlResource,
                                       String llServiceName, Map<String, RangerPolicy.RangerPolicyResource> llResource,
                                       String location) {
        LOG.debug("==> RMSMgr.createOrUpdateMapping(hlService={}, llService={}, location={})",
                  hlServiceName, llServiceName, location);

        XXService hlService = serviceDao.findByName(hlServiceName);
        XXService llService = serviceDao.findByName(llServiceName);

        if (hlService == null || llService == null) {
            LOG.error("Service not found: hlService={}, llService={}", hlServiceName, llServiceName);
            return;
        }

        XXRMSServiceResource hlSvcResource = findOrCreateServiceResource(hlService, hlResource);
        XXRMSServiceResource llSvcResource = findOrCreateServiceResource(llService, llResource);

        if (hlSvcResource == null || llSvcResource == null) {
            LOG.error("Failed to create service resources");
            return;
        }

        XXRMSResourceMapping existingMapping = resourceMappingDao.findByHlAndLlResourceId(
            hlSvcResource.getId(), llSvcResource.getId());

        if (existingMapping == null) {
            XXRMSResourceMapping newMapping = new XXRMSResourceMapping();
            newMapping.setHlResourceId(hlSvcResource.getId());
            newMapping.setLlResourceId(llSvcResource.getId());
            newMapping.setChangeTimestamp(new Date());
            resourceMappingDao.create(newMapping);
            LOG.info("Created new RMS mapping: hl={}, ll={}", hlSvcResource.getGuid(), llSvcResource.getGuid());
        } else {
            existingMapping.setChangeTimestamp(new Date());
            resourceMappingDao.update(existingMapping);
            LOG.debug("Updated existing RMS mapping: id={}", existingMapping.getId());
        }

        updateMappingProviderVersion();

        LOG.debug("<== RMSMgr.createOrUpdateMapping()");
    }

    /**
     * Delete a resource mapping.
     */
    @Transactional
    public void deleteMapping(String hlServiceName, Map<String, RangerPolicy.RangerPolicyResource> hlResource,
                              String llServiceName, Map<String, RangerPolicy.RangerPolicyResource> llResource) {
        LOG.debug("==> RMSMgr.deleteMapping(hlService={}, llService={})", hlServiceName, llServiceName);

        XXService hlService = serviceDao.findByName(hlServiceName);
        XXService llService = serviceDao.findByName(llServiceName);

        if (hlService == null || llService == null) {
            LOG.warn("Service not found for mapping deletion");
            return;
        }

        String hlResourceSignature = computeResourceSignature(hlResource);
        String llResourceSignature = computeResourceSignature(llResource);

        XXRMSServiceResource hlSvcResource = serviceResourceDao.findByServiceAndResourceSignature(
            hlService.getId(), hlResourceSignature);
        XXRMSServiceResource llSvcResource = serviceResourceDao.findByServiceAndResourceSignature(
            llService.getId(), llResourceSignature);

        if (hlSvcResource != null && llSvcResource != null) {
            XXRMSResourceMapping mapping = resourceMappingDao.findByHlAndLlResourceId(
                hlSvcResource.getId(), llSvcResource.getId());
            if (mapping != null) {
                resourceMappingDao.remove(mapping.getId());
                LOG.info("Deleted RMS mapping: id={}", mapping.getId());
                updateMappingProviderVersion();
            }
        }

        LOG.debug("<== RMSMgr.deleteMapping()");
    }

    /**
     * Perform full sync - clear all mappings and reload from HMS.
     */
    @Transactional
    public void fullSync() {
        LOG.info("==> RMSMgr.fullSync()");

        List<XXRMSResourceMapping> allMappings = resourceMappingDao.getAll();
        for (XXRMSResourceMapping mapping : allMappings) {
            resourceMappingDao.remove(mapping.getId());
        }

        List<XXRMSNotification> allNotifications = notificationDao.getAll();
        for (XXRMSNotification notification : allNotifications) {
            notificationDao.remove(notification.getId());
        }

        List<XXRMSServiceResource> allResources = serviceResourceDao.getAll();
        for (XXRMSServiceResource resource : allResources) {
            serviceResourceDao.remove(resource.getId());
        }

        XXRMSMappingProvider provider = getMappingProvider();
        if (provider != null) {
            provider.setLastKnownVersion(0L);
            mappingProviderDao.update(provider);
        }

        LOG.info("<== RMSMgr.fullSync(): Cleared all RMS data. Ready for fresh sync from HMS.");
    }

    /**
     * Get or create mapping provider.
     */
    private XXRMSMappingProvider getMappingProvider() {
        XXRMSMappingProvider provider = mappingProviderDao.findByName(RMS_MAPPING_PROVIDER_NAME);
        if (provider == null) {
            provider = new XXRMSMappingProvider();
            provider.setName(RMS_MAPPING_PROVIDER_NAME);
            provider.setLastKnownVersion(0L);
            provider.setChangeTimestamp(new Date());
            provider = mappingProviderDao.create(provider);
        }
        return provider;
    }

    /**
     * Update mapping provider version after changes.
     */
    private void updateMappingProviderVersion() {
        XXRMSMappingProvider provider = getMappingProvider();
        if (provider != null) {
            provider.setLastKnownVersion(provider.getLastKnownVersion() + 1);
            provider.setChangeTimestamp(new Date());
            mappingProviderDao.update(provider);
        }
    }

    /**
     * Find or create a service resource.
     */
    private XXRMSServiceResource findOrCreateServiceResource(XXService service,
                                                              Map<String, RangerPolicy.RangerPolicyResource> resourceElements) {
        if (service == null || MapUtils.isEmpty(resourceElements)) {
            return null;
        }

        String resourceSignature = computeResourceSignature(resourceElements);
        XXRMSServiceResource existing = serviceResourceDao.findByServiceAndResourceSignature(
            service.getId(), resourceSignature);

        if (existing != null) {
            return existing;
        }

        XXRMSServiceResource newResource = new XXRMSServiceResource();
        newResource.setGuid(guidUtil.genGUID());
        newResource.setServiceId(service.getId());
        newResource.setResourceSignature(resourceSignature);
        newResource.setIsEnabled(true);
        newResource.setServiceResourceElements(JsonUtils.objectToJson(resourceElements));
        newResource.setCreateTime(new Date());
        newResource.setUpdateTime(new Date());

        return serviceResourceDao.create(newResource);
    }

    /**
     * Compute a signature for resource elements.
     */
    private String computeResourceSignature(Map<String, RangerPolicy.RangerPolicyResource> resourceElements) {
        if (MapUtils.isEmpty(resourceElements)) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        resourceElements.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> {
                sb.append(entry.getKey()).append("=");
                if (entry.getValue() != null && entry.getValue().getValues() != null) {
                    sb.append(String.join(",", entry.getValue().getValues()));
                }
                sb.append(";");
            });
        return sb.toString();
    }

    /**
     * Get the current mapping version.
     */
    @Transactional(readOnly = true)
    public Long getMappingVersion() {
        XXRMSMappingProvider provider = getMappingProvider();
        return provider != null ? provider.getLastKnownVersion() : 0L;
    }

    /**
     * Get mapping count for a service.
     */
    @Transactional(readOnly = true)
    public long getMappingCount(String serviceName) {
        XXService service = serviceDao.findByName(serviceName);
        if (service == null) {
            return 0;
        }
        List<RangerServiceResource> resources = serviceResourceDao.findByServiceId(service.getId());
        return resources != null ? resources.size() : 0;
    }

    /**
     * Check if RMS is enabled for a service type.
     */
    public boolean isRMSEnabledForServiceType(String serviceType) {
        return SERVICE_TYPE_HDFS.equalsIgnoreCase(serviceType) ||
               SERVICE_TYPE_OZONE.equalsIgnoreCase(serviceType) ||
               SERVICE_TYPE_S3.equalsIgnoreCase(serviceType);
    }

    /**
     * Get the high-level service name (Hive) for a low-level service.
     */
    @Transactional(readOnly = true)
    public String getHlServiceName(String llServiceName) {
        List<Object[]> mappings = resourceMappingDao.getResourceMappings();
        if (CollectionUtils.isEmpty(mappings)) {
            return null;
        }

        XXService llService = serviceDao.findByName(llServiceName);
        if (llService == null) {
            return null;
        }

        for (Object[] mapping : mappings) {
            Long hlResourceId = (Long) mapping[0];
            Long llResourceId = (Long) mapping[1];

            XXRMSServiceResource llResource = serviceResourceDao.getById(llResourceId);
            if (llResource != null && llResource.getServiceId().equals(llService.getId())) {
                XXRMSServiceResource hlResource = serviceResourceDao.getById(hlResourceId);
                if (hlResource != null) {
                    XXService hlService = serviceDao.getById(hlResource.getServiceId());
                    if (hlService != null) {
                        return hlService.getName();
                    }
                }
            }
        }

        return null;
    }
}
