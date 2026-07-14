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
import org.apache.ranger.db.XXRMSDeletionLogDao;
import org.apache.ranger.db.XXRMSMappingProviderDao;
import org.apache.ranger.db.XXRMSNotificationDao;
import org.apache.ranger.db.XXRMSResourceMappingDao;
import org.apache.ranger.db.XXRMSServiceResourceDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.entity.XXRMSDeletionLog;
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
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * RMSMgr handles all Resource Mapping Server (RMS) business logic.
 * It manages mappings between high-level resources (Hive tables/databases)
 * and low-level resources (HDFS paths, S3 locations, Ozone keys).
 */
@Component
@Lazy(false)
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
    private XXRMSDeletionLogDao deletionLogDao;
    private XXServiceDao serviceDao;

    /**
     * Process-local memo of fully-built mapping payloads, keyed by service
     * name and version. When N plugins simultaneously request a full sync at
     * the same currentVersion (a common pattern after Admin restart, when a
     * version bump invalidates plugin caches, or when a stale poller falls
     * below the deletion-tracking watermark), the heavy DB+JSON work runs
     * exactly once and the rest of the requests reuse the snapshot.
     *
     * <p>Bounded size keeps memory predictable in multi-tenant Admins; the
     * version field embedded in {@link CachedFullMappings} ensures stale
     * entries can never be served after a mapping write bumps the version.
     */
    private static final int FULL_MAPPINGS_CACHE_CAPACITY = 16;
    private final Map<String, CachedFullMappings> fullMappingsCache =
            Collections.synchronizedMap(new LinkedHashMap<String, CachedFullMappings>(
                    FULL_MAPPINGS_CACHE_CAPACITY + 1, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, CachedFullMappings> eldest) {
                    return size() > FULL_MAPPINGS_CACHE_CAPACITY;
                }
            });
    private final ConcurrentMap<String, Object> fullMappingsBuildLocks = new ConcurrentHashMap<>();

    /**
     * Immutable snapshot of a fully-built mapping payload for a service.
     * The collections are populated once by {@link #buildServiceMappings}
     * and never mutated thereafter, so we can safely share their references
     * with concurrent JSON-serialization paths in different responses.
     */
    private static final class CachedFullMappings {
        final long                                version;
        final String                              serviceName;
        final String                              hlServiceName;
        final List<RMSResourceMapping>            resourceMappings;
        final Map<String, RangerServiceResource>  serviceResources;

        CachedFullMappings(ServiceRMSMappings src) {
            this.version          = src.getMappingVersion() != null ? src.getMappingVersion() : 0L;
            this.serviceName      = src.getServiceName();
            this.hlServiceName    = src.getHlServiceName();
            this.resourceMappings = src.getResourceMappings() != null
                    ? src.getResourceMappings() : Collections.emptyList();
            this.serviceResources = src.getServiceResources() != null
                    ? src.getServiceResources() : Collections.emptyMap();
        }
    }

    /**
     * How many mapping_version slots of deletion history to keep in
     * {@code x_rms_deletion_log}. At typical DDL rates (10K tables with 1%
     * delete/day -> ~100 deletions/day) this gives several months of
     * history at a few KB; at very high churn the table is bounded and
     * pruning is exercised regularly.
     */
    private static final int DELETION_LOG_RETAINED_VERSIONS = 10_000;

    public static class DeletionRecord {
        public final String hlResourceGuid;
        public final String llResourceGuid;
        public final Long   llServiceId;

        public DeletionRecord(String hlResourceGuid, String llResourceGuid, Long llServiceId) {
            this.hlResourceGuid = hlResourceGuid;
            this.llResourceGuid = llResourceGuid;
            this.llServiceId = llServiceId;
        }
    }

    @PostConstruct
    public void init() {
        LOG.info("==> RMSMgr.init()");
        mappingProviderDao = daoMgr.getXXRMSMappingProvider();
        notificationDao = daoMgr.getXXRMSNotification();
        serviceResourceDao = daoMgr.getXXRMSServiceResource();
        resourceMappingDao = daoMgr.getXXRMSResourceMapping();
        deletionLogDao = daoMgr.getXXRMSDeletionLog();
        serviceDao = daoMgr.getXXService();
        LOG.info("<== RMSMgr.init()");
    }

    /**
     * Get resource mappings for a service (for plugin download).
     * Supports delta downloads: if lastKnownVersion is provided and valid,
     * returns only mappings changed since that version (isDelta=true).
     * If lastKnownVersion is null or 0, returns the full mapping set.
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
            LOG.debug("No changes since version {}", lastKnownVersion);
            return null;
        } else if (lastKnownVersion != null && lastKnownVersion > 0 && lastKnownVersion < currentVersion) {
            ret = buildDeltaMappings(xxService, lastKnownVersion, currentVersion);
        } else {
            // Initial or refresh download: serve from the per-service memo so
            // a thundering-herd of plugin restarts collapses to a single DB
            // build per (service, version).
            ret = buildOrGetCachedFullMappings(xxService, currentVersion);
            ret.setLastKnownVersion(lastKnownVersion);
            ret.setIsDelta(false);
        }

        LOG.debug("<== RMSMgr.getServiceMappings(serviceName={}, lastKnownVersion={}): mappingVersion={}, isDelta={}",
                  serviceName, lastKnownVersion,
                  ret != null ? ret.getMappingVersion() : null,
                  ret != null ? ret.getIsDelta() : null);

        return ret;
    }

    /**
     * Build delta mappings: only mappings changed since lastKnownVersion.
     * Returns isDelta=true with only changed/added mappings + removed GUIDs.
     * Falls back to full download if deletion history is incomplete.
     */
    private ServiceRMSMappings buildDeltaMappings(XXService xxService, Long lastKnownVersion, Long currentVersion) {
        LOG.info("Building delta mappings for {}: version {} -> {}", xxService.getName(), lastKnownVersion, currentVersion);

        if (!hasDeletionHistorySince(lastKnownVersion)) {
            LOG.info("Deletion history incomplete since version {}, falling back to full download", lastKnownVersion);
            ServiceRMSMappings full = buildOrGetCachedFullMappings(xxService, currentVersion);
            full.setIsDelta(false);
            full.setLastKnownVersion(lastKnownVersion);
            return full;
        }

        ServiceRMSMappings ret = new ServiceRMSMappings();
        ret.setServiceName(xxService.getName());
        ret.setMappingVersion(currentVersion);
        ret.setLastKnownVersion(lastKnownVersion);
        ret.setIsDelta(true);

        List<Object[]> changedMappings = resourceMappingDao.findChangedMappingsForService(
                xxService.getId(), lastKnownVersion, currentVersion);
        // The DB query already restricts to mappings whose ll-resource belongs
        // to xxService, so we skip the Java-side llService filter.
        assembleMappings(changedMappings, xxService, ret, false);

        List<DeletionRecord> deletions = getDeletionsSinceVersion(xxService.getId(), lastKnownVersion);
        if (CollectionUtils.isNotEmpty(deletions)) {
            for (DeletionRecord deletion : deletions) {
                ret.addRemovedResourceGuid(deletion.llResourceGuid);
            }
        }

        LOG.info("Delta built for {}: {} additions, {} removals (version {} -> {})",
                 xxService.getName(),
                 ret.getResourceMappings() != null ? ret.getResourceMappings().size() : 0,
                 ret.getRemovedResourceGuids() != null ? ret.getRemovedResourceGuids().size() : 0,
                 lastKnownVersion, currentVersion);

        return ret;
    }

    /**
     * Stampede-safe accessor for the fully-built mapping payload.
     * <p>
     * The hot path is a lock-free read of {@link #fullMappingsCache}; on a
     * cache hit at the requested version we return a fresh
     * {@link ServiceRMSMappings} header that <em>shares</em> the immutable
     * resource-mapping list and resource map from the cache. Sharing is safe
     * because nothing mutates those collections after they're populated by
     * {@link #buildServiceMappings}.
     * <p>
     * On miss we serialize concurrent rebuilders behind a per-service lock,
     * so 100 simultaneous full-sync requests collapse to a single DB build.
     * Cache eviction is automatic: a stale (lower-version) entry simply
     * fails the version check and is replaced under the lock.
     */
    private ServiceRMSMappings buildOrGetCachedFullMappings(XXService xxService, Long currentVersion) {
        String name = xxService.getName();
        long   wantVersion = currentVersion != null ? currentVersion : 0L;

        CachedFullMappings hit = fullMappingsCache.get(name);
        if (hit != null && hit.version == wantVersion) {
            return wrapCachedFullMappings(hit);
        }

        Object lock = fullMappingsBuildLocks.computeIfAbsent(name, k -> new Object());
        synchronized (lock) {
            hit = fullMappingsCache.get(name);
            if (hit != null && hit.version == wantVersion) {
                return wrapCachedFullMappings(hit);
            }
            ServiceRMSMappings fresh = buildServiceMappings(xxService, currentVersion);
            fullMappingsCache.put(name, new CachedFullMappings(fresh));
            return fresh;
        }
    }

    private ServiceRMSMappings wrapCachedFullMappings(CachedFullMappings cached) {
        ServiceRMSMappings ret = new ServiceRMSMappings();
        ret.setServiceName(cached.serviceName);
        ret.setHlServiceName(cached.hlServiceName);
        ret.setMappingVersion(cached.version);
        ret.setIsDelta(false);
        // Share immutable references with the cache. The REST layer only
        // serializes these to JSON, never mutates them.
        ret.setResourceMappings(cached.resourceMappings);
        ret.setServiceResources(cached.serviceResources);
        return ret;
    }

    /**
     * Drop all cached full payloads. Called whenever a write operation
     * mutates mappings; the next reader will rebuild from the source of
     * truth at the new currentVersion. (Strictly speaking, the version
     * check in the read path already guarantees correctness, but eagerly
     * clearing avoids carrying obsolete bytes in memory.)
     */
    private void invalidateFullMappingsCache() {
        fullMappingsCache.clear();
    }

    /**
     * Build complete mappings for a service.
     */
    private ServiceRMSMappings buildServiceMappings(XXService xxService, Long mappingVersion) {
        ServiceRMSMappings ret = new ServiceRMSMappings();
        ret.setServiceName(xxService.getName());
        ret.setMappingVersion(mappingVersion);
        ret.setIsDelta(false);

        String serviceType = getServiceType(xxService);
        if (StringUtils.isBlank(serviceType)) {
            return ret;
        }

        List<Object[]> mappings = resourceMappingDao.getResourceMappings();
        if (CollectionUtils.isEmpty(mappings)) {
            return ret;
        }

        // Full-mapping path returns rows for every service; filter to xxService in Java.
        assembleMappings(mappings, xxService, ret, true);

        return ret;
    }

    /**
     * Hydrate (hlResourceId, llResourceId) tuples into {@code ret} using two
     * batched DB queries instead of the previous N+1 per-row pattern:
     * <ol>
     *   <li>One IN-list lookup for all distinct resource ids touched.</li>
     *   <li>One getById() per distinct hl-service id (cardinality is
     *       typically a single digit, and JPA's persistence-context cache
     *       collapses repeats within a transaction).</li>
     * </ol>
     * For deltas, callers must pass {@code filterByLlService=false} because
     * the SQL query already restricts results to xxService. For full builds,
     * pass {@code true} to drop rows whose ll-resource belongs to a different
     * service.
     */
    private void assembleMappings(List<Object[]> rows,
                                  XXService xxService,
                                  ServiceRMSMappings ret,
                                  boolean filterByLlService) {
        if (CollectionUtils.isEmpty(rows)) {
            return;
        }

        Set<Long> resourceIds = new HashSet<>(rows.size() * 2);
        for (Object[] row : rows) {
            Long hlId = (Long) row[0];
            Long llId = (Long) row[1];
            if (hlId != null) resourceIds.add(hlId);
            if (llId != null) resourceIds.add(llId);
        }

        Map<Long, XXRMSServiceResource> resourcesById = serviceResourceDao.findByIds(resourceIds);
        Map<Long, XXService> servicesById = new HashMap<>();

        for (Object[] row : rows) {
            Long hlResourceId = (Long) row[0];
            Long llResourceId = (Long) row[1];

            XXRMSServiceResource hlResource = resourcesById.get(hlResourceId);
            XXRMSServiceResource llResource = resourcesById.get(llResourceId);

            if (hlResource == null || llResource == null) {
                continue;
            }

            if (filterByLlService) {
                XXService llService = lookupServiceCached(llResource.getServiceId(), servicesById);
                if (llService == null || !llService.getId().equals(xxService.getId())) {
                    continue;
                }
            }

            XXService hlService = lookupServiceCached(hlResource.getServiceId(), servicesById);
            if (hlService != null) {
                ret.setHlServiceName(hlService.getName());
            }

            RMSResourceMapping rmsMapping = new RMSResourceMapping();
            rmsMapping.setHlResourceGuid(hlResource.getGuid());
            rmsMapping.setLlResourceGuid(llResource.getGuid());
            rmsMapping.setHlResourceElements(parseResourceElements(hlResource.getServiceResourceElements()));
            rmsMapping.setLlResourceElements(parseResourceElements(llResource.getServiceResourceElements()));

            ret.addResourceMapping(rmsMapping);

            ret.addServiceResource(toRangerServiceResource(hlResource, hlService));
            ret.addServiceResource(toRangerServiceResource(llResource, xxService));
        }
    }

    private XXService lookupServiceCached(Long serviceId, Map<Long, XXService> cache) {
        if (serviceId == null) {
            return null;
        }
        XXService cached = cache.get(serviceId);
        if (cached != null || cache.containsKey(serviceId)) {
            return cached;
        }
        XXService loaded = serviceDao.getById(serviceId);
        cache.put(serviceId, loaded);
        return loaded;
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
     * @throws Exception if services cannot be resolved or resource creation fails
     */
    @Transactional
    public void createOrUpdateMapping(String hlServiceName, Map<String, RangerPolicy.RangerPolicyResource> hlResource,
                                       String llServiceName, Map<String, RangerPolicy.RangerPolicyResource> llResource,
                                       String location) throws Exception {
        LOG.debug("==> RMSMgr.createOrUpdateMapping(hlService={}, llService={}, location={})",
                  hlServiceName, llServiceName, location);

        XXService hlService = serviceDao.findByName(hlServiceName);
        XXService llService = serviceDao.findByName(llServiceName);

        if (hlService == null) {
            throw new Exception("HL service not found: " + hlServiceName);
        }
        if (llService == null) {
            throw new Exception("LL service not found: " + llServiceName);
        }

        XXRMSServiceResource hlSvcResource = findOrCreateServiceResource(hlService, hlResource);
        XXRMSServiceResource llSvcResource = findOrCreateServiceResource(llService, llResource);

        if (hlSvcResource == null) {
            throw new Exception("Failed to create HL service resource for " + hlServiceName);
        }
        if (llSvcResource == null) {
            throw new Exception("Failed to create LL service resource for " + llServiceName);
        }

        XXRMSResourceMapping existingMapping = resourceMappingDao.findByHlAndLlResourceId(
            hlSvcResource.getId(), llSvcResource.getId());

        if (existingMapping == null) {
            // Genuine new mapping: bump the global version and persist it on the row.
            Long newVersion = getNextMappingVersion();
            XXRMSResourceMapping newMapping = new XXRMSResourceMapping();
            newMapping.setHlResourceId(hlSvcResource.getId());
            newMapping.setLlResourceId(llSvcResource.getId());
            newMapping.setChangeTimestamp(new Date());
            newMapping.setMappingVersion(newVersion);
            resourceMappingDao.create(newMapping);
            LOG.info("Created new RMS mapping: hl={}, ll={}, version={}",
                     hlSvcResource.getGuid(), llSvcResource.getGuid(), newVersion);
            updateMappingProviderVersion();
        } else {
            // The mapping is keyed by (hl_resource_id, ll_resource_id); both are
            // resolved from stable signatures by findOrCreateServiceResource. If
            // both look-ups returned existing rows AND the (hl, ll) pair is
            // already in x_rms_resource_mapping, the mapping is unchanged. Do
            // not bump mapping_version or the provider version — otherwise every
            // full-sync would trigger every HDFS plugin to re-download the full
            // mapping payload (isDelta=false) for no real change.
            LOG.debug("RMS mapping unchanged (no-op): id={}, hl={}, ll={}, version={}",
                      existingMapping.getId(), hlSvcResource.getGuid(),
                      llSvcResource.getGuid(), existingMapping.getMappingVersion());
        }

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

                // Record the deletion so that delta downloads return the removed
                // LL resource GUID to plugins. Without this, plugins that use
                // delta mode would never learn the mapping was removed and would
                // continue to evaluate stale policies for the LL path.
                List<DeletionRecord> deletions = new ArrayList<>();
                deletions.add(new DeletionRecord(
                    hlSvcResource.getGuid(), llSvcResource.getGuid(), llSvcResource.getServiceId()));

                updateMappingProviderVersion();
                Long version = getMappingProvider().getLastKnownVersion();
                recordDeletions(version, deletions);

                // If the LL resource is now orphaned (no other HL maps to it),
                // drop the service-resource row too, mirroring deleteMappingsByHlResource.
                List<Long> otherHlIds = resourceMappingDao.findByLlResourceId(llSvcResource.getId());
                if (CollectionUtils.isEmpty(otherHlIds)) {
                    serviceResourceDao.remove(llSvcResource.getId());
                }
            }
        }

        LOG.debug("<== RMSMgr.deleteMapping()");
    }

    /**
     * Delete all mappings for a given HL (Hive) resource.
     */
    @Transactional
    public void deleteMappingsByHlResource(String hlServiceName,
                                            Map<String, RangerPolicy.RangerPolicyResource> hlResource) {
        LOG.debug("==> RMSMgr.deleteMappingsByHlResource(hlService={})", hlServiceName);

        XXService hlService = serviceDao.findByName(hlServiceName);
        if (hlService == null) {
            LOG.warn("Service not found for deletion: {}", hlServiceName);
            return;
        }

        String resourceSignature = computeResourceSignature(hlResource);
        XXRMSServiceResource hlSvcResource = serviceResourceDao.findByServiceAndResourceSignature(
            hlService.getId(), resourceSignature);

        if (hlSvcResource != null) {
            List<Long> llResourceIds = resourceMappingDao.findByHlResourceId(hlSvcResource.getId());

            List<DeletionRecord> deletions = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(llResourceIds)) {
                for (Long llResourceId : llResourceIds) {
                    XXRMSServiceResource llSvcResource = serviceResourceDao.getById(llResourceId);
                    if (llSvcResource != null) {
                        deletions.add(new DeletionRecord(
                            hlSvcResource.getGuid(), llSvcResource.getGuid(), llSvcResource.getServiceId()));
                    }
                }
            }

            resourceMappingDao.deleteByHlResourceId(hlSvcResource.getId());
            serviceResourceDao.remove(hlSvcResource.getId());

            if (CollectionUtils.isNotEmpty(llResourceIds)) {
                for (Long llResourceId : llResourceIds) {
                    List<Long> otherHlIds = resourceMappingDao.findByLlResourceId(llResourceId);
                    if (CollectionUtils.isEmpty(otherHlIds)) {
                        serviceResourceDao.remove(llResourceId);
                    }
                }
            }

            updateMappingProviderVersion();

            Long version = getMappingProvider().getLastKnownVersion();
            recordDeletions(version, deletions);

            LOG.info("Deleted RMS mappings for HL resource: signature={}, version={}", resourceSignature, version);
        } else {
            LOG.debug("No HL resource found for deletion: signature={}", resourceSignature);
        }

        LOG.debug("<== RMSMgr.deleteMappingsByHlResource()");
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
     * Persist a batch of deletions made at {@code version}, then prune old
     * entries that have aged out of the retention window. Runs inside the
     * caller's transaction (callers are already {@code @Transactional}), so
     * a deletion is durably visible to other Admin instances as soon as
     * the enclosing transaction commits.
     */
    private void recordDeletions(Long version, List<DeletionRecord> deletions) {
        if (version == null || CollectionUtils.isEmpty(deletions)) {
            return;
        }
        List<XXRMSDeletionLog> rows = new ArrayList<>(deletions.size());
        for (DeletionRecord d : deletions) {
            rows.add(new XXRMSDeletionLog(version, d.hlResourceGuid, d.llResourceGuid, d.llServiceId));
        }
        deletionLogDao.recordAll(rows);
        pruneOldDeletions(version);
    }

    /**
     * Drop persisted deletion records older than the retention window and,
     * if any rows were removed, advance the persisted watermark so plugins
     * below the new minimum version are correctly forced to full-download.
     */
    private void pruneOldDeletions(Long currentVersion) {
        if (currentVersion == null || currentVersion <= DELETION_LOG_RETAINED_VERSIONS) {
            return;
        }
        long cutoff = currentVersion - DELETION_LOG_RETAINED_VERSIONS;
        int removed = deletionLogDao.deleteOlderThanVersion(cutoff);
        if (removed > 0) {
            advanceDeletionTrackingTo(cutoff);
            LOG.info("Pruned {} deletion-log rows older than version {}", removed, cutoff);
        }
    }

    /**
     * Return persisted deletion records affecting {@code serviceId} that
     * happened strictly after {@code sinceVersion}. The query is bounded by
     * an index on (ll_service_id, version) so multi-tenant Admins with one
     * heavy tenant don't block lighter ones.
     */
    public List<DeletionRecord> getDeletionsSinceVersion(Long serviceId, Long sinceVersion) {
        if (serviceId == null || sinceVersion == null) {
            return Collections.emptyList();
        }
        List<XXRMSDeletionLog> rows = deletionLogDao.findByServiceSinceVersion(serviceId, sinceVersion);
        if (CollectionUtils.isEmpty(rows)) {
            return Collections.emptyList();
        }
        List<DeletionRecord> ret = new ArrayList<>(rows.size());
        for (XXRMSDeletionLog row : rows) {
            ret.add(new DeletionRecord(row.getHlResourceGuid(), row.getLlResourceGuid(), row.getLlServiceId()));
        }
        return ret;
    }

    /**
     * Check whether the persisted deletion log is complete from
     * {@code sinceVersion} onwards. The watermark lives on
     * {@code x_rms_mapping_provider.deletion_tracking_from_version}, which
     * is shared across every Admin instance and survives restart, so all
     * Admins agree on the cutoff a plugin must be at to be served deltas.
     *
     * On a fresh upgrade the column is 0; the first check after upgrade
     * lazily snapshots it to the then-current mapping version, after which
     * it only moves forward when older records are pruned.
     */
    public boolean hasDeletionHistorySince(Long sinceVersion) {
        if (sinceVersion == null) {
            return false;
        }
        return sinceVersion >= getOrInitDeletionTrackingFromVersion();
    }

    /**
     * Read the persisted deletion-tracking watermark, lazily snapshotting it
     * on first call after upgrade.
     * <p>
     * Both the lazy-init and the subsequent reads use {@link
     * XXRMSMappingProviderDao#initDeletionTrackingFromVersion} which writes
     * <em>only</em> the watermark column with a guarded WHERE. This is critical
     * because this method runs on the read (download) path and would otherwise
     * race with the RMS poller's concurrent {@code last_known_version} write
     * via {@code mappingProviderDao.update(entity)} — a stale read on the
     * download side could overwrite the poller's bumped version (lost update).
     */
    private long getOrInitDeletionTrackingFromVersion() {
        XXRMSMappingProvider provider = getMappingProvider();
        Long persisted = provider != null ? provider.getDeletionTrackingFromVersion() : null;
        if (persisted != null && persisted > 0L) {
            return persisted;
        }
        if (provider == null) {
            return 0L;
        }
        long current = provider.getLastKnownVersion() != null ? provider.getLastKnownVersion() : 0L;
        int updated = mappingProviderDao.initDeletionTrackingFromVersion(RMS_MAPPING_PROVIDER_NAME, current);
        if (updated > 0) {
            LOG.info("Initialized persisted deletion-tracking watermark at mapping version {}", current);
            return current;
        }
        // Either another Admin/thread won the race or the column is already
        // set to a non-zero value; re-read to find the authoritative value.
        Long fresh = mappingProviderDao.getEntityManager()
                .createNamedQuery("XXRMSMappingProvider.findByName", XXRMSMappingProvider.class)
                .setParameter("name", RMS_MAPPING_PROVIDER_NAME)
                .getSingleResult()
                .getDeletionTrackingFromVersion();
        return fresh != null ? fresh : current;
    }

    /**
     * Atomically move the watermark forward; no-op if already at/beyond
     * {@code minVersion}. The update is monotonic forward-only at the SQL
     * layer thanks to the {@code WHERE deletion_tracking_from_version &lt;
     * :minVersion} guard, so concurrent calls (and concurrent Admins in HA)
     * collapse safely.
     */
    private void advanceDeletionTrackingTo(long minVersion) {
        mappingProviderDao.advanceDeletionTrackingFromVersion(RMS_MAPPING_PROVIDER_NAME, minVersion);
    }

    private Long getNextMappingVersion() {
        XXRMSMappingProvider provider = getMappingProvider();
        return provider != null ? provider.getLastKnownVersion() + 1 : 1L;
    }

    /**
     * Update mapping provider version after changes. Also drops the cached
     * full-mapping snapshots so subsequent readers rebuild against the new
     * version. Strictly speaking the version check in the read path already
     * guarantees correctness, but eager invalidation reclaims memory and
     * keeps the cache state synchronized with the DB.
     */
    private void updateMappingProviderVersion() {
        XXRMSMappingProvider provider = getMappingProvider();
        if (provider != null) {
            provider.setLastKnownVersion(provider.getLastKnownVersion() + 1);
            provider.setChangeTimestamp(new Date());
            mappingProviderDao.update(provider);
            invalidateFullMappingsCache();
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
     * Get the last processed HMS event ID (persisted across restarts).
     * Stored in the mapping provider's changeTimestamp field as epoch millis
     * would waste precision; instead we encode it in the notification table.
     * For simplicity, we use the x_rms_mapping_provider name column with a
     * secondary record to store the event ID.
     */
    @Transactional(readOnly = true)
    public long getLastProcessedEventId() {
        XXRMSMappingProvider provider = mappingProviderDao.findByName("HMS_LAST_EVENT_ID");
        if (provider != null) {
            return provider.getLastKnownVersion() != null ? provider.getLastKnownVersion() : -1L;
        }
        return -1L;
    }

    @Transactional
    public void saveLastProcessedEventId(long eventId) {
        XXRMSMappingProvider provider = mappingProviderDao.findByName("HMS_LAST_EVENT_ID");
        if (provider == null) {
            provider = new XXRMSMappingProvider();
            provider.setName("HMS_LAST_EVENT_ID");
            provider.setLastKnownVersion(eventId);
            provider.setChangeTimestamp(new Date());
            mappingProviderDao.create(provider);
        } else {
            provider.setLastKnownVersion(eventId);
            provider.setChangeTimestamp(new Date());
            mappingProviderDao.update(provider);
        }
    }

    /**
     * Check if there are existing RMS mappings in the database.
     */
    @Transactional(readOnly = true)
    public boolean hasExistingMappings() {
        List<Object[]> mappings = resourceMappingDao.getResourceMappings();
        return CollectionUtils.isNotEmpty(mappings);
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
