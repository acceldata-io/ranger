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

package org.apache.ranger.plugin.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.util.ServiceRMSMappings.RMSResourceMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RangerRMSMappingCache caches RMS mappings and provides lookup functionality.
 * Used by chained plugins to map storage resources (HDFS paths, S3 keys, Ozone keys)
 * to Hive resources (databases, tables).
 */
public class RangerRMSMappingCache {
    private static final Logger LOG = LoggerFactory.getLogger(RangerRMSMappingCache.class);

    private final String serviceName;
    private final String hlServiceName;
    private volatile Long mappingVersion;

    private static class CacheSnapshot {
        final Map<String, RangerServiceResource> resourcesByGuid;
        final Map<String, List<MappingEntry>> mappingsByLlPath;
        final Map<String, MappingEntry> mappingsByHlResource;

        CacheSnapshot() {
            this.resourcesByGuid = Collections.emptyMap();
            this.mappingsByLlPath = Collections.emptyMap();
            this.mappingsByHlResource = Collections.emptyMap();
        }

        CacheSnapshot(Map<String, RangerServiceResource> resourcesByGuid,
                       Map<String, List<MappingEntry>> mappingsByLlPath,
                       Map<String, MappingEntry> mappingsByHlResource) {
            this.resourcesByGuid = Collections.unmodifiableMap(resourcesByGuid);
            this.mappingsByLlPath = Collections.unmodifiableMap(mappingsByLlPath);
            this.mappingsByHlResource = Collections.unmodifiableMap(mappingsByHlResource);
        }
    }

    private final AtomicReference<CacheSnapshot> snapshot = new AtomicReference<>(new CacheSnapshot());

    public RangerRMSMappingCache(String serviceName) {
        this.serviceName = serviceName;
        this.hlServiceName = null;
        this.mappingVersion = 0L;
    }

    /**
     * Update the cache atomically with new mappings (copy-on-write).
     * Supports both full replacement (isDelta=false) and incremental merge (isDelta=true).
     */
    public void update(ServiceRMSMappings rmsMappings) {
        if (rmsMappings == null) {
            return;
        }

        if (Boolean.TRUE.equals(rmsMappings.getIsDelta())) {
            applyDelta(rmsMappings);
        } else {
            applyFull(rmsMappings);
        }
    }

    private void applyFull(ServiceRMSMappings rmsMappings) {
        LOG.debug("==> applyFull(mappingVersion={})", rmsMappings.getMappingVersion());

        Map<String, RangerServiceResource> newResources = new HashMap<>();
        Map<String, List<MappingEntry>> newPathMappings = new HashMap<>();
        Map<String, MappingEntry> newHlMappings = new HashMap<>();

        if (MapUtils.isNotEmpty(rmsMappings.getServiceResources())) {
            newResources.putAll(rmsMappings.getServiceResources());
        }

        if (CollectionUtils.isNotEmpty(rmsMappings.getResourceMappings())) {
            for (RMSResourceMapping mapping : rmsMappings.getResourceMappings()) {
                processMapping(mapping, newResources, newPathMappings, newHlMappings);
            }
        }

        snapshot.set(new CacheSnapshot(newResources, newPathMappings, newHlMappings));
        this.mappingVersion = rmsMappings.getMappingVersion() != null ? rmsMappings.getMappingVersion() : 0L;

        LOG.info("RangerRMSMappingCache full update: resourceCount={}, mappingCount={}, version={}",
                 newResources.size(), newPathMappings.size(), mappingVersion);
    }

    private void applyDelta(ServiceRMSMappings rmsMappings) {
        LOG.debug("==> applyDelta(mappingVersion={})", rmsMappings.getMappingVersion());

        CacheSnapshot current = snapshot.get();

        Map<String, RangerServiceResource> mergedResources = new HashMap<>(current.resourcesByGuid);
        Map<String, List<MappingEntry>> mergedPathMappings = new HashMap<>();
        Map<String, MappingEntry> mergedHlMappings = new HashMap<>(current.mappingsByHlResource);

        for (Map.Entry<String, List<MappingEntry>> entry : current.mappingsByLlPath.entrySet()) {
            mergedPathMappings.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }

        // Remove deleted resources
        List<String> removedGuids = rmsMappings.getRemovedResourceGuids();
        if (CollectionUtils.isNotEmpty(removedGuids)) {
            for (String removedGuid : removedGuids) {
                RangerServiceResource removedResource = mergedResources.remove(removedGuid);
                if (removedResource != null) {
                    String path = extractPath(removedResource);
                    if (StringUtils.isNotBlank(path)) {
                        String normalizedPath = normalizePathForLookup(path);
                        mergedPathMappings.remove(normalizedPath);
                    }
                }
            }
            // Clean up HL mappings that reference removed LL resources
            mergedHlMappings.entrySet().removeIf(e ->
                removedGuids.contains(e.getValue().getLlResource().getGuid()));
        }

        // Add new/updated resources and mappings
        if (MapUtils.isNotEmpty(rmsMappings.getServiceResources())) {
            mergedResources.putAll(rmsMappings.getServiceResources());
        }

        if (CollectionUtils.isNotEmpty(rmsMappings.getResourceMappings())) {
            for (RMSResourceMapping mapping : rmsMappings.getResourceMappings()) {
                processMapping(mapping, mergedResources, mergedPathMappings, mergedHlMappings);
            }
        }

        snapshot.set(new CacheSnapshot(mergedResources, mergedPathMappings, mergedHlMappings));
        this.mappingVersion = rmsMappings.getMappingVersion() != null ? rmsMappings.getMappingVersion() : 0L;

        int addCount = rmsMappings.getResourceMappings() != null ? rmsMappings.getResourceMappings().size() : 0;
        int removeCount = removedGuids != null ? removedGuids.size() : 0;

        LOG.info("RangerRMSMappingCache delta update: +{} added, -{} removed, total mappings={}, version={}",
                 addCount, removeCount, mergedPathMappings.size(), mappingVersion);
    }

    private void processMapping(RMSResourceMapping mapping,
                                 Map<String, RangerServiceResource> resources,
                                 Map<String, List<MappingEntry>> pathMappings,
                                 Map<String, MappingEntry> hlMappings) {
        if (mapping == null) {
            return;
        }

        String hlGuid = mapping.getHlResourceGuid();
        String llGuid = mapping.getLlResourceGuid();

        if (StringUtils.isBlank(hlGuid) || StringUtils.isBlank(llGuid)) {
            return;
        }

        RangerServiceResource hlResource = resources.get(hlGuid);
        RangerServiceResource llResource = resources.get(llGuid);

        if (hlResource == null || llResource == null) {
            return;
        }

        String llPath = extractPath(llResource);
        if (StringUtils.isBlank(llPath)) {
            return;
        }

        MappingEntry entry = new MappingEntry(hlResource, llResource, mapping);

        pathMappings.computeIfAbsent(normalizePathForLookup(llPath), k -> new ArrayList<>()).add(entry);

        String hlKey = buildHlResourceKey(hlResource);
        if (StringUtils.isNotBlank(hlKey)) {
            hlMappings.put(hlKey, entry);
        }
    }

    /**
     * Find the Hive resource mapping for a storage path.
     */
    public MappingEntry findMappingForPath(String path) {
        if (StringUtils.isBlank(path)) {
            return null;
        }

        LOG.debug("==> findMappingForPath({})", path);

        CacheSnapshot current = snapshot.get();
        String normalizedPath = normalizePathForLookup(path);
        MappingEntry ret = null;

        List<MappingEntry> exactMatches = current.mappingsByLlPath.get(normalizedPath);
        if (CollectionUtils.isNotEmpty(exactMatches)) {
            ret = exactMatches.get(0);
        }

        if (ret == null) {
            ret = findMappingForParentPath(normalizedPath, current);
        }

        LOG.debug("<== findMappingForPath({}): found={}", path, ret != null);
        return ret;
    }

    private MappingEntry findMappingForParentPath(String path, CacheSnapshot current) {
        if (StringUtils.isBlank(path)) {
            return null;
        }

        String parentPath = getParentPath(path);
        while (StringUtils.isNotBlank(parentPath)) {
            List<MappingEntry> matches = current.mappingsByLlPath.get(parentPath);
            if (CollectionUtils.isNotEmpty(matches)) {
                for (MappingEntry entry : matches) {
                    if (entry.isRecursive()) {
                        return entry;
                    }
                }
            }
            parentPath = getParentPath(parentPath);
        }

        return null;
    }

    private String getParentPath(String path) {
        if (StringUtils.isBlank(path) || "/".equals(path)) {
            return null;
        }

        int lastSlash = path.lastIndexOf('/');
        if (lastSlash <= 0) {
            return "/";
        }

        return path.substring(0, lastSlash);
    }

    /**
     * Find Hive resource by database and table name.
     */
    public MappingEntry findMappingForHiveResource(String databaseName, String tableName) {
        if (StringUtils.isBlank(databaseName)) {
            return null;
        }

        String key = buildHlResourceKey(databaseName, tableName);
        return snapshot.get().mappingsByHlResource.get(key);
    }

    /**
     * Extract storage path from a resource.
     */
    private String extractPath(RangerServiceResource resource) {
        if (resource == null || MapUtils.isEmpty(resource.getResourceElements())) {
            return null;
        }

        Map<String, RangerPolicy.RangerPolicyResource> elements = resource.getResourceElements();

        RangerPolicy.RangerPolicyResource pathResource = elements.get("path");
        if (pathResource != null && CollectionUtils.isNotEmpty(pathResource.getValues())) {
            return pathResource.getValues().get(0);
        }

        RangerPolicy.RangerPolicyResource keyResource = elements.get("key");
        if (keyResource != null && CollectionUtils.isNotEmpty(keyResource.getValues())) {
            StringBuilder sb = new StringBuilder();

            RangerPolicy.RangerPolicyResource volumeResource = elements.get("volume");
            RangerPolicy.RangerPolicyResource bucketResource = elements.get("bucket");

            if (volumeResource != null && CollectionUtils.isNotEmpty(volumeResource.getValues())) {
                sb.append("/").append(volumeResource.getValues().get(0));
            }
            if (bucketResource != null && CollectionUtils.isNotEmpty(bucketResource.getValues())) {
                sb.append("/").append(bucketResource.getValues().get(0));
            }
            sb.append("/").append(keyResource.getValues().get(0));

            return sb.toString();
        }

        RangerPolicy.RangerPolicyResource s3PathResource = elements.get("path");
        if (s3PathResource == null) {
            s3PathResource = elements.get("object");
        }
        if (s3PathResource != null && CollectionUtils.isNotEmpty(s3PathResource.getValues())) {
            RangerPolicy.RangerPolicyResource bucketResource = elements.get("bucket");
            StringBuilder sb = new StringBuilder();

            if (bucketResource != null && CollectionUtils.isNotEmpty(bucketResource.getValues())) {
                sb.append(bucketResource.getValues().get(0)).append("/");
            }
            sb.append(s3PathResource.getValues().get(0));

            return sb.toString();
        }

        return null;
    }

    /**
     * Normalize a path for lookup.
     */
    private String normalizePathForLookup(String path) {
        if (StringUtils.isBlank(path)) {
            return path;
        }

        String normalized = path;

        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }

        while (normalized.endsWith("/") && normalized.length() > 1) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }

        return normalized;
    }

    /**
     * Build a key for HL resource lookup.
     */
    private String buildHlResourceKey(RangerServiceResource resource) {
        if (resource == null || MapUtils.isEmpty(resource.getResourceElements())) {
            return null;
        }

        Map<String, RangerPolicy.RangerPolicyResource> elements = resource.getResourceElements();

        String database = null;
        String table = null;

        RangerPolicy.RangerPolicyResource dbResource = elements.get("database");
        if (dbResource != null && CollectionUtils.isNotEmpty(dbResource.getValues())) {
            database = dbResource.getValues().get(0);
        }

        RangerPolicy.RangerPolicyResource tableResource = elements.get("table");
        if (tableResource != null && CollectionUtils.isNotEmpty(tableResource.getValues())) {
            table = tableResource.getValues().get(0);
        }

        return buildHlResourceKey(database, table);
    }

    private String buildHlResourceKey(String database, String table) {
        if (StringUtils.isBlank(database)) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(database.toLowerCase());

        if (StringUtils.isNotBlank(table)) {
            sb.append(".").append(table.toLowerCase());
        }

        return sb.toString();
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getHlServiceName() {
        return hlServiceName;
    }

    public Long getMappingVersion() {
        return mappingVersion;
    }

    public int getMappingCount() {
        return snapshot.get().mappingsByLlPath.size();
    }

    /**
     * Mapping entry containing both HL and LL resources.
     */
    public static class MappingEntry {
        private final RangerServiceResource hlResource;
        private final RangerServiceResource llResource;
        private final RMSResourceMapping mapping;

        public MappingEntry(RangerServiceResource hlResource, RangerServiceResource llResource, RMSResourceMapping mapping) {
            this.hlResource = hlResource;
            this.llResource = llResource;
            this.mapping = mapping;
        }

        public RangerServiceResource getHlResource() {
            return hlResource;
        }

        public RangerServiceResource getLlResource() {
            return llResource;
        }

        public RMSResourceMapping getMapping() {
            return mapping;
        }

        public String getHiveDatabaseName() {
            return getResourceValue(hlResource, "database");
        }

        public String getHiveTableName() {
            return getResourceValue(hlResource, "table");
        }

        public boolean isRecursive() {
            if (llResource == null || MapUtils.isEmpty(llResource.getResourceElements())) {
                return false;
            }

            for (RangerPolicy.RangerPolicyResource resource : llResource.getResourceElements().values()) {
                if (resource != null && Boolean.TRUE.equals(resource.getIsRecursive())) {
                    return true;
                }
            }

            return false;
        }

        private String getResourceValue(RangerServiceResource resource, String key) {
            if (resource == null || MapUtils.isEmpty(resource.getResourceElements())) {
                return null;
            }

            RangerPolicy.RangerPolicyResource policyResource = resource.getResourceElements().get(key);
            if (policyResource != null && CollectionUtils.isNotEmpty(policyResource.getValues())) {
                return policyResource.getValues().get(0);
            }

            return null;
        }
    }
}
