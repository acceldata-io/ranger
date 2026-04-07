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
import java.util.concurrent.ConcurrentHashMap;

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
    private final Map<String, RangerServiceResource> resourcesByGuid = new ConcurrentHashMap<>();
    private final Map<String, List<MappingEntry>> mappingsByLlPath = new ConcurrentHashMap<>();
    private final Map<String, MappingEntry> mappingsByHlResource = new ConcurrentHashMap<>();

    public RangerRMSMappingCache(String serviceName) {
        this.serviceName = serviceName;
        this.hlServiceName = null;
        this.mappingVersion = 0L;
    }

    /**
     * Update the cache with new mappings.
     */
    public synchronized void update(ServiceRMSMappings rmsMappings) {
        if (rmsMappings == null) {
            return;
        }

        LOG.debug("==> RangerRMSMappingCache.update(mappingVersion={})", rmsMappings.getMappingVersion());

        resourcesByGuid.clear();
        mappingsByLlPath.clear();
        mappingsByHlResource.clear();

        if (MapUtils.isNotEmpty(rmsMappings.getServiceResources())) {
            resourcesByGuid.putAll(rmsMappings.getServiceResources());
        }

        if (CollectionUtils.isNotEmpty(rmsMappings.getResourceMappings())) {
            for (RMSResourceMapping mapping : rmsMappings.getResourceMappings()) {
                processMapping(mapping);
            }
        }

        this.mappingVersion = rmsMappings.getMappingVersion();

        LOG.debug("<== RangerRMSMappingCache.update(): resourceCount={}, mappingCount={}",
                  resourcesByGuid.size(), mappingsByLlPath.size());
    }

    private void processMapping(RMSResourceMapping mapping) {
        if (mapping == null) {
            return;
        }

        String hlGuid = mapping.getHlResourceGuid();
        String llGuid = mapping.getLlResourceGuid();

        if (StringUtils.isBlank(hlGuid) || StringUtils.isBlank(llGuid)) {
            return;
        }

        RangerServiceResource hlResource = resourcesByGuid.get(hlGuid);
        RangerServiceResource llResource = resourcesByGuid.get(llGuid);

        if (hlResource == null || llResource == null) {
            return;
        }

        String llPath = extractPath(llResource);
        if (StringUtils.isBlank(llPath)) {
            return;
        }

        MappingEntry entry = new MappingEntry(hlResource, llResource, mapping);

        mappingsByLlPath.computeIfAbsent(normalizePathForLookup(llPath), k -> new ArrayList<>()).add(entry);

        String hlKey = buildHlResourceKey(hlResource);
        if (StringUtils.isNotBlank(hlKey)) {
            mappingsByHlResource.put(hlKey, entry);
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

        String normalizedPath = normalizePathForLookup(path);
        MappingEntry ret = null;

        List<MappingEntry> exactMatches = mappingsByLlPath.get(normalizedPath);
        if (CollectionUtils.isNotEmpty(exactMatches)) {
            ret = exactMatches.get(0);
        }

        if (ret == null) {
            ret = findMappingForParentPath(normalizedPath);
        }

        LOG.debug("<== findMappingForPath({}): found={}", path, ret != null);
        return ret;
    }

    /**
     * Find mapping by traversing parent paths.
     */
    private MappingEntry findMappingForParentPath(String path) {
        if (StringUtils.isBlank(path)) {
            return null;
        }

        String parentPath = getParentPath(path);
        while (StringUtils.isNotBlank(parentPath)) {
            List<MappingEntry> matches = mappingsByLlPath.get(parentPath);
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
        return mappingsByHlResource.get(key);
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

        return normalized.toLowerCase();
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
        return mappingsByLlPath.size();
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
