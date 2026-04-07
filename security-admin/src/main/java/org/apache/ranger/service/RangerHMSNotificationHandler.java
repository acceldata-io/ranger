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

package org.apache.ranger.service;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.RMSMgr;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * RangerHMSNotificationHandler handles HMS (Hive Metastore) notifications
 * and updates RMS mappings accordingly.
 *
 * This service can be configured to:
 * 1. Poll HMS for notification events (if HMS notification API is available)
 * 2. Receive push notifications from HMS (via REST callback)
 * 3. Accept manual mapping updates via REST API
 *
 * Configuration properties:
 * - ranger.rms.hms.enabled: Enable/disable HMS notification processing
 * - ranger.rms.hms.polling.interval.ms: Polling interval in milliseconds
 * - ranger.rms.hive.service.name: Name of the Hive service in Ranger
 * - ranger.rms.hdfs.service.name: Name of the HDFS service in Ranger
 * - ranger.rms.ozone.service.name: Name of the Ozone service in Ranger
 * - ranger.rms.s3.service.name: Name of the S3 service in Ranger
 * - ranger.rms.supported.uri.schemes: Comma-separated list of supported URI schemes
 */
@Service
public class RangerHMSNotificationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RangerHMSNotificationHandler.class);

    private static final String DEFAULT_HIVE_SERVICE_NAME = "cm_hive";
    private static final String DEFAULT_HDFS_SERVICE_NAME = "cm_hdfs";
    private static final String DEFAULT_OZONE_SERVICE_NAME = "cm_ozone";
    private static final String DEFAULT_S3_SERVICE_NAME = "cm_s3";
    private static final String DEFAULT_SUPPORTED_URI_SCHEMES = "hdfs,o3fs,ofs,s3a";

    private static final String HIVE_RESOURCE_DATABASE = "database";
    private static final String HIVE_RESOURCE_TABLE = "table";
    private static final String HIVE_RESOURCE_COLUMN = "column";

    private static final String HDFS_RESOURCE_PATH = "path";
    private static final String OZONE_RESOURCE_VOLUME = "volume";
    private static final String OZONE_RESOURCE_BUCKET = "bucket";
    private static final String OZONE_RESOURCE_KEY = "key";
    private static final String S3_RESOURCE_BUCKET = "bucket";
    private static final String S3_RESOURCE_PATH = "path";

    @Autowired
    private RMSMgr rmsMgr;

    @Value("${ranger.rms.hms.enabled:false}")
    private boolean hmsEnabled;

    @Value("${ranger.rms.hms.polling.interval.ms:30000}")
    private long pollingIntervalMs;

    @Value("${ranger.rms.hive.service.name:" + DEFAULT_HIVE_SERVICE_NAME + "}")
    private String hiveServiceName;

    @Value("${ranger.rms.hdfs.service.name:" + DEFAULT_HDFS_SERVICE_NAME + "}")
    private String hdfsServiceName;

    @Value("${ranger.rms.ozone.service.name:" + DEFAULT_OZONE_SERVICE_NAME + "}")
    private String ozoneServiceName;

    @Value("${ranger.rms.s3.service.name:" + DEFAULT_S3_SERVICE_NAME + "}")
    private String s3ServiceName;

    @Value("${ranger.rms.supported.uri.schemes:" + DEFAULT_SUPPORTED_URI_SCHEMES + "}")
    private String supportedUriSchemes;

    @Value("${ranger.rms.map.managed.tables:false}")
    private boolean mapManagedTables;

    private ScheduledExecutorService scheduler;

    @PostConstruct
    public void init() {
        LOG.info("==> RangerHMSNotificationHandler.init()");
        LOG.info("RMS HMS notification handler configuration:");
        LOG.info("  hmsEnabled: {}", hmsEnabled);
        LOG.info("  pollingIntervalMs: {}", pollingIntervalMs);
        LOG.info("  hiveServiceName: {}", hiveServiceName);
        LOG.info("  hdfsServiceName: {}", hdfsServiceName);
        LOG.info("  ozoneServiceName: {}", ozoneServiceName);
        LOG.info("  s3ServiceName: {}", s3ServiceName);
        LOG.info("  supportedUriSchemes: {}", supportedUriSchemes);
        LOG.info("  mapManagedTables: {}", mapManagedTables);

        if (hmsEnabled) {
            startPolling();
        }

        LOG.info("<== RangerHMSNotificationHandler.init()");
    }

    @PreDestroy
    public void cleanup() {
        LOG.info("==> RangerHMSNotificationHandler.cleanup()");
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("<== RangerHMSNotificationHandler.cleanup()");
    }

    private void startPolling() {
        LOG.info("Starting HMS notification polling with interval: {}ms", pollingIntervalMs);
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "RMS-HMS-Notification-Poller");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::pollHMSNotifications, 
            pollingIntervalMs, pollingIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void pollHMSNotifications() {
        LOG.debug("==> pollHMSNotifications()");
        LOG.debug("<== pollHMSNotifications()");
    }

    /**
     * Process a database creation event.
     */
    public void handleDatabaseCreate(String databaseName, String location) {
        LOG.info("==> handleDatabaseCreate(database={}, location={})", databaseName, location);

        if (StringUtils.isBlank(databaseName) || StringUtils.isBlank(location)) {
            LOG.warn("Invalid database create event: database={}, location={}", databaseName, location);
            return;
        }

        try {
            Map<String, RangerPolicy.RangerPolicyResource> hiveResource = createHiveDatabaseResource(databaseName);
            String llServiceName = getStorageServiceName(location);
            Map<String, RangerPolicy.RangerPolicyResource> storageResource = createStorageResource(location);

            if (llServiceName != null && storageResource != null) {
                rmsMgr.createOrUpdateMapping(hiveServiceName, hiveResource, llServiceName, storageResource, location);
            }
        } catch (Exception e) {
            LOG.error("Failed to handle database create event: database={}", databaseName, e);
        }

        LOG.info("<== handleDatabaseCreate(database={})", databaseName);
    }

    /**
     * Process a database drop event.
     */
    public void handleDatabaseDrop(String databaseName, String location) {
        LOG.info("==> handleDatabaseDrop(database={}, location={})", databaseName, location);

        if (StringUtils.isBlank(databaseName)) {
            return;
        }

        try {
            Map<String, RangerPolicy.RangerPolicyResource> hiveResource = createHiveDatabaseResource(databaseName);
            String llServiceName = getStorageServiceName(location);
            Map<String, RangerPolicy.RangerPolicyResource> storageResource = createStorageResource(location);

            if (llServiceName != null && storageResource != null) {
                rmsMgr.deleteMapping(hiveServiceName, hiveResource, llServiceName, storageResource);
            }
        } catch (Exception e) {
            LOG.error("Failed to handle database drop event: database={}", databaseName, e);
        }

        LOG.info("<== handleDatabaseDrop(database={})", databaseName);
    }

    /**
     * Process a table creation event.
     */
    public void handleTableCreate(String databaseName, String tableName, String location, boolean isManaged) {
        LOG.info("==> handleTableCreate(database={}, table={}, location={}, isManaged={})",
                 databaseName, tableName, location, isManaged);

        if (StringUtils.isBlank(databaseName) || StringUtils.isBlank(tableName) || StringUtils.isBlank(location)) {
            LOG.warn("Invalid table create event");
            return;
        }

        if (isManaged && !mapManagedTables) {
            LOG.debug("Skipping managed table: {}.{}", databaseName, tableName);
            return;
        }

        try {
            Map<String, RangerPolicy.RangerPolicyResource> hiveResource = createHiveTableResource(databaseName, tableName);
            String llServiceName = getStorageServiceName(location);
            Map<String, RangerPolicy.RangerPolicyResource> storageResource = createStorageResource(location);

            if (llServiceName != null && storageResource != null) {
                rmsMgr.createOrUpdateMapping(hiveServiceName, hiveResource, llServiceName, storageResource, location);
            }
        } catch (Exception e) {
            LOG.error("Failed to handle table create event: {}.{}", databaseName, tableName, e);
        }

        LOG.info("<== handleTableCreate(database={}, table={})", databaseName, tableName);
    }

    /**
     * Process a table drop event.
     */
    public void handleTableDrop(String databaseName, String tableName, String location) {
        LOG.info("==> handleTableDrop(database={}, table={}, location={})", databaseName, tableName, location);

        if (StringUtils.isBlank(databaseName) || StringUtils.isBlank(tableName)) {
            return;
        }

        try {
            Map<String, RangerPolicy.RangerPolicyResource> hiveResource = createHiveTableResource(databaseName, tableName);
            String llServiceName = getStorageServiceName(location);
            Map<String, RangerPolicy.RangerPolicyResource> storageResource = createStorageResource(location);

            if (llServiceName != null && storageResource != null) {
                rmsMgr.deleteMapping(hiveServiceName, hiveResource, llServiceName, storageResource);
            }
        } catch (Exception e) {
            LOG.error("Failed to handle table drop event: {}.{}", databaseName, tableName, e);
        }

        LOG.info("<== handleTableDrop(database={}, table={})", databaseName, tableName);
    }

    /**
     * Process a table alter event (location change).
     */
    public void handleTableAlter(String databaseName, String tableName, String oldLocation, String newLocation) {
        LOG.info("==> handleTableAlter(database={}, table={}, oldLocation={}, newLocation={})",
                 databaseName, tableName, oldLocation, newLocation);

        if (StringUtils.isNotBlank(oldLocation)) {
            handleTableDrop(databaseName, tableName, oldLocation);
        }

        if (StringUtils.isNotBlank(newLocation)) {
            handleTableCreate(databaseName, tableName, newLocation, false);
        }

        LOG.info("<== handleTableAlter(database={}, table={})", databaseName, tableName);
    }

    /**
     * Create Hive database resource elements.
     */
    private Map<String, RangerPolicy.RangerPolicyResource> createHiveDatabaseResource(String databaseName) {
        Map<String, RangerPolicy.RangerPolicyResource> ret = new HashMap<>();
        ret.put(HIVE_RESOURCE_DATABASE, new RangerPolicy.RangerPolicyResource(databaseName));
        return ret;
    }

    /**
     * Create Hive table resource elements.
     */
    private Map<String, RangerPolicy.RangerPolicyResource> createHiveTableResource(String databaseName, String tableName) {
        Map<String, RangerPolicy.RangerPolicyResource> ret = new HashMap<>();
        ret.put(HIVE_RESOURCE_DATABASE, new RangerPolicy.RangerPolicyResource(databaseName));
        ret.put(HIVE_RESOURCE_TABLE, new RangerPolicy.RangerPolicyResource(tableName));
        return ret;
    }

    /**
     * Get the storage service name based on the location URI scheme.
     */
    private String getStorageServiceName(String location) {
        if (StringUtils.isBlank(location)) {
            return null;
        }

        try {
            URI uri = new URI(location);
            String scheme = uri.getScheme();

            if (StringUtils.isBlank(scheme)) {
                return hdfsServiceName;
            }

            scheme = scheme.toLowerCase();

            if (!isSupportedScheme(scheme)) {
                LOG.warn("Unsupported URI scheme: {}", scheme);
                return null;
            }

            if ("hdfs".equals(scheme)) {
                return hdfsServiceName;
            } else if ("o3fs".equals(scheme) || "ofs".equals(scheme)) {
                return ozoneServiceName;
            } else if ("s3a".equals(scheme) || "s3".equals(scheme) || "s3n".equals(scheme)) {
                return s3ServiceName;
            }

            return hdfsServiceName;

        } catch (Exception e) {
            LOG.error("Failed to parse location URI: {}", location, e);
            return null;
        }
    }

    /**
     * Check if a URI scheme is supported.
     */
    private boolean isSupportedScheme(String scheme) {
        if (StringUtils.isBlank(supportedUriSchemes)) {
            return true;
        }

        for (String supported : supportedUriSchemes.split(",")) {
            if (supported.trim().equalsIgnoreCase(scheme)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Create storage resource elements based on the location.
     */
    private Map<String, RangerPolicy.RangerPolicyResource> createStorageResource(String location) {
        if (StringUtils.isBlank(location)) {
            return null;
        }

        try {
            URI uri = new URI(location);
            String scheme = uri.getScheme();

            if (StringUtils.isBlank(scheme)) {
                scheme = "hdfs";
            }

            scheme = scheme.toLowerCase();

            if ("hdfs".equals(scheme)) {
                return createHdfsResource(uri);
            } else if ("o3fs".equals(scheme) || "ofs".equals(scheme)) {
                return createOzoneResource(uri, scheme);
            } else if ("s3a".equals(scheme) || "s3".equals(scheme) || "s3n".equals(scheme)) {
                return createS3Resource(uri);
            }

            return createHdfsResource(uri);

        } catch (Exception e) {
            LOG.error("Failed to create storage resource for location: {}", location, e);
            return null;
        }
    }

    /**
     * Create HDFS resource elements.
     */
    private Map<String, RangerPolicy.RangerPolicyResource> createHdfsResource(URI uri) {
        Map<String, RangerPolicy.RangerPolicyResource> ret = new HashMap<>();
        String path = uri.getPath();
        if (StringUtils.isNotBlank(path)) {
            RangerPolicy.RangerPolicyResource pathResource = new RangerPolicy.RangerPolicyResource(path);
            pathResource.setIsRecursive(true);
            ret.put(HDFS_RESOURCE_PATH, pathResource);
        }
        return ret;
    }

    /**
     * Create Ozone resource elements.
     */
    private Map<String, RangerPolicy.RangerPolicyResource> createOzoneResource(URI uri, String scheme) {
        Map<String, RangerPolicy.RangerPolicyResource> ret = new HashMap<>();

        if ("ofs".equals(scheme)) {
            String path = uri.getPath();
            if (StringUtils.isNotBlank(path)) {
                String[] parts = path.split("/");
                if (parts.length >= 2) {
                    ret.put(OZONE_RESOURCE_VOLUME, new RangerPolicy.RangerPolicyResource(parts[1]));
                    if (parts.length >= 3) {
                        ret.put(OZONE_RESOURCE_BUCKET, new RangerPolicy.RangerPolicyResource(parts[2]));
                        if (parts.length >= 4) {
                            String key = String.join("/", java.util.Arrays.copyOfRange(parts, 3, parts.length));
                            RangerPolicy.RangerPolicyResource keyResource = new RangerPolicy.RangerPolicyResource(key);
                            keyResource.setIsRecursive(true);
                            ret.put(OZONE_RESOURCE_KEY, keyResource);
                        }
                    }
                }
            }
        } else {
            String host = uri.getHost();
            String path = uri.getPath();

            if (StringUtils.isNotBlank(host)) {
                String[] hostParts = host.split("\\.");
                if (hostParts.length >= 2) {
                    ret.put(OZONE_RESOURCE_BUCKET, new RangerPolicy.RangerPolicyResource(hostParts[0]));
                    ret.put(OZONE_RESOURCE_VOLUME, new RangerPolicy.RangerPolicyResource(hostParts[1]));
                }
            }

            if (StringUtils.isNotBlank(path) && !"/".equals(path)) {
                String key = path.startsWith("/") ? path.substring(1) : path;
                RangerPolicy.RangerPolicyResource keyResource = new RangerPolicy.RangerPolicyResource(key);
                keyResource.setIsRecursive(true);
                ret.put(OZONE_RESOURCE_KEY, keyResource);
            }
        }

        return ret;
    }

    /**
     * Create S3 resource elements.
     */
    private Map<String, RangerPolicy.RangerPolicyResource> createS3Resource(URI uri) {
        Map<String, RangerPolicy.RangerPolicyResource> ret = new HashMap<>();

        String bucket = uri.getHost();
        String path = uri.getPath();

        if (StringUtils.isNotBlank(bucket)) {
            ret.put(S3_RESOURCE_BUCKET, new RangerPolicy.RangerPolicyResource(bucket));
        }

        if (StringUtils.isNotBlank(path) && !"/".equals(path)) {
            String s3Path = path.startsWith("/") ? path.substring(1) : path;
            RangerPolicy.RangerPolicyResource pathResource = new RangerPolicy.RangerPolicyResource(s3Path);
            pathResource.setIsRecursive(true);
            ret.put(S3_RESOURCE_PATH, pathResource);
        }

        return ret;
    }

    public boolean isHmsEnabled() {
        return hmsEnabled;
    }

    public String getHiveServiceName() {
        return hiveServiceName;
    }

    public String getHdfsServiceName() {
        return hdfsServiceName;
    }

    public String getOzoneServiceName() {
        return ozoneServiceName;
    }

    public String getS3ServiceName() {
        return s3ServiceName;
    }
}
