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

package org.apache.ranger.rms;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.RMSMgr;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.rms.HMSClientWrapper.DatabaseInfo;
import org.apache.ranger.rms.HMSClientWrapper.TableInfo;
import org.apache.ranger.rms.HMSClientWrapper.NotificationEventInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RangerRMSPollerService polls HMS (Hive Metastore) for notification events
 * and updates RMS mappings accordingly.
 *
 * RMS polls HMS via Thrift to discover table/database metadata and their
 * storage locations, eliminating the need for an HMS listener configuration.
 *
 * Configuration properties (in ranger-admin-site.xml):
 * - ranger.rms.enabled: Enable/disable RMS (default: false)
 * - ranger.rms.hms.uri: HMS Thrift URI (e.g., thrift://hms-host:9083)
 * - ranger.rms.polling.notifications.frequency.ms: Polling interval (default: 30000)
 * - ranger.rms.hive.service.name: Hive service name in Ranger
 * - ranger.rms.hdfs.service.name: HDFS service name in Ranger
 * - ranger.rms.ozone.service.name: Ozone service name in Ranger
 * - ranger.rms.s3.service.name: S3 service name in Ranger
 * - ranger.rms.supported.uri.schemes: Supported URI schemes (default: hdfs,o3fs,ofs,s3a)
 * - ranger.rms.map.managed.tables: Track managed tables (default: true)
 */
@Service
@Lazy(false)
public class RangerRMSPollerService {
    private static final Logger LOG = LoggerFactory.getLogger(RangerRMSPollerService.class);

    private static final String CONFIG_RMS_ENABLED = "ranger.rms.enabled";
    private static final String CONFIG_HMS_URI = "ranger.rms.hms.uri";
    private static final String CONFIG_POLLING_INTERVAL = "ranger.rms.polling.notifications.frequency.ms";
    private static final String CONFIG_HIVE_SERVICE_NAME = "ranger.rms.hive.service.name";
    private static final String CONFIG_HDFS_SERVICE_NAME = "ranger.rms.hdfs.service.name";
    private static final String CONFIG_OZONE_SERVICE_NAME = "ranger.rms.ozone.service.name";
    private static final String CONFIG_S3_SERVICE_NAME = "ranger.rms.s3.service.name";
    private static final String CONFIG_SUPPORTED_URI_SCHEMES = "ranger.rms.supported.uri.schemes";
    private static final String CONFIG_MAP_MANAGED_TABLES = "ranger.rms.map.managed.tables";
    private static final String CONFIG_INITIAL_FULL_SYNC = "ranger.rms.initial.full.sync";
    private static final String CONFIG_HMS_SASL_ENABLED = "ranger.rms.hms.sasl.enabled";
    private static final String CONFIG_HMS_KERBEROS_PRINCIPAL = "ranger.rms.hms.kerberos.principal";
    private static final String CONFIG_HMS_SSL_ENABLED = "ranger.rms.hms.ssl.enabled";
    private static final String CONFIG_HMS_TRUSTSTORE_PATH = "ranger.rms.hms.ssl.truststore.path";
    private static final String CONFIG_HMS_TRUSTSTORE_PASSWORD = "ranger.rms.hms.ssl.truststore.password";
    // Client-side principal/keytab the RMS poller logs in as when SASL is enabled.
    // Falls back to ranger.admin.kerberos.* if not specified.
    private static final String CONFIG_HMS_CLIENT_PRINCIPAL = "ranger.rms.hms.kerberos.client.principal";
    private static final String CONFIG_HMS_CLIENT_KEYTAB = "ranger.rms.hms.kerberos.client.keytab";
    private static final String CONFIG_RANGER_ADMIN_PRINCIPAL = "ranger.admin.kerberos.principal";
    private static final String CONFIG_RANGER_ADMIN_KEYTAB = "ranger.admin.kerberos.keytab";

    private static final String DEFAULT_HIVE_SERVICE_NAME = "hive";
    private static final String DEFAULT_HDFS_SERVICE_NAME = "hdfs";
    private static final String DEFAULT_OZONE_SERVICE_NAME = "ozone";
    private static final String DEFAULT_S3_SERVICE_NAME = "s3";
    private static final String DEFAULT_SUPPORTED_URI_SCHEMES = "hdfs,o3fs,ofs,s3a";
    private static final long DEFAULT_POLLING_INTERVAL_MS = 30000;

    private static final String EVENT_CREATE_DATABASE = "CREATE_DATABASE";
    private static final String EVENT_DROP_DATABASE = "DROP_DATABASE";
    private static final String EVENT_ALTER_DATABASE = "ALTER_DATABASE";
    private static final String EVENT_CREATE_TABLE = "CREATE_TABLE";
    private static final String EVENT_DROP_TABLE = "DROP_TABLE";
    private static final String EVENT_ALTER_TABLE = "ALTER_TABLE";
    private static final String EVENT_RENAME_TABLE = "RENAME_TABLE";

    private static final String HIVE_RESOURCE_DATABASE = "database";
    private static final String HIVE_RESOURCE_TABLE = "table";
    private static final String HDFS_RESOURCE_PATH = "path";
    private static final String OZONE_RESOURCE_VOLUME = "volume";
    private static final String OZONE_RESOURCE_BUCKET = "bucket";
    private static final String OZONE_RESOURCE_KEY = "key";
    private static final String S3_RESOURCE_BUCKET = "bucket";
    private static final String S3_RESOURCE_PATH = "path";

    @Autowired
    private RMSMgr rmsMgr;

    private boolean enabled;
    private String hmsUri;
    private long pollingIntervalMs;
    private String hiveServiceName;
    private String hdfsServiceName;
    private String ozoneServiceName;
    private String s3ServiceName;
    private Set<String> supportedUriSchemes;
    private boolean mapManagedTables;
    private boolean initialFullSync;
    private boolean hmsSaslEnabled;
    private String hmsKerberosPrincipal;
    private String hmsClientPrincipal;
    private String hmsClientKeytab;
    private boolean hmsSslEnabled;
    private String hmsTruststorePath;
    private String hmsTruststorePassword;
    private boolean kerberosLoginAttempted = false;

    private ScheduledExecutorService scheduler;
    private HMSClientWrapper hmsClient;
    private AtomicLong lastEventId = new AtomicLong(-1);
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private AtomicBoolean fullSyncCompleted = new AtomicBoolean(false);

    @PostConstruct
    public void init() {
        LOG.info("==> RangerRMSPollerService.init()");

        loadConfiguration();

        LOG.info("RMS Poller Configuration:");
        LOG.info("  enabled: {}", enabled);
        LOG.info("  hmsUri: {}", hmsUri);
        LOG.info("  pollingIntervalMs: {}", pollingIntervalMs);
        LOG.info("  hiveServiceName: {}", hiveServiceName);
        LOG.info("  hdfsServiceName: {}", hdfsServiceName);
        LOG.info("  ozoneServiceName: {}", ozoneServiceName);
        LOG.info("  s3ServiceName: {}", s3ServiceName);
        LOG.info("  supportedUriSchemes: {}", supportedUriSchemes);
        LOG.info("  mapManagedTables: {}", mapManagedTables);
        LOG.info("  initialFullSync: {}", initialFullSync);
        LOG.info("  hmsSaslEnabled: {}", hmsSaslEnabled);
        LOG.info("  hmsKerberosPrincipal: {}", hmsKerberosPrincipal);
        LOG.info("  hmsClientPrincipal:   {}", hmsClientPrincipal);
        LOG.info("  hmsClientKeytab:      {}", hmsClientKeytab);
        LOG.info("  hmsSslEnabled: {}", hmsSslEnabled);
        LOG.info("  hmsTruststorePath: {}", hmsTruststorePath);

        if (enabled) {
            restoreState();
            startPolling();
        } else {
            LOG.info("RMS is disabled. Polling will not start.");
        }

        LOG.info("<== RangerRMSPollerService.init()");
    }

    private void loadConfiguration() {
        enabled = PropertiesUtil.getBooleanProperty(CONFIG_RMS_ENABLED, false);
        hmsUri = PropertiesUtil.getProperty(CONFIG_HMS_URI, "");
        pollingIntervalMs = PropertiesUtil.getLongProperty(CONFIG_POLLING_INTERVAL, DEFAULT_POLLING_INTERVAL_MS);
        hiveServiceName = PropertiesUtil.getProperty(CONFIG_HIVE_SERVICE_NAME, DEFAULT_HIVE_SERVICE_NAME);
        hdfsServiceName = PropertiesUtil.getProperty(CONFIG_HDFS_SERVICE_NAME, DEFAULT_HDFS_SERVICE_NAME);
        ozoneServiceName = PropertiesUtil.getProperty(CONFIG_OZONE_SERVICE_NAME, DEFAULT_OZONE_SERVICE_NAME);
        s3ServiceName = PropertiesUtil.getProperty(CONFIG_S3_SERVICE_NAME, DEFAULT_S3_SERVICE_NAME);
        mapManagedTables = PropertiesUtil.getBooleanProperty(CONFIG_MAP_MANAGED_TABLES, true);
        initialFullSync = PropertiesUtil.getBooleanProperty(CONFIG_INITIAL_FULL_SYNC, true);

        hmsSaslEnabled = PropertiesUtil.getBooleanProperty(CONFIG_HMS_SASL_ENABLED, false);
        hmsKerberosPrincipal = PropertiesUtil.getProperty(CONFIG_HMS_KERBEROS_PRINCIPAL, "");
        hmsSslEnabled = PropertiesUtil.getBooleanProperty(CONFIG_HMS_SSL_ENABLED, false);
        hmsTruststorePath = PropertiesUtil.getProperty(CONFIG_HMS_TRUSTSTORE_PATH, "");
        hmsTruststorePassword = PropertiesUtil.getProperty(CONFIG_HMS_TRUSTSTORE_PASSWORD, "");

        // Client identity for the SASL/Kerberos handshake. Prefer RMS-specific keys,
        // fall back to ranger.admin.kerberos.{principal,keytab} so a single set of
        // admin credentials suffices in the common case.
        hmsClientPrincipal = PropertiesUtil.getProperty(CONFIG_HMS_CLIENT_PRINCIPAL, "");
        if (StringUtils.isBlank(hmsClientPrincipal)) {
            hmsClientPrincipal = PropertiesUtil.getProperty(CONFIG_RANGER_ADMIN_PRINCIPAL, "");
        }
        hmsClientKeytab = PropertiesUtil.getProperty(CONFIG_HMS_CLIENT_KEYTAB, "");
        if (StringUtils.isBlank(hmsClientKeytab)) {
            hmsClientKeytab = PropertiesUtil.getProperty(CONFIG_RANGER_ADMIN_KEYTAB, "");
        }

        String schemes = PropertiesUtil.getProperty(CONFIG_SUPPORTED_URI_SCHEMES, DEFAULT_SUPPORTED_URI_SCHEMES);
        supportedUriSchemes = new HashSet<>();
        for (String scheme : schemes.split(",")) {
            supportedUriSchemes.add(scheme.trim().toLowerCase());
        }
    }

    private void restoreState() {
        try {
            LOG.info("Attempting to restore RMS poller state from database...");
            long persistedEventId = rmsMgr.getLastProcessedEventId();
            boolean hasMappings = rmsMgr.hasExistingMappings();
            LOG.info("DB state: persistedEventId={}, hasMappings={}", persistedEventId, hasMappings);

            if (persistedEventId >= 0 && hasMappings) {
                lastEventId.set(persistedEventId);
                fullSyncCompleted.set(true);
                LOG.info("Restored RMS poller state: lastEventId={}, skipping full sync", persistedEventId);
            } else {
                LOG.info("No persisted RMS state found (eventId={}, hasMappings={}), will perform full sync", persistedEventId, hasMappings);
            }
        } catch (Exception e) {
            LOG.warn("Failed to restore RMS poller state, will perform full sync", e);
        }
    }

    @PreDestroy
    public void cleanup() {
        LOG.info("==> RangerRMSPollerService.cleanup()");
        stopPolling();
        persistState();
        closeHMSClient();
        LOG.info("<== RangerRMSPollerService.cleanup()");
    }

    private void persistState() {
        try {
            long eventId = lastEventId.get();
            if (eventId >= 0) {
                rmsMgr.saveLastProcessedEventId(eventId);
                LOG.info("Persisted RMS poller state: lastEventId={}", eventId);
            } else {
                LOG.debug("Not persisting state: eventId={}", eventId);
            }
        } catch (Exception e) {
            LOG.error("Failed to persist RMS poller state (eventId={})", lastEventId.get(), e);
        }
    }

    private void startPolling() {
        LOG.info("Starting RMS HMS polling with interval: {}ms", pollingIntervalMs);
        
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "RMS-HMS-Poller");
            t.setDaemon(true);
            return t;
        });

        // Initial delay to allow Ranger Admin to fully start
        long initialDelay = 10000;

        scheduler.scheduleAtFixedRate(this::pollHMS, initialDelay, pollingIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("RMS HMS polling scheduled with initial delay: {}ms, interval: {}ms", initialDelay, pollingIntervalMs);
    }

    private void stopPolling() {
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
    }

    private void pollHMS() {
        if (!isRunning.compareAndSet(false, true)) {
            LOG.debug("Previous polling cycle still running, skipping...");
            return;
        }

        try {
            LOG.debug("==> pollHMS()");

            HMSClientWrapper client = getHMSClient();
            if (client == null) {
                LOG.warn("Failed to get HMS client, skipping poll cycle");
                return;
            }

            // Perform full sync on first run if enabled
            if (initialFullSync && !fullSyncCompleted.get()) {
                LOG.info("Performing initial full sync from HMS...");
                performFullSync(client);
                fullSyncCompleted.set(true);
                
                long currentId = client.getCurrentNotificationEventId();
                lastEventId.set(currentId);
                persistState();
                LOG.info("Full sync completed. Current event ID: {}", lastEventId.get());
                return;
            }

            // Get current event ID
            long currentEventId = client.getCurrentNotificationEventId();

            if (lastEventId.get() < 0) {
                // First time, just record the event ID
                lastEventId.set(currentEventId);
                LOG.info("Initialized last event ID: {}", lastEventId.get());
                return;
            }

            if (currentEventId == lastEventId.get()) {
                LOG.debug("No new events since last poll (eventId={})", currentEventId);
                return;
            }

            // Fetch notification events
            LOG.info("Fetching HMS notifications from eventId {} to {}", lastEventId.get(), currentEventId);
            
            try {
                List<NotificationEventInfo> events = client.getNextNotification(lastEventId.get(), 1000);

                if (events != null && !events.isEmpty()) {
                    LOG.info("Processing {} HMS notification events", events.size());
                    
                    for (NotificationEventInfo event : events) {
                        processNotificationEvent(client, event);
                        lastEventId.set(event.eventId);
                    }
                } else {
                    // No events returned but ID advanced - skip ahead to avoid stuck watermark
                    lastEventId.set(currentEventId);
                    LOG.info("No notification events returned, advancing watermark to {}", currentEventId);
                }
            } catch (Exception e) {
                LOG.warn("Error fetching notifications, advancing watermark to {}", currentEventId);
                lastEventId.set(currentEventId);
            }

            persistState();
            LOG.debug("<== pollHMS() processed events up to eventId={}", lastEventId.get());

        } catch (Exception e) {
            LOG.error("Error polling HMS for notifications", e);
            closeHMSClient();
        } finally {
            isRunning.set(false);
        }
    }

    private void performFullSync(HMSClientWrapper client) throws Exception {
        LOG.info("==> performFullSync()");

        try {
            // Get all databases
            List<String> databases = client.getAllDatabases();
            LOG.info("Found {} databases in HMS", databases.size());

            for (String dbName : databases) {
                try {
                    // Skip system databases
                    if ("sys".equalsIgnoreCase(dbName) || "information_schema".equalsIgnoreCase(dbName)) {
                        continue;
                    }

                    DatabaseInfo db = client.getDatabase(dbName);
                    if (db != null) {
                        String dbLocation = db.locationUri;

                        // Process database
                        if (StringUtils.isNotBlank(dbLocation) && isSupportedLocation(dbLocation)) {
                            processCreateDatabase(dbName, dbLocation);
                        }
                    }

                    // Get all tables in database
                    List<String> tables = client.getAllTables(dbName);
                    LOG.info("Database {}: found {} tables", dbName, tables.size());

                    for (String tableName : tables) {
                        try {
                            TableInfo table = client.getTable(dbName, tableName);
                            processTable(table);
                        } catch (Exception e) {
                            LOG.warn("Error processing table {}.{}: {}", dbName, tableName, e.getMessage());
                        }
                    }

                } catch (Exception e) {
                    LOG.warn("Error processing database {}: {}", dbName, e.getMessage());
                }
            }

        } catch (Exception e) {
            LOG.error("Error during full sync", e);
            throw e;
        }

        LOG.info("<== performFullSync()");
    }

    private void processNotificationEvent(HMSClientWrapper client, NotificationEventInfo event) {
        String eventType = event.eventType;
        String dbName = event.dbName;
        String tableName = event.tableName;

        LOG.debug("Processing event: type={}, db={}, table={}, eventId={}", 
                  eventType, dbName, tableName, event.eventId);

        try {
            switch (eventType) {
                case EVENT_CREATE_DATABASE:
                    handleCreateDatabase(client, dbName);
                    break;

                case EVENT_DROP_DATABASE:
                    handleDropDatabase(dbName);
                    break;

                case EVENT_ALTER_DATABASE:
                    handleAlterDatabase(client, dbName);
                    break;

                case EVENT_CREATE_TABLE:
                    handleCreateTable(client, dbName, tableName);
                    break;

                case EVENT_DROP_TABLE:
                    handleDropTable(dbName, tableName);
                    break;

                case EVENT_ALTER_TABLE:
                    handleAlterTable(client, dbName, tableName);
                    break;

                case EVENT_RENAME_TABLE:
                    handleRenameTable(client, event);
                    break;

                default:
                    LOG.debug("Ignoring event type: {}", eventType);
            }
        } catch (Exception e) {
            LOG.error("Error processing event: type={}, db={}, table={}", eventType, dbName, tableName, e);
        }
    }

    private void handleCreateDatabase(HMSClientWrapper client, String dbName) throws Exception {
        DatabaseInfo db = client.getDatabase(dbName);
        if (db != null) {
            String location = db.locationUri;
            
            if (StringUtils.isNotBlank(location) && isSupportedLocation(location)) {
                processCreateDatabase(dbName, location);
            }
        }
    }

    private void handleDropDatabase(String dbName) {
        LOG.info("Processing DROP_DATABASE: {}", dbName);
        try {
            Map<String, RangerPolicy.RangerPolicyResource> hiveResource = createHiveDatabaseResource(dbName);
            rmsMgr.deleteMappingsByHlResource(hiveServiceName, hiveResource);
            LOG.info("Deleted RMS mappings for dropped database: {}", dbName);
        } catch (Exception e) {
            LOG.error("Error deleting mappings for dropped database: {}", dbName, e);
        }
    }

    private void handleAlterDatabase(HMSClientWrapper client, String dbName) throws Exception {
        DatabaseInfo db = client.getDatabase(dbName);
        if (db != null) {
            String location = db.locationUri;
            
            if (StringUtils.isNotBlank(location) && isSupportedLocation(location)) {
                processCreateDatabase(dbName, location);
            }
        }
    }

    private void handleCreateTable(HMSClientWrapper client, String dbName, String tableName) throws Exception {
        TableInfo table = client.getTable(dbName, tableName);
        processTable(table);
    }

    private void handleDropTable(String dbName, String tableName) {
        LOG.info("Processing DROP_TABLE: {}.{}", dbName, tableName);
        try {
            Map<String, RangerPolicy.RangerPolicyResource> hiveResource = createHiveTableResource(dbName, tableName);
            rmsMgr.deleteMappingsByHlResource(hiveServiceName, hiveResource);
            LOG.info("Deleted RMS mappings for dropped table: {}.{}", dbName, tableName);
        } catch (Exception e) {
            LOG.error("Error deleting mappings for dropped table: {}.{}", dbName, tableName, e);
        }
    }

    private void handleAlterTable(HMSClientWrapper client, String dbName, String tableName) throws Exception {
        TableInfo table = client.getTable(dbName, tableName);
        processTable(table);
    }

    private void handleRenameTable(HMSClientWrapper client, NotificationEventInfo event) {
        String dbName = event.dbName;
        String oldTableName = event.tableName;

        LOG.info("Processing RENAME_TABLE: {}.{}", dbName, oldTableName);

        try {
            Map<String, RangerPolicy.RangerPolicyResource> oldHiveResource = createHiveTableResource(dbName, oldTableName);
            rmsMgr.deleteMappingsByHlResource(hiveServiceName, oldHiveResource);
            LOG.info("Deleted old RMS mappings for renamed table: {}.{}", dbName, oldTableName);
        } catch (Exception e) {
            LOG.error("Error deleting old mappings for renamed table: {}.{}", dbName, oldTableName, e);
        }

        String newTableName = extractNewTableName(event.message);
        if (StringUtils.isBlank(newTableName)) {
            LOG.warn("Could not extract new table name from RENAME_TABLE event for {}.{}, skipping new mapping creation", dbName, oldTableName);
            return;
        }

        LOG.info("RENAME_TABLE: {}.{} -> {}.{}", dbName, oldTableName, dbName, newTableName);

        try {
            TableInfo newTable = client.getTable(dbName, newTableName);
            processTable(newTable);
        } catch (Exception e) {
            LOG.error("Error creating new mappings for renamed table: {}.{}", dbName, newTableName, e);
        }
    }

    /**
     * Extract the new table name from a RENAME_TABLE event message JSON.
     * Hive 4 message format: {"table":{"tableName":"new_name","dbName":"mydb",...}}
     */
    private String extractNewTableName(String message) {
        if (StringUtils.isBlank(message)) {
            return null;
        }

        try {
            int tableObjIdx = message.indexOf("\"table\"");
            if (tableObjIdx < 0) {
                tableObjIdx = message.indexOf("\"tableObjAfterRename\"");
            }
            if (tableObjIdx < 0) {
                return null;
            }

            String afterTableKey = message.substring(tableObjIdx);
            int tableNameIdx = afterTableKey.indexOf("\"tableName\"");
            if (tableNameIdx < 0) {
                return null;
            }

            String afterTableName = afterTableKey.substring(tableNameIdx + "\"tableName\"".length());
            int colonIdx = afterTableName.indexOf(':');
            if (colonIdx < 0) {
                return null;
            }

            String afterColon = afterTableName.substring(colonIdx + 1).trim();
            if (afterColon.startsWith("\"")) {
                int endQuote = afterColon.indexOf('"', 1);
                if (endQuote > 0) {
                    return afterColon.substring(1, endQuote);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse RENAME_TABLE message: {}", e.getMessage());
        }

        return null;
    }

    private void processTable(TableInfo table) {
        if (table == null) {
            return;
        }

        String dbName = table.dbName;
        String tableName = table.tableName;
        String location = table.location;
        boolean isManaged = table.isManaged();

        if (!mapManagedTables && isManaged) {
            LOG.debug("Skipping managed table: {}.{}", dbName, tableName);
            return;
        }

        if (StringUtils.isBlank(location) || !isSupportedLocation(location)) {
            LOG.debug("Skipping table with unsupported location: {}.{} -> {}", dbName, tableName, location);
            return;
        }

        LOG.info("Processing table: {}.{} -> {} (managed={})", dbName, tableName, location, isManaged);

        try {
            Map<String, RangerPolicy.RangerPolicyResource> hiveResource = createHiveTableResource(dbName, tableName);
            String llServiceName = getStorageServiceName(location);
            Map<String, RangerPolicy.RangerPolicyResource> storageResource = createStorageResource(location);

            if (llServiceName != null && storageResource != null) {
                rmsMgr.createOrUpdateMapping(hiveServiceName, hiveResource, llServiceName, storageResource, location);
            }
        } catch (Exception e) {
            LOG.error("Failed to create mapping for table {}.{}", dbName, tableName, e);
        }
    }

    private void processCreateDatabase(String dbName, String location) {
        LOG.info("Processing database: {} -> {}", dbName, location);

        try {
            Map<String, RangerPolicy.RangerPolicyResource> hiveResource = createHiveDatabaseResource(dbName);
            String llServiceName = getStorageServiceName(location);
            Map<String, RangerPolicy.RangerPolicyResource> storageResource = createStorageResource(location);

            if (llServiceName != null && storageResource != null) {
                rmsMgr.createOrUpdateMapping(hiveServiceName, hiveResource, llServiceName, storageResource, location);
            }
        } catch (Exception e) {
            LOG.error("Failed to create mapping for database {}", dbName, e);
        }
    }

    private boolean isSupportedLocation(String location) {
        if (StringUtils.isBlank(location)) {
            return false;
        }

        try {
            URI uri = new URI(location);
            String scheme = uri.getScheme();
            
            if (StringUtils.isBlank(scheme)) {
                // No scheme means local or HDFS default
                return supportedUriSchemes.contains("hdfs");
            }
            
            return supportedUriSchemes.contains(scheme.toLowerCase());
        } catch (Exception e) {
            LOG.warn("Failed to parse location URI: {}", location);
            return false;
        }
    }

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

    private Map<String, RangerPolicy.RangerPolicyResource> createHiveDatabaseResource(String databaseName) {
        Map<String, RangerPolicy.RangerPolicyResource> ret = new HashMap<>();
        ret.put(HIVE_RESOURCE_DATABASE, new RangerPolicy.RangerPolicyResource(databaseName));
        return ret;
    }

    private Map<String, RangerPolicy.RangerPolicyResource> createHiveTableResource(String databaseName, String tableName) {
        Map<String, RangerPolicy.RangerPolicyResource> ret = new HashMap<>();
        ret.put(HIVE_RESOURCE_DATABASE, new RangerPolicy.RangerPolicyResource(databaseName));
        ret.put(HIVE_RESOURCE_TABLE, new RangerPolicy.RangerPolicyResource(tableName));
        return ret;
    }

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

    private synchronized HMSClientWrapper getHMSClient() {
        if (hmsClient != null && hmsClient.isConnected()) {
            if (hmsClient.testConnection()) {
                return hmsClient;
            }
            LOG.warn("HMS client connection lost, reconnecting...");
            closeHMSClient();
        }

        try {
            hmsClient = new HMSClientWrapper();
            hmsClient.setSaslEnabled(hmsSaslEnabled);
            hmsClient.setKerberosServerPrincipal(hmsKerberosPrincipal);
            hmsClient.setSslEnabled(hmsSslEnabled);
            hmsClient.setTruststorePath(hmsTruststorePath);
            hmsClient.setTruststorePassword(hmsTruststorePassword);

            boolean connected;
            if (hmsSaslEnabled) {
                ensureKerberosLogin();
                connected = hmsClient.connectAsKerberosUser(hmsUri);
            } else {
                connected = hmsClient.connect(hmsUri);
            }

            if (connected) {
                return hmsClient;
            } else {
                hmsClient = null;
                return null;
            }
        } catch (Exception e) {
            LOG.error("Failed to create HMS client", e);
            return null;
        }
    }

    private synchronized void closeHMSClient() {
        if (hmsClient != null) {
            try {
                hmsClient.close();
            } catch (Exception e) {
                LOG.debug("Error closing HMS client", e);
            }
            hmsClient = null;
        }
    }

    /**
     * Ensure a Kerberos TGT exists for the JVM before any SASL/GSSAPI handshake.
     *
     * Performs a one-shot {@code UserGroupInformation.loginUserFromKeytab} on first call,
     * then on subsequent calls invokes {@code checkTGTAndReloginFromKeytab} so the ticket
     * is renewed before expiry. Reflection is used to avoid a hard compile-time dependency
     * on hadoop-common.
     */
    private void ensureKerberosLogin() {
        if (StringUtils.isBlank(hmsClientPrincipal) || StringUtils.isBlank(hmsClientKeytab)) {
            LOG.debug("RMS Kerberos client principal/keytab not configured; "
                    + "relying on existing JVM login (ranger.rms.hms.kerberos.client.principal/keytab "
                    + "or ranger.admin.kerberos.principal/keytab not set)");
            return;
        }
        try {
            Class<?> ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");

            if (!kerberosLoginAttempted) {
                java.lang.reflect.Method loginFromKeytab = ugiClass.getMethod(
                        "loginUserFromKeytab", String.class, String.class);
                LOG.info("Performing Kerberos login for RMS poller: principal={}, keytab={}",
                        hmsClientPrincipal, hmsClientKeytab);
                loginFromKeytab.invoke(null, hmsClientPrincipal, hmsClientKeytab);
                kerberosLoginAttempted = true;

                java.lang.reflect.Method getLoginUser = ugiClass.getMethod("getLoginUser");
                Object loginUser = getLoginUser.invoke(null);
                LOG.info("Kerberos login successful: loginUser={}", loginUser);
            } else {
                java.lang.reflect.Method getLoginUser = ugiClass.getMethod("getLoginUser");
                Object loginUser = getLoginUser.invoke(null);
                if (loginUser != null) {
                    java.lang.reflect.Method relogin = ugiClass.getMethod("checkTGTAndReloginFromKeytab");
                    relogin.invoke(loginUser);
                }
            }
        } catch (ClassNotFoundException e) {
            LOG.warn("Hadoop UserGroupInformation not found on classpath; cannot perform keytab login");
        } catch (Exception e) {
            LOG.error("Failed to login from keytab (principal={}, keytab={}): {}",
                    hmsClientPrincipal, hmsClientKeytab, e.getMessage(), e);
        }
    }

    /**
     * Trigger a full sync manually. Called via REST API.
     */
    public void triggerFullSync() {
        LOG.info("Manual full sync triggered");
        fullSyncCompleted.set(false);
        // The next poll cycle will perform full sync
    }

    /**
     * Get the last processed event ID.
     */
    public long getLastEventId() {
        return lastEventId.get();
    }

    /**
     * Check if RMS is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Check if full sync has been completed.
     */
    public boolean isFullSyncCompleted() {
        return fullSyncCompleted.get();
    }
}
