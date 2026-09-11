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

    // Initial full-sync scale controls (added to support 1M+ table clusters).
    // Serial + per-table Thrift RPCs cannot finish in a reasonable time at
    // that scale; these knobs turn the outer database loop into a bounded
    // thread pool with per-worker HMS clients, and turn the inner per-table
    // fetch into a single batched Thrift call.
    private static final String CONFIG_FULLSYNC_PARALLELISM = "ranger.rms.fullsync.parallelism";
    private static final String CONFIG_FULLSYNC_BATCH_SIZE = "ranger.rms.fullsync.batch.size";
    private static final String CONFIG_FULLSYNC_INCLUDED_DB_REGEX = "ranger.rms.fullsync.included.databases.regex";
    private static final String CONFIG_FULLSYNC_EXCLUDED_DB_REGEX = "ranger.rms.fullsync.excluded.databases.regex";

    private static final int DEFAULT_FULLSYNC_PARALLELISM = 8;
    private static final int DEFAULT_FULLSYNC_BATCH_SIZE = 200;
    // Skip the two schema-owned databases HMS ships with by default.
    private static final String DEFAULT_FULLSYNC_EXCLUDED_DB_REGEX = "(?i)^(sys|information_schema)$";
    // Absolute upper bound on parallelism: too many concurrent connections
    // are a DoS vector on HMS's Thrift server. If you're running with a
    // sharded/replicated HMS behind a VIP you can raise this at build time.
    private static final int MAX_FULLSYNC_PARALLELISM = 32;

    private static final String DEFAULT_HIVE_SERVICE_NAME = "hive";
    private static final String DEFAULT_HDFS_SERVICE_NAME = "hdfs";
    private static final String DEFAULT_OZONE_SERVICE_NAME = "ozone";
    private static final String DEFAULT_S3_SERVICE_NAME = "s3";
    private static final String DEFAULT_SUPPORTED_URI_SCHEMES = "hdfs,o3fs,ofs,s3a";
    private static final long DEFAULT_POLLING_INTERVAL_MS = 30000;
    // Lower bound on the HMS poll interval. A misconfigured 0 or negative
    // value would make scheduleAtFixedRate throw IllegalArgumentException
    // inside init(), failing Spring bean init and preventing Ranger Admin
    // from starting. Anything below 5s is also unhealthy for HMS load.
    private static final long MIN_POLLING_INTERVAL_MS = 5000;

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

    private int fullSyncParallelism;
    private int fullSyncBatchSize;
    private java.util.regex.Pattern fullSyncIncludedDbPattern;
    private java.util.regex.Pattern fullSyncExcludedDbPattern;

    private ScheduledExecutorService scheduler;
    private HMSClientWrapper hmsClient;
    private AtomicLong lastEventId = new AtomicLong(-1);
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private AtomicBoolean fullSyncCompleted = new AtomicBoolean(false);

    // Consecutive-empty-poll gate for the "range non-empty but response empty" case.
    // A single empty response is indistinguishable from a real event gap vs. a
    // transient HMS-VIP lag or metastore restart. Only after MIN_CONSECUTIVE_EMPTY_POLLS
    // empties at the same watermark do we accept the "real gap" verdict and advance.
    // Any non-empty response (progress) or a change to the watermark resets the counter.
    // Only ever touched from the single-threaded poll executor thread, so plain
    // fields are safe (no cross-thread visibility guarantees needed).
    private static final int MIN_CONSECUTIVE_EMPTY_POLLS = 3;
    private int consecutiveEmptyPolls = 0;
    private long consecutiveEmptyPollsBaseEventId = -1L;

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
        LOG.info("  fullSyncParallelism: {}", fullSyncParallelism);
        LOG.info("  fullSyncBatchSize: {}", fullSyncBatchSize);
        LOG.info("  fullSyncIncludedDbRegex: {}", fullSyncIncludedDbPattern == null ? "(all)" : fullSyncIncludedDbPattern.pattern());
        LOG.info("  fullSyncExcludedDbRegex: {}", fullSyncExcludedDbPattern == null ? "(none)" : fullSyncExcludedDbPattern.pattern());

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
        long configuredPollingIntervalMs = PropertiesUtil.getLongProperty(CONFIG_POLLING_INTERVAL, DEFAULT_POLLING_INTERVAL_MS);
        pollingIntervalMs = Math.max(MIN_POLLING_INTERVAL_MS, configuredPollingIntervalMs);
        if (pollingIntervalMs != configuredPollingIntervalMs) {
            LOG.warn("Configured {}={}ms is below the {}ms floor; clamped to {}ms",
                    CONFIG_POLLING_INTERVAL, configuredPollingIntervalMs, MIN_POLLING_INTERVAL_MS, pollingIntervalMs);
        }
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

        int rawParallelism = PropertiesUtil.getIntProperty(CONFIG_FULLSYNC_PARALLELISM, DEFAULT_FULLSYNC_PARALLELISM);
        fullSyncParallelism = Math.max(1, Math.min(MAX_FULLSYNC_PARALLELISM, rawParallelism));
        fullSyncBatchSize = Math.max(1, PropertiesUtil.getIntProperty(CONFIG_FULLSYNC_BATCH_SIZE, DEFAULT_FULLSYNC_BATCH_SIZE));

        String includedRegex = PropertiesUtil.getProperty(CONFIG_FULLSYNC_INCLUDED_DB_REGEX, "");
        fullSyncIncludedDbPattern = StringUtils.isBlank(includedRegex) ? null : java.util.regex.Pattern.compile(includedRegex);
        String excludedRegex = PropertiesUtil.getProperty(CONFIG_FULLSYNC_EXCLUDED_DB_REGEX, DEFAULT_FULLSYNC_EXCLUDED_DB_REGEX);
        fullSyncExcludedDbPattern = StringUtils.isBlank(excludedRegex) ? null : java.util.regex.Pattern.compile(excludedRegex);
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
                FullSyncStats stats = performFullSync(client);

                if (stats.isFullyFailed()) {
                    // Every mapping write failed (e.g. schema patch missing,
                    // DB connection broken). Don't flip fullSyncCompleted to
                    // true — leave it false so a future trigger / next cycle
                    // retries once the underlying issue is fixed. Also keep
                    // lastEventId where it was so we don't silently skip past
                    // events that should have produced mappings.
                    LOG.error("Full sync attempted {} mappings, all failed. "
                            + "Leaving fullSyncCompleted=false; check Admin DB / schema patches.",
                            stats.attempted);
                    return;
                }

                fullSyncCompleted.set(true);

                long currentId = client.getCurrentNotificationEventId();
                lastEventId.set(currentId);
                persistState();
                LOG.info("Full sync completed (attempted={}, failed={}). Current event ID: {}",
                        stats.attempted, stats.failed, lastEventId.get());
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
                    persistState();
                    // Progress observed — reset the empty-poll gate.
                    consecutiveEmptyPolls = 0;
                    consecutiveEmptyPollsBaseEventId = -1L;
                } else {
                    // HMS reports a higher current event ID than we've processed
                    // but returned no events for the requested range. Two very
                    // different possibilities produce the same signal:
                    //   (a) real event gap — events between (lastEventId, currentEventId]
                    //       were trimmed/expired between two polls; safe to advance.
                    //   (b) transient empty — a lagging HMS secondary behind a
                    //       load-balancer/VIP returned currentEventId but the
                    //       matching event rows haven't replicated yet, or an HMS
                    //       just restarted and the notification log is warming up.
                    //       Advancing here permanently skips real events.
                    //
                    // Since we cannot tell (a) from (b) on a single poll, require
                    // MIN_CONSECUTIVE_EMPTY_POLLS empties at the same watermark before
                    // advancing. A subsequent non-empty response (progress) resets
                    // the counter; a change to lastEventId from any other path also
                    // resets it because the "base" no longer matches.
                    long base = lastEventId.get();
                    if (consecutiveEmptyPollsBaseEventId != base) {
                        consecutiveEmptyPollsBaseEventId = base;
                        consecutiveEmptyPolls = 1;
                    } else {
                        consecutiveEmptyPolls++;
                    }

                    if (consecutiveEmptyPolls >= MIN_CONSECUTIVE_EMPTY_POLLS) {
                        LOG.warn("No notification events for range ({}, {}] across {} consecutive polls, "
                                + "advancing watermark to {} (accepting event gap on HMS)",
                                base, currentEventId, consecutiveEmptyPolls, currentEventId);
                        lastEventId.set(currentEventId);
                        persistState();
                        consecutiveEmptyPolls = 0;
                        consecutiveEmptyPollsBaseEventId = -1L;
                    } else {
                        LOG.info("No notification events for range ({}, {}] "
                                + "(empty poll {}/{} at same watermark; waiting before advancing)",
                                base, currentEventId, consecutiveEmptyPolls, MIN_CONSECUTIVE_EMPTY_POLLS);
                    }
                }
            } catch (Exception e) {
                // On fetch / processing error keep the watermark unchanged so
                // the next cycle retries the same range. Advancing here would
                // permanently skip events and silently corrupt RMS state.
                LOG.warn("Error fetching notifications from eventId {} (current={}). "
                        + "Watermark unchanged; will retry next cycle.",
                        lastEventId.get(), currentEventId, e);
            }
            LOG.debug("<== pollHMS() processed events up to eventId={}", lastEventId.get());

        } catch (Throwable t) {
            // Catching Throwable (not just Exception) because scheduleAtFixedRate
            // suppresses all future runs if the task escapes with an uncaught
            // Throwable — an Error from the Thrift/Hive reflection init path
            // (NoClassDefFoundError, ExceptionInInitializerError) or a sync-time
            // OutOfMemoryError would otherwise silently kill the poller until
            // Admin restart, with no further log lines.
            LOG.error("Error polling HMS for notifications", t);
            closeHMSClient();
        } finally {
            isRunning.set(false);
        }
    }

    /**
     * Result summary of a full-sync pass: how many mappings were attempted and
     * how many failed. Used by the caller to decide whether the sync truly
     * completed (e.g. don't mark fullSyncCompleted=true if every mapping
     * failed — that usually indicates a systemic problem like a missing schema
     * patch that we want to surface instead of silently flipping to "done").
     */
    static final class FullSyncStats {
        int attempted;
        int failed;

        boolean isFullyFailed() {
            return attempted > 0 && failed >= attempted;
        }
    }

    /**
     * Full-sync the HMS catalog into RMS.
     *
     * At the scale we target on this branch (up to a few million tables) a
     * serial per-table walk is not viable — a naive implementation takes
     * many hours and holds a hot write-lock on
     * {@code x_rms_mapping_provider.last_known_version} for every row.
     *
     * <p>Three coordinated optimizations run here:
     * <ol>
     *   <li><b>Bulk mode:</b> {@link RMSMgr#beginBulkFullSync()} reserves the
     *       next {@code mapping_version} up-front, and every
     *       {@code createOrUpdateMapping} write during the sync uses that
     *       reserved version without bumping the provider row. The single
     *       commit-bump happens in {@link RMSMgr#endBulkFullSync(boolean)}
     *       at the end. Plugins polling mid-sync see the previous version and
     *       receive "no change" — they never observe intermediate state.</li>
     *   <li><b>Parallel database workers:</b> the outer database loop runs
     *       across N worker threads (see {@code ranger.rms.fullsync.parallelism}).
     *       Each worker owns its own {@link HMSClientWrapper} because the
     *       wrapper wraps a single non-thread-safe Thrift transport. Databases
     *       are independent units of work in HMS, so this fans out cleanly.</li>
     *   <li><b>Batched table fetch:</b> instead of one {@code get_table} RPC
     *       per table, each worker groups the table list of a database into
     *       chunks of {@code ranger.rms.fullsync.batch.size} and calls the
     *       bulk {@code get_table_objects_by_name_req} Thrift API. Cuts
     *       round-trips ~100-200×.</li>
     * </ol>
     */
    private FullSyncStats performFullSync(HMSClientWrapper primaryClient) throws Exception {
        LOG.info("==> performFullSync(parallelism={}, batchSize={})", fullSyncParallelism, fullSyncBatchSize);

        FullSyncStats stats = new FullSyncStats();

        List<String> allDatabases;
        try {
            allDatabases = primaryClient.getAllDatabases();
        } catch (Exception e) {
            LOG.error("Error listing databases from HMS", e);
            throw e;
        }
        LOG.info("Found {} databases in HMS", allDatabases.size());

        List<String> databases = filterDatabases(allDatabases);
        LOG.info("After include/exclude filtering: {} databases eligible for sync", databases.size());

        if (databases.isEmpty()) {
            LOG.info("<== performFullSync() attempted=0, failed=0 (no databases to sync)");
            return stats;
        }

        rmsMgr.beginBulkFullSync();
        boolean bulkSuccess = false;
        java.util.concurrent.ExecutorService executor = null;
        List<HMSClientWrapper> workerClients = new java.util.ArrayList<>();
        java.util.concurrent.atomic.AtomicInteger attempted = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger failed = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger dbCounter = new java.util.concurrent.atomic.AtomicInteger(0);

        try {
            // We want the primary client to remain usable for post-sync work
            // (e.g. get_current_notificationEventId). If parallelism == 1 the
            // primary client is our only worker; if > 1 we spin up fresh
            // wrappers so we don't share a non-thread-safe Thrift connection.
            int workers = fullSyncParallelism;
            executor = java.util.concurrent.Executors.newFixedThreadPool(workers,
                    new java.util.concurrent.ThreadFactory() {
                        private final java.util.concurrent.atomic.AtomicInteger seq = new java.util.concurrent.atomic.AtomicInteger();
                        @Override public Thread newThread(Runnable r) {
                            Thread t = new Thread(r, "RMS-FullSync-Worker-" + seq.incrementAndGet());
                            t.setDaemon(true);
                            return t;
                        }
                    });

            for (int i = 0; i < workers; i++) {
                HMSClientWrapper client = (i == 0) ? primaryClient : createHMSClient();
                if (client == null) {
                    LOG.warn("Could not create HMS client for worker #{}, reducing parallelism", i);
                    continue;
                }
                workerClients.add(client);
            }

            if (workerClients.isEmpty()) {
                throw new IllegalStateException("No HMS clients available for full sync");
            }

            int effectiveWorkers = workerClients.size();
            LOG.info("Full sync starting with {} worker(s), {} database(s), batch size {}",
                    effectiveWorkers, databases.size(), fullSyncBatchSize);

            // Round-robin each database to a fixed worker so we never share
            // a wrapper across threads. This is simpler than a work-stealing
            // queue over a client pool and adequate: within a worker the
            // per-database cost is dominated by table count, and Hive
            // clusters have a long-tail distribution across databases.
            List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < effectiveWorkers; i++) {
                final int workerIdx = i;
                final HMSClientWrapper client = workerClients.get(i);
                futures.add(executor.submit(() -> {
                    for (int dbIdx = workerIdx; dbIdx < databases.size(); dbIdx += effectiveWorkers) {
                        String dbName = databases.get(dbIdx);
                        int done = dbCounter.incrementAndGet();
                        try {
                            processDatabaseFully(client, dbName, attempted, failed);
                        } catch (Exception e) {
                            Throwable rootCause = unwrap(e);
                            LOG.warn("Error processing database {}: [{}] {}",
                                    dbName, rootCause.getClass().getName(), rootCause.getMessage(), e);
                        }
                        if (done % 100 == 0 || done == databases.size()) {
                            LOG.info("Full sync progress: {}/{} databases processed, attempted={}, failed={}",
                                    done, databases.size(), attempted.get(), failed.get());
                        }
                    }
                }));
            }

            for (java.util.concurrent.Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    LOG.warn("Full sync worker terminated with error: {}", e.getMessage(), e);
                }
            }

            stats.attempted = attempted.get();
            stats.failed = failed.get();

            // Only commit the reserved version if at least one write succeeded.
            // isFullyFailed() (checked by the caller) additionally guards the
            // fullSyncCompleted flag; consistent semantics between the two.
            bulkSuccess = stats.attempted > 0 && !stats.isFullyFailed();

        } finally {
            rmsMgr.endBulkFullSync(bulkSuccess);
            if (executor != null) {
                executor.shutdownNow();
            }
            // Close every extra worker client we opened. The primary client
            // stays open — the poll loop needs it for the notification cursor.
            for (int i = 1; i < workerClients.size(); i++) {
                try {
                    workerClients.get(i).close();
                } catch (Exception ignored) { /* best-effort cleanup */ }
            }
        }

        LOG.info("<== performFullSync() attempted={}, failed={}, bulkCommitted={}",
                stats.attempted, stats.failed, bulkSuccess);
        return stats;
    }

    /**
     * Process a single database's worth of work using the given (worker-owned)
     * HMS client: fetch DB metadata, walk tables in batches, and hand each
     * batch to {@link #processTable} via {@link RMSMgr#createOrUpdateMapping}.
     * Failures on individual tables are logged but never propagate — one bad
     * table shouldn't fail the whole sync.
     */
    private void processDatabaseFully(HMSClientWrapper client,
                                       String dbName,
                                       java.util.concurrent.atomic.AtomicInteger attempted,
                                       java.util.concurrent.atomic.AtomicInteger failed) {
        try {
            DatabaseInfo db = client.getDatabase(dbName);
            if (db != null) {
                String dbLocation = db.locationUri;
                if (StringUtils.isNotBlank(dbLocation) && isSupportedLocation(dbLocation)) {
                    attempted.incrementAndGet();
                    if (!processCreateDatabase(dbName, dbLocation)) {
                        failed.incrementAndGet();
                    }
                }
            }

            List<String> tables = client.getAllTables(dbName);
            if (tables == null || tables.isEmpty()) {
                return;
            }
            LOG.info("Database {}: found {} tables", dbName, tables.size());

            for (int start = 0; start < tables.size(); start += fullSyncBatchSize) {
                int end = Math.min(start + fullSyncBatchSize, tables.size());
                List<String> batch = tables.subList(start, end);

                List<TableInfo> fetched;
                try {
                    fetched = client.getTables(dbName, batch);
                } catch (Exception e) {
                    // Bulk fetch failed for the whole batch. Charge each
                    // requested table to `failed` so isFullyFailed() sees
                    // the true error rate.
                    failed.addAndGet(batch.size());
                    attempted.addAndGet(batch.size());
                    Throwable rootCause = unwrap(e);
                    LOG.warn("Error batch-fetching tables from {} (batch {}..{}): [{}] {}",
                            dbName, start, end, rootCause.getClass().getName(), rootCause.getMessage(), e);
                    continue;
                }

                for (TableInfo table : fetched) {
                    attempted.incrementAndGet();
                    try {
                        if (!processTable(table)) {
                            failed.incrementAndGet();
                        }
                    } catch (Exception e) {
                        failed.incrementAndGet();
                        Throwable rootCause = unwrap(e);
                        LOG.warn("Error processing table {}.{}: [{}] {}",
                                dbName, table != null ? table.tableName : "?",
                                rootCause.getClass().getName(), rootCause.getMessage(), e);
                    }
                }
            }
        } catch (Exception e) {
            Throwable rootCause = unwrap(e);
            LOG.warn("Error processing database {}: [{}] {}",
                    dbName, rootCause.getClass().getName(), rootCause.getMessage(), e);
        }
    }

    /**
     * Walk an exception's cause chain to its terminal cause. Used to surface
     * the real HMS-side error (e.g.Â {@code UnsupportedFileSystemException:
     * No FileSystem for scheme "ofs"}) on the WARN one-liner instead of the
     * outer reflection wrapper (usually {@code InvocationTargetException}
     * with a {@code null} message that hides the actual problem).
     *
     * <p>Cycle-guarded: some exception chains reference themselves as their
     * own cause, so we bail if we see the same throwable twice.
     */
    private static Throwable unwrap(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }

    /**
     * Apply include/exclude regex filters to the raw HMS database list.
     * Returns a new list preserving input order (workers stripe over it).
     */
    private List<String> filterDatabases(List<String> allDatabases) {
        List<String> result = new java.util.ArrayList<>(allDatabases.size());
        for (String dbName : allDatabases) {
            if (fullSyncIncludedDbPattern != null && !fullSyncIncludedDbPattern.matcher(dbName).matches()) {
                LOG.debug("Skipping {}: does not match include regex", dbName);
                continue;
            }
            if (fullSyncExcludedDbPattern != null && fullSyncExcludedDbPattern.matcher(dbName).matches()) {
                LOG.debug("Skipping {}: matches exclude regex", dbName);
                continue;
            }
            result.add(dbName);
        }
        return result;
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

    private boolean processTable(TableInfo table) {
        if (table == null) {
            return true;
        }

        String dbName = table.dbName;
        String tableName = table.tableName;
        String location = table.location;
        boolean isManaged = table.isManaged();

        if (!mapManagedTables && isManaged) {
            LOG.debug("Skipping managed table: {}.{}", dbName, tableName);
            return true;
        }

        if (StringUtils.isBlank(location) || !isSupportedLocation(location)) {
            LOG.debug("Skipping table with unsupported location: {}.{} -> {}", dbName, tableName, location);
            return true;
        }

        LOG.info("Processing table: {}.{} -> {} (managed={})", dbName, tableName, location, isManaged);

        try {
            Map<String, RangerPolicy.RangerPolicyResource> hiveResource = createHiveTableResource(dbName, tableName);
            String llServiceName = getStorageServiceName(location);
            Map<String, RangerPolicy.RangerPolicyResource> storageResource = createStorageResource(location);

            if (llServiceName != null && storageResource != null) {
                rmsMgr.createOrUpdateMapping(hiveServiceName, hiveResource, llServiceName, storageResource, location);
            }
            return true;
        } catch (Exception e) {
            LOG.error("Failed to create mapping for table {}.{}", dbName, tableName, e);
            return false;
        }
    }

    private boolean processCreateDatabase(String dbName, String location) {
        LOG.info("Processing database: {} -> {}", dbName, location);

        try {
            Map<String, RangerPolicy.RangerPolicyResource> hiveResource = createHiveDatabaseResource(dbName);
            String llServiceName = getStorageServiceName(location);
            Map<String, RangerPolicy.RangerPolicyResource> storageResource = createStorageResource(location);

            if (llServiceName != null && storageResource != null) {
                rmsMgr.createOrUpdateMapping(hiveServiceName, hiveResource, llServiceName, storageResource, location);
            }
            return true;
        } catch (Exception e) {
            LOG.error("Failed to create mapping for database {}", dbName, e);
            return false;
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
        hmsClient = createHMSClient();
        return hmsClient;
    }

    /**
     * Build a fresh, connected HMS client instance without touching the
     * singleton {@link #hmsClient}. Used by parallel full-sync workers, each
     * of which owns its own client because {@code HMSClientWrapper} wraps a
     * single Thrift connection and is NOT thread-safe. Returns {@code null}
     * if the connection or Kerberos handshake fails.
     */
    private HMSClientWrapper createHMSClient() {
        try {
            HMSClientWrapper client = new HMSClientWrapper();
            client.setSaslEnabled(hmsSaslEnabled);
            client.setKerberosServerPrincipal(hmsKerberosPrincipal);
            client.setSslEnabled(hmsSslEnabled);
            client.setTruststorePath(hmsTruststorePath);
            client.setTruststorePassword(hmsTruststorePassword);

            boolean connected;
            if (hmsSaslEnabled) {
                ensureKerberosLogin();
                connected = client.connectAsKerberosUser(hmsUri);
            } else {
                connected = client.connect(hmsUri);
            }
            return connected ? client : null;
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

        // UGI.loginUserFromKeytab() does NOT substitute _HOST in the principal —
        // that is the job of SecurityUtil.getServerPrincipal(). If we hand a
        // literal "service/_HOST@REALM" straight to loginUserFromKeytab, the
        // JDK Krb5LoginModule fails with "Unable to obtain password from user"
        // because the literal _HOST is not a real entry in the keytab.
        String resolvedPrincipal = resolveHostInPrincipal(hmsClientPrincipal);

        try {
            Class<?> ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");

            if (!kerberosLoginAttempted) {
                java.lang.reflect.Method loginFromKeytab = ugiClass.getMethod(
                        "loginUserFromKeytab", String.class, String.class);
                LOG.info("Performing Kerberos login for RMS poller: principal={} (resolved from {}), keytab={}",
                        resolvedPrincipal, hmsClientPrincipal, hmsClientKeytab);
                loginFromKeytab.invoke(null, resolvedPrincipal, hmsClientKeytab);
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
                    resolvedPrincipal, hmsClientKeytab, e.getMessage(), e);
        }
    }

    /**
     * Expand the literal {@code _HOST} token in a Kerberos principal to the
     * local host's FQDN. Prefers Hadoop's {@code SecurityUtil.getServerPrincipal}
     * when available (which uses DNS and matches Hadoop's own semantics); falls
     * back to {@code InetAddress.getLocalHost().getCanonicalHostName()} otherwise.
     */
    private String resolveHostInPrincipal(String principal) {
        if (StringUtils.isBlank(principal) || !principal.contains("_HOST")) {
            return principal;
        }
        // Try Hadoop's SecurityUtil first for parity with other Hadoop components.
        try {
            Class<?> securityUtilClass = Class.forName("org.apache.hadoop.security.SecurityUtil");
            java.lang.reflect.Method getServerPrincipal = securityUtilClass.getMethod(
                    "getServerPrincipal", String.class, String.class);
            Object resolved = getServerPrincipal.invoke(null, principal, (String) null);
            if (resolved instanceof String && StringUtils.isNotBlank((String) resolved)
                    && !((String) resolved).contains("_HOST")) {
                return (String) resolved;
            }
        } catch (Exception e) {
            LOG.debug("SecurityUtil.getServerPrincipal unavailable, falling back to local FQDN: {}", e.getMessage());
        }
        // Fallback: local canonical hostname.
        try {
            String fqdn = java.net.InetAddress.getLocalHost().getCanonicalHostName();
            if (StringUtils.isNotBlank(fqdn)) {
                return principal.replace("_HOST", fqdn);
            }
        } catch (Exception e) {
            LOG.warn("Unable to resolve local canonical hostname for principal _HOST substitution: {}", e.getMessage());
        }
        return principal;
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
