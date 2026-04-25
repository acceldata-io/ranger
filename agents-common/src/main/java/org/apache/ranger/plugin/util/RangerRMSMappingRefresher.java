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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.service.RangerRMSChainedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RangerRMSMappingRefresher periodically downloads resource mappings from RMS
 * and updates the chained plugin's mapping cache.
 *
 * Configuration properties:
 * - ranger.plugin.{service}.mapping.source.download.interval: Download interval in milliseconds (default: 30000)
 * - ranger.plugin.{service}.mapping.source.url: RMS URL (if different from Ranger Admin)
 * - ranger.plugin.{service}.mapping.cache.dir: Directory to cache mappings locally
 */
public class RangerRMSMappingRefresher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RangerRMSMappingRefresher.class);
    private static final Logger PERF_LOG = RangerPerfTracer.getPerfLogger("rms.mapping.refresh");

    private static final String MAPPING_FILE_SUFFIX = "_resource_mapping.json";
    private static final long DEFAULT_DOWNLOAD_INTERVAL_MS = 30000;
    private static final long MIN_DOWNLOAD_INTERVAL_MS = 10000;

    private final RangerRMSChainedPlugin chainedPlugin;
    private final String serviceName;
    private final String hlServiceName;
    private final RangerAdminClient adminClient;
    private final long downloadIntervalMs;
    private final String cacheDir;
    private final String cacheFile;
    private final Gson gson;

    private volatile Long lastKnownVersion;
    private volatile long lastDownloadTimeMs;
    private ScheduledExecutorService scheduler;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public RangerRMSMappingRefresher(RangerRMSChainedPlugin chainedPlugin,
                                      String serviceName,
                                      String hlServiceName,
                                      RangerAdminClient adminClient,
                                      RangerPluginConfig config) {
        this.chainedPlugin = chainedPlugin;
        this.serviceName = serviceName;
        this.hlServiceName = hlServiceName;
        this.adminClient = adminClient;

        String propertyPrefix = config.getPropertyPrefix();
        this.downloadIntervalMs = Math.max(MIN_DOWNLOAD_INTERVAL_MS,
            config.getLong(propertyPrefix + ".mapping.source.download.interval", DEFAULT_DOWNLOAD_INTERVAL_MS));

        String defaultCacheDir = config.get(propertyPrefix + ".policy.cache.dir",
            "/etc/ranger/" + serviceName + "/policycache");
        this.cacheDir = config.get(propertyPrefix + ".mapping.cache.dir", defaultCacheDir);

        this.cacheFile = cacheDir + File.separator + serviceName + "_" + hlServiceName + MAPPING_FILE_SUFFIX;

        this.gson = new GsonBuilder().setPrettyPrinting().create();

        LOG.info("RangerRMSMappingRefresher created: serviceName={}, hlServiceName={}, downloadIntervalMs={}, cacheFile={}",
                 serviceName, hlServiceName, downloadIntervalMs, cacheFile);
    }

    /**
     * Start the periodic refresh task. Safe to call once per refresher
     * instance; subsequent calls are ignored to prevent leaking a second
     * scheduler on accidental double-init.
     */
    public synchronized void startRefresher() {
        LOG.info("==> RangerRMSMappingRefresher.startRefresher()");

        if (scheduler != null && !scheduler.isShutdown()) {
            LOG.warn("RangerRMSMappingRefresher already started for serviceName={}; ignoring duplicate start", serviceName);
            return;
        }

        loadFromCache();

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "RMS-Mapping-Refresher-" + serviceName);
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(this, 0, downloadIntervalMs, TimeUnit.MILLISECONDS);

        LOG.info("<== RangerRMSMappingRefresher.startRefresher()");
    }

    /**
     * Stop the periodic refresh task. Idempotent: safe to call multiple times
     * (e.g. from a chained-plugin cleanup() override that may itself be invoked
     * more than once during plugin re-init / failover).
     */
    public synchronized void stopRefresher() {
        LOG.info("==> RangerRMSMappingRefresher.stopRefresher()");

        ScheduledExecutorService toStop = scheduler;
        scheduler = null;

        if (toStop != null && !toStop.isShutdown()) {
            toStop.shutdown();
            try {
                if (!toStop.awaitTermination(30, TimeUnit.SECONDS)) {
                    toStop.shutdownNow();
                }
            } catch (InterruptedException e) {
                toStop.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        LOG.info("<== RangerRMSMappingRefresher.stopRefresher()");
    }

    @Override
    public void run() {
        if (!isRunning.compareAndSet(false, true)) {
            LOG.debug("Skipping refresh - previous refresh still in progress");
            return;
        }

        try {
            downloadMappings();
        } catch (Exception e) {
            LOG.error("Error during RMS mapping refresh", e);
        } finally {
            isRunning.set(false);
        }
    }

    /**
     * Download mappings from RMS.
     * Passes lastKnownVersion so the server can return 304 (no changes)
     * or a full/delta response. Only updates cache when version changes.
     */
    private void downloadMappings() {
        LOG.debug("==> downloadMappings(serviceName={}, lastKnownVersion={})", serviceName, lastKnownVersion);

        RangerPerfTracer perf = null;
        if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_LOG,
                "RangerRMSMappingRefresher.downloadMappings(serviceName=" + serviceName + ")");
        }

        try {
            ServiceRMSMappings mappings = fetchMappingsFromRMS();

            if (mappings == null) {
                LOG.debug("No mappings returned (304 Not Modified or null): serviceName={}", serviceName);
            } else {
                Long newVersion = mappings.getMappingVersion();

                if (newVersion != null && !newVersion.equals(lastKnownVersion)) {
                    LOG.info("New mappings received: serviceName={}, version={} -> {}, isDelta={}",
                             serviceName, lastKnownVersion, newVersion, mappings.getIsDelta());

                    chainedPlugin.updateMappings(mappings);
                    saveToCache(mappings);

                    this.lastKnownVersion = newVersion;
                } else {
                    LOG.debug("No mapping changes: serviceName={}, version={}", serviceName, lastKnownVersion);
                }
            }

            this.lastDownloadTimeMs = System.currentTimeMillis();

        } catch (Exception e) {
            LOG.error("Failed to download RMS mappings for service: {}", serviceName, e);
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== downloadMappings()");
    }

    /**
     * Fetch mappings from RMS server.
     */
    private ServiceRMSMappings fetchMappingsFromRMS() {
        LOG.debug("==> fetchMappingsFromRMS(serviceName={}, lastKnownVersion={})", serviceName, lastKnownVersion);

        ServiceRMSMappings ret = null;

        try {
            ret = adminClient.getRMSMappings(serviceName, lastKnownVersion);
        } catch (Exception e) {
            LOG.error("Error fetching RMS mappings from server", e);
        }

        LOG.debug("<== fetchMappingsFromRMS(): {}", ret);
        return ret;
    }

    /**
     * Load mappings from local cache file.
     */
    private void loadFromCache() {
        LOG.debug("==> loadFromCache(cacheFile={})", cacheFile);

        RangerPerfTracer perf = null;
        if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_LOG,
                "RangerRMSMappingRefresher.loadFromCache(cacheFile=" + cacheFile + ")");
        }

        try {
            File file = new File(cacheFile);
            if (file.exists() && file.canRead()) {
                try (Reader reader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
                    ServiceRMSMappings mappings = gson.fromJson(reader, ServiceRMSMappings.class);

                    if (mappings != null) {
                        chainedPlugin.updateMappings(mappings);
                        this.lastKnownVersion = mappings.getMappingVersion();
                        LOG.info("Loaded RMS mappings from cache: version={}", lastKnownVersion);
                    }
                }
            } else {
                LOG.debug("Cache file does not exist or is not readable: {}", cacheFile);
            }
        } catch (Exception e) {
            LOG.error("Error loading RMS mappings from cache", e);
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== loadFromCache()");
    }

    /**
     * Save mappings to local cache file.
     */
    private void saveToCache(ServiceRMSMappings mappings) {
        LOG.debug("==> saveToCache(cacheFile={})", cacheFile);

        RangerPerfTracer perf = null;
        if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_LOG,
                "RangerRMSMappingRefresher.saveToCache(cacheFile=" + cacheFile + ")");
        }

        try {
            File cacheDirectory = new File(cacheDir);
            if (!cacheDirectory.exists()) {
                if (!cacheDirectory.mkdirs()) {
                    LOG.error("Failed to create cache directory: {}", cacheDir);
                    return;
                }
            }

            File tmpFile = new File(cacheFile + ".tmp");
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)) {
                gson.toJson(mappings, writer);
            }

            File targetFile = new File(cacheFile);
            if (targetFile.exists()) {
                targetFile.delete();
            }
            if (!tmpFile.renameTo(targetFile)) {
                LOG.error("Failed to rename tmp cache file {} to {}", tmpFile.getAbsolutePath(), targetFile.getAbsolutePath());
            } else {
                LOG.info("Saved RMS mappings to cache: version={}", mappings.getMappingVersion());
            }
        } catch (Exception e) {
            LOG.error("Error saving RMS mappings to cache", e);
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== saveToCache()");
    }

    public Long getLastKnownVersion() {
        return lastKnownVersion;
    }

    public long getLastDownloadTimeMs() {
        return lastDownloadTimeMs;
    }

    public String getCacheFile() {
        return cacheFile;
    }
}
