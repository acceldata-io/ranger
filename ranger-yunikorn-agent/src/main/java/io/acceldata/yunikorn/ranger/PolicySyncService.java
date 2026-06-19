/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.yunikorn.ranger;

import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The agent's main work loop.
 *
 * <p>Every {@code pollIntervalMs}, asks Ranger Admin if the policy version
 * has advanced since {@code lastKnownVersion}; if it has, fetches the new
 * policies, converts them to YuniKorn ACL entries, and writes them into the
 * YuniKorn ConfigMap.
 *
 * <h2>Why a separate scheduled executor instead of {@code Thread.sleep}</h2>
 * A single-threaded {@link ScheduledExecutorService} gives us:
 * <ul>
 *   <li>serialized cycles (never two syncs running at the same time)</li>
 *   <li>graceful shutdown via {@link #stop()}</li>
 *   <li>fixed-delay scheduling (interval is from end-of-last-sync to start-
 *       of-next, so a slow sync doesn't queue up)</li>
 * </ul>
 *
 * <h2>Drift recovery: force-resync</h2>
 * Inspired by Gravitino's pattern. Every {@code forceResyncMs}, we reset
 * {@code lastKnownVersion} to {@code -1} so the next call refetches the
 * full policy set even if Ranger's version counter says nothing changed.
 * This catches:
 * <ul>
 *   <li>Ranger DB restored from a backup that drifts from our cached version</li>
 *   <li>Ranger service deleted and recreated (serviceId changes)</li>
 *   <li>Bugs in our own version-tracking</li>
 * </ul>
 *
 * <h2>Service-id change detection</h2>
 * Every response carries a {@code serviceId}. If it changes between cycles
 * (Ranger admin deleted and recreated the service), the version counter
 * could go backwards. We detect this and force a resync immediately rather
 * than wait for the timed force-resync.
 *
 * <h2>Failure handling</h2>
 * Cycle failures are logged and counted but never propagate. The loop
 * keeps running. Specifically:
 * <ul>
 *   <li>Ranger fetch fails → log, increment failure counter, retry next cycle.
 *       Don't update {@code lastKnownVersion} so we'll refetch.</li>
 *   <li>ConfigMap write fails → log, retry next cycle. Don't update
 *       {@code lastKnownVersion} so we'll redo the conversion.</li>
 *   <li>Splicer raises {@link YamlStructureException} → log, do NOT retry
 *       indefinitely. The ConfigMap is corrupt and we can't fix it.
 *       (We still try every cycle in case an operator fixes it.)</li>
 * </ul>
 *
 * <h2>Threading</h2>
 * Single-threaded sync. {@link #start()} and {@link #stop()} are idempotent
 * and safe to call from any thread. {@link #cycleCount()} and similar
 * accessors are non-blocking.
 */
public class PolicySyncService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PolicySyncService.class);

    /** Sentinel value: refetch all policies on the next cycle. */
    static final long FORCE_RESYNC_VERSION = -1L;

    private static final long INITIAL_SERVICE_ID = -1L;

    private final AgentConfig       config;
    private final RangerAdminClient adminClient;
    private final AclConverter      converter;
    private final ConfigMapWriter   configMapWriter;
    private final ScheduledExecutorService scheduler;

    private final AtomicLong    lastKnownVersion   = new AtomicLong(FORCE_RESYNC_VERSION);
    private final AtomicLong    lastKnownServiceId = new AtomicLong(INITIAL_SERVICE_ID);
    private final AtomicLong    cycleCount         = new AtomicLong(0);
    private final AtomicLong    successCount       = new AtomicLong(0);
    private final AtomicLong    failureCount       = new AtomicLong(0);
    private final AtomicLong    lastSuccessAtMs    = new AtomicLong(-1);
    private final AtomicBoolean running            = new AtomicBoolean(false);

    /**
     * Production constructor. Builds its own scheduler and lifecycle. The
     * caller (typically the agent's main class) drives {@link #start()} and
     * {@link #stop()}.
     */
    public PolicySyncService(AgentConfig config,
                             RangerAdminClient adminClient,
                             AclConverter converter,
                             ConfigMapWriter configMapWriter) {
        this(config, adminClient, converter, configMapWriter, defaultScheduler());
    }

    /**
     * Test-friendly constructor: lets tests inject a scheduler they control
     * (or a no-op stand-in when running cycles synchronously via
     * {@link #runOneCycle()}).
     */
    PolicySyncService(AgentConfig config,
                      RangerAdminClient adminClient,
                      AclConverter converter,
                      ConfigMapWriter configMapWriter,
                      ScheduledExecutorService scheduler) {
        this.config          = Objects.requireNonNull(config,          "config");
        this.adminClient     = Objects.requireNonNull(adminClient,     "adminClient");
        this.converter       = Objects.requireNonNull(converter,       "converter");
        this.configMapWriter = Objects.requireNonNull(configMapWriter, "configMapWriter");
        this.scheduler       = Objects.requireNonNull(scheduler,       "scheduler");
    }

    private static ScheduledExecutorService defaultScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "policy-sync");
            t.setDaemon(true);
            return t;
        });
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /**
     * Begin polling at fixed delay. Idempotent — calling start() multiple
     * times has no effect after the first.
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("PolicySyncService.start() called but already running");
            return;
        }
        LOG.info("PolicySyncService starting. pollInterval={}ms, forceResync={}ms",
                config.pollIntervalMs(), config.forceResyncMs());

        // First cycle: schedule for "now" (well, ~immediately).
        // Subsequent cycles: pollIntervalMs after the previous one ended.
        scheduler.scheduleWithFixedDelay(
                this::runOneCycle,
                /* initialDelay = */ 0L,
                /* delay        = */ config.pollIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Stop polling and shut down the scheduler.
     * Blocks up to 30s for in-flight cycles to finish.
     * Idempotent.
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        LOG.info("PolicySyncService stopping...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("Scheduler did not terminate within 30s; forcing shutdown");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.info("PolicySyncService stopped");
    }

    @Override
    public void close() {
        stop();
    }

    // -----------------------------------------------------------------------
    // The cycle
    // -----------------------------------------------------------------------

    /**
     * One iteration of the polling loop. Visible for testing — production
     * code drives this via the scheduler. Tests can call it directly to
     * exercise behaviour without sleeping.
     */
    void runOneCycle() {
        long cycleId = cycleCount.incrementAndGet();
        long startedAtMs = nowMillis();

        try {
            // Periodic full resync. Reset lastKnownVersion so Ranger sends
            // us everything, regardless of whether the version actually
            // advanced.
            if (shouldForceResync(cycleId)) {
                LOG.info("cycleId={} — force-resync triggered", cycleId);
                lastKnownVersion.set(FORCE_RESYNC_VERSION);
            }

            long currentVersion = lastKnownVersion.get();

            // The activation time we report to Ranger drives the "Active"
            // timestamp in its plugin-status UI. Report the wall-clock of the
            // last cycle that confirmed we were in sync — a successful apply OR
            // a no-op/304 cycle — NOT this cycle's start time. Using the last
            // success means the timestamp goes stale in Ranger when applies are
            // failing (the signal operators actually want), instead of falsely
            // advancing every poll. 0 means "never activated" (first poll before
            // any success), matching the convention real Ranger plugins use.
            long lastActivationMs = Math.max(0L, lastSuccessAtMs.get());

            ServicePolicies update;
            try {
                update = adminClient.getServicePoliciesIfUpdated(currentVersion, lastActivationMs);
            } catch (Throwable t) {
                LOG.error("cycleId={} — Ranger fetch failed", cycleId, t);
                failureCount.incrementAndGet();
                return;
            }

            if (update == null) {
                LOG.debug("cycleId={} — no policy updates (version={})", cycleId, currentVersion);
                successCount.incrementAndGet();
                lastSuccessAtMs.set(nowMillis());
                return;
            }

            // Detect Ranger service deletion+recreation.
            Long incomingServiceId = update.getServiceId();
            if (incomingServiceId != null) {
                long previousServiceId = lastKnownServiceId.getAndSet(incomingServiceId);
                if (previousServiceId != INITIAL_SERVICE_ID
                        && previousServiceId != incomingServiceId) {
                    LOG.warn("cycleId={} — Ranger serviceId changed {} → {}, " +
                            "force-resyncing on next cycle",
                            cycleId, previousServiceId, incomingServiceId);
                    lastKnownVersion.set(FORCE_RESYNC_VERSION);
                    return;     // re-fetch on next cycle so all paths use the new id
                }
            }

            Long incomingVersion = update.getPolicyVersion();
            int policyCount = update.getPolicies() == null ? 0 : update.getPolicies().size();

            LOG.info("cycleId={} — policies received. version {} → {}, count={}",
                    cycleId, currentVersion, incomingVersion, policyCount);

            // Convert and write. Either failing leaves lastKnownVersion
            // untouched so we'll retry next cycle.
            Map<String, QueueAclEntry> aclMap = converter.convert(
                    update.getPolicies() == null
                            ? java.util.Collections.emptyList()
                            : update.getPolicies());

            boolean wrote;
            try {
                wrote = configMapWriter.applyAcls(aclMap);
            } catch (YamlStructureException e) {
                LOG.error("cycleId={} — YuniKorn ConfigMap is malformed; cannot apply " +
                        "policies until an operator fixes it: {}", cycleId, e.getMessage());
                failureCount.incrementAndGet();
                return;
            } catch (PreflightException e) {
                LOG.error("cycleId={} — YuniKorn preflight rejected the spliced config; " +
                        "NOT writing. Will retry next cycle. Reason: {}", cycleId, e.getMessage());
                failureCount.incrementAndGet();
                return;
            } catch (ConfigMapWriterException e) {
                LOG.error("cycleId={} — ConfigMap write failed", cycleId, e);
                failureCount.incrementAndGet();
                return;
            }

            // Only advance lastKnownVersion after a successful write.
            if (incomingVersion != null) {
                lastKnownVersion.set(incomingVersion);
            }

            long durationMs = nowMillis() - startedAtMs;
            LOG.info("cycleId={} — applied {} ACL entries (wrote={}, durationMs={})",
                    cycleId, aclMap.size(), wrote, durationMs);

            successCount.incrementAndGet();
            lastSuccessAtMs.set(nowMillis());

        } catch (Throwable t) {
            // Catch-all so the scheduled task never dies. Without this, a
            // single uncaught error would kill the polling loop.
            LOG.error("cycleId={} — unexpected error in sync cycle", cycleId, t);
            failureCount.incrementAndGet();
        }
    }

    private boolean shouldForceResync(long cycleId) {
        if (config.forceResyncMs() <= 0 || config.pollIntervalMs() <= 0) return false;
        long ratio = config.forceResyncMs() / config.pollIntervalMs();
        return ratio > 0 && cycleId % ratio == 0;
    }

    /**
     * Wall-clock time. Wrapped so tests can override via subclassing if they
     * ever need deterministic timestamps. In production this is just
     * {@code System.currentTimeMillis()}.
     */
    long nowMillis() {
        return System.currentTimeMillis();
    }

    // -----------------------------------------------------------------------
    // Observability
    // -----------------------------------------------------------------------

    public long cycleCount()      { return cycleCount.get(); }
    public long successCount()    { return successCount.get(); }
    public long failureCount()    { return failureCount.get(); }
    public long lastKnownVersion(){ return lastKnownVersion.get(); }

    /** Wall-clock time of the last successful cycle, or {@code -1} if never. */
    public long lastSuccessAtMs() { return lastSuccessAtMs.get(); }

    /** True if at least one successful cycle has completed. */
    public boolean isReady()      { return lastSuccessAtMs.get() > 0; }

    public boolean isRunning()    { return running.get(); }
}
