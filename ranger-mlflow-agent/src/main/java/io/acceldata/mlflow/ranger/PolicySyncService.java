/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.acceldata.mlflow.ranger;

import io.acceldata.mlflow.ranger.MlflowRbacReconciler.ReconcileStats;
import io.acceldata.mlflow.ranger.MlflowRoleSpec.WorkspaceRoleKey;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Main polling and reconciliation loop for {@code ranger-mlflow-agent}.
 *
 * <p>Every {@code sync.poll.interval.ms}, polls Ranger Admin for:
 * <ol>
 *   <li>{@link ServicePolicies} ({@code getServicePoliciesIfUpdated})</li>
 *   <li>{@link RangerUserStore} ({@code getUserStoreIfUpdated})</li>
 *   <li>{@link RangerRoles} ({@code getRolesIfUpdated})</li>
 * </ol>
 *
 * <p>Whenever any of the three versions advances—or when a periodic
 * {@code forceResync} is triggered, or when a new user/workspace appears in
 * MLflow—the service converts the cached Ranger state into desired
 * {@link MlflowRoleSpec}s and runs {@link MlflowRbacReconciler} against MLflow's
 * native RBAC endpoints.
 *
 * <p>Notice that running {@link MlflowRbacReconciler#reconcile(Map)} on every
 * cycle once policies are loaded is cheap (two GET calls when nothing drifted:
 * {@code /roles/list} and {@code /users/list}) and automatically heals manual
 * edits in MLflow or newly registered MLflow users who belong to a Ranger group.
 */
public class PolicySyncService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PolicySyncService.class);

    static final long FORCE_RESYNC_VERSION = -1L;
    private static final long INITIAL_SERVICE_ID = -1L;

    private final AgentConfig              config;
    private final RangerAdminClient        adminClient;
    private final RbacConverter            converter;
    private final MlflowRbacClient         mlflowClient;
    private final MlflowRbacReconciler     reconciler;
    private final ScheduledExecutorService scheduler;

    private final AtomicLong lastKnownPolicyVersion    = new AtomicLong(FORCE_RESYNC_VERSION);
    private final AtomicLong lastKnownUserStoreVersion = new AtomicLong(FORCE_RESYNC_VERSION);
    private final AtomicLong lastKnownRoleVersion      = new AtomicLong(FORCE_RESYNC_VERSION);
    private final AtomicLong lastKnownServiceId        = new AtomicLong(INITIAL_SERVICE_ID);

    private final AtomicReference<ServicePolicies> cachedPolicies  = new AtomicReference<>();
    private final AtomicReference<RangerUserStore> cachedUserStore = new AtomicReference<>();
    private final AtomicReference<RangerRoles>     cachedRoles     = new AtomicReference<>();

    private final AtomicLong    cycleCount      = new AtomicLong(0);
    private final AtomicLong    successCount    = new AtomicLong(0);
    private final AtomicLong    failureCount    = new AtomicLong(0);
    private final AtomicLong    lastSuccessAtMs = new AtomicLong(-1);
    private final AtomicBoolean running         = new AtomicBoolean(false);

    public PolicySyncService(AgentConfig config,
                             RangerAdminClient adminClient,
                             RbacConverter converter,
                             MlflowRbacClient mlflowClient,
                             MlflowRbacReconciler reconciler) {
        this(config, adminClient, converter, mlflowClient, reconciler, defaultScheduler());
    }

    PolicySyncService(AgentConfig config,
                      RangerAdminClient adminClient,
                      RbacConverter converter,
                      MlflowRbacClient mlflowClient,
                      MlflowRbacReconciler reconciler,
                      ScheduledExecutorService scheduler) {
        this.config       = Objects.requireNonNull(config,       "config");
        this.adminClient  = Objects.requireNonNull(adminClient,  "adminClient");
        this.converter    = Objects.requireNonNull(converter,    "converter");
        this.mlflowClient = Objects.requireNonNull(mlflowClient, "mlflowClient");
        this.reconciler   = Objects.requireNonNull(reconciler,   "reconciler");
        this.scheduler    = Objects.requireNonNull(scheduler,    "scheduler");
    }

    private static ScheduledExecutorService defaultScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mlflow-policy-sync");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("PolicySyncService.start() called while already running");
            return;
        }
        LOG.info("PolicySyncService starting: pollInterval={}ms, forceResync={}ms",
                config.pollIntervalMs(), config.forceResyncMs());
        scheduler.scheduleWithFixedDelay(
                this::runOneCycle,
                0L,
                config.pollIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        LOG.info("PolicySyncService stopping...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
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

    void runOneCycle() {
        long cycleId = cycleCount.incrementAndGet();
        long startedAtMs = nowMillis();

        try {
            if (shouldForceResync(cycleId)) {
                LOG.info("cycleId={} — force-resync triggered", cycleId);
                lastKnownPolicyVersion.set(FORCE_RESYNC_VERSION);
                lastKnownUserStoreVersion.set(FORCE_RESYNC_VERSION);
                lastKnownRoleVersion.set(FORCE_RESYNC_VERSION);
            }

            long lastActivationMs = Math.max(0L, lastSuccessAtMs.get());
            long reqPolicyVer     = lastKnownPolicyVersion.get();
            long reqUserStoreVer  = lastKnownUserStoreVersion.get();
            long reqRoleVer       = lastKnownRoleVersion.get();

            // 1. Poll ServicePolicies from Ranger Admin
            ServicePolicies policyUpdate;
            try {
                policyUpdate = adminClient.getServicePoliciesIfUpdated(reqPolicyVer, lastActivationMs);
            } catch (Throwable t) {
                LOG.error("cycleId={} — Ranger policy fetch failed", cycleId, t);
                failureCount.incrementAndGet();
                return;
            }

            if (policyUpdate != null) {
                Long incomingServiceId = policyUpdate.getServiceId();
                if (incomingServiceId != null) {
                    long prevServiceId = lastKnownServiceId.get();
                    if (prevServiceId != INITIAL_SERVICE_ID && prevServiceId != incomingServiceId) {
                        LOG.warn("cycleId={} — Ranger serviceId changed {} -> {}; forcing full resync",
                                cycleId, prevServiceId, incomingServiceId);
                        lastKnownPolicyVersion.set(FORCE_RESYNC_VERSION);
                        lastKnownUserStoreVersion.set(FORCE_RESYNC_VERSION);
                        lastKnownRoleVersion.set(FORCE_RESYNC_VERSION);
                        lastKnownServiceId.set(incomingServiceId);
                        return;
                    }
                    lastKnownServiceId.set(incomingServiceId);
                }
                cachedPolicies.set(policyUpdate);
            }

            // 2. Poll RangerUserStore (best-effort if endpoint is unsupported on older setups)
            RangerUserStore userStoreUpdate = null;
            try {
                userStoreUpdate = adminClient.getUserStoreIfUpdated(reqUserStoreVer, lastActivationMs);
                if (userStoreUpdate != null) {
                    cachedUserStore.set(userStoreUpdate);
                }
            } catch (Throwable t) {
                LOG.warn("cycleId={} — Ranger user-store fetch failed (continuing with cached user-store): {}",
                        cycleId, t.getMessage());
            }

            // 3. Poll RangerRoles (best-effort)
            RangerRoles rolesUpdate = null;
            try {
                rolesUpdate = adminClient.getRolesIfUpdated(reqRoleVer, lastActivationMs);
                if (rolesUpdate != null) {
                    cachedRoles.set(rolesUpdate);
                }
            } catch (Throwable t) {
                LOG.warn("cycleId={} — Ranger roles fetch failed (continuing with cached roles): {}",
                        cycleId, t.getMessage());
            }

            ServicePolicies activePolicies = cachedPolicies.get();
            if (activePolicies == null) {
                LOG.debug("cycleId={} — no policies loaded yet", cycleId);
                return;
            }

            // 4. Discover active MLflow workspaces and convert Ranger state to MLflow RoleSpecs
            Set<String> workspaces = mlflowClient.listWorkspaces();
            Map<WorkspaceRoleKey, MlflowRoleSpec> desiredRoles = converter.convert(
                    activePolicies,
                    cachedUserStore.get(),
                    cachedRoles.get(),
                    workspaces);

            // 5. Reconcile MLflow native RBAC state
            ReconcileStats stats = reconciler.reconcile(desiredRoles);

            // 6. Commit versions only after reconciliation succeeds
            if (policyUpdate != null && policyUpdate.getPolicyVersion() != null) {
                lastKnownPolicyVersion.set(policyUpdate.getPolicyVersion());
            }
            if (userStoreUpdate != null && userStoreUpdate.getUserStoreVersion() != null) {
                lastKnownUserStoreVersion.set(userStoreUpdate.getUserStoreVersion());
            }
            if (rolesUpdate != null && rolesUpdate.getRoleVersion() != null) {
                lastKnownRoleVersion.set(rolesUpdate.getRoleVersion());
            }

            long durationMs = nowMillis() - startedAtMs;
            if (policyUpdate != null || userStoreUpdate != null || rolesUpdate != null || stats.hasChanges()) {
                LOG.info("cycleId={} — synced MLflow RBAC (policyVersion={}, desiredRoles={}, stats={}, durationMs={})",
                        cycleId, lastKnownPolicyVersion.get(), desiredRoles.size(), stats, durationMs);
            }

            successCount.incrementAndGet();
            lastSuccessAtMs.set(nowMillis());
        } catch (Throwable t) {
            LOG.error("cycleId={} — unexpected error during MLflow RBAC sync cycle", cycleId, t);
            failureCount.incrementAndGet();
        }
    }

    private boolean shouldForceResync(long cycleId) {
        long force = config.forceResyncMs();
        long poll  = config.pollIntervalMs();
        if (force <= 0 || poll <= 0) {
            return false;
        }
        if (force < poll) {
            return true;
        }
        long ratio = force / poll;
        return cycleId % ratio == 0;
    }

    long nowMillis() {
        return System.currentTimeMillis();
    }

    public long cycleCount()                  { return cycleCount.get(); }
    public long successCount()                { return successCount.get(); }
    public long failureCount()                { return failureCount.get(); }
    public long lastKnownPolicyVersion()      { return lastKnownPolicyVersion.get(); }
    public long lastKnownUserStoreVersion()   { return lastKnownUserStoreVersion.get(); }
    public long lastKnownRoleVersion()        { return lastKnownRoleVersion.get(); }
    public long lastSuccessAtMs()             { return lastSuccessAtMs.get(); }
    public boolean isReady()                  { return lastSuccessAtMs.get() > 0; }
    public boolean isRunning()                { return running.get(); }
}
