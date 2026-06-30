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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PolicySyncService}.
 *
 * <p>Approach: drive {@code runOneCycle()} synchronously instead of relying
 * on the scheduler. Mock {@link RangerAdminClient} so we control what each
 * cycle "sees" from Ranger. Use a real (in-memory) K8s mock for the
 * {@link ConfigMapWriter} so we exercise the actual write path.
 *
 * <p>What's covered:
 * <ul>
 *   <li><b>Happy paths</b> — version advance fetched, written, version recorded</li>
 *   <li><b>No-update path</b> — Ranger returns null, no write attempted</li>
 *   <li><b>Failure handling</b> — Ranger error / write error don't kill the loop
 *       and don't advance lastKnownVersion</li>
 *   <li><b>Service-id rotation</b> — change is detected, force-resync triggered</li>
 *   <li><b>Lifecycle</b> — start/stop idempotency, isReady semantics</li>
 * </ul>
 */
class PolicySyncServiceTest {

    private static final String NS  = "yunikorn";
    private static final String CM  = "yunikorn-configs";
    private static final String KEY = "queues.yaml";

    private static final String SAMPLE_YAML =
            "partitions:\n" +
            "  - name: default\n" +
            "    queues:\n" +
            "      - name: root\n" +
            "        queues:\n" +
            "          - name: research\n" +
            "            resources:\n" +
            "              guaranteed: { memory: 100G, vcore: 50 }\n" +
            "          - name: production\n" +
            "            resources:\n" +
            "              guaranteed: { memory: 50G, vcore: 25 }\n";

    private KubernetesServer        k8sServer;
    private KubernetesClient        k8sClient;
    private ConfigMapWriter         writer;
    private RangerAdminClient       admin;
    private AclConverter            converter;
    private AgentConfig             config;

    @BeforeEach
    void setUp() throws Throwable {
        k8sServer = new KubernetesServer(false, true);
        k8sServer.before();
        k8sClient = k8sServer.getClient();
        seedConfigMap(SAMPLE_YAML);

        config    = baseConfig();
        admin     = mock(RangerAdminClient.class);
        converter = new AclConverter();
        writer    = new ConfigMapWriter(k8sClient, new AclSplicer(), config);
    }

    @AfterEach
    void tearDown() {
        try { k8sServer.after(); } catch (Exception ignored) {}
    }

    private CountingScheduler scheduler;

    private PolicySyncService buildService() {
        // Real (counting) scheduler — tests drive runOneCycle() directly,
        // so the scheduler never actually fires anything; we just count
        // schedule() calls to assert lifecycle behaviour.
        scheduler = new CountingScheduler();
        return new PolicySyncService(config, admin, converter, writer, scheduler);
    }

    // -----------------------------------------------------------------------
    // Happy paths
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Happy paths")
    class HappyPaths {

        @Test
        @DisplayName("Version advance is fetched, written, recorded")
        void versionAdvances() throws Exception {
            ServicePolicies update = servicePolicies(/*version*/ 5L, /*serviceId*/ 1L,
                    List.of(submitPolicy("root.research", "alice", "bob")));
            when(admin.getServicePoliciesIfUpdated(eq(-1L), anyLong())).thenReturn(update);

            PolicySyncService svc = buildService();
            svc.runOneCycle();

            assertThat(svc.lastKnownVersion()).isEqualTo(5L);
            assertThat(svc.successCount()).isEqualTo(1L);
            assertThat(svc.failureCount()).isZero();
            assertThat(svc.isReady()).isTrue();

            String storedYaml = readYaml();
            assertThat(storedYaml).contains("alice,bob");
            assertThat(storedYaml).contains("memory: 100G");      // capacity preserved
        }

        @Test
        @DisplayName("Subsequent cycle uses the new lastKnownVersion")
        void cycleAdvancesVersion() throws Exception {
            ServicePolicies first = servicePolicies(5L, 1L,
                    List.of(submitPolicy("root.research", "alice")));
            ServicePolicies second = servicePolicies(7L, 1L,
                    List.of(submitPolicy("root.research", "alice", "bob")));

            when(admin.getServicePoliciesIfUpdated(eq(-1L), anyLong())).thenReturn(first);
            when(admin.getServicePoliciesIfUpdated(eq(5L), anyLong())).thenReturn(second);

            PolicySyncService svc = buildService();
            svc.runOneCycle();
            svc.runOneCycle();

            assertThat(svc.lastKnownVersion()).isEqualTo(7L);
            assertThat(readYaml()).contains("alice,bob");
        }

        @Test
        @DisplayName("Null response (no updates) → no write, no failure")
        void noUpdates() throws Exception {
            when(admin.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(null);

            PolicySyncService svc = buildService();
            svc.runOneCycle();

            assertThat(svc.successCount()).isEqualTo(1L);
            assertThat(svc.failureCount()).isZero();
            assertThat(svc.lastKnownVersion()).isEqualTo(-1L);    // unchanged
            assertThat(readYaml()).isEqualTo(SAMPLE_YAML);        // ConfigMap untouched
        }

        @Test
        @DisplayName("isReady returns false until first successful cycle")
        void notReadyUntilFirstSuccess() {
            PolicySyncService svc = buildService();
            assertThat(svc.isReady()).isFalse();
        }
    }

    // -----------------------------------------------------------------------
    // Failure handling
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Failure handling")
    class FailureHandling {

        @Test
        @DisplayName("Ranger fetch error increments failure, doesn't update version")
        void rangerFetchError() throws Exception {
            when(admin.getServicePoliciesIfUpdated(anyLong(), anyLong()))
                    .thenThrow(new RuntimeException("network down"));

            PolicySyncService svc = buildService();
            svc.runOneCycle();

            assertThat(svc.failureCount()).isEqualTo(1L);
            assertThat(svc.successCount()).isZero();
            assertThat(svc.lastKnownVersion()).isEqualTo(-1L);   // unchanged
            assertThat(svc.isReady()).isFalse();
        }

        @Test
        @DisplayName("Loop survives transient failure and recovers on next cycle")
        void loopRecoversAfterFailure() throws Exception {
            ServicePolicies update = servicePolicies(5L, 1L,
                    List.of(submitPolicy("root.research", "alice")));
            when(admin.getServicePoliciesIfUpdated(anyLong(), anyLong()))
                    .thenThrow(new RuntimeException("transient"))
                    .thenReturn(update);

            PolicySyncService svc = buildService();
            svc.runOneCycle();
            svc.runOneCycle();

            assertThat(svc.failureCount()).isEqualTo(1L);
            assertThat(svc.successCount()).isEqualTo(1L);
            assertThat(svc.lastKnownVersion()).isEqualTo(5L);
        }

        @Test
        @DisplayName("ConfigMap write failure does not advance version")
        void writeFailureNoVersionAdvance() throws Exception {
            ConfigMapWriter throwingWriter = new AlwaysThrowingWriter(
                    new ConfigMapWriterException("k8s api down"));

            ServicePolicies update = servicePolicies(5L, 1L,
                    List.of(submitPolicy("root.research", "alice")));
            when(admin.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(update);

            PolicySyncService svc = new PolicySyncService(
                    config, admin, converter, throwingWriter,
                    new CountingScheduler());
            svc.runOneCycle();

            assertThat(svc.failureCount()).isEqualTo(1L);
            assertThat(svc.lastKnownVersion()).isEqualTo(-1L);    // unchanged
        }

        @Test
        @DisplayName("YamlStructureException is logged but doesn't kill the loop")
        void malformedConfigMap() throws Exception {
            ConfigMapWriter brokenWriter = new AlwaysThrowingWriter(
                    new YamlStructureException("malformed"));

            ServicePolicies update = servicePolicies(5L, 1L,
                    List.of(submitPolicy("root.research", "alice")));
            when(admin.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(update);

            PolicySyncService svc = new PolicySyncService(
                    config, admin, converter, brokenWriter,
                    new CountingScheduler());
            svc.runOneCycle();
            svc.runOneCycle();   // confirm we keep trying

            assertThat(svc.failureCount()).isEqualTo(2L);
            assertThat(svc.lastKnownVersion()).isEqualTo(-1L);
        }
    }

    // -----------------------------------------------------------------------
    // Service-id rotation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Service-id rotation")
    class ServiceIdRotation {

        @Test
        @DisplayName("Change in serviceId triggers force-resync next cycle")
        void serviceIdChangeForcesResync() throws Exception {
            ServicePolicies first  = servicePolicies(5L, 1L,
                    List.of(submitPolicy("root.research", "alice")));
            ServicePolicies after  = servicePolicies(2L, 99L,    // <-- serviceId changed
                    List.of(submitPolicy("root.research", "alice", "bob")));
            ServicePolicies third  = servicePolicies(2L, 99L,
                    List.of(submitPolicy("root.research", "alice", "bob")));

            when(admin.getServicePoliciesIfUpdated(anyLong(), anyLong()))
                    .thenReturn(first, after, third);

            PolicySyncService svc = buildService();
            svc.runOneCycle();    // version 5, serviceId 1
            svc.runOneCycle();    // serviceId 99 detected, force-resync triggered, no write
            svc.runOneCycle();    // version 2 applied with new serviceId

            assertThat(svc.lastKnownVersion()).isEqualTo(2L);
            assertThat(readYaml()).contains("alice,bob");
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Lifecycle")
    class Lifecycle {

        @Test
        @DisplayName("start() is idempotent")
        void startIdempotent() throws Exception {
            CountingScheduler scheduler = new CountingScheduler();
            PolicySyncService svc = new PolicySyncService(
                    config, admin, converter, writer, scheduler);

            svc.start();
            svc.start();
            svc.start();

            // Scheduler should only have been told to schedule once.
            assertThat(scheduler.scheduleWithFixedDelayCount.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("stop() before start() is a no-op")
        void stopBeforeStart() {
            PolicySyncService svc = buildService();
            svc.stop();    // should not throw
            assertThat(svc.isRunning()).isFalse();
        }

        @Test
        @DisplayName("close() invokes stop()")
        void closeStops() throws Exception {
            PolicySyncService svc = buildService();
            svc.start();
            svc.close();
            assertThat(svc.isRunning()).isFalse();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void seedConfigMap(String yaml) {
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata().withNamespace(NS).withName(CM).endMetadata()
                .addToData(KEY, yaml)
                .build();
        k8sClient.configMaps().inNamespace(NS).resource(cm).create();
    }

    private String readYaml() {
        return k8sClient.configMaps().inNamespace(NS).withName(CM).get().getData().get(KEY);
    }

    private static AgentConfig baseConfig() {
        Properties p = new Properties();
        p.setProperty("ranger.admin.url",    "https://ranger.example.com:6182");
        p.setProperty("ranger.service.name", "test-yunikorn");
        p.setProperty("yunikorn.namespace",  NS);
        p.setProperty("yunikorn.configmap",  CM);
        p.setProperty("yunikorn.conf.key",   KEY);
        // Force-resync set very high so default tests don't trigger it
        p.setProperty("sync.poll.interval.ms", "1000");
        p.setProperty("sync.force.resync.ms",  "999999999");
        return AgentConfig.fromProperties(p, key -> null);
    }

    private static ServicePolicies servicePolicies(long version,
                                                   long serviceId,
                                                   List<RangerPolicy> policies) {
        ServicePolicies sp = new ServicePolicies();
        sp.setServiceId(serviceId);
        sp.setPolicyVersion(version);
        sp.setPolicies(policies);
        sp.setServiceName("test-yunikorn");
        return sp;
    }

    private static RangerPolicy submitPolicy(String queuePath, String... users) {
        RangerPolicy p = new RangerPolicy();
        p.setName("policy-" + queuePath);
        p.setIsEnabled(true);

        Map<String, RangerPolicyResource> resources = new HashMap<>();
        RangerPolicyResource queue = new RangerPolicyResource();
        queue.setValues(List.of(queuePath));
        resources.put("queue", queue);
        p.setResources(resources);

        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(Arrays.asList(users));
        item.setGroups(Collections.emptyList());
        item.setRoles(Collections.emptyList());

        RangerPolicyItemAccess access = new RangerPolicyItemAccess();
        access.setType("submit");
        access.setIsAllowed(true);
        item.setAccesses(List.of(access));

        p.setPolicyItems(List.of(item));
        return p;
    }

    /**
     * Test fixture: a {@link ConfigMapWriter} subclass that always throws a
     * configured exception from {@link #applyAcls}. Used to exercise the
     * sync service's failure handling without needing Mockito to mock the
     * concrete writer class (which the inline mock-maker rejects on Java 17
     * for classes that share lineage with JDK system types).
     */
    private final class AlwaysThrowingWriter extends ConfigMapWriter {
        private final RuntimeException toThrow;

        AlwaysThrowingWriter(RuntimeException toThrow) {
            super(k8sClient, new AclSplicer(), config);
            this.toThrow = toThrow;
        }

        @Override
        public boolean applyAcls(Map<String, QueueAclEntry> aclsByPath) {
            throw toThrow;
        }
    }

    /**
     * Test fixture: a no-op {@link ScheduledExecutorService} that counts how
     * many times each scheduling method is called. Tests drive
     * {@link PolicySyncService#runOneCycle()} directly, so the scheduler's
     * tasks never actually fire — we only need to assert the lifecycle
     * methods (start/stop) interact with it correctly.
     *
     * <p>We avoid mocking {@link ScheduledExecutorService} because Mockito's
     * inline mock-maker can't redefine that JDK class on Java 17.
     */
    private static final class CountingScheduler implements ScheduledExecutorService {

        final AtomicInteger scheduleWithFixedDelayCount = new AtomicInteger(0);
        private volatile boolean shutdown = false;

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            scheduleWithFixedDelayCount.incrementAndGet();
            return new NoopScheduledFuture();
        }

        @Override public ScheduledFuture<?> schedule(Runnable c, long d, TimeUnit u) { return new NoopScheduledFuture(); }
        @Override public <V> ScheduledFuture<V> schedule(Callable<V> c, long d, TimeUnit u) { return null; }
        @Override public ScheduledFuture<?> scheduleAtFixedRate(Runnable c, long i, long p, TimeUnit u) { return new NoopScheduledFuture(); }

        @Override public void shutdown() { shutdown = true; }
        @Override public List<Runnable> shutdownNow() { shutdown = true; return Collections.emptyList(); }
        @Override public boolean isShutdown() { return shutdown; }
        @Override public boolean isTerminated() { return shutdown; }
        @Override public boolean awaitTermination(long t, TimeUnit u) { return true; }

        @Override public <T> java.util.concurrent.Future<T> submit(Callable<T> task) { return null; }
        @Override public <T> java.util.concurrent.Future<T> submit(Runnable task, T result) { return null; }
        @Override public java.util.concurrent.Future<?> submit(Runnable task) { return null; }
        @Override public <T> List<java.util.concurrent.Future<T>> invokeAll(java.util.Collection<? extends Callable<T>> tasks) { return Collections.emptyList(); }
        @Override public <T> List<java.util.concurrent.Future<T>> invokeAll(java.util.Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) { return Collections.emptyList(); }
        @Override public <T> T invokeAny(java.util.Collection<? extends Callable<T>> tasks) { return null; }
        @Override public <T> T invokeAny(java.util.Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) { return null; }
        @Override public void execute(Runnable command) { /* noop */ }

        private static final class NoopScheduledFuture implements ScheduledFuture<Object> {
            @Override public long getDelay(TimeUnit unit) { return 0; }
            @Override public int compareTo(Delayed o) { return 0; }
            @Override public boolean cancel(boolean mayInterruptIfRunning) { return true; }
            @Override public boolean isCancelled() { return false; }
            @Override public boolean isDone() { return false; }
            @Override public Object get() { return null; }
            @Override public Object get(long timeout, TimeUnit unit) { return null; }
        }
    }
}
