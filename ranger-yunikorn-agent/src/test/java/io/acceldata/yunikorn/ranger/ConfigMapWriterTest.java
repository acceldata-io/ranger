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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link ConfigMapWriter} against fabric8's in-memory Kubernetes mock
 * server. The mock implements enough of the K8s API surface to exercise our
 * read/splice/write/CAS flow end-to-end without needing a real cluster.
 *
 * <p>What's covered:
 * <ul>
 *   <li><b>Happy path:</b> ConfigMap exists, gets correctly updated.</li>
 *   <li><b>No-op detection:</b> when the splicer produces identical YAML,
 *       no API write is issued.</li>
 *   <li><b>Missing ConfigMap / key:</b> fail fast with a clear exception.</li>
 *   <li><b>Splicer rejection:</b> malformed YAML in the ConfigMap surfaces
 *       as a {@link YamlStructureException}, no write attempted.</li>
 * </ul>
 *
 * <p>The CAS retry path is tested via direct mocking in a separate class
 * because the in-memory mock server doesn't easily simulate 409 Conflict.
 */
class ConfigMapWriterTest {

    private static final String NS  = "yunikorn";
    private static final String CM  = "yunikorn-configs";
    private static final String KEY = "queues.yaml";

    private static final String SAMPLE_YAML =
            "partitions:\n" +
            "  - name: default\n" +
            "    queues:\n" +
            "      - name: root\n" +
            "        adminacl: 'admin'\n" +
            "        queues:\n" +
            "          - name: research\n" +
            "            submitacl: 'old-research'\n" +
            "            resources:\n" +
            "              guaranteed: { memory: 100G, vcore: 50 }\n" +
            "              max:        { memory: 500G, vcore: 250 }\n" +
            "          - name: production\n" +
            "            resources:\n" +
            "              guaranteed: { memory: 50G, vcore: 25 }\n";

    private KubernetesServer server;
    private KubernetesClient client;
    private AgentConfig      config;

    @BeforeEach
    void setUp() throws Throwable {
        // (https=false, crudMode=true): the mock implements full
        // create/read/update/delete on stored resources, behaving like a
        // real (in-memory) K8s API.
        server = new KubernetesServer(false, true);
        server.before();
        client = server.getClient();
        config = baseAgentConfig();
    }

    @AfterEach
    void tearDown() {
        try { server.after(); } catch (Exception ignored) {}
    }

    // -----------------------------------------------------------------------
    // Happy path
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Happy paths")
    class HappyPaths {

        @Test
        @DisplayName("Splices ACLs into existing ConfigMap and persists the change")
        void writesUpdatedYaml() {
            seedConfigMap(SAMPLE_YAML);
            ConfigMapWriter writer = new ConfigMapWriter(client, new AclSplicer(), config);

            boolean updated = writer.applyAcls(Map.of(
                    "root.research",
                    new QueueAclEntry("root.research", "alice,bob research-leads", null)
            ));

            assertThat(updated).isTrue();

            String storedYaml = readBackYaml();
            assertThat(storedYaml).contains("alice,bob research-leads");
            assertThat(storedYaml).contains("memory: 500G");      // capacity preserved
            assertThat(storedYaml).contains("name: production");  // structure preserved
        }

        @Test
        @DisplayName("STRICT doctrine flows through to the splicer")
        void strictDoctrinePropagates() {
            seedConfigMap(SAMPLE_YAML);
            AgentConfig strict = AgentConfig.fromProperties(
                    propsWithDoctrine("STRICT"), key -> null);
            ConfigMapWriter writer = new ConfigMapWriter(client, new AclSplicer(), strict);

            boolean updated = writer.applyAcls(Map.of());

            assertThat(updated).isTrue();
            String yaml = readBackYaml();
            // Pre-existing ACLs should be cleared (default-deny).
            assertThat(yaml).doesNotContain("old-research");
            assertThat(yaml).doesNotContain("adminacl: admin");
        }

        @Test
        @DisplayName("LENIENT + empty ACL map = no write (idempotent)")
        void noChangeNoWrite() {
            seedConfigMap(SAMPLE_YAML);
            ConfigMapWriter writer = new ConfigMapWriter(client, new AclSplicer(), config);

            // First call (under LENIENT, empty ACLs) — splicer returns
            // structurally-equal YAML; depending on SnakeYAML's serializer
            // it may not be byte-equal to the input due to formatting.
            // We accept that the *first* call may write once to canonicalise
            // the document, but after that, repeated empty applies must be
            // stable (no write).
            writer.applyAcls(Map.of());      // canonicalise (may write once)
            boolean second = writer.applyAcls(Map.of());

            assertThat(second).isFalse();
        }
    }

    // -----------------------------------------------------------------------
    // Error paths
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Error paths")
    class ErrorPaths {

        @Test
        @DisplayName("Missing ConfigMap → ConfigMapWriterException")
        void missingConfigMap() {
            // Don't seed anything.
            ConfigMapWriter writer = new ConfigMapWriter(client, new AclSplicer(), config);

            assertThatThrownBy(() -> writer.applyAcls(Map.of()))
                    .isInstanceOf(ConfigMapWriterException.class)
                    .hasMessageContaining("does not exist");
        }

        @Test
        @DisplayName("ConfigMap exists but is missing the queues.yaml key")
        void missingKey() {
            ConfigMap cm = new ConfigMapBuilder()
                    .withNewMetadata().withNamespace(NS).withName(CM).endMetadata()
                    .addToData("other-key", "value")
                    .build();
            client.configMaps().inNamespace(NS).resource(cm).create();

            ConfigMapWriter writer = new ConfigMapWriter(client, new AclSplicer(), config);

            assertThatThrownBy(() -> writer.applyAcls(Map.of()))
                    .isInstanceOf(ConfigMapWriterException.class)
                    .hasMessageContaining("queues.yaml");
        }

        @Test
        @DisplayName("Malformed YAML surfaces YamlStructureException, no write")
        void malformedYaml() {
            seedConfigMap("not: \n  - a: valid\n  - yunikorn: structure\n");
            ConfigMapWriter writer = new ConfigMapWriter(client, new AclSplicer(), config);

            assertThatThrownBy(() -> writer.applyAcls(Map.of()))
                    .isInstanceOf(YamlStructureException.class);

            // ConfigMap still has its original (malformed) content — we did not write
            String stored = readBackYaml();
            assertThat(stored).contains("yunikorn: structure");
        }

        @Test
        @DisplayName("ConfigMap with no data field is rejected")
        void configMapWithoutData() {
            // Note: fabric8's mock server normalises a missing `data` field
            // to an empty map (matching real K8s behaviour). So in practice
            // the missing-data branch in the writer manifests as "no key
            // queues.yaml" rather than "no data". We assert the user-visible
            // behaviour: a clear ConfigMapWriterException naming the missing
            // field, regardless of which internal branch caught it.
            ConfigMap cm = new ConfigMapBuilder()
                    .withNewMetadata().withNamespace(NS).withName(CM).endMetadata()
                    // no data
                    .build();
            client.configMaps().inNamespace(NS).resource(cm).create();

            ConfigMapWriter writer = new ConfigMapWriter(client, new AclSplicer(), config);

            assertThatThrownBy(() -> writer.applyAcls(Map.of()))
                    .isInstanceOf(ConfigMapWriterException.class);
        }
    }

    // -----------------------------------------------------------------------
    // Preflight
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Preflight")
    class Preflight {

        @Test
        @DisplayName("Rejection aborts the write — ConfigMap is left untouched")
        void rejectionAbortsWrite() {
            seedConfigMap(SAMPLE_YAML);
            PreflightValidator rejecting = yaml -> {
                throw new PreflightException("YuniKorn rejected the spliced queues.yaml: bad");
            };
            ConfigMapWriter writer =
                    new ConfigMapWriter(client, new AclSplicer(), config, rejecting);

            assertThatThrownBy(() -> writer.applyAcls(Map.of(
                    "root.research",
                    new QueueAclEntry("root.research", "alice", null)))
            ).isInstanceOf(PreflightException.class);

            // The original ACL must still be there; the new one must NOT.
            String stored = readBackYaml();
            assertThat(stored).contains("old-research");
            assertThat(stored).doesNotContain("alice");
        }

        @Test
        @DisplayName("Acceptance lets the write proceed")
        void acceptanceWrites() {
            seedConfigMap(SAMPLE_YAML);
            PreflightValidator accepting = yaml -> { /* allowed */ };
            ConfigMapWriter writer =
                    new ConfigMapWriter(client, new AclSplicer(), config, accepting);

            boolean updated = writer.applyAcls(Map.of(
                    "root.research",
                    new QueueAclEntry("root.research", "alice,bob", null)));

            assertThat(updated).isTrue();
            assertThat(readBackYaml()).contains("alice,bob");
        }

        @Test
        @DisplayName("No-op (byte-identical) write never calls preflight")
        void noChangeSkipsPreflight() {
            seedConfigMap(SAMPLE_YAML);
            int[] calls = {0};
            PreflightValidator counting = yaml -> calls[0]++;
            ConfigMapWriter writer =
                    new ConfigMapWriter(client, new AclSplicer(), config, counting);

            // First apply may canonicalise (one write + one preflight); the
            // second identical apply must short-circuit before preflight.
            writer.applyAcls(Map.of());
            int afterFirst = calls[0];
            writer.applyAcls(Map.of());

            assertThat(calls[0]).isEqualTo(afterFirst);   // no extra preflight on the no-op
        }

        @Test
        @DisplayName("Null validator (preflight disabled) writes without validating")
        void disabledPreflightWrites() {
            seedConfigMap(SAMPLE_YAML);
            ConfigMapWriter writer =
                    new ConfigMapWriter(client, new AclSplicer(), config, (PreflightValidator) null);

            boolean updated = writer.applyAcls(Map.of(
                    "root.research",
                    new QueueAclEntry("root.research", "carol", null)));

            assertThat(updated).isTrue();
            assertThat(readBackYaml()).contains("carol");
        }
    }

    // -----------------------------------------------------------------------
    // Orphan cleanup — Ranger policy deletion
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Orphan cleanup (policy deletion)")
    class OrphanCleanup {

        @Test
        @DisplayName("Deleting the policy clears the ACL the agent wrote — even under LENIENT")
        void deletedPolicyAclIsReclaimed() {
            seedConfigMap(SAMPLE_YAML);
            ConfigMapWriter writer = new ConfigMapWriter(client, new AclSplicer(), config); // LENIENT

            // Cycle 1: Ranger grants submit on root.research.
            writer.applyAcls(Map.of("root.research",
                    new QueueAclEntry("root.research", "alice,bob", null)));
            assertThat(readBackYaml()).contains("alice,bob");
            assertThat(managedAnnotationValue()).isEqualTo("root.research");

            // Cycle 2: the policy was deleted → empty ACL map. LENIENT, but
            // root.research was agent-managed, so its ACL must be reclaimed.
            boolean updated = writer.applyAcls(Map.of());

            assertThat(updated).isTrue();
            String yaml = readBackYaml();
            assertThat(yaml).doesNotContain("alice,bob");           // orphan reclaimed
            assertThat(yaml).contains("name: production");          // structure preserved
            assertThat(managedAnnotationValue()).isEmpty();         // ownership relinquished
        }

        @Test
        @DisplayName("Truly-unmanaged queues are untouched while an orphan is reclaimed")
        void leavesNeverManagedUntouched() {
            String yaml =
                    "partitions:\n" +
                    "  - name: default\n" +
                    "    queues:\n" +
                    "      - name: root\n" +
                    "        queues:\n" +
                    "          - name: research\n" +
                    "          - name: production\n" +
                    "            submitacl: 'operator-set'\n";
            seedConfigMap(yaml);
            ConfigMapWriter writer = new ConfigMapWriter(client, new AclSplicer(), config);

            writer.applyAcls(Map.of("root.research",
                    new QueueAclEntry("root.research", "alice", null)));
            writer.applyAcls(Map.of());   // research policy deleted

            String out = readBackYaml();
            assertThat(out).doesNotContain("alice");        // orphan reclaimed
            assertThat(out).contains("operator-set");       // never-managed left alone
        }

        @Test
        @DisplayName("Orphan tracking survives a restart (state lives on the ConfigMap)")
        void orphanStateSurvivesRestart() {
            seedConfigMap(SAMPLE_YAML);

            // First writer instance manages root.research, then "crashes".
            new ConfigMapWriter(client, new AclSplicer(), config)
                    .applyAcls(Map.of("root.research",
                            new QueueAclEntry("root.research", "alice,bob", null)));

            // A brand-new writer (fresh in-memory state) sees the policy gone.
            // It must still reclaim, because the managed-set is read from the
            // ConfigMap annotation, not from memory.
            ConfigMapWriter restarted = new ConfigMapWriter(client, new AclSplicer(), config);
            restarted.applyAcls(Map.of());

            assertThat(readBackYaml()).doesNotContain("alice,bob");
            assertThat(managedAnnotationValue()).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private String managedAnnotationValue() {
        ConfigMap cm = client.configMaps().inNamespace(NS).withName(CM).get();
        Map<String, String> ann = cm.getMetadata().getAnnotations();
        return ann == null ? "" : ann.getOrDefault(ConfigMapWriter.MANAGED_QUEUES_ANNOTATION, "");
    }

    private void seedConfigMap(String yaml) {
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata().withNamespace(NS).withName(CM).endMetadata()
                .addToData(KEY, yaml)
                .build();
        client.configMaps().inNamespace(NS).resource(cm).create();
    }

    private String readBackYaml() {
        ConfigMap cm = client.configMaps().inNamespace(NS).withName(CM).get();
        return cm.getData().get(KEY);
    }

    private static AgentConfig baseAgentConfig() {
        Properties p = baseProps();
        return AgentConfig.fromProperties(p, key -> null);
    }

    private static Properties baseProps() {
        Properties p = new Properties();
        p.setProperty("ranger.admin.url",    "https://ranger.example.com:6182");
        p.setProperty("ranger.service.name", "test-yunikorn");
        p.setProperty("yunikorn.namespace",  NS);
        p.setProperty("yunikorn.configmap",  CM);
        p.setProperty("yunikorn.conf.key",   KEY);
        return p;
    }

    private static Properties propsWithDoctrine(String doctrine) {
        Properties p = baseProps();
        p.setProperty("doctrine", doctrine);
        return p;
    }
}
