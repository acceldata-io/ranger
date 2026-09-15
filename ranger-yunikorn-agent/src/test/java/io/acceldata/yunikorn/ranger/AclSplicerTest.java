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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AclSplicer}. Three concerns covered:
 *
 * <ol>
 *   <li><b>Golden cases</b> — concrete inputs and expected outputs for the
 *       common shapes operators will hit.</li>
 *   <li><b>Splice invariant (the big one)</b> — for any non-ACL field in the
 *       input, the corresponding field in the output is structurally equal.
 *       Run against a corpus of hand-written and procedurally generated
 *       documents.</li>
 *   <li><b>Error paths</b> — malformed YAML and structural problems raise
 *       {@link YamlStructureException} rather than producing silently
 *       wrong output.</li>
 * </ol>
 */
class AclSplicerTest {

    private final AclSplicer splicer = new AclSplicer();

    // -----------------------------------------------------------------------
    // Sample documents used across multiple tests
    // -----------------------------------------------------------------------

    private static final String FLAT_DOC =
            "partitions:\n" +
            "  - name: default\n" +
            "    queues:\n" +
            "      - name: root\n" +
            "        submitacl: 'old-root-submit'\n" +
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

    private static final String NESTED_DOC =
            "partitions:\n" +
            "  - name: default\n" +
            "    queues:\n" +
            "      - name: root\n" +
            "        queues:\n" +
            "          - name: research\n" +
            "            queues:\n" +
            "              - name: nlp\n" +
            "                resources: { guaranteed: { memory: 30G, vcore: 15 } }\n" +
            "              - name: vision\n" +
            "                resources: { guaranteed: { memory: 30G, vcore: 15 } }\n" +
            "          - name: experiments\n" +
            "            queues:\n" +
            "              - name: nlp\n" +
            "                resources: { guaranteed: { memory: 10G, vcore: 5 } }\n";

    // -----------------------------------------------------------------------
    // Golden cases
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Golden cases — concrete input/output")
    class GoldenCases {

        @Test
        @DisplayName("LENIENT with no ACL map leaves the document unchanged in spirit")
        void lenientNoOp() {
            String out = splicer.splice(FLAT_DOC, Map.of(), UnmanagedDoctrine.LENIENT);

            // The non-ACL fields are preserved...
            assertThat(yamlValueAtPath(out, "partitions[0].name")).isEqualTo("default");
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].name"))
                    .isEqualTo("research");
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].resources.max.memory"))
                    .isEqualTo("500G");

            // ...and so are the ACLs that were already in the doc, since LENIENT
            // doesn't touch unmanaged queues.
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].submitacl"))
                    .isEqualTo("old-root-submit");
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].adminacl"))
                    .isEqualTo("admin");
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].submitacl"))
                    .isEqualTo("old-research");
        }

        @Test
        @DisplayName("STRICT with no ACL map clears all ACL fields (default-deny)")
        void strictDefaultDeny() {
            String out = splicer.splice(FLAT_DOC, Map.of(), UnmanagedDoctrine.STRICT);

            // ACL fields gone everywhere...
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].submitacl")).isNull();
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].adminacl")).isNull();
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].submitacl")).isNull();

            // ...but capacity untouched.
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].resources.guaranteed.memory"))
                    .isEqualTo("100G");
        }

        @Test
        @DisplayName("Existing operator-owned fields preserved when only one queue has a Ranger policy")
        void onlyOnePolicyApplied() {
            Map<String, QueueAclEntry> acls = Map.of(
                    "root.research", new QueueAclEntry(
                            "root.research", "alice,bob research-leads", null)
            );

            String out = splicer.splice(FLAT_DOC, acls, UnmanagedDoctrine.LENIENT);

            // Ranger-managed queue: submit ACL written, admin ACL absent (was null in entry).
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].submitacl"))
                    .isEqualTo("alice,bob research-leads");
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].adminacl"))
                    .isNull();

            // Capacity preserved on that queue.
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].resources.max.memory"))
                    .isEqualTo("500G");

            // Other queues unchanged under LENIENT.
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].submitacl"))
                    .isEqualTo("old-root-submit");
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[1].name"))
                    .isEqualTo("production");
        }

        @Test
        @DisplayName("Path collision: same leaf name under different parents gets correct ACL")
        void pathCollision() {
            Map<String, QueueAclEntry> acls = Map.of(
                    "root.research.nlp",     new QueueAclEntry(
                            "root.research.nlp",     "alice", null),
                    "root.experiments.nlp",  new QueueAclEntry(
                            "root.experiments.nlp",  "charlie", null)
            );

            String out = splicer.splice(NESTED_DOC, acls, UnmanagedDoctrine.LENIENT);

            // research.nlp should get alice
            assertThat(yamlValueAtPath(out,
                    "partitions[0].queues[0].queues[0].queues[0].name")).isEqualTo("nlp");
            assertThat(yamlValueAtPath(out,
                    "partitions[0].queues[0].queues[0].queues[0].submitacl")).isEqualTo("alice");

            // experiments.nlp should get charlie — different node despite same leaf name
            assertThat(yamlValueAtPath(out,
                    "partitions[0].queues[0].queues[1].queues[0].name")).isEqualTo("nlp");
            assertThat(yamlValueAtPath(out,
                    "partitions[0].queues[0].queues[1].queues[0].submitacl")).isEqualTo("charlie");
        }

        @Test
        @DisplayName("Entry with null submitAcl removes the field even when previously set")
        void nullAclRemovesField() {
            String docWithSubmit =
                    "partitions:\n" +
                    "  - name: default\n" +
                    "    queues:\n" +
                    "      - name: root\n" +
                    "        queues:\n" +
                    "          - name: foo\n" +
                    "            submitacl: 'previously-set'\n" +
                    "            adminacl: 'old-admin'\n";

            Map<String, QueueAclEntry> acls = Map.of(
                    "root.foo", new QueueAclEntry("root.foo", null, null)
            );

            String out = splicer.splice(docWithSubmit, acls, UnmanagedDoctrine.LENIENT);

            // Both ACL fields removed because the entry had nulls
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].submitacl")).isNull();
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].adminacl")).isNull();
            // The queue itself still exists with its name
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].name")).isEqualTo("foo");
        }

        @Test
        @DisplayName("Operator scalars are not retyped on round-trip (Norway problem / octal / sexagesimal)")
        void preservesScalarTypesOnRoundTrip() {
            // Values SnakeYAML's default YAML 1.1 resolver would corrupt:
            //   yes/no/on -> true/false,  0755 -> 493,  3:30 -> 210
            String doc =
                    "partitions:\n" +
                    "  - name: default\n" +
                    "    queues:\n" +
                    "      - name: root\n" +
                    "        properties:\n" +
                    "          autofill: yes\n" +
                    "          country: no\n" +
                    "          mode: on\n" +
                    "          build: 0755\n" +
                    "          ratio: 3:30\n" +
                    "          vcore: 50\n" +
                    "          weight: 1.5\n";

            // Touch only an ACL; every property must survive unchanged.
            Map<String, QueueAclEntry> acls = Map.of(
                    "root", new QueueAclEntry("root", "alice", null));

            String out = splicer.splice(doc, acls, UnmanagedDoctrine.LENIENT);

            // Assert on the serialized bytes — the bug is a serialization defect,
            // and re-parsing with a default YAML 1.1 loader would itself re-coerce
            // these, masking the result. The operator's literal text is preserved:
            assertThat(out).contains("autofill: yes");     // not -> true
            assertThat(out).contains("country: no");        // not -> false (Norway)
            assertThat(out).contains("mode: on");           // not -> true
            assertThat(out).contains("ratio: 3:30");        // not -> 210 (sexagesimal)
            assertThat(out).contains("0755");               // not -> 493 (octal); quoted is fine

            // The corrupted forms must NOT appear anywhere.
            assertThat(out).doesNotContain("autofill: true");
            assertThat(out).doesNotContain("country: false");
            assertThat(out).doesNotContain("210");
            assertThat(out).doesNotContain("493");

            // Genuine numbers stay numbers (no spurious quoting).
            assertThat(out).contains("vcore: 50");
            assertThat(out).contains("weight: 1.5");
            // And the ACL we meant to set is applied.
            assertThat(out).contains("submitacl: alice");
        }

        @Test
        @DisplayName("LENIENT: orphaned (formerly-managed) queue is reclaimed; never-managed queue left alone")
        void lenientReclaimsOrphanLeavesUnmanaged() {
            // root.research was managed last cycle and has no entry now (policy
            // deleted) → orphan. root itself was never managed by the agent.
            String out = splicer.splice(
                    FLAT_DOC,
                    Map.of(),
                    Set.of("root.research"),
                    UnmanagedDoctrine.LENIENT);

            // Orphan: the ACL the agent wrote is retracted, even under LENIENT.
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].submitacl")).isNull();
            // Capacity on the reclaimed queue is untouched.
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].queues[0].resources.max.memory"))
                    .isEqualTo("500G");
            // Never-managed root keeps its operator-owned ACLs (true LENIENT).
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].submitacl"))
                    .isEqualTo("old-root-submit");
            assertThat(yamlValueAtPath(out, "partitions[0].queues[0].adminacl"))
                    .isEqualTo("admin");
        }

        @Test
        @DisplayName("Determinism: same input, same ACLs, same output bytes")
        void deterministic() {
            Map<String, QueueAclEntry> acls = Map.of(
                    "root.research", new QueueAclEntry("root.research", "alice", null));
            String first  = splicer.splice(FLAT_DOC, acls, UnmanagedDoctrine.LENIENT);
            String second = splicer.splice(FLAT_DOC, acls, UnmanagedDoctrine.LENIENT);
            assertThat(first).isEqualTo(second);
        }
    }

    // -----------------------------------------------------------------------
    // The big one: splice invariant
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Splice invariant — non-ACL fields are preserved structurally")
    class SpliceInvariant {

        @Test
        @DisplayName("FLAT_DOC: non-ACL fields after splice equal those before splice")
        void flatDocPreservesNonAclFields() {
            assertNonAclFieldsPreserved(FLAT_DOC, Map.of(
                    "root.research", new QueueAclEntry("root.research", "ranger-set", null)
            ));
        }

        @Test
        @DisplayName("NESTED_DOC: non-ACL fields preserved across deep tree")
        void nestedDocPreservesNonAclFields() {
            assertNonAclFieldsPreserved(NESTED_DOC, Map.of(
                    "root.research.nlp",    new QueueAclEntry("root.research.nlp",   "alice", "admin"),
                    "root.experiments.nlp", new QueueAclEntry("root.experiments.nlp", "bob",  null)
            ));
        }

        @Test
        @DisplayName("STRICT clears ACLs but preserves all non-ACL fields")
        void strictPreservesNonAclFields() {
            assertNonAclFieldsPreserved(FLAT_DOC, Map.of());
        }

        @Test
        @DisplayName("Procedural fuzz: 50 random ACL maps preserve non-ACL fields")
        void fuzzPreservesNonAclFields() {
            // Deterministic 'random' so the test is reproducible without
            // depending on Math.random / new Date.
            int seed = 0;
            for (int i = 0; i < 50; i++) {
                Map<String, QueueAclEntry> acls = generateAclMap(seed + i);
                UnmanagedDoctrine doctrine = (i % 2 == 0)
                        ? UnmanagedDoctrine.LENIENT
                        : UnmanagedDoctrine.STRICT;
                assertNonAclFieldsPreservedDoctrine(NESTED_DOC, acls, doctrine);
                assertNonAclFieldsPreservedDoctrine(FLAT_DOC,   acls, doctrine);
            }
        }

        // ---- helpers ---------------------------------------------------

        private void assertNonAclFieldsPreserved(String doc,
                                                 Map<String, QueueAclEntry> acls) {
            assertNonAclFieldsPreservedDoctrine(doc, acls, UnmanagedDoctrine.LENIENT);
        }

        /**
         * Parse before and after, walk both trees, assert every non-ACL field
         * is structurally equal. ACL fields ({@code submitacl}/{@code adminacl})
         * are explicitly excluded from the comparison.
         */
        private void assertNonAclFieldsPreservedDoctrine(String doc,
                                                         Map<String, QueueAclEntry> acls,
                                                         UnmanagedDoctrine doctrine) {
            String spliced = splicer.splice(doc, acls, doctrine);

            Yaml yaml = new Yaml();
            Object before = yaml.load(doc);
            Object after  = yaml.load(spliced);

            assertStructurallyEqualIgnoringAcls(before, after, "$");
        }

        private void assertStructurallyEqualIgnoringAcls(Object before, Object after, String path) {
            if (before == null && after == null) return;

            if (before instanceof Map<?, ?> && after instanceof Map<?, ?>) {
                Map<?, ?> beforeMap = (Map<?, ?>) before;
                Map<?, ?> afterMap = (Map<?, ?>) after;
                // Compare every non-ACL key from `before`
                for (Map.Entry<?, ?> entry : beforeMap.entrySet()) {
                    String key = String.valueOf(entry.getKey());
                    if (isAclField(key)) continue;
                    assertThat(afterMap.keySet().stream().map(String::valueOf).collect(java.util.stream.Collectors.toList()))
                            .as("missing key '%s' at %s after splice", key, path)
                            .contains(key);
                    assertStructurallyEqualIgnoringAcls(
                            entry.getValue(), afterMap.get(entry.getKey()), path + "." + key);
                }
                // Make sure splice didn't *introduce* unexpected non-ACL keys
                for (Object k : afterMap.keySet()) {
                    String key = String.valueOf(k);
                    if (isAclField(key)) continue;
                    assertThat(beforeMap.keySet().stream().map(String::valueOf).collect(java.util.stream.Collectors.toList()))
                            .as("unexpected key '%s' appeared at %s after splice", key, path)
                            .contains(key);
                }
            } else if (before instanceof List<?> && after instanceof List<?>) {
                List<?> beforeList = (List<?>) before;
                List<?> afterList = (List<?>) after;
                assertThat(afterList).as("list size at %s changed", path).hasSameSizeAs(beforeList);
                for (int i = 0; i < beforeList.size(); i++) {
                    assertStructurallyEqualIgnoringAcls(
                            beforeList.get(i), afterList.get(i), path + "[" + i + "]");
                }
            } else {
                assertThat(after).as("scalar at %s changed", path).isEqualTo(before);
            }
        }

        private boolean isAclField(String key) {
            return AclSplicer.FIELD_SUBMIT_ACL.equals(key)
                    || AclSplicer.FIELD_ADMIN_ACL.equals(key);
        }

        /**
         * Generate a deterministic but variable ACL map keyed by valid queue
         * paths from our sample docs. The mix of populated and missing entries
         * exercises all the splicer branches.
         */
        private Map<String, QueueAclEntry> generateAclMap(int seed) {
            String[] paths = {
                    "root", "root.research", "root.research.nlp", "root.research.vision",
                    "root.experiments", "root.experiments.nlp", "root.production",
                    "root.does-not-exist"        // intentionally unmatched
            };
            Map<String, QueueAclEntry> map = new HashMap<>();
            for (int i = 0; i < paths.length; i++) {
                int bits = (seed + i) & 0b111;
                if ((bits & 1) == 0) continue;        // ~half the queues unmanaged
                String submit = (bits & 2) == 0 ? null : "user" + i;
                String admin  = (bits & 4) == 0 ? null : "admin" + i;
                map.put(paths[i], new QueueAclEntry(paths[i], submit, admin));
            }
            return map;
        }
    }

    // -----------------------------------------------------------------------
    // Error paths
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Error paths — structurally invalid YAML raises")
    class ErrorPaths {

        @Test
        @DisplayName("missing partitions raises YamlStructureException")
        void missingPartitions() {
            String doc = "queues: []\n";
            assertThatThrownBy(() ->
                    splicer.splice(doc, Map.of(), UnmanagedDoctrine.LENIENT))
                    .isInstanceOf(YamlStructureException.class)
                    .hasMessageContaining("partitions");
        }

        @Test
        @DisplayName("partitions as scalar raises")
        void partitionsAsScalar() {
            String doc = "partitions: oops\n";
            assertThatThrownBy(() ->
                    splicer.splice(doc, Map.of(), UnmanagedDoctrine.LENIENT))
                    .isInstanceOf(YamlStructureException.class)
                    .hasMessageContaining("partitions");
        }

        @Test
        @DisplayName("queue without name raises")
        void queueWithoutName() {
            String doc =
                    "partitions:\n" +
                    "  - name: default\n" +
                    "    queues:\n" +
                    "      - resources: { guaranteed: { memory: 1G } }\n";
            assertThatThrownBy(() ->
                    splicer.splice(doc, Map.of(), UnmanagedDoctrine.LENIENT))
                    .isInstanceOf(YamlStructureException.class)
                    .hasMessageContaining("name");
        }

        @Test
        @DisplayName("non-list queues field raises")
        void nonListQueues() {
            String doc =
                    "partitions:\n" +
                    "  - name: default\n" +
                    "    queues:\n" +
                    "      - name: root\n" +
                    "        queues: not-a-list\n";
            assertThatThrownBy(() ->
                    splicer.splice(doc, Map.of(), UnmanagedDoctrine.LENIENT))
                    .isInstanceOf(YamlStructureException.class);
        }

        @Test
        @DisplayName("empty document returns input unchanged")
        void emptyDocument() {
            String out = splicer.splice("", Map.of(), UnmanagedDoctrine.LENIENT);
            assertThat(out).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Path-collection helper used by other test classes
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("collectQueuePathsForTest returns full dotted paths in pre-order")
    void collectQueuePathsHelper() {
        List<String> paths = AclSplicer.collectQueuePathsForTest(NESTED_DOC);
        assertThat(paths).containsExactly(
                "root",
                "root.research",
                "root.research.nlp",
                "root.research.vision",
                "root.experiments",
                "root.experiments.nlp"
        );
    }

    // -----------------------------------------------------------------------
    // Tiny YAML-path navigator. Not a full JSONPath; just enough to read
    // values from test outputs without writing manual cast chains.
    //
    // Supported syntax:
    //   foo.bar         — map key access
    //   foo[3]          — list index
    //   foo.bar[3].baz  — chains thereof
    // -----------------------------------------------------------------------

    private static Object yamlValueAtPath(String yamlString, String path) {
        Yaml yaml = new Yaml();
        Object cur = yaml.load(yamlString);
        if (cur == null) return null;

        for (String segment : tokenizePath(path)) {
            if (cur == null) return null;
            if (segment.startsWith("[") && segment.endsWith("]")) {
                int idx = Integer.parseInt(segment.substring(1, segment.length() - 1));
                if (!(cur instanceof List<?>)) return null;
                List<?> list = (List<?>) cur;
                if (idx >= list.size()) return null;
                cur = list.get(idx);
            } else {
                if (!(cur instanceof Map<?, ?>)) return null;
                Map<?, ?> map = (Map<?, ?>) cur;
                cur = map.get(segment);
            }
        }
        return cur;
    }

    private static List<String> tokenizePath(String path) {
        // Walk the string, splitting on '.' and treating '[N]' segments specially.
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '.') {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else if (c == '[') {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
                int end = path.indexOf(']', i);
                tokens.add(path.substring(i, end + 1));
                i = end;
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) tokens.add(current.toString());
        return tokens;
    }
}
