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

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.representer.Representer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Writes Ranger-derived ACLs into YuniKorn's {@code queues.yaml} document
 * without disturbing any operator-owned field.
 *
 * <h2>Why this exists</h2>
 * The YuniKorn ConfigMap is co-owned: operators manage queue structure,
 * capacity, placement rules, and properties; this agent manages ACL fields
 * only. The splicer is the discipline that enforces "Ranger writes only
 * {@code submitacl} and {@code adminacl}; everything else passes through
 * unchanged."
 *
 * <h2>Correctness invariants</h2>
 * <ol>
 *   <li><b>No Ranger-side typed model.</b> The YAML is parsed into a generic
 *       tree of {@code Map<String,Object>} / {@code List<Object>} and the
 *       same tree is re-serialized. Unknown fields stay because we never
 *       tried to understand them.</li>
 *
 *   <li><b>Queues identified by full dotted path</b> (e.g.
 *       {@code root.research.nlp}), never by leaf name. Different branches
 *       can legally have queues with the same leaf name; matching on leaf
 *       alone would corrupt the wrong queue.</li>
 *
 *   <li><b>Single in-memory transformation.</b> One parse, modifications in
 *       place on the tree, one dump. Caller does a single atomic ConfigMap
 *       write — no partial state visible to anyone.</li>
 *
 *   <li><b>Doctrine for unmanaged queues</b> ({@link UnmanagedDoctrine})
 *       is explicit, not implicit.</li>
 *
 *   <li><b>Deterministic.</b> Same input + same ACL map = same output.
 *       Block-style dumper, no timestamps, no random ordering.</li>
 *
 *   <li><b>Scalar fidelity.</b> Parsing uses a YAML 1.2 core-schema resolver
 *       ({@link Yaml12CoreResolver}) rather than SnakeYAML's YAML 1.1 default,
 *       so operator-owned scalars are not silently retyped on the round-trip
 *       (e.g. {@code yes} stays {@code yes}, not {@code true}; {@code 0755}
 *       stays {@code "0755"}, not {@code 493}). The one cosmetic effect is
 *       that a value which would otherwise be ambiguous (like {@code 0755})
 *       is emitted quoted to preserve it as a string.</li>
 * </ol>
 *
 * <h2>What this class does NOT do</h2>
 * <ul>
 *   <li>Validate the resulting YAML against YuniKorn's schema. The caller
 *       (typically {@code ConfigMapWriter}) handles preflight via
 *       {@code /ws/v1/validate-conf}.</li>
 *   <li>Read or write the K8s API. Pure function: YAML in, YAML out.</li>
 *   <li>Comment preservation. SnakeYAML at this layer does not round-trip
 *       comments. Callers should not rely on comments persisting across
 *       splice cycles. (Documented as a known limitation; if comment
 *       preservation becomes important, swap SnakeYAML for snakeyaml-engine
 *       in event-stream mode.)</li>
 * </ul>
 *
 * <h2>Threading</h2>
 * Stateless. Safe to call concurrently. (No instance fields beyond a final
 * {@code DumperOptions}.)
 */
public final class AclSplicer {

    static final String FIELD_PARTITIONS = "partitions";
    static final String FIELD_QUEUES     = "queues";
    static final String FIELD_NAME       = "name";
    static final String FIELD_SUBMIT_ACL = "submitacl";
    static final String FIELD_ADMIN_ACL  = "adminacl";

    private final DumperOptions dumperOptions;

    public AclSplicer() {
        this.dumperOptions = new DumperOptions();
        this.dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        this.dumperOptions.setPrettyFlow(true);
        this.dumperOptions.setIndent(2);
        this.dumperOptions.setIndicatorIndent(0);
    }

    /**
     * Build a {@link Yaml} for one load+dump cycle.
     *
     * <p>SnakeYAML instances are not thread-safe, so we make a fresh one per
     * {@link #splice} call (splice is otherwise stateless).
     *
     * <p>Two non-default choices, both about fidelity on a co-owned document:
     * <ul>
     *   <li>{@link Yaml12CoreResolver} — resolves scalar types with YAML 1.2
     *       core semantics, so operator values like {@code yes}, {@code 0755},
     *       and {@code 3:30} are NOT silently rewritten to {@code true},
     *       {@code 493}, {@code 210} (which SnakeYAML's default YAML 1.1
     *       resolver would do on the dump half of the round-trip).</li>
     *   <li>{@link SafeConstructor} — we only ever expect maps/lists/scalars,
     *       so refuse to instantiate arbitrary Java types from {@code !!} tags
     *       in the ConfigMap.</li>
     * </ul>
     */
    private Yaml newYaml() {
        LoaderOptions loaderOptions = new LoaderOptions();
        return new Yaml(
                new SafeConstructor(loaderOptions),
                new Representer(dumperOptions),
                dumperOptions,
                loaderOptions,
                new Yaml12CoreResolver());
    }

    /**
     * Returns a new YAML document with {@code submitacl}/{@code adminacl}
     * fields updated to match {@code aclsByPath}, leaving every other field
     * unchanged.
     *
     * @param queuesYaml  the existing {@code queues.yaml} content
     * @param aclsByPath  per-queue ACL state, keyed by full dotted path
     *                    ({@code "root.research.nlp"}). May be empty.
     * @param doctrine    behaviour for queues NOT present in {@code aclsByPath}
     * @return            updated YAML
     * @throws IllegalArgumentException if {@code queuesYaml} is null
     * @throws YamlStructureException   if the YAML's structure does not match
     *                                  the YuniKorn schema (missing
     *                                  {@code partitions}, non-list children, etc.)
     */
    public String splice(String queuesYaml,
                         Map<String, QueueAclEntry> aclsByPath,
                         UnmanagedDoctrine doctrine) {
        return splice(queuesYaml, aclsByPath, Collections.emptySet(), doctrine);
    }

    /**
     * Same as {@link #splice(String, Map, UnmanagedDoctrine)} but also
     * garbage-collects ACLs the agent previously wrote.
     *
     * <p>{@code orphanedPaths} are queues the agent managed on a prior cycle
     * (present in the last applied ACL map) but that no longer appear in
     * {@code aclsByPath} — typically because their Ranger policy was deleted
     * or disabled. The splicer <b>removes the {@code submitacl}/{@code adminacl}
     * fields on these queues regardless of doctrine</b>, because they were the
     * agent's to begin with. This is what makes a Ranger policy deletion
     * actually propagate under LENIENT (where truly-unmanaged queues are still
     * left untouched).
     *
     * <p>A path that is in both {@code aclsByPath} and {@code orphanedPaths}
     * is treated as managed (the entry wins); callers should pass the set
     * difference, but this guard keeps the method total.
     *
     * @param orphanedPaths queue paths whose agent-written ACLs should be cleared
     */
    public String splice(String queuesYaml,
                         Map<String, QueueAclEntry> aclsByPath,
                         Set<String> orphanedPaths,
                         UnmanagedDoctrine doctrine) {
        Objects.requireNonNull(queuesYaml,    "queuesYaml must not be null");
        Objects.requireNonNull(aclsByPath,    "aclsByPath must not be null");
        Objects.requireNonNull(orphanedPaths, "orphanedPaths must not be null");
        Objects.requireNonNull(doctrine,      "doctrine must not be null");

        Yaml yaml = newYaml();
        Object loaded = yaml.load(queuesYaml);

        if (loaded == null) {
            // Empty or whitespace-only document — nothing to splice into.
            // Return the input verbatim. Callers that hit this in practice
            // probably have a bigger problem with their ConfigMap.
            return queuesYaml;
        }
        if (!(loaded instanceof Map<?, ?>)) {
            throw new YamlStructureException(
                    "expected YAML to be a mapping at the root, got " +
                            loaded.getClass().getSimpleName());
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> root = (Map<String, Object>) loaded;

        Object partitionsObj = root.get(FIELD_PARTITIONS);
        if (partitionsObj == null) {
            throw new YamlStructureException("missing 'partitions' at root");
        }
        if (!(partitionsObj instanceof List<?>)) {
            throw new YamlStructureException(
                    "'partitions' must be a list, got " + partitionsObj.getClass().getSimpleName());
        }
        List<?> partitions = (List<?>) partitionsObj;

        for (Object partitionObj : partitions) {
            if (!(partitionObj instanceof Map<?, ?>)) {
                throw new YamlStructureException("each partition must be a mapping");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> partition = (Map<String, Object>) partitionObj;

            Object queues = partition.get(FIELD_QUEUES);
            if (queues == null) {
                continue;     // a partition with no queues is structurally fine
            }
            if (!(queues instanceof List<?>)) {
                throw new YamlStructureException(
                        "'queues' under a partition must be a list");
            }
            List<?> queueList = (List<?>) queues;
            walkQueues(queueList, /* parentPath = */ null, aclsByPath, orphanedPaths, doctrine);
        }

        return yaml.dump(root);
    }

    /**
     * Recursive pre-order traversal. For each queue node:
     * <ul>
     *   <li>compute its full dotted path from the parent;</li>
     *   <li>look up its ACL entry; apply the doctrine for non-matches;</li>
     *   <li>recurse into nested {@code queues}.</li>
     * </ul>
     */
    private void walkQueues(List<?> queueList,
                            String parentPath,
                            Map<String, QueueAclEntry> aclsByPath,
                            Set<String> orphanedPaths,
                            UnmanagedDoctrine doctrine) {
        for (Object queueObj : queueList) {
            if (!(queueObj instanceof Map<?, ?>)) {
                throw new YamlStructureException("each queue must be a mapping");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> queue = (Map<String, Object>) queueObj;

            Object nameObj = queue.get(FIELD_NAME);
            if (!(nameObj instanceof String) || ((String) nameObj).isBlank()) {
                throw new YamlStructureException(
                        "queue mapping is missing required 'name' field");
            }
            String name = (String) nameObj;

            String fullPath = (parentPath == null) ? name : parentPath + "." + name;

            applyAclEntry(queue, aclsByPath.get(fullPath),
                    orphanedPaths.contains(fullPath), doctrine);

            // Recurse into children. A leaf queue typically has no 'queues' key
            // at all — that's fine, we just skip recursion. An empty list is
            // also fine.
            Object children = queue.get(FIELD_QUEUES);
            if (children == null) {
                continue;
            }
            if (!(children instanceof List<?>)) {
                throw new YamlStructureException(
                        "'queues' under queue '" + fullPath + "' must be a list");
            }
            walkQueues((List<?>) children, fullPath, aclsByPath, orphanedPaths, doctrine);
        }
    }

    /**
     * Apply the ACL entry (or doctrine-determined default) to a single queue
     * node.
     *
     * <p><b>Mapping rules:</b></p>
     * <table>
     *   <tr><th>Entry state</th><th>Doctrine</th><th>Action on submitacl/adminacl</th></tr>
     *   <tr><td>Ranger has policy, ACL value non-null</td><td>any</td>
     *       <td>set the field to the value</td></tr>
     *   <tr><td>Ranger has policy, ACL value null</td><td>any</td>
     *       <td>remove the field (Ranger explicitly says no allow)</td></tr>
     *   <tr><td>No Ranger policy, queue was agent-managed last cycle</td><td>any</td>
     *       <td>remove the field — GC the ACL the agent itself wrote (handles
     *           Ranger policy deletion/disable)</td></tr>
     *   <tr><td>No Ranger policy, never agent-managed</td><td>LENIENT</td>
     *       <td>leave the field as-is (operator's value wins)</td></tr>
     *   <tr><td>No Ranger policy, never agent-managed</td><td>STRICT</td>
     *       <td>remove the field (default-deny)</td></tr>
     * </table>
     *
     * @param wasAgentManaged true if the agent wrote an ACL to this queue on a
     *                        prior cycle but no longer has an entry for it (an
     *                        orphan to be cleaned up)
     */
    private void applyAclEntry(Map<String, Object> queue,
                               QueueAclEntry entry,
                               boolean wasAgentManaged,
                               UnmanagedDoctrine doctrine) {
        if (entry != null) {
            applySingleAcl(queue, FIELD_SUBMIT_ACL, entry.submitAcl());
            applySingleAcl(queue, FIELD_ADMIN_ACL,  entry.adminAcl());
            return;
        }
        if (wasAgentManaged || doctrine == UnmanagedDoctrine.STRICT) {
            // Orphan cleanup (any doctrine): the agent owned this queue's ACL
            // and Ranger no longer grants it, so retract what we wrote.
            // STRICT additionally clears never-managed queues (default-deny).
            queue.remove(FIELD_SUBMIT_ACL);
            queue.remove(FIELD_ADMIN_ACL);
        }
        // LENIENT + never agent-managed: deliberately do nothing.
    }

    /**
     * If {@code value} is non-null, write it to {@code key}. If null, remove
     * the key entirely. Removing rather than writing an empty string keeps
     * the YAML clean and matches what an operator would naturally write
     * when they don't care about a particular ACL.
     */
    private void applySingleAcl(Map<String, Object> queue, String key, String value) {
        if (value == null) {
            queue.remove(key);
        } else {
            queue.put(key, value);
        }
    }

    // -----------------------------------------------------------------------
    // Internals exposed for testing only
    // -----------------------------------------------------------------------

    /** Visible-for-test helper: walk the tree and collect every queue's full path. */
    static List<String> collectQueuePathsForTest(String queuesYaml) {
        Yaml yaml = new Yaml();
        @SuppressWarnings("unchecked")
        Map<String, Object> root = yaml.load(queuesYaml);
        List<String> out = new ArrayList<>();
        if (root == null) return out;
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> partitions = (List<Map<String, Object>>) root.get(FIELD_PARTITIONS);
        if (partitions == null) return out;
        for (Map<String, Object> p : partitions) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> qs = (List<Map<String, Object>>) p.get(FIELD_QUEUES);
            if (qs != null) collectPathsRecursive(qs, null, out);
        }
        return out;
    }

    private static void collectPathsRecursive(List<Map<String, Object>> queues,
                                              String parentPath,
                                              List<String> out) {
        for (Map<String, Object> q : queues) {
            String name = (String) q.get(FIELD_NAME);
            if (name == null) continue;
            String full = (parentPath == null) ? name : parentPath + "." + name;
            out.add(full);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> children = (List<Map<String, Object>>) q.get(FIELD_QUEUES);
            if (children != null) collectPathsRecursive(children, full, out);
        }
    }
}
