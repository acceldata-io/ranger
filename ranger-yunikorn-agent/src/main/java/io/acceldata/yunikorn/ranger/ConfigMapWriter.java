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
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Reads the YuniKorn ConfigMap, splices Ranger-derived ACLs into its embedded
 * {@code queues.yaml}, and writes it back with optimistic concurrency.
 *
 * <h2>The K8s side of the integration</h2>
 * This is the only class that talks to the K8s API. The {@link AclSplicer}
 * does the actual YAML transformation; this class is the I/O wrapper around
 * it.
 *
 * <h2>Optimistic concurrency</h2>
 * The ConfigMap is co-owned with operators. To avoid losing operator edits
 * that land between our read and write, we use Kubernetes' native
 * {@code resourceVersion}-based compare-and-swap:
 *
 * <ol>
 *   <li>{@code GET configmaps/yunikorn-configs} — captures resourceVersion N</li>
 *   <li>{@code AclSplicer.splice(...)} — produces updated YAML</li>
 *   <li>{@code REPLACE configmaps/yunikorn-configs}, sending resourceVersion N</li>
 *   <li>If the API returns 409 Conflict (someone else wrote since our read),
 *       loop with a fresh fetch up to {@code MAX_CAS_ATTEMPTS} times.</li>
 * </ol>
 *
 * <p>The fabric8 client's {@code resource(...).update()} method handles the
 * CAS at the HTTP level — it sends the {@code resourceVersion} from the
 * object we pass in, and surfaces a {@link KubernetesClientException} with
 * code 409 on conflict. We catch that, refetch, and retry.
 *
 * <h2>Error semantics</h2>
 * <ul>
 *   <li>ConfigMap not found, namespace not accessible → {@link ConfigMapWriterException}.
 *       Caller (sync service) logs and tries again next cycle.</li>
 *   <li>Splicer rejects the document (malformed YAML) → propagates
 *       {@link YamlStructureException}. We refuse to write anything if we
 *       can't safely splice.</li>
 *   <li>Preflight rejects the spliced document → propagates
 *       {@link PreflightException}. We refuse to write a config YuniKorn
 *       won't accept (or that we couldn't validate).</li>
 *   <li>CAS exhaustion → {@link ConfigMapWriterException} after
 *       {@code MAX_CAS_ATTEMPTS}. Sync service moves on; reconciler-style
 *       retries happen on the next polling cycle.</li>
 * </ul>
 *
 * <h2>Threading</h2>
 * Each instance owns one {@link KubernetesClient}. The fabric8 client is
 * thread-safe, but this class assumes the sync service drives it from a
 * single thread (which we do — single-threaded {@code ScheduledExecutorService}).
 */
public class ConfigMapWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigMapWriter.class);

    /** Number of times to retry on 409 Conflict before giving up. */
    static final int MAX_CAS_ATTEMPTS = 5;

    /**
     * Annotation on the YuniKorn ConfigMap recording the set of queue paths
     * the agent currently manages (comma-separated, sorted). Read at the start
     * of each apply to detect orphans — queues we wrote to before but no
     * longer have a policy for — so a deleted Ranger policy actually clears
     * its ACL even under LENIENT. Persisting it on the ConfigMap (rather than
     * in memory) keeps orphan detection correct across agent restarts.
     */
    static final String MANAGED_QUEUES_ANNOTATION =
            "ranger-yunikorn-agent.acceldata.io/managed-queues";

    private final KubernetesClient k8s;
    private final AclSplicer        splicer;
    private final AgentConfig       config;
    private final boolean           ownsClient;

    /**
     * Validates the spliced YAML against YuniKorn before each write.
     * {@code null} when preflight is disabled or no YuniKorn REST URL is
     * configured — in which case the write proceeds without validation.
     */
    private final PreflightValidator preflight;

    /**
     * Production constructor. Builds an in-cluster {@link KubernetesClient}
     * via fabric8's auto-configuration (uses the pod's ServiceAccount token
     * mounted at {@code /var/run/secrets/kubernetes.io/serviceaccount/}) and
     * a {@link YuniKornPreflightClient} from config (may be a no-op).
     */
    public ConfigMapWriter(AgentConfig config) {
        this(new io.fabric8.kubernetes.client.KubernetesClientBuilder().build(),
             new AclSplicer(),
             config,
             YuniKornPreflightClient.forConfig(config),
             /* ownsClient = */ true);
    }

    /**
     * Test-friendly constructor. Lets callers inject a pre-built client
     * (typically {@code KubernetesMockServer.createClient()}) and a custom
     * splicer. Preflight is derived from {@code config} (skipped unless
     * {@code yunikorn.rest.url} is set).
     */
    ConfigMapWriter(KubernetesClient k8s, AclSplicer splicer, AgentConfig config) {
        this(k8s, splicer, config, YuniKornPreflightClient.forConfig(config), /* ownsClient = */ false);
    }

    /**
     * Test-friendly constructor that also injects an explicit
     * {@link PreflightValidator} (pass {@code null} to disable preflight, or
     * a fake to assert on validation behaviour without a live YuniKorn).
     */
    ConfigMapWriter(KubernetesClient k8s, AclSplicer splicer, AgentConfig config,
                    PreflightValidator preflight) {
        this(k8s, splicer, config, preflight, /* ownsClient = */ false);
    }

    private ConfigMapWriter(KubernetesClient k8s,
                            AclSplicer splicer,
                            AgentConfig config,
                            PreflightValidator preflight,
                            boolean ownsClient) {
        this.k8s        = Objects.requireNonNull(k8s,     "k8s");
        this.splicer    = Objects.requireNonNull(splicer, "splicer");
        this.config     = Objects.requireNonNull(config,  "config");
        this.preflight  = preflight;     // nullable: null == preflight disabled
        this.ownsClient = ownsClient;
    }

    /**
     * Splice {@code aclsByPath} into the YuniKorn ConfigMap's
     * {@code queues.yaml} and write it back.
     *
     * <p>If the rendered YAML is byte-identical to what's currently stored
     * AND the managed-queue set is unchanged, we skip the write entirely.
     * Saves a needless K8s API call and avoids triggering a k8shim reload
     * for nothing.
     *
     * <p><b>Orphan cleanup.</b> The agent records which queues it manages in
     * the {@value #MANAGED_QUEUES_ANNOTATION} annotation. On each apply, any
     * queue present in that annotation but absent from {@code aclsByPath}
     * (e.g. its Ranger policy was deleted or disabled) has its ACL fields
     * cleared — see {@link AclSplicer}. The annotation is then rewritten to
     * the current set, in the same atomic ConfigMap update.
     *
     * @param aclsByPath  the per-queue ACL state from {@link AclConverter}
     * @return            {@code true} if the ConfigMap was actually updated;
     *                    {@code false} if no change was needed.
     * @throws ConfigMapWriterException  on K8s API errors or CAS exhaustion
     * @throws YamlStructureException    if the existing ConfigMap can't be parsed
     * @throws PreflightException        if YuniKorn rejected the spliced config
     *                                   (or the validate endpoint was unreachable)
     */
    public boolean applyAcls(Map<String, QueueAclEntry> aclsByPath) {
        Objects.requireNonNull(aclsByPath, "aclsByPath");

        String namespace = config.yunikornNamespace();
        String name      = config.yunikornConfigMap();
        String confKey   = config.yunikornConfKey();

        // The queues this apply manages, persisted so the NEXT cycle can spot
        // orphans. Computed once — it doesn't depend on the fetched object.
        String newManagedValue = formatManagedSet(aclsByPath.keySet());

        for (int attempt = 1; attempt <= MAX_CAS_ATTEMPTS; attempt++) {
            ConfigMap current = fetch(namespace, name);
            String existingYaml = readYaml(current, confKey);

            // Queues the agent wrote to last cycle but no longer manages.
            // Their ACLs get retracted regardless of doctrine.
            Set<String> orphaned = new HashSet<>(readManagedSet(current));
            orphaned.removeAll(aclsByPath.keySet());

            String updatedYaml =
                    splicer.splice(existingYaml, aclsByPath, orphaned, config.doctrine());

            boolean yamlUnchanged    = existingYaml.equals(updatedYaml);
            boolean managedUnchanged = newManagedValue.equals(managedAnnotation(current));
            if (yamlUnchanged && managedUnchanged) {
                LOG.debug("No ACL changes detected; skipping ConfigMap write");
                return false;
            }
            if (!orphaned.isEmpty()) {
                LOG.info("Reclaiming {} orphaned queue ACL(s) (policy deleted/disabled): {}",
                        orphaned.size(), orphaned);
            }

            // Preflight: prove YuniKorn would accept the spliced document
            // before we overwrite the live ConfigMap. A rejection (or an
            // unreachable validator) throws PreflightException, aborting the
            // write — fail-closed. Skipped entirely when preflight is not
            // configured, or when the data YAML is unchanged (we're only
            // touching the bookkeeping annotation). Done inside the CAS loop so
            // we validate the exact bytes we're about to PUT (a retry
            // re-splices against a fresh fetch and so must re-validate).
            if (preflight != null && !yamlUnchanged) {
                preflight.validate(updatedYaml);
            }

            current.getData().put(confKey, updatedYaml);
            setManagedAnnotation(current, newManagedValue);

            try {
                k8s.configMaps()
                   .inNamespace(namespace)
                   .resource(current)
                   .update();
                LOG.info("ConfigMap {}/{} updated (attempt {}/{})",
                        namespace, name, attempt, MAX_CAS_ATTEMPTS);
                return true;
            } catch (KubernetesClientException e) {
                if (isConflict(e) && attempt < MAX_CAS_ATTEMPTS) {
                    LOG.warn("ConfigMap {}/{} update conflicted (HTTP {}, resourceVersion stale); " +
                             "retrying — attempt {}/{}",
                            namespace, name, e.getCode(), attempt, MAX_CAS_ATTEMPTS);
                    backoffBeforeRetry(attempt);
                    continue;
                }
                throw new ConfigMapWriterException(
                        "Failed to update ConfigMap " + namespace + "/" + name +
                        " after " + attempt + " attempts", e);
            }
        }

        // Fell out of the retry loop without success
        throw new ConfigMapWriterException(
                "Exhausted " + MAX_CAS_ATTEMPTS + " CAS retries updating " +
                namespace + "/" + name);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private ConfigMap fetch(String namespace, String name) {
        ConfigMap cm;
        try {
            cm = k8s.configMaps().inNamespace(namespace).withName(name).get();
        } catch (KubernetesClientException e) {
            throw new ConfigMapWriterException(
                    "Failed to fetch ConfigMap " + namespace + "/" + name, e);
        }
        if (cm == null) {
            throw new ConfigMapWriterException(
                    "ConfigMap " + namespace + "/" + name + " does not exist");
        }
        if (cm.getData() == null) {
            throw new ConfigMapWriterException(
                    "ConfigMap " + namespace + "/" + name + " has no data");
        }
        return cm;
    }

    private String readYaml(ConfigMap cm, String key) {
        String yaml = cm.getData().get(key);
        if (yaml == null) {
            throw new ConfigMapWriterException(
                    "ConfigMap " + config.yunikornNamespace() + "/" +
                    config.yunikornConfigMap() + " has no key '" + key + "'");
        }
        return yaml;
    }

    private static boolean isConflict(KubernetesClientException e) {
        // 409 Conflict: resourceVersion changed since our read.
        // 410 Gone: our resourceVersion aged out of the API server's cache.
        // Both are resolved the same way — refetch and retry.
        return e.getCode() == 409 || e.getCode() == 410;
    }

    /**
     * Brief randomized backoff between CAS retries so we don't lock-step with a
     * competing writer and burn all attempts in the same instant.
     */
    private static void backoffBeforeRetry(int attempt) {
        long delayMs = 50L * attempt + ThreadLocalRandom.current().nextLong(50L); // ~50–250ms
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt(); // preserve interrupt (e.g. shutdown)
            throw new ConfigMapWriterException("Interrupted during CAS retry backoff", ie);
        }
    }

    // -----------------------------------------------------------------------
    // Managed-queue ownership tracking (orphan detection)
    // -----------------------------------------------------------------------

    /** Raw value of the managed-queues annotation, or {@code ""} if absent. */
    private static String managedAnnotation(ConfigMap cm) {
        ObjectMeta meta = cm.getMetadata();
        if (meta == null || meta.getAnnotations() == null) return "";
        return meta.getAnnotations().getOrDefault(MANAGED_QUEUES_ANNOTATION, "");
    }

    /** Parse the managed-queues annotation into a set of queue paths. */
    private static Set<String> readManagedSet(ConfigMap cm) {
        String value = managedAnnotation(cm);
        if (value.isBlank()) return Collections.emptySet();
        Set<String> out = new HashSet<>();
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) out.add(trimmed);
        }
        return out;
    }

    /** Sorted, comma-joined queue paths — deterministic so no-op cycles stay byte-stable. */
    private static String formatManagedSet(Set<String> paths) {
        return paths.stream()
                .filter(p -> p != null && !p.isBlank())
                .sorted()
                .collect(Collectors.joining(","));
    }

    /**
     * Write (or, when empty, remove) the managed-queues annotation on the
     * ConfigMap object, preserving any other annotations already present.
     */
    private static void setManagedAnnotation(ConfigMap cm, String value) {
        ObjectMeta meta = cm.getMetadata();
        if (meta == null) {
            meta = new ObjectMeta();
            cm.setMetadata(meta);
        }
        Map<String, String> annotations = meta.getAnnotations();
        if (annotations == null) {
            annotations = new LinkedHashMap<>();
            meta.setAnnotations(annotations);
        }
        if (value.isEmpty()) {
            annotations.remove(MANAGED_QUEUES_ANNOTATION);
        } else {
            annotations.put(MANAGED_QUEUES_ANNOTATION, value);
        }
    }

    @Override
    public void close() {
        if (ownsClient) {
            k8s.close();
        }
    }
}
