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

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Translates a list of Ranger policies into a per-queue map of YuniKorn ACL
 * strings, ready for {@link AclSplicer} to write into {@code queues.yaml}.
 *
 * <h2>What it handles</h2>
 * <ul>
 *   <li><b>Users and groups in policy items</b> — accumulated into ACL strings.</li>
 *   <li><b>Two access types</b> — {@code submit} and {@code admin}.</li>
 *   <li><b>Multiple policies for the same queue</b> — users and groups are
 *       unioned (matches Ranger's own evaluator semantics).</li>
 *   <li><b>One policy with N queue values</b> — produces N entries in the
 *       output map.</li>
 * </ul>
 *
 * <h2>What it deliberately ignores (v1 scope)</h2>
 * <ul>
 *   <li><b>Roles.</b> Logged once per policy that uses them; their members are
 *       not flattened. Customers using roles will see a warning telling them
 *       to express access via users/groups directly until role flattening
 *       lands.</li>
 *   <li><b>Disabled policies</b> ({@code isEnabled() == false}).</li>
 *   <li><b>Deny rules</b> and <b>excludes</b>. The service-def disables them
 *       in the UI, but we still defensively skip {@code denyPolicyItems},
 *       {@code allowExceptions}, and {@code denyExceptions} in case they
 *       ever appear via the REST API or a downgraded service-def.</li>
 *   <li><b>Access entries with {@code isAllowed == false}.</b></li>
 *   <li><b>Wildcards in queue values.</b> Passed through verbatim; the
 *       splicer simply won't match them since real YuniKorn queue paths
 *       don't contain wildcards. Customers using {@code *} for a "deny by
 *       absence" pattern will need to enumerate.</li>
 * </ul>
 *
 * <h2>Output format</h2>
 * ACL strings follow YuniKorn's native format:
 * {@code "user1,user2 group1,group2"}. Users and groups are sorted
 * alphabetically for deterministic output. If a queue has only users (no
 * groups), the trailing space is omitted; if only groups, the leading user
 * portion is empty: {@code " devs,sre"}.
 *
 * <p>Names that don't fit YuniKorn's plain ACL form (e.g. AD names with
 * spaces, {@code [} or {@code :}) are percent-encoded, e.g.
 * {@code "%5BDEV%5D%20Admins"} for the group {@code "[DEV] Admins"}. The ODP
 * YuniKorn build decodes them; a stock YuniKorn ignores them, so they get no
 * access there rather than breaking the whole config.
 *
 * <p>{@code null} ACL means "no allow policy for this access type" — the
 * splicer interprets this as "remove the field". Empty entries are not
 * produced; if a queue has no users and no groups granted, no map entry
 * is emitted at all.
 *
 * <h2>Threading</h2>
 * Stateless. Safe to call concurrently.
 */
public final class AclConverter {

    private static final Logger LOG = LoggerFactory.getLogger(AclConverter.class);

    static final String ACCESS_SUBMIT = "submit";
    static final String ACCESS_ADMIN  = "admin";
    static final String RESOURCE_QUEUE = "queue";

    // Names YuniKorn accepts as-is in an ACL (yunikorn-core pkg/common/security/acl.go).
    private static final Pattern PLAIN_USER  = Pattern.compile("^[_a-zA-Z][a-zA-Z0-9_.@-]*[$]?$");
    private static final Pattern PLAIN_GROUP = Pattern.compile("^[_a-zA-Z][a-zA-Z0-9_-]*$");

    /**
     * @param policies the policy set as returned by Ranger Admin
     *                 (typically {@code servicePolicies.getPolicies()})
     * @return a map keyed by full dotted queue path, with merged ACL strings.
     *         Queues with no grants do not appear.
     */
    public Map<String, QueueAclEntry> convert(List<RangerPolicy> policies) {
        Objects.requireNonNull(policies, "policies must not be null");

        // Per-queue, per-access-type accumulators.
        // TreeSet keeps users/groups sorted → deterministic output.
        Map<String, Set<String>> submitUsers  = new HashMap<>();
        Map<String, Set<String>> submitGroups = new HashMap<>();
        Map<String, Set<String>> adminUsers   = new HashMap<>();
        Map<String, Set<String>> adminGroups  = new HashMap<>();

        int policiesProcessed = 0;
        int policiesSkipped   = 0;
        int rolesSeen         = 0;

        for (RangerPolicy policy : policies) {
            if (!isEligible(policy)) {
                policiesSkipped++;
                continue;
            }

            List<String> queuePaths = extractQueuePaths(policy);
            if (queuePaths.isEmpty()) {
                continue;
            }

            for (RangerPolicyItem item : policy.getPolicyItems()) {
                if (hasRoles(item)) {
                    rolesSeen++;
                    LOG.warn("Policy '{}' (id={}) uses roles {} — role flattening is " +
                                    "not yet implemented; users/groups in the same item " +
                                    "are still applied",
                            policy.getName(), policy.getId(), item.getRoles());
                }

                boolean grantsSubmit = grantsAccess(item, ACCESS_SUBMIT);
                boolean grantsAdmin  = grantsAccess(item, ACCESS_ADMIN);

                if (!grantsSubmit && !grantsAdmin) {
                    continue;
                }

                List<String> users  = nullSafe(item.getUsers());
                List<String> groups = nullSafe(item.getGroups());

                if (users.isEmpty() && groups.isEmpty()) {
                    continue;     // role-only item, nothing to add (yet)
                }

                for (String queuePath : queuePaths) {
                    if (grantsSubmit) {
                        addAll(submitUsers,  queuePath, users);
                        addAll(submitGroups, queuePath, groups);
                    }
                    if (grantsAdmin) {
                        addAll(adminUsers,   queuePath, users);
                        addAll(adminGroups,  queuePath, groups);
                    }
                }
            }

            policiesProcessed++;
        }

        // Build the final entries. Iterate the union of all queue paths
        // we've seen any grant on.
        Set<String> allQueues = new HashSet<>();
        allQueues.addAll(submitUsers.keySet());
        allQueues.addAll(submitGroups.keySet());
        allQueues.addAll(adminUsers.keySet());
        allQueues.addAll(adminGroups.keySet());

        Map<String, QueueAclEntry> result = new HashMap<>();
        for (String queuePath : allQueues) {
            String submitAcl = formatAcl(submitUsers.get(queuePath), submitGroups.get(queuePath));
            String adminAcl  = formatAcl(adminUsers.get(queuePath),  adminGroups.get(queuePath));
            if (submitAcl == null && adminAcl == null) {
                // Shouldn't happen given the accumulation logic above, but
                // defensively skip empty entries.
                continue;
            }
            result.put(queuePath, new QueueAclEntry(queuePath, submitAcl, adminAcl));
        }

        LOG.info("Converted Ranger policies: processed={}, skipped={}, queues={}, roleMentions={}",
                policiesProcessed, policiesSkipped, result.size(), rolesSeen);

        return result;
    }

    // -----------------------------------------------------------------------
    // Filters
    // -----------------------------------------------------------------------

    /** A policy is eligible when it's enabled and has at least one allow item. */
    private boolean isEligible(RangerPolicy policy) {
        if (policy == null) return false;
        if (Boolean.FALSE.equals(policy.getIsEnabled())) return false;
        if (policy.getPolicyItems() == null || policy.getPolicyItems().isEmpty()) return false;
        return true;
    }

    /**
     * Pull the queue resource values from a policy. Ranger's data model lets
     * a single policy target multiple queue values; we treat each as a
     * separate target.
     */
    private List<String> extractQueuePaths(RangerPolicy policy) {
        Map<String, RangerPolicyResource> resources = policy.getResources();
        if (resources == null) return Collections.emptyList();
        RangerPolicyResource queueResource = resources.get(RESOURCE_QUEUE);
        if (queueResource == null) return Collections.emptyList();
        List<String> values = queueResource.getValues();
        if (values == null) return Collections.emptyList();
        return values;
    }

    private boolean grantsAccess(RangerPolicyItem item, String accessType) {
        if (item.getAccesses() == null) return false;
        for (RangerPolicyItemAccess access : item.getAccesses()) {
            if (Boolean.FALSE.equals(access.getIsAllowed())) continue;
            if (accessType.equals(access.getType())) return true;
        }
        return false;
    }

    private boolean hasRoles(RangerPolicyItem item) {
        return item.getRoles() != null && !item.getRoles().isEmpty();
    }

    // -----------------------------------------------------------------------
    // Accumulators + formatter
    // -----------------------------------------------------------------------

    private void addAll(Map<String, Set<String>> accumulator,
                        String key,
                        Collection<String> values) {
        if (values.isEmpty()) return;
        accumulator.computeIfAbsent(key, k -> new TreeSet<>()).addAll(values);
    }

    private List<String> nullSafe(List<String> list) {
        return list == null ? Collections.emptyList() : list;
    }

    /**
     * Build a YuniKorn ACL string from accumulated users and groups.
     *
     * <p>Format rules:
     * <ul>
     *   <li>users only: {@code "alice,bob"}</li>
     *   <li>groups only: {@code " devs,sre"} (leading space, empty user side)</li>
     *   <li>both: {@code "alice,bob devs,sre"}</li>
     *   <li>neither: returns {@code null}</li>
     * </ul>
     *
     * <p>The leading space when only groups are present is intentional —
     * YuniKorn's parser splits on space first, so the empty user side has
     * to be a real empty token.
     */
    static String formatAcl(Set<String> users, Set<String> groups) {
        boolean hasUsers  = users  != null && !users.isEmpty();
        boolean hasGroups = groups != null && !groups.isEmpty();
        if (!hasUsers && !hasGroups) return null;

        String userPart  = hasUsers  ? joinAclNames(users,  PLAIN_USER)  : "";
        String groupPart = hasGroups ? joinAclNames(groups, PLAIN_GROUP) : "";

        if (hasUsers && !hasGroups) return userPart;
        return userPart + " " + groupPart;
    }

    private static String joinAclNames(Set<String> names, Pattern plain) {
        return names.stream().map(n -> aclName(n, plain)).collect(Collectors.joining(","));
    }

    /**
     * Returns the name as written into a YuniKorn ACL. Names matching
     * {@code plain} are kept as-is. Others are percent-encoded: every UTF-8
     * byte outside {@code [A-Za-z0-9_-]} becomes {@code %XX}, so the result
     * never contains the ACL separators (space, comma). YuniKorn only decodes
     * entries containing {@code %}, so a name that needed no encoding but
     * still isn't plain (e.g. {@code "1team"}) gets its first byte encoded.
     */
    static String aclName(String name, Pattern plain) {
        if (name.isEmpty() || plain.matcher(name).matches()) {
            return name;
        }
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        StringBuilder out = new StringBuilder();
        for (byte b : bytes) {
            int c = b & 0xFF;
            boolean keep = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9') || c == '_' || c == '-';
            out.append(keep ? String.valueOf((char) c) : String.format("%%%02X", c));
        }
        if (out.indexOf("%") < 0) {
            return String.format("%%%02X", bytes[0] & 0xFF) + out.substring(1);
        }
        return out.toString();
    }
}
