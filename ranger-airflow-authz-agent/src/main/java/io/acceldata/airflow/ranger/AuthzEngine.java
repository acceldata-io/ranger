/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.airflow.ranger;

import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;

/**
 * The policy engine the HTTP layer talks to. Production wraps
 * {@code RangerBasePlugin}; tests substitute a fake.
 */
public interface AuthzEngine extends AutoCloseable {

    boolean isReady();

    String notReadyReason();

    long policyVersion();

    String serviceName();

    /**
     * Cluster name stamped on every audit record. Comes from
     * {@code ranger.plugin.airflow.access.cluster.name} via the plugin's own
     * {@code RangerPluginConfig} — the standard Ranger key — rather than a
     * second agent-specific property that could disagree with it.
     */
    String clusterName();

    Integer serviceDefVersion();

    /**
     * Version of the downloaded Ranger user store, or a negative value when no
     * user store has been downloaded. Group-based policies cannot match until
     * this is non-negative, so it is reported on {@code /v1/info} as a
     * diagnostic.
     */
    long userStoreVersion();

    /**
     * Groups the downloaded Ranger user store holds for a normalized username,
     * or an empty set when the store has no entry. These are the groups the
     * engine itself will evaluate against — the caller never supplies groups —
     * so this is the authoritative answer to "why did my group policy not
     * match". Exposed on {@code /v1/whoami}.
     */
    java.util.Set<String> resolvedGroups(String user);

    RangerAccessResult evaluate(RangerAccessRequest request);

    /**
     * Evaluate without writing an audit record.
     *
     * <p>Only the filter path uses this. A filter call evaluates hundreds of
     * keys for one user action, and the contract requires exactly one audit
     * record for the call — the non-permitted keys are not access attempts,
     * so recording them would be both voluminous and false. The caller
     * follows up with {@link #auditFilterSummary}.
     */
    RangerAccessResult evaluateNoAudit(RangerAccessRequest request);

    /**
     * Write the single audit record summarising a filter call.
     *
     * @param request     a representative request, carrying the user, resource
     *                    type, access type and request context
     * @param result      a representative result; prefer one that was allowed,
     *                    so the recorded policy id points at a policy that
     *                    actually granted something
     * @param allowed     true when at least one key passed
     * @param requestData replaces the request URI, e.g.
     *                    {@code filter: /api/v2/dags evaluated=812 allowed=5}
     */
    void auditFilterSummary(RangerAccessRequest request, RangerAccessResult result,
                            boolean allowed, String requestData);

    @Override
    void close();
}
