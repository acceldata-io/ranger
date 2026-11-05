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

import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Turns the principal Airflow authenticated into the short name Ranger's
 * usersync holds. Groups are never taken from the caller — RangerBasePlugin
 * resolves them from the user store when {@code use.rangerGroups} is set.
 *
 * <p>Three input shapes, in the order they are tried:
 *
 * <ol>
 *   <li>an LDAP distinguished name ({@code CN=alice,OU=...}, {@code uid=alice,ou=...})
 *       — the short name is the leftmost RDN value</li>
 *   <li>a Kerberos principal ({@code alice@REALM}, {@code alice/host@REALM})
 *       — evaluated through Hadoop's {@link KerberosName}, so the same
 *       {@code hadoop.security.auth_to_local} rules the rest of the ODP stack
 *       applies are applied here</li>
 *   <li>anything else — returned unchanged</li>
 * </ol>
 *
 * <p>Using {@code KerberosName} rather than splitting on {@code @} matters for
 * any realm whose rules do more than strip the realm: cross-realm trust,
 * service principals with an instance component, and rules that rewrite the
 * name. Splitting on {@code @} silently disagrees with the rest of the cluster
 * in exactly those cases, and the symptom is a user matching no policy at all.
 */
public final class IdentityNormalizer {

    private static final Logger LOG = LoggerFactory.getLogger(IdentityNormalizer.class);

    private static final AtomicBoolean FALLBACK_WARNED = new AtomicBoolean(false);

    private IdentityNormalizer() {}

    /**
     * Install {@code auth_to_local} rules. Call once at startup, before the
     * first request. A blank value leaves whatever Hadoop already configured
     * in place, which for a Kerberized deployment is the rule set loaded from
     * {@code core-site.xml}.
     */
    public static void configureRules(String rules) {
        if (rules != null && !rules.isBlank()) {
            try {
                KerberosName.setRules(rules);
                LOG.info("auth_to_local rules installed from agent configuration");
            } catch (Exception e) {
                // A bad rule string fails startup rather than the first
                // authorization request.
                throw new IllegalArgumentException(
                        "invalid hadoop.security.auth_to_local rules: " + e.getMessage(), e);
            }
            return;
        }
        ensureRules();
    }

    /**
     * Guarantee some rule set is installed, matching {@code StormRangerPlugin}:
     * Hadoop leaves the rules null until something loads a {@code core-site.xml}
     * carrying {@code hadoop.security.auth_to_local}, and a null rule set makes
     * every {@code getShortName()} throw.
     */
    private static void ensureRules() {
        if (KerberosName.getRules() == null) {
            KerberosName.setRules("DEFAULT");
            LOG.info("no auth_to_local rules configured; installed DEFAULT");
        }
    }

    public static String normalize(String principal) {
        if (principal == null) {
            return null;
        }
        String trimmed = principal.strip();
        if (trimmed.isEmpty()) {
            return trimmed;
        }

        String fromDn = firstRdnValue(trimmed);
        if (fromDn != null) {
            return fromDn;
        }

        int at = trimmed.indexOf('@');
        if (at > 0) {
            ensureRules();
            String shortName = kerberosShortName(trimmed);
            // Fall back to stripping the realm. Without configured rules
            // KerberosName refuses to map anything, and returning the full
            // principal would mean matching no policy at all — strictly worse
            // than the obvious interpretation.
            return shortName != null ? shortName : trimmed.substring(0, at);
        }
        return trimmed;
    }

    /** @return the short name, or null when no rule applies. */
    private static String kerberosShortName(String principal) {
        try {
            String shortName = new KerberosName(principal).getShortName();
            return shortName == null || shortName.isBlank() ? null : shortName;
        } catch (IOException | IllegalArgumentException | IllegalStateException e) {
            // No auth_to_local rule matched, or none are configured at all.
            // Warn once -- per request would flood, and silence would hide a
            // real cross-realm misconfiguration behind the realm-strip
            // fallback.
            if (FALLBACK_WARNED.compareAndSet(false, true)) {
                LOG.warn("auth_to_local produced no short name for '{}' ({}); falling back to "
                        + "stripping the realm. Set authz.agent.auth.to.local.rules, or put "
                        + "core-site.xml on the agent classpath, if the cluster rules do more "
                        + "than strip realms.", principal, e.getMessage());
            } else {
                LOG.debug("auth_to_local did not map '{}'; stripped realm instead", principal);
            }
            return null;
        }
    }

    /**
     * Leftmost RDN value: {@code CN=alice,OU=...} and {@code uid=alice,ou=...}
     * both become {@code alice}. A string with no {@code =} or {@code ,} is
     * not treated as a DN.
     */
    private static String firstRdnValue(String value) {
        if (value.indexOf('=') < 0 || value.indexOf(',') < 0) {
            return null;
        }
        String first = value.split(",", 2)[0].strip();
        int eq = first.indexOf('=');
        if (eq <= 0) {
            return null;
        }
        String shortName = first.substring(eq + 1).strip();
        return shortName.isEmpty() ? null : shortName;
    }
}
