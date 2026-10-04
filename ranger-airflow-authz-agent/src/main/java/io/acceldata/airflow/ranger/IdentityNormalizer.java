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

/**
 * Turns the principal Airflow authenticated into the short name Ranger's
 * usersync holds. Groups are never taken from the caller — RangerBasePlugin
 * resolves them from the user store when {@code use.rangerGroups} is set.
 *
 * <p>M1 covers the two shapes we actually see: a Kerberos principal
 * ({@code alice@REALM}) and an LDAP DN ({@code CN=alice,...} or
 * {@code uid=alice,ou=people,...}). The short name is the first RDN value.
 * Full {@code auth_to_local} rule evaluation is M2.
 */
public final class IdentityNormalizer {

    private IdentityNormalizer() {}

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
            return trimmed.substring(0, at);
        }
        return trimmed;
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
