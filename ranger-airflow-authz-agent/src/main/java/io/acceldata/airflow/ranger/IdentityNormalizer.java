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
 * ({@code alice@REALM}) and an LDAP DN ({@code CN=alice,...}). Full
 * {@code auth_to_local} rule evaluation is M2.
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
        String fromDn = cnFromDn(trimmed);
        if (fromDn != null) {
            return fromDn;
        }
        int at = trimmed.indexOf('@');
        if (at > 0) {
            return trimmed.substring(0, at);
        }
        return trimmed;
    }

    private static String cnFromDn(String value) {
        if (value.indexOf('=') < 0 || value.indexOf(',') < 0) {
            return null;
        }
        for (String part : value.split(",")) {
            String piece = part.strip();
            int eq = piece.indexOf('=');
            if (eq > 0 && piece.substring(0, eq).strip().equalsIgnoreCase("CN")) {
                String cn = piece.substring(eq + 1).strip();
                return cn.isEmpty() ? null : cn;
            }
        }
        return null;
    }
}
