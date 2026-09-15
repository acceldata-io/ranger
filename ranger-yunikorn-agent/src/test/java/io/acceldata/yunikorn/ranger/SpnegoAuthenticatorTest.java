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
import org.junit.jupiter.api.Test;

import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the bits of {@link SpnegoAuthenticator} that don't require a
 * live KDC. The actual SPNEGO handshake is exercised end-to-end via the
 * agent on a real cluster — there's no useful unit test for the
 * GSSManager call itself without spinning up a real Kerberos environment.
 */
class SpnegoAuthenticatorTest {

    @Test
    @DisplayName("Hostname is unchanged when not an IP address")
    void hostnameUnchanged() {
        assertThat(SpnegoAuthenticator.canonicalHostForSpn("ranger.example.com"))
                .isEqualTo("ranger.example.com");
    }

    @Test
    @DisplayName("Empty / null input returned unchanged")
    void emptyInput() {
        assertThat(SpnegoAuthenticator.canonicalHostForSpn(null)).isNull();
        assertThat(SpnegoAuthenticator.canonicalHostForSpn("")).isEqualTo("");
    }

    @Test
    @DisplayName("IP address triggers reverse-DNS path (best-effort)")
    void numericIpAttemptsReverseDns() {
        // 127.0.0.1 typically resolves back to "localhost" or itself depending
        // on /etc/hosts. Either is a valid result; we just want to confirm
        // the function doesn't throw.
        String result = SpnegoAuthenticator.canonicalHostForSpn("127.0.0.1");
        assertThat(result).isNotNull();
    }

    // ---- TGT re-login threshold -------------------------------------------

    @Test
    @DisplayName("A null TGT always requires re-login")
    void nullTgtNeedsRelogin() {
        assertThat(SpnegoAuthenticator.needsRelogin(null)).isTrue();
    }

    @Test
    @DisplayName("A fresh TGT (early in its lifetime) does not require re-login")
    void freshTgtDoesNotNeedRelogin() {
        long now = System.currentTimeMillis();
        // Started 1 min ago, valid for 10h → ~0% elapsed.
        KerberosTicket tgt = tgt(now - 60_000L, now + 10 * 3600_000L);
        assertThat(SpnegoAuthenticator.needsRelogin(tgt)).isFalse();
    }

    @Test
    @DisplayName("A TGT past 80% of its lifetime requires re-login")
    void agedTgtNeedsRelogin() {
        long now = System.currentTimeMillis();
        // 10h window, started 9h ago → 90% elapsed, past the 0.8 threshold.
        KerberosTicket tgt = tgt(now - 9 * 3600_000L, now + 1 * 3600_000L);
        assertThat(SpnegoAuthenticator.needsRelogin(tgt)).isTrue();
    }

    @Test
    @DisplayName("An already-expired TGT requires re-login")
    void expiredTgtNeedsRelogin() {
        long now = System.currentTimeMillis();
        KerberosTicket tgt = tgt(now - 10 * 3600_000L, now - 60_000L);
        assertThat(SpnegoAuthenticator.needsRelogin(tgt)).isTrue();
    }

    /** Minimal valid {@link KerberosTicket} with the given start/end window. */
    private static KerberosTicket tgt(long startMs, long endMs) {
        return new KerberosTicket(
                new byte[]{0x1},                                  // asn1Encoding (non-empty)
                new KerberosPrincipal("agent@ADSRE.COM"),         // client
                new KerberosPrincipal("krbtgt/ADSRE.COM@ADSRE.COM"), // server (the TGT)
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},               // sessionKey
                1,                                                // keyType
                null,                                             // flags → default
                new Date(startMs),                                // authTime
                new Date(startMs),                                // startTime
                new Date(endMs),                                  // endTime
                null,                                             // renewTill
                null);                                            // clientAddresses
    }
}
