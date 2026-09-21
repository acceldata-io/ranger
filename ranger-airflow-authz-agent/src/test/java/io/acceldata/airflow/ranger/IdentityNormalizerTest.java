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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IdentityNormalizerTest {

    @Test
    @DisplayName("strips Kerberos realm")
    void kerberos() {
        assertThat(IdentityNormalizer.normalize("alice@CORP.EXAMPLE")).isEqualTo("alice");
    }

    @Test
    @DisplayName("takes CN from an LDAP DN")
    void ldapDn() {
        assertThat(IdentityNormalizer.normalize("CN=alice,OU=users,DC=corp,DC=example"))
                .isEqualTo("alice");
    }

    @Test
    @DisplayName("short names pass through")
    void shortName() {
        assertThat(IdentityNormalizer.normalize("alice")).isEqualTo("alice");
    }

    @Test
    @DisplayName("blank and null stay blank/null")
    void blank() {
        assertThat(IdentityNormalizer.normalize(null)).isNull();
        assertThat(IdentityNormalizer.normalize("  ")).isEqualTo("");
    }
}
