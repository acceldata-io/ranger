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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SharedSecretTest {

    private static final String TOKEN = "0123456789abcdef0123456789abcdef";

    @TempDir
    Path tmp;

    @Test
    @DisplayName("Bearer token matches; missing or wrong header does not")
    void matchesBearer() throws Exception {
        SharedSecret secret = SharedSecret.fromFile(tokenFile(TOKEN));
        assertThat(secret.matches("Bearer " + TOKEN)).isTrue();
        assertThat(secret.matches("bearer " + TOKEN)).isTrue();
        assertThat(secret.matches("Bearer wrong-token-that-is-32-bytes!!")).isFalse();
        assertThat(secret.matches(null)).isFalse();
        assertThat(secret.matches("Basic " + TOKEN)).isFalse();
        assertThat(secret.matches("Bearer ")).isFalse();
    }

    @Test
    @DisplayName("refuses a world-readable token file")
    void refusesWorldReadable() throws Exception {
        Path file = tokenFile(TOKEN);
        Files.setPosixFilePermissions(file, Set.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OTHERS_READ));
        assertThatThrownBy(() -> SharedSecret.fromFile(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("0400");
    }

    @Test
    @DisplayName("refuses a short token")
    void refusesShortToken() throws Exception {
        Path file = tokenFile("tooshort");
        assertThatThrownBy(() -> SharedSecret.fromFile(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("32");
    }

    @Test
    @DisplayName("refuses a missing file")
    void refusesMissing() {
        assertThatThrownBy(() -> SharedSecret.fromFile(tmp.resolve("nope")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing");
    }

    private Path tokenFile(String token) throws Exception {
        Path file = tmp.resolve("token");
        Files.writeString(file, token);
        Files.setPosixFilePermissions(file, Set.of(PosixFilePermission.OWNER_READ));
        return file;
    }
}
