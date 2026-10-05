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

class AgentConfigTest {

    private static final String TOKEN = "0123456789abcdef0123456789abcdef";

    @TempDir
    Path tmp;

    @Test
    @DisplayName("loads a valid properties file")
    void loadValid() throws Exception {
        Path token = writeToken();
        Path props = tmp.resolve("agent.properties");
        Files.writeString(props, ""
                + "authz.agent.bind.address=127.0.0.1\n"
                + "authz.agent.port=0\n"
                + "authz.agent.token.file=" + token.toAbsolutePath() + "\n"
                + "authz.agent.cluster.name=odp-dev\n");
        AgentConfig cfg = AgentConfig.load(props);
        assertThat(cfg.bindAddress()).isEqualTo("127.0.0.1");
        assertThat(cfg.port()).isZero();
        assertThat(cfg.clusterName()).isEqualTo("odp-dev");
        assertThat(cfg.serviceType()).isEqualTo("airflow");
        assertThat(cfg.sharedSecret().matches("Bearer " + TOKEN)).isTrue();
    }

    @Test
    @DisplayName("refuses a non-loopback bind address")
    void refusesNonLoopback() {
        assertThatThrownBy(() -> AgentConfig.requireLoopback("0.0.0.0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("loopback");
        assertThatThrownBy(() -> AgentConfig.requireLoopback("192.168.1.10"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("loopback");
    }

    @Test
    @DisplayName("localhost is loopback")
    void localhostOk() {
        assertThat(AgentConfig.requireLoopback("localhost")).isEqualTo("localhost");
    }

    private Path writeToken() throws Exception {
        Path file = tmp.resolve("token");
        Files.writeString(file, TOKEN);
        Files.setPosixFilePermissions(file, Set.of(PosixFilePermission.OWNER_READ));
        return file;
    }
}
