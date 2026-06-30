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

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the URL builder used in {@link RangerEndpointDiagnostic}. We don't
 * test the actual HTTP call (it requires a live Ranger); we just verify the
 * URL the agent will emit matches the shape Ranger's policy-download endpoint
 * expects.
 */
class RangerEndpointDiagnosticTest {

    @Test
    @DisplayName("URL trims trailing slashes from adminUrl")
    void trailingSlashTrimmed() {
        AgentConfig c = configWith("https://ranger.example:6182///", "yunikorn-prod", "yunikorn");
        String url = RangerEndpointDiagnostic.buildPolicyDownloadUrl(c);
        assertThat(url).startsWith("https://ranger.example:6182/service/plugins/secure/policies/download/");
        assertThat(url).doesNotContain("//service");
    }

    @Test
    @DisplayName("URL uses /secure/ path and contains expected query params")
    void urlShape() {
        AgentConfig c = configWith("http://ranger:6080", "yunikorn-prod", "yunikorn2");
        String url = RangerEndpointDiagnostic.buildPolicyDownloadUrl(c);

        assertThat(url).contains("/service/plugins/secure/policies/download/yunikorn-prod");
        assertThat(url).contains("lastKnownVersion=-1");
        assertThat(url).contains("lastActivationTime=0");
        assertThat(url).contains("pluginId=");
        // pluginId should embed the configured service-type
        assertThat(url).contains("yunikorn2");
    }

    @Test
    @DisplayName("pluginId uses the configured service-type")
    void pluginIdUsesServiceType() {
        AgentConfig c = configWith("http://ranger:6080", "yunikorn-prod", "yunikorn2");
        String pid = RangerEndpointDiagnostic.pluginId(c);
        assertThat(pid).startsWith("yunikorn2@");
        assertThat(pid).endsWith("-yunikorn-prod");
    }

    private static AgentConfig configWith(String url, String name, String type) {
        Properties p = new Properties();
        p.setProperty("ranger.admin.url",    url);
        p.setProperty("ranger.service.name", name);
        p.setProperty("ranger.service.type", type);
        return AgentConfig.fromProperties(p, key -> null);
    }
}
