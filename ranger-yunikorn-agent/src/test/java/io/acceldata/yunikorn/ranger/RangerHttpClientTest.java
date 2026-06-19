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

class RangerHttpClientTest {

    @Test
    @DisplayName("URL hits the /secure/ path and includes all expected query params")
    void buildUrlSecureWithParams() {
        AgentConfig config = configWith("http://ranger:6080", "yunikorn-prod", "yunikorn2");
        RangerHttpClient client = new RangerHttpClient(config);

        String url = client.buildUrl(-1L, 1234567890L);

        assertThat(url).startsWith("http://ranger:6080/service/plugins/secure/policies/download/yunikorn-prod");
        assertThat(url).contains("lastKnownVersion=-1");
        assertThat(url).contains("lastActivationTime=1234567890");
        assertThat(url).contains("pluginId=");
        // pluginId encodes serviceType@host-serviceName
        assertThat(url).contains("yunikorn2");
        assertThat(url).contains("yunikorn-prod");
    }

    @Test
    @DisplayName("Trailing slashes on adminUrl are trimmed")
    void trailingSlashTrimmed() {
        AgentConfig config = configWith("http://ranger:6080///", "yunikorn-prod", "yunikorn2");
        RangerHttpClient client = new RangerHttpClient(config);

        String url = client.buildUrl(0L, 0L);
        assertThat(url).startsWith("http://ranger:6080/service/plugins/secure/policies/download/");
        assertThat(url).doesNotContain("//service");
    }

    @Test
    @DisplayName("Different service-types produce different pluginId prefixes")
    void serviceTypeFlowsIntoPluginId() {
        String urlA = new RangerHttpClient(configWith("http://r:80", "svc", "yunikorn"))
                .buildUrl(0L, 0L);
        String urlB = new RangerHttpClient(configWith("http://r:80", "svc", "yunikorn2"))
                .buildUrl(0L, 0L);
        assertThat(urlA).contains("yunikorn%40");
        assertThat(urlB).contains("yunikorn2%40");
        assertThat(urlA).isNotEqualTo(urlB);
    }

    private static AgentConfig configWith(String url, String name, String type) {
        Properties p = new Properties();
        p.setProperty("ranger.admin.url",    url);
        p.setProperty("ranger.service.name", name);
        p.setProperty("ranger.service.type", type);
        return AgentConfig.fromProperties(p, key -> null);
    }
}
