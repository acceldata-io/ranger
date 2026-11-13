/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.authorization.hadoop.config;

import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Additional configuration files given to RangerPluginConfig: they can carry the service name, and they keep
 * precedence over the class path files, service-specific overrides included.
 */
public class TestRangerPluginConfig {
    @Test
    public void classPathOnly() {
        RangerPluginConfig config = new RangerPluginConfig("cfgtest", null, null, null, null, null);

        assertEquals("from_classpath", config.getServiceName());
        assertEquals("classpath", config.get("cfgtest.x"));
    }

    @Test
    public void serviceNameFromAnAdditionalFile() throws Exception {
        File file = file("<property><name>ranger.plugin.cfgtest.service.name</name><value>svc1</value></property>"
                + "<property><name>cfgtest.x</name><value>additional</value></property>");

        RangerPluginConfig config = new RangerPluginConfig("cfgtest", null, null, null, null, Collections.singletonList(file), null);

        assertEquals("service name only in the additional file", "svc1", config.getServiceName());
        assertEquals("service-specific overrides of that service are loaded", "service-specific", config.get("cfgtest.y"));
        assertEquals("the additional file keeps precedence over them", "additional", config.get("cfgtest.x"));
    }

    @Test
    public void additionalFileOverridesTheClassPath() throws Exception {
        File file = file("<property><name>cfgtest.x</name><value>additional</value></property>");

        RangerPluginConfig config = new RangerPluginConfig("cfgtest", null, null, null, null, Collections.singletonList(file), null);

        assertEquals("from_classpath", config.getServiceName());
        assertEquals("additional", config.get("cfgtest.x"));
    }

    private static File file(String properties) throws Exception {
        File file = File.createTempFile("ranger-cfgtest-", ".xml");

        file.deleteOnExit();
        Files.write(file.toPath(), ("<?xml version=\"1.0\"?><configuration>" + properties + "</configuration>").getBytes(StandardCharsets.UTF_8));

        return file;
    }
}
