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
package org.apache.ranger.services.pinot;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.pinot.client.PinotAdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Ranger Admin side of the Apache Pinot service: Test Connection and the database / table lookup used while editing
 * policies. Talks to the Pinot controller REST API only, so it needs no Pinot libraries.
 */
public class RangerServicePinot extends RangerBaseService {
    private static final Logger LOG = LoggerFactory.getLogger(RangerServicePinot.class);

    public static final String RESOURCE_CLUSTER  = "cluster";
    public static final String RESOURCE_DATABASE = "database";
    public static final String RESOURCE_TABLE    = "table";

    static final int MAX_LOOKUP_RESULTS = 1000;

    @Override
    public HashMap<String, Object> validateConfig() throws Exception {
        HashMap<String, Object> response = new HashMap<>();

        try {
            newClient(configs).testConnection();

            String message = "Connection test successful";

            BaseClient.generateResponseDataMap(true, message, message, null, null, response);
        } catch (Exception e) {
            LOG.warn("Pinot connection test failed for service {}: {}", serviceName, e.getMessage());

            String message = "Unable to connect to the Pinot controller: " + e.getMessage();

            BaseClient.generateResponseDataMap(false, message, message + ". Check the controller URL, the credentials, and that Ranger Admin can reach the controller.", null, null, response);
        }

        return response;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        if (context == null) {
            return Collections.emptyList();
        }

        String                    resource  = context.getResourceName();
        String                    input     = StringUtils.trimToEmpty(context.getUserInput());
        Map<String, List<String>> selected  = context.getResources() != null ? context.getResources() : Collections.<String, List<String>>emptyMap();
        PinotAdminClient          client    = newClient(configs);
        List<String>              candidates;

        if (RESOURCE_DATABASE.equals(resource)) {
            candidates = client.getDatabases();
        } else if (RESOURCE_TABLE.equals(resource)) {
            Set<String> tables    = new LinkedHashSet<>();
            List<String> databases = selected.get(RESOURCE_DATABASE);

            if (databases == null || databases.isEmpty()) {
                databases = Collections.singletonList("default");
            }

            List<String> known = null;

            for (String database : databases) {
                if (database.contains("*") || database.contains("?")) {
                    if (known == null) {
                        known = client.getDatabases();
                    }

                    for (String db : known) {
                        if (FilenameUtils.wildcardMatch(db, database)) {
                            tables.addAll(client.getTables(db));
                        }
                    }
                } else {
                    tables.addAll(client.getTables(database));
                }
            }

            candidates = new ArrayList<>(tables);
        } else {
            return Collections.emptyList(); // the cluster resource has no lookup: use *
        }

        List<String> already = selected.get(resource);
        List<String> ret     = new ArrayList<>();

        for (String name : candidates) {
            if (already != null && already.contains(name)) {
                continue;
            }

            if (input.isEmpty() || name.startsWith(input) || FilenameUtils.wildcardMatch(name, input)) {
                ret.add(name);

                if (ret.size() >= MAX_LOOKUP_RESULTS) {
                    break;
                }
            }
        }

        return ret;
    }

    PinotAdminClient newClient(Map<String, String> configs) {
        return new PinotAdminClient(configs != null ? configs : Collections.<String, String>emptyMap());
    }
}
