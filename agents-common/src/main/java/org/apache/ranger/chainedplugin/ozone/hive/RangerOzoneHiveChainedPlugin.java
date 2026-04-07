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

package org.apache.ranger.chainedplugin.ozone.hive;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.service.RangerRMSChainedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * RangerOzoneHiveChainedPlugin enables Hive-Ozone ACL Sync (RMS).
 * It evaluates Hive policies for Ozone access requests when the Ozone key
 * maps to a Hive table/database.
 *
 * Configuration:
 * - ranger.plugin.ozone.chained.services = cm_hive
 * - ranger.plugin.ozone.chained.services.cm_hive.impl = org.apache.ranger.chainedplugin.ozone.hive.RangerOzoneHiveChainedPlugin
 *
 * Access type mapping (Ozone -> Hive Table):
 * - read -> select
 * - write -> update
 * - create -> create
 * - list -> select
 * - delete -> drop
 * - read_acl -> select
 * - write_acl -> update
 *
 * Access type mapping (Ozone -> Hive Database):
 * - read -> _any
 * - write -> update
 * - create -> create
 * - list -> select
 * - delete -> drop
 * - read_acl -> select
 * - write_acl -> update
 */
public class RangerOzoneHiveChainedPlugin extends RangerRMSChainedPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(RangerOzoneHiveChainedPlugin.class);

    private static final String OZONE_SERVICE_TYPE = "hive";
    private static final String OZONE_RESOURCE_VOLUME = "volume";
    private static final String OZONE_RESOURCE_BUCKET = "bucket";
    private static final String OZONE_RESOURCE_KEY = "key";

    public RangerOzoneHiveChainedPlugin(RangerBasePlugin rootPlugin, String serviceName) {
        super(rootPlugin, OZONE_SERVICE_TYPE, serviceName);
        LOG.info("RangerOzoneHiveChainedPlugin created for service: {}", serviceName);
    }

    @Override
    protected String extractStoragePath(RangerAccessRequest request) {
        if (request == null) {
            return null;
        }

        RangerAccessResource resource = request.getResource();
        if (resource == null) {
            return null;
        }

        StringBuilder pathBuilder = new StringBuilder();

        String volume = getResourceValue(resource, OZONE_RESOURCE_VOLUME);
        String bucket = getResourceValue(resource, OZONE_RESOURCE_BUCKET);
        String key = getResourceValue(resource, OZONE_RESOURCE_KEY);

        if (StringUtils.isNotBlank(volume)) {
            pathBuilder.append("/").append(volume);
        }

        if (StringUtils.isNotBlank(bucket)) {
            pathBuilder.append("/").append(bucket);
        }

        if (StringUtils.isNotBlank(key)) {
            if (!key.startsWith("/")) {
                pathBuilder.append("/");
            }
            pathBuilder.append(key);
        }

        String path = pathBuilder.toString();
        return StringUtils.isNotBlank(path) ? path : null;
    }

    private String getResourceValue(RangerAccessResource resource, String key) {
        Object value = resource.getValue(key);
        if (value != null) {
            return value.toString();
        }

        Map<String, Object> resourceAsMap = resource.getAsMap();
        if (MapUtils.isNotEmpty(resourceAsMap)) {
            Object mapValue = resourceAsMap.get(key);
            if (mapValue != null) {
                return mapValue.toString();
            }
        }

        return null;
    }

    @Override
    public void init() {
        LOG.info("==> RangerOzoneHiveChainedPlugin.init()");
        super.init();
        LOG.info("<== RangerOzoneHiveChainedPlugin.init(): Hive-Ozone ACL Sync enabled");
    }
}
