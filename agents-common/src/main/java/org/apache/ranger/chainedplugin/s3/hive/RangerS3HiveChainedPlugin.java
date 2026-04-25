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

package org.apache.ranger.chainedplugin.s3.hive;

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
 * RangerS3HiveChainedPlugin enables Hive-S3 ACL Sync (RMS).
 * It evaluates Hive policies for S3 access requests when the S3 path
 * maps to a Hive table/database.
 *
 * This plugin is designed to work with RAZ (Ranger Authorization Service) for AWS.
 *
 * Configuration:
 * - ranger.raz.service-type.s3.chained.services = {hive_service_name}
 * - ranger.raz.service-type.s3.chained.services.{hive_service_name}.impl = org.apache.ranger.chainedplugin.s3.hive.RangerS3HiveChainedPlugin
 *
 * Access type mapping (S3 -> Hive):
 * - read -> select
 * - write -> update, alter
 */
public class RangerS3HiveChainedPlugin extends RangerRMSChainedPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(RangerS3HiveChainedPlugin.class);

    private static final String S3_SERVICE_TYPE = "hive";
    private static final String S3_RESOURCE_BUCKET = "bucket";
    private static final String S3_RESOURCE_PATH = "path";
    private static final String S3_RESOURCE_OBJECT = "object";

    public RangerS3HiveChainedPlugin(RangerBasePlugin rootPlugin, String serviceName) {
        super(rootPlugin, S3_SERVICE_TYPE, serviceName);
        LOG.info("RangerS3HiveChainedPlugin created for service: {}", serviceName);
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

        String bucket = getResourceValue(resource, S3_RESOURCE_BUCKET);
        String path = getResourceValue(resource, S3_RESOURCE_PATH);

        if (StringUtils.isBlank(path)) {
            path = getResourceValue(resource, S3_RESOURCE_OBJECT);
        }

        if (StringUtils.isNotBlank(bucket)) {
            pathBuilder.append(bucket);
        }

        if (StringUtils.isNotBlank(path)) {
            if (pathBuilder.length() > 0 && !path.startsWith("/")) {
                pathBuilder.append("/");
            }
            pathBuilder.append(path);
        }

        String fullPath = pathBuilder.toString();
        return StringUtils.isNotBlank(fullPath) ? fullPath : null;
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
        LOG.info("==> RangerS3HiveChainedPlugin.init()");
        super.init();
        LOG.info("<== RangerS3HiveChainedPlugin.init(): Hive-S3 ACL Sync enabled");
    }
}
