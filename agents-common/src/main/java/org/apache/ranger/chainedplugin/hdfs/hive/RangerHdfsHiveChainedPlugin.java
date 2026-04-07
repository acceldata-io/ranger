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

package org.apache.ranger.chainedplugin.hdfs.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.service.RangerRMSChainedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * RangerHdfsHiveChainedPlugin enables Hive-HDFS ACL Sync (RMS).
 * It evaluates Hive policies for HDFS access requests when the HDFS path
 * maps to a Hive table/database.
 *
 * Configuration:
 * - ranger.plugin.hdfs.chained.services = cm_hive
 * - ranger.plugin.hdfs.chained.services.cm_hive.impl = org.apache.ranger.chainedplugin.hdfs.hive.RangerHdfsHiveChainedPlugin
 *
 * Access type mapping (HDFS -> Hive):
 * - read -> select
 * - write -> update, alter
 * - execute -> _any
 */
public class RangerHdfsHiveChainedPlugin extends RangerRMSChainedPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(RangerHdfsHiveChainedPlugin.class);

    private static final String HDFS_SERVICE_TYPE = "hive";
    private static final String HDFS_RESOURCE_PATH = "path";

    public RangerHdfsHiveChainedPlugin(RangerBasePlugin rootPlugin, String serviceName) {
        super(rootPlugin, HDFS_SERVICE_TYPE, serviceName);
        LOG.info("RangerHdfsHiveChainedPlugin created for service: {}", serviceName);
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

        Object pathValue = resource.getValue(HDFS_RESOURCE_PATH);
        if (pathValue != null) {
            return pathValue.toString();
        }

        Map<String, Object> resourceAsMap = resource.getAsMap();
        if (resourceAsMap != null && !resourceAsMap.isEmpty()) {
            Object path = resourceAsMap.get(HDFS_RESOURCE_PATH);
            if (path != null) {
                return path.toString();
            }
        }

        String resourceString = resource.getAsString();
        if (StringUtils.isNotBlank(resourceString)) {
            return resourceString;
        }

        return null;
    }

    @Override
    public void init() {
        LOG.info("==> RangerHdfsHiveChainedPlugin.init()");
        super.init();
        LOG.info("<== RangerHdfsHiveChainedPlugin.init(): Hive-HDFS ACL Sync enabled");
    }
}
