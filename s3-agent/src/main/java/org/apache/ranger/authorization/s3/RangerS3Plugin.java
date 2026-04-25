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

package org.apache.ranger.authorization.s3;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RangerS3Plugin is the Ranger plugin for S3 authorization.
 * It can be used with RAZ-like architectures or standalone S3 authorization interceptors.
 *
 * This plugin supports:
 * 1. Direct S3 policy evaluation
 * 2. Chained Hive policy evaluation via RMS for Hive-S3 ACL sync
 */
public class RangerS3Plugin extends RangerBasePlugin {
    private static final Logger LOG = LoggerFactory.getLogger(RangerS3Plugin.class);

    public static final String SERVICE_TYPE = "s3";
    public static final String APP_ID = "s3";

    private static volatile RangerS3Plugin instance;

    public RangerS3Plugin() {
        super(SERVICE_TYPE, APP_ID);
    }

    public RangerS3Plugin(String serviceName) {
        super(SERVICE_TYPE, serviceName, APP_ID);
    }

    public RangerS3Plugin(RangerPluginConfig pluginConfig) {
        super(pluginConfig);
    }

    /**
     * Get the singleton instance of the S3 plugin.
     */
    public static RangerS3Plugin getInstance() {
        RangerS3Plugin plugin = instance;

        if (plugin == null) {
            synchronized (RangerS3Plugin.class) {
                plugin = instance;

                if (plugin == null) {
                    plugin = new RangerS3Plugin();
                    plugin.init();
                    instance = plugin;
                }
            }
        }

        return plugin;
    }

    /**
     * Get or create an instance with specific service name.
     */
    public static RangerS3Plugin getInstance(String serviceName) {
        RangerS3Plugin plugin = instance;

        if (plugin == null) {
            synchronized (RangerS3Plugin.class) {
                plugin = instance;

                if (plugin == null) {
                    plugin = new RangerS3Plugin(serviceName);
                    plugin.init();
                    instance = plugin;
                }
            }
        }

        return plugin;
    }

    @Override
    public void init() {
        LOG.info("==> RangerS3Plugin.init()");

        super.init();

        LOG.info("<== RangerS3Plugin.init(): S3 plugin initialized with {} chained plugins",
                 getChainedPlugins() != null ? getChainedPlugins().size() : 0);
    }

    @Override
    public void cleanup() {
        LOG.info("==> RangerS3Plugin.cleanup()");

        super.cleanup();

        instance = null;

        LOG.info("<== RangerS3Plugin.cleanup()");
    }
}
