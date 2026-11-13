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
package org.apache.ranger.authorization.pinot;

import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The class Pinot's {@code ranger} authorization mode creates. It sits on Pinot's class path with the Ranger plug-in class
 * loader and the {@link PinotAccessChecker} contract, and loads the real checker – with Ranger's own libraries (Hadoop
 * configuration, Jersey, Jackson, ...) – from the {@code ranger-pinot-plugin-impl} directory next to this jar through a
 * child-first class loader, so those libraries never mix with Pinot's. Every call runs with the plug-in class loader
 * as context class loader. Thread-safe.
 */
public class RangerPinotPlugin implements PinotAccessChecker {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPinotPlugin.class);

    private static final String RANGER_PLUGIN_TYPE = "pinot";
    private static final String IMPL_CLASS_NAME    = "org.apache.ranger.authorization.pinot.RangerPinotAccessChecker";

    private final RangerPluginClassLoader classLoader;
    private final PinotAccessChecker      impl;

    public RangerPinotPlugin() {
        try {
            classLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, getClass());

            Class<?> cls = Class.forName(IMPL_CLASS_NAME, true, classLoader);

            classLoader.activate();

            try {
                impl = (PinotAccessChecker) cls.getDeclaredConstructor().newInstance();
            } finally {
                classLoader.deactivate();
            }
        } catch (Exception e) {
            LOG.error("Cannot load the Ranger plug-in for Pinot", e);

            throw new IllegalStateException("Cannot load the Ranger plug-in for Pinot from ranger-pinot-plugin-impl", e);
        }
    }

    @Override
    public void init(Map<String, String> settings) {
        classLoader.activate();

        try {
            impl.init(settings);
        } finally {
            classLoader.deactivate();
        }
    }

    @Override
    public Map.Entry<Boolean, String> checkAccess(String user, Set<String> groups, String resourceType, String database, String table, String accessType, String action) {
        classLoader.activate();

        try {
            return impl.checkAccess(user, groups, resourceType, database, table, accessType, action);
        } finally {
            classLoader.deactivate();
        }
    }

    @Override
    public List<String> getRowFilters(String user, Set<String> groups, String database, String table) {
        classLoader.activate();

        try {
            return impl.getRowFilters(user, groups, database, table);
        } finally {
            classLoader.deactivate();
        }
    }

    @Override
    public void close() {
        classLoader.activate();

        try {
            impl.close();
        } finally {
            classLoader.deactivate();
        }
    }
}
