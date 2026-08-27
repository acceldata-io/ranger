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
package org.apache.ranger.services.abfs.client;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.FileSystemItem;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.apache.ranger.services.abfs.RangerABFSConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ABFSResourceMgr {

    private static final Logger LOG = LoggerFactory.getLogger(ABFSResourceMgr.class);

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) throws Exception {
        LOG.debug("==> ABFSResourceMgr.connectionTest ServiceName: {}", serviceName);
        Map<String, Object> ret;
        try {
            ret = ABFSClientConnectionMgr.connectionTest(serviceName, configs);
        } catch (HadoopException e) {
            LOG.error("<== ABFSResourceMgr.connectionTest Error: {}", e.getMessage(), e);
            throw e;
        }
        LOG.debug("<== ABFSResourceMgr.connectionTest Result: {}", ret);
        return ret;
    }

    /**
     * Autocomplete resource lookup for the Ranger Admin UI.
     *
     * <ul>
     *   <li>{@code storageaccount}: returns the configured storage account when it
     *       matches the user input (the data plane cannot enumerate accounts).</li>
     *   <li>{@code container}: lists file systems (containers) in the account.</li>
     *   <li>{@code relativepath}: lists paths within the selected container(s).</li>
     * </ul>
     */
    public static List<String> getABFSResources(String serviceName, Map<String, String> configs,
                                                ResourceLookupContext context, long timeLookupMs) throws Exception {
        List<String> resultList = new ArrayList<>();
        final String userInput  = context.getUserInput();
        final String resource   = context.getResourceName();
        final Map<String, List<String>> resourceMap = context.getResources();

        LOG.debug("==> ABFSResourceMgr.getABFSResources ServiceName: {}, resource: {}, userInput: {}",
                serviceName, resource, userInput);

        if (serviceName == null || userInput == null || "*".equals(userInput)) {
            return resultList;
        }

        final String prefix = userInput.replace("*", "");

        try {
            if (RangerABFSConstants.STORAGE_ACCOUNT_RESOURCE.equals(resource)) {
                String configuredAccount = configs.get(RangerABFSConstants.STORAGE_ACCOUNT);
                if (StringUtils.isNotBlank(configuredAccount)
                        && (prefix.isEmpty() || configuredAccount.startsWith(prefix))) {
                    resultList.add(configuredAccount);
                }
                return resultList;
            }

            final DataLakeServiceClient serviceClient = ABFSClientConnectionMgr.getDataLakeServiceClient(configs);

            if (RangerABFSConstants.CONTAINER.equals(resource)) {
                Callable<List<String>> callable = () -> {
                    List<String> names = new ArrayList<>();
                    for (FileSystemItem fs : serviceClient.listFileSystems()) {
                        if (names.size() >= RangerABFSConstants.MAX_AUTOCOMPLETE_RESULTS) {
                            break;
                        }
                        String name = fs.getName();
                        if (prefix.isEmpty() || name.startsWith(prefix)) {
                            names.add(name);
                        }
                    }
                    return names;
                };
                resultList = TimedEventUtil.timedTask(callable, timeLookupMs, TimeUnit.MILLISECONDS);

            } else if (RangerABFSConstants.RELATIVE_PATH.equals(resource)) {
                final List<String> selectedContainers = getSelectedValues(resourceMap, RangerABFSConstants.CONTAINER);
                if (selectedContainers.isEmpty() || selectedContainers.contains("*")) {
                    return resultList;
                }

                final String pathPrefix = normalizeLookupPath(prefix);
                Callable<List<String>> callable = () -> {
                    List<String> paths = new ArrayList<>();
                    for (String container : selectedContainers) {
                        if (paths.size() >= RangerABFSConstants.MAX_AUTOCOMPLETE_RESULTS) {
                            break;
                        }
                        DataLakeFileSystemClient fsClient = serviceClient.getFileSystemClient(container);
                        ListPathsOptions options = new ListPathsOptions()
                                .setMaxResults(RangerABFSConstants.ABFS_LIST_MAX_RESULTS);
                        if (StringUtils.isNotBlank(pathPrefix)) {
                            options.setPath(pathPrefix);
                        }
                        for (PathItem item : fsClient.listPaths(options, null)) {
                            if (paths.size() >= RangerABFSConstants.MAX_AUTOCOMPLETE_RESULTS) {
                                break;
                            }
                            paths.add("/" + item.getName());
                        }
                    }
                    return paths;
                };
                resultList = TimedEventUtil.timedTask(callable, timeLookupMs, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            LOG.error("ABFSResourceMgr.getABFSResources Unable to get ABFS resources.", e);
            throw e;
        }

        LOG.debug("<== ABFSResourceMgr.getABFSResources result count: {}", resultList.size());
        return resultList;
    }

    private static List<String> getSelectedValues(Map<String, List<String>> resourceMap, String key) {
        List<String> values = new ArrayList<>();
        if (resourceMap != null && resourceMap.get(key) != null) {
            values.addAll(resourceMap.get(key));
        }
        return values;
    }

    /**
     * ABFS list-paths uses a directory path without a leading slash. Strips the
     * leading slash from the user-typed prefix so lookups work for inputs like
     * {@code /finance}.
     */
    private static String normalizeLookupPath(String input) {
        if (StringUtils.isBlank(input)) {
            return "";
        }
        String normalized = input;
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        return normalized;
    }
}
