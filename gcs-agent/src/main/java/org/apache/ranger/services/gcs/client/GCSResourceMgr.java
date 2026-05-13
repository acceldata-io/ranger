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
package org.apache.ranger.services.gcs.client;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.apache.ranger.services.gcs.RangerGCSConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class GCSResourceMgr {

    private static final Logger LOG = LoggerFactory.getLogger(GCSResourceMgr.class);

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) throws Exception {
        LOG.debug("==> GCSResourceMgr.connectionTest ServiceName: {}", serviceName);
        Map<String, Object> ret;
        try {
            ret = GCSClientConnectionMgr.connectionTest(serviceName, configs);
        } catch (HadoopException e) {
            LOG.error("<== GCSResourceMgr.connectionTest Error: {}", e.getMessage(), e);
            throw e;
        }
        LOG.debug("<== GCSResourceMgr.connectionTest Result: {}", ret);
        return ret;
    }

    /**
     * Autocomplete resource lookup for the Ranger Admin UI.
     *
     * <ul>
     *   <li>When {@code resourceName} is {@code "bucket"}: lists GCS buckets in the configured
     *       project whose names start with {@code userInput}.</li>
     *   <li>When {@code resourceName} is {@code "object"}: lists blobs inside the bucket(s)
     *       already selected in the policy, using {@code userInput} as a key prefix.</li>
     * </ul>
     */
    public static List<String> getGCSResources(String serviceName, Map<String, String> configs,
                                               ResourceLookupContext context, long timeLookupMs) throws Exception {
        List<String> resultList = new ArrayList<>();
        final String userInput   = context.getUserInput();
        final String resource    = context.getResourceName();
        final Map<String, List<String>> resourceMap = context.getResources();

        LOG.debug("==> GCSResourceMgr.getGCSResources ServiceName: {}, resource: {}, userInput: {}",
                serviceName, resource, userInput);

        if (serviceName == null || userInput == null) {
            return resultList;
        }

        if ("*".equals(userInput)) {
            return resultList;
        }

        final List<String> selectedBuckets = new ArrayList<>();
        if (resourceMap != null && resourceMap.get(RangerGCSConstants.BUCKET) != null) {
            selectedBuckets.addAll(resourceMap.get(RangerGCSConstants.BUCKET));
        }

        try {
            final Storage storage = GCSClientConnectionMgr.getStorageClient(configs);

            if (RangerGCSConstants.BUCKET.equals(resource)) {
                final String bucketPrefix = userInput.replace("*", "");
                Callable<List<String>> callable = () -> {
                    List<String> names = new ArrayList<>();
                    Page<Bucket> buckets = storage.list();
                    for (Bucket bucket : buckets.iterateAll()) {
                        if (names.size() >= RangerGCSConstants.MAX_AUTOCOMPLETE_RESULTS) {
                            break;
                        }
                        String name = bucket.getName();
                        if (bucketPrefix.isEmpty() || name.startsWith(bucketPrefix)) {
                            names.add(name);
                        }
                    }
                    return names;
                };
                resultList = TimedEventUtil.timedTask(callable, timeLookupMs, TimeUnit.MILLISECONDS);

            } else if (RangerGCSConstants.OBJECT.equals(resource) && !selectedBuckets.isEmpty()
                       && !selectedBuckets.contains("*")) {
                final String objectPrefix = userInput.replace("*", "");
                Callable<List<String>> callable = () -> {
                    List<String> blobs = new ArrayList<>();
                    for (String bucketName : selectedBuckets) {
                        if (blobs.size() >= RangerGCSConstants.MAX_AUTOCOMPLETE_RESULTS) {
                            break;
                        }
                        Page<Blob> blobPage = objectPrefix.isEmpty()
                                ? storage.list(bucketName,
                                    Storage.BlobListOption.pageSize(RangerGCSConstants.GCS_LIST_MAX_RESULTS))
                                : storage.list(bucketName,
                                    Storage.BlobListOption.prefix(objectPrefix),
                                    Storage.BlobListOption.pageSize(RangerGCSConstants.GCS_LIST_MAX_RESULTS));
                        for (Blob blob : blobPage.iterateAll()) {
                            if (blobs.size() >= RangerGCSConstants.MAX_AUTOCOMPLETE_RESULTS) {
                                break;
                            }
                            blobs.add(blob.getName());
                        }
                    }
                    return blobs;
                };
                resultList = TimedEventUtil.timedTask(callable, timeLookupMs, TimeUnit.MILLISECONDS);
            }

        } catch (StorageException e) {
            LOG.error("GCSResourceMgr.getGCSResources StorageException: {}", e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("GCSResourceMgr.getGCSResources Unable to get GCS resources.", e);
            throw e;
        }

        LOG.debug("<== GCSResourceMgr.getGCSResources result count: {}", resultList.size());
        return resultList;
    }
}
