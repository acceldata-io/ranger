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
package org.apache.ranger.services.s3.client;

import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.apache.ranger.services.s3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class S3ResourceMgr {
    private static final Logger LOG = LoggerFactory.getLogger(S3ResourceMgr.class);
    public static final String PATH	= "path";

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) throws Exception {
        Map<String, Object> ret;
        LOG.debug("==> S3ResourceMgr.connectionTest ServiceName: "+ serviceName + "Configs" + configs );

        try {
            ret = S3ClientConnectionMgr.connectionTest(serviceName, configs);
        } catch (HadoopException e) {
            LOG.error("<== S3ResourceMgr.testConnection Error: " + e.getMessage(),  e);
            throw e;
        }

        LOG.debug("<== S3ResourceMgr.connectionTest Result : "+ ret  );
        return ret;
    }

    public static List<String> getS3Resources(String serviceName, Map<String, String> configs, ResourceLookupContext context) throws Exception {

        List<String> resultList = new ArrayList<>();
        String userInput = context.getUserInput();
        String resource = context.getResourceName();
        Map<String, List<String>> resourceMap = context.getResources();
        String bucketName = configs.get(RangerS3Constants.BUCKET_NAME);
        final List<String> pathList = new ArrayList<>();

        LOG.info("==> S3ResourceMgr.connectionTest ServiceName:{}", serviceName);

        if (resourceMap != null && resource != null && resourceMap.get(PATH) !=null) {
            for (String path: resourceMap.get(PATH)) {
                pathList.add(path);
            }
        }

        if (serviceName != null && userInput != null) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== S3ResourceMgr.getS3Resources()  UserInput: \"" + userInput + "\" resource : " + resource +
                              " resourceMap: " + resourceMap);
                }
                S3Client s3 = S3ClientConnectionMgr.getS3client(configs);

                if (s3 != null) {
                    final Callable<List<String>> callableObj = new Callable<List<String>>() {

                        @Override
                        public List<String> call() throws Exception {
                            ListObjectsV2Request.Builder listObjectsRequestBuilder = ListObjectsV2Request.builder()
                                    .bucket(bucketName);

                            if (!userInput.isEmpty()) {
                                listObjectsRequestBuilder.prefix(userInput.replace("*", ""));  // Replace '*' to use only the prefix part
                            }
                            List<String> resultListInner = new ArrayList<>();
                            ListObjectsV2Request listObjectsRequest = listObjectsRequestBuilder.build();
                            ListObjectsV2Response listObjectsResponse;
                            do {
                                listObjectsResponse = s3.listObjectsV2(listObjectsRequest);

                                listObjectsResponse.contents().forEach(s3Object -> {
                                    String key = s3Object.key();
                                    for (String path : pathList) {
                                        String prefixPath = userInput.replace("*", "");
                                        if (key.startsWith(prefixPath) && !path.equals(prefixPath)) {
                                            resultListInner.add(key);
                                            break;
                                        }
                                    }
                                });

                                // If the response is truncated, set the next continuation token for the next request
                                String nextContinuationToken = listObjectsResponse.nextContinuationToken();
                                if (nextContinuationToken != null) {
                                    listObjectsRequest = listObjectsRequest.toBuilder().continuationToken(nextContinuationToken).build();
                                }
                            } while (listObjectsResponse.isTruncated());
                            return resultListInner;
                        }
                    };
                    if (callableObj != null) {
                        synchronized (s3) {
                            resultList = TimedEventUtil.timedTask(callableObj, 5,
                                    TimeUnit.SECONDS);
                        }
                    }
                }

            } catch (S3Exception e) {
                LOG.error("Failed to list objects for bucket: {}", e.getMessage());
            }
            catch (Exception e) {
                LOG.error("Unable to get S3 resources.", e);
                throw e;
            }
        }
            LOG.info("<== S3ResourceMgr.getS3Resources()");
            return resultList;
        }

}
