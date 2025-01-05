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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.services.s3.RangerS3Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3ClientConnectionMgr extends BaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(S3ClientConnectionMgr.class);

    public S3ClientConnectionMgr(String svcName, Map<String, String> connectionProperties) {
        super(svcName, connectionProperties);
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        LOG.debug("==> S3ClientConnectionMgr.connectionTest ServiceName: "+ serviceName + "Configs" + configs );
        boolean connectivityStatus = false;
        Map<String, Object> responseData = new HashMap<String, Object>();


        try {
            S3Client s3 = getS3client(configs);

            ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
            ListBucketsResponse listBucketsResponse = s3.listBuckets(listBucketsRequest);

            if (listBucketsResponse != null) {
                connectivityStatus = true;
                String successMsg = "ConnectionTest Successful";
                generateResponseDataMap(connectivityStatus, successMsg, successMsg,
                        null, null, responseData);
            } else {
                String failureMsg = "Unable to connect to S3 using given parameters.";
                generateResponseDataMap(connectivityStatus, failureMsg, failureMsg,
                        null, null, responseData);
            }
        } catch (S3Exception e) {
            String failureMsg = "Unable to connect to S3 using given parameters.";
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg,
                    null, null, responseData);
            LOG.error("<== S3ClientConnectionMgr.testConnection Error: " + e.getMessage(),  e);
        }

        LOG.debug("<== S3ClientConnectionMgr.connectionTest Result : "+ responseData  );
        return responseData;
    }

    public static S3Client getS3client(Map<String, String> configs) {
        String accessKey = configs.get(RangerS3Constants.USER_NAME);
        String secretKey = configs.get(RangerS3Constants.SECRET_KEY);
        String endPointOCE = configs.get(RangerS3Constants.ENDPOINT);
        String regionstr = configs.get(RangerS3Constants.REGION);
        AwsBasicCredentials awsCreds3 = AwsBasicCredentials.create(accessKey, secretKey);
        Region region = Region.of(regionstr);
        return S3Client.builder()
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds3))
                .endpointOverride(URI.create(endPointOCE))
                .build();
    }

    public static IamClient getIamClient(Map<String, String> configs) {
        String accessKey = configs.get(RangerS3Constants.USER_NAME);
        String secretKey = configs.get(RangerS3Constants.SECRET_KEY);
        AwsBasicCredentials awsCreds3 = AwsBasicCredentials.create(accessKey, secretKey);
        return IamClient.builder()
                .region(Region.AWS_GLOBAL)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds3))
                .build();
    }
}
