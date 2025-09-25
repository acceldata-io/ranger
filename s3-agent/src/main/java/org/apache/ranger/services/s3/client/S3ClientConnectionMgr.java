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
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;

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
        try {
            LOG.info("Creating S3Client with explicit trust store configuration");

            // Get trust store information from system properties
            String trustStorePath = System.getProperty("javax.net.ssl.trustStore");
            String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword", "changeit");
            String trustStoreType = System.getProperty("javax.net.ssl.trustStoreType", "JKS");

            LOG.info("Trust store path: {}", trustStorePath);
            LOG.info("Trust store type: {}", trustStoreType);

            // Initialize TrustManagerFactory with explicit trust store
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm()
            );

            KeyStore trustStore = null;
            if (trustStorePath != null) {
                // Load the trust store explicitly using system properties
                LOG.info("Loading trust store from: {}", trustStorePath);
                trustStore = KeyStore.getInstance(trustStoreType);
                try (FileInputStream fis = new FileInputStream(trustStorePath)) {
                    trustStore.load(fis, trustStorePassword.toCharArray());
                }
                LOG.info("Trust store loaded successfully, contains {} entries", trustStore.size());
            } else {
                // Fallback to default location if system property is not set
                String javaHome = System.getProperty("java.home");
                String defaultTrustStorePath = javaHome + "/lib/security/cacerts";
                LOG.info("No trust store system property set, using default: {}", defaultTrustStorePath);

                trustStore = KeyStore.getInstance("JKS");
                try (FileInputStream fis = new FileInputStream(defaultTrustStorePath)) {
                    trustStore.load(fis, "changeit".toCharArray());
                }
            }

            // Initialize TrustManagerFactory with the loaded trust store
            trustManagerFactory.init(trustStore);

            // Get trust managers and verify X509TrustManager exists
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();

            // Log available trust managers
            LOG.info("Found {} trust managers", trustManagers.length);
            for (int i = 0; i < trustManagers.length; i++) {
                TrustManager tm = trustManagers[i];
                LOG.debug("Trust manager {}: {}", i, tm.getClass().getName());
                if (tm instanceof X509TrustManager) {
                    X509TrustManager x509tm = (X509TrustManager) tm;
                    LOG.info("✓ X509TrustManager found: {} - Accepted issuers: {}",
                            x509tm.getClass().getSimpleName(), x509tm.getAcceptedIssuers().length);
                } else {
                    LOG.warn("✗ Trust manager {} is not an X509TrustManager", tm.getClass().getSimpleName());
                }
            }

            X509TrustManager x509TrustManager = findX509TrustManager(trustManagers);

            if (x509TrustManager == null) {
                LOG.error("No X509TrustManager found in system trust store");
                throw new RuntimeException("No X509TrustManager found in system trust store");
            }

            LOG.info("Successfully found X509TrustManager: {}", x509TrustManager.getClass().getName());


            // Configure HTTP client with explicit trust managers
            SdkHttpClient httpClient = ApacheHttpClient.builder()
                    .maxConnections(50)
                    .connectionTimeout(Duration.ofSeconds(30))
                    .socketTimeout(Duration.ofSeconds(60))
                    .tlsTrustManagersProvider(() -> trustManagerFactory.getTrustManagers())
                    .build();
            // Return S3Client with configured HTTP client
            return S3Client.builder()
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(awsCreds3))
                    .endpointOverride(URI.create(endPointOCE))
                    .httpClient(httpClient)  // Add this line!
                    .build();

        } catch (Exception e) {
            throw new RuntimeException("Failed to create S3Client", e);
        }
    }
    /**
     * Helper method to find X509TrustManager from trust managers array
     */
    private static X509TrustManager findX509TrustManager(TrustManager[] trustManagers) {
        for (TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509TrustManager) {
                return (X509TrustManager) trustManager;
            }
        }
        return null;
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
