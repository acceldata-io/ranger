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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.services.gcs.RangerGCSConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GCSClientConnectionMgr extends BaseClient {

    private static final Logger LOG = LoggerFactory.getLogger(GCSClientConnectionMgr.class);

    public GCSClientConnectionMgr(String svcName, Map<String, String> connectionProperties) {
        super(svcName, connectionProperties);
    }

    /**
     * Builds a GCS {@link Storage} client using explicit service-account credentials when
     * {@code credential_file} is configured, falling back to Application Default Credentials
     * (ADC) otherwise.
     */
    public static Storage getStorageClient(Map<String, String> configs) throws IOException {
        String projectId      = configs.get(RangerGCSConstants.PROJECT_ID);
        String credentialFile = configs.get(RangerGCSConstants.CREDENTIAL_FILE);

        validateRequiredConfig(projectId, RangerGCSConstants.PROJECT_ID, "GCS client");

        GoogleCredentials credentials;
        if (StringUtils.isNotBlank(credentialFile)) {
            LOG.debug("GCSClientConnectionMgr: loading service-account credentials from file: {}", credentialFile);
            try (FileInputStream fis = new FileInputStream(credentialFile)) {
                credentials = ServiceAccountCredentials.fromStream(fis)
                        .createScoped("https://www.googleapis.com/auth/cloud-platform");
            }
        } else {
            LOG.debug("GCSClientConnectionMgr: credential_file not set, using Application Default Credentials");
            credentials = GoogleCredentials.getApplicationDefault()
                    .createScoped("https://www.googleapis.com/auth/cloud-platform");
        }

        return StorageOptions.newBuilder()
                .setProjectId(projectId)
                .setCredentials(credentials)
                .build()
                .getService();
    }

    /**
     * Validates connectivity by fetching the configured bucket's metadata. Returns a
     * {@code Map<String, Object>} in the standard Ranger {@code generateResponseDataMap} shape.
     */
    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        LOG.debug("==> GCSClientConnectionMgr.connectionTest ServiceName: {}, Configs: {}", serviceName, configs);

        boolean connectivityStatus = false;
        Map<String, Object> responseData = new HashMap<>();
        String bucketName = configs.get(RangerGCSConstants.BUCKET_NAME);

        Storage storage;
        try {
            storage = getStorageClient(configs);
        } catch (IllegalArgumentException e) {
            String failureMsg = "Configuration error: " + e.getMessage();
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== GCSClientConnectionMgr.connectionTest Configuration error: {}", e.getMessage(), e);
            return responseData;
        } catch (IOException e) {
            String failureMsg = "Failed to load GCS credentials: " + e.getMessage();
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== GCSClientConnectionMgr.connectionTest Credential error: {}", e.getMessage(), e);
            return responseData;
        }

        validateRequiredConfig(bucketName, RangerGCSConstants.BUCKET_NAME, "GCS connection test");

        try {
            Bucket bucket = storage.get(bucketName);
            if (bucket != null) {
                connectivityStatus = true;
                String successMsg = "ConnectionTest Successful - Bucket '" + bucketName + "' is accessible";
                generateResponseDataMap(connectivityStatus, successMsg, successMsg, null, null, responseData);
            } else {
                String failureMsg = "Bucket '" + bucketName + "' does not exist or is not accessible";
                generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            }
        } catch (StorageException e) {
            String failureMsg = "Unable to connect to GCS using given parameters: " + e.getMessage();
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== GCSClientConnectionMgr.connectionTest Error: {}", e.getMessage(), e);
        } catch (Exception e) {
            String failureMsg = "Unexpected error during GCS connection test: " + e.getMessage();
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== GCSClientConnectionMgr.connectionTest Unexpected error: {}", e.getMessage(), e);
        }

        LOG.debug("<== GCSClientConnectionMgr.connectionTest Result: {}", responseData);
        return responseData;
    }

    private static void validateRequiredConfig(String value, String configName, String context) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Required configuration '" + configName + "' is missing or empty for " + context);
        }
    }
}
