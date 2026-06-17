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

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.FileSystemProperties;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.services.abfs.RangerABFSConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ABFSClientConnectionMgr extends BaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(ABFSClientConnectionMgr.class);

    public ABFSClientConnectionMgr(String svcName, Map<String, String> connectionProperties) {
        super(svcName, connectionProperties);
    }

    public static DataLakeServiceClient getDataLakeServiceClient(Map<String, String> configs) {
        String endpoint = getEndpoint(configs);

        return new DataLakeServiceClientBuilder()
                .endpoint(endpoint)
                .credential(getTokenCredential(configs))
                .buildClient();
    }

    public static DataLakeFileSystemClient getFileSystemClient(Map<String, String> configs, String fileSystemName) {
        validateRequiredConfig(fileSystemName, RangerABFSConstants.CONTAINER, "ABFS file-system client");

        return getDataLakeServiceClient(configs).getFileSystemClient(fileSystemName);
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        LOG.debug("==> ABFSClientConnectionMgr.connectionTest ServiceName: {}, Configs: {}", serviceName, configs);

        boolean connectivityStatus = false;
        Map<String, Object> responseData = new HashMap<>();
        String container = configs.get(RangerABFSConstants.DEFAULT_CONTAINER);

        if (StringUtils.isBlank(container)) {
            String failureMsg = "Configuration error: Required configuration '" + RangerABFSConstants.DEFAULT_CONTAINER
                    + "' is missing or empty for ABFS connection test";
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== ABFSClientConnectionMgr.connectionTest Configuration error: {}", failureMsg);
            return responseData;
        }

        try {
            DataLakeFileSystemClient fileSystemClient = getFileSystemClient(configs, container);
            FileSystemProperties properties = fileSystemClient.getProperties();

            connectivityStatus = true;
            String successMsg = "ConnectionTest Successful - ADLS Gen2 file system '" + container
                    + "' is accessible; eTag=" + properties.getETag();
            generateResponseDataMap(connectivityStatus, successMsg, successMsg, null, null, responseData);
        } catch (IllegalArgumentException e) {
            String failureMsg = "Configuration error: " + e.getMessage();
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== ABFSClientConnectionMgr.connectionTest Configuration error: {}", e.getMessage(), e);
        } catch (Exception e) {
            String failureMsg = "Unable to connect to ADLS Gen2 using given parameters: " + e.getMessage();
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== ABFSClientConnectionMgr.connectionTest Error: {}", e.getMessage(), e);
        }

        LOG.debug("<== ABFSClientConnectionMgr.connectionTest Result: {}", responseData);
        return responseData;
    }

    private static TokenCredential getTokenCredential(Map<String, String> configs) {
        String authType = StringUtils.defaultIfBlank(configs.get(RangerABFSConstants.AUTH_TYPE),
                RangerABFSConstants.AUTH_TYPE_SERVICE_PRINCIPAL);

        if (RangerABFSConstants.AUTH_TYPE_MANAGED_IDENTITY.equalsIgnoreCase(authType)) {
            ManagedIdentityCredentialBuilder builder = new ManagedIdentityCredentialBuilder();
            String managedIdentityClientId = configs.get(RangerABFSConstants.MANAGED_IDENTITY_CLIENT_ID);
            if (StringUtils.isNotBlank(managedIdentityClientId)) {
                builder.clientId(managedIdentityClientId);
            }
            return builder.build();
        }

        String tenantId = configs.get(RangerABFSConstants.TENANT_ID);
        String clientId = configs.get(RangerABFSConstants.CLIENT_ID);
        String clientSecret = configs.get(RangerABFSConstants.CLIENT_SECRET);

        validateRequiredConfig(tenantId, RangerABFSConstants.TENANT_ID, "ABFS service-principal credential");
        validateRequiredConfig(clientId, RangerABFSConstants.CLIENT_ID, "ABFS service-principal credential");
        validateRequiredConfig(clientSecret, RangerABFSConstants.CLIENT_SECRET, "ABFS service-principal credential");

        return new ClientSecretCredentialBuilder()
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();
    }

    private static String getEndpoint(Map<String, String> configs) {
        String endpoint = configs.get(RangerABFSConstants.ENDPOINT);
        if (StringUtils.isNotBlank(endpoint)) {
            return endpoint;
        }

        String storageAccount = configs.get(RangerABFSConstants.STORAGE_ACCOUNT);
        validateRequiredConfig(storageAccount, RangerABFSConstants.STORAGE_ACCOUNT, "ABFS endpoint");

        return "https://" + storageAccount + ".dfs.core.windows.net";
    }

    private static void validateRequiredConfig(String value, String configName, String context) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Required configuration '" + configName + "' is missing or empty for " + context);
        }
    }
}
