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
import com.azure.core.http.HttpClient;
import com.azure.core.http.jdk.httpclient.JdkHttpClientProvider;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.services.abfs.RangerABFSConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds Azure Data Lake Storage Gen2 (ABFS) clients used to read and update
 * directory/file ACLs.
 *
 * <p>The plugin authenticates with a Microsoft Entra ID (Azure AD) service
 * principal — the minimum set of configs required to open a connection and
 * update ACLs are: {@code storageAccount}, {@code tenantId}, {@code clientId}
 * and {@code clientSecret}. The service principal must be granted the
 * <em>Storage Blob Data Owner</em> role on the target storage account/container
 * so it can modify ACLs.</p>
 */
public class ABFSClientConnectionMgr extends BaseClient {

    private static final Logger LOG = LoggerFactory.getLogger(ABFSClientConnectionMgr.class);

    // Use the JDK's built-in HTTP client transport instead of the Azure SDK's
    // default reactor-netty transport, whose bundled netty conflicts with the
    // netty version pinned by the Ranger runtime. Created once and shared.
    private static final HttpClient HTTP_CLIENT = new JdkHttpClientProvider().createInstance();

    public ABFSClientConnectionMgr(String svcName, Map<String, String> connectionProperties) {
        super(svcName, connectionProperties);
    }

    /**
     * Builds a {@link DataLakeServiceClient} scoped to the configured storage
     * account, authorized with a service-principal token credential.
     */
    public static DataLakeServiceClient getDataLakeServiceClient(Map<String, String> configs) {
        String endpoint = getEndpoint(configs);
        TokenCredential credential = getTokenCredential(configs);

        LOG.debug("ABFSClientConnectionMgr: building DataLakeServiceClient for endpoint {}", endpoint);

        return new DataLakeServiceClientBuilder()
                .endpoint(endpoint)
                .httpClient(HTTP_CLIENT)
                .credential(credential)
                .buildClient();
    }

    /**
     * Returns a {@link DataLakeFileSystemClient} for a single container (file system)
     * within the configured storage account.
     */
    public static DataLakeFileSystemClient getFileSystemClient(Map<String, String> configs, String container) {
        validateRequiredConfig(container, RangerABFSConstants.CONTAINER, "ABFS file system client");
        return getDataLakeServiceClient(configs).getFileSystemClient(container);
    }

    /**
     * Validates connectivity by listing a single path under the default container.
     * Returns a {@code Map} in the standard Ranger {@code generateResponseDataMap} shape.
     */
    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        LOG.debug("==> ABFSClientConnectionMgr.connectionTest ServiceName: {}", serviceName);

        boolean connectivityStatus = false;
        Map<String, Object> responseData = new HashMap<>();
        String defaultContainer = configs.get(RangerABFSConstants.DEFAULT_CONTAINER);

        try {
            DataLakeServiceClient serviceClient = getDataLakeServiceClient(configs);

            if (StringUtils.isNotBlank(defaultContainer)) {
                DataLakeFileSystemClient fsClient = serviceClient.getFileSystemClient(defaultContainer);
                // Existence check triggers an authenticated round-trip to the account.
                boolean exists = fsClient.exists();
                if (exists) {
                    connectivityStatus = true;
                    String successMsg = "ConnectionTest Successful - container '" + defaultContainer + "' is accessible";
                    generateResponseDataMap(connectivityStatus, successMsg, successMsg, null, null, responseData);
                } else {
                    String failureMsg = "Container '" + defaultContainer + "' does not exist or is not accessible";
                    generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
                }
            } else {
                // No default container configured; just confirm we can enumerate file systems.
                serviceClient.listFileSystems().iterator().hasNext();
                connectivityStatus = true;
                String successMsg = "ConnectionTest Successful - storage account is accessible";
                generateResponseDataMap(connectivityStatus, successMsg, successMsg, null, null, responseData);
            }
        } catch (IllegalArgumentException e) {
            String failureMsg = "Configuration error: " + e.getMessage();
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== ABFSClientConnectionMgr.connectionTest Configuration error: {}", e.getMessage(), e);
        } catch (Exception e) {
            String failureMsg = "Unable to connect to ABFS using given parameters: " + e.getMessage();
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== ABFSClientConnectionMgr.connectionTest Error: {}", e.getMessage(), e);
        }

        LOG.debug("<== ABFSClientConnectionMgr.connectionTest Result: {}", responseData);
        return responseData;
    }

    /**
     * Builds a service-principal {@link TokenCredential} from the configured
     * {@code tenantId}, {@code clientId} and {@code clientSecret}.
     */
    private static TokenCredential getTokenCredential(Map<String, String> configs) {
        String tenantId     = configs.get(RangerABFSConstants.TENANT_ID);
        String clientId     = configs.get(RangerABFSConstants.CLIENT_ID);
        String clientSecret = configs.get(RangerABFSConstants.CLIENT_SECRET);

        validateRequiredConfig(tenantId, RangerABFSConstants.TENANT_ID, "ABFS client");
        validateRequiredConfig(clientId, RangerABFSConstants.CLIENT_ID, "ABFS client");
        validateRequiredConfig(clientSecret, RangerABFSConstants.CLIENT_SECRET, "ABFS client");

        return new ClientSecretCredentialBuilder()
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();
    }

    /**
     * Resolves the DFS endpoint URL for the storage account. An explicit
     * {@code endpoint} config wins; otherwise it is derived from
     * {@code storageAccount} as {@code https://<account>.dfs.core.windows.net}.
     */
    private static String getEndpoint(Map<String, String> configs) {
        String endpoint = configs.get(RangerABFSConstants.ENDPOINT);
        if (StringUtils.isNotBlank(endpoint)) {
            return endpoint;
        }

        String storageAccount = configs.get(RangerABFSConstants.STORAGE_ACCOUNT);
        validateRequiredConfig(storageAccount, RangerABFSConstants.STORAGE_ACCOUNT, "ABFS client");

        return "https://" + storageAccount + RangerABFSConstants.DFS_ENDPOINT_SUFFIX;
    }

    private static void validateRequiredConfig(String value, String configName, String context) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(
                    "Required configuration '" + configName + "' is missing or empty for " + context);
        }
    }
}
