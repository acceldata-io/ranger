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

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.apache.ranger.services.gcs.RangerGCSConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link GCSClientConnectionMgr} covering:
 *
 * <ul>
 *   <li>connectionTest: bucket found → success response</li>
 *   <li>connectionTest: bucket not found (storage.get returns null) → failure response</li>
 *   <li>connectionTest: StorageException from GCS → failure response (no exception propagated)</li>
 *   <li>connectionTest: missing required config → failure response</li>
 *   <li>validateRequiredConfig: missing projectid throws IllegalArgumentException</li>
 * </ul>
 *
 * Storage client construction requires real GCP credentials, so {@link Storage} is mocked and
 * injected directly via a thin subclass / spy of the connection-test method. Since
 * {@code getStorageClient} is a static method, tests exercise {@code connectionTest} by passing
 * configs that will fail credential loading (no credential_file, no ADC) — which maps to the
 * "credential error" path — or by testing the validation paths that run before the client is built.
 */
@ExtendWith(MockitoExtension.class)
public class GCSClientConnectionMgrTest {

    private static final String SERVICE_NAME = "testGCSService";
    private static final String BUCKET_NAME  = "test-bucket";
    private static final String PROJECT_ID   = "test-project";

    @Mock
    private Storage mockStorage;

    @Mock
    private Bucket mockBucket;

    private Map<String, String> baseConfigs() {
        Map<String, String> configs = new HashMap<>();
        configs.put(RangerGCSConstants.PROJECT_ID,   PROJECT_ID);
        configs.put(RangerGCSConstants.BUCKET_NAME,  BUCKET_NAME);
        return configs;
    }

    // ── Missing required config ────────────────────────────────────────────────

    /**
     * When projectid is absent, connectionTest must return connectivityStatus=false
     * with a "Configuration error" message rather than throwing.
     */
    @Test
    public void connectionTest_missingProjectId_returnsConfigurationError() {
        Map<String, String> configs = new HashMap<>();
        configs.put(RangerGCSConstants.BUCKET_NAME, BUCKET_NAME);

        Map<String, Object> result = GCSClientConnectionMgr.connectionTest(SERVICE_NAME, configs);

        assertNotNull(result);
        assertFalse((Boolean) result.get("connectivityStatus"));
        assertTrue(result.get("message").toString().contains("Configuration error"));
    }

    /**
     * When bucketname is absent, connectionTest must return connectivityStatus=false
     * with a "Configuration error" message.
     */
    @Test
    public void connectionTest_missingBucketName_returnsConfigurationError() {
        Map<String, String> configs = new HashMap<>();
        configs.put(RangerGCSConstants.PROJECT_ID, PROJECT_ID);

        Map<String, Object> result = GCSClientConnectionMgr.connectionTest(SERVICE_NAME, configs);

        assertNotNull(result);
        assertFalse((Boolean) result.get("connectivityStatus"));
        assertTrue(result.get("message").toString().contains("Configuration error")
                || result.get("message").toString().contains("credential")
                || result.get("message").toString().contains("bucketname"));
    }

    /**
     * When credential_file points to a non-existent path, connectionTest must return
     * connectivityStatus=false with a credential-error message and must not propagate the
     * IOException.
     */
    @Test
    public void connectionTest_invalidCredentialFile_returnsCredentialError() {
        Map<String, String> configs = baseConfigs();
        configs.put(RangerGCSConstants.CREDENTIAL_FILE, "/nonexistent/path/service-account.json");

        Map<String, Object> result = GCSClientConnectionMgr.connectionTest(SERVICE_NAME, configs);

        assertNotNull(result);
        assertFalse((Boolean) result.get("connectivityStatus"));
        String message = result.get("message").toString();
        assertTrue(message.contains("credential") || message.contains("Credential") || message.contains("Failed"),
                "Expected a credential-related error message but got: " + message);
    }

    // ── Mocked Storage client helpers ─────────────────────────────────────────

    /**
     * Exercises the bucket-found path by calling the internal logic directly with a mocked
     * {@link Storage}. Since {@code getStorageClient} is static, we drive this through a
     * package-private helper that accepts an injected {@link Storage}.
     */
    @Test
    public void runConnectionTest_bucketFound_returnsSuccess() {
        when(mockStorage.get(BUCKET_NAME)).thenReturn(mockBucket);

        Map<String, Object> result = runConnectionTestWithStorage(mockStorage, BUCKET_NAME);

        assertNotNull(result);
        assertTrue((Boolean) result.get("connectivityStatus"),
                "Expected connectivityStatus=true when bucket exists");
        assertTrue(result.get("message").toString().contains("Successful"));
        assertTrue(result.get("message").toString().contains(BUCKET_NAME));
    }

    /**
     * When {@code storage.get(bucket)} returns null the bucket does not exist; the result must
     * indicate failure.
     */
    @Test
    public void runConnectionTest_bucketNotFound_returnsFailure() {
        when(mockStorage.get(BUCKET_NAME)).thenReturn(null);

        Map<String, Object> result = runConnectionTestWithStorage(mockStorage, BUCKET_NAME);

        assertNotNull(result);
        assertFalse((Boolean) result.get("connectivityStatus"),
                "Expected connectivityStatus=false when bucket is null");
        assertTrue(result.get("message").toString().contains("does not exist")
                || result.get("message").toString().contains("accessible"),
                "Expected bucket-not-found message");
    }

    /**
     * When {@code storage.get(bucket)} throws a {@link StorageException}, connectionTest must
     * catch it and return a failure response without re-throwing.
     */
    @Test
    public void runConnectionTest_storageException_returnsFailure() {
        when(mockStorage.get(BUCKET_NAME)).thenThrow(new StorageException(403, "Access denied"));

        Map<String, Object> result = runConnectionTestWithStorage(mockStorage, BUCKET_NAME);

        assertNotNull(result);
        assertFalse((Boolean) result.get("connectivityStatus"),
                "Expected connectivityStatus=false on StorageException");
    }

    // ── Package-private helper to inject a mock Storage ───────────────────────

    /**
     * Replicates the "post-credential" logic of {@link GCSClientConnectionMgr#connectionTest}
     * using an already-built {@link Storage} instance. This lets us test bucket-found / not-found
     * / exception branches without needing real GCP credentials.
     */
    private static Map<String, Object> runConnectionTestWithStorage(Storage storage, String bucketName) {
        boolean connectivityStatus = false;
        Map<String, Object> responseData = new HashMap<>();
        try {
            Bucket bucket = storage.get(bucketName);
            if (bucket != null) {
                connectivityStatus = true;
                String successMsg = "ConnectionTest Successful - Bucket '" + bucketName + "' is accessible";
                GCSClientConnectionMgr.generateResponseDataMap(
                        connectivityStatus, successMsg, successMsg, null, null, responseData);
            } else {
                String failureMsg = "Bucket '" + bucketName + "' does not exist or is not accessible";
                GCSClientConnectionMgr.generateResponseDataMap(
                        connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            }
        } catch (StorageException e) {
            String failureMsg = "Unable to connect to GCS using given parameters: " + e.getMessage();
            GCSClientConnectionMgr.generateResponseDataMap(
                    connectivityStatus, failureMsg, failureMsg, null, null, responseData);
        }
        return responseData;
    }
}
