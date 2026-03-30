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

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.ListUsersRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.util.HashMap;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link S3ClientConnectionMgr} covering the ODP-6188 changes.
 *
 * getIamClient (ODP-6188) — tested via WireMock acting as a stand-in for a
 * non-AWS IAM endpoint (e.g. Ceph RGW):
 *      With {@code aws_s3=false} the client must send requests to the custom endpoint
 *          via {@code endpointOverride(URI.create(endpoint))}.
 *      Region defaults to {@code us-east-1} when not configured; an explicit region
 *          overrides the default. Both are verified via the AWS Signature V4
 *          {@code Authorization} header: {@code Credential=KEY/DATE/<region>/iam/aws4_request}.
 *
 * getS3client / connectionTest — tested via S3Mock confirming that
 * {@code endpointOverride} routes S3 traffic to a custom endpoint and that
 * {@code connectionTest} correctly reports success/failure.
 */
public class S3ClientConnectionMgrTest {

    // Force the AWS SDK to use a single HTTP implementation when multiple are on the classpath
    // (e.g. url-connection-client vs apache-client pulled in by s3mock or other test deps).
    static {
        System.setProperty("software.amazon.awssdk.http.service.impl",
                "software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService");
    }

    // ── WireMock (IAM endpoint stub) ──────────────────────────────────────────

    @RegisterExtension
    static final WireMockExtension WIRE_MOCK = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    // ── S3Mock (S3 endpoint stub) ─────────────────────────────────────────────

    private static final String BUCKET       = "test-bucket";
    private static final String SERVICE_NAME = "testS3Service";
    private static final int PORT = 9090;

    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().withHttpPort(PORT).silent().build();

    @BeforeEach
    void createBucket(final S3Client s3MockClient) {
        s3MockClient.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }
    // ── helpers ───────────────────────────────────────────────────────────────

    private Map<String, String> iamConfigs(String region) {
        Map<String, String> configs = new HashMap<>();
        configs.put("accesskey", "test-access-key");
        configs.put("secretkey", "test-secret-key");
        configs.put("endpoint",  WIRE_MOCK.baseUrl());
        configs.put("aws_s3",    "false");
        if (region != null) {
            configs.put("region", region);
        }
        return configs;
    }

    private Map<String, String> s3Configs(String bucket) {
        Map<String, String> configs = new HashMap<>();
        configs.put("accesskey",  "test-access-key");
        configs.put("secretkey",  "test-secret-key");
        configs.put("endpoint",   "http://localhost:" + PORT);
        configs.put("region",     "us-east-1");
        configs.put("bucketname", bucket);
        return configs;
    }

    private void makeIamCall(IamClient client) {
        try {
            client.listUsers(ListUsersRequest.builder().build());
        } catch (Exception ignored) {
            // WireMock returns an empty 200; SDK may fail to parse it — that is fine.
            // What matters is that the HTTP request was sent to the stub.
        }
    }

    // ── getIamClient: endpoint override ──────────────────────────────────────

    /**
     * When aws_s3=false, the IamClient must send requests to the configured custom endpoint,
     * not to the default AWS IAM endpoint (iam.amazonaws.com).
     * WireMock captures the outbound request — if endpointOverride is working,
     * WireMock receives exactly one request.
     */
    @Test
    public void getIamClient_awsS3False_requestsRoutedToCustomEndpoint() {
        WIRE_MOCK.stubFor(post(anyUrl()).willReturn(aResponse().withStatus(200)));

        IamClient client = S3ClientConnectionMgr.getIamClient(iamConfigs("us-east-1"));
        makeIamCall(client);

        WIRE_MOCK.verify(1, postRequestedFor(anyUrl()));
        client.close();
    }

    /**
     * When aws_s3=true, the IamClient must use the default AWS IAM endpoint (iam.amazonaws.com)
     * and must NOT use the custom endpoint even if one is present in the configs.
     * WireMock must receive zero requests, confirming endpointOverride was not applied.
     * The IAM call itself will fail (no real AWS connectivity in tests), but that is expected
     * and intentionally swallowed in makeIamCall().
     */
    @Test
    public void getIamClient_awsS3True_requestsNotRoutedToCustomEndpoint() {
        Map<String, String> configs = iamConfigs("us-east-1");
        configs.put("aws_s3", "true");

        IamClient client = S3ClientConnectionMgr.getIamClient(configs);
        makeIamCall(client);

        WIRE_MOCK.verify(0, postRequestedFor(anyUrl()));
        client.close();
    }

    // ── getIamClient: region defaulting ──────────────────────────────────────

    /**
     * When aws_s3=false and no region is configured, the IamClient must fall back to us-east-1.
     * The region is verified via the AWS Signature V4 Authorization header, which embeds it as:
     * Credential=KEY/DATE/{region}/iam/aws4_request
     */
    @Test
    public void getIamClient_awsS3False_regionAbsent_defaultsToUsEast1InRequestSigning() {
        WIRE_MOCK.stubFor(post(anyUrl()).willReturn(aResponse().withStatus(200)));

        IamClient client = S3ClientConnectionMgr.getIamClient(iamConfigs(null));
        makeIamCall(client);

        WIRE_MOCK.verify(postRequestedFor(anyUrl())
                .withHeader("Authorization", containing("/us-east-1/iam/")));
        client.close();
    }

    /**
     * When aws_s3=false and a region is explicitly configured, that region must be used
     * in request signing instead of the us-east-1 default.
     * Verified via the Authorization header, same as the test above.
     */
    @Test
    public void getIamClient_awsS3False_explicitRegion_usedInRequestSigning() {
        WIRE_MOCK.stubFor(post(anyUrl()).willReturn(aResponse().withStatus(200)));

        IamClient client = S3ClientConnectionMgr.getIamClient(iamConfigs("eu-west-1"));
        makeIamCall(client);

        WIRE_MOCK.verify(postRequestedFor(anyUrl())
                .withHeader("Authorization", containing("/eu-west-1/iam/")));
        client.close();
    }

    // ── getS3client ───────────────────────────────────────────────────────────

    /**
     * Verifies that getS3client() builds a working S3 client that can communicate
     * through a custom endpoint via endpointOverride.
     * A successful ListObjectsV2 call against S3Mock confirms end-to-end connectivity,
     * not just that the client object was constructed without error.
     */
    @Test
    public void getS3client_customEndpoint_canListObjectsViaMock() {
        try (S3Client s3ClientUnderTest = S3ClientConnectionMgr.getS3client(s3Configs(BUCKET))) {
            assertNotNull(s3ClientUnderTest);
            try {
                ListObjectsV2Response response = s3ClientUnderTest.listObjectsV2(
                        ListObjectsV2Request.builder().bucket(BUCKET).build());
                assertNotNull(response);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }
    }

    // ── connectionTest ────────────────────────────────────────────────────────

    /**
     * When the configured bucket exists and is accessible, connectionTest() must return
     * connectivityStatus=true with a success message containing the bucket name.
     * The bucket is pre-created in S3Mock via @BeforeEach.
     */
    @Test
    public void connectionTest_existingBucket_returnsSuccess() {
        Map<String, Object> result = S3ClientConnectionMgr.connectionTest(SERVICE_NAME, s3Configs(BUCKET));

        assertTrue((Boolean) result.get("connectivityStatus"));
        assertTrue(result.get("message").toString().contains("ConnectionTest Successful"));
        assertTrue(result.get("message").toString().contains(BUCKET));
    }

    /**
     * When the configured bucket does not exist, S3Mock returns NoSuchBucketException.
     * connectionTest() must catch it and return connectivityStatus=false with a descriptive
     * failure message, rather than propagating the exception to the caller.
     */
    @Test
    public void connectionTest_nonExistingBucket_returnsFailure() {
        Map<String, Object> result = S3ClientConnectionMgr.connectionTest(SERVICE_NAME, s3Configs("nonexistent-bucket"));

        assertFalse((Boolean) result.get("connectivityStatus"));
        assertTrue(result.get("message").toString().contains("does not exist"));
    }
}
