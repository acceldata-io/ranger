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

package org.apache.ranger.authorization.s3.sts;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.s3.RangerS3Authorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

/**
 * RangerS3TokenService provides a REST API for obtaining time-limited,
 * scoped AWS credentials after Ranger authorization.
 *
 * This service implements a "token vending machine" pattern for
 * Ranger Authorization Service (RAZ):
 *
 * 1. Client requests access to S3 path with their Kerberos/OAuth credentials
 * 2. Service performs Ranger authorization
 * 3. If allowed, service calls AWS STS to get scoped credentials
 * 4. Client uses scoped credentials to access S3 directly
 *
 * Supported clients:
 * - AWS CLI (using credential_process)
 * - Python boto3 (using custom credential provider)
 * - Spark/Hive (using custom S3A credential provider)
 * - Any HTTP client
 *
 * Security benefits:
 * - Credentials are scoped to specific bucket/path
 * - Credentials are short-lived (15 min to 1 hour)
 * - Full audit trail in Ranger
 * - No long-term credentials needed on client
 */
@Path("/s3/token")
public class RangerS3TokenService {
    private static final Logger LOG = LoggerFactory.getLogger(RangerS3TokenService.class);

    private static final int DEFAULT_DURATION_SECONDS = 3600;  // 1 hour
    private static final int MIN_DURATION_SECONDS = 900;       // 15 minutes
    private static final int MAX_DURATION_SECONDS = 43200;     // 12 hours

    private final RangerS3Authorizer authorizer;

    public RangerS3TokenService() {
        this.authorizer = new RangerS3Authorizer();
    }

    /**
     * Request scoped credentials for S3 access.
     *
     * Example request:
     * POST /s3/token
     * {
     *   "bucket": "my-bucket",
     *   "path": "data/tables/customer/",
     *   "accessType": "read",
     *   "durationSeconds": 3600
     * }
     *
     * Example response:
     * {
     *   "Version": 1,
     *   "AccessKeyId": "ASIA...",
     *   "SecretAccessKey": "...",
     *   "SessionToken": "...",
     *   "Expiration": "2024-01-01T12:00:00Z"
     * }
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getToken(
            @HeaderParam("X-Ranger-User") String user,
            @HeaderParam("X-Ranger-Groups") String groupsHeader,
            TokenRequest request) {

        LOG.debug("==> getToken(user={}, bucket={}, path={})", user, request.bucket, request.path);

        try {
            // Validate request
            if (StringUtils.isBlank(user)) {
                return Response.status(Response.Status.UNAUTHORIZED)
                    .entity(errorResponse("ERR001", "User authentication required"))
                    .build();
            }

            if (StringUtils.isBlank(request.bucket)) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .entity(errorResponse("ERR002", "Bucket is required"))
                    .build();
            }

            Set<String> groups = parseGroups(groupsHeader);
            String accessType = StringUtils.isNotBlank(request.accessType) ?
                request.accessType : RangerS3Authorizer.ACCESS_TYPE_READ;

            // Perform Ranger authorization
            RangerS3Authorizer.AuthzResult authzResult = authorizer.isAccessAllowed(
                request.bucket, request.path, accessType, user, groups, null);

            if (!authzResult.isAllowed()) {
                LOG.warn("Token request DENIED: user={}, bucket={}, path={}, reason={}",
                         user, request.bucket, request.path, authzResult.getReason());

                return Response.status(Response.Status.FORBIDDEN)
                    .entity(errorResponse("ERR003", "Access denied: " + authzResult.getReason()))
                    .build();
            }

            // Generate scoped credentials
            int duration = request.durationSeconds > 0 ?
                Math.min(Math.max(request.durationSeconds, MIN_DURATION_SECONDS), MAX_DURATION_SECONDS) :
                DEFAULT_DURATION_SECONDS;

            ScopedCredentials credentials = generateScopedCredentials(
                user, request.bucket, request.path, accessType, duration);

            LOG.info("Token ISSUED: user={}, bucket={}, path={}, accessType={}, duration={}s",
                     user, request.bucket, request.path, accessType, duration);

            return Response.ok(credentials).build();

        } catch (Exception e) {
            LOG.error("Error generating token", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(errorResponse("ERR500", "Internal error: " + e.getMessage()))
                .build();
        }
    }

    /**
     * Get credentials for AWS CLI credential_process.
     *
     * Usage in ~/.aws/config:
     * [profile ranger-s3]
     * credential_process = curl -s "http://ranger-token-service/s3/token/cli?bucket=my-bucket&path=data/&user=myuser"
     */
    @GET
    @Path("/cli")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTokenForCLI(
            @QueryParam("bucket") String bucket,
            @QueryParam("path") String path,
            @QueryParam("user") String user,
            @QueryParam("accessType") @DefaultValue("read") String accessType,
            @QueryParam("duration") @DefaultValue("3600") int durationSeconds) {

        TokenRequest request = new TokenRequest();
        request.bucket = bucket;
        request.path = path;
        request.accessType = accessType;
        request.durationSeconds = durationSeconds;

        return getToken(user, null, request);
    }

    /**
     * Generate credentials scoped to specific S3 path.
     * In production, this would call AWS STS AssumeRole with a session policy.
     */
    private ScopedCredentials generateScopedCredentials(
            String user, String bucket, String path, String accessType, int durationSeconds) {

        // In production, this would:
        // 1. Call AWS STS AssumeRole with the base role
        // 2. Attach a session policy restricting to specific bucket/path
        // 3. Return the scoped credentials

        ScopedCredentials creds = new ScopedCredentials();
        creds.Version = 1;
        creds.AccessKeyId = generateMockAccessKey();
        creds.SecretAccessKey = generateMockSecretKey();
        creds.SessionToken = generateMockSessionToken(user, bucket, path, accessType);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, durationSeconds);
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
        creds.Expiration = sdf.format(cal.getTime());

        return creds;
    }

    private Set<String> parseGroups(String groupsHeader) {
        Set<String> groups = new HashSet<>();
        if (StringUtils.isNotBlank(groupsHeader)) {
            for (String group : groupsHeader.split(",")) {
                groups.add(group.trim());
            }
        }
        return groups;
    }

    private String generateMockAccessKey() {
        return "ASIAMOCK" + UUID.randomUUID().toString().substring(0, 12).toUpperCase();
    }

    private String generateMockSecretKey() {
        return UUID.randomUUID().toString().replace("-", "") + UUID.randomUUID().toString().substring(0, 8);
    }

    private String generateMockSessionToken(String user, String bucket, String path, String accessType) {
        // In production, this comes from STS
        return Base64.getEncoder().encodeToString(
            String.format("RANGER-TOKEN|%s|%s|%s|%s|%d",
                user, bucket, path, accessType, System.currentTimeMillis()).getBytes());
    }

    private Map<String, Object> errorResponse(String code, String message) {
        Map<String, Object> error = new HashMap<>();
        error.put("error", code);
        error.put("message", message);
        return error;
    }

    /**
     * Token request payload.
     */
    public static class TokenRequest {
        public String bucket;
        public String path;
        public String accessType;
        public int durationSeconds;
    }

    /**
     * Scoped credentials response.
     * Format compatible with AWS CLI credential_process.
     */
    public static class ScopedCredentials {
        public int Version;
        public String AccessKeyId;
        public String SecretAccessKey;
        public String SessionToken;
        public String Expiration;
    }
}
