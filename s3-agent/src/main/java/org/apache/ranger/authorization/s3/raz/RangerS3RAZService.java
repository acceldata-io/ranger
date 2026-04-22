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

package org.apache.ranger.authorization.s3.raz;

import org.apache.ranger.authorization.s3.RangerS3Authorizer;
import org.apache.ranger.authorization.s3.RangerS3Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

/**
 * RangerS3RAZService provides a REST API for S3 authorization.
 * This provides a RAZ (Ranger Authorization Service) architecture for S3.
 *
 * Integration points:
 * 1. S3 Proxy/Gateway can call this service before forwarding requests to S3
 * 2. Custom S3 clients can use this for pre-flight authorization checks
 * 3. Can be deployed as a sidecar service for S3-compatible storage
 *
 * Configuration:
 * - ranger.plugin.s3.service.name: S3 service name in Ranger
 * - ranger.plugin.s3.chained.services: Chained services (e.g., {hive_service_name})
 * - ranger.plugin.s3.chained.services.{hive_service_name}.impl: Hive chained plugin class
 */
@Path("/s3/authz")
public class RangerS3RAZService {
    private static final Logger LOG = LoggerFactory.getLogger(RangerS3RAZService.class);

    private final RangerS3Authorizer authorizer;

    public RangerS3RAZService() {
        String serviceName = System.getProperty("ranger.plugin.s3.service.name", "s3");
        this.authorizer = new RangerS3Authorizer(serviceName);
        LOG.info("RangerS3RAZService initialized with service: {}", serviceName);
    }

    public RangerS3RAZService(String serviceName) {
        this.authorizer = new RangerS3Authorizer(serviceName);
    }

    /**
     * Authorize an S3 request.
     *
     * Example request:
     * POST /s3/authz/check
     * {
     *   "bucket": "my-bucket",
     *   "path": "data/file.parquet",
     *   "operation": "GetObject",
     *   "user": "alice",
     *   "groups": ["data-team", "analysts"]
     * }
     */
    @POST
    @Path("/check")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkAccess(S3AuthzRequest request, @Context HttpServletRequest httpRequest) {
        LOG.debug("==> RangerS3RAZService.checkAccess({})", request);

        S3AuthzResponse response = new S3AuthzResponse();

        try {
            if (request == null) {
                response.setAllowed(false);
                response.setReason("Request cannot be null");
                return Response.status(Response.Status.BAD_REQUEST).entity(response).build();
            }

            String accessType = RangerS3Authorizer.mapS3OperationToAccessType(request.getOperation());
            String clientIP = httpRequest != null ? httpRequest.getRemoteAddr() : request.getClientIP();

            Set<String> groups = request.getGroups() != null ?
                new HashSet<>(request.getGroups()) : Collections.emptySet();

            RangerS3Authorizer.AuthzResult result = authorizer.isAccessAllowed(
                request.getBucket(),
                request.getPath(),
                accessType,
                request.getUser(),
                groups,
                clientIP
            );

            response.setAllowed(result.isAllowed());
            response.setAudited(result.isAudited());
            response.setPolicyId(result.getPolicyId());
            response.setReason(result.getReason());
            response.setRequestId(UUID.randomUUID().toString());

            LOG.debug("<== RangerS3RAZService.checkAccess(): {}", response);

            return Response.ok(response).build();

        } catch (Exception e) {
            LOG.error("Error processing authorization request", e);
            response.setAllowed(false);
            response.setReason("Internal error: " + e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(response).build();
        }
    }

    /**
     * Batch authorization check for multiple S3 requests.
     */
    @POST
    @Path("/check/batch")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkAccessBatch(List<S3AuthzRequest> requests, @Context HttpServletRequest httpRequest) {
        LOG.debug("==> RangerS3RAZService.checkAccessBatch(count={})", requests != null ? requests.size() : 0);

        List<S3AuthzResponse> responses = new ArrayList<>();

        if (requests == null || requests.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(Collections.singletonMap("error", "Requests list cannot be empty"))
                .build();
        }

        for (S3AuthzRequest request : requests) {
            S3AuthzResponse response = new S3AuthzResponse();

            try {
                String accessType = RangerS3Authorizer.mapS3OperationToAccessType(request.getOperation());
                String clientIP = httpRequest != null ? httpRequest.getRemoteAddr() : request.getClientIP();

                Set<String> groups = request.getGroups() != null ?
                    new HashSet<>(request.getGroups()) : Collections.emptySet();

                RangerS3Authorizer.AuthzResult result = authorizer.isAccessAllowed(
                    request.getBucket(),
                    request.getPath(),
                    accessType,
                    request.getUser(),
                    groups,
                    clientIP
                );

                response.setAllowed(result.isAllowed());
                response.setAudited(result.isAudited());
                response.setPolicyId(result.getPolicyId());
                response.setReason(result.getReason());

            } catch (Exception e) {
                response.setAllowed(false);
                response.setReason("Error: " + e.getMessage());
            }

            response.setRequestId(request.getRequestId());
            responses.add(response);
        }

        LOG.debug("<== RangerS3RAZService.checkAccessBatch(): {} results", responses.size());
        return Response.ok(responses).build();
    }

    /**
     * Health check endpoint.
     */
    @GET
    @Path("/health")
    @Produces(MediaType.APPLICATION_JSON)
    public Response health() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("service", "RangerS3RAZService");

        RangerS3Plugin plugin = authorizer.getPlugin();
        if (plugin != null) {
            health.put("serviceName", plugin.getServiceName());
            health.put("serviceType", plugin.getServiceType());
            health.put("chainedPlugins", plugin.getChainedPlugins() != null ?
                plugin.getChainedPlugins().size() : 0);
        }

        return Response.ok(health).build();
    }

    /**
     * S3 authorization request.
     */
    public static class S3AuthzRequest {
        private String requestId;
        private String bucket;
        private String path;
        private String operation;
        private String user;
        private List<String> groups;
        private List<String> roles;
        private String clientIP;

        public String getRequestId() { return requestId; }
        public void setRequestId(String requestId) { this.requestId = requestId; }

        public String getBucket() { return bucket; }
        public void setBucket(String bucket) { this.bucket = bucket; }

        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }

        public String getOperation() { return operation; }
        public void setOperation(String operation) { this.operation = operation; }

        public String getUser() { return user; }
        public void setUser(String user) { this.user = user; }

        public List<String> getGroups() { return groups; }
        public void setGroups(List<String> groups) { this.groups = groups; }

        public List<String> getRoles() { return roles; }
        public void setRoles(List<String> roles) { this.roles = roles; }

        public String getClientIP() { return clientIP; }
        public void setClientIP(String clientIP) { this.clientIP = clientIP; }

        @Override
        public String toString() {
            return "S3AuthzRequest{bucket='" + bucket + "', path='" + path +
                   "', operation='" + operation + "', user='" + user + "'}";
        }
    }

    /**
     * S3 authorization response.
     */
    public static class S3AuthzResponse {
        private String requestId;
        private boolean allowed;
        private boolean audited;
        private Long policyId;
        private String reason;

        public String getRequestId() { return requestId; }
        public void setRequestId(String requestId) { this.requestId = requestId; }

        public boolean isAllowed() { return allowed; }
        public void setAllowed(boolean allowed) { this.allowed = allowed; }

        public boolean isAudited() { return audited; }
        public void setAudited(boolean audited) { this.audited = audited; }

        public Long getPolicyId() { return policyId; }
        public void setPolicyId(Long policyId) { this.policyId = policyId; }

        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }

        @Override
        public String toString() {
            return "S3AuthzResponse{allowed=" + allowed + ", policyId=" + policyId +
                   ", reason='" + reason + "'}";
        }
    }
}
