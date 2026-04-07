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

package org.apache.ranger.authorization.s3;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerChainedPlugin;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Set;

/**
 * RangerS3Authorizer is the main authorization interface for S3 requests.
 * It can be integrated with:
 * 1. RAZ (Ranger Authorization Service) for AWS
 * 2. S3 proxy/gateway implementations
 * 3. Custom S3 authorization interceptors
 *
 * This authorizer supports:
 * - Direct S3 policy evaluation
 * - Chained Hive policy evaluation via RMS (Hive-S3 ACL sync)
 * - Masking/Row-filter policy enforcement
 * - Tag-based policies via Atlas integration
 */
public class RangerS3Authorizer {
    private static final Logger LOG = LoggerFactory.getLogger(RangerS3Authorizer.class);
    private static final Logger PERF_LOG = RangerPerfTracer.getPerfLogger("s3.authorizer");

    public static final String RESOURCE_BUCKET = "bucket";
    public static final String RESOURCE_PATH = "path";

    public static final String ACCESS_TYPE_READ = "read";
    public static final String ACCESS_TYPE_WRITE = "write";
    public static final String ACCESS_TYPE_DELETE = "delete";
    public static final String ACCESS_TYPE_LIST = "list";
    public static final String ACCESS_TYPE_GET_ACL = "get_acl";
    public static final String ACCESS_TYPE_PUT_ACL = "put_acl";

    private final RangerS3Plugin plugin;
    private final RangerS3AuditHandler auditHandler;

    public RangerS3Authorizer() {
        this(RangerS3Plugin.getInstance());
    }

    public RangerS3Authorizer(String serviceName) {
        this(RangerS3Plugin.getInstance(serviceName));
    }

    public RangerS3Authorizer(RangerS3Plugin plugin) {
        this.plugin = plugin;
        this.auditHandler = new RangerS3AuditHandler();
    }

    /**
     * Check if access is allowed for an S3 request.
     *
     * @param bucket     S3 bucket name
     * @param path       Object path within the bucket
     * @param accessType Type of access (read, write, delete, list, etc.)
     * @param user       User making the request
     * @param groups     Groups the user belongs to
     * @param clientIP   Client IP address
     * @return AuthzResult containing the authorization decision
     */
    public AuthzResult isAccessAllowed(String bucket, String path, String accessType,
                                       String user, Set<String> groups, String clientIP) {
        LOG.debug("==> RangerS3Authorizer.isAccessAllowed(bucket={}, path={}, accessType={}, user={})",
                  bucket, path, accessType, user);

        RangerPerfTracer perf = null;
        if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_LOG,
                "RangerS3Authorizer.isAccessAllowed(bucket=" + bucket + ", path=" + path + ")");
        }

        AuthzResult result = new AuthzResult();

        try {
            RangerAccessRequestImpl request = createAccessRequest(bucket, path, accessType, user, groups, clientIP);
            RangerAccessResult rangerResult = plugin.isAccessAllowed(request);

            if (rangerResult != null) {
                result.setAllowed(rangerResult.getIsAllowed());
                result.setAudited(rangerResult.getIsAudited());
                result.setPolicyId(rangerResult.getPolicyId());
                result.setReason(rangerResult.getReason());

                if (rangerResult.getIsAllowed()) {
                    result = checkChainedPlugins(request, result);
                }

                auditHandler.processResult(rangerResult);
            } else {
                result.setAllowed(false);
                result.setReason("No Ranger result - access denied by default");
            }

        } catch (Exception e) {
            LOG.error("Error during S3 authorization", e);
            result.setAllowed(false);
            result.setReason("Authorization error: " + e.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== RangerS3Authorizer.isAccessAllowed(): allowed={}", result.isAllowed());
        return result;
    }

    /**
     * Check chained plugins (e.g., Hive policy enforcer for RMS).
     */
    private AuthzResult checkChainedPlugins(RangerAccessRequest request, AuthzResult result) {
        if (plugin.getChainedPlugins() == null || plugin.getChainedPlugins().isEmpty()) {
            return result;
        }

        for (RangerChainedPlugin chainedPlugin : plugin.getChainedPlugins()) {
            if (chainedPlugin == null) {
                continue;
            }

            RangerAccessResult maskResult = chainedPlugin.evalDataMaskPolicies(request);
            if (maskResult != null && maskResult.isMaskEnabled()) {
                LOG.info("Access denied by chained plugin due to masking policy");
                result.setAllowed(false);
                result.setReason("Masking policy exists for mapped Hive resource");
                return result;
            }

            RangerAccessResult rowFilterResult = chainedPlugin.evalRowFilterPolicies(request);
            if (rowFilterResult != null && rowFilterResult.isRowFilterEnabled()) {
                LOG.info("Access denied by chained plugin due to row-filter policy");
                result.setAllowed(false);
                result.setReason("Row-filter policy exists for mapped Hive resource");
                return result;
            }

            if (chainedPlugin.isAuthorizeOnlyWithChainedPlugin()) {
                RangerAccessResult chainedResult = chainedPlugin.isAccessAllowed(request);
                if (chainedResult != null) {
                    if (!chainedResult.getIsAllowed()) {
                        result.setAllowed(false);
                        result.setReason("Denied by chained Hive policy");
                    }
                    return result;
                }
            }
        }

        return result;
    }

    /**
     * Create a Ranger access request from S3 parameters.
     */
    private RangerAccessRequestImpl createAccessRequest(String bucket, String path, String accessType,
                                                         String user, Set<String> groups, String clientIP) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setServiceDef(plugin.getServiceDef());
        resource.setValue(RESOURCE_BUCKET, bucket);

        if (StringUtils.isNotBlank(path)) {
            String normalizedPath = path;
            if (normalizedPath.startsWith("/")) {
                normalizedPath = normalizedPath.substring(1);
            }
            resource.setValue(RESOURCE_PATH, normalizedPath);
        }

        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setResource(resource);
        request.setAccessType(accessType);
        request.setUser(user);
        request.setUserGroups(groups);
        request.setAccessTime(new Date());
        request.setAction(accessType);

        if (StringUtils.isNotBlank(clientIP)) {
            request.setClientIPAddress(clientIP);
        }

        request.setClusterName(plugin.getClusterName());

        return request;
    }

    /**
     * Map S3 operation to access type.
     */
    public static String mapS3OperationToAccessType(String s3Operation) {
        if (StringUtils.isBlank(s3Operation)) {
            return ACCESS_TYPE_READ;
        }

        String operation = s3Operation.toLowerCase();

        if (operation.contains("getobject") || operation.contains("headobject")) {
            return ACCESS_TYPE_READ;
        } else if (operation.contains("putobject") || operation.contains("copyobject") ||
                   operation.contains("upload") || operation.contains("createobject")) {
            return ACCESS_TYPE_WRITE;
        } else if (operation.contains("deleteobject") || operation.contains("delete")) {
            return ACCESS_TYPE_DELETE;
        } else if (operation.contains("listobject") || operation.contains("listbucket")) {
            return ACCESS_TYPE_LIST;
        } else if (operation.contains("getbucketacl") || operation.contains("getobjectacl")) {
            return ACCESS_TYPE_GET_ACL;
        } else if (operation.contains("putbucketacl") || operation.contains("putobjectacl")) {
            return ACCESS_TYPE_PUT_ACL;
        }

        return ACCESS_TYPE_READ;
    }

    /**
     * Get the underlying plugin.
     */
    public RangerS3Plugin getPlugin() {
        return plugin;
    }

    /**
     * Authorization result.
     */
    public static class AuthzResult {
        private boolean allowed;
        private boolean audited;
        private Long policyId;
        private String reason;

        public boolean isAllowed() {
            return allowed;
        }

        public void setAllowed(boolean allowed) {
            this.allowed = allowed;
        }

        public boolean isAudited() {
            return audited;
        }

        public void setAudited(boolean audited) {
            this.audited = audited;
        }

        public Long getPolicyId() {
            return policyId;
        }

        public void setPolicyId(Long policyId) {
            this.policyId = policyId;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }

        @Override
        public String toString() {
            return "AuthzResult{allowed=" + allowed + ", audited=" + audited +
                   ", policyId=" + policyId + ", reason='" + reason + "'}";
        }
    }

    /**
     * Audit handler for S3 authorization events.
     */
    private static class RangerS3AuditHandler extends RangerDefaultAuditHandler {
        private static final Logger LOG = LoggerFactory.getLogger(RangerS3AuditHandler.class);

        @Override
        public void processResult(RangerAccessResult result) {
            if (result != null && result.getIsAudited()) {
                super.processResult(result);
            }
        }
    }
}
