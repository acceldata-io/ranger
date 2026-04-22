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

package org.apache.ranger.authorization.s3.proxy;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.s3.RangerS3Authorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RangerS3ProxyHandler is a Servlet Filter that can be deployed in front of
 * any S3-compatible storage to intercept ALL S3 requests.
 *
 * Deployment options:
 * 1. Deploy as a filter in an S3 proxy (Nginx, HAProxy with Lua, etc.)
 * 2. Deploy with MinIO Gateway
 * 3. Deploy as a standalone reverse proxy
 * 4. Integrate with S3Proxy (https://github.com/gaul/s3proxy)
 *
 * This intercepts:
 * - AWS CLI: aws s3 cp, aws s3 ls, etc.
 * - Python boto3: s3.get_object(), s3.put_object(), etc.
 * - Java AWS SDK: AmazonS3Client.getObject(), etc.
 * - Spark/Hive: s3a:// filesystem
 * - ANY S3-compatible client
 *
 * Request flow:
 * Client → S3 Proxy (with this filter) → Ranger Auth → Backend S3
 */
public class RangerS3ProxyHandler implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(RangerS3ProxyHandler.class);

    // S3 URL patterns
    // Path-style: http://s3.amazonaws.com/bucket/key
    // Virtual-hosted-style: http://bucket.s3.amazonaws.com/key
    private static final Pattern PATH_STYLE_PATTERN = Pattern.compile("^/([^/]+)(/.*)?$");
    private static final Pattern VIRTUAL_HOST_PATTERN = Pattern.compile("^([^.]+)\\.s3[.-]");

    private RangerS3Authorizer authorizer;
    private String userHeader = "X-Ranger-User";
    private String groupsHeader = "X-Ranger-Groups";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("==> RangerS3ProxyHandler.init()");

        String serviceName = filterConfig.getInitParameter("ranger.service.name");
        if (StringUtils.isBlank(serviceName)) {
            serviceName = "s3";
        }

        this.authorizer = new RangerS3Authorizer(serviceName);

        String userHeaderConfig = filterConfig.getInitParameter("ranger.user.header");
        if (StringUtils.isNotBlank(userHeaderConfig)) {
            this.userHeader = userHeaderConfig;
        }

        String groupsHeaderConfig = filterConfig.getInitParameter("ranger.groups.header");
        if (StringUtils.isNotBlank(groupsHeaderConfig)) {
            this.groupsHeader = groupsHeaderConfig;
        }

        LOG.info("<== RangerS3ProxyHandler.init(): serviceName={}, userHeader={}, groupsHeader={}",
                 serviceName, userHeader, groupsHeader);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        String method = httpRequest.getMethod();
        String uri = httpRequest.getRequestURI();
        String host = httpRequest.getHeader("Host");

        LOG.debug("==> doFilter(): method={}, uri={}, host={}", method, uri, host);

        try {
            // Parse S3 request to extract bucket and key
            S3RequestInfo s3Request = parseS3Request(httpRequest);

            if (s3Request == null || StringUtils.isBlank(s3Request.bucket)) {
                // Not a valid S3 object request, let it pass (might be bucket list, etc.)
                chain.doFilter(request, response);
                return;
            }

            // Get user info from request
            String user = extractUser(httpRequest);
            Set<String> groups = extractGroups(httpRequest);
            String clientIP = httpRequest.getRemoteAddr();

            // Map HTTP method to S3 operation
            String accessType = mapHttpMethodToAccessType(method, uri);

            // Perform authorization
            RangerS3Authorizer.AuthzResult result = authorizer.isAccessAllowed(
                s3Request.bucket,
                s3Request.key,
                accessType,
                user,
                groups,
                clientIP
            );

            if (result.isAllowed()) {
                LOG.info("S3 Access ALLOWED: user={}, bucket={}, key={}, method={}, policyId={}",
                         user, s3Request.bucket, s3Request.key, method, result.getPolicyId());
                chain.doFilter(request, response);
            } else {
                LOG.warn("S3 Access DENIED: user={}, bucket={}, key={}, method={}, reason={}",
                         user, s3Request.bucket, s3Request.key, method, result.getReason());
                sendAccessDenied(httpResponse, result.getReason());
            }

        } catch (Exception e) {
            LOG.error("Error processing S3 request", e);
            sendError(httpResponse, "Internal authorization error");
        }

        LOG.debug("<== doFilter()");
    }

    /**
     * Parse S3 request to extract bucket and key.
     */
    private S3RequestInfo parseS3Request(HttpServletRequest request) {
        String host = request.getHeader("Host");
        String uri = request.getRequestURI();

        S3RequestInfo info = new S3RequestInfo();

        // Try virtual-hosted-style first: bucket.s3.amazonaws.com/key
        if (StringUtils.isNotBlank(host)) {
            Matcher matcher = VIRTUAL_HOST_PATTERN.matcher(host);
            if (matcher.find()) {
                info.bucket = matcher.group(1);
                info.key = uri.startsWith("/") ? uri.substring(1) : uri;
                return info;
            }
        }

        // Try path-style: /bucket/key
        if (StringUtils.isNotBlank(uri)) {
            Matcher matcher = PATH_STYLE_PATTERN.matcher(uri);
            if (matcher.find()) {
                info.bucket = matcher.group(1);
                String keyPart = matcher.group(2);
                info.key = (keyPart != null && keyPart.length() > 1) ? keyPart.substring(1) : "";
                return info;
            }
        }

        return null;
    }

    /**
     * Extract user from request (can come from various sources).
     */
    private String extractUser(HttpServletRequest request) {
        // 1. Check custom header (set by auth proxy like Apache Knox)
        String user = request.getHeader(userHeader);

        // 2. Check standard remote user
        if (StringUtils.isBlank(user)) {
            user = request.getRemoteUser();
        }

        // 3. Check AWS Signature V4 for access key (could map to user)
        if (StringUtils.isBlank(user)) {
            String authHeader = request.getHeader("Authorization");
            if (StringUtils.isNotBlank(authHeader) && authHeader.contains("Credential=")) {
                // Extract access key from AWS Signature
                // Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request
                int credStart = authHeader.indexOf("Credential=") + 11;
                int credEnd = authHeader.indexOf("/", credStart);
                if (credEnd > credStart) {
                    user = authHeader.substring(credStart, credEnd);
                }
            }
        }

        // 4. Default to anonymous
        if (StringUtils.isBlank(user)) {
            user = "anonymous";
        }

        return user;
    }

    /**
     * Extract user groups from request.
     */
    private Set<String> extractGroups(HttpServletRequest request) {
        Set<String> groups = new HashSet<>();

        String groupsHeader = request.getHeader(this.groupsHeader);
        if (StringUtils.isNotBlank(groupsHeader)) {
            for (String group : groupsHeader.split(",")) {
                groups.add(group.trim());
            }
        }

        return groups;
    }

    /**
     * Map HTTP method to S3 access type.
     */
    private String mapHttpMethodToAccessType(String method, String uri) {
        if (method == null) {
            return RangerS3Authorizer.ACCESS_TYPE_READ;
        }

        switch (method.toUpperCase()) {
            case "GET":
                if (uri != null && (uri.contains("?acl") || uri.contains("?policy"))) {
                    return RangerS3Authorizer.ACCESS_TYPE_GET_ACL;
                }
                if (uri != null && uri.contains("?list-type=")) {
                    return RangerS3Authorizer.ACCESS_TYPE_LIST;
                }
                return RangerS3Authorizer.ACCESS_TYPE_READ;

            case "HEAD":
                return RangerS3Authorizer.ACCESS_TYPE_READ;

            case "PUT":
                if (uri != null && (uri.contains("?acl") || uri.contains("?policy"))) {
                    return RangerS3Authorizer.ACCESS_TYPE_PUT_ACL;
                }
                return RangerS3Authorizer.ACCESS_TYPE_WRITE;

            case "POST":
                return RangerS3Authorizer.ACCESS_TYPE_WRITE;

            case "DELETE":
                return RangerS3Authorizer.ACCESS_TYPE_DELETE;

            default:
                return RangerS3Authorizer.ACCESS_TYPE_READ;
        }
    }

    /**
     * Send access denied response in S3 format.
     */
    private void sendAccessDenied(HttpServletResponse response, String reason) throws IOException {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        response.setContentType("application/xml");
        response.getWriter().write(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<Error>\n" +
            "  <Code>AccessDenied</Code>\n" +
            "  <Message>Access Denied by Ranger: " + escapeXml(reason) + "</Message>\n" +
            "  <RequestId>" + System.currentTimeMillis() + "</RequestId>\n" +
            "</Error>"
        );
    }

    /**
     * Send error response in S3 format.
     */
    private void sendError(HttpServletResponse response, String message) throws IOException {
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        response.setContentType("application/xml");
        response.getWriter().write(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<Error>\n" +
            "  <Code>InternalError</Code>\n" +
            "  <Message>" + escapeXml(message) + "</Message>\n" +
            "</Error>"
        );
    }

    private String escapeXml(String input) {
        if (input == null) return "";
        return input.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace("\"", "&quot;");
    }

    @Override
    public void destroy() {
        LOG.info("RangerS3ProxyHandler destroyed");
    }

    /**
     * S3 request information.
     */
    private static class S3RequestInfo {
        String bucket;
        String key;
    }
}
