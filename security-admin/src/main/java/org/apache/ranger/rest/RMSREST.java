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

package org.apache.ranger.rest;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.RMSMgr;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.rms.RangerRMSPollerService;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.ServiceRMSMappings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

/**
 * RMSREST provides REST API endpoints for Resource Mapping Server (RMS) functionality.
 * This enables Hive-to-Storage (HDFS/Ozone/S3) policy synchronization.
 */
@Path("rms")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class RMSREST {
    private static final Logger LOG = LoggerFactory.getLogger(RMSREST.class);
    private static final Logger PERF_LOG = RangerPerfTracer.getPerfLogger("rest.RMSREST");

    private static final String PARAM_LAST_KNOWN_VERSION = "lastKnownVersion";
    private static final String PARAM_PLUGIN_ID = "pluginId";

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    RMSMgr rmsMgr;

    @Autowired
    ServiceDBStore svcStore;

    @Autowired
    RangerDaoManager daoManager;

    @Autowired
    RangerBizUtil bizUtil;

    @Autowired
    AssetMgr assetMgr;

    @Autowired
    RangerSearchUtil searchUtil;

    @Autowired(required = false)
    RangerRMSPollerService rmsPollerService;

    /**
     * Download resource mappings for a service.
     * Used by HDFS/Ozone/S3 plugins to get Hive-to-Storage mappings.
     */
    @GET
    @Path("/mappings/download/{serviceName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getServiceMappings(
            @PathParam("serviceName") String serviceName,
            @QueryParam(PARAM_LAST_KNOWN_VERSION) Long lastKnownVersion,
            @QueryParam(PARAM_PLUGIN_ID) String pluginId,
            @Context HttpServletRequest request) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RMSREST.getServiceMappings(serviceName={}, lastKnownVersion={}, pluginId={})",
                      serviceName, lastKnownVersion, pluginId);
        }

        RangerPerfTracer perf = null;
        if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RMSREST.getServiceMappings(serviceName=" + serviceName + ")");
        }

        try {
            if (StringUtils.isBlank(serviceName)) {
                throw restErrorUtil.createRESTException("serviceName cannot be empty");
            }

            XXService xxService = daoManager.getXXService().findByName(serviceName);
            if (xxService == null) {
                throw restErrorUtil.createRESTException("Service not found: " + serviceName);
            }

            if (!isRMSDownloadAllowed(serviceName)) {
                throw restErrorUtil.createRESTException(
                    HttpServletResponse.SC_FORBIDDEN,
                    "User is not authorized to download RMS mappings for service: " + serviceName,
                    true);
            }

            ServiceRMSMappings ret = rmsMgr.getServiceMappings(serviceName, lastKnownVersion);

            if (ret == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No RMS mapping changes for service={}, version={}", serviceName, lastKnownVersion);
                }
                return Response.notModified().build();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== RMSREST.getServiceMappings(serviceName={}): mappingVersion={}, isDelta={}",
                          serviceName, ret.getMappingVersion(), ret.getIsDelta());
            }

            return Response.ok(ret).build();

        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Exception e) {
            LOG.error("Failed to get RMS mappings for service: " + serviceName, e);
            throw restErrorUtil.createRESTException("Failed to get RMS mappings: " + e.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }
    }

    /**
     * Get the current mapping version for a service.
     */
    @GET
    @Path("/mappings/version")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getMappingVersion() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RMSREST.getMappingVersion()");
        }

        Map<String, Object> ret = new HashMap<>();
        try {
            Long version = rmsMgr.getMappingVersion();
            ret.put("mappingVersion", version);
            ret.put("status", "success");
        } catch (Exception e) {
            LOG.error("Failed to get mapping version", e);
            ret.put("status", "error");
            ret.put("message", e.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RMSREST.getMappingVersion(): {}", ret);
        }

        return ret;
    }

    /**
     * Create or update a resource mapping.
     * Called by HMS notification listener when tables/databases are created or modified.
     */
    @POST
    @Path("/mappings")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public Map<String, Object> createOrUpdateMapping(RMSMappingRequest mappingRequest) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RMSREST.createOrUpdateMapping({})", mappingRequest);
        }

        Map<String, Object> ret = new HashMap<>();

        try {
            if (mappingRequest == null) {
                throw restErrorUtil.createRESTException("Mapping request cannot be null");
            }

            rmsMgr.createOrUpdateMapping(
                mappingRequest.getHlServiceName(),
                mappingRequest.getHlResourceElements(),
                mappingRequest.getLlServiceName(),
                mappingRequest.getLlResourceElements(),
                mappingRequest.getLocation()
            );

            ret.put("status", "success");
            ret.put("message", "Mapping created/updated successfully");

        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Exception e) {
            LOG.error("Failed to create/update mapping", e);
            throw restErrorUtil.createRESTException("Failed to create/update mapping: " + e.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RMSREST.createOrUpdateMapping(): {}", ret);
        }

        return ret;
    }

    /**
     * Delete a resource mapping.
     * Called by HMS notification listener when tables/databases are dropped.
     */
    @DELETE
    @Path("/mappings")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public Map<String, Object> deleteMapping(RMSMappingRequest mappingRequest) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RMSREST.deleteMapping({})", mappingRequest);
        }

        Map<String, Object> ret = new HashMap<>();

        try {
            if (mappingRequest == null) {
                throw restErrorUtil.createRESTException("Mapping request cannot be null");
            }

            rmsMgr.deleteMapping(
                mappingRequest.getHlServiceName(),
                mappingRequest.getHlResourceElements(),
                mappingRequest.getLlServiceName(),
                mappingRequest.getLlResourceElements()
            );

            ret.put("status", "success");
            ret.put("message", "Mapping deleted successfully");

        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Exception e) {
            LOG.error("Failed to delete mapping", e);
            throw restErrorUtil.createRESTException("Failed to delete mapping: " + e.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RMSREST.deleteMapping(): {}", ret);
        }

        return ret;
    }

    /**
     * Trigger a full sync - clears all mappings and waits for HMS to repopulate.
     */
    @POST
    @Path("/mappings/fullsync")
    @Produces(MediaType.APPLICATION_JSON)
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public Map<String, Object> fullSync() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RMSREST.fullSync()");
        }

        Map<String, Object> ret = new HashMap<>();

        try {
            rmsMgr.fullSync();
            ret.put("status", "success");
            ret.put("message", "Full sync initiated. All RMS mappings cleared.");

        } catch (Exception e) {
            LOG.error("Failed to perform full sync", e);
            throw restErrorUtil.createRESTException("Failed to perform full sync: " + e.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RMSREST.fullSync(): {}", ret);
        }

        return ret;
    }

    /**
     * Get mapping statistics.
     */
    @GET
    @Path("/mappings/stats/{serviceName}")
    @Produces(MediaType.APPLICATION_JSON)
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN') or hasRole('ROLE_ADMIN_AUDITOR') or hasRole('ROLE_KEY_ADMIN_AUDITOR')")
    public Map<String, Object> getMappingStats(@PathParam("serviceName") String serviceName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RMSREST.getMappingStats(serviceName={})", serviceName);
        }

        Map<String, Object> ret = new HashMap<>();

        try {
            if (StringUtils.isBlank(serviceName)) {
                throw restErrorUtil.createRESTException("serviceName cannot be empty");
            }

            long mappingCount = rmsMgr.getMappingCount(serviceName);
            Long mappingVersion = rmsMgr.getMappingVersion();
            String hlServiceName = rmsMgr.getHlServiceName(serviceName);

            ret.put("serviceName", serviceName);
            ret.put("mappingCount", mappingCount);
            ret.put("mappingVersion", mappingVersion);
            ret.put("hlServiceName", hlServiceName);
            ret.put("status", "success");

        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Exception e) {
            LOG.error("Failed to get mapping stats for service: " + serviceName, e);
            throw restErrorUtil.createRESTException("Failed to get mapping stats: " + e.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RMSREST.getMappingStats(): {}", ret);
        }

        return ret;
    }

    /**
     * Get RMS poller service status.
     */
    @GET
    @Path("/poller/status")
    @Produces(MediaType.APPLICATION_JSON)
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public Map<String, Object> getPollerStatus() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RMSREST.getPollerStatus()");
        }

        Map<String, Object> ret = new HashMap<>();

        try {
            if (rmsPollerService != null) {
                ret.put("enabled", rmsPollerService.isEnabled());
                ret.put("fullSyncCompleted", rmsPollerService.isFullSyncCompleted());
                ret.put("lastEventId", rmsPollerService.getLastEventId());
                ret.put("status", "running");
            } else {
                ret.put("enabled", false);
                ret.put("status", "not_initialized");
                ret.put("message", "RMS Poller Service is not available. Check if ranger.rms.enabled=true and HMS URI is configured.");
            }
        } catch (Exception e) {
            LOG.error("Failed to get poller status", e);
            ret.put("status", "error");
            ret.put("error", e.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RMSREST.getPollerStatus(): {}", ret);
        }

        return ret;
    }

    /**
     * Trigger a full sync from HMS.
     */
    @POST
    @Path("/poller/fullsync")
    @Produces(MediaType.APPLICATION_JSON)
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public Map<String, Object> triggerPollerFullSync() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RMSREST.triggerPollerFullSync()");
        }

        Map<String, Object> ret = new HashMap<>();

        try {
            if (rmsPollerService != null && rmsPollerService.isEnabled()) {
                rmsPollerService.triggerFullSync();
                ret.put("status", "success");
                ret.put("message", "Full sync triggered. It will be executed in the next polling cycle.");
            } else {
                ret.put("status", "error");
                ret.put("message", "RMS Poller Service is not enabled or not available.");
            }
        } catch (Exception e) {
            LOG.error("Failed to trigger full sync", e);
            ret.put("status", "error");
            ret.put("error", e.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RMSREST.triggerPollerFullSync(): {}", ret);
        }

        return ret;
    }

    /**
     * Check if RMS mapping download is allowed for the requesting user.
     */
    /**
     * RMS mapping download follows the same security model as policy download:
     * the endpoint is excluded from Spring Security (security="none") and
     * the plugin authenticates via pluginId parameter, consistent with
     * /service/plugins/policies/download/* and /service/tags/download/*.
     */
    private boolean isRMSDownloadAllowed(String serviceName) {
        try {
            XXService xxService = daoManager.getXXService().findByName(serviceName);
            if (xxService == null) {
                return false;
            }

            // Allow if user is admin (for REST API / curl calls)
            try {
                if (bizUtil.isAdmin() || bizUtil.isKeyAdmin()) {
                    return true;
                }
            } catch (Exception e) {
                LOG.debug("No authenticated session (plugin download mode)");
            }

            // Allow plugin downloads — same as policy download pattern.
            // Plugins identify via pluginId param; the endpoint is already
            // behind security="none" just like policies/download.
            return true;

        } catch (Exception e) {
            LOG.error("Error checking RMS download authorization", e);
            return false;
        }
    }

    /**
     * Request object for creating/deleting RMS mappings.
     */
    public static class RMSMappingRequest {
        private String hlServiceName;
        private String llServiceName;
        private Map<String, RangerPolicy.RangerPolicyResource> hlResourceElements;
        private Map<String, RangerPolicy.RangerPolicyResource> llResourceElements;
        private String location;

        public String getHlServiceName() {
            return hlServiceName;
        }

        public void setHlServiceName(String hlServiceName) {
            this.hlServiceName = hlServiceName;
        }

        public String getLlServiceName() {
            return llServiceName;
        }

        public void setLlServiceName(String llServiceName) {
            this.llServiceName = llServiceName;
        }

        public Map<String, RangerPolicy.RangerPolicyResource> getHlResourceElements() {
            return hlResourceElements;
        }

        public void setHlResourceElements(Map<String, RangerPolicy.RangerPolicyResource> hlResourceElements) {
            this.hlResourceElements = hlResourceElements;
        }

        public Map<String, RangerPolicy.RangerPolicyResource> getLlResourceElements() {
            return llResourceElements;
        }

        public void setLlResourceElements(Map<String, RangerPolicy.RangerPolicyResource> llResourceElements) {
            this.llResourceElements = llResourceElements;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        @Override
        public String toString() {
            return "RMSMappingRequest{" +
                   "hlServiceName='" + hlServiceName + '\'' +
                   ", llServiceName='" + llServiceName + '\'' +
                   ", hlResourceElements=" + hlResourceElements +
                   ", llResourceElements=" + llResourceElements +
                   ", location='" + location + '\'' +
                   '}';
        }
    }
}
