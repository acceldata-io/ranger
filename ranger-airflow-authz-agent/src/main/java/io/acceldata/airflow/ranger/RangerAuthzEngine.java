/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.airflow.ranger;

import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Production engine: {@link RangerBasePlugin} with the default audit handler.
 * Policy download, cache, heartbeat and evaluation are all the plugin's.
 */
public final class RangerAuthzEngine implements AuthzEngine {

    private static final Logger LOG = LoggerFactory.getLogger(RangerAuthzEngine.class);

    private final RangerBasePlugin          plugin;
    private final RangerDefaultAuditHandler auditHandler;

    public RangerAuthzEngine(AgentConfig config) {
        Objects.requireNonNull(config, "config");
        this.plugin = new RangerBasePlugin(config.serviceType(), config.appId());
        this.auditHandler = new RangerDefaultAuditHandler(plugin.getConfig());
        this.plugin.setResultProcessor(auditHandler);
        this.plugin.init();
        LOG.info("RangerBasePlugin started serviceType={} appId={} serviceName={}",
                config.serviceType(), config.appId(), plugin.getServiceName());
    }

    /** Visible for tests that inject an in-memory plugin. */
    RangerAuthzEngine(RangerBasePlugin plugin) {
        this.plugin = Objects.requireNonNull(plugin, "plugin");
        this.auditHandler = null;
    }

    @Override
    public boolean isReady() {
        return plugin.getPoliciesVersion() >= 0 && plugin.getUserStoreVersion() >= 0;
    }

    @Override
    public String notReadyReason() {
        boolean policiesLoaded = plugin.getPoliciesVersion() >= 0;
        boolean userStoreLoaded = plugin.getUserStoreVersion() >= 0;
        if (policiesLoaded && userStoreLoaded) {
            return null;
        }
        if (!policiesLoaded && !userStoreLoaded) {
            return "policies not loaded; user store not loaded";
        }
        return policiesLoaded ? "user store not loaded" : "policies not loaded";
    }

    @Override
    public long policyVersion() {
        return plugin.getPoliciesVersion();
    }

    @Override
    public String serviceName() {
        return plugin.getServiceName();
    }

    @Override
    public Integer serviceDefVersion() {
        RangerServiceDef def = plugin.getServiceDef();
        if (def == null || def.getVersion() == null) {
            return null;
        }
        return def.getVersion().intValue();
    }

    @Override
    public long userStoreVersion() {
        return plugin.getUserStoreVersion();
    }

    @Override
    public RangerAccessResult evaluate(RangerAccessRequest request) {
        return plugin.isAccessAllowed(request);
    }

    @Override
    public RangerAccessResult evaluateNoAudit(RangerAccessRequest request) {
        // A null result processor is how RangerBasePlugin is told to skip
        // auditing for one evaluation.
        return plugin.isAccessAllowed(request, null);
    }

    @Override
    public void auditFilterSummary(RangerAccessRequest request, RangerAccessResult result,
                                   boolean allowed, String requestData) {
        if (auditHandler == null || result == null) {
            return;
        }
        AuthzAuditEvent event = auditHandler.getAuthzEvents(result);
        if (event == null) {
            // Audit is switched off for this resource by an audit filter.
            return;
        }
        event.setAccessResult((short) (allowed ? 1 : 0));
        event.setRequestData(requestData);
        if (!allowed) {
            // A representative result only carries a policy id when something
            // matched; a summary that denied everything must not claim one.
            event.setPolicyId(-1L);
        }
        auditHandler.logAuthzAudit(event);
    }

    @Override
    public void close() {
        plugin.cleanup();
    }
}
