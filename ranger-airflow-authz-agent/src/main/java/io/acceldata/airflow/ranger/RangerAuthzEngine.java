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

    private final RangerBasePlugin plugin;

    public RangerAuthzEngine(AgentConfig config) {
        Objects.requireNonNull(config, "config");
        this.plugin = new RangerBasePlugin(config.serviceType(), config.appId());
        this.plugin.setResultProcessor(new RangerDefaultAuditHandler(plugin.getConfig()));
        this.plugin.init();
        LOG.info("RangerBasePlugin started serviceType={} appId={} serviceName={}",
                config.serviceType(), config.appId(), plugin.getServiceName());
    }

    /** Visible for tests that inject an in-memory plugin. */
    RangerAuthzEngine(RangerBasePlugin plugin) {
        this.plugin = Objects.requireNonNull(plugin, "plugin");
    }

    @Override
    public boolean isReady() {
        return plugin.getPoliciesVersion() >= 0;
    }

    @Override
    public String notReadyReason() {
        return isReady() ? null : "policies not loaded";
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
    public void close() {
        plugin.cleanup();
    }
}
