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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/**
 * Process entry point. Starts the Ranger plugin then the loopback HTTP server,
 * and shuts both down on SIGTERM.
 *
 * <p>Usage:
 * <pre>
 *   java -cp conf:ranger-airflow-authz-agent.jar \
 *        io.acceldata.airflow.ranger.RangerAirflowAuthzAgent \
 *        /etc/ranger-airflow-authz-agent/conf/authz-agent.properties
 * </pre>
 */
public final class RangerAirflowAuthzAgent {

    private static final Logger LOG = LoggerFactory.getLogger(RangerAirflowAuthzAgent.class);

    private RangerAirflowAuthzAgent() {}

    public static void main(String[] args) {
        String configPath = args.length > 0 ? args[0] : AgentConfig.DEFAULT_CONFIG_PATH;
        LOG.info("ranger-airflow-authz-agent starting; configPath={}", configPath);

        try {
            run(Path.of(configPath));
            System.exit(0);
        } catch (Throwable t) {
            LOG.error("Fatal startup error", t);
            System.exit(1);
        }
    }

    static void run(Path configPath) throws Exception {
        AgentConfig config = AgentConfig.load(configPath);
        RangerAuthzEngine engine = new RangerAuthzEngine(config);
        AuthzServer server = new AuthzServer(config, engine);
        CountDownLatch shutdown = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("shutdown requested");
            server.stop();
            engine.close();
            shutdown.countDown();
        }, "authz-agent-shutdown"));

        try {
            server.start();
        } catch (Exception e) {
            engine.close();
            throw e;
        }

        shutdown.await();
        LOG.info("ranger-airflow-authz-agent stopped");
    }
}
