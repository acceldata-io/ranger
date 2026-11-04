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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
        // Before the engine, so a bad rule string fails startup rather than the
        // first authorization request.
        IdentityNormalizer.configureRules(config.authToLocalRules());
        // Before the engine too: RangerBasePlugin.init() starts the policy and
        // user-store refreshers immediately, and they authenticate as whoever
        // UGI says we are at that moment.
        loginFromKeytab(config.serviceType());
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

    /**
     * Log in from the keytab named in {@code ranger-<serviceType>-security.xml}.
     *
     * <p>Every other Ranger plugin inherits a Kerberos login from the service it
     * runs inside. This agent is its own process, so nothing logs it in and
     * {@link UserGroupInformation} falls back to SIMPLE: Ranger Admin then answers
     * every policy, role and user-store download with
     * {@code "Unauthenticated access not allowed"} and the agent never becomes
     * ready.
     *
     * <p>Failure here is deliberately fatal. An agent that starts without its
     * identity would run on a stale policy cache and look healthy.
     */
    private static void loginFromKeytab(String serviceType) throws IOException {
        String prefix = "ranger.plugin." + serviceType + ".ugi.";

        Configuration conf = new Configuration();
        conf.addResource("ranger-" + serviceType + "-security.xml");

        if (!conf.getBoolean(prefix + "initialize", false)) {
            LOG.warn("{}initialize is false; the agent will reach Ranger Admin as the OS user."
                    + " Set it true on any Kerberos cluster.", prefix);
            return;
        }

        String principal = conf.getTrimmed(prefix + "keytab.principal");
        String keytab    = conf.getTrimmed(prefix + "keytab.file");
        if (principal == null || principal.isEmpty() || keytab == null || keytab.isEmpty()) {
            throw new IllegalStateException(prefix + "initialize is true but "
                    + prefix + "keytab.principal and " + prefix + "keytab.file are not both set");
        }

        // UGI reads this from core-site.xml, which this agent has no reason to
        // carry on its classpath. Without it isSecurityEnabled() stays false and
        // the login below is silently downgraded to SIMPLE.
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);

        // loginUserFromKeytab installs its own TGT renewal thread, so there is
        // nothing to schedule here.
        LOG.info("Kerberos login as {} from {}", principal, keytab);
    }
}
