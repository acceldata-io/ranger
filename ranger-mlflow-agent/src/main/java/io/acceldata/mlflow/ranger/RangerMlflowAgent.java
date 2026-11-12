/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.acceldata.mlflow.ranger;

import org.apache.ranger.admin.client.RangerAdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for {@code ranger-mlflow-agent}.
 *
 * <p>Polls Ranger Admin for {@code mlflow} policies, user-group mappings, and
 * roles, and reconciles them into MLflow 3.16.0's native RBAC system.
 */
public final class RangerMlflowAgent {

    private static final Logger LOG = LoggerFactory.getLogger(RangerMlflowAgent.class);

    public static final String DEFAULT_CONFIG_PATH =
            "/etc/ranger-mlflow-agent/agent.properties";

    public static void main(String[] args) {
        String configPath = args.length > 0 ? args[0] : DEFAULT_CONFIG_PATH;
        prewireKerberosSystemProps(configPath);

        LOG.info("ranger-mlflow-agent starting; configPath={}", configPath);
        try {
            run(configPath);
            System.exit(0);
        } catch (Throwable t) {
            LOG.error("Fatal startup error", t);
            System.exit(1);
        }
    }

    static void run(String configPath) throws Exception {
        AgentConfig config = AgentConfig.load(configPath);
        logEffectiveConfig(config);

        RangerAdminClient    adminClient  = RangerAdminClientFactory.create(config);
        RbacConverter        converter    = new RbacConverter(config.managedRolePrefix(), config.mlflowDefaultWorkspace());
        MlflowRbacClient     mlflowClient = new MlflowRbacClient(config);
        MlflowRbacReconciler reconciler   = new MlflowRbacReconciler(mlflowClient, config);
        PolicySyncService    sync         = new PolicySyncService(
                config, adminClient, converter, mlflowClient, reconciler);
        HealthServer         health       = new HealthServer(config.healthPort(), sync);

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received; stopping ranger-mlflow-agent");
            try { sync.stop(); } catch (Throwable t) { LOG.error("sync.stop() failed", t); }
            try { health.stop(); } catch (Throwable t) { LOG.error("health.stop() failed", t); }
            shutdownLatch.countDown();
        }, "mlflow-agent-shutdown"));

        try {
            health.start();
            sync.start();
            LOG.info("ranger-mlflow-agent started and running");
            shutdownLatch.await();
        } catch (Throwable t) {
            try { sync.stop(); } catch (Throwable ignored) { }
            try { health.stop(); } catch (Throwable ignored) { }
            throw t;
        }
    }

    private static void prewireKerberosSystemProps(String configPath) {
        try {
            Properties props = new Properties();
            try (FileInputStream in = new FileInputStream(configPath)) {
                props.load(in);
            }
            String mode = props.getProperty("ranger.auth.mode", "basic").trim();
            if (!"kerberos".equalsIgnoreCase(mode)) {
                return;
            }
            String krb5Path = props.getProperty("kerberos.krb5.conf.path");
            if (krb5Path != null && !krb5Path.isBlank()) {
                System.setProperty("java.security.krb5.conf", krb5Path.trim());
            }
            String principal  = nullIfBlank(props.getProperty("kerberos.principal"));
            String keytabPath = nullIfBlank(props.getProperty("kerberos.keytab.path"));
            if (principal != null && keytabPath != null) {
                String jaasPath = writeJaasConfigForSpnego(principal, keytabPath);
                System.setProperty("java.security.auth.login.config", jaasPath);
            }
        } catch (Throwable ignored) {
            // AgentConfig.load will report any configuration error with a clear message.
        }
    }

    private static String writeJaasConfigForSpnego(String principal, String keytabPath) throws IOException {
        rejectUnsafeJaasValue(principal, "kerberos.principal");
        rejectUnsafeJaasValue(keytabPath, "kerberos.keytab.path");

        String jaas = String.join("\n",
                "com.sun.security.jgss.krb5.initiate {",
                "    com.sun.security.auth.module.Krb5LoginModule required",
                "    useKeyTab=true",
                "    keyTab=\"" + keytabPath + "\"",
                "    principal=\"" + principal + "\"",
                "    doNotPrompt=true",
                "    storeKey=true",
                "    refreshKrb5Config=true;",
                "};",
                "");

        Path out = Paths.get("/tmp/ranger-mlflow-agent-jaas.conf");
        Set<PosixFilePermission> ownerOnly = EnumSet.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE);
        Files.deleteIfExists(out);
        Files.createFile(out, PosixFilePermissions.asFileAttribute(ownerOnly));
        Files.write(out, jaas.getBytes(StandardCharsets.UTF_8));
        return out.toString();
    }

    private static void rejectUnsafeJaasValue(String value, String key) {
        if (value == null
                || value.indexOf('"')  >= 0
                || value.indexOf('\\') >= 0
                || value.indexOf('\n') >= 0
                || value.indexOf('\r') >= 0
                || value.indexOf(';')  >= 0) {
            throw new IllegalArgumentException(
                    key + " contains unsafe characters for JAAS configuration");
        }
    }

    private static String nullIfBlank(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }

    private static void logEffectiveConfig(AgentConfig c) {
        LOG.info("Effective configuration:");
        LOG.info("  ranger.admin.url         = {}", c.rangerAdminUrl());
        LOG.info("  ranger.service.name      = {}", c.rangerServiceName());
        LOG.info("  ranger.service.type      = {}", c.rangerServiceType());
        LOG.info("  ranger.auth.mode         = {}", c.authMode());
        LOG.info("  mlflow.url               = {}", c.mlflowUrl());
        LOG.info("  mlflow.admin.username    = {}", c.mlflowAdminUsername());
        LOG.info("  mlflow.default.workspace = {}", c.mlflowDefaultWorkspace());
        LOG.info("  mlflow.managed.prefix    = {}", c.managedRolePrefix());
        LOG.info("  mlflow.auto.create.users = {}", c.autoCreateUsers());
        LOG.info("  sync.poll.interval.ms    = {}", c.pollIntervalMs());
        LOG.info("  sync.force.resync.ms     = {}", c.forceResyncMs());
        LOG.info("  doctrine                 = {}", c.doctrine());
        LOG.info("  health.port              = {}", c.healthPort());
    }

    private RangerMlflowAgent() {
        // entry-point class
    }
}
