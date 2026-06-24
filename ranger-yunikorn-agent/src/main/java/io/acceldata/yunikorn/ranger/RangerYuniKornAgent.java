/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.yunikorn.ranger;

import org.apache.ranger.admin.client.RangerAdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Process entry point. Starts every component in dependency order, runs
 * forever, and shuts everything down cleanly on SIGTERM (the signal K8s
 * sends pods being terminated).
 *
 * <p>Usage:
 * <pre>
 *   java -jar ranger-yunikorn-agent.jar /etc/ranger-yunikorn-agent/agent.properties
 * </pre>
 *
 * <p>If no argument is supplied, defaults to
 * {@code /etc/ranger-yunikorn-agent/agent.properties}.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>Load and validate {@link AgentConfig} (fail fast on missing required keys).</li>
 *   <li>Build the {@link RangerAdminClient} via {@link RangerAdminClientFactory}.</li>
 *   <li>Construct {@link AclConverter}, {@link ConfigMapWriter},
 *       {@link PolicySyncService}, {@link HealthServer}.</li>
 *   <li>Start health server first so K8s probes can succeed before the first
 *       sync completes.</li>
 *   <li>Start sync service.</li>
 *   <li>Register a JVM shutdown hook that stops everything in reverse order.</li>
 *   <li>Block on a latch — the shutdown hook releases it.</li>
 * </ol>
 *
 * <p>Exit codes:
 * <ul>
 *   <li>{@code 0} — clean shutdown after SIGTERM</li>
 *   <li>{@code 1} — startup failed (config invalid, Ranger init error, port bind failure)</li>
 * </ul>
 */
public final class RangerYuniKornAgent {

    private static final Logger LOG = LoggerFactory.getLogger(RangerYuniKornAgent.class);

    private static final String DEFAULT_CONFIG_PATH =
            "/etc/ranger-yunikorn-agent/agent.properties";

    private static final int DEFAULT_HEALTH_PORT = 8080;

    public static void main(String[] args) {
        String configPath = args.length > 0 ? args[0] : DEFAULT_CONFIG_PATH;

        // Wire up Kerberos system properties before ANY class that might
        // read them loads. Java's Kerberos library (sun.security.krb5.Config)
        // caches its config on first access; if Hadoop's UGI internals or a
        // logger or anything else triggers that load before we set the
        // properties, the realm lookup permanently fails — even if we
        // later set the property correctly. Doing it as the first line of
        // main() avoids the whole class of "I set the property too late"
        // bugs.
        prewireKerberosSystemProps(configPath);

        LOG.info("ranger-yunikorn-agent starting; configPath={}", configPath);

        try {
            run(configPath);
            System.exit(0);
        } catch (Throwable t) {
            LOG.error("Fatal startup error", t);
            System.exit(1);
        }
    }

    /**
     * Best-effort: peek at agent.properties to find Kerberos config and
     * wire up the JDK system properties + JAAS config that SPNEGO needs,
     * BEFORE any class that uses Kerberos loads.
     *
     * <p>Why this matters: when the JDK's {@code HttpURLConnection} handles
     * a {@code WWW-Authenticate: Negotiate} response, it does NOT use the
     * Subject populated by {@code UGI.loginUserFromKeytab(...)}. Instead it
     * does its own JAAS login by looking up the config section named
     * {@code com.sun.security.jgss.krb5.initiate} in the file pointed to by
     * {@code java.security.auth.login.config}. If we don't provide that
     * file, the SPNEGO login fails silently, the Negotiate header is never
     * sent, and Ranger keeps replying with 401.
     *
     * <p>So we:
     * <ol>
     *   <li>set {@code java.security.krb5.conf} from
     *       {@code kerberos.krb5.conf.path}</li>
     *   <li>generate a JAAS file at {@code /tmp/agent-jaas.conf} containing
     *       the {@code com.sun.security.jgss.krb5.initiate} section, using
     *       {@code kerberos.principal} and {@code kerberos.keytab.path}</li>
     *   <li>set {@code java.security.auth.login.config} to that file</li>
     * </ol>
     *
     * <p>Failures here are silent; the proper config load runs immediately
     * after and will surface the same problems with clearer error messages.
     */
    private static void prewireKerberosSystemProps(String configPath) {
        try {
            java.util.Properties props = new java.util.Properties();
            try (java.io.FileInputStream in = new java.io.FileInputStream(configPath)) {
                props.load(in);
            }
            String mode = props.getProperty("ranger.auth.mode", "basic").trim();
            if (!"kerberos".equalsIgnoreCase(mode)) return;

            String krb5Path = props.getProperty("kerberos.krb5.conf.path");
            if (krb5Path != null && !krb5Path.isBlank()) {
                System.setProperty("java.security.krb5.conf", krb5Path.trim());
            }

            // Generate and install the JAAS config required by HttpURLConnection's
            // built-in SPNEGO handler. Without this, the JDK can't perform the
            // second leg of the Negotiate handshake even after UGI logs in.
            String principal  = nullIfBlank(props.getProperty("kerberos.principal"));
            String keytabPath = nullIfBlank(props.getProperty("kerberos.keytab.path"));
            if (principal != null && keytabPath != null) {
                String jaasPath = writeJaasConfigForSpnego(principal, keytabPath);
                System.setProperty("java.security.auth.login.config", jaasPath);
            }

            // Allow operators to enable the JDK's verbose Kerberos diagnostics
            // (-Dsun.security.krb5.debug=true) by setting kerberos.debug=true.
            if ("true".equalsIgnoreCase(props.getProperty("kerberos.debug"))) {
                System.setProperty("sun.security.krb5.debug",       "true");
                System.setProperty("sun.security.spnego.debug",     "true");
            }
        } catch (Throwable t) {
            // Don't fail main() over a peek; the real config load will
            // produce a clearer error.
        }
    }

    /**
     * Writes a JAAS config file containing the section names that
     * {@code HttpURLConnection}'s SPNEGO handler looks up
     * ({@code com.sun.security.jgss.krb5.initiate} and friends), pointing
     * the {@code Krb5LoginModule} at the operator's keytab and principal.
     *
     * <p>The file is written to {@code /tmp/agent-jaas.conf} (ephemeral,
     * tied to pod lifetime) rather than mounted from outside, since we
     * already have all the inputs we need from agent.properties. It is
     * created {@code 0600} (owner-only) because it names the Kerberos keytab,
     * which no other user or process on the host should be able to read.
     *
     * <p>{@code principal} and {@code keytabPath} are interpolated into
     * double-quoted JAAS options, so they are validated first: a value
     * containing a quote, backslash, newline, or semicolon could otherwise
     * break out of the quoting and inject extra JAAS options. Legitimate
     * Kerberos values never contain these, so the check also catches typos.
     */
    private static String writeJaasConfigForSpnego(String principal, String keytabPath)
            throws java.io.IOException {
        rejectUnsafeJaasValue(principal,  "kerberos.principal");
        rejectUnsafeJaasValue(keytabPath, "kerberos.keytab.path");

        String jaas = String.join("\n",
                "// Auto-generated by ranger-yunikorn-agent at startup.",
                "// Provides JAAS sections that the JDK's HttpURLConnection",
                "// SPNEGO handler looks up when responding to a 'WWW-Authenticate:",
                "// Negotiate' challenge from the server.",
                "",
                "com.sun.security.jgss.krb5.initiate {",
                "    com.sun.security.auth.module.Krb5LoginModule required",
                "    useKeyTab=true",
                "    keyTab=\"" + keytabPath + "\"",
                "    principal=\"" + principal + "\"",
                "    doNotPrompt=true",
                "    storeKey=true",
                "    refreshKrb5Config=true;",
                "};",
                "",
                "// Some older Hadoop clients lookup this name instead.",
                "com.sun.security.jgss.initiate {",
                "    com.sun.security.auth.module.Krb5LoginModule required",
                "    useKeyTab=true",
                "    keyTab=\"" + keytabPath + "\"",
                "    principal=\"" + principal + "\"",
                "    doNotPrompt=true",
                "    storeKey=true",
                "    refreshKrb5Config=true;",
                "};",
                "");

        java.nio.file.Path out = java.nio.file.Paths.get("/tmp/agent-jaas.conf");

        // Create owner-only (0600) up front so the file is never briefly
        // world-readable under the default umask before we can tighten it.
        java.util.Set<java.nio.file.attribute.PosixFilePermission> ownerOnly =
                java.util.EnumSet.of(
                        java.nio.file.attribute.PosixFilePermission.OWNER_READ,
                        java.nio.file.attribute.PosixFilePermission.OWNER_WRITE);
        java.nio.file.Files.deleteIfExists(out);
        java.nio.file.Files.createFile(out,
                java.nio.file.attribute.PosixFilePermissions.asFileAttribute(ownerOnly));
        java.nio.file.Files.write(out, jaas.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return out.toString();
    }

    /**
     * Rejects a value that cannot be safely interpolated into a double-quoted
     * JAAS option. A quote, backslash, newline, or semicolon would let the
     * value escape its quoting and inject additional JAAS configuration.
     */
    private static void rejectUnsafeJaasValue(String value, String configKey) {
        if (value == null
                || value.indexOf('"')  >= 0
                || value.indexOf('\\') >= 0
                || value.indexOf('\n') >= 0
                || value.indexOf('\r') >= 0
                || value.indexOf(';')  >= 0) {
            throw new IllegalArgumentException(
                    configKey + " contains characters not allowed in a JAAS value "
                    + "(double-quote, backslash, newline, or semicolon)");
        }
    }

    private static String nullIfBlank(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }

    /**
     * Wire up every component and block until shutdown. Visible for testing
     * and for embedding in another JVM if anyone ever wants to.
     */
    static void run(String configPath) throws Exception {
        AgentConfig config = AgentConfig.load(configPath);
        logEffectiveConfig(config);

        // One-shot startup probe so the URL we're about to hit is visible in
        // the logs *before* the sync service starts. Failures are logged but
        // never fatal — sync will still attempt regardless.
        RangerEndpointDiagnostic.run(config);

        RangerAdminClient adminClient = RangerAdminClientFactory.create(config);
        AclConverter      converter   = new AclConverter();
        ConfigMapWriter   writer      = new ConfigMapWriter(config);
        PolicySyncService sync        = new PolicySyncService(
                config, adminClient, converter, writer);
        HealthServer      health      = new HealthServer(DEFAULT_HEALTH_PORT, sync);

        CountDownLatch shutdownLatch = new CountDownLatch(1);

        // Shutdown hook: K8s sends SIGTERM, we get ~30s by default before
        // SIGKILL. Stop in reverse-of-startup order so dependents go first.
        Thread shutdownHook = new Thread(() -> {
            LOG.info("Shutdown signal received; stopping components");
            try { sync.stop(); } catch (Throwable t) { LOG.error("sync.stop() failed", t); }
            try { health.stop(); } catch (Throwable t) { LOG.error("health.stop() failed", t); }
            try { writer.close(); } catch (Throwable t) { LOG.error("writer.close() failed", t); }
            shutdownLatch.countDown();
        }, "agent-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try {
            health.start();        // probes can answer before first sync completes
            sync.start();
            LOG.info("ranger-yunikorn-agent started and running");

            // Block forever; shutdown hook releases the latch.
            shutdownLatch.await();
        } catch (Throwable t) {
            // Startup failure or unexpected interruption: tear down whatever
            // we managed to start.
            LOG.error("Agent runtime error; tearing down", t);
            try { sync.stop(); } catch (Throwable ignored) {}
            try { health.stop(); } catch (Throwable ignored) {}
            try { writer.close(); } catch (Throwable ignored) {}
            throw t;
        }

        LOG.info("ranger-yunikorn-agent shut down cleanly");
    }

    /**
     * Log the resolved configuration at startup so operational debugging
     * doesn't require pawing through the source for what gets defaulted.
     * Sensitive fields are masked.
     */
    private static void logEffectiveConfig(AgentConfig c) {
        LOG.info("Effective configuration:");
        LOG.info("  ranger.admin.url       = {}", c.rangerAdminUrl());
        LOG.info("  ranger.service.name    = {}", c.rangerServiceName());
        LOG.info("  ranger.service.type    = {}", c.rangerServiceType());
        LOG.info("  ranger.auth.mode       = {}", c.authMode());
        if (c.authMode() == AgentConfig.AuthMode.BASIC) {
            LOG.info("  ranger.auth            = {}",
                    c.hasRangerCredentials() ? "basic (user='" + c.rangerUser() + "')" : "none");
        } else {
            LOG.info("  kerberos.principal     = {}", c.kerberosPrincipal());
            LOG.info("  kerberos.keytab.path   = {}", c.kerberosKeytabPath());
            LOG.info("  kerberos.krb5.conf     = {}",
                    c.krb5ConfigPath() == null ? "(JVM default)" : c.krb5ConfigPath());
            LOG.info("  java.security.auth.login.config = {}",
                    System.getProperty("java.security.auth.login.config", "(not set)"));
            LOG.info("  javax.security.auth.useSubjectCredsOnly = {}",
                    System.getProperty("javax.security.auth.useSubjectCredsOnly", "(default=true)"));
        }
        if (c.rangerAdminUrl() != null && c.rangerAdminUrl().toLowerCase().startsWith("https")) {
            LOG.info("  ranger.ssl.insecure    = {}", c.rangerSslInsecure());
            LOG.info("  ranger.ssl.truststore  = {}{}",
                    c.rangerSslTruststorePath() == null ? "(JVM default)" : c.rangerSslTruststorePath(),
                    c.rangerSslTruststorePath() == null ? "" : " [" + c.rangerSslTruststoreType() + "]");
        }
        LOG.info("  yunikorn.namespace     = {}", c.yunikornNamespace());
        LOG.info("  yunikorn.configmap     = {}", c.yunikornConfigMap());
        LOG.info("  yunikorn.conf.key      = {}", c.yunikornConfKey());
        LOG.info("  yunikorn.rest.url      = {}",
                c.yunikornRestUrl() == null ? "(not set)" : c.yunikornRestUrl());
        LOG.info("  preflight.enabled      = {}", c.preflightEnabled());
        LOG.info("  sync.poll.interval.ms  = {}", c.pollIntervalMs());
        LOG.info("  sync.force.resync.ms   = {}", c.forceResyncMs());
        LOG.info("  doctrine               = {}", c.doctrine());
        LOG.info("  policy.cache.dir       = {}", c.policyCacheDir());
    }

    private RangerYuniKornAgent() {
        // entry-point class
    }
}
