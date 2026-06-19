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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.function.Function;

/**
 * Resolved, validated runtime configuration for the agent.
 *
 * <p>Loaded from a Java Properties file (typically mounted into the pod via a
 * K8s ConfigMap at {@code /etc/ranger-yunikorn-agent/agent.properties}) with
 * a few credential-shaped fields read from environment variables (sourced
 * from a K8s Secret).
 *
 * <p>Required values fail fast at startup with a clear error message rather
 * than producing surprising behaviour later. Optional values get sensible
 * defaults.
 *
 * <h2>Why properties + env vars instead of a single source</h2>
 * Non-secret config (Ranger Admin URL, service name, poll interval, doctrine)
 * lives in a ConfigMap so operators can edit it via {@code kubectl edit cm}
 * without touching pod state. Secrets (Ranger user, password) live in a K8s
 * Secret so they get the right access controls and aren't accidentally
 * exposed in {@code kubectl describe} output.
 *
 * <h2>Threading</h2>
 * Immutable. Safe to share across threads.
 */
public final class AgentConfig {

    // --- Required ----------------------------------------------------------
    private final String rangerAdminUrl;
    private final String rangerServiceName;

    /**
     * Ranger service-TYPE (not the service-name instance). Must match the
     * {@code "name"} field of the service-def JSON registered in Ranger Admin.
     * Defaults to {@code "yunikorn"}; can be overridden when an installation
     * has the def registered under a different name (e.g. {@code "yunikorn2"}).
     */
    private final String rangerServiceType;

    // --- Auth mode and credentials -----------------------------------------
    private final AuthMode authMode;

    // Basic auth — used when authMode == BASIC. Null when not configured.
    private final String rangerUser;
    private final String rangerPassword;

    // Kerberos — used when authMode == KERBEROS.
    private final String kerberosPrincipal;        // null unless KERBEROS
    private final String kerberosKeytabPath;       // null unless KERBEROS
    private final String krb5ConfigPath;           // null unless KERBEROS

    // --- TLS to Ranger Admin (https) ---------------------------------------
    // All optional. When none are set the JVM default TLS behaviour is used.
    private final boolean rangerSslInsecure;            // skip cert + hostname verification (DEV ONLY)
    private final String  rangerSslTruststorePath;      // CA/cert chain trusting Ranger's server cert
    private final String  rangerSslTruststoreType;      // JKS (default) | PKCS12
    private final String  rangerSslTruststorePassword;  // from RANGER_SSL_TRUSTSTORE_PASSWORD env

    // --- ConfigMap target --------------------------------------------------
    private final String yunikornNamespace;
    private final String yunikornConfigMap;
    private final String yunikornConfKey;

    // --- Optional preflight ------------------------------------------------
    private final String  yunikornRestUrl;
    private final boolean preflightEnabled;

    // --- Sync cadence ------------------------------------------------------
    private final long pollIntervalMs;
    private final long forceResyncMs;

    // --- Doctrine ----------------------------------------------------------
    private final UnmanagedDoctrine doctrine;

    // --- Local policy cache (Ranger admin client writes here) --------------
    private final String policyCacheDir;

    private AgentConfig(Builder b) {
        this.rangerAdminUrl     = b.rangerAdminUrl;
        this.rangerServiceName  = b.rangerServiceName;
        this.rangerServiceType  = b.rangerServiceType;
        this.authMode           = b.authMode;
        this.rangerUser         = b.rangerUser;
        this.rangerPassword     = b.rangerPassword;
        this.kerberosPrincipal  = b.kerberosPrincipal;
        this.kerberosKeytabPath = b.kerberosKeytabPath;
        this.krb5ConfigPath     = b.krb5ConfigPath;
        this.rangerSslInsecure            = b.rangerSslInsecure;
        this.rangerSslTruststorePath      = b.rangerSslTruststorePath;
        this.rangerSslTruststoreType      = b.rangerSslTruststoreType;
        this.rangerSslTruststorePassword  = b.rangerSslTruststorePassword;
        this.yunikornNamespace  = b.yunikornNamespace;
        this.yunikornConfigMap  = b.yunikornConfigMap;
        this.yunikornConfKey    = b.yunikornConfKey;
        this.yunikornRestUrl    = b.yunikornRestUrl;
        this.preflightEnabled   = b.preflightEnabled;
        this.pollIntervalMs     = b.pollIntervalMs;
        this.forceResyncMs      = b.forceResyncMs;
        this.doctrine           = b.doctrine;
        this.policyCacheDir     = b.policyCacheDir;
    }

    // -----------------------------------------------------------------------
    // Loaders
    // -----------------------------------------------------------------------

    /**
     * Load from a properties file, with credentials read from environment
     * variables. Equivalent to {@code load(path, System::getenv)}.
     */
    public static AgentConfig load(String propertiesPath) throws IOException {
        return load(propertiesPath, System::getenv);
    }

    /**
     * Visible for testing. Lets callers inject a fake {@code env} lookup so
     * tests don't need to set real environment variables.
     */
    public static AgentConfig load(String propertiesPath,
                                   Function<String, String> env) throws IOException {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(propertiesPath)) {
            props.load(in);
        }
        return fromProperties(props, env);
    }

    /** Build directly from an in-memory {@link Properties}. */
    static AgentConfig fromProperties(Properties props, Function<String, String> env) {
        Builder b = new Builder();

        b.rangerAdminUrl    = required(props, "ranger.admin.url");
        b.rangerServiceName = required(props, "ranger.service.name");
        b.rangerServiceType = props.getProperty("ranger.service.type", "yunikorn").trim();

        // Auth mode: BASIC (default) or KERBEROS. The two modes use entirely
        // different code paths and entirely different config — see comments
        // in RangerAdminClientFactory for the architectural why.
        String authModeStr = props.getProperty("ranger.auth.mode", "basic").trim().toUpperCase();
        try {
            b.authMode = AuthMode.valueOf(authModeStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "ranger.auth.mode must be one of " + java.util.Arrays.toString(AuthMode.values()) +
                    ", got: " + authModeStr);
        }

        if (b.authMode == AuthMode.BASIC) {
            // Basic-auth credentials are OPTIONAL. If both are present we
            // use them; if both are absent we send no auth; if exactly one
            // is present we fail fast so the operator notices the
            // misconfiguration rather than silently sending no auth in
            // production.
            String user = nullIfBlank(env.apply("RANGER_PRINCIPAL"));
            String pass = nullIfBlank(env.apply("RANGER_CREDENTIAL"));
            if (user == null ^ pass == null) {
                throw new IllegalArgumentException(
                        "RANGER_PRINCIPAL and RANGER_CREDENTIAL must be set together. " +
                        "Set both for basic auth or neither for unauthenticated access.");
            }
            b.rangerUser         = user;
            b.rangerPassword     = pass;
            b.kerberosPrincipal  = null;
            b.kerberosKeytabPath = null;
            b.krb5ConfigPath     = null;
        } else {
            // Kerberos: principal + keytab path are mandatory.
            // krb5.conf path is mandatory unless the JVM already has one
            // wired up via -Djava.security.krb5.conf or /etc/krb5.conf.
            b.kerberosPrincipal = nullIfBlank(props.getProperty("kerberos.principal"));
            if (b.kerberosPrincipal == null) {
                b.kerberosPrincipal = nullIfBlank(env.apply("RANGER_PRINCIPAL"));
            }
            b.kerberosKeytabPath = nullIfBlank(props.getProperty("kerberos.keytab.path"));
            b.krb5ConfigPath     = nullIfBlank(props.getProperty("kerberos.krb5.conf.path"));

            if (b.kerberosPrincipal == null) {
                throw new IllegalArgumentException(
                        "ranger.auth.mode=kerberos requires kerberos.principal " +
                        "(in agent.properties) or RANGER_PRINCIPAL (env var). " +
                        "Example: ranger-yunikorn-agent/agent-host@YOUR.REALM");
            }
            if (b.kerberosKeytabPath == null) {
                throw new IllegalArgumentException(
                        "ranger.auth.mode=kerberos requires kerberos.keytab.path " +
                        "in agent.properties. Example: /etc/security/keytabs/agent.keytab");
            }
            // Basic-auth fields stay null
            b.rangerUser     = null;
            b.rangerPassword = null;
        }

        b.yunikornNamespace = props.getProperty("yunikorn.namespace", "yunikorn");
        b.yunikornConfigMap = props.getProperty("yunikorn.configmap", "yunikorn-configs");
        b.yunikornConfKey   = props.getProperty("yunikorn.conf.key",  "queues.yaml");

        b.yunikornRestUrl  = nullIfBlank(props.getProperty("yunikorn.rest.url"));
        b.preflightEnabled = Boolean.parseBoolean(
                props.getProperty("preflight.enabled", "true"));

        b.pollIntervalMs = parseLongOr(props, "sync.poll.interval.ms", 30_000L);
        b.forceResyncMs  = parseLongOr(props, "sync.force.resync.ms", 86_400_000L);

        if (b.pollIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "sync.poll.interval.ms must be positive, got " + b.pollIntervalMs);
        }

        b.doctrine = parseDoctrineOr(props, "doctrine", UnmanagedDoctrine.LENIENT);

        b.policyCacheDir = props.getProperty(
                "policy.cache.dir", "/var/cache/ranger-yunikorn-agent");

        // --- TLS to Ranger Admin (when ranger.admin.url is https) ----------
        // The truststore file is referenced by path (mounted into the pod);
        // its password (if any) comes from the RANGER_SSL_TRUSTSTORE_PASSWORD
        // env var, sourced from a K8s Secret.
        b.rangerSslInsecure = Boolean.parseBoolean(
                props.getProperty("ranger.ssl.insecure", "false"));
        b.rangerSslTruststorePath = nullIfBlank(props.getProperty("ranger.ssl.truststore.path"));
        b.rangerSslTruststoreType = props.getProperty("ranger.ssl.truststore.type", "JKS")
                .trim().toUpperCase();
        b.rangerSslTruststorePassword = nullIfBlank(env.apply("RANGER_SSL_TRUSTSTORE_PASSWORD"));

        return new AgentConfig(b);
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    public String   rangerAdminUrl()    { return rangerAdminUrl; }
    public String   rangerServiceName() { return rangerServiceName; }
    public String   rangerServiceType() { return rangerServiceType; }
    public AuthMode authMode()          { return authMode; }

    // Basic-auth accessors
    public String  rangerUser()           { return rangerUser; }
    public String  rangerPassword()       { return rangerPassword; }
    public boolean hasRangerCredentials() { return rangerUser != null; }

    // Kerberos accessors
    public String kerberosPrincipal()  { return kerberosPrincipal; }
    public String kerberosKeytabPath() { return kerberosKeytabPath; }
    public String krb5ConfigPath()     { return krb5ConfigPath; }

    // TLS accessors
    public boolean rangerSslInsecure()            { return rangerSslInsecure; }
    public String  rangerSslTruststorePath()      { return rangerSslTruststorePath; }
    public String  rangerSslTruststoreType()      { return rangerSslTruststoreType; }
    public String  rangerSslTruststorePassword()  { return rangerSslTruststorePassword; }

    public String yunikornNamespace() { return yunikornNamespace; }
    public String yunikornConfigMap() { return yunikornConfigMap; }
    public String yunikornConfKey()   { return yunikornConfKey; }

    public String  yunikornRestUrl()  { return yunikornRestUrl; }
    public boolean preflightEnabled() { return preflightEnabled; }

    public long pollIntervalMs() { return pollIntervalMs; }
    public long forceResyncMs()  { return forceResyncMs; }

    public UnmanagedDoctrine doctrine() { return doctrine; }
    public String policyCacheDir()      { return policyCacheDir; }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static String required(Properties p, String key) {
        String v = nullIfBlank(p.getProperty(key));
        if (v == null) {
            throw new IllegalArgumentException(
                    "Missing required property '" + key + "' in agent.properties");
        }
        return v;
    }

    private static String nullIfBlank(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }

    private static long parseLongOr(Properties p, String key, long defaultValue) {
        String v = nullIfBlank(p.getProperty(key));
        if (v == null) return defaultValue;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Property '" + key + "' must be a long, got: " + v);
        }
    }

    private static UnmanagedDoctrine parseDoctrineOr(Properties p, String key,
                                                     UnmanagedDoctrine defaultValue) {
        String v = nullIfBlank(p.getProperty(key));
        if (v == null) return defaultValue;
        try {
            return UnmanagedDoctrine.valueOf(v.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Property 'doctrine' must be LENIENT or STRICT, got: " + v);
        }
    }

    /** Internal mutable holder used while parsing. */
    private static final class Builder {
        String rangerAdminUrl;
        String rangerServiceName;
        String rangerServiceType;
        AuthMode authMode;
        String rangerUser;
        String rangerPassword;
        String kerberosPrincipal;
        String kerberosKeytabPath;
        String krb5ConfigPath;
        boolean rangerSslInsecure;
        String rangerSslTruststorePath;
        String rangerSslTruststoreType;
        String rangerSslTruststorePassword;
        String yunikornNamespace;
        String yunikornConfigMap;
        String yunikornConfKey;
        String yunikornRestUrl;
        boolean preflightEnabled;
        long pollIntervalMs;
        long forceResyncMs;
        UnmanagedDoctrine doctrine;
        String policyCacheDir;
    }

    /**
     * Authentication mode the agent uses to connect to Ranger Admin.
     *
     * <ul>
     *   <li>{@link #BASIC} — sends {@code Authorization: Basic} headers.
     *       Uses the agent's own {@code RangerHttpClient}. No Hadoop UGI,
     *       no Kerberos. Default. Works against any Ranger that accepts
     *       basic auth on the {@code /service/plugins/secure/...} path.</li>
     *
     *   <li>{@link #KERBEROS} — uses Hadoop UGI with a keytab login.
     *       Routes through the official {@code RangerAdminRESTClient},
     *       which gets us delta protocol, on-disk policy cache, and HA URL
     *       support for free. Requires a working KDC and a keytab mounted
     *       into the pod.</li>
     * </ul>
     */
    public enum AuthMode {
        BASIC,
        KERBEROS
    }
}
