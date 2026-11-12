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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Function;

/**
 * Validated, immutable runtime configuration for {@code ranger-mlflow-agent}.
 */
public final class AgentConfig {

    /**
     * Reserved prefix in MLflow 3.16.0 ({@code SqlAlchemyStore._SYNTHETIC_ROLE_PREFIX}).
     * MLflow rejects any role CRUD using this prefix.
     */
    public static final String MLFLOW_SYNTHETIC_ROLE_PREFIX = "__user_";

    public static final String DEFAULT_MANAGED_ROLE_PREFIX = "ranger_";
    public static final String DEFAULT_WORKSPACE_NAME      = "default";

    private final String   rangerAdminUrl;
    private final String   rangerServiceName;
    private final String   rangerServiceType;
    private final AuthMode authMode;
    private final String   rangerUser;
    private final String   rangerPassword;
    private final String   kerberosPrincipal;
    private final String   kerberosKeytabPath;
    private final String   krb5ConfigPath;

    private final boolean rangerSslInsecure;
    private final String  rangerSslTruststorePath;
    private final String  rangerSslTruststoreType;
    private final String  rangerSslTruststorePassword;

    private final String  mlflowUrl;
    private final String  mlflowAdminUsername;
    private final String  mlflowAdminPassword;
    private final String  mlflowDefaultWorkspace;
    private final String  managedRolePrefix;
    private final boolean autoCreateUsers;

    private final long              pollIntervalMs;
    private final long              forceResyncMs;
    private final UnmanagedDoctrine doctrine;
    private final int               healthPort;

    private AgentConfig(Builder b) {
        this.rangerAdminUrl              = b.rangerAdminUrl;
        this.rangerServiceName           = b.rangerServiceName;
        this.rangerServiceType           = b.rangerServiceType;
        this.authMode                    = b.authMode;
        this.rangerUser                  = b.rangerUser;
        this.rangerPassword              = b.rangerPassword;
        this.kerberosPrincipal           = b.kerberosPrincipal;
        this.kerberosKeytabPath          = b.kerberosKeytabPath;
        this.krb5ConfigPath              = b.krb5ConfigPath;
        this.rangerSslInsecure           = b.rangerSslInsecure;
        this.rangerSslTruststorePath     = b.rangerSslTruststorePath;
        this.rangerSslTruststoreType     = b.rangerSslTruststoreType;
        this.rangerSslTruststorePassword = b.rangerSslTruststorePassword;
        this.mlflowUrl                   = b.mlflowUrl;
        this.mlflowAdminUsername         = b.mlflowAdminUsername;
        this.mlflowAdminPassword         = b.mlflowAdminPassword;
        this.mlflowDefaultWorkspace      = b.mlflowDefaultWorkspace;
        this.managedRolePrefix           = b.managedRolePrefix;
        this.autoCreateUsers             = b.autoCreateUsers;
        this.pollIntervalMs              = b.pollIntervalMs;
        this.forceResyncMs               = b.forceResyncMs;
        this.doctrine                    = b.doctrine;
        this.healthPort                  = b.healthPort;
    }

    public static AgentConfig load(String propertiesPath) throws IOException {
        return load(propertiesPath, System::getenv);
    }

    public static AgentConfig load(String propertiesPath,
                                   Function<String, String> env) throws IOException {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(propertiesPath)) {
            props.load(in);
        }
        return fromProperties(props, env);
    }

    static AgentConfig fromProperties(Properties props, Function<String, String> env) {
        Builder b = new Builder();

        b.rangerAdminUrl    = required(props, "ranger.admin.url");
        b.rangerServiceName = required(props, "ranger.service.name");
        b.rangerServiceType = props.getProperty("ranger.service.type", "mlflow").trim();

        String authModeStr = props.getProperty("ranger.auth.mode", "basic").trim().toUpperCase();
        try {
            b.authMode = AuthMode.valueOf(authModeStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "ranger.auth.mode must be one of " + Arrays.toString(AuthMode.values())
                    + ", got: " + authModeStr);
        }

        if (b.authMode == AuthMode.BASIC) {
            String user = firstNonBlank(env.apply("RANGER_PRINCIPAL"), props.getProperty("ranger.username"));
            String pass = firstNonBlank(env.apply("RANGER_CREDENTIAL"), props.getProperty("ranger.password"));
            if (user == null ^ pass == null) {
                throw new IllegalArgumentException(
                        "Ranger basic auth username and password must be set together "
                        + "(via RANGER_PRINCIPAL/RANGER_CREDENTIAL or ranger.username/ranger.password).");
            }
            b.rangerUser     = user;
            b.rangerPassword = pass;
        } else {
            b.kerberosPrincipal  = firstNonBlank(props.getProperty("kerberos.principal"), env.apply("RANGER_PRINCIPAL"));
            b.kerberosKeytabPath = nullIfBlank(props.getProperty("kerberos.keytab.path"));
            b.krb5ConfigPath     = nullIfBlank(props.getProperty("kerberos.krb5.conf.path"));
            if (b.kerberosPrincipal == null) {
                throw new IllegalArgumentException(
                        "ranger.auth.mode=kerberos requires kerberos.principal or RANGER_PRINCIPAL");
            }
            if (b.kerberosKeytabPath == null) {
                throw new IllegalArgumentException(
                        "ranger.auth.mode=kerberos requires kerberos.keytab.path");
            }
        }

        b.mlflowUrl = required(props, "mlflow.url");
        b.mlflowAdminUsername = firstNonBlank(
                env.apply("MLFLOW_ADMIN_USERNAME"),
                props.getProperty("mlflow.admin.username"));
        b.mlflowAdminPassword = firstNonBlank(
                env.apply("MLFLOW_ADMIN_PASSWORD"),
                props.getProperty("mlflow.admin.password"));
        if (b.mlflowAdminUsername == null || b.mlflowAdminPassword == null) {
            throw new IllegalArgumentException(
                    "MLflow admin credentials are required to manage MLflow native RBAC. "
                    + "Set MLFLOW_ADMIN_USERNAME and MLFLOW_ADMIN_PASSWORD (or mlflow.admin.username / mlflow.admin.password).");
        }

        b.mlflowDefaultWorkspace = props.getProperty("mlflow.default.workspace", DEFAULT_WORKSPACE_NAME).trim();
        if (b.mlflowDefaultWorkspace.isEmpty()) {
            throw new IllegalArgumentException("mlflow.default.workspace must not be blank");
        }

        b.managedRolePrefix = props.getProperty("mlflow.managed.role.prefix", DEFAULT_MANAGED_ROLE_PREFIX).trim();
        if (b.managedRolePrefix.isEmpty()) {
            throw new IllegalArgumentException("mlflow.managed.role.prefix must not be blank");
        }
        if (b.managedRolePrefix.startsWith(MLFLOW_SYNTHETIC_ROLE_PREFIX)) {
            throw new IllegalArgumentException(
                    "mlflow.managed.role.prefix cannot start with '" + MLFLOW_SYNTHETIC_ROLE_PREFIX
                    + "' (reserved by MLflow 3.16.0 for per-user synthetic roles).");
        }

        b.autoCreateUsers = Boolean.parseBoolean(props.getProperty("mlflow.auto.create.users", "false"));

        b.pollIntervalMs = parseLongOr(props, "sync.poll.interval.ms", 30_000L);
        b.forceResyncMs  = parseLongOr(props, "sync.force.resync.ms", 86_400_000L);
        if (b.pollIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "sync.poll.interval.ms must be positive, got " + b.pollIntervalMs);
        }

        b.doctrine   = parseDoctrineOr(props, "doctrine", UnmanagedDoctrine.LENIENT);
        b.healthPort = (int) parseLongOr(props, "health.port", 8081L);

        b.rangerSslInsecure = Boolean.parseBoolean(props.getProperty("ranger.ssl.insecure", "false"));
        b.rangerSslTruststorePath = nullIfBlank(props.getProperty("ranger.ssl.truststore.path"));
        b.rangerSslTruststoreType = props.getProperty("ranger.ssl.truststore.type", "JKS").trim().toUpperCase();
        b.rangerSslTruststorePassword = firstNonBlank(
                env.apply("RANGER_SSL_TRUSTSTORE_PASSWORD"),
                props.getProperty("ranger.ssl.truststore.password"));

        return new AgentConfig(b);
    }

    public String   rangerAdminUrl()              { return rangerAdminUrl; }
    public String   rangerServiceName()           { return rangerServiceName; }
    public String   rangerServiceType()           { return rangerServiceType; }
    public AuthMode authMode()                    { return authMode; }
    public String   rangerUser()                  { return rangerUser; }
    public String   rangerPassword()              { return rangerPassword; }
    public boolean  hasRangerCredentials()        { return rangerUser != null; }
    public String   kerberosPrincipal()           { return kerberosPrincipal; }
    public String   kerberosKeytabPath()          { return kerberosKeytabPath; }
    public String   krb5ConfigPath()              { return krb5ConfigPath; }
    public boolean  rangerSslInsecure()           { return rangerSslInsecure; }
    public String   rangerSslTruststorePath()     { return rangerSslTruststorePath; }
    public String   rangerSslTruststoreType()     { return rangerSslTruststoreType; }
    public String   rangerSslTruststorePassword() { return rangerSslTruststorePassword; }
    public String   mlflowUrl()                   { return mlflowUrl; }
    public String   mlflowAdminUsername()         { return mlflowAdminUsername; }
    public String   mlflowAdminPassword()         { return mlflowAdminPassword; }
    public String   mlflowDefaultWorkspace()      { return mlflowDefaultWorkspace; }
    public String   managedRolePrefix()           { return managedRolePrefix; }
    public boolean  autoCreateUsers()             { return autoCreateUsers; }
    public long     pollIntervalMs()              { return pollIntervalMs; }
    public long     forceResyncMs()               { return forceResyncMs; }
    public UnmanagedDoctrine doctrine()           { return doctrine; }
    public int      healthPort()                  { return healthPort; }

    private static String required(Properties p, String key) {
        String v = nullIfBlank(p.getProperty(key));
        if (v == null) {
            throw new IllegalArgumentException(
                    "Missing required property '" + key + "' in agent.properties");
        }
        return v;
    }

    private static String firstNonBlank(String first, String second) {
        String a = nullIfBlank(first);
        return a != null ? a : nullIfBlank(second);
    }

    private static String nullIfBlank(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }

    private static long parseLongOr(Properties p, String key, long defaultValue) {
        String v = nullIfBlank(p.getProperty(key));
        if (v == null) {
            return defaultValue;
        }
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
        if (v == null) {
            return defaultValue;
        }
        try {
            return UnmanagedDoctrine.valueOf(v.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Property 'doctrine' must be LENIENT or STRICT, got: " + v);
        }
    }

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
        String mlflowUrl;
        String mlflowAdminUsername;
        String mlflowAdminPassword;
        String mlflowDefaultWorkspace;
        String managedRolePrefix;
        boolean autoCreateUsers;
        long pollIntervalMs;
        long forceResyncMs;
        UnmanagedDoctrine doctrine;
        int healthPort;
    }

    public enum AuthMode {
        BASIC,
        KERBEROS
    }
}
