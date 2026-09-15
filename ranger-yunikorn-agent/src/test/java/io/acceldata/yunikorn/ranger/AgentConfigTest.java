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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AgentConfigTest {

    private static Function<String, String> emptyEnv() {
        return key -> null;
    }

    private static Function<String, String> envWith(String... kv) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i], kv[i + 1]);
        }
        return map::get;
    }

    private static Properties baseProps() {
        Properties p = new Properties();
        p.setProperty("ranger.admin.url",     "https://ranger.example.com:6182");
        p.setProperty("ranger.service.name",  "test-yunikorn");
        return p;
    }

    @Nested
    @DisplayName("Required values")
    class Required {

        @Test
        @DisplayName("missing ranger.admin.url fails fast")
        void missingAdminUrl() {
            Properties p = baseProps();
            p.remove("ranger.admin.url");
            assertThatThrownBy(() -> AgentConfig.fromProperties(p, emptyEnv()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ranger.admin.url");
        }

        @Test
        @DisplayName("missing ranger.service.name fails fast")
        void missingServiceName() {
            Properties p = baseProps();
            p.remove("ranger.service.name");
            assertThatThrownBy(() -> AgentConfig.fromProperties(p, emptyEnv()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ranger.service.name");
        }

        @Test
        @DisplayName("blank required values rejected")
        void blankRequired() {
            Properties p = baseProps();
            p.setProperty("ranger.admin.url", "   ");
            assertThatThrownBy(() -> AgentConfig.fromProperties(p, emptyEnv()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("Default values")
    class Defaults {

        @Test
        @DisplayName("only required properties → all defaults populated")
        void allDefaults() {
            AgentConfig c = AgentConfig.fromProperties(baseProps(), emptyEnv());

            assertThat(c.yunikornNamespace()).isEqualTo("yunikorn");
            assertThat(c.yunikornConfigMap()).isEqualTo("yunikorn-configs");
            assertThat(c.yunikornConfKey()).isEqualTo("queues.yaml");
            assertThat(c.yunikornRestUrl()).isNull();
            assertThat(c.preflightEnabled()).isTrue();
            assertThat(c.pollIntervalMs()).isEqualTo(30_000L);
            assertThat(c.forceResyncMs()).isEqualTo(86_400_000L);
            assertThat(c.doctrine()).isEqualTo(UnmanagedDoctrine.LENIENT);
            assertThat(c.policyCacheDir()).isEqualTo("/var/cache/ranger-yunikorn-agent");
        }

        @Test
        @DisplayName("preflight.enabled=false honored")
        void preflightDisabled() {
            Properties p = baseProps();
            p.setProperty("preflight.enabled", "false");
            assertThat(AgentConfig.fromProperties(p, emptyEnv()).preflightEnabled()).isFalse();
        }

        @Test
        @DisplayName("doctrine=STRICT honored")
        void strictDoctrine() {
            Properties p = baseProps();
            p.setProperty("doctrine", "STRICT");
            assertThat(AgentConfig.fromProperties(p, emptyEnv()).doctrine())
                    .isEqualTo(UnmanagedDoctrine.STRICT);
        }

        @Test
        @DisplayName("doctrine accepts mixed case")
        void doctrineCaseInsensitive() {
            Properties p = baseProps();
            p.setProperty("doctrine", "strict");
            assertThat(AgentConfig.fromProperties(p, emptyEnv()).doctrine())
                    .isEqualTo(UnmanagedDoctrine.STRICT);
        }

        @Test
        @DisplayName("invalid doctrine value rejected")
        void invalidDoctrine() {
            Properties p = baseProps();
            p.setProperty("doctrine", "WHATEVER");
            assertThatThrownBy(() -> AgentConfig.fromProperties(p, emptyEnv()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("doctrine");
        }

        @Test
        @DisplayName("non-numeric poll interval rejected")
        void invalidPollInterval() {
            Properties p = baseProps();
            p.setProperty("sync.poll.interval.ms", "fast");
            assertThatThrownBy(() -> AgentConfig.fromProperties(p, emptyEnv()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sync.poll.interval.ms");
        }

        @Test
        @DisplayName("zero poll interval rejected")
        void zeroPollInterval() {
            Properties p = baseProps();
            p.setProperty("sync.poll.interval.ms", "0");
            assertThatThrownBy(() -> AgentConfig.fromProperties(p, emptyEnv()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sync.poll.interval.ms");
        }
    }

    @Nested
    @DisplayName("Credentials")
    class Credentials {

        @Test
        @DisplayName("neither RANGER_PRINCIPAL nor RANGER_CREDENTIAL → no creds")
        void noCredentials() {
            AgentConfig c = AgentConfig.fromProperties(baseProps(), emptyEnv());
            assertThat(c.hasRangerCredentials()).isFalse();
            assertThat(c.rangerUser()).isNull();
            assertThat(c.rangerPassword()).isNull();
        }

        @Test
        @DisplayName("both env vars set → creds populated")
        void bothCredentials() {
            AgentConfig c = AgentConfig.fromProperties(baseProps(),
                    envWith("RANGER_PRINCIPAL", "svc-yunikorn",
                            "RANGER_CREDENTIAL", "s3cret"));
            assertThat(c.hasRangerCredentials()).isTrue();
            assertThat(c.rangerUser()).isEqualTo("svc-yunikorn");
            assertThat(c.rangerPassword()).isEqualTo("s3cret");
        }

        @Test
        @DisplayName("principal set, credential missing → fail fast")
        void principalOnly() {
            assertThatThrownBy(() ->
                    AgentConfig.fromProperties(baseProps(),
                            envWith("RANGER_PRINCIPAL", "svc-yunikorn")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("RANGER_PRINCIPAL")
                    .hasMessageContaining("RANGER_CREDENTIAL");
        }

        @Test
        @DisplayName("credential set, principal missing → fail fast")
        void credentialOnly() {
            assertThatThrownBy(() ->
                    AgentConfig.fromProperties(baseProps(),
                            envWith("RANGER_CREDENTIAL", "s3cret")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("RANGER_PRINCIPAL")
                    .hasMessageContaining("RANGER_CREDENTIAL");
        }

        @Test
        @DisplayName("blank env values treated as unset")
        void blankCredentialsTreatedAsUnset() {
            AgentConfig c = AgentConfig.fromProperties(baseProps(),
                    envWith("RANGER_PRINCIPAL", "  ",
                            "RANGER_CREDENTIAL", ""));
            assertThat(c.hasRangerCredentials()).isFalse();
        }
    }

    @Nested
    @DisplayName("Kerberos auth mode")
    class Kerberos {

        @Test
        @DisplayName("auth.mode=basic by default")
        void defaultAuthMode() {
            AgentConfig c = AgentConfig.fromProperties(baseProps(), emptyEnv());
            assertThat(c.authMode()).isEqualTo(AgentConfig.AuthMode.BASIC);
        }

        @Test
        @DisplayName("auth.mode=kerberos requires principal + keytab path")
        void kerberosRequiresPrincipalAndKeytab() {
            Properties p = baseProps();
            p.setProperty("ranger.auth.mode", "kerberos");

            assertThatThrownBy(() -> AgentConfig.fromProperties(p, emptyEnv()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("kerberos.principal");

            p.setProperty("kerberos.principal", "agent/host@REALM");
            assertThatThrownBy(() -> AgentConfig.fromProperties(p, emptyEnv()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("kerberos.keytab.path");

            p.setProperty("kerberos.keytab.path", "/etc/keytab");
            AgentConfig c = AgentConfig.fromProperties(p, emptyEnv());
            assertThat(c.authMode()).isEqualTo(AgentConfig.AuthMode.KERBEROS);
            assertThat(c.kerberosPrincipal()).isEqualTo("agent/host@REALM");
            assertThat(c.kerberosKeytabPath()).isEqualTo("/etc/keytab");
        }

        @Test
        @DisplayName("auth.mode=kerberos accepts principal from RANGER_PRINCIPAL env")
        void kerberosPrincipalFromEnv() {
            Properties p = baseProps();
            p.setProperty("ranger.auth.mode", "kerberos");
            p.setProperty("kerberos.keytab.path", "/etc/keytab");

            AgentConfig c = AgentConfig.fromProperties(p,
                    envWith("RANGER_PRINCIPAL", "agent/host@REALM"));
            assertThat(c.kerberosPrincipal()).isEqualTo("agent/host@REALM");
        }

        @Test
        @DisplayName("auth.mode=kerberos sets krb5.conf path when provided")
        void kerberosKrb5ConfPath() {
            Properties p = baseProps();
            p.setProperty("ranger.auth.mode", "kerberos");
            p.setProperty("kerberos.principal", "agent/host@REALM");
            p.setProperty("kerberos.keytab.path", "/etc/keytab");
            p.setProperty("kerberos.krb5.conf.path", "/etc/krb5/krb5.conf");

            AgentConfig c = AgentConfig.fromProperties(p, emptyEnv());
            assertThat(c.krb5ConfigPath()).isEqualTo("/etc/krb5/krb5.conf");
        }

        @Test
        @DisplayName("invalid auth.mode value rejected")
        void invalidAuthMode() {
            Properties p = baseProps();
            p.setProperty("ranger.auth.mode", "oauth2");
            assertThatThrownBy(() -> AgentConfig.fromProperties(p, emptyEnv()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ranger.auth.mode");
        }
    }

    @Nested
    @DisplayName("Overrides")
    class Overrides {

        @Test
        @DisplayName("custom namespace and configmap honored")
        void customConfigMapTarget() {
            Properties p = baseProps();
            p.setProperty("yunikorn.namespace", "scheduler");
            p.setProperty("yunikorn.configmap", "my-yunikorn-cm");
            p.setProperty("yunikorn.conf.key",  "config.yaml");

            AgentConfig c = AgentConfig.fromProperties(p, emptyEnv());
            assertThat(c.yunikornNamespace()).isEqualTo("scheduler");
            assertThat(c.yunikornConfigMap()).isEqualTo("my-yunikorn-cm");
            assertThat(c.yunikornConfKey()).isEqualTo("config.yaml");
        }

        @Test
        @DisplayName("custom poll interval and resync interval")
        void customCadence() {
            Properties p = baseProps();
            p.setProperty("sync.poll.interval.ms", "5000");
            p.setProperty("sync.force.resync.ms", "3600000");
            AgentConfig c = AgentConfig.fromProperties(p, emptyEnv());
            assertThat(c.pollIntervalMs()).isEqualTo(5_000L);
            assertThat(c.forceResyncMs()).isEqualTo(3_600_000L);
        }

        @Test
        @DisplayName("ranger.service.type defaults to 'yunikorn'")
        void serviceTypeDefault() {
            AgentConfig c = AgentConfig.fromProperties(baseProps(), emptyEnv());
            assertThat(c.rangerServiceType()).isEqualTo("yunikorn");
        }

        @Test
        @DisplayName("ranger.service.type can be overridden (e.g. yunikorn2)")
        void serviceTypeOverride() {
            Properties p = baseProps();
            p.setProperty("ranger.service.type", "yunikorn2");
            assertThat(AgentConfig.fromProperties(p, emptyEnv()).rangerServiceType())
                    .isEqualTo("yunikorn2");
        }

        @Test
        @DisplayName("custom policy cache dir")
        void customCacheDir() {
            Properties p = baseProps();
            p.setProperty("policy.cache.dir", "/tmp/agent-cache");
            assertThat(AgentConfig.fromProperties(p, emptyEnv()).policyCacheDir())
                    .isEqualTo("/tmp/agent-cache");
        }

        @Test
        @DisplayName("yunikorn rest URL is optional")
        void yunikornRestOptional() {
            Properties p = baseProps();
            p.setProperty("yunikorn.rest.url", "http://yunikorn-service.yunikorn:9080");
            assertThat(AgentConfig.fromProperties(p, emptyEnv()).yunikornRestUrl())
                    .isEqualTo("http://yunikorn-service.yunikorn:9080");
        }
    }

    @Nested
    @DisplayName("Ranger TLS")
    class Tls {

        @Test
        @DisplayName("no TLS config → all defaults / disabled")
        void noTlsConfig() {
            AgentConfig c = AgentConfig.fromProperties(baseProps(), emptyEnv());
            assertThat(c.rangerSslInsecure()).isFalse();
            assertThat(c.rangerSslTruststorePath()).isNull();
            assertThat(c.rangerSslTruststorePassword()).isNull();
            assertThat(c.rangerSslTruststoreType()).isEqualTo("JKS");
        }

        @Test
        @DisplayName("insecure flag parsed")
        void insecureFlag() {
            Properties p = baseProps();
            p.setProperty("ranger.ssl.insecure", "true");
            assertThat(AgentConfig.fromProperties(p, emptyEnv()).rangerSslInsecure()).isTrue();
        }

        @Test
        @DisplayName("truststore type defaults to JKS, override honoured")
        void truststoreType() {
            assertThat(AgentConfig.fromProperties(baseProps(), emptyEnv()).rangerSslTruststoreType())
                    .isEqualTo("JKS");

            Properties p = baseProps();
            p.setProperty("ranger.ssl.truststore.type", "pkcs12");
            assertThat(AgentConfig.fromProperties(p, emptyEnv()).rangerSslTruststoreType())
                    .isEqualTo("PKCS12");
        }

        @Test
        @DisplayName("truststore password read from env only (no property fallback)")
        void truststorePasswordFromEnv() {
            Properties p = baseProps();
            p.setProperty("ranger.ssl.truststore.path", "/etc/ranger-tls/truststore.jks");

            // A property is NOT a source for the password — env only.
            p.setProperty("ranger.ssl.truststore.password", "ignored-property");
            assertThat(AgentConfig.fromProperties(p, emptyEnv()).rangerSslTruststorePassword())
                    .isNull();

            assertThat(AgentConfig.fromProperties(p,
                    envWith("RANGER_SSL_TRUSTSTORE_PASSWORD", "from-env")).rangerSslTruststorePassword())
                    .isEqualTo("from-env");
        }
    }
}
