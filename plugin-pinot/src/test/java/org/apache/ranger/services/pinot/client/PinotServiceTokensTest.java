/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.services.pinot.client;

import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test Connection with Pinot's own token sources (any source but client_credentials): Pinot's entry point (a
 * stand-in here) is loaded from the jars directory next to the plug-in in its own class loader, or run in a child
 * process on an older Java runtime.
 */
public class PinotServiceTokensTest {
    private static final String ENTRY = "org/apache/pinot/plugin/auth/oauth2/ServiceTokens.class";

    private static File jarsDir;

    private HttpServer         server;
    private String             base;
    private final List<String> authorizations = new CopyOnWriteArrayList<>();

    @BeforeClass
    public static void jars() throws IOException {
        // Pinot's token-source jars as they ship next to the plug-in: the stand-in entry point in a jar of its own
        jarsDir = Files.createTempDirectory("pinot-auth").toFile();

        try (InputStream in = PinotServiceTokensTest.class.getClassLoader().getResourceAsStream(ENTRY);
                JarOutputStream jar = new JarOutputStream(Files.newOutputStream(new File(jarsDir, "pinot-auth-oauth2.jar").toPath()))) {
            jar.putNextEntry(new JarEntry(ENTRY));

            byte[] buf = new byte[8192];
            int    n;

            while ((n = in.read(buf)) > 0) {
                jar.write(buf, 0, n);
            }

            jar.closeEntry();
        }

        System.setProperty(PinotServiceTokens.JARS_DIR_PROPERTY, jarsDir.getAbsolutePath());
    }

    @AfterClass
    public static void clear() {
        System.clearProperty(PinotServiceTokens.JARS_DIR_PROPERTY);
    }

    @Before
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);

        // A controller: /health is public, everything else needs the token of the stand-in source
        server.createContext("/", exchange -> {
            String  auth = String.valueOf(exchange.getRequestHeaders().getFirst("Authorization"));
            boolean ok   = "Bearer tok-1".equals(auth) || "/health".equals(exchange.getRequestURI().getPath());
            byte[]  body = (ok ? "{\"tables\":[]}" : "{\"code\":401}").getBytes(StandardCharsets.UTF_8);

            authorizations.add(auth);
            exchange.sendResponseHeaders(ok ? 200 : 401, body.length);

            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();

        base = "http://localhost:" + server.getAddress().getPort();
    }

    @After
    public void stop() {
        server.stop(0);
    }

    private Map<String, String> javaSource(String secret) {
        Map<String, String> configs = new HashMap<>();

        configs.put(PinotAdminClient.CONFIG_CONTROLLER_URL, base);
        configs.put(PinotAdminClient.CONFIG_AUTHENTICATION_TYPE, "oauth2");
        configs.put(PinotAdminClient.CONFIG_OAUTH2_SOURCE, "java");
        configs.put(PinotAdminClient.CONFIG_OAUTH2_SOURCE_SETTINGS, "# in-house provider\nclass = com.example.IdmTokenSource\njar.dir=/opt/idm\n");
        configs.put(PinotAdminClient.CONFIG_OAUTH2_TOKEN_URL, "https://idm.example.com/v2/token");
        configs.put(PinotAdminClient.CONFIG_OAUTH2_CLIENT_ID, "pinot-service");
        configs.put(PinotAdminClient.CONFIG_OAUTH2_CLIENT_SECRET, secret);

        return configs;
    }

    @Test
    public void settingsOfPinotsTokenSource() {
        Map<String, String> settings = PinotAdminClient.sourceSettings(javaSource("s3cret"));

        assertEquals("java", settings.get("source"));
        assertEquals("com.example.IdmTokenSource", settings.get("class"));
        assertEquals("/opt/idm", settings.get("jar.dir"));
        assertEquals("https://idm.example.com/v2/token", settings.get("token.url"));
        assertEquals("pinot-service", settings.get("client.id"));
        assertEquals("s3cret", settings.get("client.secret"));
        assertFalse(settings.containsKey("# in-house provider"));
    }

    @Test
    public void testConnectionWithAJavaTokenSource() throws IOException {
        new PinotAdminClient(javaSource("s3cret")).testConnection();

        assertEquals("Bearer tok-1", authorizations.get(authorizations.size() - 1));
    }

    @Test
    public void wrongSecretGivesPinotsReasonWithoutTheSecret() {
        try {
            new PinotAdminClient(javaSource("wrong-secret")).testConnection();
            fail("connected with a wrong secret");
        } catch (IOException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Pinot service token (source java)") && e.getMessage().contains("401"));
            assertFalse(e.getMessage().contains("wrong-secret"));
        }
    }

    @Test
    public void missingJarsAreNamed() {
        String dir = System.getProperty(PinotServiceTokens.JARS_DIR_PROPERTY);

        System.setProperty(PinotServiceTokens.JARS_DIR_PROPERTY, new File(jarsDir, "absent").getAbsolutePath());

        try {
            new PinotAdminClient(javaSource("s3cret")).testConnection();
            fail("connected without Pinot's jars");
        } catch (IOException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("jars are missing"));
        } finally {
            System.setProperty(PinotServiceTokens.JARS_DIR_PROPERTY, dir);
        }
    }

    @Test
    public void childProcessOnAnOlderJavaRuntime() throws IOException {
        // The path taken when Ranger Admin's Java cannot load Pinot's classes: Pinot's Java runs the entry point
        Map<String, String> settings = PinotAdminClient.sourceSettings(javaSource("s3cret"));
        String              javaHome = System.getProperty("java.home");

        assertEquals("tok-1", PinotServiceTokens.fetchInChildProcess(jarsDir, settings, javaHome, "java"));

        settings.put("client.secret", "wrong-secret");

        try {
            PinotServiceTokens.fetchInChildProcess(jarsDir, settings, javaHome, "java");
            fail("token with a wrong secret");
        } catch (IOException e) {
            assertTrue(e.getMessage(), e.getMessage().endsWith("Token source java: token endpoint answered 401"));
        }

        try {
            PinotServiceTokens.fetchInChildProcess(jarsDir, settings, "/no/such/java", "java");
            fail("ran without Java");
        } catch (IOException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("no Java at /no/such/java"));
        }
    }

    @Test
    public void brokenSslDefaultsOfRangerAdminFallBackToPinotsJava() throws IOException {
        Map<String, String> configs = javaSource("s3cret");

        System.setProperty("test.broken.ssl.defaults", "true");

        try {
            // Without Pinot's Java home: the reason is reported
            try {
                new PinotAdminClient(configs).testConnection();
                fail("connected with broken SSL defaults and no Java home");
            } catch (IOException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("NoSuchAlgorithmException"));
            }

            // With it: a JVM of its own fetches the token
            configs.put(PinotAdminClient.CONFIG_OAUTH2_SOURCE_JAVA_HOME, System.getProperty("java.home"));
            new PinotAdminClient(configs).testConnection();

            assertEquals("Bearer tok-1", authorizations.get(authorizations.size() - 1));
        } finally {
            System.clearProperty("test.broken.ssl.defaults");
        }
    }

    @Test
    public void providerRefusalsAreNotRetried() {
        assertFalse(PinotServiceTokens.isJvmEnvironmentProblem(new IllegalStateException("Token source java: token endpoint answered 401")));
        assertTrue(PinotServiceTokens.isJvmEnvironmentProblem(new IllegalStateException(new java.security.KeyStoreException("missing"))));
    }
}
