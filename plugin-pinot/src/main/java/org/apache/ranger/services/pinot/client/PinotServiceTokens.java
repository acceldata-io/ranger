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

import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Pinot service tokens for Ranger Admin's calls to the Pinot controllers, from Pinot's own token sources (any source
 * Pinot supports: client_credentials, google_service_account, http, java), so that Ranger signs in exactly like Pinot's
 * own calls do, with the settings Ambari copies from Pinot's configuration.
 *
 * Pinot's token-source code and its few dependencies ship in the "pinot-auth" directory next to this plug-in's jar,
 * outside the plug-in's class path, and are loaded in their own class loader. The token is fetched by a child process
 * run with Pinot's Java ("java.home" setting), the settings passed on its standard input so that secrets never appear
 * on a command line, when that cannot work inside Ranger Admin's JVM: Pinot needs Java 17, and settings Ranger Admin
 * applies to the whole JVM can break Java's defaults Pinot's HTTP client relies on (e.g. javax.net.ssl.trustStore
 * naming a missing file: no default SSL context, even for an http URL).
 */
final class PinotServiceTokens {
    static final String ENTRY_CLASS      = "org.apache.pinot.plugin.auth.oauth2.ServiceTokens";
    static final String JARS_DIR         = "pinot-auth";
    static final String JARS_DIR_PROPERTY = "ranger.pinot.auth.jars.dir"; // tests, or a non-standard layout
    static final long   CHILD_TIMEOUT_SECONDS = 60;

    private static ClassLoader loader;
    private static File        loaderDir;

    private PinotServiceTokens() {
    }

    /**
     * @param settings the token source settings, as in Pinot (source, token.url, client.id, client.secret, ...)
     * @param javaHome a Java 17+ installation for runtimes older than Pinot's, or null
     * @return an access token
     */
    static String fetch(Map<String, String> settings, String javaHome) throws IOException {
        String source = StringUtils.defaultIfBlank(settings.get("source"), "client_credentials");
        File   dir    = jarsDir();
        File[] jars   = dir.listFiles((d, name) -> name.endsWith(".jar"));

        if (jars == null || jars.length == 0) {
            throw new IOException("Pinot service token (source " + source + "): Pinot's token-source jars are missing in " + dir.getAbsolutePath());
        }

        try {
            return fetchInProcess(dir, jars, settings);
        } catch (UnsupportedClassVersionError e) {
            if (StringUtils.isBlank(javaHome)) {
                throw new IOException("Pinot service token (source " + source + "): Pinot's token sources need Java 17 or later, Ranger Admin runs on Java "
                        + System.getProperty("java.specification.version") + " and no Java home for Pinot is set ('" + PinotAdminClient.CONFIG_OAUTH2_SOURCE_JAVA_HOME + "')");
            }

            return fetchInChildProcess(dir, settings, javaHome.trim(), source);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;

            // Ranger Admin's JVM, not the provider, refused (security or SSL setup): a JVM of its own has Java's defaults
            if (StringUtils.isNotBlank(javaHome) && isJvmEnvironmentProblem(cause)) {
                return fetchInChildProcess(dir, settings, javaHome.trim(), source);
            }

            throw new IOException("Pinot service token (source " + source + "): " + cause.getMessage(), cause);
        } catch (ReflectiveOperationException | LinkageError e) {
            throw new IOException("Pinot service token (source " + source + "): cannot run Pinot's token sources from " + dir.getAbsolutePath() + ": " + e, e);
        }
    }

    /** A failure of the JVM's security or SSL setup (e.g. no default SSL context), anywhere in the cause chain. */
    static boolean isJvmEnvironmentProblem(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof java.security.GeneralSecurityException || t instanceof LinkageError) {
                return true;
            }
        }

        return false;
    }

    static File jarsDir() {
        String configured = System.getProperty(JARS_DIR_PROPERTY);

        if (StringUtils.isNotBlank(configured)) {
            return new File(configured.trim());
        }

        try {
            File pluginJar = new File(PinotServiceTokens.class.getProtectionDomain().getCodeSource().getLocation().toURI());

            return new File(pluginJar.getParentFile(), JARS_DIR);
        } catch (Exception e) {
            return new File(JARS_DIR);
        }
    }

    private static String fetchInProcess(File dir, File[] jars, Map<String, String> settings) throws ReflectiveOperationException {
        ClassLoader cl       = classLoader(dir, jars);
        Thread      thread   = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();

        thread.setContextClassLoader(cl);

        try {
            return (String) Class.forName(ENTRY_CLASS, true, cl).getMethod("fetch", Map.class).invoke(null, settings);
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    /** One class loader per directory for the life of Ranger Admin: Pinot's classes are loaded once. */
    private static synchronized ClassLoader classLoader(File dir, File[] jars) {
        if (loader == null || !dir.equals(loaderDir)) {
            Arrays.sort(jars);

            List<URL> urls = new ArrayList<>();

            for (File jar : jars) {
                try {
                    urls.add(jar.toURI().toURL());
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException("Bad jar path " + jar, e);
                }
            }

            // Parent: the Java platform only, so that Ranger Admin's own libraries never mix with Pinot's
            loader    = new URLClassLoader(urls.toArray(new URL[0]), platformClassLoader());
            loaderDir = dir;
        }

        return loader;
    }

    private static ClassLoader platformClassLoader() {
        try {
            return (ClassLoader) ClassLoader.class.getMethod("getPlatformClassLoader").invoke(null);
        } catch (ReflectiveOperationException e) {
            return null; // Java 8: the bootstrap class loader holds the whole platform
        }
    }

    static String fetchInChildProcess(File dir, Map<String, String> settings, String javaHome, String source) throws IOException {
        File java = new File(new File(javaHome, "bin"), "java");

        if (!java.canExecute()) {
            throw new IOException("Pinot service token (source " + source + "): no Java at " + java.getAbsolutePath() + " on the Ranger Admin host");
        }

        Process process = new ProcessBuilder(java.getAbsolutePath(), "-cp", new File(dir, "*").getAbsolutePath(), ENTRY_CLASS).start();

        try {
            Properties properties = new Properties();

            properties.putAll(settings);

            try (OutputStream in = process.getOutputStream()) {
                properties.store(in, null);
            }

            // Standard error on its own thread: a full pipe would otherwise block the child
            StringBuilder err    = new StringBuilder();
            Thread        reader = new Thread(() -> {
                try {
                    err.append(read(process.getErrorStream()));
                } catch (IOException ignored) {
                    // the reason is then unknown
                }
            }, "pinot-service-token-stderr");

            reader.setDaemon(true);
            reader.start();

            String out = read(process.getInputStream());

            reader.join(TimeUnit.SECONDS.toMillis(CHILD_TIMEOUT_SECONDS));

            if (!process.waitFor(CHILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IOException("Pinot service token (source " + source + "): no token within " + CHILD_TIMEOUT_SECONDS + " seconds");
            }

            if (process.exitValue() != 0 || out.trim().isEmpty()) {
                throw new IOException("Pinot service token (source " + source + "): " + lastLine(err.toString()));
            }

            return out.trim();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IOException("Pinot service token (source " + source + "): interrupted", e);
        } finally {
            process.destroy();
        }
    }

    private static String read(InputStream in) throws IOException {
        try (InputStream is = in) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[]                buf = new byte[8192];
            int                   n;

            while ((n = is.read(buf)) > 0) {
                out.write(buf, 0, n);
            }

            return new String(out.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private static String lastLine(String text) {
        String[] lines = text.trim().split("\n");

        return lines.length == 0 || lines[lines.length - 1].trim().isEmpty() ? "the token source failed" : lines[lines.length - 1].trim();
    }
}
