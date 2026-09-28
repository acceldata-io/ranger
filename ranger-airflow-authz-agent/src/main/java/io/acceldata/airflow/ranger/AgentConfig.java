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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

/**
 * Resolved runtime configuration for the agent. Immutable and safe to share
 * across threads. Invalid values fail at load, not at first request.
 */
public final class AgentConfig {

    public static final String CONTRACT_VERSION = "v1";
    public static final String DEFAULT_CONFIG_PATH =
            "/etc/ranger-airflow-authz-agent/conf/authz-agent.properties";

    private final String bindAddress;
    private final int port;
    private final SharedSecret sharedSecret;
    private final String clusterName;
    private final int threadPoolSize;
    private final String serviceType;
    private final String appId;
    private final String supportedAirflow;

    AgentConfig(String bindAddress, int port, SharedSecret sharedSecret, String clusterName,
                int threadPoolSize, String serviceType, String appId, String supportedAirflow) {
        this.bindAddress = requireLoopback(bindAddress);
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        this.port = port;
        this.sharedSecret = Objects.requireNonNull(sharedSecret, "sharedSecret");
        this.clusterName = clusterName == null ? "" : clusterName;
        if (threadPoolSize < 1) {
            throw new IllegalArgumentException("thread pool size must be >= 1");
        }
        this.threadPoolSize = threadPoolSize;
        this.serviceType = requireText(serviceType, "authz.agent.service.type");
        this.appId = requireText(appId, "authz.agent.app.id");
        this.supportedAirflow = requireText(supportedAirflow, "authz.agent.supported.airflow");
    }

    public static AgentConfig load(Path propertiesFile) {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(propertiesFile)) {
            props.load(in);
        } catch (IOException e) {
            throw new IllegalArgumentException("cannot read " + propertiesFile + ": " + e.getMessage(), e);
        }
        Path tokenFile = Path.of(required(props, "authz.agent.token.file"));
        return new AgentConfig(
                props.getProperty("authz.agent.bind.address", "127.0.0.1"),
                Integer.parseInt(props.getProperty("authz.agent.port", "9183")),
                SharedSecret.fromFile(tokenFile),
                props.getProperty("authz.agent.cluster.name", ""),
                Integer.parseInt(props.getProperty("authz.agent.thread.pool.size", "16")),
                props.getProperty("authz.agent.service.type", "airflow"),
                props.getProperty("authz.agent.app.id", "airflow"),
                props.getProperty("authz.agent.supported.airflow", ">=3.2,<3.3"));
    }

    public String bindAddress() { return bindAddress; }
    public int port() { return port; }
    public SharedSecret sharedSecret() { return sharedSecret; }
    public String clusterName() { return clusterName; }
    public int threadPoolSize() { return threadPoolSize; }
    public String serviceType() { return serviceType; }
    public String appId() { return appId; }
    public String supportedAirflow() { return supportedAirflow; }

    private static String required(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required property " + key);
        }
        return value.strip();
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.strip();
    }

    static String requireLoopback(String bindAddress) {
        String trimmed = requireText(bindAddress, "authz.agent.bind.address");
        try {
            InetAddress addr = InetAddress.getByName(trimmed);
            if (!addr.isLoopbackAddress()) {
                throw new IllegalArgumentException(
                        "bind address must be loopback (127.0.0.1 / ::1), not " + trimmed);
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid bind address " + trimmed + ": " + e.getMessage(), e);
        }
        return trimmed;
    }
}
