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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Posts a candidate {@code queues.yaml} to YuniKorn's
 * {@code POST /ws/v1/validate-conf} endpoint and reports whether YuniKorn
 * would accept it.
 *
 * <h2>The contract (YuniKorn REST API)</h2>
 * <ul>
 *   <li><b>Request:</b> {@code POST <restUrl>/ws/v1/validate-conf} with the
 *       raw scheduler-config YAML as the request body. YuniKorn reads the
 *       body verbatim and runs it through {@code LoadSchedulerConfigFromByteArray}
 *       — i.e. exactly the same parse + validation the scheduler does on a
 *       real ConfigMap reload, but without applying anything.</li>
 *   <li><b>Response:</b> always {@code 200} with a JSON body
 *       {@code {"allowed": <bool>, "reason": "<string>"}}. {@code allowed=false}
 *       carries the validation error in {@code reason}.</li>
 * </ul>
 *
 * <h2>Why preflight at all</h2>
 * The {@link AclSplicer} guarantees structural ACL correctness, but it cannot
 * know YuniKorn's full schema (queue resource arithmetic, placement rules,
 * limits, reserved names). Without preflight, a config that splices cleanly
 * but violates a YuniKorn rule only fails when k8shim reloads — after we've
 * already overwritten the live ConfigMap. Validating the spliced bytes
 * against the real scheduler validator first means a bad document never
 * reaches the cluster.
 *
 * <h2>Fail-closed</h2>
 * Any outcome other than an explicit {@code allowed=true} raises
 * {@link PreflightException}, which aborts the write:
 * <ul>
 *   <li>{@code allowed=false} → rejection, with YuniKorn's reason.</li>
 *   <li>non-200 status → treated as "couldn't validate".</li>
 *   <li>connection/timeout error → treated as "couldn't validate".</li>
 * </ul>
 * The sync service logs and retries next cycle, so a transient YuniKorn REST
 * blip pauses ACL propagation rather than risking a bad write.
 *
 * <h2>Threading</h2>
 * Stateless aside from immutable config + a thread-safe {@link ObjectMapper}.
 * Safe to share.
 */
public class YuniKornPreflightClient implements PreflightValidator {

    private static final Logger LOG = LoggerFactory.getLogger(YuniKornPreflightClient.class);

    static final String VALIDATE_CONF_PATH = "/ws/v1/validate-conf";

    static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
    static final int DEFAULT_READ_TIMEOUT_MS    = 10_000;

    private final String validateConfUrl;
    private final int    connectTimeoutMs;
    private final int    readTimeoutMs;
    private final ObjectMapper mapper;

    /**
     * Build the validator from agent config, or return {@code null} when
     * preflight should not run.
     *
     * <ul>
     *   <li>{@code preflight.enabled=false} → {@code null} (skip silently;
     *       the operator opted out).</li>
     *   <li>{@code preflight.enabled=true} but no {@code yunikorn.rest.url} →
     *       {@code null} with a loud WARN. We can't validate without an
     *       endpoint, and we refuse to pretend we did.</li>
     * </ul>
     *
     * @return a configured client, or {@code null} if preflight is not active.
     */
    static PreflightValidator forConfig(AgentConfig config) {
        if (!config.preflightEnabled()) {
            LOG.info("Preflight validation disabled (preflight.enabled=false); "
                    + "spliced ConfigMaps will be written without a validate-conf check.");
            return null;
        }
        String restUrl = config.yunikornRestUrl();
        if (restUrl == null) {
            LOG.warn("preflight.enabled=true but yunikorn.rest.url is not set — preflight "
                    + "validation will be SKIPPED. Set yunikorn.rest.url (e.g. "
                    + "http://yunikorn-service.yunikorn.svc:9080) to enable validate-conf, "
                    + "or set preflight.enabled=false to silence this warning.");
            return null;
        }
        LOG.info("Preflight validation enabled; candidate configs will be checked against {}{}",
                restUrl.replaceAll("/+$", ""), VALIDATE_CONF_PATH);
        return new YuniKornPreflightClient(restUrl);
    }

    public YuniKornPreflightClient(String restUrl) {
        this(restUrl, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
    }

    YuniKornPreflightClient(String restUrl, int connectTimeoutMs, int readTimeoutMs) {
        if (restUrl == null || restUrl.isBlank()) {
            throw new IllegalArgumentException("restUrl must not be null/blank");
        }
        this.validateConfUrl  = restUrl.replaceAll("/+$", "") + VALIDATE_CONF_PATH;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs    = readTimeoutMs;
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                // YuniKorn's DAO serialises Go fields; tolerate either casing
                // (allowed/Allowed) across versions.
                .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
    }

    @Override
    public void validate(String queuesYaml) throws PreflightException {
        byte[] body = queuesYaml.getBytes(StandardCharsets.UTF_8);

        HttpURLConnection conn;
        try {
            conn = (HttpURLConnection) URI.create(validateConfUrl).toURL().openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(connectTimeoutMs);
            conn.setReadTimeout(readTimeoutMs);
            conn.setDoOutput(true);
            // YuniKorn reads the body as raw bytes; content-type is advisory.
            conn.setRequestProperty("Content-Type", "application/x-yaml; charset=utf-8");
            conn.setRequestProperty("Accept", "application/json");
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body);
            }
        } catch (IOException e) {
            throw new PreflightException(
                    "Could not reach YuniKorn validate-conf at " + validateConfUrl +
                    " — refusing to write an unvalidated config. " +
                    "If YuniKorn has no reachable REST endpoint, set preflight.enabled=false.", e);
        }

        int status;
        try {
            status = conn.getResponseCode();
        } catch (IOException e) {
            throw new PreflightException(
                    "No response from YuniKorn validate-conf at " + validateConfUrl, e);
        }

        if (status != 200) {
            throw new PreflightException(
                    "YuniKorn validate-conf returned HTTP " + status + " from " + validateConfUrl +
                    " — body: " + truncate(readBody(conn, status), 500));
        }

        ValidateConfResponse result;
        try (InputStream in = conn.getInputStream()) {
            result = mapper.readValue(in, ValidateConfResponse.class);
        } catch (IOException e) {
            throw new PreflightException(
                    "Failed to parse YuniKorn validate-conf response from " + validateConfUrl, e);
        }

        if (result == null) {
            throw new PreflightException(
                    "YuniKorn validate-conf returned an empty response from " + validateConfUrl);
        }
        if (!result.allowed) {
            throw new PreflightException(
                    "YuniKorn rejected the spliced queues.yaml: " +
                    (result.reason == null || result.reason.isBlank()
                            ? "(no reason provided)" : result.reason));
        }
        LOG.info("Preflight OK — YuniKorn accepted the candidate config.");
    }

    private static String readBody(HttpURLConnection conn, int status) {
        try (InputStream in = (status >= 400) ? conn.getErrorStream() : conn.getInputStream()) {
            if (in == null) return "";
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Throwable t) {
            return "<failed to read body: " + t.getMessage() + ">";
        }
    }

    private static String truncate(String s, int max) {
        return (s == null || s.length() <= max) ? s : s.substring(0, max) + "...";
    }

    /** Minimal view of YuniKorn's {@code dao.ValidateConfResponse}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class ValidateConfResponse {
        public boolean allowed;
        public String  reason;
    }
}
