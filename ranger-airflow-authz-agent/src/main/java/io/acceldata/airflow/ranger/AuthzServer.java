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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Embedded Jetty bound to loopback. Four contract endpoints; {@code /v1/filter}
 * is M2 and returns 404 until then.
 */
public final class AuthzServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AuthzServer.class);

    static final String AGENT_VERSION = "1.0.0";
    private static final int MAX_CHECKS = 1000;
    private static final long UNMAPPED_LOG_INTERVAL_MS = 3_600_000L;

    private final AgentConfig config;
    private final AuthzEngine engine;
    private final ObjectMapper mapper;
    private final ConcurrentHashMap<String, Long> unmappedLogTimes = new ConcurrentHashMap<>();
    private Server server;

    public AuthzServer(AgentConfig config, AuthzEngine engine) {
        this.config = Objects.requireNonNull(config, "config");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public synchronized void start() throws Exception {
        if (server != null) {
            return;
        }
        QueuedThreadPool pool = new QueuedThreadPool(config.threadPoolSize());
        pool.setName("airflow-authz");
        server = new Server(pool);

        ServerConnector connector = new ServerConnector(server);
        connector.setHost(config.bindAddress());
        connector.setPort(config.port());
        server.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new V1Servlet()), "/v1/*");
        server.setHandler(context);
        server.start();
        LOG.info("authz agent listening on {}:{}", config.bindAddress(), boundPort());
    }

    public synchronized void stop() {
        if (server == null) {
            return;
        }
        try {
            server.stop();
        } catch (Exception e) {
            LOG.warn("error stopping HTTP server", e);
        } finally {
            server = null;
        }
    }

    @Override
    public void close() {
        stop();
    }

    public int boundPort() {
        if (server == null) {
            return -1;
        }
        return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    private final class V1Servlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String path = subPath(req);
            if ("/health".equals(path)) {
                writeJson(resp, 200, "{\"status\":\"ok\"}");
                return;
            }
            if ("/ready".equals(path)) {
                if (engine.isReady()) {
                    writeJson(resp, 200, "{\"ready\":true}");
                } else {
                    writeJson(resp, 503, mapper.writeValueAsString(
                            new ReadyBody(false, engine.notReadyReason())));
                }
                return;
            }
            if ("/info".equals(path)) {
                if (!requireSecret(req, resp)) {
                    return;
                }
                writeJson(resp, 200, mapper.writeValueAsString(infoBody()));
                return;
            }
            writeJson(resp, 404, "{\"error\":\"not_found\"}");
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String path = subPath(req);
            if ("/authorize".equals(path)) {
                if (!requireSecret(req, resp)) {
                    return;
                }
                handleAuthorize(req, resp);
                return;
            }
            if ("/filter".equals(path)) {
                if (!requireSecret(req, resp)) {
                    return;
                }
                writeJson(resp, 404, "{\"error\":\"not_implemented\"}");
                return;
            }
            writeJson(resp, 404, "{\"error\":\"not_found\"}");
        }

        private boolean requireSecret(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            if (config.sharedSecret().matches(req.getHeader("Authorization"))) {
                return true;
            }
            writeJson(resp, 401, "{\"error\":\"unauthorized\"}");
            return false;
        }

        private void handleAuthorize(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            if (!engine.isReady()) {
                writeJson(resp, 503, mapper.writeValueAsString(
                        new ReadyBody(false, engine.notReadyReason())));
                return;
            }
            AuthorizeRequest body;
            try {
                body = mapper.readValue(req.getInputStream(), AuthorizeRequest.class);
            } catch (Exception e) {
                writeJson(resp, 400, "{\"error\":\"bad_request\",\"message\":\"malformed JSON\"}");
                return;
            }
            String problem = validate(body);
            if (problem != null) {
                writeJson(resp, 400, mapper.writeValueAsString(new ErrorBody("bad_request", problem)));
                return;
            }
            try {
                String user = IdentityNormalizer.normalize(body.user);
                List<Decision> decisions = new ArrayList<>(body.checks.size());
                for (Check check : body.checks) {
                    AccessMapper.Result mapped = AccessMapper.map(
                            check.resource_type, check.method, check.access_entity);
                    if (mapped.status == AccessMapper.Status.UNKNOWN_VOCABULARY) {
                        writeJson(resp, 422, mapper.writeValueAsString(
                                new ErrorBody("unknown_vocabulary", mapped.detail)));
                        return;
                    }
                    String id = check.id == null ? "" : check.id;
                    if (mapped.status == AccessMapper.Status.UNMAPPED) {
                        warnUnmapped(mapped.detail);
                        decisions.add(Decision.denied(id, "unmapped_access"));
                        continue;
                    }
                    RangerAccessRequestImpl rangerReq = buildRequest(user, body.context, mapped, check.key);
                    RangerAccessResult result = engine.evaluate(rangerReq);
                    decisions.add(toDecision(id, result));
                }
                AuthorizeResponse out = new AuthorizeResponse();
                out.policy_version = engine.policyVersion();
                out.decisions = decisions;
                writeJson(resp, 200, mapper.writeValueAsString(out));
            } catch (Exception e) {
                LOG.error("authorize failed", e);
                writeJson(resp, 500, "{\"error\":\"internal\"}");
            }
        }
    }

    private RangerAccessRequestImpl buildRequest(String user, RequestContext ctx,
                                                 AccessMapper.Result mapped, String key) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        if (key != null && !key.isBlank()) {
            resource.setValue(mapped.resourceType, key.strip());
        }
        RangerAccessRequestImpl request = new RangerAccessRequestImpl(
                resource, mapped.accessType, user, Collections.emptySet(), null);
        if (ctx != null) {
            request.setClientIPAddress(ctx.client_ip);
            request.setRemoteIPAddress(ctx.client_ip);
            request.setRequestData(ctx.request_uri);
            if (ctx.request_id != null && !ctx.request_id.isBlank()) {
                request.setSessionId(ctx.request_id);
            }
        }
        request.setClusterName(config.clusterName());
        request.setClientType("airflow");
        request.setAction(mapped.accessType);
        request.setAccessTime(new Date());
        return request;
    }

    private static Decision toDecision(String id, RangerAccessResult result) {
        boolean allowed = result != null && result.getIsAllowed();
        long policyId = result == null ? -1 : result.getPolicyId();
        if (allowed) {
            return Decision.allowed(id, policyId > 0 ? policyId : null);
        }
        String reason = policyId > 0 ? "explicit_deny" : "no_matching_policy";
        return Decision.denied(id, reason, policyId > 0 ? policyId : null);
    }

    private static String validate(AuthorizeRequest body) {
        if (body == null) {
            return "request body is required";
        }
        if (body.user == null || body.user.isBlank()) {
            return "user is required";
        }
        if (body.context == null || body.context.client_ip == null || body.context.client_ip.isBlank()
                || body.context.request_uri == null || body.context.request_uri.isBlank()) {
            return "context.client_ip and context.request_uri are required";
        }
        if (body.checks == null || body.checks.isEmpty()) {
            return "checks must contain at least one entry";
        }
        if (body.checks.size() > MAX_CHECKS) {
            return "checks exceeds " + MAX_CHECKS;
        }
        return null;
    }

    private InfoBody infoBody() {
        InfoBody info = new InfoBody();
        info.agent_version = AGENT_VERSION;
        info.contract_version = AgentConfig.CONTRACT_VERSION;
        info.ranger_service = engine.serviceName();
        info.service_def_version = engine.serviceDefVersion();
        info.supported_airflow = config.supportedAirflow();
        return info;
    }

    private void warnUnmapped(String combo) {
        long now = System.currentTimeMillis();
        Long previous = unmappedLogTimes.get(combo);
        if (previous != null && now - previous < UNMAPPED_LOG_INTERVAL_MS) {
            return;
        }
        unmappedLogTimes.put(combo, now);
        LOG.warn("unmapped access combination denied: {}", combo);
    }

    private static String subPath(HttpServletRequest req) {
        String path = req.getPathInfo();
        return path == null ? "" : path;
    }

    private static void writeJson(HttpServletResponse resp, int status, String json) throws IOException {
        resp.setStatus(status);
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json");
        resp.getWriter().write(json);
    }

    // --- wire types (Airflow vocabulary; names match the contract JSON) -----

    public static final class AuthorizeRequest {
        public String user;
        public RequestContext context;
        public List<Check> checks;
    }

    public static final class RequestContext {
        public String client_ip;
        public String request_uri;
        public String request_id;
    }

    public static final class Check {
        public String id;
        public String resource_type;
        public String method;
        public String access_entity;
        public String key;
    }

    public static final class AuthorizeResponse {
        public long policy_version;
        public List<Decision> decisions;
    }

    public static final class Decision {
        public String id;
        public boolean allowed;
        public Long policy_id;
        public String reason;

        static Decision allowed(String id, Long policyId) {
            Decision d = new Decision();
            d.id = id;
            d.allowed = true;
            d.policy_id = policyId;
            return d;
        }

        static Decision denied(String id, String reason) {
            return denied(id, reason, null);
        }

        static Decision denied(String id, String reason, Long policyId) {
            Decision d = new Decision();
            d.id = id;
            d.allowed = false;
            d.reason = reason;
            d.policy_id = policyId;
            return d;
        }
    }

    public static final class InfoBody {
        public String agent_version;
        public String contract_version;
        public String ranger_service;
        public Integer service_def_version;
        public String supported_airflow;
    }

    public static final class ReadyBody {
        public boolean ready;
        public String reason;
        ReadyBody(boolean ready, String reason) {
            this.ready = ready;
            this.reason = reason;
        }
    }

    public static final class ErrorBody {
        public String error;
        public String message;
        ErrorBody(String error, String message) {
            this.error = error;
            this.message = message;
        }
    }
}
