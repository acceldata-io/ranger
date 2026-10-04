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
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
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

    /**
     * v1 endpoints this build actually implements. The client checks this rather
     * than inferring the endpoint set from {@code contract_version} alone, so a
     * build that predates {@code /v1/filter} is detected at startup instead of
     * on the first grid load. Add {@code "filter"} in M2.
     */
    static final List<String> CAPABILITIES = List.of("authorize", "filter");

    private static final int MAX_CHECKS = 1000;
    private static final int MAX_KEYS   = 5000;
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
                handleFilter(req, resp);
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

                // Pass 1 - map every check before evaluating any of them. Unknown
                // vocabulary fails the whole request, and it has to fail before the
                // engine runs: RangerDefaultAuditHandler writes a record per
                // evaluation, so mapping and evaluating in one loop leaves audit
                // rows behind for a request that returned no decisions, and
                // duplicates them when the client retries.
                List<AccessMapper.Result> mappings = new ArrayList<>(body.checks.size());
                for (Check check : body.checks) {
                    AccessMapper.Result mapped = AccessMapper.map(
                            check.resource_type, check.method, check.access_entity);
                    if (mapped.status == AccessMapper.Status.UNKNOWN_VOCABULARY) {
                        writeJson(resp, 422, mapper.writeValueAsString(
                                new ErrorBody("unknown_vocabulary", mapped.detail)));
                        return;
                    }
                    mappings.add(mapped);
                }

                // Pass 2 - evaluate. Past this point every check produces a
                // decision and an audit record, and the response is always 200.
                List<Decision> decisions = new ArrayList<>(body.checks.size());
                for (int i = 0; i < body.checks.size(); i++) {
                    Check check = body.checks.get(i);
                    AccessMapper.Result mapped = mappings.get(i);
                    String id = check.id == null ? "" : check.id;
                    if (mapped.status == AccessMapper.Status.UNMAPPED) {
                        warnUnmapped(mapped.detail);
                        // Audit only: there is no Ranger access type to honor.
                        // HTTP reason stays unmapped_access regardless of the engine result.
                        engine.evaluate(buildUnmappedRequest(user, body.context, check));
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

        /**
         * One question shape, many candidate keys, the permitted subset out.
         *
         * <p>Audited as a single event, never one per key: the keys that do not
         * pass are not access attempts. A page was opened and Airflow asked
         * about everything on it, so recording hundreds of denials would be
         * both voluminous and untrue.
         */
        private void handleFilter(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            if (!engine.isReady()) {
                writeJson(resp, 503, mapper.writeValueAsString(
                        new ReadyBody(false, engine.notReadyReason())));
                return;
            }
            FilterRequest body;
            try {
                body = mapper.readValue(req.getInputStream(), FilterRequest.class);
            } catch (Exception e) {
                writeJson(resp, 400, "{\"error\":\"bad_request\",\"message\":\"malformed JSON\"}");
                return;
            }
            String problem = validateFilter(body);
            if (problem != null) {
                writeJson(resp, 400, mapper.writeValueAsString(new ErrorBody("bad_request", problem)));
                return;
            }
            try {
                long start = System.nanoTime();
                String user = IdentityNormalizer.normalize(body.user);

                // Map once: every key in a filter call shares one resource type,
                // method and access entity.
                AccessMapper.Result mapped = AccessMapper.map(
                        body.resource_type, body.method, body.access_entity);
                if (mapped.status == AccessMapper.Status.UNKNOWN_VOCABULARY) {
                    writeJson(resp, 422, mapper.writeValueAsString(
                            new ErrorBody("unknown_vocabulary", mapped.detail)));
                    return;
                }

                FilterResponse out = new FilterResponse();
                out.policy_version = engine.policyVersion();
                out.evaluated = body.keys.size();

                if (mapped.status == AccessMapper.Status.UNMAPPED) {
                    // Nothing is permitted, and there is no Ranger access type to
                    // evaluate against, so there is no result to summarise either.
                    warnUnmapped(mapped.detail);
                    out.allowed_keys = Collections.emptyList();
                    out.elapsed_ms = elapsedMs(start);
                    writeJson(resp, 200, mapper.writeValueAsString(out));
                    return;
                }

                List<String> allowedKeys = new ArrayList<>();
                RangerAccessRequestImpl representative = null;
                RangerAccessResult representativeResult = null;

                for (String key : body.keys) {
                    if (key == null || key.isBlank()) {
                        continue;
                    }
                    RangerAccessRequestImpl rangerReq =
                            buildRequest(user, body.context, mapped, key);
                    RangerAccessResult result = engine.evaluateNoAudit(rangerReq);
                    boolean allowed = result != null && result.getIsAllowed();
                    if (allowed) {
                        allowedKeys.add(key);
                    }
                    // Prefer an allowed result so the summary's policy id points
                    // at a policy that actually granted something.
                    if (representativeResult == null || (allowed && !representativeResult.getIsAllowed())) {
                        representative = rangerReq;
                        representativeResult = result;
                    }
                }

                String summary = String.format("filter: %s evaluated=%d allowed=%d",
                        body.context == null || body.context.request_uri == null
                                ? "-" : body.context.request_uri,
                        out.evaluated, allowedKeys.size());
                engine.auditFilterSummary(representative, representativeResult,
                        !allowedKeys.isEmpty(), summary);

                out.allowed_keys = allowedKeys;
                out.elapsed_ms = elapsedMs(start);
                writeJson(resp, 200, mapper.writeValueAsString(out));
            } catch (Exception e) {
                LOG.error("filter failed", e);
                writeJson(resp, 500, "{\"error\":\"internal\"}");
            }
        }
    }

    private static long elapsedMs(long startNanos) {
        return Math.max(0L, (System.nanoTime() - startNanos) / 1_000_000L);
    }

    private static String validateFilter(FilterRequest body) {
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
        if (body.resource_type == null || body.resource_type.isBlank()) {
            return "resource_type is required";
        }
        if (body.method == null || body.method.isBlank()) {
            return "method is required";
        }
        if (body.keys == null || body.keys.isEmpty()) {
            return "keys must contain at least one entry";
        }
        if (body.keys.size() > MAX_KEYS) {
            return "keys exceeds " + MAX_KEYS;
        }
        return null;
    }

    private RangerAccessRequestImpl buildRequest(String user, RequestContext ctx,
                                                 AccessMapper.Result mapped, String key) {
        return buildAccessRequest(user, ctx, mapped.resourceType, mapped.accessType, key);
    }

    /**
     * Same audit fields as a mapped check, but the access type is the contract
     * reason {@code unmapped_access} — there is no Ranger access type to evaluate.
     */
    private RangerAccessRequestImpl buildUnmappedRequest(String user, RequestContext ctx, Check check) {
        String resourceType = check.resource_type == null ? "" : check.resource_type.strip();
        return buildAccessRequest(user, ctx, resourceType, "unmapped_access", check.key);
    }

    private RangerAccessRequestImpl buildAccessRequest(String user, RequestContext ctx,
                                                       String resourceType, String accessType, String key) {
        boolean anyResource = key == null || key.isBlank();

        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        if (!anyResource && resourceType != null && !resourceType.isBlank()) {
            resource.setValue(resourceType, key.strip());
        }
        RangerAccessRequestImpl request = new RangerAccessRequestImpl(
                resource, accessType, user, Collections.emptySet(), null);

        if (anyResource) {
            // An omitted key means "any resource of this type", and it needs the
            // matching scope widened to work.
            //
            // Note RangerAccessResourceImpl.setValue(name, null) *removes* the key,
            // so there is no way to express "any" as a resource value -- the
            // resource simply carries no keys. Against a policy scoped to a prefix
            // (dag=etl_*) RangerDefaultPolicyResourceMatcher.getMatchType then
            // returns DESCENDANT, and under the default SELF scope
            // RangerDefaultPolicyEvaluator counts DESCENDANT as no match. A user
            // holding read on etl_* would be denied the class-level read and the
            // list page would come back empty.
            //
            // SELF_OR_DESCENDANTS makes any match type other than NONE count,
            // which is what the contract's "omitting key means any" requires.
            request.setResourceMatchingScope(
                    RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);
        }

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
        request.setAction(accessType);
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
        info.capabilities = CAPABILITIES;
        // Absent means no user store has been downloaded, which means group-based
        // policies cannot match. The enricher is added implicitly by
        // RangerBasePlugin.setPolicies because use.rangerGroups is set, but the
        // download itself still depends on usersync having run, so this is the
        // field to check first when a group policy appears to be ignored.
        long userStoreVersion = engine.userStoreVersion();
        info.user_store_version = userStoreVersion < 0 ? null : userStoreVersion;
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

    public static final class FilterRequest {
        public String user;
        public RequestContext context;
        public String resource_type;
        public String method;
        public String access_entity;
        public List<String> keys;
    }

    public static final class FilterResponse {
        public long policy_version;
        public List<String> allowed_keys;
        public int evaluated;
        public long elapsed_ms;
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
        public List<String> capabilities;
        public Long user_store_version;
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
