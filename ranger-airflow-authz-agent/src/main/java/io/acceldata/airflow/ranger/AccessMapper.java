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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The only place that knows Airflow's taxonomy. Turns
 * {@code (resource_type, method, access_entity)} into a Ranger resource name
 * and access type. Table-driven: adding an access type is a data change.
 *
 * <p>Unknown vocabulary (not in the contract enums) is distinct from an
 * unmapped combination of known values — the first is a 422, the second a
 * per-check deny with {@code reason: unmapped_access}.
 */
public final class AccessMapper {

    public enum Status { MAPPED, UNKNOWN_VOCABULARY, UNMAPPED }

    public static final class Result {
        public final Status status;
        public final String resourceType;
        public final String accessType;
        public final String detail;

        private Result(Status status, String resourceType, String accessType, String detail) {
            this.status = status;
            this.resourceType = resourceType;
            this.accessType = accessType;
            this.detail = detail;
        }

        static Result mapped(String resourceType, String accessType) {
            return new Result(Status.MAPPED, resourceType, accessType, null);
        }

        static Result unknown(String detail) {
            return new Result(Status.UNKNOWN_VOCABULARY, null, null, detail);
        }

        static Result unmapped(String detail) {
            return new Result(Status.UNMAPPED, null, null, detail);
        }
    }

    private static final Set<String> RESOURCE_TYPES = Set.of(
            "dag", "connection", "variable", "pool", "asset", "asset_alias",
            "config", "view", "custom_view");

    private static final Set<String> METHODS = Set.of("GET", "POST", "PUT", "DELETE");

    private static final Set<String> ACCESS_ENTITIES = Set.of(
            "AUDIT_LOG", "CODE", "DEPENDENCIES", "HITL_DETAIL", "RUN", "TASK",
            "TASK_INSTANCE", "TASK_LOGS", "VERSION", "WARNING", "XCOM");

    private static final Set<String> CRUD_RESOURCES = Set.of(
            "connection", "variable", "pool", "asset", "asset_alias", "custom_view");

    private static final Map<String, String> DAG_TABLE = new HashMap<>();

    static {
        putDag(null, "GET", "read");
        putDag(null, "PUT", "edit");
        putDag(null, "DELETE", "delete");
        putDag("RUN", "GET", "read_run");
        putDag("RUN", "POST", "trigger");
        putDag("RUN", "PUT", "edit_run");
        putDag("RUN", "DELETE", "delete_run");
        putDag("TASK", "GET", "read_task");
        putDag("TASK_INSTANCE", "GET", "read_task_instance");
        putDag("TASK_INSTANCE", "PUT", "clear_task");
        putDag("TASK_INSTANCE", "DELETE", "clear_task");
        putDag("TASK_LOGS", "GET", "read_logs");
        putDag("CODE", "GET", "read_code");
        putDag("XCOM", "GET", "read_xcom");
        putDag("XCOM", "POST", "edit_xcom");
        putDag("XCOM", "PUT", "edit_xcom");
        putDag("XCOM", "DELETE", "edit_xcom");
        putDag("AUDIT_LOG", "GET", "read_audit_log");
        putDag("DEPENDENCIES", "GET", "read_dependencies");
        putDag("WARNING", "GET", "read_warning");
        putDag("VERSION", "GET", "read_version");
        putDag("HITL_DETAIL", "GET", "read_hitl");
        putDag("HITL_DETAIL", "POST", "respond_hitl");
        putDag("HITL_DETAIL", "PUT", "respond_hitl");
    }

    private AccessMapper() {}

    public static Result map(String resourceType, String method, String accessEntity) {
        if (resourceType == null || resourceType.isBlank()) {
            return Result.unknown("resource_type is required");
        }
        String type = resourceType.strip();
        if (!RESOURCE_TYPES.contains(type)) {
            return Result.unknown("unknown resource_type: " + type);
        }
        if (method == null || method.isBlank()) {
            return Result.unknown("method is required");
        }
        String meth = method.strip().toUpperCase(Locale.ROOT);
        if (!METHODS.contains(meth)) {
            return Result.unknown("unknown method: " + method);
        }
        String entity = blankToNull(accessEntity);
        if (entity != null && !ACCESS_ENTITIES.contains(entity)) {
            return Result.unknown("unknown access_entity: " + accessEntity);
        }

        if ("dag".equals(type)) {
            String access = DAG_TABLE.get(dagKey(entity, meth));
            if (access == null) {
                return Result.unmapped(type + " " + meth + (entity == null ? "" : " " + entity));
            }
            return Result.mapped(type, access);
        }

        if (entity != null) {
            return Result.unmapped(type + " does not take access_entity");
        }
        if (CRUD_RESOURCES.contains(type)) {
            switch (meth) {
                case "GET":    return Result.mapped(type, "read");
                case "POST":   return Result.mapped(type, "create");
                case "PUT":    return Result.mapped(type, "edit");
                case "DELETE": return Result.mapped(type, "delete");
                default:       return Result.unmapped(type + " " + meth);
            }
        }
        if ("config".equals(type) || "view".equals(type)) {
            if ("GET".equals(meth)) {
                return Result.mapped(type, "read");
            }
            return Result.unmapped(type + " " + meth);
        }
        return Result.unmapped(type + " " + meth);
    }

    private static void putDag(String entity, String method, String accessType) {
        DAG_TABLE.put(dagKey(entity, method), accessType);
    }

    private static String dagKey(String entity, String method) {
        return (entity == null ? "-" : entity) + "|" + method;
    }

    private static String blankToNull(String value) {
        if (value == null) {
            return null;
        }
        String stripped = value.strip();
        return stripped.isEmpty() ? null : stripped;
    }

    public static boolean isKnownResourceType(String resourceType) {
        return resourceType != null && RESOURCE_TYPES.contains(resourceType.strip());
    }

    @Override
    public String toString() {
        return Objects.toString(DAG_TABLE.size());
    }
}
