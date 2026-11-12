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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * REST client for MLflow 3.16.0's native RBAC endpoints ({@code mlflow/server/auth/routes.py}).
 *
 * <p>All RBAC endpoints are called with HTTP Basic Auth using the configured
 * MLflow admin account:
 * <ul>
 *   <li>{@code GET    /api/3.0/mlflow/workspaces}</li>
 *   <li>{@code GET    /api/2.0/mlflow/users/list}</li>
 *   <li>{@code POST   /api/2.0/mlflow/users/create}</li>
 *   <li>{@code GET    /api/3.0/mlflow/roles/list}</li>
 *   <li>{@code POST   /api/3.0/mlflow/roles/create}</li>
 *   <li>{@code PATCH  /api/3.0/mlflow/roles/update}</li>
 *   <li>{@code DELETE /api/3.0/mlflow/roles/delete}</li>
 *   <li>{@code POST   /api/3.0/mlflow/roles/permissions/add}</li>
 *   <li>{@code PATCH  /api/3.0/mlflow/roles/permissions/update}</li>
 *   <li>{@code DELETE /api/3.0/mlflow/roles/permissions/remove}</li>
 *   <li>{@code POST   /api/3.0/mlflow/roles/assign}</li>
 *   <li>{@code DELETE /api/3.0/mlflow/roles/unassign}</li>
 * </ul>
 */
public class MlflowRbacClient {

    private static final Logger LOG = LoggerFactory.getLogger(MlflowRbacClient.class);

    public static final String PATH_WORKSPACES              = "/api/3.0/mlflow/workspaces";
    public static final String PATH_USERS_LIST              = "/api/2.0/mlflow/users/list";
    public static final String PATH_USERS_CREATE            = "/api/2.0/mlflow/users/create";
    public static final String PATH_ROLES_LIST              = "/api/3.0/mlflow/roles/list";
    public static final String PATH_ROLES_CREATE            = "/api/3.0/mlflow/roles/create";
    public static final String PATH_ROLES_UPDATE            = "/api/3.0/mlflow/roles/update";
    public static final String PATH_ROLES_DELETE            = "/api/3.0/mlflow/roles/delete";
    public static final String PATH_ROLE_PERMISSIONS_ADD    = "/api/3.0/mlflow/roles/permissions/add";
    public static final String PATH_ROLE_PERMISSIONS_UPDATE = "/api/3.0/mlflow/roles/permissions/update";
    public static final String PATH_ROLE_PERMISSIONS_REMOVE = "/api/3.0/mlflow/roles/permissions/remove";
    public static final String PATH_ROLES_ASSIGN            = "/api/3.0/mlflow/roles/assign";
    public static final String PATH_ROLES_UNASSIGN          = "/api/3.0/mlflow/roles/unassign";

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS    = 30_000;

    private static final SecureRandom RANDOM = new SecureRandom();

    private final String       baseUrl;
    private final String       defaultWorkspace;
    private final String       basicAuthHeader;
    private final ObjectMapper mapper;
    private final RangerTls    tls;

    public MlflowRbacClient(AgentConfig config) {
        this(config.mlflowUrl(),
                config.mlflowAdminUsername(),
                config.mlflowAdminPassword(),
                config.mlflowDefaultWorkspace(),
                RangerTls.forConfig(config));
    }

    public MlflowRbacClient(String mlflowUrl,
                            String adminUsername,
                            String adminPassword,
                            String defaultWorkspace,
                            RangerTls tls) {
        this.baseUrl          = Objects.requireNonNull(mlflowUrl, "mlflowUrl").replaceAll("/+$", "");
        this.defaultWorkspace = Objects.requireNonNull(defaultWorkspace, "defaultWorkspace");
        String creds          = Objects.requireNonNull(adminUsername, "adminUsername")
                + ":" + Objects.requireNonNull(adminPassword, "adminPassword");
        this.basicAuthHeader  = "Basic " + Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8));
        this.mapper           = new ObjectMapper();
        this.tls              = tls;
    }

    /**
     * Lists active MLflow workspaces. When {@code MLFLOW_ENABLE_WORKSPACES} is disabled
     * on the MLflow server, {@code GET /api/3.0/mlflow/workspaces} returns 400/404;
     * in that case we return the single configured default workspace.
     */
    public Set<String> listWorkspaces() throws IOException {
        Set<String> workspaces = new LinkedHashSet<>();
        workspaces.add(defaultWorkspace);
        HttpResult res = execute("GET", PATH_WORKSPACES, null);
        if (res.status == 200) {
            JsonNode root = mapper.readTree(res.body);
            JsonNode arr = root == null ? null : root.get("workspaces");
            if (arr != null && arr.isArray()) {
                for (JsonNode ws : arr) {
                    String name = text(ws, "name");
                    if (name != null) {
                        workspaces.add(name);
                    }
                }
            }
            return workspaces;
        }
        if (res.status == 400 || res.status == 404 || res.status == 501) {
            LOG.debug("MLflow workspaces endpoint returned {} (workspaces disabled); using '{}'",
                    res.status, defaultWorkspace);
            return workspaces;
        }
        throw new IOException("GET " + PATH_WORKSPACES + " failed with status " + res.status + ": " + res.body);
    }

    /**
     * Calls {@code GET /api/2.0/mlflow/users/list}, which in MLflow 3.16.0 eager-loads
     * every user along with their assigned roles across workspaces.
     */
    public List<MlflowUserRecord> listUsersWithRoles() throws IOException {
        HttpResult res = execute("GET", PATH_USERS_LIST, null);
        if (res.status != 200) {
            throw new IOException("GET " + PATH_USERS_LIST + " failed with status " + res.status + ": " + res.body);
        }
        return parseUsersList(res.body);
    }

    /**
     * Calls {@code GET /api/3.0/mlflow/roles/list} to retrieve all roles and their
     * attached {@code role_permissions}.
     */
    public List<MlflowRoleRecord> listAllRoles() throws IOException {
        HttpResult res = execute("GET", PATH_ROLES_LIST, null);
        if (res.status != 200) {
            throw new IOException("GET " + PATH_ROLES_LIST + " failed with status " + res.status + ": " + res.body);
        }
        return parseRolesList(res.body, defaultWorkspace);
    }

    public List<MlflowRoleRecord> listRolesForWorkspace(String workspace) throws IOException {
        String path = PATH_ROLES_LIST + "?workspace=" + URLEncoder.encode(workspace, StandardCharsets.UTF_8);
        HttpResult res = execute("GET", path, null);
        if (res.status != 200) {
            throw new IOException("GET " + path + " failed with status " + res.status + ": " + res.body);
        }
        return parseRolesList(res.body, workspace);
    }

    public MlflowUserRecord createUserWithRandomPassword(String username) throws IOException {
        byte[] bytes = new byte[24];
        RANDOM.nextBytes(bytes);
        String randomPassword = "Rgr!" + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("username", username);
        payload.put("password", randomPassword);
        HttpResult res = execute("POST", PATH_USERS_CREATE, payload);
        if (res.status != 200) {
            throw new IOException("POST " + PATH_USERS_CREATE + " for user '" + username
                    + "' failed with status " + res.status + ": " + res.body);
        }
        JsonNode root = mapper.readTree(res.body);
        JsonNode u = root == null ? null : root.get("user");
        if (u == null) {
            throw new IOException("POST " + PATH_USERS_CREATE + " response missing 'user' field");
        }
        return new MlflowUserRecord(
                u.path("id").asInt(),
                text(u, "username"),
                u.path("is_admin").asBoolean(false),
                Collections.emptySet());
    }

    public MlflowRoleRecord createRole(String workspace, String name, String description) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("workspace", workspace);
        payload.put("name", name);
        if (description != null) {
            payload.put("description", description);
        }
        HttpResult res = execute("POST", PATH_ROLES_CREATE, payload);
        if (res.status != 200) {
            throw new IOException("POST " + PATH_ROLES_CREATE + " (" + workspace + "/" + name
                    + ") failed with status " + res.status + ": " + res.body);
        }
        JsonNode root = mapper.readTree(res.body);
        JsonNode roleNode = root == null ? null : root.get("role");
        if (roleNode == null) {
            throw new IOException("POST " + PATH_ROLES_CREATE + " response missing 'role' field");
        }
        return parseSingleRole(roleNode, workspace);
    }

    public void updateRoleDescription(int roleId, String description) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("role_id", roleId);
        payload.put("description", description == null ? "" : description);
        HttpResult res = execute("PATCH", PATH_ROLES_UPDATE, payload);
        if (res.status != 200) {
            throw new IOException("PATCH " + PATH_ROLES_UPDATE + " (role_id=" + roleId
                    + ") failed with status " + res.status + ": " + res.body);
        }
    }

    public void deleteRole(int roleId) throws IOException {
        Map<String, Object> payload = Collections.singletonMap("role_id", roleId);
        HttpResult res = execute("DELETE", PATH_ROLES_DELETE, payload);
        if (res.status != 200 && res.status != 204 && res.status != 404) {
            throw new IOException("DELETE " + PATH_ROLES_DELETE + " (role_id=" + roleId
                    + ") failed with status " + res.status + ": " + res.body);
        }
    }

    public MlflowRolePermissionRecord addRolePermission(int roleId,
                                                        String resourceType,
                                                        String resourcePattern,
                                                        String permission) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("role_id", roleId);
        payload.put("resource_type", resourceType);
        payload.put("resource_pattern", resourcePattern);
        payload.put("permission", permission);
        HttpResult res = execute("POST", PATH_ROLE_PERMISSIONS_ADD, payload);
        if (res.status != 200) {
            throw new IOException("POST " + PATH_ROLE_PERMISSIONS_ADD + " (role_id=" + roleId
                    + ", " + resourceType + ":" + resourcePattern + "=" + permission
                    + ") failed with status " + res.status + ": " + res.body);
        }
        JsonNode root = mapper.readTree(res.body);
        JsonNode rp = root == null ? null : root.get("role_permission");
        int id = rp == null ? -1 : rp.path("id").asInt(-1);
        return new MlflowRolePermissionRecord(id, roleId, resourceType, resourcePattern, permission);
    }

    public void updateRolePermission(int rolePermissionId, String permission) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("role_permission_id", rolePermissionId);
        payload.put("permission", permission);
        HttpResult res = execute("PATCH", PATH_ROLE_PERMISSIONS_UPDATE, payload);
        if (res.status != 200) {
            throw new IOException("PATCH " + PATH_ROLE_PERMISSIONS_UPDATE
                    + " (role_permission_id=" + rolePermissionId + ", permission=" + permission
                    + ") failed with status " + res.status + ": " + res.body);
        }
    }

    public void removeRolePermission(int rolePermissionId) throws IOException {
        Map<String, Object> payload = Collections.singletonMap("role_permission_id", rolePermissionId);
        HttpResult res = execute("DELETE", PATH_ROLE_PERMISSIONS_REMOVE, payload);
        if (res.status != 200 && res.status != 204 && res.status != 404) {
            throw new IOException("DELETE " + PATH_ROLE_PERMISSIONS_REMOVE
                    + " (role_permission_id=" + rolePermissionId
                    + ") failed with status " + res.status + ": " + res.body);
        }
    }

    public void assignRole(String username, int roleId) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("username", username);
        payload.put("role_id", roleId);
        HttpResult res = execute("POST", PATH_ROLES_ASSIGN, payload);
        // 400 RESOURCE_ALREADY_EXISTS is tolerated idempotently
        if (res.status != 200 && !(res.status == 400 && res.body.contains("RESOURCE_ALREADY_EXISTS"))) {
            throw new IOException("POST " + PATH_ROLES_ASSIGN + " (username=" + username
                    + ", role_id=" + roleId + ") failed with status " + res.status + ": " + res.body);
        }
    }

    public void unassignRole(String username, int roleId) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("username", username);
        payload.put("role_id", roleId);
        HttpResult res = execute("DELETE", PATH_ROLES_UNASSIGN, payload);
        if (res.status != 200 && res.status != 204 && res.status != 404) {
            throw new IOException("DELETE " + PATH_ROLES_UNASSIGN + " (username=" + username
                    + ", role_id=" + roleId + ") failed with status " + res.status + ": " + res.body);
        }
    }

    // -----------------------------------------------------------------------
    // JSON parsers (package-private for unit tests)
    // -----------------------------------------------------------------------

    static List<MlflowUserRecord> parseUsersList(String json) throws IOException {
        ObjectMapper m = new ObjectMapper();
        JsonNode root = m.readTree(json);
        JsonNode usersNode = root == null ? null : root.get("users");
        if (usersNode == null || !usersNode.isArray()) {
            return Collections.emptyList();
        }
        List<MlflowUserRecord> result = new ArrayList<>();
        for (JsonNode u : usersNode) {
            int id = u.path("id").asInt(-1);
            String username = text(u, "username");
            boolean isAdmin = u.path("is_admin").asBoolean(false);
            Set<Integer> roleIds = new LinkedHashSet<>();
            JsonNode rolesNode = u.get("roles");
            if (rolesNode != null && rolesNode.isArray()) {
                for (JsonNode r : rolesNode) {
                    int rid = r.path("id").asInt(-1);
                    if (rid >= 0) {
                        roleIds.add(rid);
                    }
                }
            }
            if (username != null) {
                result.add(new MlflowUserRecord(id, username, isAdmin, roleIds));
            }
        }
        return result;
    }

    static List<MlflowRoleRecord> parseRolesList(String json, String fallbackWorkspace) throws IOException {
        ObjectMapper m = new ObjectMapper();
        JsonNode root = m.readTree(json);
        JsonNode rolesNode = root == null ? null : root.get("roles");
        if (rolesNode == null || !rolesNode.isArray()) {
            return Collections.emptyList();
        }
        List<MlflowRoleRecord> result = new ArrayList<>();
        for (JsonNode r : rolesNode) {
            result.add(parseSingleRole(r, fallbackWorkspace));
        }
        return result;
    }

    private static MlflowRoleRecord parseSingleRole(JsonNode r, String fallbackWorkspace) {
        int id = r.path("id").asInt(-1);
        String name = text(r, "name");
        String workspace = text(r, "workspace");
        if (workspace == null) {
            workspace = fallbackWorkspace;
        }
        String description = text(r, "description");
        List<MlflowRolePermissionRecord> perms = new ArrayList<>();
        JsonNode permsNode = r.get("permissions");
        if (permsNode != null && permsNode.isArray()) {
            for (JsonNode p : permsNode) {
                int pid = p.path("id").asInt(-1);
                int roleId = p.path("role_id").asInt(id);
                String rt = text(p, "resource_type");
                String rp = text(p, "resource_pattern");
                String perm = text(p, "permission");
                if (rt != null && rp != null && perm != null) {
                    perms.add(new MlflowRolePermissionRecord(pid, roleId, rt, rp, perm));
                }
            }
        }
        return new MlflowRoleRecord(id, workspace, name, description, perms);
    }

    private static String text(JsonNode node, String field) {
        if (node == null || field == null) {
            return null;
        }
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            return null;
        }
        String s = v.asText();
        return (s == null || s.isBlank()) ? null : s;
    }

    // -----------------------------------------------------------------------
    // HTTP execution supporting GET, POST, PATCH, and DELETE with JSON bodies
    // -----------------------------------------------------------------------

    HttpResult execute(String method, String pathWithQuery, Map<String, Object> bodyPayload) throws IOException {
        URI uri = URI.create(baseUrl + pathWithQuery);
        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        try {
            RangerTls.applyIfPresent(conn, tls);
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            conn.setRequestProperty("Authorization", basicAuthHeader);
            conn.setRequestProperty("Accept", "application/json");

            if ("PATCH".equalsIgnoreCase(method)) {
                // HttpURLConnection in the standard JDK rejects setRequestMethod("PATCH");
                // use X-HTTP-Method-Override / Werkzeug method or reflective fallback if needed.
                setHttpMethod(conn, "PATCH");
            } else {
                conn.setRequestMethod(method);
            }

            if (bodyPayload != null) {
                byte[] jsonBytes = mapper.writeValueAsBytes(bodyPayload);
                conn.setDoOutput(true);
                conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                try (OutputStream out = conn.getOutputStream()) {
                    out.write(jsonBytes);
                }
            }

            int status = conn.getResponseCode();
            String respBody;
            try (InputStream in = (status >= 400) ? conn.getErrorStream() : conn.getInputStream()) {
                respBody = (in == null) ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
            return new HttpResult(status, respBody);
        } finally {
            conn.disconnect();
        }
    }

    private static void setHttpMethod(HttpURLConnection conn, String method) throws IOException {
        try {
            conn.setRequestMethod(method);
        } catch (java.net.ProtocolException pe) {
            try {
                java.lang.reflect.Field methodField = HttpURLConnection.class.getDeclaredField("method");
                methodField.setAccessible(true);
                methodField.set(conn, method);
            } catch (Exception reflectionEx) {
                throw pe;
            }
        }
    }

    static final class HttpResult {
        final int status;
        final String body;

        HttpResult(int status, String body) {
            this.status = status;
            this.body   = body == null ? "" : body;
        }
    }

    // -----------------------------------------------------------------------
    // DTO records representing MLflow 3.16.0's live RBAC state
    // -----------------------------------------------------------------------

    public static final class MlflowUserRecord {
        private final int id;
        private final String username;
        private final boolean admin;
        private final Set<Integer> assignedRoleIds;

        public MlflowUserRecord(int id, String username, boolean admin, Set<Integer> assignedRoleIds) {
            this.id              = id;
            this.username        = username;
            this.admin           = admin;
            this.assignedRoleIds = Collections.unmodifiableSet(new LinkedHashSet<>(assignedRoleIds));
        }

        public int id()                           { return id; }
        public String username()                  { return username; }
        public boolean isAdmin()                  { return admin; }
        public Set<Integer> assignedRoleIds()     { return assignedRoleIds; }
    }

    public static final class MlflowRoleRecord {
        private final int id;
        private final String workspace;
        private final String name;
        private final String description;
        private final List<MlflowRolePermissionRecord> permissions;

        public MlflowRoleRecord(int id,
                                String workspace,
                                String name,
                                String description,
                                List<MlflowRolePermissionRecord> permissions) {
            this.id          = id;
            this.workspace   = workspace;
            this.name        = name;
            this.description = description;
            this.permissions = Collections.unmodifiableList(new ArrayList<>(permissions));
        }

        public int id()                                           { return id; }
        public String workspace()                                 { return workspace; }
        public String name()                                      { return name; }
        public String description()                               { return description; }
        public List<MlflowRolePermissionRecord> permissions()     { return permissions; }
    }

    public static final class MlflowRolePermissionRecord {
        private final int id;
        private final int roleId;
        private final String resourceType;
        private final String resourcePattern;
        private final String permission;

        public MlflowRolePermissionRecord(int id,
                                          int roleId,
                                          String resourceType,
                                          String resourcePattern,
                                          String permission) {
            this.id              = id;
            this.roleId          = roleId;
            this.resourceType    = resourceType;
            this.resourcePattern = resourcePattern;
            this.permission      = permission;
        }

        public int id()                 { return id; }
        public int roleId()             { return roleId; }
        public String resourceType()    { return resourceType; }
        public String resourcePattern() { return resourcePattern; }
        public String permission()      { return permission; }
    }
}
