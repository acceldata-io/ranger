# Ranger UserSync: Admin Central (Keycloak) source

See implementation under `ugsync/src/main/java/org/apache/ranger/unixusersync/process/`.

## Objective

Synchronize **users**, **service users**, and **groups** from the XDP Admin Central API (Keycloak as backing IdP) into **Apache Ranger Admin**, using the existing UserSync pipeline (`UserGroupSource` → `PolicyMgrUserGroupBuilder` sink).

## Scope

- **In scope:** New sync source implementation, configuration keys, optional on-demand sync trigger, unit tests.
- **Out of scope:** Gravitino; changes to Ranger Admin REST API; OAuth token refresh (static bearer or basic auth in v1).

## Design

1. `**AdminCentralUserGroupBuilder`** (`org.apache.ranger.unixusersync.process.AdminCentralUserGroupBuilder`)
  - Implements `UserGroupSource`, extends `AbstractUserGroupSource`.
  - On each cycle (or force trigger): HTTP GET Admin Central, parse JSON, build `sourceUsers`, `sourceGroups`, `sourceGroupUsers`.
  - Calls `UserGroupSink.addOrUpdateUsersGroups(..., computeDeletes)` with the same **delete cadence** as LDAP/File (`ranger.usersync.deletes.enabled`, `ranger.usersync.deletes.frequency`).
2. `**AdminCentralRestClient`**
  - `GET` with optional **Basic** or **Bearer** auth; configurable connect/read timeouts.
  - Uses `HttpURLConnection` / `HttpsURLConnection` (JVM default trust store; document `-Djavax.net.ssl.trustStore*` for custom CAs).
3. `**AdminCentralResponseParser`**
  - Jackson-based parsing with configurable JSON paths and field names so different Admin Central payload shapes can be mapped without code changes.
4. **Force sync**
  - Property `ranger.usersync.force.sync.trigger.file`: if set, the main UserSync loop sleeps in short chunks and **wakes early** when the file’s modification time increases (e.g. `touch /var/run/ranger/force-usersync`). No extra port or thread.
5. **Configuration**


| Property                                             | Purpose                                                              |
| ---------------------------------------------------- | -------------------------------------------------------------------- |
| `ranger.usersync.admincentral.base.url`              | Tenant base URL (required), e.g. `https://tenant.example.com`        |
| `ranger.usersync.admincentral.users.path`            | Default `/xdp-cp-service/api/users`                                  |
| `ranger.usersync.admincentral.groups.path`           | Optional separate groups API path                                    |
| `ranger.usersync.admincentral.auth.type`             | `none`, `basic`, or `bearer`                                         |
| `ranger.usersync.admincentral.username` / `password` | Basic auth                                                           |
| `ranger.usersync.admincentral.bearer.token`          | Static bearer token                                                  |
| `ranger.usersync.admincentral.users.array.path`      | JSON path to user array, e.g. `content` (empty = root must be array) |
| `ranger.usersync.admincentral.groups.array.path`     | Same for groups response                                             |
| `ranger.usersync.admincentral.user.name.field`       | Default `username`                                                   |
| `ranger.usersync.admincentral.user.groups.field`     | Default `groups` (array of strings or `{name:...}` objects)          |
| `ranger.usersync.admincentral.group.name.field`      | Default `name`                                                       |
| `ranger.usersync.admincentral.group.members.field`   | Default `members`                                                    |
| `ranger.usersync.admincentral.user.enabled.field`    | If set, users with false/0 are skipped                               |
| `ranger.usersync.admincentral.pagination.page.size`  | `0` = single GET; else page through with `page`/`size` query params  |
| `ranger.usersync.admincentral.pagination.page.param` | Default `page`                                                       |
| `ranger.usersync.admincentral.pagination.size.param` | Default `size`                                                       |
| `ranger.usersync.force.sync.trigger.file`            | Optional path; `touch` to run next sync sooner                       |


1. **Activation**
  - Set `ranger.usersync.source.impl.class` to `org.apache.ranger.unixusersync.process.AdminCentralUserGroupBuilder`, **or** set `ranger.usersync.sync.source` to `ADMIN_CENTRAL` (with empty `source.impl.class`).
  - Keep `ranger.usersync.sink.impl.class` as `org.apache.ranger.unixusersync.process.PolicyMgrUserGroupBuilder`.

## Follow-ups

- OAuth2 client-credentials token acquisition and refresh.
- Richer audit object than reusing `FileSyncSourceInfo` for endpoint metadata.
- Optional HTTPS client trust store aligned with `ranger.usersync.truststore.`* (same as Policy Manager client).

## Verification

```bash
cd ranger
mvn -pl ugsync test
```

