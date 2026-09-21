# Airflow authorization agent — API contract v1

Status: **frozen for implementation**. Changes to this document require a bump of
`contract_version` and a corresponding change to the client's startup handshake.

The agent is a policy decision point colocated with each Airflow api-server. This
document is the complete interface between it and `odp-airflow-ranger-auth-manager`.
A client can be implemented from this document alone.

---

## 1. Transport and binding

| | |
|---|---|
| Protocol | HTTP/1.1, `Content-Type: application/json`, UTF-8 |
| Bind address | `127.0.0.1` only — never `0.0.0.0` |
| Default port | `9183` (configurable via `authz.agent.port`) |
| Authentication | shared secret, see below |
| Base path | `/v1` |

The agent MUST refuse to start if configured to bind to a non-loopback address.
Loopback means the kernel never delivers traffic from another host; that is the
network boundary. It is not the authentication boundary — any local process can
still open `127.0.0.1:9183`.

### Shared secret

Loopback stops a remote caller from reaching the agent. It does not stop a local
unprivileged user from enumerating policies or forging audit records (the agent
takes `user` from the request body). A shared secret closes that gap: it moves
the trust boundary from "anyone on the host" to "anyone who can read the Airflow
service user's files" — the same set that could already steal the api-server
keytab.

The same mechanism is required on Kubernetes and on bare metal. On Kubernetes
the pod network namespace already excludes other pods, but anything else in the
pod (a log shipper, a debug container) can still reach loopback.

| | |
|---|---|
| Header | `Authorization: Bearer <token>` |
| Token source | a file, path configurable via `authz.agent.token.file` |
| Default path | `/etc/airflow/ranger-authz-agent.token` |
| File mode | `0400`, owned by the Airflow service user |
| Token | cryptographically random, at least 32 bytes before encoding |
| Comparison | constant-time (`MessageDigest.isEqual` / `hmac.compare_digest`) |

The agent MUST refuse to start if the token file is missing, empty, world-readable,
or shorter than 32 decoded bytes. It MUST never log the token.

Required on `/v1/authorize`, `/v1/filter` and `/v1/info`. Not required on
`/v1/health` or `/v1/ready`, so liveness and readiness probes (typically `exec`
curls, because loopback is not reachable from the kubelet) do not need the
secret.

A missing, malformed or incorrect `Authorization` header yields `401` with
`{"error": "unauthorized"}`. The body and timing MUST NOT distinguish those
three cases. No audit record is written — the claimed `user` is untrusted until
the secret matches.

On Kubernetes, mount the same Secret volume into both the api-server and agent
containers. On bare metal, both processes read the same `0400` file.

## 2. Vocabulary

The client speaks **Airflow's vocabulary**. It has no knowledge of Ranger resources or
access types; that mapping is the agent's, and section 7 is normative.

### `resource_type`

`dag` · `connection` · `variable` · `pool` · `asset` · `asset_alias` · `config` ·
`view` · `custom_view`

### `method`

`GET` · `POST` · `PUT` · `DELETE`

### `access_entity` (only meaningful when `resource_type` is `dag`)

`AUDIT_LOG` · `CODE` · `DEPENDENCIES` · `HITL_DETAIL` · `RUN` · `TASK` ·
`TASK_INSTANCE` · `TASK_LOGS` · `VERSION` · `WARNING` · `XCOM`

Omitted or null means the DAG object itself rather than a part of it.

### `key`

The resource identity — `dag_id`, `conn_id`, variable key, pool name, asset id,
config section, view name, or plugin view resource name.

**Omitting `key` means "any".** The question becomes *may this user perform this
access on any resource of this type?* — used by endpoints that ask a class-level
question, such as whether a list page may be opened at all. It does **not** mean
`*`, and a user holding `read` on `dev_*` alone MUST be allowed an "any" read.

> Implementation note for M1: this is Ranger's null-resource-value matching, the same
> mechanism the Hive plugin uses for `SHOW DATABASES`. Verify the exact behaviour
> against the engine during the vertical slice before relying on it.

### `context`

Present on every request. Carries what the audit record needs and what conditions may
later evaluate against.

| Field | Required | Notes |
|---|---|---|
| `client_ip` | yes | The end user's address, not the api-server's |
| `request_uri` | yes | Path only, no query string, no secrets |
| `request_id` | no | Correlates the audit record with the api-server log |

---

## 3. `POST /v1/authorize`

One or many checks for a single user. Each check gets its own decision — the agent
never returns a combined boolean, so the client can compose whatever shape the
`BaseAuthManager` method needs and every decision is individually auditable.

### Request

```json
{
  "user": "alice@CORP.EXAMPLE",
  "context": {
    "client_ip": "10.4.2.19",
    "request_uri": "/api/v2/dags/etl_sales/dagRuns",
    "request_id": "c3f1a8e2"
  },
  "checks": [
    { "id": "0", "resource_type": "dag", "method": "POST",
      "access_entity": "RUN", "key": "etl_sales" },
    { "id": "1", "resource_type": "pool", "method": "GET", "key": "default_pool" },
    { "id": "2", "resource_type": "connection", "method": "GET" }
  ]
}
```

`id` is opaque to the agent and echoed back. `checks` MUST contain at least one entry
and at most 1000.

### Response — `200`

```json
{
  "policy_version": 118,
  "decisions": [
    { "id": "0", "allowed": true,  "policy_id": 47 },
    { "id": "1", "allowed": true,  "policy_id": 12 },
    { "id": "2", "allowed": false, "reason": "no_matching_policy" }
  ]
}
```

`decisions` is in request order and the same length as `checks`. `policy_id` is present
only when a policy produced the decision. `reason` is present only on denials:
`no_matching_policy` · `explicit_deny` · `unmapped_access`.

---

## 4. `POST /v1/filter`

The list-page path. Several hundred candidates in, the permitted subset out, one round
trip. Equivalent to N authorize checks of the same shape, but audited as one event —
see section 6.

### Request

```json
{
  "user": "alice@CORP.EXAMPLE",
  "context": { "client_ip": "10.4.2.19", "request_uri": "/api/v2/dags" },
  "resource_type": "dag",
  "method": "GET",
  "access_entity": null,
  "keys": ["etl_sales", "etl_finance", "mktg_daily"]
}
```

`keys` MUST contain at least one entry and at most 5000. The client is responsible for
chunking beyond that.

### Response — `200`

```json
{
  "policy_version": 118,
  "allowed_keys": ["etl_sales", "etl_finance"],
  "evaluated": 3,
  "elapsed_ms": 1
}
```

`allowed_keys` is a subset of `keys`, order not guaranteed. The client MUST treat a key
absent from `allowed_keys` as denied.

### Menu filtering

`filter_authorized_menu_items` is served by this endpoint with `resource_type: "view"`
and `keys` set to the menu item enum values verbatim. The agent normalizes case. The
client performs no name mapping.

---

## 5. Status endpoints

### `GET /v1/health`

`200` when the process is up. Never reflects policy state. Suitable for a liveness
probe.

### `GET /v1/ready`

`200` when policies are loaded and the engine can decide. `503` with
`{"ready": false, "reason": "..."}` until then. Suitable for a readiness probe, and the
api-server MUST NOT serve traffic while this returns 503.

### `GET /v1/info`

```json
{
  "agent_version": "1.0.0",
  "contract_version": "v1",
  "ranger_service": "odp_airflow",
  "service_def_version": 3,
  "supported_airflow": ">=3.2,<3.3"
}
```

The client calls this at startup and **refuses to start** if `contract_version` is not
one it implements, or if the running Airflow version falls outside `supported_airflow`.
A mismatched pair would not fail on its own — it would quietly authorize the wrong
thing, which is the worst outcome this system can produce.

---

## 6. Audit behaviour

| Endpoint | Records written |
|---|---|
| `/v1/authorize` | One per check — allows and denials alike |
| `/v1/filter` | **Exactly one** per call, never one per key |
| status endpoints | None |
| any 401 | None — the claimed user is untrusted |

A filter call's non-permitted keys are **not** access attempts. Nobody tried to open
the DAGs they cannot see; a page was loaded and Airflow asked about everything. Writing
those as denials would be both voluminous and false.

The single filter record carries the resource type and access type, the user, the
context, `requestData` of the form `filter: evaluated=812 allowed=5`, and a result of
Allowed when at least one key passed, Denied when none did.

Every record carries the real end-user principal, the real `client_ip`, the
`request_uri`, the cluster name, and the deciding policy id where one exists.

---

## 7. Mapping — normative

The only place Airflow vocabulary becomes Ranger vocabulary. Ranger resource name
equals `resource_type` in every case; only the access type requires a table.

### `dag`

| method | access_entity | access type |
|---|---|---|
| GET | — | `read` |
| PUT | — | `edit` |
| DELETE | — | `delete` |
| GET | RUN | `read_run` |
| POST | RUN | `trigger` |
| PUT | RUN | `edit_run` |
| DELETE | RUN | `delete_run` |
| GET | TASK | `read_task` |
| GET | TASK_INSTANCE | `read_task_instance` |
| PUT, DELETE | TASK_INSTANCE | `clear_task` |
| GET | TASK_LOGS | `read_logs` |
| GET | CODE | `read_code` |
| GET | XCOM | `read_xcom` |
| POST, PUT, DELETE | XCOM | `edit_xcom` |
| GET | AUDIT_LOG | `read_audit_log` |
| GET | DEPENDENCIES | `read_dependencies` |
| GET | WARNING | `read_warning` |
| GET | VERSION | `read_version` |
| GET | HITL_DETAIL | `read_hitl` |
| POST, PUT | HITL_DETAIL | `respond_hitl` |

### `connection`, `variable`, `pool`, `asset`, `asset_alias`, `custom_view`

| method | access type |
|---|---|
| GET | `read` |
| POST | `create` |
| PUT | `edit` |
| DELETE | `delete` |

### `config`, `view`

| method | access type |
|---|---|
| GET | `read` |

Any other method on these two is unmapped.

### Unmapped combinations

An unmapped combination is **denied**, with `reason: "unmapped_access"`, and logged at
WARN — rate-limited to one log line per distinct combination per hour, so a new Airflow
endpoint surfaces in the logs without flooding them. It is never permitted, and never a
500: an unmapped combination is a known-unknown, not an error.

---

## 8. Error model

| Status | Meaning |
|---|---|
| 400 | Malformed JSON, missing required field, list too long |
| 401 | Missing, malformed or incorrect `Authorization` header (section 1) |
| 422 | Unknown `resource_type`, `method`, or `access_entity` |
| 503 | Engine not ready |
| 500 | Anything unexpected |

**The agent never returns `allowed: true` on any error path, and the client treats
every non-200 response — and every timeout, connection refusal and parse failure — as a
denial.** Fail-closed is a property of both halves or it is not a property.

---

## 9. Client requirements

These are part of the contract, not suggestions.

1. **Handshake at startup.** Call `/v1/info`; refuse to start on mismatch (section 5).
2. **Send the shared secret** on `/v1/authorize`, `/v1/filter` and `/v1/info` as
   `Authorization: Bearer <token>`, reading the token from the same file the agent
   does. Never log it. A 401 is a deny, same as any other non-200.
3. **Fail closed.** Any non-200, timeout or transport error is a deny. No retries — a
   retry storm against a struggling agent turns a slow page into an outage.
4. **Cache decisions for 30 seconds**, configurable. Keyed on user, resource type,
   method, access entity and key. This TTL plus the agent's policy poll interval is the
   deployment's revocation window, and it bounds the precision of time-bound policies.
5. **Timeouts:** 200 ms connect, 2000 ms read, both configurable.
6. **Never send groups.** Send the principal as Airflow authenticated it. Normalization
   and group resolution are the agent's, so that Ranger's usersync is the single source
   of truth — and because a Kerberos ticket carries no groups at all.
7. **Prefer `/v1/filter`** wherever `BaseAuthManager` offers a filter or batch method.
   The default implementations in Airflow loop per item; leaving them unoverridden
   makes a several-hundred-DAG list page unusable.

---

## 10. Non-goals

- **No task-runtime enforcement.** The Execution API authenticates a task instance, not
  a person. There is no user for the agent to decide about, and this contract has no
  shape for one.
- **No policy administration.** The agent reads policies. It never creates, updates or
  grants.
- **No caching of user or group state in the client.** Only decisions are cached.
