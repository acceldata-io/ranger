# ranger-yunikorn-agent

In-cluster sidecar that synchronizes Apache Ranger policies to Apache
YuniKorn queue ACLs.

## What it does

1. Polls Ranger Admin every N seconds for policy changes against a
   configured `yunikorn` service (using Ranger's standard delta protocol —
   `getServicePoliciesIfUpdated(lastVersion)`).
2. Translates allow-rules into YuniKorn-format ACL strings
   (`"user1,user2 group1,group2"`), keyed by full queue path.
3. Reads the YuniKorn ConfigMap, splices the rendered ACLs into the
   embedded `queues.yaml` (preserving every other operator-owned field),
   and patches the ConfigMap with optimistic concurrency.
4. The k8shim's existing ConfigMap watcher absorbs the change and pushes
   the new config to yunikorn-core via gRPC. End-to-end propagation is
   typically under 5 seconds.

## What it does not do

- Push or evaluate authorization at request time. yunikorn-core continues
  to enforce ACLs natively from its config.
- Modify queue structure, capacity, placement rules, or any non-ACL field.
  Operators retain full control of `queues.yaml` for everything except
  `submitacl` and `adminacl`.
- Install the Ranger plugin. The companion `plugin-yunikorn` module
  (shipped inside Ranger Admin) must be present and a `yunikorn` service
  must be registered in Ranger.

## Architecture

```
  Ranger Admin (ODP VMs)        YuniKorn cluster (K8s)
  ┌────────────────────┐        ┌─────────────────────────────────┐
  │  policies in DB    │ ◀──────┤  ranger-yunikorn-agent (this)   │
  │  /service/plugins/ │  poll  │  - PolicySyncService            │
  │  policies/download │  every │  - AclConverter                 │
  └────────────────────┘  30s   │  - ConfigMapWriter              │
                                │     │ patch                     │
                                │     ▼                           │
                                │  yunikorn-configs ConfigMap     │
                                │     │ watch                     │
                                │     ▼                           │
                                │  k8shim ──gRPC── yunikorn-core  │
                                └─────────────────────────────────┘
```

## Build

```
mvn clean package
```

Produces `target/ranger-yunikorn-agent-<version>.jar` (shaded fat jar).

## Image

```
docker build -t acceldata/ranger-yunikorn-agent:0.1.0 .
```

## Install

See `deploy/helm/` for the Helm chart, or `deploy/manifests/` for raw YAML.

Required:
- A registered `yunikorn` service in your Ranger Admin
- Ranger credentials (basic auth via `RANGER_PRINCIPAL`/`RANGER_CREDENTIAL`, or Kerberos keytab when `ranger.auth.mode=kerberos`)
- ServiceAccount in the YuniKorn namespace with
  `get`/`update`/`patch` on the `yunikorn-configs` ConfigMap

## TLS (SSL-enabled Ranger Admin)

When `ranger.admin.url` is `https://`, the agent needs to trust Ranger Admin's
server certificate. ODP Ranger typically uses a private-CA or self-signed cert
that the JRE doesn't trust by default, so provide a truststore:

```
ranger.ssl.truststore.path=/etc/ranger-tls/truststore.jks
ranger.ssl.truststore.type=JKS          # defaults to JKS; set PKCS12 if needed
# password via RANGER_SSL_TRUSTSTORE_PASSWORD env var (from a Secret)
```

`ranger.ssl.insecure=true` disables certificate and hostname verification
entirely (like `curl -k`) — development only, never in production.

Via Helm, put the truststore + its password in one Secret and reference it
under `ranger.tls.secret` (see `deploy/helm/values.yaml`):

```
kubectl -n yunikorn create secret generic ranger-yunikorn-agent-tls \
    --from-file=truststore.jks=/path/to/truststore.jks \
    --from-literal=truststore-password=changeit
```

## Config

See `src/main/resources/default.properties` for all available keys.
