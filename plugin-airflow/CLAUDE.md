# plugin-airflow

## Read this first

```bash
D=~/work_files/ranger-airflow-plugin/docs/INDEX.md
grep -A8 "^## AirflowClient.fetchCollection" $D
grep -A6 "^## AirflowConnectionMgr" $D
grep -A5 "^GAP-11" $D
```

Narrative walkthrough:
`~/work_files/ranger-airflow-plugin/docs/03-service-def-and-autocomplete.md`

`AirflowClient.java` is 521 lines — grep and `sed -n 'X,Yp'` rather than `cat`.

## What this module is

The **Ranger Admin side**. It is loaded into Admin's JVM and does exactly two
things: Test Connection, and autocomplete for policy resource fields.

It performs **no authorization**. Decisions are made by
`ranger-airflow-authz-agent`, a separate process colocated with each Airflow
api-server, which pulls policies from Admin. This module never talks to that
agent and the agent never talks to this module.

This is also the **only** place in the design where Ranger Admin reaches out to
Airflow. Enforcement is entirely pull-based.

## The service definition lives elsewhere

`agents-common/src/main/resources/service-defs/ranger-servicedef-airflow.json`,
registered in `EmbeddedServiceDefsUtil.java` at lines 52, 82, 134, 187, 285.
This module supplies the class its `implClass` names.

The service-def's access types and the agent's `AccessMapper.DAG_TABLE` must
agree. `ServiceDefConsistencyTest` in the agent module asserts that, which is
why the mapper's vocabulary sets are package-private.

## Build and test

```bash
mvn -pl plugin-airflow test
```

**Checkstyle and spotbugs apply to this module.** The agent is exempt; this one
is a normal reactor module and is not. The YuniKorn agent produced 944
checkstyle violations when that exemption was first needed — expect friction and
build before stacking commits.

Same caveat as everywhere: the Ranger reactor does not build on the laptop
(`GAP-01`).

## Settled — do not re-propose

- **DEC-08** No `getDefaultRangerPolicies()` override. `RangerBaseService.
  getUserList()` (`RangerBaseService.java:420`) already adds the service's
  `username` config to the default `all - <resource>` policies, so the lookup
  account is permitted automatically. An earlier review of mine said to add the
  override and was **wrong** — YuniKorn needs it only because it uses a Kerberos
  `lookUpUser` instead.
- **DEC-10** Do **not** adopt `TimedEventUtil.timedTask`. Kafka and Ozone wrap
  lookups in it, but its timeout is commented out upstream — it calls straight
  through. `fetchCollection` uses an explicit `LOOKUP_BUDGET_MS` deadline.

## Things that are easy to break here

**Four independent bounds** in `fetchCollection` (`AirflowClient.java:317`).
Removing any one reintroduces a real failure:

1. Jersey per-request connect/read timeouts — bound one HTTP call
2. `LOOKUP_BUDGET_MS` (10s) across the whole paginated walk — without it, ten
   pages at a ten-second read timeout hangs the policy form for two minutes
3. `MAX_PAGES × PAGE_LIMIT` = 1000 names, with a WARN at the cap
4. natural end when `rawCount < PAGE_LIMIT`

**Pagination compares `rawCount`, not extracted ids.** A page containing a
record with a blank id would otherwise break the loop early and silently drop
every later page. That is what `CollectionPage` exists for.

**Token caching is `synchronized` on purpose.** `cachedAccessToken:420` and
`invalidateAccessToken:430` both are, because `AirflowConnectionMgr` now shares
one client across Admin's request threads. Unsynchronised fields there produce
torn reads and `Bearer null`.

**Wildcards must survive.** `normalizeUserInput:166` turns `etl_*` into the
prefix `etl_` before matching. Without it a literal `startsWith("etl_*")` matches
nothing and an admin concludes wildcards are unsupported — on a service-def
whose headline feature is wildcards.

**`ignoreCase` must follow the service-def** per resource: `false` for
dag/connection/variable/pool, `true` for view. Otherwise autocomplete and
enforcement disagree about case.

## The constraint that generates support tickets

`airflow.url` must be the **api-server**, not a Knox/SPNEGO frontend, and the
lookup account must be **password-capable** (LDAP or FAB).

Airflow 3's `/api/v2` authenticates with the auth manager's JWT and does not
speak SPNEGO — so no `WWW-Authenticate: Negotiate` challenge is ever issued, and
wrapping the client in `Subject.doAs` would achieve nothing. In a Kerberos-only
shop that forbids password auth, autocomplete is simply unavailable, and that is
a documented limitation rather than a bug.

## Open in this module

- **GAP-07** The lab's `odp_airflow` service has no `username` configured, so
  autocomplete cannot run there.
- **GAP-10** `AirflowConnectionMgr.CACHE` is never evicted and `clearCache()`
  has no caller — a deleted service leaks a client and its JWT.
- **GAP-11** The URL/password constraint is in the class javadoc but not in
  `ERR_TAIL`, so a 401 reads as "broken plugin".
- No truststore config for an https Airflow behind a private CA. Do **not** add
  a verification-disable flag.

## Conventions

`ODP-XXXX: short subject`. No `Co-Authored-By` naming an agent. Package stays
`org.apache.ranger.services.airflow` — this module is the one piece of the
project with a plausible upstream path, so keep it upstream-shaped.
