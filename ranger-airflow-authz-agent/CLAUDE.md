# ranger-airflow-authz-agent

## Read this first

**Before exploring this module, grep the index** — it answers most questions in
a few lines and saves reading the tree:

```bash
D=~/work_files/ranger-airflow-plugin/docs/INDEX.md
grep -A8 "^## AuthzServer.handleFilter" $D
grep -A6 "^DEC-05" $D
grep -A5 "^GAP-01" $D
```

Narrative walkthrough: `~/work_files/ranger-airflow-plugin/docs/01-jvm-agent-decisions.md`

Prefer `grep -n` plus `sed -n 'X,Yp'` over `cat` on these files — `AuthzServer.java`
alone is 640 lines.

## What this module is

A **policy decision point** colocated with each Airflow api-server. It answers
"may this user perform this access on this resource" and writes the audit
record. It creates nothing, stores nothing, syncs nothing.

Do **not** reason about it like `ranger-yunikorn-agent` in this same repo. That
one translates Ranger policies into YuniKorn ACLs and writes a ConfigMap. This
one translates nothing and writes nothing — there is no sync loop here.

Turning `false` into a 403 is the Python client's job, not this module's.

## Build and test

```bash
mvn -pl ranger-airflow-authz-agent -am -DskipTests install
mvn -pl ranger-airflow-authz-agent test
```

**This does not work on the laptop** (`GAP-01`): `ranger-plugins-cred` needs
`hadoop-common:3.3.6.3.3.6.6-SNAPSHOT`, which is in no configured repo. Build
on Jenkins/Argo or a machine with a populated `~/.m2`. Never claim this module
compiles without having seen it compile.

This module is **exempt from checkstyle, spotbugs and per-module RAT** (the
YuniKorn-agent playbook). `plugin-airflow` is not — do not assume the exemption
travels.

## Settled — do not re-propose

Full list with reasons: `grep -A3 "^DEC-" $D`. The ones that bite here:

- **DEC-02** JVM plugin, never a Python policy engine. "Approximately Ranger"
  fails a governance review.
- **DEC-03** All Airflow→Ranger mapping lives in `AccessMapper`. The client
  sends Airflow vocabulary verbatim and must stay ignorant of Ranger terms.
- **DEC-05** Filter writes exactly one audit record. Non-permitted keys are not
  access attempts.
- **DEC-06** An omitted key means "any" via `SELF_OR_DESCENDANTS`, **not** `*`.
  Literal `*` would deny a user holding `etl_*` — the matcher treats the policy
  value as a glob and the request value as a literal.
- **DEC-09** Audit cluster name comes from `plugin.getClusterName()`, not a
  second agent property.
- **DEC-12** Send a username, never groups. Kerberos tickets carry no groups.
- **DEC-15** Assert audits on distinct `eventId` counts, never on log-line
  matches — `RangerDefaultAuditHandler` logs one event several times at DEBUG,
  which already produced one wrong measurement.

## Reuse Ranger, don't reimplement

Check `agents-common` before writing a utility. Already corrected once each:
`KerberosName` for `auth_to_local`, `plugin.getClusterName()` for the audit
cluster name, `RangerBaseService.getUserList()` for lookup-user seeding.

But **read the implementation before adopting a convention**. `TimedEventUtil.
timedTask` is used by Kafka and Ozone and its timeout is commented out upstream
— it bounds nothing (`DEC-10`).

## Open in this module

`grep -A4 "^GAP-0" $D` for the full text.

- **GAP-05** `auditFilterSummary` has zero coverage — `RangerAuthzEngineTest`
  uses the test constructor, which nulls the audit handler. Governance-critical.
- **GAP-06** `IdentityNormalizer` strips the realm when no rule applies, so
  `alice@EVIL.REALM` → `alice`. Strict fix is two lines; decide with test output.
- **GAP-08** Filter edges: all-blank keys write no audit record; duplicate keys
  inflate `evaluated`.
- **GAP-09** `MIN_TOKEN_BYTES` measures token text, not decoded entropy.

## Invariants

- The agent **never** returns `allowed: true` on any error path.
- A 401 writes **no** audit record — the claimed user is untrusted until the
  secret matches.
- A 422 writes **no** audit record — hence the two-pass map-then-evaluate in
  `handleAuthorize`.
- Readiness requires policies **and** the user store. With
  `use.only.rangerGroups=true`, an absent store means every group policy
  silently fails while the agent looks healthy.

## Conventions

Commit subject `ODP-XXXX: short subject`. Never add `Co-Authored-By` naming an
agent. Keep the contract doc `docs/api-v1.md` in step with any wire change — a
frozen contract the code diverges from is worse than either.
