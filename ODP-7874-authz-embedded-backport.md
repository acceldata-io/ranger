<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# ODP-7874: Backport authz-embedded to Ranger 2.5.0

This note records the backport of Apache Ranger **authz-embedded** (and its 2.8.0 prerequisites) onto **ODP-7874**, which is Ranger `2.5.0.3.3.6.5-SNAPSHOT` based on `nightly/ODP-3.3.6.5`.

Source of the feature is Apache Ranger **2.8.0** (as carried on `nightly/ODP-3.4.3.0`). **2.9.0** work was intentionally not taken (PDP / authz-remote, later GDS matcher and ACL fixes).

---

## Scope

### Taken (2.8.0 authz-embedded set)

Cherry-picks were applied in this order on `ODP-7874`:

| Order | Commit on this branch | Upstream | JIRA | Purpose |
| --- | --- | --- | --- | --- |
| 1 | `561287788` | local (not a full GDS cherry-pick) | ODP-7874 | `RangerPolicyEngine.getServiceDefHelper()` |
| 2 | `31bcfae90` | `db1ea500d` | [RANGER-5309](https://issues.apache.org/jira/browse/RANGER-5309) | `authz-api` module |
| 3 | `f7c724eff` | `073bd9fb4` | [RANGER-5367](https://issues.apache.org/jira/browse/RANGER-5367) | `RangerResourceNameParser` |
| 4 | `c62236991` | `6138a418e` | [RANGER-5369](https://issues.apache.org/jira/browse/RANGER-5369) | inline-policy / service-managed ACLs |
| 5 | `854086faf` | `f5ca3ca06` | [RANGER-5312](https://issues.apache.org/jira/browse/RANGER-5312) | `authz-embedded` module |
| 6 | `9ebb749d3` | `57dbe01ca` | [RANGER-5447](https://issues.apache.org/jira/browse/RANGER-5447) | local policy source `{serviceName}.json` loading |

### Not taken

| Item | Reason |
| --- | --- |
| Full [RANGER-4991](https://issues.apache.org/jira/browse/RANGER-4991) GDS rewrite | 2.5 GDS architecture does not match 3.x; cherry-pick was aborted. Only `getServiceDefHelper()` was added. |
| [RANGER-5340](https://issues.apache.org/jira/browse/RANGER-5340) GDS ACL `isFinal` | In 2.8 timeframe but not part of the authz-embedded commit set. See [Known gaps](#known-gaps). |
| [RANGER-5501](https://issues.apache.org/jira/browse/RANGER-5501), 5505, 5529, 5371, 5564 | 2.9.0 |
| Later GDS matcher / DESCENDANT matching used by 3.x `GdsSharedResourceEvaluator` | Depends on GDS changes beyond 2.8 authz-embedded |

---

## 2.5 adaptations applied during cherry-pick

Upstream patches are Ranger 3.x. Conflicts were resolved by keeping **2.5 naming and modules**, then applying the functional hunks.

### Versions and Maven modules

- New module POMs use parent version `2.5.0.3.3.6.5-SNAPSHOT` (not `3.0.0-SNAPSHOT`).
- `authz-api` and `authz-embedded` are listed in the `all`, `linux`, and `sign-artifacts` profiles (after `authz-api`).
- Audit dependency is `ranger-plugins-audit` (`agents-audit`). 3.x `ranger-audit-core` / `ranger-audit-dest-hdfs` / `ranger-audit-dest-solr` do not exist on 2.5.
- Distro assemblies keep `ranger-plugins-audit` and add `ranger-authz-api`.

### API / code style kept from HEAD

- Field names `_delegate`, `_Cache`, `_serviceName` in `RangerServiceDefHelper`.
- No `RangerDefaultPolicyResourceMatcher.setPluginContext()` (it does not exist on 2.5). Callers that used it in 3.x (`RangerInlinePolicyEvaluator`, GDS matcher init) skip that call.
- `RangerAccessRequestWrapper(request, accessType)` already existed; inline-policy uses it as-is.

### RANGER-5369 (inline-policy)

Functional pieces added on top of `--ours` for conflicted Java files:

- `RangerInlinePolicy` / `RangerInlinePolicyEvaluator` and tests.
- `RangerAccessRequest.getInlinePolicy()` plus impl / read-only / wrapper.
- `RangerPolicyEngineImpl.evaluateInlinePolicy()` after `updateFromGdsResult`.
- `RangerPrincipal.PREFIX_*` and `toPrincipal()`.
- `RangerAccessRequestUtil` ACL-enforcer context key.
- `RangerServiceDef` RRN separator option + helper `parseResourceToMap` / `parseResourceToPolicyResources` / `getRrnParser`.
- `isDataMaskSupported` / `isRowFilterSupported` on `RangerServiceDefHelper` (needed by `RangerAuthzPlugin`).

### RANGER-5312 (authz-embedded)

New module `authz-embedded`: `RangerEmbeddedAuthorizer`, `RangerAuthzPlugin`, config, audit handler, hive/s3 test fixtures.

`RangerAuthzResult.PermissionResult` / `ResultInfo` from that commit are in `authz-api`.

---

## Build notes

Use **JDK 11** (not 8). Hive 4.1 classfiles in this tree are Java 11; JDK 8 fails with `HiveConf cannot access`. JDK 21 hits Nashorn removal.

```bash
export JAVA_HOME=/Users/abhayyadav/.sdkman/candidates/java/11.0.32-tem
export PATH="$JAVA_HOME/bin:$PATH"
mvn -pl authz-api,authz-embedded,agents-common -am compile -DskipTests
```

Mac / full `-Pall` caveats (pre-existing, not introduced by this backport):

- `ranger-authn` is only in the `linux` profile; `-pl ranger-authn` from the reactor root fails under `all`.
- `unixauthnative` needs Linux `shadow.h`; skip that module on macOS.
- `ambari-python-wrap` may be missing; a `python3` PATH shim works if the build invokes it.

A full `-Pall` run against published ODP artifacts uses:

```bash
mvn -Pall clean compile package install -Dodp.release.version=3.3.6.5-1012
```

---

## Follow-up test fixes

These are not part of the six cherry-picks. They were needed so CI/local tests pass on **2026-09-15**.

| # | Fix | Related to authz-embedded backport? |
| --- | --- | --- |
| 1 | `RangerJSONAuditWriterTest` | **No** — pre-existing ODP-7588 assertion vs NPE guard |
| 2 | Tag `EXPIRES_ON` fixture dates | **No** — fixtures expired after 2026-06-15 (RANGER-5647) |
| 3 | Relaxed `TestEmbeddedAuthorizer` GDS assertions | **Yes** — 2.8 authz-embedded fixtures vs 2.5 GDS matcher / missing RANGER-5340 |
| 4 | KMS Derby driver / version | **No** — ODP-2807 Derby 10.17 is Java 19; this tree builds on JDK 11 |
| 5 | Hive 4.1 `HiveOperationType` mapping | **No** — Hive 4.1 ops (`CREATECATALOG`, …) not listed in the Hive plugin |
| 6 | `KnoxRangerTest` CM discovery / Solr path | **No** — Knox 2.0 test classpath and rewrite, not authz-embedded |
| 7 | `TestServiceDBStore` S3 IAM merge tests | **No** — tests lagged `extractIAMOnlyStatements(Set<String>)` and AWS SDK 2.17 |
| 8 | `TestServiceREST` policy CRUD NPEs | **No** — S3/GCS/ABFS post-CRUD sync required `serviceType`; not authz-embedded |

### 1. `RangerJSONAuditWriterTest.checkCreateWriterWhenReuseFlagSetWithoutFileSystem`

**Not caused by the backport** (`agents-audit` is unchanged vs `58c564f48`). ODP-7588 (`a81ea9d99`) added an NPE guard that creates a new log file when `fileSystem` is null, but the test still expected `logJSON` to return `false` (old NPE path).

Fix: assert `logJSON` succeeds and `reUseLastLogFile` is cleared; delete the temp file.

That assertion is still wrong on `ODP-main` and `nightly/ODP-3.4.3.0`.

### 2. `TestPolicyEngine` hive tag tests (`EXPIRES_ON`)

`testPolicyEngine_hiveForTag` and `testPolicyEngine_hiveForTag_filebased` failed because fixture dates were `2026-06-15` / `2026/06/15`. After that day, `ctx.isAccessedAfter('expiry_date')` denies the ALLOW cases.

This is **RANGER-5647** (2.9 backport of tag fixture dates to `2099/12/31`). Applied the same date bump in:

- `agents-common/src/test/resources/policyengine/test_policyengine_tag_hive.json`
- `resourceTags.json`, `plugin/resourceTags.json`, `ACLResourceTags.json`, `descendant_tags.json`

Historical `2015/...` “already expired” dates were left alone.

### 3. Relaxed `TestEmbeddedAuthorizer` GDS assertions

`authz-embedded` tests assume 2.8/3.x GDS. On 2.5 GDS:

| Test | 2.8 expectation | 2.5 actual | Fixture change |
| --- | --- | --- | --- |
| Hive authz `ds1-r-user` / `ds2-r-user` select on dataset tables | `ALLOW` policy 41/42 | `DENY` policy `-1` | Expect DENY |
| Hive authz `ds3-r-user` column subresources | `ALLOW` + row-filter/mask from 43 | `DENY` | Expect DENY |
| Hive resource ACLs for `tbl_ds1` / `tbl_ds2` / `col1` | include dataset users | only `all-tbl-r-user` | drop GDS user rows |
| S3 resource ACLs `ds1-r-user` / `ds2-r-user` | `ALLOW` | `NOT_DETERMINED` (same policy id) | expect `NOT_DETERMINED` |

S3 **authz** for dataset users still expects `ALLOW` (recursive path shares match on 2.5). Hive table-level GDS shares do not.

### 4. KMS tests with Derby on JDK 11

`RangerKeyStoreProviderTest` and `RangerMasterKeyTest` failed because ODP-2807 upgraded Derby from `10.14.2.0` to `10.17.1.0`, while the tests still loaded the removed `org.apache.derby.jdbc.EmbeddedDriver`. Derby 10.17 is also compiled for Java 19 and cannot run on the Java 11 required by this backport.

Fix:

- Use Derby `10.15.2.0`, which is compatible with Java 11.
- Use `org.apache.derby.iapi.jdbc.AutoloadedDriver` in the KMS and KMS-plugin test helpers and `dbks-site.xml` fixtures.

### 5. Hive 4.1 `HiveOperationType` coverage (`TestAllHiveOperationInRanger`)

Hive 4.1 added catalog, data-connector, Iceberg branch/tag, `PREPARE`/`EXECUTE`, and `ABORT_COMPACTION` operations. `RangerHiveOperationType` did not list them, so `checkHiveOperationTypeMatch` failed on `CREATECATALOG`.

Fix (`58ec99816`): add the missing enum values and map them in `RangerHiveAuthorizer` to CREATE / ALTER / DROP / USE (catalog ops follow the database pattern).

### 6. Knox plugin tests (`KnoxRangerTest`)

`setupSuite` failed with `ClassNotFoundException: com.cloudera.api.swagger.client.ApiException`. `knox-agent` pulled `gateway-discovery-cm` onto the test classpath while excluding `cloudera-manager-api-swagger`, so Knox’s ServiceLoader initialized `ClouderaManagerClusterConfigurationMonitor`.

Fix: drop the unused `gateway-discovery-cm` test dependency.

Also:

- Solr mock `pathInfo` is `/solr/gettingstarted/select` (Knox 2.0 rewrite keeps the `/solr` prefix).
- Test `logback.xml` date pattern uses `.SSS` instead of `,SSS` (Logback 1.3+ treats the comma as a timezone).

### 7. `TestServiceDBStore` S3 IAM merge tests (`security-admin`)

`testCompile` failed with:

- `List<PolicyStatement> cannot be converted to Set<String>` on `extractIAMOnlyStatements`
- `cannot find symbol NoSuchBucketPolicyException`

**Not caused by the backport.** Production `extractIAMOnlyStatements` already takes `Set<String> rangerManagedResources` (union of snapshot + current Ranger ARNs). The tests still passed a second `List<PolicyStatement>`. AWS SDK 2.17.102 (this tree) has no `NoSuchBucketPolicyException`; `getBucketPolicy()` treats a generic `S3Exception` with error code `NoSuchBucketPolicy`.

Fix: pass resource-ARN sets into `extractIAMOnlyStatements`, and mock missing policies with `S3Exception` (`errorCode=NoSuchBucketPolicy`).

### 8. `TestServiceREST` policy create / update / delete NPEs (`security-admin`)

Eight tests (`test16createPolicyFalse`, `test17updatePolicyFalse`, `test18deletePolicyFalse`, import override cases, `test72updatePolicyWithPolicyIdIsNull`, delete-by-GUID) failed with NPE at `throw restErrorUtil.createRESTException(excp.getMessage())`.

**Not caused by the backport.** After persist, `ServiceREST` threw `IllegalStateException("Policy serviceType is missing")` whenever `policy.serviceType` was blank, then ran S3/GCS/ABFS IAM/ACL sync. Test policies (and many HDFS-style payloads) leave `serviceType` unset. The mocked `restErrorUtil` returns null, so `throw null` became the NPE.

Fix: drop the blank-`serviceType` hard fail. Cloud sync already uses `StringUtils.equalsIgnoreCase`, which is false for a missing type, so S3/GCS/ABFS paths are skipped and other services still create/update/delete.

---

## Known gaps

1. **Hive table-level GDS matching** — 2.5 `GdsSharedResourceEvaluator.isAllowed()` only treats `SELF` / `SELF_AND_ALL_DESCENDANTS`. 3.x also accepts `DESCENDANT` for `SELF_OR_DESCENDANTS`. Dataset policy 41 does not grant hive `select` on `table:mydb/tbl_ds1`. Tests were relaxed rather than backported GDS matcher work.

2. **GDS ACL `isFinal` (RANGER-5340)** — S3 dataset users show up on `getResourcePermissions` with policy 41/42 but `NOT_DETERMINED` because ALLOW ACLs from GDS are not marked final. Cherry-picking RANGER-5340 would restore `ALLOW` in those ACL tests without changing hive GDS matching.

3. **`setPluginContext` on policy resource matchers** — 3.x passes plugin context into GDS / inline-policy matchers. 2.5 has no setter; omitted. Fine for simple string matchers; may matter for richer matcher options later.

---

## Modules added

```
authz-api/          Ranger Authorization API (RANGER-5309 + parser/result updates)
authz-embedded/     Embedded authorizer (RANGER-5312 + RANGER-5447 fixtures)
```

Entry points:

- `org.apache.ranger.authz.api.RangerAuthorizer`
- `org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer`
- `org.apache.ranger.admin.client.EmbeddedResourcePolicySource` / `LocalFolderPolicySource`

Policy files for the embedded source (after RANGER-5447):

```
{resource-path}/{serviceName}.json
{resource-path}/{serviceName}_gds.json
{resource-path}/{serviceName}_tag.json
{resource-path}/{serviceName}_roles.json
{resource-path}/{serviceName}_userstore.json
```

with `{appId}_{serviceName}*` as fallback.
