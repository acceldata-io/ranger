/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.authorization.pinot;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicy.RangerRowFilterPolicyItem;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Decisions of the Ranger checker for Pinot against in-memory (file-based style) policies of the pinot service-def. */
public class RangerPinotAccessCheckerTest {
    private static final String SERVICE = "pinotdev";

    @Test
    public void tableAccessFollowsPolicies() {
        RangerPinotAccessChecker checker = checker(policies(), 0, new Counting());

        assertTrue(table(checker, "alice", groups("analysts"), "default", "orders", "query"));
        assertTrue("no database = default", table(checker, "alice", groups("analysts"), null, "orders", "read_metadata"));
        assertFalse("other table", table(checker, "alice", groups("analysts"), "default", "customers", "query"));
        assertFalse("other access type", table(checker, "alice", groups("analysts"), "default", "orders", "manage_table"));
        assertFalse("no matching group", table(checker, "mallory", groups("guests"), "default", "orders", "query"));
        assertTrue("wildcard table", table(checker, "bob", groups(), "sales", "any_table", "query"));
        assertFalse("wildcard is per database", table(checker, "bob", groups(), "default", "orders", "query"));
    }

    @Test
    public void databaseAndClusterRequests() {
        RangerPinotAccessChecker checker = checker(policies(), 0, new Counting());

        assertTrue("database request matches database/* policy", checker.checkAccess("carol", groups(), PinotAccessChecker.RESOURCE_DATABASE, "sales", null, "manage_database", "a").getKey());
        assertFalse(checker.checkAccess("carol", groups(), PinotAccessChecker.RESOURCE_DATABASE, "default", null, "manage_database", "a").getKey());
        assertTrue("'all' implies every access type", checker.checkAccess("admin", groups(), PinotAccessChecker.RESOURCE_CLUSTER, null, null, "manage_cluster_config", "a").getKey());
        assertTrue(checker.checkAccess("admin", groups(), PinotAccessChecker.RESOURCE_CLUSTER, null, null, "manage_users", "a").getKey());
        assertFalse(checker.checkAccess("alice", groups("analysts"), PinotAccessChecker.RESOURCE_CLUSTER, null, null, "read_metadata", "a").getKey());
    }

    @Test
    public void reasonsNameThePolicyOrTheMissingGrant() {
        RangerPinotAccessChecker checker = checker(policies(), 0, new Counting());

        assertEquals("Ranger policy 1", checker.checkAccess("alice", groups("analysts"), PinotAccessChecker.RESOURCE_TABLE, "default", "orders", "query", "a").getValue());
        assertTrue(checker.checkAccess("alice", groups("analysts"), PinotAccessChecker.RESOURCE_TABLE, "default", "x", "query", "a").getValue().startsWith("no Ranger policy allows query"));
        assertFalse("missing access type", checker.checkAccess("admin", groups(), PinotAccessChecker.RESOURCE_CLUSTER, null, null, null, "UnknownAction").getKey());
    }

    @Test
    public void rowFiltersComeFromRowFilterPolicies() {
        RangerPinotAccessChecker checker = checker(policies(), 0, new Counting());

        assertEquals(Collections.singletonList("region = 'south'"), checker.getRowFilters("alice", groups("analysts"), "default", "orders"));
        assertEquals(Collections.emptyList(), checker.getRowFilters("bob", groups(), "default", "orders"));
        assertEquals(Collections.emptyList(), checker.getRowFilters("alice", groups("analysts"), "default", "customers"));
    }

    @Test
    public void failsClosedWithoutPolicies() {
        RangerPinotAccessChecker checker = checker(Collections.<RangerPolicy>emptyList(), 0, new Counting());

        assertFalse(checker.checkAccess("admin", groups(), PinotAccessChecker.RESOURCE_CLUSTER, null, null, "manage_cluster_config", "a").getKey());
        assertFalse(table(checker, "alice", groups("analysts"), "default", "orders", "query"));
    }

    @Test
    public void decisionsAreCachedPerUserGroupsResourceAndAccess() {
        Counting                 audit   = new Counting();
        RangerPinotAccessChecker checker = checker(policies(), 60_000, audit);

        table(checker, "alice", groups("analysts"), "default", "orders", "query");
        table(checker, "alice", groups("analysts"), "default", "orders", "query");
        assertEquals("second identical request answered from the cache", 1, audit.count.get());

        table(checker, "alice", groups("analysts", "other"), "default", "orders", "query");
        table(checker, "alice", groups("analysts"), "default", "orders", "read_metadata");
        assertEquals("different groups or access type: evaluated again", 3, audit.count.get());
    }

    @Test(expected = IllegalStateException.class)
    public void initNeedsTheRangerFilesOnTheClassPath() {
        new RangerPinotAccessChecker().init(Collections.<String, String>emptyMap());
    }

    @Test
    public void configFilesAreReadFromThePinotConfDirectory() throws Exception {
        java.nio.file.Path home = java.nio.file.Files.createTempDirectory("pinot-home");
        java.io.File      conf = new java.io.File(home.toFile(), "conf");

        conf.mkdirs();
        java.nio.file.Files.write(new java.io.File(conf, "ranger-pinot-security.xml").toPath(), "<configuration/>".getBytes(StandardCharsets.UTF_8));
        java.nio.file.Files.write(new java.io.File(conf, "pinot-broker.conf").toPath(), "x=y".getBytes(StandardCharsets.UTF_8));

        assertEquals(Collections.singletonList(new java.io.File(conf, "ranger-pinot-security.xml")), RangerPinotAccessChecker.pinotConfFiles(conf));
        assertEquals(Collections.emptyList(), RangerPinotAccessChecker.pinotConfFiles(null));
    }

    @Test
    public void serviceNameIsTakenFromTheConfDirectoryFiles() throws Exception {
        java.nio.file.Path conf = java.nio.file.Files.createTempDirectory("pinot-conf");
        java.io.File       file = new java.io.File(conf.toFile(), "ranger-pinot-security.xml");

        java.nio.file.Files.write(file.toPath(), ("<configuration><property><name>ranger.plugin.pinot.service.name</name>"
                + "<value>prod_pinot</value></property></configuration>").getBytes(StandardCharsets.UTF_8));

        try {
            new RangerPinotAccessChecker().init(Collections.singletonList(file));
            org.junit.Assert.fail("no policy URL configured");
        } catch (IllegalStateException e) {
            // the service name was found in the file; what is missing is the policy URL
            assertTrue(e.getMessage(), e.getMessage().startsWith("No Ranger Admin URL"));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void kerberosNeedsPrincipalAndKeytab() {
        RangerPinotAccessChecker.enableKerberos(ugi("pinot@EXAMPLE.COM", null));
    }

    @Test(expected = IllegalStateException.class)
    public void kerberosRefusesAMissingKeytab() {
        RangerPinotAccessChecker.enableKerberos(ugi("pinot@EXAMPLE.COM", "/no/such/pinot.keytab"));
    }

    @Test
    public void noUgiSettingsMeansNoKerberos() {
        RangerPinotAccessChecker.enableKerberos(new org.apache.hadoop.conf.Configuration(false));
    }

    private static org.apache.hadoop.conf.Configuration ugi(String principal, String keytab) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);

        conf.set("ranger.plugin.pinot.ugi.initialize", "true");
        conf.set("ranger.plugin.pinot.ugi.login.type", "keytab");
        conf.set("ranger.plugin.pinot.ugi.keytab.principal", principal);

        if (keytab != null) {
            conf.set("ranger.plugin.pinot.ugi.keytab.file", keytab);
        }

        return conf;
    }

    // ---------------------------------------------------------------------------------------------------------------

    public static RangerServiceDef serviceDef() {
        InputStream in = RangerPinotAccessCheckerTest.class.getResourceAsStream("/service-defs/ranger-servicedef-pinot.json");

        return JsonUtils.jsonToObject(new InputStreamReader(in, StandardCharsets.UTF_8), RangerServiceDef.class);
    }

    private static boolean table(RangerPinotAccessChecker checker, String user, Set<String> groups, String database, String table, String accessType) {
        return checker.checkAccess(user, groups, PinotAccessChecker.RESOURCE_TABLE, database, table, accessType, "action").getKey();
    }

    private static Set<String> groups(String... groups) {
        return new HashSet<>(Arrays.asList(groups));
    }

    private static RangerPinotAccessChecker checker(List<RangerPolicy> policies, long cacheTtlMs, RangerAccessResultProcessor audit) {
        ServicePolicies servicePolicies = new ServicePolicies();

        servicePolicies.setServiceName(SERVICE);
        servicePolicies.setServiceDef(serviceDef());
        servicePolicies.setPolicies(policies);
        servicePolicies.setPolicyVersion(1L);

        RangerPluginConfig config = new RangerPluginConfig("pinot", SERVICE, "pinot", null, null, null);
        RangerBasePlugin   plugin = new RangerBasePlugin(config);

        plugin.setPolicies(servicePolicies); // policies as if downloaded; no Ranger Admin, no refresher

        return new RangerPinotAccessChecker(plugin, audit, "pinot", cacheTtlMs);
    }

    private static List<RangerPolicy> policies() {
        List<RangerPolicy> ret = new ArrayList<>();

        ret.add(access(1, resources("database", "default", "table", "orders"), Collections.<String>emptyList(), Collections.singletonList("analysts"), "query", "read_metadata"));
        ret.add(access(2, resources("database", "sales", "table", "*"), Collections.singletonList("bob"), Collections.<String>emptyList(), "query"));
        ret.add(access(3, resources("database", "sales", "table", "*"), Collections.singletonList("carol"), Collections.<String>emptyList(), "manage_database"));
        ret.add(access(4, resources("cluster", "*"), Collections.singletonList("admin"), Collections.<String>emptyList(), "all"));

        RangerPolicy              rowFilter = new RangerPolicy();
        RangerRowFilterPolicyItem item      = new RangerRowFilterPolicyItem(new RangerPolicyItemRowFilterInfo("region = 'south'"), Collections.singletonList(new RangerPolicyItemAccess("query")), null, Collections.singletonList("analysts"), null, null, null);

        rowFilter.setId(5L);
        rowFilter.setService(SERVICE);
        rowFilter.setName("orders south");
        rowFilter.setPolicyType(RangerPolicy.POLICY_TYPE_ROWFILTER);
        rowFilter.setResources(resources("database", "default", "table", "orders"));
        rowFilter.setRowFilterPolicyItems(Collections.singletonList(item));
        ret.add(rowFilter);

        return ret;
    }

    private static RangerPolicy access(long id, Map<String, RangerPolicyResource> resources, List<String> users, List<String> groups, String... accesses) {
        RangerPolicy                 policy = new RangerPolicy();
        List<RangerPolicyItemAccess> list   = new ArrayList<>();

        for (String access : accesses) {
            list.add(new RangerPolicyItemAccess(access));
        }

        policy.setId(id);
        policy.setService(SERVICE);
        policy.setName("policy " + id);
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        policy.setResources(resources);
        policy.setPolicyItems(Collections.singletonList(new RangerPolicyItem(list, users, groups, null, null, false)));

        return policy;
    }

    private static Map<String, RangerPolicyResource> resources(String... nameValues) {
        Map<String, RangerPolicyResource> ret = new HashMap<>();

        for (int i = 0; i < nameValues.length; i += 2) {
            ret.put(nameValues[i], new RangerPolicyResource(nameValues[i + 1]));
        }

        return ret;
    }

    /** Counts the decisions that reached Ranger (each is audited). */
    static final class Counting implements RangerAccessResultProcessor {
        final AtomicInteger count = new AtomicInteger();

        @Override
        public void processResult(RangerAccessResult result) {
            count.incrementAndGet();
        }

        @Override
        public void processResults(Collection<RangerAccessResult> results) {
            count.addAndGet(results.size());
        }
    }
}
