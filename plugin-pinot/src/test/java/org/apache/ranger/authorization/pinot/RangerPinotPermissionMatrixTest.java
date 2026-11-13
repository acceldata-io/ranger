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
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Permission matrix of the Ranger checker for Pinot: every access type, every resource level, wildcards and excludes,
 * several users / groups / policies, deny items and exceptions, disabled policies, the time-of-day condition and
 * validity schedules.
 */
public class RangerPinotPermissionMatrixTest {
    static final List<String> ACCESS_TYPES = Arrays.asList("query", "read_metadata", "manage_table", "manage_segments",
            "manage_ingestion", "rebalance", "manage_tasks", "manage_quotas", "manage_database", "manage_queries",
            "manage_tenants_instances", "manage_cluster_config", "manage_zookeeper", "manage_users");

    private static final String T  = PinotAccessChecker.RESOURCE_TABLE;
    private static final String D  = PinotAccessChecker.RESOURCE_DATABASE;
    private static final String CL = PinotAccessChecker.RESOURCE_CLUSTER;

    private long nextId = 1;

    @Test
    public void serviceDefHasEveryAccessTypeAndAllImpliesThem() {
        List<String> names = new ArrayList<>();

        for (org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef def : RangerPinotAccessCheckerTest.serviceDef().getAccessTypes()) {
            names.add(def.getName());

            if ("all".equals(def.getName())) {
                assertEquals(new HashSet<>(ACCESS_TYPES), new HashSet<>(def.getImpliedGrants()));
            }
        }

        List<String> expected = new ArrayList<>(ACCESS_TYPES);

        expected.add("all");
        assertEquals(expected, names);
    }

    @Test
    public void eachAccessTypeGrantsOnlyItself() {
        for (String granted : ACCESS_TYPES) {
            RangerPinotAccessChecker checker = checker(
                    allow(res("database", "default", "table", "orders"), users("u"), groups(), granted),
                    allow(res("database", "default"), users("u"), groups(), granted),
                    allow(res("cluster", "*"), users("u"), groups(), granted));

            for (String asked : ACCESS_TYPES) {
                boolean expected = asked.equals(granted);

                assertEquals(granted + " grants " + asked + " on table?", expected, check(checker, "u", groups(), T, "default", "orders", asked));
                assertEquals(granted + " grants " + asked + " on database?", expected, check(checker, "u", groups(), D, "default", null, asked));
                assertEquals(granted + " grants " + asked + " on cluster?", expected, check(checker, "u", groups(), CL, null, null, asked));
            }
        }
    }

    @Test
    public void allGrantsEveryAccessTypeOnItsResourceOnly() {
        RangerPinotAccessChecker checker = checker(allow(res("database", "sales", "table", "*"), users("owner"), groups(), "all"));

        for (String asked : ACCESS_TYPES) {
            assertTrue(asked, check(checker, "owner", groups(), T, "sales", "orders", asked));
            assertFalse("other database: " + asked, check(checker, "owner", groups(), T, "default", "orders", asked));
            assertFalse("cluster: " + asked, check(checker, "owner", groups(), CL, null, null, asked));
        }
    }

    @Test
    public void resourceLevelsWildcardsAndExcludes() {
        RangerPolicyResource excludedTables = new RangerPolicyResource(Arrays.asList("secret*", "audit"), true, false);
        Map<String, RangerPolicyResource> notSecret = new HashMap<>();

        notSecret.put("database", new RangerPolicyResource("fin"));
        notSecret.put("table", excludedTables);

        RangerPinotAccessChecker checker = checker(
                allow(res("database", "sal*", "table", "ord?rs"), users("w"), groups(), "query"),
                allow(notSecret, users("x"), groups(), "query"),
                allow(res("database", "hr"), users("dba"), groups(), "manage_database"),
                allow(res("cluster", "*"), users("ops"), groups(), "manage_cluster_config"));

        assertTrue("database wildcard + table ?-wildcard", check(checker, "w", groups(), T, "sales_eu", "orders", "query"));
        assertFalse("? matches exactly one character", check(checker, "w", groups(), T, "sales_eu", "ordrs", "query"));
        assertFalse(check(checker, "w", groups(), T, "marketing", "orders", "query"));
        assertTrue("table names are case sensitive", !check(checker, "w", groups(), T, "sales", "ORDERS", "query"));

        assertTrue("excludes: other tables allowed", check(checker, "x", groups(), T, "fin", "ledger", "query"));
        assertFalse("excludes: secret*", check(checker, "x", groups(), T, "fin", "secret_salaries", "query"));
        assertFalse("excludes: audit", check(checker, "x", groups(), T, "fin", "audit", "query"));

        assertTrue("database-level policy, database request", check(checker, "dba", groups(), D, "hr", null, "manage_database"));
        assertFalse("database-only policy does not cover the tables (that needs table=*)", check(checker, "dba", groups(), T, "hr", "staff", "manage_database"));
        assertFalse(check(checker, "dba", groups(), D, "fin", null, "manage_database"));

        assertTrue("cluster policy", check(checker, "ops", groups(), CL, null, null, "manage_cluster_config"));
        assertFalse("cluster policy does not cover tables", check(checker, "ops", groups(), T, "default", "orders", "manage_cluster_config"));
    }

    @Test
    public void multipleUsersGroupsAndPoliciesCombine() {
        RangerPinotAccessChecker checker = checker(
                allow(res("database", "default", "table", "orders"), users("alice", "bob"), groups(), "query"),
                allow(res("database", "default", "table", "orders"), users(), groups("analysts", "auditors"), "read_metadata"),
                allow(res("database", "default", "table", "orders"), users("carol"), groups("loaders"), "manage_segments", "manage_ingestion"),
                allow(res("database", "default", "table", "*"), users(), groups("public-readers"), "read_metadata"));

        assertTrue(check(checker, "alice", groups(), T, "default", "orders", "query"));
        assertTrue(check(checker, "bob", groups(), T, "default", "orders", "query"));
        assertFalse("user policy does not grant read_metadata", check(checker, "bob", groups(), T, "default", "orders", "read_metadata"));
        assertTrue("user + group from different policies", check(checker, "alice", groups("analysts"), T, "default", "orders", "read_metadata"));
        assertTrue("second group of the item", check(checker, "dave", groups("auditors"), T, "default", "orders", "read_metadata"));
        assertFalse(check(checker, "dave", groups("auditors"), T, "default", "orders", "query"));
        assertTrue("user OR group in one item", check(checker, "carol", groups(), T, "default", "orders", "manage_ingestion"));
        assertTrue(check(checker, "erin", groups("loaders"), T, "default", "orders", "manage_segments"));
        assertTrue("wildcard policy for a group", check(checker, "frank", groups("public-readers"), T, "default", "anything", "read_metadata"));
        assertFalse("unknown user", check(checker, "mallory", groups("guests"), T, "default", "orders", "read_metadata"));
    }

    @Test
    public void denyItemsAndExceptions() {
        RangerPolicy policy = allow(res("database", "default", "table", "*"), users(), groups("analysts"), "query", "read_metadata");

        // allow analysts except intern-group, deny contractor everywhere, deny-exception for lead contractor
        policy.setAllowExceptions(Collections.singletonList(item(users(), groups("interns"), "query")));
        policy.setDenyPolicyItems(Collections.singletonList(item(users("contractor", "lead"), groups(), "query")));
        policy.setDenyExceptions(Collections.singletonList(item(users("lead"), groups(), "query")));

        RangerPinotAccessChecker checker = checker(policy,
                allow(res("database", "default", "table", "orders"), users("contractor"), groups(), "query"));

        assertTrue(check(checker, "ann", groups("analysts"), T, "default", "orders", "query"));
        assertFalse("allow exception", check(checker, "ian", groups("analysts", "interns"), T, "default", "orders", "query"));
        assertTrue("allow exception only for query", check(checker, "ian", groups("analysts", "interns"), T, "default", "orders", "read_metadata"));
        assertFalse("deny beats an allow from another policy", check(checker, "contractor", groups(), T, "default", "orders", "query"));
        assertTrue("deny exception", check(checker, "lead", groups("analysts"), T, "default", "orders", "query"));
    }

    @Test
    public void disabledPoliciesHaveNoEffect() {
        RangerPolicy disabled = allow(res("database", "default", "table", "orders"), users("alice"), groups(), "query");

        disabled.setIsEnabled(false);

        RangerPinotAccessChecker checker = checker(disabled);

        assertFalse(check(checker, "alice", groups(), T, "default", "orders", "query"));
    }

    @Test
    public void timeOfDayCondition() {
        Calendar now   = Calendar.getInstance();
        int      hour  = now.get(Calendar.HOUR_OF_DAY);
        String   open  = "12am-11:59pm";
        // a two-hour window that ends before the current hour (or starts after it)
        String   closed = hour >= 3 ? hour12(hour - 3) + "-" + hour12(hour - 2) : hour12(hour + 2) + "-" + hour12(hour + 3);

        RangerPolicy always = allow(res("database", "default", "table", "orders"), users("day"), groups(), "query");
        RangerPolicy never  = allow(res("database", "default", "table", "orders"), users("night"), groups(), "query");

        always.getPolicyItems().get(0).setConditions(Collections.singletonList(new RangerPolicyItemCondition("time-of-day", Collections.singletonList(open))));
        never.getPolicyItems().get(0).setConditions(Collections.singletonList(new RangerPolicyItemCondition("time-of-day", Collections.singletonList(closed))));

        RangerPinotAccessChecker checker = checker(always, never);

        assertTrue("inside the window", check(checker, "day", groups(), T, "default", "orders", "query"));
        assertFalse("outside the window " + closed, check(checker, "night", groups(), T, "default", "orders", "query"));
    }

    @Test
    public void validitySchedules() {
        SimpleDateFormat fmt     = new SimpleDateFormat(RangerValiditySchedule.VALIDITY_SCHEDULE_DATE_STRING_SPECIFICATION);
        long             day     = 24L * 3600 * 1000;
        Date             now     = new Date();
        RangerPolicy     current = allow(res("database", "default", "table", "orders"), users("temp"), groups(), "query");
        RangerPolicy     expired = allow(res("database", "default", "table", "orders"), users("old"), groups(), "query");

        current.setValiditySchedules(Collections.singletonList(new RangerValiditySchedule(fmt.format(new Date(now.getTime() - day)), fmt.format(new Date(now.getTime() + day)), null, null)));
        expired.setValiditySchedules(Collections.singletonList(new RangerValiditySchedule(fmt.format(new Date(now.getTime() - 3 * day)), fmt.format(new Date(now.getTime() - 2 * day)), null, null)));

        RangerPinotAccessChecker checker = checker(current, expired);

        assertTrue(check(checker, "temp", groups(), T, "default", "orders", "query"));
        assertFalse("expired policy", check(checker, "old", groups(), T, "default", "orders", "query"));
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static String hour12(int h) {
        h = ((h % 24) + 24) % 24;
        return (h % 12 == 0 ? 12 : h % 12) + (h < 12 ? "am" : "pm");
    }

    private static boolean check(RangerPinotAccessChecker checker, String user, Set<String> groups, String type, String database, String table, String access) {
        return checker.checkAccess(user, groups, type, database, table, access, "action").getKey();
    }

    private RangerPinotAccessChecker checker(RangerPolicy... policies) {
        ServicePolicies servicePolicies = new ServicePolicies();

        servicePolicies.setServiceName("pinotdev");
        servicePolicies.setServiceDef(RangerPinotAccessCheckerTest.serviceDef());
        servicePolicies.setPolicies(Arrays.asList(policies));
        servicePolicies.setPolicyVersion(1L);

        RangerBasePlugin plugin = new RangerBasePlugin(new RangerPluginConfig("pinot", "pinotdev", "pinot", null, null, null));

        plugin.setPolicies(servicePolicies);

        return new RangerPinotAccessChecker(plugin, null, "pinot", 0);
    }

    private RangerPolicy allow(Map<String, RangerPolicyResource> resources, List<String> users, Set<String> groups, String... accesses) {
        RangerPolicy policy = new RangerPolicy();

        policy.setId(nextId);
        policy.setName("policy " + nextId++);
        policy.setService("pinotdev");
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        policy.setResources(resources);
        policy.setPolicyItems(new ArrayList<>(Collections.singletonList(item(users, groups, accesses))));

        return policy;
    }

    private static RangerPolicyItem item(List<String> users, Set<String> groups, String... accesses) {
        List<RangerPolicyItemAccess> list = new ArrayList<>();

        for (String access : accesses) {
            list.add(new RangerPolicyItemAccess(access));
        }

        return new RangerPolicyItem(list, users, new ArrayList<>(groups), null, null, false);
    }

    private static Map<String, RangerPolicyResource> res(String... nameValues) {
        Map<String, RangerPolicyResource> ret = new HashMap<>();

        for (int i = 0; i < nameValues.length; i += 2) {
            ret.put(nameValues[i], new RangerPolicyResource(nameValues[i + 1]));
        }

        return ret;
    }

    private static List<String> users(String... users) {
        return Arrays.asList(users);
    }

    private static Set<String> groups(String... groups) {
        return new HashSet<>(Arrays.asList(groups));
    }
}
