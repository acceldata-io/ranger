/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.yunikorn.ranger;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link AclConverter}. Organised by concern:
 *
 * <ul>
 *   <li><b>Happy paths</b> — typical inputs producing the right output.</li>
 *   <li><b>Filtering</b> — disabled policies, deny rules, isAllowed=false,
 *       missing resources are correctly skipped.</li>
 *   <li><b>Merging</b> — multiple policies for the same queue, multiple
 *       queue values in one policy.</li>
 *   <li><b>ACL string formatting</b> — users-only, groups-only, both,
 *       neither, deterministic ordering.</li>
 *   <li><b>Roles (v1 limitation)</b> — users/groups in the same item still
 *       apply; role-only items produce no entry.</li>
 * </ul>
 */
class AclConverterTest {

    private final AclConverter converter = new AclConverter();

    // -----------------------------------------------------------------------
    // Happy paths
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Happy paths")
    class HappyPaths {

        @Test
        @DisplayName("Single policy with submit grant produces one entry")
        void singlePolicySubmit() {
            RangerPolicy policy = builder()
                    .name("nlp-team")
                    .queue("root.research.nlp")
                    .policyItem(item().users("alice", "bob").groups("devs").submit())
                    .build();

            Map<String, QueueAclEntry> result = converter.convert(List.of(policy));

            assertThat(result).hasSize(1);
            QueueAclEntry entry = result.get("root.research.nlp");
            assertThat(entry).isNotNull();
            assertThat(entry.submitAcl()).isEqualTo("alice,bob devs");
            assertThat(entry.adminAcl()).isNull();
        }

        @Test
        @DisplayName("Policy with admin grant only sets adminAcl, leaves submitAcl null")
        void singlePolicyAdmin() {
            RangerPolicy policy = builder()
                    .name("prod-admins")
                    .queue("root.production")
                    .policyItem(item().groups("sre-team").admin())
                    .build();

            Map<String, QueueAclEntry> result = converter.convert(List.of(policy));
            QueueAclEntry entry = result.get("root.production");
            assertThat(entry.submitAcl()).isNull();
            assertThat(entry.adminAcl()).isEqualTo(" sre-team");   // groups-only → leading space
        }

        @Test
        @DisplayName("Policy with both submit and admin populates both ACLs")
        void singlePolicyBothAccessTypes() {
            RangerPolicy policy = builder()
                    .name("dual-grant")
                    .queue("root.research")
                    .policyItem(item().users("alice").submit().admin())
                    .build();

            QueueAclEntry entry = converter.convert(List.of(policy)).get("root.research");
            assertThat(entry.submitAcl()).isEqualTo("alice");
            assertThat(entry.adminAcl()).isEqualTo("alice");
        }

        @Test
        @DisplayName("Empty policy list produces empty map")
        void emptyPolicyList() {
            assertThat(converter.convert(List.of())).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Filtering
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Filtering")
    class Filtering {

        @Test
        @DisplayName("Disabled policy is skipped")
        void disabledPolicy() {
            RangerPolicy policy = builder()
                    .name("disabled")
                    .queue("root.research")
                    .policyItem(item().users("alice").submit())
                    .disabled()
                    .build();

            assertThat(converter.convert(List.of(policy))).isEmpty();
        }

        @Test
        @DisplayName("Policy with no resources is skipped")
        void missingResources() {
            RangerPolicy policy = new RangerPolicy();
            policy.setName("no-resources");
            policy.setIsEnabled(true);
            policy.setPolicyItems(List.of(item().users("alice").submit().build()));
            // resources stays null

            assertThat(converter.convert(List.of(policy))).isEmpty();
        }

        @Test
        @DisplayName("Policy with no queue resource is skipped")
        void missingQueueResource() {
            RangerPolicy policy = new RangerPolicy();
            policy.setName("wrong-resource-name");
            policy.setIsEnabled(true);
            Map<String, RangerPolicyResource> resources = new HashMap<>();
            RangerPolicyResource other = new RangerPolicyResource();
            other.setValues(List.of("root.research"));
            resources.put("not-queue", other);
            policy.setResources(resources);
            policy.setPolicyItems(List.of(item().users("alice").submit().build()));

            assertThat(converter.convert(List.of(policy))).isEmpty();
        }

        @Test
        @DisplayName("Access entry with isAllowed=false is treated as no grant")
        void isAllowedFalse() {
            RangerPolicy policy = builder()
                    .name("denied-access")
                    .queue("root.research")
                    .policyItem(item().users("alice").submitDenied())
                    .build();

            assertThat(converter.convert(List.of(policy))).isEmpty();
        }

        @Test
        @DisplayName("Policy with empty policyItems list is skipped")
        void emptyPolicyItems() {
            RangerPolicy policy = new RangerPolicy();
            policy.setName("no-items");
            policy.setIsEnabled(true);
            policy.setResources(queueResource("root.research"));
            policy.setPolicyItems(Collections.emptyList());

            assertThat(converter.convert(List.of(policy))).isEmpty();
        }

        @Test
        @DisplayName("Policy item with neither users nor groups is skipped")
        void emptyUsersAndGroups() {
            RangerPolicy policy = builder()
                    .name("nobody")
                    .queue("root.research")
                    .policyItem(item().submit())   // no users, no groups
                    .build();
            assertThat(converter.convert(List.of(policy))).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Merging
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Merging")
    class Merging {

        @Test
        @DisplayName("Two policies on the same queue union their users and groups")
        void unionAcrossPolicies() {
            RangerPolicy p1 = builder()
                    .name("policy-1")
                    .queue("root.research")
                    .policyItem(item().users("alice").groups("devs").submit())
                    .build();
            RangerPolicy p2 = builder()
                    .name("policy-2")
                    .queue("root.research")
                    .policyItem(item().users("bob").groups("ml-team").submit())
                    .build();

            QueueAclEntry entry = converter.convert(List.of(p1, p2)).get("root.research");
            assertThat(entry.submitAcl()).isEqualTo("alice,bob devs,ml-team");
        }

        @Test
        @DisplayName("Multiple policy items in one policy are unioned")
        void unionAcrossPolicyItems() {
            RangerPolicy policy = builder()
                    .name("multi-item")
                    .queue("root.research")
                    .policyItem(item().users("alice").submit())
                    .policyItem(item().users("bob").groups("devs").submit())
                    .build();

            QueueAclEntry entry = converter.convert(List.of(policy)).get("root.research");
            assertThat(entry.submitAcl()).isEqualTo("alice,bob devs");
        }

        @Test
        @DisplayName("Single policy with multiple queue values produces multiple entries")
        void multipleQueueValues() {
            RangerPolicy policy = builder()
                    .name("multi-queue")
                    .queue("root.research", "root.production")
                    .policyItem(item().users("alice").submit())
                    .build();

            Map<String, QueueAclEntry> result = converter.convert(List.of(policy));
            assertThat(result).hasSize(2);
            assertThat(result.get("root.research").submitAcl()).isEqualTo("alice");
            assertThat(result.get("root.production").submitAcl()).isEqualTo("alice");
        }

        @Test
        @DisplayName("Submit and admin grants from different policies don't bleed")
        void submitAndAdminIndependent() {
            RangerPolicy submitPolicy = builder()
                    .name("submitters")
                    .queue("root.research")
                    .policyItem(item().users("alice").submit())
                    .build();
            RangerPolicy adminPolicy = builder()
                    .name("admins")
                    .queue("root.research")
                    .policyItem(item().users("admin-user").admin())
                    .build();

            QueueAclEntry entry = converter.convert(List.of(submitPolicy, adminPolicy))
                    .get("root.research");
            assertThat(entry.submitAcl()).isEqualTo("alice");
            assertThat(entry.adminAcl()).isEqualTo("admin-user");
        }

        @Test
        @DisplayName("Duplicate users across policies are deduplicated")
        void deduplication() {
            RangerPolicy p1 = builder()
                    .name("p1")
                    .queue("root.research")
                    .policyItem(item().users("alice", "bob").submit())
                    .build();
            RangerPolicy p2 = builder()
                    .name("p2")
                    .queue("root.research")
                    .policyItem(item().users("bob", "charlie").submit())
                    .build();

            QueueAclEntry entry = converter.convert(List.of(p1, p2)).get("root.research");
            assertThat(entry.submitAcl()).isEqualTo("alice,bob,charlie");
        }
    }

    // -----------------------------------------------------------------------
    // ACL string formatting
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("ACL string formatting")
    class Formatting {

        @Test
        @DisplayName("Users only — no trailing space")
        void usersOnly() {
            assertThat(AclConverter.formatAcl(set("alice", "bob"), null))
                    .isEqualTo("alice,bob");
            assertThat(AclConverter.formatAcl(set("alice", "bob"), set()))
                    .isEqualTo("alice,bob");
        }

        @Test
        @DisplayName("Groups only — leading space, empty user side")
        void groupsOnly() {
            assertThat(AclConverter.formatAcl(null, set("devs", "sre")))
                    .isEqualTo(" devs,sre");
            assertThat(AclConverter.formatAcl(set(), set("devs", "sre")))
                    .isEqualTo(" devs,sre");
        }

        @Test
        @DisplayName("Both users and groups — single space separator")
        void usersAndGroups() {
            assertThat(AclConverter.formatAcl(set("alice"), set("devs")))
                    .isEqualTo("alice devs");
        }

        @Test
        @DisplayName("Neither users nor groups returns null")
        void neither() {
            assertThat(AclConverter.formatAcl(null, null)).isNull();
            assertThat(AclConverter.formatAcl(set(), set())).isNull();
        }

        @Test
        @DisplayName("Output is sorted alphabetically for determinism")
        void deterministicOrdering() {
            // TreeSet handles sorting; this confirms the contract works
            // through formatAcl when users come from a TreeSet.
            assertThat(AclConverter.formatAcl(set("charlie", "alice", "bob"), set("zeta", "alpha")))
                    .isEqualTo("alice,bob,charlie alpha,zeta");
        }
    }

    // -----------------------------------------------------------------------
    // Roles (v1 limitation)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Roles (v1 limitation)")
    class Roles {

        @Test
        @DisplayName("Item with users + groups + roles still applies the users and groups")
        void rolesPlusUsersAndGroups() {
            RangerPolicy policy = builder()
                    .name("mixed")
                    .queue("root.research")
                    .policyItem(item().users("alice").groups("devs").roles("data-leads").submit())
                    .build();

            QueueAclEntry entry = converter.convert(List.of(policy)).get("root.research");
            assertThat(entry).isNotNull();
            assertThat(entry.submitAcl()).isEqualTo("alice devs");
        }

        @Test
        @DisplayName("Item with roles only produces no entry (no users/groups to add)")
        void rolesOnly() {
            RangerPolicy policy = builder()
                    .name("roles-only")
                    .queue("root.research")
                    .policyItem(item().roles("data-leads").submit())
                    .build();
            assertThat(converter.convert(List.of(policy))).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Builder helpers — keep tests readable
    // -----------------------------------------------------------------------

    private static PolicyBuilder builder() { return new PolicyBuilder(); }
    private static ItemBuilder   item()    { return new ItemBuilder(); }

    private static TreeSet<String> set(String... values) {
        return new TreeSet<>(Arrays.asList(values));
    }

    private static Map<String, RangerPolicyResource> queueResource(String... values) {
        Map<String, RangerPolicyResource> resources = new HashMap<>();
        RangerPolicyResource queue = new RangerPolicyResource();
        queue.setValues(Arrays.asList(values));
        resources.put(AclConverter.RESOURCE_QUEUE, queue);
        return resources;
    }

    private static class PolicyBuilder {
        private String name = "test-policy";
        private boolean enabled = true;
        private Map<String, RangerPolicyResource> resources = new HashMap<>();
        private final List<RangerPolicyItem> items = new java.util.ArrayList<>();
        private static long nextId = 1;

        PolicyBuilder name(String n) { this.name = n; return this; }
        PolicyBuilder disabled()     { this.enabled = false; return this; }

        PolicyBuilder queue(String... values) {
            this.resources = queueResource(values);
            return this;
        }

        PolicyBuilder policyItem(ItemBuilder ib) {
            items.add(ib.build());
            return this;
        }

        RangerPolicy build() {
            RangerPolicy p = new RangerPolicy();
            p.setId(nextId++);
            p.setName(name);
            p.setIsEnabled(enabled);
            p.setResources(resources);
            p.setPolicyItems(items);
            return p;
        }
    }

    private static class ItemBuilder {
        private final List<String> users    = new java.util.ArrayList<>();
        private final List<String> groups   = new java.util.ArrayList<>();
        private final List<String> roles    = new java.util.ArrayList<>();
        private final List<RangerPolicyItemAccess> accesses = new java.util.ArrayList<>();

        ItemBuilder users(String... v)  { users.addAll(Arrays.asList(v));  return this; }
        ItemBuilder groups(String... v) { groups.addAll(Arrays.asList(v)); return this; }
        ItemBuilder roles(String... v)  { roles.addAll(Arrays.asList(v));  return this; }

        ItemBuilder submit()        { return access(AclConverter.ACCESS_SUBMIT, true); }
        ItemBuilder admin()         { return access(AclConverter.ACCESS_ADMIN,  true); }
        ItemBuilder submitDenied()  { return access(AclConverter.ACCESS_SUBMIT, false); }

        private ItemBuilder access(String type, boolean allowed) {
            RangerPolicyItemAccess a = new RangerPolicyItemAccess();
            a.setType(type);
            a.setIsAllowed(allowed);
            accesses.add(a);
            return this;
        }

        RangerPolicyItem build() {
            RangerPolicyItem item = new RangerPolicyItem();
            item.setUsers(users);
            item.setGroups(groups);
            item.setRoles(roles);
            item.setAccesses(accesses);
            return item;
        }
    }
}
