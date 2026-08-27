/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.services.abfs.client;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.services.abfs.RangerABFSConstants;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ABFSAclSyncServiceTest {
    private static final String USER_ID = "12345678-1234-1234-1234-123456789012";

    @Test
    void unionsGrantsFromRecursiveParentAndExplicitChild() {
        RangerPolicy parent = policy("account", "container", "/a", true, "read");
        RangerPolicy child = policy("account", "container", "/a/b", false, "write");
        Map<String, String> configs = configs();
        ABFSAclSyncService service = new ABFSAclSyncService();

        List<ABFSAclSyncService.DesiredGrant> grants = service.buildEffectiveGrants(
                Arrays.asList(parent, child), new ABFSIdentityResolver(configs),
                "container", "a/b", configs);

        assertEquals(1, grants.size());
        assertEquals(USER_ID, grants.get(0).getIdentity().getObjectId());
        assertTrue(grants.get(0).getFlags().isRead());
        assertTrue(grants.get(0).getFlags().isWrite());
    }

    @Test
    void matchesWildcardContainerAndPath() {
        RangerPolicy wildcard = policy("account", "*", "/reports/*", false, "read");

        assertTrue(ABFSAclSyncService.policyAppliesToPath(
                wildcard, "data-prod", "reports/2026", configs()));
        assertTrue(ABFSAclSyncService.policyAppliesToPath(
                wildcard, "archive", "reports/2026", configs()));
    }

    @Test
    void doesNotApplyNonRecursiveParentToChild() {
        RangerPolicy parent = policy("account", "container", "/a", false, "read");

        assertFalse(ABFSAclSyncService.policyAppliesToPath(
                parent, "container", "a/b", configs()));
    }

    private static RangerPolicy policy(String account, String container, String path,
                                       boolean recursive, String accessType) {
        RangerPolicy policy = new RangerPolicy();
        policy.setIsEnabled(true);
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);

        Map<String, RangerPolicyResource> resources = new LinkedHashMap<>();
        resources.put(RangerABFSConstants.STORAGE_ACCOUNT_RESOURCE, resource(account, false));
        resources.put(RangerABFSConstants.CONTAINER, resource(container, false));
        resources.put(RangerABFSConstants.RELATIVE_PATH, resource(path, recursive));
        policy.setResources(resources);

        RangerPolicyItemAccess access = new RangerPolicyItemAccess();
        access.setType(accessType);
        access.setIsAllowed(true);

        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(Arrays.asList(USER_ID));
        item.setAccesses(Arrays.asList(access));
        policy.setPolicyItems(Arrays.asList(item));
        return policy;
    }

    private static RangerPolicyResource resource(String value, boolean recursive) {
        RangerPolicyResource resource = new RangerPolicyResource();
        resource.setValues(Arrays.asList(value));
        resource.setIsRecursive(recursive);
        return resource;
    }

    private static Map<String, String> configs() {
        Map<String, String> configs = new HashMap<>();
        configs.put(RangerABFSConstants.STORAGE_ACCOUNT, "account");
        return configs;
    }
}
