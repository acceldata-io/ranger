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
package org.apache.ranger.services.kudu;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RangerService for Apache Kudu.
 */
public class RangerServiceKudu extends RangerBaseService {
    public static final String ACCESS_TYPE_METADATA = "metadata";

    @Override
    public HashMap<String, Object> validateConfig() {
        HashMap<String, Object> responseData = new HashMap<String, Object>();
        // The Kudu service definition has no connection settings (no master address, user or
        // password): the Kudu master pulls policies from Ranger, Ranger never connects to Kudu.
        // There is nothing that can fail here, so report success instead of a permanent
        // "Connection Failed" for a correctly configured service.
        String message = "Kudu has no connection settings to validate. Policies are downloaded by the Kudu master.";
        BaseClient.generateResponseDataMap(true, message, message, null, null, responseData);
        return responseData;
    }

    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        List<RangerPolicy> ret = super.getDefaultRangerPolicies();

        // The Kudu service definition does not enable per-hierarchy default policies, so a new
        // service starts with none and the lookup user cannot even list databases and tables.
        if (StringUtils.isNotBlank(lookUpUser)) {
            Map<String, RangerPolicyResource> resources = new HashMap<>();

            resources.put("database", new RangerPolicyResource("*", false, false));
            resources.put("table", new RangerPolicyResource("*", false, false));
            resources.put("column", new RangerPolicyResource("*", false, false));

            RangerPolicyItem item = new RangerPolicyItem();

            item.setUsers(Collections.singletonList(lookUpUser));
            item.setAccesses(Collections.singletonList(new RangerPolicyItemAccess(ACCESS_TYPE_METADATA)));
            item.setDelegateAdmin(false);

            RangerPolicy policy = new RangerPolicy();

            policy.setIsEnabled(true);
            policy.setVersion(1L);
            policy.setName("lookup - database, table, column");
            policy.setService(service.getName());
            policy.setDescription("Lets the Ranger lookup user browse Kudu databases and tables (metadata only)");
            policy.setIsAuditEnabled(true);
            policy.setResources(resources);
            policy.setPolicyItems(new ArrayList<>(Collections.singletonList(item)));

            ret.add(policy);
        }

        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        // TODO: implement resource lookup for Kudu policies.
        return new ArrayList<>();
    }
}
