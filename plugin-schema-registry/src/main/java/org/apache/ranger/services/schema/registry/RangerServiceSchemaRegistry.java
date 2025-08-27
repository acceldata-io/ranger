/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.services.schema.registry;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.services.schema.registry.client.AutocompletionAgent;
import org.apache.ranger.services.schema.registry.client.SchemaRegistryResourceMgr;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

public class RangerServiceSchemaRegistry extends RangerBaseService {

    public static final String ACCESS_TYPE_CREATE = "create";
    public static final String ACCESS_TYPE_UPDATE = "update";
    public static final String ACCESS_TYPE_READ  = "read";
    public static final String ACCESS_TYPE_DELETE = "delete";

    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceSchemaRegistry.class);


    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    @Override
    public HashMap<String, Object> validateConfig() {
        HashMap<String, Object> ret = new HashMap<String, Object>();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceSchemaRegistry.validateConfig(" + serviceName + ")");
        }

        if (configs != null) {
            try {
                AutocompletionAgent autocompletionAgent = new AutocompletionAgent(serviceName, configs);
                ret = autocompletionAgent.connectionTest();
            } catch (Exception e) {
                LOG.error("<== RangerServiceSchemaRegistry.validateConfig Error:" + e);
                throw e;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceSchemaRegistry.validateConfig(" + serviceName + "): ret=" + ret);
        }

        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<String>();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceSchemaRegistry.lookupResource(" + serviceName + ")");
        }

        if (configs != null) {
            AutocompletionAgent autocompletionAgent = new AutocompletionAgent(serviceName, configs);
            ret = SchemaRegistryResourceMgr.getSchemaRegistryResources(serviceName, configs, context, autocompletionAgent);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceSchemaRegistry.lookupResource(" + serviceName + "): ret=" + ret);
        }

        return ret;
    }

    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceSchemaRegistry.getDefaultRangerPolicies() ");
        }

        List<RangerPolicy> ret = super.getDefaultRangerPolicies();

        for (RangerPolicy defaultPolicy : ret) {
            final Map<String, RangerPolicy.RangerPolicyResource> policyResources = defaultPolicy.getResources();
            if (defaultPolicy.getName().contains("all") && StringUtils.isNotBlank(lookUpUser)) {
                RangerPolicy.RangerPolicyItem policyItemForLookupUser = new RangerPolicy.RangerPolicyItem();
                List<RangerPolicy.RangerPolicyItemAccess> accessListForLookupUser = new ArrayList<>();
                accessListForLookupUser.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_READ));
                policyItemForLookupUser.setUsers(Collections.singletonList(lookUpUser));
                policyItemForLookupUser.setAccesses(accessListForLookupUser);
                policyItemForLookupUser.setDelegateAdmin(false);
                defaultPolicy.addPolicyItem(policyItemForLookupUser);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceSchemaRegistry.getDefaultRangerPolicies() ");
        }
        return ret;
    }

}
