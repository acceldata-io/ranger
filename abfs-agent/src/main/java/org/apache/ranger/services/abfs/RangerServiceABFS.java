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
package org.apache.ranger.services.abfs;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.abfs.client.ABFSResourceMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RangerServiceABFS extends RangerBaseService {

    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceABFS.class);

    public RangerServiceABFS() {
        super();
    }

    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret = null;
        String serviceName = getServiceName();
        LOG.info("==> RangerServiceABFS.validateConfig Service: ({})", serviceName);

        if (configs != null) {
            try {
                ret = ABFSResourceMgr.connectionTest(serviceName, configs);
            } catch (Exception e) {
                LOG.error("<== RangerServiceABFS.validateConfig Error: {}", e.getMessage(), e);
                throw e;
            }
        }

        LOG.info("<== RangerServiceABFS.validateConfig Response: ({})", ret);
        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<>();
        String serviceName = getServiceName();
        Map<String, String> svcConfigs = getConfigs();

        LOG.info("==> RangerServiceABFS.lookupResource Context: ({})", context);

        if (context != null) {
            try {
                long timeLookup = Long.parseLong(
                        this.config.getProperties().getProperty("ranger.resource.lookup.timeout.value.in.ms", "10000"));
                ret = ABFSResourceMgr.getABFSResources(serviceName, svcConfigs, context, timeLookup);
            } catch (Exception e) {
                LOG.error("<== RangerServiceABFS.lookupResource Error: {}", e.getMessage(), e);
                throw e;
            }
        }

        LOG.info("<== RangerServiceABFS.lookupResource Response Received");
        return ret;
    }

    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceABFS.getDefaultRangerPolicies()");
        }

        List<RangerPolicy> ret = super.getDefaultRangerPolicies();

        for (RangerPolicy defaultPolicy : ret) {
            String lookupUser = configs.get(RangerABFSConstants.USER_NAME);
            String container  = configs.get(RangerABFSConstants.DEFAULT_CONTAINER);

            if (defaultPolicy.getName().contains("all") && StringUtils.isNotBlank(lookupUser)) {
                RangerPolicy.RangerPolicyResource containerResource =
                        defaultPolicy.getResources().get(RangerABFSConstants.CONTAINER);
                if (containerResource != null && StringUtils.isNotBlank(container)) {
                    containerResource.setValues(Collections.singletonList(container));
                }

                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
                List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
                accesses.add(new RangerPolicy.RangerPolicyItemAccess(RangerABFSConstants.ACCESS_TYPE_READ, true));
                accesses.add(new RangerPolicy.RangerPolicyItemAccess(RangerABFSConstants.ACCESS_TYPE_LIST, true));
                policyItem.setAccesses(accesses);
                policyItem.setUsers(Collections.singletonList(lookupUser));
                policyItem.setDelegateAdmin(true);

                defaultPolicy.getPolicyItems().add(policyItem);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceABFS.getDefaultRangerPolicies()");
        }

        return ret;
    }
}
