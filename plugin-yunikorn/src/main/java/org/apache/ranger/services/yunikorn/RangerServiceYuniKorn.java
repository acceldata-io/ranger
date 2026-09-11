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

package org.apache.ranger.services.yunikorn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.yunikorn.client.YuniKornResourceMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ranger service implementation for Apache YuniKorn.
 *
 * <p>Loaded into Ranger Admin's JVM at startup to:
 * <ul>
 *   <li>validate connectivity to a YuniKorn REST endpoint when an admin
 *       registers a {@code yunikorn} service in the Ranger UI</li>
 *   <li>populate queue dropdowns when authoring policies, by querying the
 *       YuniKorn REST API for the live queue tree</li>
 *   <li>seed default {@code all - queue} policies on service creation</li>
 * </ul>
 *
 * <p>This class does <em>not</em> push policies into YuniKorn or perform
 * runtime authorization; that work lives elsewhere (e.g. the in-cluster
 * sidecar agent that pulls policies from Ranger Admin and patches the
 * YuniKorn ConfigMap).
 */
public class RangerServiceYuniKorn extends RangerBaseService {

    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceYuniKorn.class);

    public RangerServiceYuniKorn() {
        super();
    }

    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret = new HashMap<>();
        String serviceName = getServiceName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceYuniKorn.validateConfig service=[{}]", serviceName);
        }

        if (configs != null) {
            try {
                ret = YuniKornResourceMgr.validateConfig(serviceName, configs);
            } catch (Exception e) {
                LOG.error("<== RangerServiceYuniKorn.validateConfig failed", e);
                throw e;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceYuniKorn.validateConfig response={}", ret);
        }
        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<>();
        String serviceName = getServiceName();
        Map<String, String> configs = getConfigs();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceYuniKorn.lookupResource context={}", context);
        }

        if (context != null) {
            try {
                ret = YuniKornResourceMgr.getYuniKornResources(serviceName, configs, context);
            } catch (Exception e) {
                LOG.error("<== RangerServiceYuniKorn.lookupResource failed", e);
                throw e;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceYuniKorn.lookupResource returned {} item(s)",
                    ret == null ? 0 : ret.size());
        }
        return ret;
    }

    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceYuniKorn.getDefaultRangerPolicies()");
        }

        List<RangerPolicy> ret = super.getDefaultRangerPolicies();
        String queueResourceName = RangerYuniKornConstants.RESOURCE_QUEUE;

        for (RangerPolicy defaultPolicy : ret) {
            if (!defaultPolicy.getName().contains("all")) {
                continue;
            }

            RangerPolicyResource queuePolicyResource =
                    defaultPolicy.getResources().get(queueResourceName);

            if (StringUtils.isNotBlank(lookUpUser)) {
                RangerPolicyItem lookupItem = new RangerPolicyItem();
                lookupItem.setUsers(Collections.singletonList(lookUpUser));
                lookupItem.setAccesses(Collections.singletonList(
                        new RangerPolicyItemAccess(RangerYuniKornConstants.ACCESS_SUBMIT)));
                lookupItem.setDelegateAdmin(false);
                defaultPolicy.addPolicyItem(lookupItem);
            }

            if (queuePolicyResource != null) {
                RangerServiceDef.RangerResourceDef queueResourceDef = null;
                for (RangerServiceDef.RangerResourceDef resourceDef : serviceDef.getResources()) {
                    if (resourceDef.getName().equals(queueResourceName)) {
                        queueResourceDef = resourceDef;
                        break;
                    }
                }
                if (queueResourceDef != null) {
                    // Every YuniKorn queue descends from "root". Seed the default
                    // "all - queue" policy with the root of the tree, matched
                    // recursively, so it covers root and all descendants. This
                    // passes the strict queue validationRegEx (unlike "*") and
                    // works with the service-def's wildCard:false matcher, since
                    // recursion — not globbing — provides the match-all behaviour.
                    queuePolicyResource.setValue(RangerYuniKornConstants.QUEUE_ROOT);
                    queuePolicyResource.setIsRecursive(Boolean.TRUE);
                } else {
                    LOG.warn("No resourceDef found in YuniKorn service-def for '{}'", queueResourceName);
                }
            } else {
                LOG.warn("No '{}' found in default policy", queueResourceName);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceYuniKorn.getDefaultRangerPolicies() count={}", ret.size());
        }
        return ret;
    }
}
