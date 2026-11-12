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

package org.apache.ranger.services.mlflow;

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
import org.apache.ranger.services.mlflow.client.MlflowResourceMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ranger service implementation for MLflow 3.16.0.
 *
 * <p>Loaded into Ranger Admin's JVM at startup so administrators can register an
 * {@code mlflow} service, test connectivity against the MLflow Tracking Server,
 * autocomplete MLflow resources (workspaces, experiments, registered models,
 * prompts, scorers, gateway secrets/endpoints/model-definitions, and MCP servers),
 * and author policies that map onto MLflow 3.16.0's native RBAC model.
 *
 * <p>Runtime authorization is enforced natively by MLflow 3.16.0's RBAC subsystem
 * ({@code roles}, {@code role_permissions}, and {@code user_role_assignments}),
 * synchronized from Ranger Admin by {@code ranger-mlflow-agent}.
 */
public class RangerServiceMlflow extends RangerBaseService {

    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceMlflow.class);

    public RangerServiceMlflow() {
        super();
    }

    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret = new HashMap<String, Object>();
        String serviceName = getServiceName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceMlflow.validateConfig service=[{}]", serviceName);
        }

        if (configs != null) {
            try {
                ret = MlflowResourceMgr.validateConfig(serviceName, configs);
            } catch (Exception e) {
                LOG.error("<== RangerServiceMlflow.validateConfig failed", e);
                throw e;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceMlflow.validateConfig response={}", ret);
        }
        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<String>();
        String serviceName = getServiceName();
        Map<String, String> configs = getConfigs();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceMlflow.lookupResource context={}", context);
        }

        if (context != null) {
            try {
                ret = MlflowResourceMgr.getMlflowResources(serviceName, configs, context);
            } catch (Exception e) {
                LOG.error("<== RangerServiceMlflow.lookupResource failed", e);
                throw e;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceMlflow.lookupResource returned {} item(s)",
                    ret == null ? 0 : ret.size());
        }
        return ret;
    }

    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceMlflow.getDefaultRangerPolicies()");
        }

        List<RangerPolicy> ret = super.getDefaultRangerPolicies();
        String adminUser = StringUtils.isNotBlank(lookUpUser)
                ? lookUpUser
                : (configs == null ? null : configs.get(RangerMlflowConstants.CONFIG_USERNAME));

        for (RangerPolicy defaultPolicy : ret) {
            Map<String, RangerPolicyResource> resources = defaultPolicy.getResources();
            if (resources != null) {
                RangerPolicyResource wsResource = resources.get(RangerMlflowConstants.RESOURCE_WORKSPACE);
                if (wsResource != null && (wsResource.getValues() == null || wsResource.getValues().isEmpty())) {
                    wsResource.setValue(RangerMlflowConstants.WILDCARD);
                }
            }

            if (StringUtils.isNotBlank(adminUser) && defaultPolicy.getName().contains("all")) {
                RangerPolicyItem adminItem = new RangerPolicyItem();
                adminItem.setUsers(Collections.singletonList(adminUser));
                adminItem.setAccesses(Collections.singletonList(
                        new RangerPolicyItemAccess(RangerMlflowConstants.ACCESS_MANAGE)));
                adminItem.setDelegateAdmin(true);
                defaultPolicy.addPolicyItem(adminItem);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceMlflow.getDefaultRangerPolicies() count={}", ret.size());
        }
        return ret;
    }
}
