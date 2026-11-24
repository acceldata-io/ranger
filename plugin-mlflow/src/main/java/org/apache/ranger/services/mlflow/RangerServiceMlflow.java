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
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.mlflow.client.MlflowResourceMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ranger service implementation for MLflow 3.16.0+.
 *
 * <p>Registered in Ranger Admin to manage MLflow control-plane policies
 * (workspaces, experiments, registered models, prompts, scorers, AI Gateway
 * endpoints/secrets/model definitions, and MCP servers) and provide resource
 * autocomplete across HTTP Basic/LDAP, Kerberos/SPNEGO, and Apache Knox.
 *
 * <p>Runtime policy decisions are served directly by Ranger Admin's built-in
 * REST Policy Decision Point ({@code /service/plugins/pdp/*}) to MLflow's
 * {@code RangerPermissionProvider} without requiring a sidecar JVM.
 */
public class RangerServiceMlflow extends RangerBaseService {

    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceMlflow.class);

    public RangerServiceMlflow() {
        super();
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
        String lookupUser = resolveLookupUsername();

        if (StringUtils.isNotBlank(lookupUser) && ret != null) {
            for (RangerPolicy defaultPolicy : ret) {
                RangerPolicyItem lookupItem = new RangerPolicyItem();
                lookupItem.setUsers(Collections.singletonList(lookupUser));
                lookupItem.setAccesses(Collections.singletonList(
                        new RangerPolicyItemAccess(RangerMlflowConstants.ACCESS_READ, Boolean.TRUE)));
                lookupItem.setDelegateAdmin(Boolean.FALSE);
                defaultPolicy.addPolicyItem(lookupItem);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceMlflow.getDefaultRangerPolicies(): count={}",
                    ret == null ? 0 : ret.size());
        }
        return ret;
    }

    String resolveLookupUsername() {
        if (configs == null) {
            return null;
        }
        String configuredUser = configs.get(RangerMlflowConstants.CONFIG_USERNAME);
        if (StringUtils.isNotBlank(configuredUser)) {
            return extractShortName(configuredUser.trim());
        }
        String principal = configs.get(RangerMlflowConstants.CONFIG_KERBEROS_PRINCIPAL);
        if (StringUtils.isNotBlank(principal)) {
            return extractShortName(principal.trim());
        }
        return null;
    }

    static String extractShortName(String principalOrUser) {
        if (StringUtils.isBlank(principalOrUser)) {
            return null;
        }
        int slashIdx = principalOrUser.indexOf('/');
        int atIdx = principalOrUser.indexOf('@');
        int endIdx = principalOrUser.length();
        if (slashIdx > 0) {
            endIdx = Math.min(endIdx, slashIdx);
        }
        if (atIdx > 0) {
            endIdx = Math.min(endIdx, atIdx);
        }
        return principalOrUser.substring(0, endIdx);
    }
}
