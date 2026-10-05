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

package org.apache.ranger.services.airflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.airflow.client.AirflowResourceMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ranger service implementation for Apache Airflow.
 *
 * <p>Loaded into Ranger Admin's JVM at startup so that an admin can register an
 * {@code airflow} service and author policies against Airflow resources (dag,
 * connection, variable, pool, asset, asset_alias, config, view, custom_view).
 *
 * <p>This class does <em>not</em> perform runtime authorization. Decisions are made
 * by {@code ranger-airflow-authz-agent}, a colocated process that embeds
 * {@code RangerBasePlugin} alongside each Airflow api-server.
 *
 * <p>Resource lookup talks to the Airflow api-server (YuniKorn-shaped HTTP
 * client) so the policy form can autocomplete DAG ids, connections, variables,
 * pools, and built-in views. Put {@code airflow.url}, {@code username} and
 * {@code password} on the service. Ranger already seeds that username onto the
 * default {@code all - *} policies, which is what the list APIs need once
 * Ranger is the Airflow auth manager.
 *
 * <p>{@code airflow.url} must be the api-server base (the process that serves
 * {@code /auth/token} and {@code /api/v2}), not a SPNEGO/Knox UI frontend.
 * Airflow 3's public API accepts a JWT, not a Kerberos ticket, so
 * {@code username} must be a password-capable account (LDAP or FAB).
 */
public class RangerServiceAirflow extends RangerBaseService {

    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceAirflow.class);

    public RangerServiceAirflow() {
        super();
    }

    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    /**
     * Test Connection: obtain a JWT from {@code POST /auth/token} and list DAGs.
     */
    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret = new HashMap<String, Object>();
        String serviceName = getServiceName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceAirflow.validateConfig service=[{}]", serviceName);
        }

        if (configs != null) {
            try {
                ret = AirflowResourceMgr.validateConfig(serviceName, configs);
            } catch (Exception e) {
                LOG.error("<== RangerServiceAirflow.validateConfig failed", e);
                throw e;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceAirflow.validateConfig response={}", ret);
        }
        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<String>();
        String serviceName = getServiceName();
        Map<String, String> configs = getConfigs();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceAirflow.lookupResource context={}", context);
        }

        if (context != null) {
            try {
                ret = AirflowResourceMgr.getAirflowResources(serviceName, configs, context);
            } catch (Exception e) {
                LOG.error("<== RangerServiceAirflow.lookupResource failed", e);
                throw e;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceAirflow.lookupResource returned {} item(s)",
                    ret == null ? 0 : ret.size());
        }
        return ret;
    }
}
