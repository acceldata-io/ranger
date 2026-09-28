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
 * <p>Resource lookup is not implemented in this release: the service-def declares
 * {@code lookupSupported:false} on every resource, so the policy form accepts
 * free-text values and wildcards rather than offering autocomplete.
 */
public class RangerServiceAirflow extends RangerBaseService {

    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceAirflow.class);

    private static final String KEY_CONNECTIVITY_STATUS = "connectivityStatus";
    private static final String KEY_MESSAGE             = "message";
    private static final String KEY_DESCRIPTION         = "description";

    private static final String MSG_LOOKUP_UNSUPPORTED =
            "Resource lookup is not implemented for the Airflow plugin in this release.";
    private static final String MSG_NO_CONNECTIVITY_CHECK =
            "The Airflow plugin does not contact Airflow to validate this service. "
            + "Policy enforcement does not require connectivity from Ranger Admin; "
            + "the authorization agent colocated with each api-server pulls policies instead.";

    public RangerServiceAirflow() {
        super();
    }

    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    /**
     * Ranger Admin has no connection to validate: the plugin is a policy consumer,
     * not a client of Airflow. Report success with an explanation rather than
     * attempting a check that cannot be meaningful.
     */
    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret = new HashMap<>();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceAirflow.validateConfig service=[{}]", getServiceName());
        }

        ret.put(KEY_CONNECTIVITY_STATUS, true);
        ret.put(KEY_MESSAGE, MSG_NO_CONNECTIVITY_CHECK);
        ret.put(KEY_DESCRIPTION, MSG_NO_CONNECTIVITY_CHECK);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceAirflow.validateConfig response={}", ret);
        }
        return ret;
    }

    /**
     * Always empty. Airflow resource ids are free-text in this release; see the
     * class comment. Returning an empty list leaves the policy form usable rather
     * than leaving an autocomplete box spinning.
     */
    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("RangerServiceAirflow.lookupResource: {} context={}", MSG_LOOKUP_UNSUPPORTED, context);
        }
        return new ArrayList<>();
    }
}
