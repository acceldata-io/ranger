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

package org.apache.ranger.services.airflow.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.airflow.RangerAirflowConstants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AirflowClientTest {

    @Test
    void collectIdsReadsDagIds() throws Exception {
        String json = "{\"dags\":[{\"dag_id\":\"etl_daily\"},{\"dag_id\":\"etl_hourly\"}],\"total_entries\":2}";
        assertEquals(Arrays.asList("etl_daily", "etl_hourly"),
                AirflowClient.collectIds(json, RangerAirflowConstants.JSON_DAGS, RangerAirflowConstants.JSON_DAG_ID));
    }

    @Test
    void collectIdsReadsPoolNameOrPool() throws Exception {
        String byName = "{\"pools\":[{\"name\":\"default_pool\"}],\"total_entries\":1}";
        assertEquals(Collections.singletonList("default_pool"),
                AirflowClient.collectIds(byName, RangerAirflowConstants.JSON_POOLS, RangerAirflowConstants.JSON_POOL_NAME));

        String byPool = "{\"pools\":[{\"pool\":\"spark\"}],\"total_entries\":1}";
        assertEquals(Collections.singletonList("spark"),
                AirflowClient.collectIds(byPool, RangerAirflowConstants.JSON_POOLS, RangerAirflowConstants.JSON_POOL_NAME));
    }

    @Test
    void filterMatchesPrefixAndSkipsExisting() {
        List<String> values = Arrays.asList("etl_daily", "etl_hourly", "ml_train");
        List<String> existing = Collections.singletonList("etl_daily");

        assertEquals(Collections.singletonList("etl_hourly"),
                AirflowClient.filterMatches(values, "etl_", existing, false));
    }

    @Test
    void filterMatchesViewIgnoreCase() {
        List<String> matches = AirflowClient.filterMatches(
                RangerAirflowConstants.BUILTIN_VIEWS, "DOC", null, true);
        assertEquals(Collections.singletonList("docs"), matches);
    }

    @Test
    void missingRequiredConfigReportsFirstGap() {
        assertEquals(RangerAirflowConstants.CONFIG_AIRFLOW_URL,
                AirflowClient.missingRequiredConfig(null));

        Map<String, String> configs = new HashMap<String, String>();
        configs.put(RangerAirflowConstants.CONFIG_AIRFLOW_URL, "http://airflow:8080");
        assertEquals(RangerAirflowConstants.CONFIG_USERNAME,
                AirflowClient.missingRequiredConfig(configs));

        configs.put(RangerAirflowConstants.CONFIG_USERNAME, "lookup");
        configs.put(RangerAirflowConstants.CONFIG_PASSWORD, "secret");
        assertEquals(null, AirflowClient.missingRequiredConfig(configs));
    }

    @Test
    void connectionTestFailsWhenUrlMissing() {
        Map<String, Object> result = AirflowClient.connectionTest("airflow_dev", Collections.<String, String>emptyMap());
        assertFalse(Boolean.TRUE.equals(result.get("connectivityStatus")));
        assertTrue(String.valueOf(result.get("message")).contains(RangerAirflowConstants.CONFIG_AIRFLOW_URL));
    }

    @Test
    void resourceMgrReturnsViewsWithoutHttp() {
        ResourceLookupContext context = new ResourceLookupContext();
        context.setResourceName(RangerAirflowConstants.RESOURCE_VIEW);
        context.setUserInput("job");

        List<String> result = AirflowResourceMgr.getAirflowResources("airflow_dev", null, context);
        assertEquals(Collections.singletonList("jobs"), result);
    }

    @Test
    void resourceMgrReturnsEmptyWhenUrlMissing() {
        ResourceLookupContext context = new ResourceLookupContext();
        context.setResourceName(RangerAirflowConstants.RESOURCE_DAG);
        context.setUserInput("etl");

        List<String> result = AirflowResourceMgr.getAirflowResources(
                "airflow_dev", Collections.<String, String>emptyMap(), context);
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    void resourceMgrIgnoresUnknownResource() {
        Map<String, String> configs = new HashMap<String, String>();
        configs.put(RangerAirflowConstants.CONFIG_AIRFLOW_URL, "http://airflow:8080");

        ResourceLookupContext context = new ResourceLookupContext();
        context.setResourceName("custom_view");
        context.setUserInput("x");

        assertEquals(Collections.emptyList(),
                AirflowResourceMgr.getAirflowResources("airflow_dev", configs, context));
    }
}
