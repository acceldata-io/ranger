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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class RangerAirflowConstants {

    public static final String SERVICE_TYPE = "airflow";

    public static final String RESOURCE_DAG        = "dag";
    public static final String RESOURCE_CONNECTION = "connection";
    public static final String RESOURCE_VARIABLE   = "variable";
    public static final String RESOURCE_POOL       = "pool";
    public static final String RESOURCE_VIEW       = "view";

    public static final String CONFIG_AIRFLOW_URL = "airflow.url";
    public static final String CONFIG_USERNAME    = "username";
    public static final String CONFIG_PASSWORD    = "password";

    public static final String REST_PATH_TOKEN       = "/auth/token";
    public static final String REST_PATH_DAGS        = "/api/v2/dags";
    public static final String REST_PATH_CONNECTIONS = "/api/v2/connections";
    public static final String REST_PATH_VARIABLES   = "/api/v2/variables";
    public static final String REST_PATH_POOLS       = "/api/v2/pools";

    public static final String JSON_DAGS        = "dags";
    public static final String JSON_DAG_ID      = "dag_id";
    public static final String JSON_CONNECTIONS = "connections";
    public static final String JSON_CONNECTION_ID = "connection_id";
    public static final String JSON_VARIABLES   = "variables";
    public static final String JSON_VARIABLE_KEY = "key";
    public static final String JSON_POOLS       = "pools";
    public static final String JSON_POOL_NAME   = "name";
    public static final String JSON_POOL_ID     = "pool";
    public static final String JSON_ACCESS_TOKEN = "access_token";

    public static final int PAGE_LIMIT = 100;
    public static final int MAX_PAGES  = 10;

    /**
     * Built-in Airflow 3 UI views from the service-def. There is no list API
     * for these, so autocomplete returns this fixed set.
     */
    public static final List<String> BUILTIN_VIEWS = Collections.unmodifiableList(Arrays.asList(
            "audit_logs_all",
            "cluster_activity",
            "docs",
            "import_errors",
            "import_errors_all",
            "jobs",
            "plugins",
            "providers",
            "triggers",
            "website"
    ));

    private RangerAirflowConstants() {
        // utility class
    }
}
