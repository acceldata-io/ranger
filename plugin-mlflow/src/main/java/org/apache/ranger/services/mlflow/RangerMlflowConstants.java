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

public final class RangerMlflowConstants {

    public static final String CONFIG_MLFLOW_URL         = "mlflow.url";
    public static final String CONFIG_AUTH_TYPE          = "mlflow.auth.type";
    public static final String CONFIG_USERNAME           = "username";
    public static final String CONFIG_PASSWORD           = "password";
    public static final String CONFIG_KERBEROS_PRINCIPAL = "mlflow.kerberos.principal";
    public static final String CONFIG_KERBEROS_KEYTAB    = "mlflow.kerberos.keytab";
    public static final String CONFIG_KNOX_TOKEN         = "mlflow.knox.token";

    public static final String AUTH_TYPE_BASIC    = "basic";
    public static final String AUTH_TYPE_KERBEROS = "kerberos";
    public static final String AUTH_TYPE_KNOX     = "knox";

    public static final String RESOURCE_WORKSPACE                = "workspace";
    public static final String RESOURCE_EXPERIMENT               = "experiment";
    public static final String RESOURCE_REGISTERED_MODEL         = "registered_model";
    public static final String RESOURCE_PROMPT                   = "prompt";
    public static final String RESOURCE_SCORER                   = "scorer";
    public static final String RESOURCE_GATEWAY_ENDPOINT         = "gateway_endpoint";
    public static final String RESOURCE_GATEWAY_SECRET           = "gateway_secret";
    public static final String RESOURCE_GATEWAY_MODEL_DEFINITION = "gateway_model_definition";
    public static final String RESOURCE_MCP_SERVER               = "mcp_server";

    public static final String ACCESS_READ   = "read";
    public static final String ACCESS_USE    = "use";
    public static final String ACCESS_EDIT   = "edit";
    public static final String ACCESS_MANAGE = "manage";

    public static final String DEFAULT_WORKSPACE = "default";
    public static final String HEADER_WORKSPACE  = "X-MLFLOW-WORKSPACE";
    public static final String PROMPT_TAG_KEY    = "mlflow.prompt.is_prompt";

    public static final String REST_PATH_VERSION                   = "/version";
    public static final String REST_PATH_WORKSPACES                = "/api/3.0/mlflow/workspaces";
    public static final String REST_PATH_EXPERIMENTS_SEARCH        = "/api/2.0/mlflow/experiments/search";
    public static final String REST_PATH_REGISTERED_MODELS_SEARCH  = "/api/2.0/mlflow/registered-models/search";
    public static final String REST_PATH_SCORERS_LIST              = "/api/3.0/mlflow/scorers/list";
    public static final String REST_PATH_GATEWAY_ENDPOINTS         = "/api/3.0/mlflow/gateway/endpoints";
    public static final String REST_PATH_GATEWAY_SECRETS           = "/api/3.0/mlflow/gateway/secrets";
    public static final String REST_PATH_GATEWAY_MODEL_DEFINITIONS = "/api/3.0/mlflow/gateway/model-definitions";
    public static final String REST_PATH_MCP_SERVERS               = "/ajax-api/3.0/mcp-servers";

    public static final int PAGE_LIMIT = 200;

    private RangerMlflowConstants() {
        // utility class
    }
}
