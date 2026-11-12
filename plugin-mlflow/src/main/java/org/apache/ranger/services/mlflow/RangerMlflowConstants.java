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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class RangerMlflowConstants {

    public static final String SERVICE_TYPE = "mlflow";

    // Resource types matching mlflow/server/auth/permissions.py VALID_RESOURCE_TYPES
    public static final String RESOURCE_WORKSPACE                = "workspace";
    public static final String RESOURCE_EXPERIMENT               = "experiment";
    public static final String RESOURCE_REGISTERED_MODEL         = "registered_model";
    public static final String RESOURCE_PROMPT                   = "prompt";
    public static final String RESOURCE_SCORER                   = "scorer";
    public static final String RESOURCE_GATEWAY_SECRET           = "gateway_secret";
    public static final String RESOURCE_GATEWAY_ENDPOINT         = "gateway_endpoint";
    public static final String RESOURCE_GATEWAY_MODEL_DEFINITION = "gateway_model_definition";
    public static final String RESOURCE_MCP_SERVER               = "mcp_server";

    // Access types matching mlflow/server/auth/permissions.py ALL_PERMISSIONS
    public static final String ACCESS_READ   = "read";
    public static final String ACCESS_USE    = "use";
    public static final String ACCESS_EDIT   = "edit";
    public static final String ACCESS_MANAGE = "manage";

    // Service configuration keys
    public static final String CONFIG_MLFLOW_URL = "mlflow.url";
    public static final String CONFIG_USERNAME   = "username";
    public static final String CONFIG_PASSWORD   = "password";

    // MLflow workspace constants
    public static final String DEFAULT_WORKSPACE = "default";
    public static final String WORKSPACE_HEADER  = "X-MLFLOW-WORKSPACE";
    public static final String WILDCARD          = "*";

    // Tag used by MLflow 3.16.0 to distinguish prompts from registered models
    public static final String PROMPT_TAG_KEY = "mlflow.prompt.is_prompt";

    // MLflow 3.16.0 REST API paths for connection validation and resource lookup
    public static final String REST_PATH_USERS_CURRENT          = "/api/2.0/mlflow/users/current";
    public static final String REST_PATH_WORKSPACES             = "/api/3.0/mlflow/workspaces";
    public static final String REST_PATH_EXPERIMENTS_SEARCH     = "/api/2.0/mlflow/experiments/search";
    public static final String REST_PATH_REGISTERED_MODELS      = "/api/2.0/mlflow/registered-models/search";
    public static final String REST_PATH_SCORERS_LIST           = "/api/3.0/mlflow/scorers/list";
    public static final String REST_PATH_GATEWAY_SECRETS_LIST   = "/api/3.0/mlflow/gateway/secrets/list";
    public static final String REST_PATH_GATEWAY_ENDPOINTS_LIST = "/api/3.0/mlflow/gateway/endpoints/list";
    public static final String REST_PATH_GATEWAY_MODEL_DEFS     = "/api/3.0/mlflow/gateway/model-definitions/list";
    public static final String REST_PATH_MCP_SERVERS            = "/api/3.0/mlflow/mcp-servers";

    // JSON field names in MLflow REST responses
    public static final String JSON_WORKSPACES           = "workspaces";
    public static final String JSON_EXPERIMENTS          = "experiments";
    public static final String JSON_EXPERIMENT_ID        = "experiment_id";
    public static final String JSON_REGISTERED_MODELS    = "registered_models";
    public static final String JSON_SCORERS              = "scorers";
    public static final String JSON_SCORER_NAME          = "scorer_name";
    public static final String JSON_SECRETS              = "secrets";
    public static final String JSON_SECRET_ID            = "secret_id";
    public static final String JSON_ENDPOINTS            = "endpoints";
    public static final String JSON_ENDPOINT_ID          = "endpoint_id";
    public static final String JSON_MODEL_DEFINITIONS    = "model_definitions";
    public static final String JSON_MODEL_DEFINITION_ID  = "model_definition_id";
    public static final String JSON_MCP_SERVERS          = "mcp_servers";
    public static final String JSON_NAME                 = "name";
    public static final String JSON_TAGS                 = "tags";
    public static final String JSON_KEY                  = "key";
    public static final String JSON_VALUE                = "value";
    public static final String JSON_NEXT_PAGE_TOKEN      = "next_page_token";

    public static final int PAGE_LIMIT = 100;
    public static final int MAX_PAGES  = 10;

    public static final List<String> ALL_LOOKUP_RESOURCES = Collections.unmodifiableList(Arrays.asList(
            RESOURCE_WORKSPACE,
            RESOURCE_EXPERIMENT,
            RESOURCE_REGISTERED_MODEL,
            RESOURCE_PROMPT,
            RESOURCE_SCORER,
            RESOURCE_GATEWAY_SECRET,
            RESOURCE_GATEWAY_ENDPOINT,
            RESOURCE_GATEWAY_MODEL_DEFINITION,
            RESOURCE_MCP_SERVER
    ));

    private RangerMlflowConstants() {
        // utility class
    }
}
