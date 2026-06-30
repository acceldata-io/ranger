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

package org.apache.ranger.services.yunikorn;

public final class RangerYuniKornConstants {

    public static final String SERVICE_TYPE = "yunikorn";

    // Resource names (must match ranger-servicedef-yunikorn.json)
    public static final String RESOURCE_QUEUE = "queue";

    // Access types (must match ranger-servicedef-yunikorn.json)
    public static final String ACCESS_SUBMIT = "submit";
    public static final String ACCESS_ADMIN  = "admin";

    // Service config keys
    public static final String CONFIG_YUNIKORN_URL       = "yunikorn.url";
    public static final String CONFIG_YUNIKORN_PARTITION = "yunikorn.partition";

    public static final String DEFAULT_PARTITION = "default";

    // YuniKorn REST endpoints
    public static final String REST_PATH_PARTITIONS = "/ws/v1/partitions";
    public static final String REST_PATH_QUEUES_FMT = "/ws/v1/partition/%s/queues";

    // Queue path conventions
    public static final String QUEUE_PATH_SEPARATOR = ".";
    public static final String QUEUE_ROOT           = "root";

    private RangerYuniKornConstants() {
        // utility class
    }
}
