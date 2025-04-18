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

package org.apache.ranger.services.schema.registry.client.util;

import org.apache.ranger.services.schema.registry.client.connection.ISchemaRegistryClient;

import java.util.ArrayList;
import java.util.List;

public class DefaultSchemaRegistryClientForTesting implements ISchemaRegistryClient {

    @Override
    public List<String> getSchemaGroups() {
        return new ArrayList<>();
    }

    @Override
    public List<String> getSchemaNames(List<String> schemaGroup) {
        return new ArrayList<>();
    }

    @Override
    public List<String> getSchemaBranches(String schemaMetadataName) {
        return new ArrayList<>();
    }
    @Override
    public List<String> getSchemaVersions(String schemaMetadataName) {
        return new ArrayList<>();
    }
    @Override
    public void checkConnection() throws Exception {

    }
}
