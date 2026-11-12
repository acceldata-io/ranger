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

package io.acceldata.mlflow.ranger;

import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@code ranger-servicedef-mlflow.json} stays aligned with
 * MLflow 3.16.0's native RBAC resource types and permission tiers in
 * {@link RbacConverter}.
 */
class ServiceDefConsistencyTest {

    private static RangerServiceDef serviceDef;

    @BeforeAll
    static void loadServiceDef() throws Exception {
        try (InputStream in = RangerServiceDef.class.getResourceAsStream(
                "/service-defs/ranger-servicedef-mlflow.json")) {
            assertThat(in).as("mlflow service-def on classpath").isNotNull();
            serviceDef = JsonUtils.jsonToObject(
                    new InputStreamReader(in, StandardCharsets.UTF_8), RangerServiceDef.class);
        }
        assertThat(serviceDef).isNotNull();
    }

    @Test
    void serviceDefResourceTypesMatchMlflow316ValidResourceTypes() {
        Set<String> resourcesInDef = serviceDef.getResources().stream()
                .map(RangerResourceDef::getName)
                .collect(Collectors.toSet());

        assertThat(resourcesInDef).contains(RbacConverter.RESOURCE_WORKSPACE);
        assertThat(resourcesInDef).containsAll(RbacConverter.CONCRETE_RESOURCE_TYPES);
        assertThat(resourcesInDef).hasSize(1 + RbacConverter.CONCRETE_RESOURCE_TYPES.size());
    }

    @Test
    void workspaceResourceRestrictsToUseAndManageWhileConcreteResourcesAllowAllFourTiers() {
        for (RangerResourceDef res : serviceDef.getResources()) {
            Set<String> restrictions = res.getAccessTypeRestrictions();
            if (RbacConverter.RESOURCE_WORKSPACE.equals(res.getName())) {
                assertThat(restrictions).containsExactlyInAnyOrder("use", "manage");
            } else {
                assertThat(restrictions).containsExactlyInAnyOrder("read", "use", "edit", "manage");
            }
        }
    }

    @Test
    void everyDeclaredAccessTypeMapsCleanlyToMlflowPermission() {
        for (RangerAccessTypeDef accessType : serviceDef.getAccessTypes()) {
            String mapped = RbacConverter.mapAccessTypeToPermission(accessType.getName());
            assertThat(mapped)
                    .as("Mapped MLflow permission for access type %s", accessType.getName())
                    .isNotNull();
        }
    }
}
