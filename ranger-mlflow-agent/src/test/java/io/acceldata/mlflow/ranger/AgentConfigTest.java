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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AgentConfigTest {

    @Test
    void loadsValidConfigurationAndEnvCredentials() {
        Properties props = new Properties();
        props.setProperty("ranger.admin.url", "http://ranger:6080");
        props.setProperty("ranger.service.name", "odp_mlflow");
        props.setProperty("mlflow.url", "http://mlflow:5000");

        Map<String, String> env = new HashMap<>();
        env.put("RANGER_PRINCIPAL", "rangeradmin");
        env.put("RANGER_CREDENTIAL", "secret123");
        env.put("MLFLOW_ADMIN_USERNAME", "admin");
        env.put("MLFLOW_ADMIN_PASSWORD", "password123456");

        AgentConfig cfg = AgentConfig.fromProperties(props, env::get);

        assertThat(cfg.rangerAdminUrl()).isEqualTo("http://ranger:6080");
        assertThat(cfg.rangerServiceName()).isEqualTo("odp_mlflow");
        assertThat(cfg.rangerServiceType()).isEqualTo("mlflow");
        assertThat(cfg.mlflowUrl()).isEqualTo("http://mlflow:5000");
        assertThat(cfg.mlflowAdminUsername()).isEqualTo("admin");
        assertThat(cfg.managedRolePrefix()).isEqualTo("ranger_");
        assertThat(cfg.doctrine()).isEqualTo(UnmanagedDoctrine.LENIENT);
    }

    @Test
    void rejectsSyntheticRolePrefixInConfig() {
        Properties props = new Properties();
        props.setProperty("ranger.admin.url", "http://ranger:6080");
        props.setProperty("ranger.service.name", "odp_mlflow");
        props.setProperty("mlflow.url", "http://mlflow:5000");
        props.setProperty("mlflow.admin.username", "admin");
        props.setProperty("mlflow.admin.password", "password123456");
        props.setProperty("mlflow.managed.role.prefix", "__user_custom_");

        assertThatThrownBy(() -> AgentConfig.fromProperties(props, k -> null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("__user_");
    }
}
