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

/**
 * Ranger-managed authorization agent for MLflow 3.16.0.
 *
 * <p>Polls Apache Ranger Admin for policy, user-store, and role updates and
 * reconciles them into MLflow 3.16.0's native RBAC model ({@code roles},
 * {@code role_permissions}, and {@code user_role_assignments}) via MLflow's
 * REST APIs.
 */
package io.acceldata.mlflow.ranger;
