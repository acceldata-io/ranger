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
package org.apache.ranger.services.gravitino.client;

import java.util.List;

/**
 * GravitinoClient - Interface for Gravitino API operations.
 * 
 * This interface defines the methods needed to communicate with
 * the Gravitino server for resource lookup operations.
 */
public interface GravitinoClient {
    
    /**
     * List metalakes matching the given prefix.
     * 
     * @param prefix Optional prefix to filter metalakes
     * @return List of metalake names
     * @throws Exception if the operation fails
     */
    List<String> listMetalakes(String prefix) throws Exception;
    
    /**
     * List catalogs in a metalake matching the given prefix.
     * 
     * @param metalake The metalake name
     * @param prefix Optional prefix to filter catalogs
     * @return List of catalog names
     * @throws Exception if the operation fails
     */
    List<String> listCatalogs(String metalake, String prefix) throws Exception;
    
    /**
     * List schemas in a catalog matching the given prefix.
     * 
     * @param metalake The metalake name
     * @param catalog The catalog name
     * @param prefix Optional prefix to filter schemas
     * @return List of schema names
     * @throws Exception if the operation fails
     */
    List<String> listSchemas(String metalake, String catalog, String prefix) throws Exception;
    
    /**
     * List tables in a schema matching the given prefix.
     * 
     * @param metalake The metalake name
     * @param catalog The catalog name
     * @param schema The schema name
     * @param prefix Optional prefix to filter tables
     * @return List of table names
     * @throws Exception if the operation fails
     */
    List<String> listTables(String metalake, String catalog, String schema, String prefix) throws Exception;
    
    /**
     * List topics in a messaging catalog schema matching the given prefix.
     * 
     * @param metalake The metalake name
     * @param catalog The catalog name
     * @param schema The schema name
     * @param prefix Optional prefix to filter topics
     * @return List of topic names
     * @throws Exception if the operation fails
     */
    List<String> listTopics(String metalake, String catalog, String schema, String prefix) throws Exception;
    
    /**
     * List filesets in a fileset catalog schema matching the given prefix.
     * 
     * @param metalake The metalake name
     * @param catalog The catalog name
     * @param schema The schema name
     * @param prefix Optional prefix to filter filesets
     * @return List of fileset names
     * @throws Exception if the operation fails
     */
    List<String> listFilesets(String metalake, String catalog, String schema, String prefix) throws Exception;
    
    /**
     * List models in a model catalog schema matching the given prefix.
     * 
     * @param metalake The metalake name
     * @param catalog The catalog name
     * @param schema The schema name
     * @param prefix Optional prefix to filter models
     * @return List of model names
     * @throws Exception if the operation fails
     */
    List<String> listModels(String metalake, String catalog, String schema, String prefix) throws Exception;
    
    /**
     * List model versions matching the given prefix.
     * 
     * @param metalake The metalake name
     * @param catalog The catalog name
     * @param schema The schema name
     * @param model The model name
     * @param prefix Optional prefix to filter versions
     * @return List of version identifiers
     * @throws Exception if the operation fails
     */
    List<String> listModelVersions(String metalake, String catalog, String schema, 
            String model, String prefix) throws Exception;
}
