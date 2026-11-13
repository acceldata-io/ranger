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
package org.apache.ranger.authorization.pinot;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The contract between Apache Pinot's {@code ranger} authorization mode and the Ranger plug-in for Pinot. It uses only JDK
 * types, so that Pinot and Ranger build independently (Pinot on its own Java version, Ranger on its own) and a Pinot
 * upgrade never requires rebuilding the plug-in.
 *
 * Pinot creates {@link RangerPinotPlugin} (on its class path), calls {@link #init} once, then {@link #checkAccess} and {@link #getRowFilters}
 * from many threads, and {@link #close} at shutdown. Implementations are thread-safe after init.
 */
public interface PinotAccessChecker {
    /** Resource type of a request that names no table or database. */
    String RESOURCE_CLUSTER  = "CLUSTER";
    /** Resource type of a database request. */
    String RESOURCE_DATABASE = "DATABASE";
    /** Resource type of a table request. */
    String RESOURCE_TABLE    = "TABLE";

    /**
     * @param settings Pinot's {@code pinot.authz.ranger.*} settings without the prefix, e.g. {@code config.dir},
     *                 {@code service.name}, {@code cluster.name}, {@code cache.ttl.ms}
     */
    void init(Map<String, String> settings);

    /**
     * Decides one request.
     *
     * @param user         the authenticated user
     * @param groups       the user's groups from Pinot's authentication method
     * @param resourceType [#RESOURCE_CLUSTER], [#RESOURCE_DATABASE] or [#RESOURCE_TABLE]
     * @param database     the database ({@code default} when the request names none); null for cluster requests
     * @param table        the raw table name for table requests, else null
     * @param accessType   the Ranger access type of the request's action (query, read_metadata, manage_table, ...)
     * @param action       the Pinot action, for the audit log
     * @return key: whether the request is allowed; value: the reason (policy id, or why it was denied)
     */
    Map.Entry<Boolean, String> checkAccess(String user, Set<String> groups, String resourceType, String database, String table, String accessType, String action);

    /**
     * Row filters the user's queries of a table must apply; empty when the user sees every row.
     */
    List<String> getRowFilters(String user, Set<String> groups, String database, String table);

    void close();
}
