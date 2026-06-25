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
package org.apache.ranger.services.abfs;

public final class RangerABFSConstants {

    private RangerABFSConstants() {
    }

    public static final String ABFS = "abfs";

    // ---- service configs ----
    public static final String USER_NAME       = "username";
    public static final String STORAGE_ACCOUNT = "storageAccount";
    public static final String TENANT_ID       = "tenantId";
    public static final String CLIENT_ID       = "clientId";
    public static final String CLIENT_SECRET   = "clientSecret";
    public static final String DEFAULT_CONTAINER = "defaultContainer";
    public static final String ENDPOINT        = "endpoint";

    // ---- identity handling config ----
    // Ranger principals (users/groups) entered in policies are expected to be
    // Azure AD object IDs (GUIDs). This flag controls whether a non-GUID
    // principal aborts the sync (true) or is skipped with a warning (false).
    public static final String FAIL_ON_UNRESOLVED_IDENTITY = "failOnUnresolvedIdentity";

    // ---- ACL sync behavior configs ----
    public static final String PRESERVE_MANUAL_ACLS          = "preserveManualAcls";
    public static final String RECURSIVE_ACL_SYNC_ENABLED    = "recursiveAclSyncEnabled";
    public static final String DEFAULT_ACL_INHERITANCE_ENABLED = "defaultAclInheritanceEnabled";

    // ---- resource names (must match service-def resource names) ----
    public static final String STORAGE_ACCOUNT_RESOURCE = "storageaccount";
    public static final String CONTAINER                = "container";
    public static final String RELATIVE_PATH            = "relativepath";

    // ---- access types (must match service-def access types) ----
    public static final String ACCESS_TYPE_READ   = "read";
    public static final String ACCESS_TYPE_LIST   = "list";
    public static final String ACCESS_TYPE_WRITE  = "write";
    public static final String ACCESS_TYPE_DELETE = "delete";

    // ---- ABFS / DFS endpoint suffix ----
    public static final String DFS_ENDPOINT_SUFFIX = ".dfs.core.windows.net";

    public static final int MAX_AUTOCOMPLETE_RESULTS = 100;
    public static final int ABFS_LIST_MAX_RESULTS    = 500;
}
