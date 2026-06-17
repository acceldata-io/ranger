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

public class RangerABFSConstants {
    public static final String ABFS = "abfs";

    public static final String USER_NAME = "username";
    public static final String AUTH_TYPE = "authType";
    public static final String AUTH_TYPE_SERVICE_PRINCIPAL = "service-principal";
    public static final String AUTH_TYPE_MANAGED_IDENTITY = "managed-identity";
    public static final String TENANT_ID = "tenantId";
    public static final String CLIENT_ID = "clientId";
    public static final String CLIENT_SECRET = "clientSecret";
    public static final String MANAGED_IDENTITY_CLIENT_ID = "managedIdentityClientId";
    public static final String ENDPOINT = "endpoint";
    public static final String STORAGE_ACCOUNT = "storageAccount";
    public static final String DEFAULT_CONTAINER = "defaultContainer";
    public static final String IDENTITY_MAPPING_MODE = "identityMappingMode";
    public static final String IDENTITY_MAPPING_MODE_STATIC_MAP = "static-map";
    public static final String USER_IDENTITY_MAP = "userIdentityMap";
    public static final String GROUP_IDENTITY_MAP = "groupIdentityMap";
    public static final String SERVICE_PRINCIPAL_IDENTITY_MAP = "servicePrincipalIdentityMap";
    public static final String FAIL_ON_UNRESOLVED_IDENTITY = "failOnUnresolvedIdentity";
    public static final String PRESERVE_MANUAL_ACLS = "preserveManualAcls";
    public static final String RECURSIVE_ACL_SYNC_ENABLED = "recursiveAclSyncEnabled";
    public static final String DEFAULT_ACL_INHERITANCE_ENABLED = "defaultAclInheritanceEnabled";

    public static final String STORAGE_ACCOUNT_RESOURCE = "storageaccount";
    public static final String CONTAINER = "container";
    public static final String RELATIVE_PATH = "relativepath";

    public static final String ACCESS_TYPE_READ = "read";
    public static final String ACCESS_TYPE_WRITE = "write";
    public static final String ACCESS_TYPE_DELETE = "delete";
    public static final String ACCESS_TYPE_LIST = "list";
    public static final String ACCESS_TYPE_EXECUTE = "execute";
    public static final String ACCESS_TYPE_SET_ACL = "setAcl";
    public static final String ACCESS_TYPE_RENAME = "rename";

    public static final int MAX_AUTOCOMPLETE_RESULTS = 100;
    public static final int ABFS_LIST_MAX_RESULTS = 500;

    private RangerABFSConstants() {
    }
}
