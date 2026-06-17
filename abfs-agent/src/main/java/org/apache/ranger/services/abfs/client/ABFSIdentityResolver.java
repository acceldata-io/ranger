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
package org.apache.ranger.services.abfs.client;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.services.abfs.RangerABFSConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ABFSIdentityResolver {
    private static final Logger LOG = LoggerFactory.getLogger(ABFSIdentityResolver.class);

    private final Map<String, String> userMap;
    private final Map<String, String> groupMap;
    private final Map<String, String> servicePrincipalMap;
    private final boolean failOnUnresolvedIdentity;

    public ABFSIdentityResolver(Map<String, String> configs) {
        this.userMap = parseMapping(configs.get(RangerABFSConstants.USER_IDENTITY_MAP));
        this.groupMap = parseMapping(configs.get(RangerABFSConstants.GROUP_IDENTITY_MAP));
        this.servicePrincipalMap = parseMapping(configs.get(RangerABFSConstants.SERVICE_PRINCIPAL_IDENTITY_MAP));
        this.failOnUnresolvedIdentity = Boolean.parseBoolean(
                StringUtils.defaultIfBlank(configs.get(RangerABFSConstants.FAIL_ON_UNRESOLVED_IDENTITY), "true"));
    }

    public ResolvedIdentity resolveUser(String user) {
        return resolve(user, userMap, ResolvedIdentityType.USER);
    }

    public ResolvedIdentity resolveGroup(String group) {
        return resolve(group, groupMap, ResolvedIdentityType.GROUP);
    }

    public ResolvedIdentity resolveServicePrincipal(String servicePrincipal) {
        return resolve(servicePrincipal, servicePrincipalMap, ResolvedIdentityType.USER);
    }

    private ResolvedIdentity resolve(String name, Map<String, String> mapping, ResolvedIdentityType type) {
        if (StringUtils.isBlank(name)) {
            return null;
        }

        String objectId = mapping.get(name);
        if (StringUtils.isBlank(objectId) && looksLikeObjectId(name)) {
            objectId = name;
        }

        if (StringUtils.isBlank(objectId)) {
            String message = "Unable to resolve ABFS identity '" + name + "' to an Azure AD object ID";
            if (failOnUnresolvedIdentity) {
                throw new IllegalArgumentException(message);
            }
            LOG.warn("{}; skipping identity because {} is false", message, RangerABFSConstants.FAIL_ON_UNRESOLVED_IDENTITY);
            return null;
        }

        return new ResolvedIdentity(type, objectId);
    }

    private static Map<String, String> parseMapping(String configValue) {
        if (StringUtils.isBlank(configValue)) {
            return Collections.emptyMap();
        }

        Map<String, String> ret = new HashMap<>();
        String[] entries = configValue.split("[,\\n]");
        for (String entry : entries) {
            if (StringUtils.isBlank(entry)) {
                continue;
            }

            String[] parts = entry.split("=", 2);
            if (parts.length == 2 && StringUtils.isNotBlank(parts[0]) && StringUtils.isNotBlank(parts[1])) {
                ret.put(parts[0].trim(), parts[1].trim());
            }
        }
        return ret;
    }

    private static boolean looksLikeObjectId(String value) {
        return value != null && value.matches("(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
    }

    public enum ResolvedIdentityType {
        USER,
        GROUP
    }

    public static class ResolvedIdentity {
        private final ResolvedIdentityType type;
        private final String objectId;

        public ResolvedIdentity(ResolvedIdentityType type, String objectId) {
            this.type = type;
            this.objectId = objectId;
        }

        public ResolvedIdentityType getType() {
            return type;
        }

        public String getObjectId() {
            return objectId;
        }
    }
}
