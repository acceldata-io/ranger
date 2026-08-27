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

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Resolves Ranger principals (users and groups) to the Microsoft Entra ID
 * (Azure AD) object IDs that ADLS Gen2 named ACL entries require.
 *
 * <p>This plugin assumes the principals entered into Ranger policies <em>are</em>
 * Azure AD object IDs (GUIDs) — typically the case when Ranger usersync sources
 * identities from Azure AD. A principal is therefore used directly as the ACL
 * entry's entity ID after a GUID format check.</p>
 *
 * <p>If a principal is not a valid object ID it is skipped (so the rest of the
 * policy still applies), unless {@code failOnUnresolvedIdentity=true}, in which
 * case an {@link IllegalStateException} is thrown.</p>
 */
public class ABFSIdentityResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ABFSIdentityResolver.class);

    private static final Pattern GUID_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    public enum ResolvedIdentityType { USER, GROUP }

    public static final class ResolvedIdentity {
        private final ResolvedIdentityType type;
        private final String objectId;

        public ResolvedIdentity(ResolvedIdentityType type, String objectId) {
            this.type     = type;
            this.objectId = objectId;
        }

        public ResolvedIdentityType getType() {
            return type;
        }

        public String getObjectId() {
            return objectId;
        }
    }

    private final boolean failOnUnresolvedIdentity;

    public ABFSIdentityResolver(Map<String, String> configs) {
        this.failOnUnresolvedIdentity =
                Boolean.parseBoolean(configs.get(RangerABFSConstants.FAIL_ON_UNRESOLVED_IDENTITY));
    }

    public ResolvedIdentity resolveUser(String rangerUser) {
        return resolve(rangerUser, ResolvedIdentityType.USER);
    }

    public ResolvedIdentity resolveGroup(String rangerGroup) {
        return resolve(rangerGroup, ResolvedIdentityType.GROUP);
    }

    private ResolvedIdentity resolve(String principal, ResolvedIdentityType type) {
        if (StringUtils.isBlank(principal)) {
            return null;
        }

        String objectId = principal.trim();
        if (!looksLikeObjectId(objectId)) {
            String msg = type + " principal '" + principal
                    + "' is not a valid Azure AD object ID (GUID)";
            if (failOnUnresolvedIdentity) {
                throw new IllegalStateException(msg);
            }
            LOG.warn("{}; skipping ACL entry for this principal", msg);
            return null;
        }

        return new ResolvedIdentity(type, objectId);
    }

    private static boolean looksLikeObjectId(String value) {
        return value != null && GUID_PATTERN.matcher(value).matches();
    }
}
