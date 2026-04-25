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

package org.apache.ranger.db;

import java.util.Date;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXRMSMappingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 */
@Service
public class XXRMSMappingProviderDao extends BaseDao<XXRMSMappingProvider> {

    private static final Logger LOG = LoggerFactory.getLogger(XXRMSMappingProviderDao.class);

    public XXRMSMappingProviderDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<XXRMSMappingProvider> getResource() {
        List<XXRMSMappingProvider> allResource = getAll();
        return allResource;
    }

    public XXRMSMappingProvider findByName(String name) {
        if (name == null) {
            return null;
        }
        try {
            return getEntityManager()
                    .createNamedQuery("XXRMSMappingProvider.findByName", tClass)
                    .setParameter("name", name).getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    public Long getLastKnownVersion(String providerName) {

        XXRMSMappingProvider mappingProvider = findByName(providerName);

        return mappingProvider != null ? mappingProvider.getLastKnownVersion() : 0L;
    }

    public void updateLastKnownVersion(String providerName, long currentNotificationId) {

        XXRMSMappingProvider mappingProvider = findByName(providerName);

        if (mappingProvider != null) {
            if (currentNotificationId >= -1L) {
                mappingProvider.setLastKnownVersion(currentNotificationId);
                mappingProvider.setChangeTimestamp(new Date());
                update(mappingProvider);
            } else {
                LOG.error("currentNotificationId cannot be set to a value less than -1");
            }
        } else {
            LOG.error("Cannot update lastKnownVersion for providerName:[" + providerName + "]");
        }
    }

    /**
     * Snapshot the deletion-tracking watermark on first delta poll after upgrade.
     * <p>
     * Writes only the {@code deletion_tracking_from_version} column and only if
     * no real value was ever persisted (NULL or 0). This avoids the lost-update
     * hazard with the RMS poller's concurrent {@code last_known_version} write
     * which would otherwise occur if we re-saved the whole entity from a stale
     * read on the read path.
     *
     * @return number of rows updated (0 if already initialized).
     */
    public int initDeletionTrackingFromVersion(String providerName, long initVersion) {
        if (providerName == null) {
            return 0;
        }
        return getEntityManager()
                .createNamedQuery("XXRMSMappingProvider.initDeletionTrackingFromVersion")
                .setParameter("name", providerName)
                .setParameter("initVersion", initVersion)
                .executeUpdate();
    }

    /**
     * Advance the deletion-tracking watermark forward (monotonic).
     * <p>
     * The query uses a {@code WHERE deletion_tracking_from_version < :minVersion}
     * guard, so concurrent advances and lazy-inits are safe: only updates that
     * truly move the watermark forward are applied. As above, this writes only
     * the watermark column to avoid clobbering {@code last_known_version}.
     *
     * @return number of rows updated (0 if the watermark is already at or
     *         beyond {@code minVersion}).
     */
    public int advanceDeletionTrackingFromVersion(String providerName, long minVersion) {
        if (providerName == null) {
            return 0;
        }
        return getEntityManager()
                .createNamedQuery("XXRMSMappingProvider.advanceDeletionTrackingFromVersion")
                .setParameter("name", providerName)
                .setParameter("minVersion", minVersion)
                .executeUpdate();
    }
}

