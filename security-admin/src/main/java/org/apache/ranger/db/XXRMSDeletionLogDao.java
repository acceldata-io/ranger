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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXRMSDeletionLog;
import org.springframework.stereotype.Service;

/**
 * DAO for {@link XXRMSDeletionLog}, the persistent record of deleted RMS
 * mappings used to construct correct delta downloads across Admin restarts
 * and HA failovers. Entry points:
 * <ul>
 *   <li>{@link #recordAll(Collection)} appends a batch of deletions for a
 *       single mapping_version.</li>
 *   <li>{@link #findByServiceSinceVersion(Long, Long)} returns the
 *       deletions a plugin needs to apply to advance its local cache.</li>
 *   <li>{@link #getMinVersion()} feeds the deletion-tracking watermark
 *       check.</li>
 *   <li>{@link #deleteOlderThanVersion(Long)} prunes old records.</li>
 * </ul>
 */
@Service
public class XXRMSDeletionLogDao extends BaseDao<XXRMSDeletionLog> {

    public XXRMSDeletionLogDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public void recordAll(Collection<XXRMSDeletionLog> rows) {
        if (rows == null || rows.isEmpty()) {
            return;
        }
        for (XXRMSDeletionLog row : rows) {
            getEntityManager().persist(row);
        }
    }

    @SuppressWarnings("unchecked")
    public List<XXRMSDeletionLog> findByServiceSinceVersion(Long serviceId, Long sinceVersion) {
        if (serviceId == null || sinceVersion == null) {
            return Collections.emptyList();
        }
        try {
            return getEntityManager()
                    .createNamedQuery("XXRMSDeletionLog.findByServiceSinceVersion")
                    .setParameter("serviceId", serviceId)
                    .setParameter("sinceVersion", sinceVersion)
                    .getResultList();
        } catch (NoResultException e) {
            return Collections.emptyList();
        }
    }

    /**
     * Returns the smallest version present in the deletion log, or
     * {@code null} if the log is empty. An empty log paired with a
     * watermark on {@code XXRMSMappingProvider} is the post-upgrade /
     * fresh-install state.
     */
    public Long getMinVersion() {
        try {
            return (Long) getEntityManager()
                    .createNamedQuery("XXRMSDeletionLog.getMinVersion")
                    .getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    /**
     * Delete every deletion record whose version is strictly less than
     * {@code minRetainedVersion}. Returns the number of rows affected.
     */
    public int deleteOlderThanVersion(Long minRetainedVersion) {
        if (minRetainedVersion == null) {
            return 0;
        }
        return getEntityManager()
                .createNamedQuery("XXRMSDeletionLog.deleteOlderThanVersion")
                .setParameter("minRetainedVersion", minRetainedVersion)
                .executeUpdate();
    }
}
