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
package org.apache.ranger.entity;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * Persisted record of a deleted RMS resource mapping. The set of rows in
 * this table at a given mapping_version represents the deletions that
 * happened to bring the mapping graph up to that version. Plugins consuming
 * incremental delta downloads use these rows to remove resources from their
 * local cache.
 *
 * The table is append-only during normal operation and is pruned on a fixed
 * retention window defined in {@code RMSMgr}; pruning advances the
 * deletion-tracking watermark stored on
 * {@link XXRMSMappingProvider#deletionTrackingFromVersion}.
 */
@Entity
@Cacheable(false)
@Table(name = "x_rms_deletion_log")
public class XXRMSDeletionLog implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "X_RMS_DELETION_LOG_SEQ", sequenceName = "X_RMS_DELETION_LOG_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_RMS_DELETION_LOG_SEQ")
    @Column(name = "id")
    protected Long id;

    @Column(name = "version")
    protected Long version;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "change_timestamp")
    protected Date changeTimestamp;

    @Column(name = "hl_resource_guid", length = 64)
    protected String hlResourceGuid;

    @Column(name = "ll_resource_guid", length = 64)
    protected String llResourceGuid;

    @Column(name = "ll_service_id")
    protected Long llServiceId;

    public XXRMSDeletionLog() {}

    public XXRMSDeletionLog(Long version, String hlResourceGuid, String llResourceGuid, Long llServiceId) {
        this.version = version;
        this.changeTimestamp = new Date();
        this.hlResourceGuid = hlResourceGuid;
        this.llResourceGuid = llResourceGuid;
        this.llServiceId = llServiceId;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }

    public Date getChangeTimestamp() { return changeTimestamp; }
    public void setChangeTimestamp(Date changeTimestamp) { this.changeTimestamp = changeTimestamp; }

    public String getHlResourceGuid() { return hlResourceGuid; }
    public void setHlResourceGuid(String hlResourceGuid) { this.hlResourceGuid = hlResourceGuid; }

    public String getLlResourceGuid() { return llResourceGuid; }
    public void setLlResourceGuid(String llResourceGuid) { this.llResourceGuid = llResourceGuid; }

    public Long getLlServiceId() { return llServiceId; }
    public void setLlServiceId(Long llServiceId) { this.llServiceId = llServiceId; }

    @Override
    public String toString() {
        return "XXRMSDeletionLog{id=" + id +
                ", version=" + version +
                ", changeTimestamp=" + changeTimestamp +
                ", hlResourceGuid='" + hlResourceGuid + '\'' +
                ", llResourceGuid='" + llResourceGuid + '\'' +
                ", llServiceId=" + llServiceId +
                '}';
    }
}
