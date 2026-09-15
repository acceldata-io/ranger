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

package org.apache.ranger.plugin.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Date;

/**
 * RangerRMSNotification represents a notification from HMS (Hive Metastore) about
 * changes to database/table metadata. This is used by RMS to track incremental changes.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerRMSNotification implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum ChangeType {
        CREATE_DATABASE,
        DROP_DATABASE,
        ALTER_DATABASE,
        CREATE_TABLE,
        DROP_TABLE,
        ALTER_TABLE,
        ADD_PARTITION,
        DROP_PARTITION,
        ALTER_PARTITION
    }

    private Long       id;
    private String     hmsName;
    private Long       notificationId;
    private Date       changeTimestamp;
    private ChangeType changeType;
    private Long       hlResourceId;
    private Long       hlServiceId;
    private Long       llResourceId;
    private Long       llServiceId;
    private String     databaseName;
    private String     tableName;
    private String     location;

    public RangerRMSNotification() {
    }

    public RangerRMSNotification(String hmsName, Long notificationId, ChangeType changeType,
                                  String databaseName, String tableName, String location) {
        this.hmsName = hmsName;
        this.notificationId = notificationId;
        this.changeType = changeType;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.location = location;
        this.changeTimestamp = new Date();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getHmsName() {
        return hmsName;
    }

    public void setHmsName(String hmsName) {
        this.hmsName = hmsName;
    }

    public Long getNotificationId() {
        return notificationId;
    }

    public void setNotificationId(Long notificationId) {
        this.notificationId = notificationId;
    }

    public Date getChangeTimestamp() {
        return changeTimestamp;
    }

    public void setChangeTimestamp(Date changeTimestamp) {
        this.changeTimestamp = changeTimestamp;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public void setChangeType(ChangeType changeType) {
        this.changeType = changeType;
    }

    public Long getHlResourceId() {
        return hlResourceId;
    }

    public void setHlResourceId(Long hlResourceId) {
        this.hlResourceId = hlResourceId;
    }

    public Long getHlServiceId() {
        return hlServiceId;
    }

    public void setHlServiceId(Long hlServiceId) {
        this.hlServiceId = hlServiceId;
    }

    public Long getLlResourceId() {
        return llResourceId;
    }

    public void setLlResourceId(Long llResourceId) {
        this.llResourceId = llResourceId;
    }

    public Long getLlServiceId() {
        return llServiceId;
    }

    public void setLlServiceId(Long llServiceId) {
        this.llServiceId = llServiceId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RangerRMSNotification={");
        sb.append("id=").append(id);
        sb.append(", hmsName=").append(hmsName);
        sb.append(", notificationId=").append(notificationId);
        sb.append(", changeTimestamp=").append(changeTimestamp);
        sb.append(", changeType=").append(changeType);
        sb.append(", databaseName=").append(databaseName);
        sb.append(", tableName=").append(tableName);
        sb.append(", location=").append(location);
        sb.append("}");
        return sb.toString();
    }
}
