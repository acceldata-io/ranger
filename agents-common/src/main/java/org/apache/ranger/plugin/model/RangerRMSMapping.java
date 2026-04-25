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
import java.util.Map;

/**
 * RangerRMSMapping represents a mapping between a high-level resource (e.g., Hive table)
 * and a low-level resource (e.g., HDFS path, S3 location, Ozone key).
 * This is used by RMS (Resource Mapping Server) to enable Hive-to-Storage policy sync.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerRMSMapping implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long   id;
    private Long   hlResourceId;
    private Long   llResourceId;
    private String hlServiceName;
    private String llServiceName;
    private Map<String, RangerPolicy.RangerPolicyResource> hlResourceElements;
    private Map<String, RangerPolicy.RangerPolicyResource> llResourceElements;
    private Long   changeTimestamp;

    public RangerRMSMapping() {
    }

    public RangerRMSMapping(Long hlResourceId, Long llResourceId, String hlServiceName, String llServiceName,
                            Map<String, RangerPolicy.RangerPolicyResource> hlResourceElements,
                            Map<String, RangerPolicy.RangerPolicyResource> llResourceElements) {
        this.hlResourceId = hlResourceId;
        this.llResourceId = llResourceId;
        this.hlServiceName = hlServiceName;
        this.llServiceName = llServiceName;
        this.hlResourceElements = hlResourceElements;
        this.llResourceElements = llResourceElements;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getHlResourceId() {
        return hlResourceId;
    }

    public void setHlResourceId(Long hlResourceId) {
        this.hlResourceId = hlResourceId;
    }

    public Long getLlResourceId() {
        return llResourceId;
    }

    public void setLlResourceId(Long llResourceId) {
        this.llResourceId = llResourceId;
    }

    public String getHlServiceName() {
        return hlServiceName;
    }

    public void setHlServiceName(String hlServiceName) {
        this.hlServiceName = hlServiceName;
    }

    public String getLlServiceName() {
        return llServiceName;
    }

    public void setLlServiceName(String llServiceName) {
        this.llServiceName = llServiceName;
    }

    public Map<String, RangerPolicy.RangerPolicyResource> getHlResourceElements() {
        return hlResourceElements;
    }

    public void setHlResourceElements(Map<String, RangerPolicy.RangerPolicyResource> hlResourceElements) {
        this.hlResourceElements = hlResourceElements;
    }

    public Map<String, RangerPolicy.RangerPolicyResource> getLlResourceElements() {
        return llResourceElements;
    }

    public void setLlResourceElements(Map<String, RangerPolicy.RangerPolicyResource> llResourceElements) {
        this.llResourceElements = llResourceElements;
    }

    public Long getChangeTimestamp() {
        return changeTimestamp;
    }

    public void setChangeTimestamp(Long changeTimestamp) {
        this.changeTimestamp = changeTimestamp;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RangerRMSMapping={");
        sb.append("id=").append(id);
        sb.append(", hlResourceId=").append(hlResourceId);
        sb.append(", llResourceId=").append(llResourceId);
        sb.append(", hlServiceName=").append(hlServiceName);
        sb.append(", llServiceName=").append(llServiceName);
        sb.append(", hlResourceElements=").append(hlResourceElements);
        sb.append(", llResourceElements=").append(llResourceElements);
        sb.append(", changeTimestamp=").append(changeTimestamp);
        sb.append("}");
        return sb.toString();
    }
}
