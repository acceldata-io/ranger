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

package org.apache.ranger.plugin.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceResource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ServiceRMSMappings contains the resource mappings for a service.
 * This is used by plugins to download Hive-to-Storage mappings from RMS.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceRMSMappings implements Serializable {
    private static final long serialVersionUID = 1L;

    private String                        serviceName;
    private String                        hlServiceName;
    private Long                          mappingVersion;
    private Long                          lastKnownVersion;
    private List<RMSResourceMapping>      resourceMappings;
    private Map<String, RangerServiceResource> serviceResources;

    public ServiceRMSMappings() {
        this.resourceMappings = new ArrayList<>();
        this.serviceResources = new HashMap<>();
    }

    public ServiceRMSMappings(String serviceName, String hlServiceName, Long mappingVersion) {
        this();
        this.serviceName = serviceName;
        this.hlServiceName = hlServiceName;
        this.mappingVersion = mappingVersion;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getHlServiceName() {
        return hlServiceName;
    }

    public void setHlServiceName(String hlServiceName) {
        this.hlServiceName = hlServiceName;
    }

    public Long getMappingVersion() {
        return mappingVersion;
    }

    public void setMappingVersion(Long mappingVersion) {
        this.mappingVersion = mappingVersion;
    }

    public Long getLastKnownVersion() {
        return lastKnownVersion;
    }

    public void setLastKnownVersion(Long lastKnownVersion) {
        this.lastKnownVersion = lastKnownVersion;
    }

    public List<RMSResourceMapping> getResourceMappings() {
        return resourceMappings;
    }

    public void setResourceMappings(List<RMSResourceMapping> resourceMappings) {
        this.resourceMappings = resourceMappings;
    }

    public Map<String, RangerServiceResource> getServiceResources() {
        return serviceResources;
    }

    public void setServiceResources(Map<String, RangerServiceResource> serviceResources) {
        this.serviceResources = serviceResources;
    }

    public void addResourceMapping(RMSResourceMapping mapping) {
        if (this.resourceMappings == null) {
            this.resourceMappings = new ArrayList<>();
        }
        this.resourceMappings.add(mapping);
    }

    public void addServiceResource(RangerServiceResource resource) {
        if (this.serviceResources == null) {
            this.serviceResources = new HashMap<>();
        }
        if (resource.getGuid() != null) {
            this.serviceResources.put(resource.getGuid(), resource);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ServiceRMSMappings={");
        sb.append("serviceName=").append(serviceName);
        sb.append(", hlServiceName=").append(hlServiceName);
        sb.append(", mappingVersion=").append(mappingVersion);
        sb.append(", lastKnownVersion=").append(lastKnownVersion);
        sb.append(", resourceMappingsCount=").append(resourceMappings != null ? resourceMappings.size() : 0);
        sb.append(", serviceResourcesCount=").append(serviceResources != null ? serviceResources.size() : 0);
        sb.append("}");
        return sb.toString();
    }

    /**
     * RMSResourceMapping represents a single mapping between HL resource (Hive) and LL resource (Storage).
     */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RMSResourceMapping implements Serializable {
        private static final long serialVersionUID = 1L;

        private String hlResourceGuid;
        private String llResourceGuid;
        private Map<String, RangerPolicy.RangerPolicyResource> hlResourceElements;
        private Map<String, RangerPolicy.RangerPolicyResource> llResourceElements;

        public RMSResourceMapping() {
        }

        public RMSResourceMapping(String hlResourceGuid, String llResourceGuid,
                                  Map<String, RangerPolicy.RangerPolicyResource> hlResourceElements,
                                  Map<String, RangerPolicy.RangerPolicyResource> llResourceElements) {
            this.hlResourceGuid = hlResourceGuid;
            this.llResourceGuid = llResourceGuid;
            this.hlResourceElements = hlResourceElements;
            this.llResourceElements = llResourceElements;
        }

        public String getHlResourceGuid() {
            return hlResourceGuid;
        }

        public void setHlResourceGuid(String hlResourceGuid) {
            this.hlResourceGuid = hlResourceGuid;
        }

        public String getLlResourceGuid() {
            return llResourceGuid;
        }

        public void setLlResourceGuid(String llResourceGuid) {
            this.llResourceGuid = llResourceGuid;
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

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("RMSResourceMapping={");
            sb.append("hlResourceGuid=").append(hlResourceGuid);
            sb.append(", llResourceGuid=").append(llResourceGuid);
            sb.append(", hlResourceElements=").append(hlResourceElements);
            sb.append(", llResourceElements=").append(llResourceElements);
            sb.append("}");
            return sb.toString();
        }
    }
}
