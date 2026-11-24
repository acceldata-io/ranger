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

package org.apache.ranger.rest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Response body for the Ranger Admin access check endpoint {@code POST /service/plugins/access/check/{service}}.
 * {@code results} is in the same order as the request {@code items}.
 */
@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerAccessCheckResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    private long         policyVersion = -1L;
    private List<Result> results       = new ArrayList<>();

    public RangerAccessCheckResponse() {
    }

    public RangerAccessCheckResponse(long policyVersion, List<Result> results) {
        this.policyVersion = policyVersion;
        this.results       = results != null ? results : new ArrayList<>();
    }

    public long getPolicyVersion() {
        return policyVersion;
    }

    public void setPolicyVersion(long policyVersion) {
        this.policyVersion = policyVersion;
    }

    public List<Result> getResults() {
        return results;
    }

    public void setResults(List<Result> results) {
        this.results = results;
    }

    /** Decision for one request item. */
    @JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Result implements Serializable {
        private static final long serialVersionUID = 1L;

        private String  id;
        private boolean allowed;
        private long    policyId = -1L;
        private boolean audited;
        private String  reason;

        public Result() {
        }

        public Result(String id, boolean allowed, long policyId, boolean audited, String reason) {
            this.id       = id;
            this.allowed  = allowed;
            this.policyId = policyId;
            this.audited  = audited;
            this.reason   = reason;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public boolean isAllowed() {
            return allowed;
        }

        public void setAllowed(boolean allowed) {
            this.allowed = allowed;
        }

        public long getPolicyId() {
            return policyId;
        }

        public void setPolicyId(long policyId) {
            this.policyId = policyId;
        }

        public boolean isAudited() {
            return audited;
        }

        public void setAudited(boolean audited) {
            this.audited = audited;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }
    }
}
