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

package org.apache.ranger.services.yunikorn.client.json.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Models the JSON response from
 * GET /ws/v1/partition/{partition}/queues
 *
 * The response is a single PartitionQueueDAOInfo object representing the
 * partition's root queue, with nested {@code children} forming the queue
 * tree. We model only the fields relevant to building dotted queue paths
 * for Ranger resource lookup; all other fields (resources, properties,
 * usage metrics, etc.) are ignored via {@code @JsonIgnoreProperties}.
 */
@JsonAutoDetect(getterVisibility = Visibility.NONE,
                setterVisibility = Visibility.NONE,
                fieldVisibility  = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class YuniKornQueueResponse implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("queuename")
    private String queueName;

    @JsonProperty("partition")
    private String partition;

    @JsonProperty("children")
    private List<YuniKornQueueResponse> children;

    public String getQueueName() {
        return queueName;
    }

    public String getPartition() {
        return partition;
    }

    public List<YuniKornQueueResponse> getChildren() {
        return children;
    }

    /**
     * Walks the queue tree rooted at this node and appends every queue's
     * fully-qualified dotted path to {@code out}.
     *
     * <p>YuniKorn's REST API already returns each {@code queuename} as a
     * fully-qualified dotted path (e.g. {@code root.research.nlp}), so this
     * traversal simply emits {@code queueName} directly without prepending
     * the parent path.
     *
     * @param out accumulator to receive paths in pre-order traversal
     */
    public void collectQueuePaths(List<String> out) {
        if (queueName == null) {
            return;
        }
        out.add(queueName);
        if (children != null) {
            for (YuniKornQueueResponse child : children) {
                child.collectQueuePaths(out);
            }
        }
    }
}
