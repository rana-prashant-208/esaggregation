/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.cluster.node.shuffle;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class ShuffleStatsResponse extends BaseNodesResponse<NodeShuffleStats> implements ToXContentFragment {

    ShuffleStatsResponse() {
    }

    public ShuffleStatsResponse(ClusterName clusterName, List<NodeShuffleStats> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeShuffleStats> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeShuffleStats::readNodeShuffleStats);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeShuffleStats> nodes) throws IOException {
        out.writeStreamableList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (NodeShuffleStats stats : getNodes()) {
            builder.startObject(stats.getNode().getId());
            builder.field("name", stats.getNode().getName());
            builder.startObject("shuffle");
            stats.toXContent(builder, params);
            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
