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

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.shuffle.ShuffleService;

import java.io.IOException;

/**
 * Reducer-side shuffle stats of one node (docs/shuffle/PROPOSAL.md, M7).
 */
public class NodeShuffleStats extends BaseNodeResponse implements ToXContentFragment {

    private int activeStates;
    private long reservedBytes;
    private long partialsAccepted;
    private long bytesAccepted;
    private long finalizations;
    private long rejections;
    private long timeouts;

    NodeShuffleStats() {
    }

    public NodeShuffleStats(DiscoveryNode node, int activeStates, long reservedBytes, long partialsAccepted, long bytesAccepted,
                            long finalizations, long rejections, long timeouts) {
        super(node);
        this.activeStates = activeStates;
        this.reservedBytes = reservedBytes;
        this.partialsAccepted = partialsAccepted;
        this.bytesAccepted = bytesAccepted;
        this.finalizations = finalizations;
        this.rejections = rejections;
        this.timeouts = timeouts;
    }

    public static NodeShuffleStats of(DiscoveryNode node, ShuffleService service) {
        return new NodeShuffleStats(node, service.activeStates(), service.reservedBytes(), service.partialsAccepted(),
            service.bytesAccepted(), service.finalizations(), service.rejections(), service.timeouts());
    }

    public static NodeShuffleStats readNodeShuffleStats(StreamInput in) throws IOException {
        NodeShuffleStats stats = new NodeShuffleStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        activeStates = in.readVInt();
        reservedBytes = in.readVLong();
        partialsAccepted = in.readVLong();
        bytesAccepted = in.readVLong();
        finalizations = in.readVLong();
        rejections = in.readVLong();
        timeouts = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(activeStates);
        out.writeVLong(reservedBytes);
        out.writeVLong(partialsAccepted);
        out.writeVLong(bytesAccepted);
        out.writeVLong(finalizations);
        out.writeVLong(rejections);
        out.writeVLong(timeouts);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("active_states", activeStates);
        builder.field("reserved_bytes", reservedBytes);
        builder.field("partials_accepted", partialsAccepted);
        builder.field("bytes_accepted", bytesAccepted);
        builder.field("finalizations", finalizations);
        builder.field("rejections", rejections);
        builder.field("timeouts", timeouts);
        return builder;
    }

    /** Live (not finalized, not freed) shuffle states. */
    public int getActiveStates() {
        return activeStates;
    }

    /** Bytes currently reserved on the request circuit breaker. */
    public long getReservedBytes() {
        return reservedBytes;
    }

    public long getPartialsAccepted() {
        return partialsAccepted;
    }

    public long getBytesAccepted() {
        return bytesAccepted;
    }

    public long getFinalizations() {
        return finalizations;
    }

    public long getRejections() {
        return rejections;
    }

    public long getTimeouts() {
        return timeouts;
    }
}
