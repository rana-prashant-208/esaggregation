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
package org.elasticsearch.search.shuffle;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Transport messages of the shuffle protocol (docs/shuffle/PROPOSAL.md section 6.6). They are only ever exchanged between
 * fork nodes, after the protocol gate, so they need no cross-version handling.
 */
public final class ShuffleMessages {

    public static final String PARTIAL_ACTION_NAME = "internal:search/shuffle/partial";
    public static final String FINALIZE_ACTION_NAME = "internal:search/shuffle/finalize";
    public static final String FREE_ACTION_NAME = "internal:search/shuffle/free";

    private ShuffleMessages() {}

    /**
     * Serializes a partial aggregation as a NamedWriteable. Partials travel and are buffered in this form: the size is exact
     * for breaker accounting, local delivery (which skips transport serialization) behaves like remote delivery, and every
     * finalize decodes fresh objects ({@code InternalTerms#doReduce} mutates its inputs).
     */
    public static BytesReference serializePartial(InternalAggregation partial) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(partial);
            return out.bytes();
        } catch (IOException e) {
            throw new AssertionError("BytesStreamOutput does not throw", e);
        }
    }

    /** One shard's buckets for one partition, sent to the partition's reducer. */
    public static final class PartialRequest extends TransportRequest {
        private final String shuffleId;
        private final String aggName;
        private final ShardId shardId;
        private final int attempt;
        private final int partition;
        private final String coordinatorNodeId;
        private final int bucketCount;
        private final BytesReference partial;

        public PartialRequest(String shuffleId, String aggName, ShardId shardId, int attempt, int partition,
                              String coordinatorNodeId, int bucketCount, BytesReference partial) {
            this.shuffleId = Objects.requireNonNull(shuffleId);
            this.aggName = Objects.requireNonNull(aggName);
            this.shardId = Objects.requireNonNull(shardId);
            this.attempt = attempt;
            this.partition = partition;
            this.coordinatorNodeId = Objects.requireNonNull(coordinatorNodeId);
            this.bucketCount = bucketCount;
            this.partial = Objects.requireNonNull(partial);
        }

        public PartialRequest(StreamInput in) throws IOException {
            super(in);
            shuffleId = in.readString();
            aggName = in.readString();
            shardId = ShardId.readShardId(in);
            attempt = in.readVInt();
            partition = in.readVInt();
            coordinatorNodeId = in.readString();
            bucketCount = in.readVInt();
            partial = in.readBytesReference();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(shuffleId);
            out.writeString(aggName);
            shardId.writeTo(out);
            out.writeVInt(attempt);
            out.writeVInt(partition);
            out.writeString(coordinatorNodeId);
            out.writeVInt(bucketCount);
            out.writeBytesReference(partial);
        }

        @Override
        public void readFrom(StreamInput in) {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        public String shuffleId() {
            return shuffleId;
        }

        public String aggName() {
            return aggName;
        }

        public ShardId shardId() {
            return shardId;
        }

        public int attempt() {
            return attempt;
        }

        public int partition() {
            return partition;
        }

        public String coordinatorNodeId() {
            return coordinatorNodeId;
        }

        public int bucketCount() {
            return bucketCount;
        }

        public BytesReference partial() {
            return partial;
        }
    }

    /** Asks a reducer to reduce the named attempt of each shard and return the result. */
    public static final class FinalizeRequest extends TransportRequest {
        private final String shuffleId;
        private final Map<ShardId, Integer> keepAttempts;

        public FinalizeRequest(String shuffleId, Map<ShardId, Integer> keepAttempts) {
            this.shuffleId = Objects.requireNonNull(shuffleId);
            this.keepAttempts = Collections.unmodifiableMap(new HashMap<>(keepAttempts));
        }

        public FinalizeRequest(StreamInput in) throws IOException {
            super(in);
            shuffleId = in.readString();
            keepAttempts = Collections.unmodifiableMap(in.readMap(ShardId::readShardId, StreamInput::readVInt));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(shuffleId);
            out.writeMap(keepAttempts, (o, shardId) -> shardId.writeTo(o), StreamOutput::writeVInt);
        }

        @Override
        public void readFrom(StreamInput in) {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        public String shuffleId() {
            return shuffleId;
        }

        public Map<ShardId, Integer> keepAttempts() {
            return keepAttempts;
        }
    }

    /** A reducer's final, exact result for its partitions, plus stats for the profile API (M7). */
    public static final class FinalizeResponse extends TransportResponse {
        private InternalAggregation result;
        private int partialsReceived;
        private long bucketsReceived;
        private long bytesReceived;
        private long mergeTimeNanos;

        public FinalizeResponse() {}

        public FinalizeResponse(@Nullable InternalAggregation result, int partialsReceived, long bucketsReceived, long bytesReceived,
                                long mergeTimeNanos) {
            this.result = result;
            this.partialsReceived = partialsReceived;
            this.bucketsReceived = bucketsReceived;
            this.bytesReceived = bytesReceived;
            this.mergeTimeNanos = mergeTimeNanos;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            result = in.readOptionalNamedWriteable(InternalAggregation.class);
            partialsReceived = in.readVInt();
            bucketsReceived = in.readVLong();
            bytesReceived = in.readVLong();
            mergeTimeNanos = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalNamedWriteable(result);
            out.writeVInt(partialsReceived);
            out.writeVLong(bucketsReceived);
            out.writeVLong(bytesReceived);
            out.writeVLong(mergeTimeNanos);
        }

        /** The reduced aggregation, or {@code null} when no shard was kept. */
        @Nullable
        public InternalAggregation result() {
            return result;
        }

        /** Number of partials (all attempts) received before finalize. */
        public int partialsReceived() {
            return partialsReceived;
        }

        /** Number of buckets in the kept partials. */
        public long bucketsReceived() {
            return bucketsReceived;
        }

        /** Serialized size of all partials received (all attempts). */
        public long bytesReceived() {
            return bytesReceived;
        }

        public long mergeTimeNanos() {
            return mergeTimeNanos;
        }
    }

    /** Frees a shuffle's state on a reducer. Idempotent. */
    public static final class FreeRequest extends TransportRequest {
        private final String shuffleId;

        public FreeRequest(String shuffleId) {
            this.shuffleId = Objects.requireNonNull(shuffleId);
        }

        public FreeRequest(StreamInput in) throws IOException {
            super(in);
            shuffleId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(shuffleId);
        }

        @Override
        public void readFrom(StreamInput in) {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        public String shuffleId() {
            return shuffleId;
        }
    }
}
