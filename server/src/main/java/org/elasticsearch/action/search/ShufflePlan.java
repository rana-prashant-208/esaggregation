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
package org.elasticsearch.action.search;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The per-search shuffle plan (docs/shuffle/PROPOSAL.md section 5.2). Created once by the coordinator and carried to the
 * shards in the {@link #PLAN_HEADER} thread-context request header, so that shard requests keep the stock wire format.
 */
public final class ShufflePlan implements Writeable {

    /** Thread-context header carrying the encoded plan. */
    public static final String PLAN_HEADER = "_shuffle_plan";
    /** Thread-context header carrying the attempt number of one shard request. */
    public static final String ATTEMPT_HEADER = "_shuffle_attempt";

    /** Version of the header encoding; bump together with {@code ShuffleSettings.PROTOCOL_VERSION}. */
    private static final byte ENCODING_VERSION = 1;

    private final String shuffleId;
    private final String aggName;
    private final List<String> reducers;
    private final int numPartitions;
    private final int[] partitionToReducer;
    private final String coordinatorNodeId;

    public ShufflePlan(String shuffleId, String aggName, List<String> reducers, int numPartitions, int[] partitionToReducer,
                       String coordinatorNodeId) {
        this.shuffleId = Objects.requireNonNull(shuffleId, "shuffleId");
        this.aggName = Objects.requireNonNull(aggName, "aggName");
        this.reducers = Collections.unmodifiableList(Objects.requireNonNull(reducers, "reducers"));
        this.coordinatorNodeId = Objects.requireNonNull(coordinatorNodeId, "coordinatorNodeId");
        if (reducers.isEmpty()) {
            throw new IllegalArgumentException("a shuffle plan needs at least one reducer");
        }
        if (numPartitions < 1) {
            throw new IllegalArgumentException("numPartitions must be at least 1, got [" + numPartitions + "]");
        }
        if (partitionToReducer.length != numPartitions) {
            throw new IllegalArgumentException("partitionToReducer has [" + partitionToReducer.length + "] entries, expected ["
                + numPartitions + "]");
        }
        for (int reducer : partitionToReducer) {
            if (reducer < 0 || reducer >= reducers.size()) {
                throw new IllegalArgumentException("partitionToReducer references unknown reducer [" + reducer + "]");
            }
        }
        this.numPartitions = numPartitions;
        this.partitionToReducer = partitionToReducer.clone();
    }

    /** A v1 plan: one partition per reducer, identity mapping. */
    public static ShufflePlan identity(String shuffleId, String aggName, List<String> reducers, String coordinatorNodeId) {
        int[] mapping = new int[reducers.size()];
        for (int i = 0; i < mapping.length; i++) {
            mapping[i] = i;
        }
        return new ShufflePlan(shuffleId, aggName, reducers, reducers.size(), mapping, coordinatorNodeId);
    }

    public ShufflePlan(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readList(StreamInput::readString), in.readVInt(), in.readVIntArray(),
            in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(shuffleId);
        out.writeString(aggName);
        out.writeStringList(reducers);
        out.writeVInt(numPartitions);
        out.writeVIntArray(partitionToReducer);
        out.writeString(coordinatorNodeId);
    }

    /** Encodes this plan as a compact, header-safe string. */
    public String encode() {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeByte(ENCODING_VERSION);
            writeTo(out);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new AssertionError("BytesStreamOutput does not throw", e);
        }
    }

    /** Decodes a plan produced by {@link #encode()}. */
    public static ShufflePlan decode(String encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final byte[] bytes;
        try {
            bytes = Base64.getUrlDecoder().decode(encoded);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid [" + PLAN_HEADER + "] header", e);
        }
        try (StreamInput in = StreamInput.wrap(bytes)) {
            byte version = in.readByte();
            if (version != ENCODING_VERSION) {
                throw new IllegalArgumentException("unsupported [" + PLAN_HEADER + "] encoding version [" + version + "]");
            }
            ShufflePlan plan = new ShufflePlan(in);
            if (in.available() > 0) {
                throw new IllegalArgumentException("trailing bytes in [" + PLAN_HEADER + "] header");
            }
            return plan;
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid [" + PLAN_HEADER + "] header", e);
        }
    }

    /** Returns the plan in the current thread context, or {@code null} if this request is not part of a shuffle. */
    public static ShufflePlan fromThreadContext(ThreadContext threadContext) {
        String encoded = threadContext.getHeader(PLAN_HEADER);
        return encoded == null ? null : decode(encoded);
    }

    /** Returns the shard attempt in the current thread context. Fails if a plan is present but the attempt is missing. */
    public static int attemptFromThreadContext(ThreadContext threadContext) {
        String attempt = threadContext.getHeader(ATTEMPT_HEADER);
        if (attempt == null) {
            throw new IllegalStateException("[" + ATTEMPT_HEADER + "] header is missing");
        }
        try {
            return Integer.parseInt(attempt);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid [" + ATTEMPT_HEADER + "] header [" + attempt + "]", e);
        }
    }

    public String shuffleId() {
        return shuffleId;
    }

    public String aggName() {
        return aggName;
    }

    public List<String> reducers() {
        return reducers;
    }

    public int numPartitions() {
        return numPartitions;
    }

    /** The index into {@link #reducers()} owning {@code partition}. */
    public int reducerForPartition(int partition) {
        return partitionToReducer[partition];
    }

    public String coordinatorNodeId() {
        return coordinatorNodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShufflePlan that = (ShufflePlan) o;
        return numPartitions == that.numPartitions
            && shuffleId.equals(that.shuffleId)
            && aggName.equals(that.aggName)
            && reducers.equals(that.reducers)
            && Arrays.equals(partitionToReducer, that.partitionToReducer)
            && coordinatorNodeId.equals(that.coordinatorNodeId);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(shuffleId, aggName, reducers, numPartitions, coordinatorNodeId);
        return 31 * result + Arrays.hashCode(partitionToReducer);
    }

    @Override
    public String toString() {
        return "ShufflePlan{shuffleId=" + shuffleId + ", aggName=" + aggName + ", reducers=" + reducers + ", numPartitions="
            + numPartitions + ", partitionToReducer=" + Arrays.toString(partitionToReducer) + ", coordinator=" + coordinatorNodeId
            + "}";
    }
}
