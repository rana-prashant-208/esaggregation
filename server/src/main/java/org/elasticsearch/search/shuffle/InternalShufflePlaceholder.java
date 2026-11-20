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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Stands in for a shuffled terms aggregation in a shard's query result (docs/shuffle/PROPOSAL.md section 6.7): the buckets
 * went to the reducers, and the coordinator substitutes the merged result after reducing the query phase (M4).
 * <p>
 * It carries no pipeline aggregators, so the coordinator's final reduce never runs parent pipelines on it. It carries the
 * order, size and min_doc_count so the coordinator can build an empty terms result if no reducer returns one.
 * Rendering it means substitution failed, which must never happen silently.
 */
public final class InternalShufflePlaceholder extends InternalAggregation {

    public static final String NAME = "shuffle_placeholder";

    private final String shuffleId;
    private final BucketOrder order;
    private final int requiredSize;
    private final long minDocCount;

    public InternalShufflePlaceholder(String name, String shuffleId, BucketOrder order, int requiredSize, long minDocCount,
                                      Map<String, Object> metaData) {
        super(name, Collections.emptyList(), metaData);
        this.shuffleId = Objects.requireNonNull(shuffleId);
        this.order = Objects.requireNonNull(order);
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
    }

    public InternalShufflePlaceholder(StreamInput in) throws IOException {
        super(in);
        shuffleId = in.readString();
        order = InternalOrder.Streams.readOrder(in);
        requiredSize = in.readVInt();
        minDocCount = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(shuffleId);
        order.writeTo(out);
        out.writeVInt(requiredSize);
        out.writeVLong(minDocCount);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public String shuffleId() {
        return shuffleId;
    }

    public BucketOrder order() {
        return order;
    }

    public int requiredSize() {
        return requiredSize;
    }

    public long minDocCount() {
        return minDocCount;
    }

    /** All inputs are equivalent placeholders; returning one keeps batched partial reduces (batched_reduce_size) working. */
    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        for (InternalAggregation aggregation : aggregations) {
            if (aggregation instanceof InternalShufflePlaceholder == false
                || shuffleId.equals(((InternalShufflePlaceholder) aggregation).shuffleId) == false) {
                throw new IllegalStateException("cannot reduce shuffle placeholder [" + getName() + "] of shuffle [" + shuffleId
                    + "] with " + aggregation);
            }
        }
        return aggregations.get(0);
    }

    @Override
    public Object getProperty(List<String> path) {
        throw unsubstituted();
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        throw unsubstituted();
    }

    private IllegalStateException unsubstituted() {
        return new IllegalStateException("shuffle placeholder [" + getName() + "] of shuffle [" + shuffleId
            + "] was not substituted with the reducers' result");
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(shuffleId, order, requiredSize, minDocCount);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalShufflePlaceholder other = (InternalShufflePlaceholder) obj;
        return shuffleId.equals(other.shuffleId) && order.equals(other.order) && requiredSize == other.requiredSize
            && minDocCount == other.minDocCount;
    }

    @Override
    public String toString() {
        return "InternalShufflePlaceholder{name=" + getName() + ", shuffleId=" + shuffleId + "}";
    }
}
