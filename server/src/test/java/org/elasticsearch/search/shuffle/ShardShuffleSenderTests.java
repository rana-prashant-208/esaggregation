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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.ShufflePlan;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ShufflePartitioner;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class ShardShuffleSenderTests extends ESTestCase {

    private final NamedWriteableRegistry registry =
        new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedWriteables());

    private static DiscoveryNodes nodes(String... ids) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (String id : ids) {
            builder.add(new DiscoveryNode(id, buildNewFakeTransportAddress(), Collections.emptyMap(),
                EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT));
        }
        return builder.localNodeId(ids[0]).build();
    }

    private static StringTerms terms(String name, String... keys) {
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        for (String key : keys) {
            buckets.add(new StringTerms.Bucket(new BytesRef(key), 1, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));
        }
        return new StringTerms(name, BucketOrder.compound(BucketOrder.count(false)), 3, 1, Collections.emptyList(),
            Collections.singletonMap("k", "v"), DocValueFormat.RAW, Integer.MAX_VALUE, false, 0, buckets, 0);
    }

    private static QuerySearchResult result(InternalAggregation... aggs) {
        QuerySearchResult result = new QuerySearchResult();
        result.aggregations(new InternalAggregations(Arrays.asList(aggs)));
        return result;
    }

    private InternalAggregation decode(org.elasticsearch.common.bytes.BytesReference bytes) throws IOException {
        try (StreamInput in = new NamedWriteableAwareStreamInput(bytes.streamInput(), registry)) {
            return in.readNamedWriteable(InternalAggregation.class);
        }
    }

    public void testSendsToEveryReducerIncludingEmptyAndSubstitutes() throws IOException {
        List<String> reducers = Arrays.asList("r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7");
        ShufflePlan plan = ShufflePlan.identity("sh", "top_users", reducers, "coord");
        StringTerms shuffled = terms("top_users", "alice", "bob");
        InternalSum other = new InternalSum("total", 5, DocValueFormat.RAW, Collections.emptyList(), null);
        QuerySearchResult result = result(other, shuffled);
        ShardId shardId = new ShardId("events", "uuid", 3);

        List<ShardShuffleSender.SendTarget> targets = ShardShuffleSender.prepare(plan, 2, shardId, result,
            nodes("coord", "r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7"));

        assertThat(targets.size(), equalTo(8));
        Set<String> keys = new HashSet<>();
        int empty = 0;
        for (int r = 0; r < 8; r++) {
            ShardShuffleSender.SendTarget target = targets.get(r);
            assertThat(target.node.getId(), equalTo(reducers.get(r)));
            assertThat(target.request.shuffleId(), equalTo("sh"));
            assertThat(target.request.aggName(), equalTo("top_users"));
            assertThat(target.request.shardId(), equalTo(shardId));
            assertThat(target.request.attempt(), equalTo(2));
            assertThat(target.request.partition(), equalTo(r));
            assertThat(target.request.coordinatorNodeId(), equalTo("coord"));
            StringTerms part = (StringTerms) decode(target.request.partial());
            assertThat(target.request.bucketCount(), equalTo(part.getBuckets().size()));
            if (part.getBuckets().isEmpty()) {
                empty++;
            }
            for (StringTerms.Bucket bucket : part.getBuckets()) {
                assertThat(ShufflePartitioner.partition(new BytesRef(bucket.getKeyAsString()), 8), equalTo(r));
                assertTrue(keys.add(bucket.getKeyAsString()));
            }
        }
        assertThat(keys, equalTo(new HashSet<>(Arrays.asList("alice", "bob"))));
        assertThat("empty partitions are still sent", empty, equalTo(6));

        // the query result now carries the placeholder; other aggregations are untouched
        InternalAggregations aggs = (InternalAggregations) result.consumeAggs();
        assertThat(aggs.get("total"), sameInstance(other));
        InternalShufflePlaceholder placeholder = (InternalShufflePlaceholder) aggs.get("top_users");
        assertThat(placeholder.shuffleId(), equalTo("sh"));
        assertThat(placeholder.requiredSize(), equalTo(3));
        assertThat(placeholder.minDocCount(), equalTo(1L));
        assertThat(placeholder.getMetaData(), equalTo(Collections.singletonMap("k", "v")));
        assertThat(placeholder.pipelineAggregators().isEmpty(), equalTo(true));
    }

    public void testMissingReducerFails() {
        ShufflePlan plan = ShufflePlan.identity("sh", "top_users", Arrays.asList("r0", "gone"), "coord");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> ShardShuffleSender.prepare(plan, 1,
            new ShardId("events", "uuid", 0), result(terms("top_users", "a")), nodes("coord", "r0")));
        assertThat(e.getMessage(), containsString("[gone] is not in the cluster"));
    }

    public void testMissingAggregationFails() {
        ShufflePlan plan = ShufflePlan.identity("sh", "top_users", Collections.singletonList("r0"), "coord");
        QuerySearchResult result = result(terms("something_else", "a"));
        expectThrows(IllegalStateException.class,
            () -> ShardShuffleSender.prepare(plan, 1, new ShardId("events", "uuid", 0), result, nodes("r0")));
        assertTrue("the original aggregations are put back", result.hasAggs());
    }

    // ---- placeholder ----

    private static InternalShufflePlaceholder placeholder(String shuffleId) {
        return new InternalShufflePlaceholder("top_users", shuffleId, BucketOrder.compound(BucketOrder.count(false)), 10, 1,
            Collections.singletonMap("k", "v"));
    }

    public void testPlaceholderSurvivesBatchedReduce() {
        InternalShufflePlaceholder first = placeholder("sh");
        int shards = randomIntBetween(2, 10);
        // partial (batched) reduces, then a final reduce over the partial results
        List<InternalAggregations> batch = new ArrayList<>();
        for (int i = 0; i < shards; i++) {
            batch.add(new InternalAggregations(Collections.singletonList(i == 0 ? first : placeholder("sh"))));
        }
        InternalAggregations partial = InternalAggregations.reduce(batch,
            new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, false));
        List<InternalAggregations> finalInputs = new ArrayList<>();
        finalInputs.add(partial);
        finalInputs.add(new InternalAggregations(Collections.singletonList(placeholder("sh"))));
        InternalAggregations reduced = InternalAggregations.reduce(finalInputs,
            new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, true));
        assertThat(reduced.get("top_users"), instanceOf(InternalShufflePlaceholder.class));
        assertThat(((InternalShufflePlaceholder) reduced.get("top_users")).shuffleId(), equalTo("sh"));
    }

    public void testPlaceholderRefusesMixedReduce() {
        List<InternalAggregation> mixed = Arrays.asList(placeholder("sh"), placeholder("other"));
        expectThrows(IllegalStateException.class, () -> placeholder("sh").doReduce(mixed,
            new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, true)));
    }

    public void testPlaceholderCannotBeRendered() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> Strings.toString(
            new InternalAggregations(Collections.singletonList(placeholder("sh")))));
        assertThat(e.getMessage(), containsString("was not substituted"));
        expectThrows(IllegalStateException.class, () -> placeholder("sh").getProperty(Collections.singletonList("x")));
    }

    public void testPlaceholderSerialization() throws IOException {
        InternalShufflePlaceholder original = placeholder(randomAlphaOfLength(10));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(original);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                InternalAggregation copy = in.readNamedWriteable(InternalAggregation.class);
                assertEquals(original, copy);
                assertEquals(original.hashCode(), copy.hashCode());
            }
        }
    }
}
