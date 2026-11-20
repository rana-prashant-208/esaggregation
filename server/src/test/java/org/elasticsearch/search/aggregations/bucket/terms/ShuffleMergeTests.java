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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ShuffleMergeTests extends ESTestCase {

    private static final BucketOrder COUNT_DESC = BucketOrder.compound(BucketOrder.count(false));

    /** key:count pairs; the "bytes" sum is count * 10 + key length, so it orders differently from the count. */
    private static StringTerms reducer(BucketOrder order, int size, long otherDocCount, Object... keyCounts) {
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < keyCounts.length; i += 2) {
            String key = (String) keyCounts[i];
            long count = ((Number) keyCounts[i + 1]).longValue();
            InternalAggregations subs = new InternalAggregations(Collections.singletonList(
                new InternalSum("bytes", bytes(key, count), DocValueFormat.RAW, Collections.emptyList(), null)));
            buckets.add(new StringTerms.Bucket(new BytesRef(key), count, subs, false, 0, DocValueFormat.RAW));
        }
        return new StringTerms("top_users", order, size, 1, Collections.emptyList(), null, DocValueFormat.RAW, Integer.MAX_VALUE,
            false, otherDocCount, buckets, 0);
    }

    private static double bytes(String key, long count) {
        return 1000 - count * 10 + key.length();
    }

    private static List<String> keys(InternalAggregation result) {
        List<String> keys = new ArrayList<>();
        for (StringTerms.Bucket bucket : ((StringTerms) result).getBuckets()) {
            keys.add(bucket.getKeyAsString());
        }
        return keys;
    }

    private static InternalAggregation merge(BucketOrder order, int size, InternalAggregation... reducers) {
        return ShuffleMerge.merge("top_users", order, size, 1, null, Arrays.asList(reducers));
    }

    /** The canonical example from PROPOSAL.md section 10 split over three reducers. */
    private static InternalAggregation[] canonical(BucketOrder order, int size) {
        // each reducer returns its own top `size` under `order`, already exact; others' counts went into otherDocCount
        return new InternalAggregation[] {
            reducer(order, size, 0, "carol", 150, "erin", 85),
            reducer(order, size, 25, "bob", 95, "alice", 90, "frank", 25),      // reducer also dropped heidi 25
            reducer(order, size, 15 + 25, "dave", 80, "grace", 70, "ivan", 25),  // dropped judy 15 and one 25
        };
    }

    public void testCountDescending() {
        StringTerms result = (StringTerms) merge(COUNT_DESC, 3, canonical(COUNT_DESC, 3));
        assertThat(keys(result), equalTo(Arrays.asList("carol", "bob", "alice")));
        // 85 + 25 + 80 + 70 + 25 (not in top) + 25 + 40 (reducers' others)
        assertThat(result.getSumOfOtherDocCounts(), equalTo(85L + 25 + 80 + 70 + 25 + 25 + 40));
        assertThat(result.getDocCountError(), equalTo(0L));
        assertThat(result.getBucketByKey("carol").getDocCount(), equalTo(150L));
    }

    public void testCountAscendingWithKeyTieBreak() {
        BucketOrder order = BucketOrder.compound(BucketOrder.count(true));
        StringTerms result = (StringTerms) merge(order, 4, canonical(order, 4));
        // frank 25 and ivan 25 tie on count, the implicit key tie-breaker puts frank first
        assertThat(keys(result), equalTo(Arrays.asList("frank", "ivan", "grace", "dave")));
    }

    public void testKeyAscending() {
        BucketOrder order = BucketOrder.key(true);
        StringTerms result = (StringTerms) merge(order, 3, canonical(order, 3));
        assertThat(keys(result), equalTo(Arrays.asList("alice", "bob", "carol")));
    }

    public void testKeyDescending() {
        BucketOrder order = BucketOrder.key(false);
        StringTerms result = (StringTerms) merge(order, 2, canonical(order, 2));
        assertThat(keys(result), equalTo(Arrays.asList("ivan", "grace")));
    }

    public void testSubAggregationOrder() {
        // bytes = 1000 - 10 * count + key length: ascending bytes means descending count, ties broken by shorter key
        BucketOrder order = BucketOrder.compound(BucketOrder.aggregation("bytes", true));
        StringTerms result = (StringTerms) merge(order, 3, canonical(order, 3));
        assertThat(keys(result), equalTo(Arrays.asList("carol", "bob", "alice")));
        BucketOrder desc = BucketOrder.compound(BucketOrder.aggregation("bytes", false));
        StringTerms lowest = (StringTerms) merge(desc, 2, canonical(desc, 2));
        // frank (25, len 5 -> 755) and ivan (25, len 4 -> 754)
        assertThat(keys(lowest), equalTo(Arrays.asList("frank", "ivan")));
    }

    public void testSizeLargerThanTotal() {
        StringTerms result = (StringTerms) merge(COUNT_DESC, 100, reducer(COUNT_DESC, 100, 0, "a", 1),
            reducer(COUNT_DESC, 100, 0, "b", 2));
        assertThat(keys(result), equalTo(Arrays.asList("b", "a")));
        assertThat(result.getSumOfOtherDocCounts(), equalTo(0L));
    }

    public void testNullAndUnmappedReducersAreIgnored() {
        UnmappedTerms unmapped = new UnmappedTerms("top_users", COUNT_DESC, 3, 1, Collections.emptyList(), null);
        StringTerms result = (StringTerms) merge(COUNT_DESC, 3, null, unmapped, reducer(COUNT_DESC, 3, 7, "x", 5));
        assertThat(keys(result), equalTo(Collections.singletonList("x")));
        assertThat(result.getSumOfOtherDocCounts(), equalTo(7L));
    }

    public void testAllEmptyGivesUnmappedWithPlaceholderShape() {
        InternalAggregation result = ShuffleMerge.merge("top_users", COUNT_DESC, 3, 2, Collections.singletonMap("m", "v"),
            Arrays.asList(null, null));
        assertThat(result, instanceOf(UnmappedTerms.class));
        assertThat(result.getName(), equalTo("top_users"));
        assertThat(result.getMetaData(), equalTo(Collections.singletonMap("m", "v")));
    }

    public void testLongTerms() {
        List<LongTerms.Bucket> a = Arrays.asList(
            new LongTerms.Bucket(7, 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW),
            new LongTerms.Bucket(3, 4, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));
        List<LongTerms.Bucket> b = Collections.singletonList(new LongTerms.Bucket(5, 8, InternalAggregations.EMPTY, false, 0,
            DocValueFormat.RAW));
        LongTerms ra = new LongTerms("top_users", COUNT_DESC, 2, 1, Collections.emptyList(), null, DocValueFormat.RAW,
            Integer.MAX_VALUE, false, 0, a, 0);
        LongTerms rb = new LongTerms("top_users", COUNT_DESC, 2, 1, Collections.emptyList(), null, DocValueFormat.RAW,
            Integer.MAX_VALUE, false, 1, b, 0);
        LongTerms result = (LongTerms) merge(COUNT_DESC, 2, ra, rb);
        assertThat(result.getBuckets().get(0).getKeyAsNumber().longValue(), equalTo(7L));
        assertThat(result.getBuckets().get(1).getKeyAsNumber().longValue(), equalTo(5L));
        assertThat(result.getSumOfOtherDocCounts(), equalTo(4L + 1));
    }

    public void testMixedTypesFail() {
        LongTerms longs = new LongTerms("top_users", COUNT_DESC, 2, 1, Collections.emptyList(), null, DocValueFormat.RAW,
            Integer.MAX_VALUE, false, 0, Collections.emptyList(), 0);
        expectThrows(IllegalStateException.class, () -> merge(COUNT_DESC, 2, reducer(COUNT_DESC, 2, 0, "a", 1), longs));
    }

    /** The merge does not re-run reduce: a bucket keeps its reducer-computed values (and parent pipelines are not re-applied). */
    public void testBucketsArePassedThrough() {
        StringTerms r = reducer(COUNT_DESC, 3, 0, "alice", 90);
        StringTerms result = (StringTerms) merge(COUNT_DESC, 3, r);
        assertThat(((InternalSum) result.getBucketByKey("alice").getAggregations().get("bytes")).getValue(),
            equalTo(bytes("alice", 90)));
    }
}
