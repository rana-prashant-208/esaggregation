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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ShufflePartitionerTests extends ESTestCase {

    /**
     * Golden values. The partition function is part of the shuffle wire protocol: if this test fails, nodes running the old and
     * new code would route the same key to different reducers. Do not update these values without bumping
     * {@code ShuffleSettings.PROTOCOL_VERSION}.
     */
    public void testKeywordGoldenValues() {
        assertKeyword("alice", 2, 5, 37);
        assertKeyword("bob", 2, 7, 151);
        assertKeyword("carol", 4, 10, 90);
        assertKeyword("dave", 3, 4, 4);
        assertKeyword("erin", 4, 4, 100);
        assertKeyword("frank", 1, 11, 235);
        assertKeyword("grace", 0, 1, 225);
        assertKeyword("heidi", 0, 4, 116);
        assertKeyword("ivan", 2, 14, 110);
        assertKeyword("judy", 0, 1, 1);
        assertKeyword("", 1, 5, 53);
        assertKeyword("été", 1, 12, 124);
    }

    public void testLongGoldenValues() {
        assertLong(0L, 3, 15, 63);
        assertLong(1L, 2, 11, 123);
        assertLong(-1L, 2, 5, 229);
        assertLong(42L, 1, 6, 54);
        assertLong(Long.MAX_VALUE, 0, 8, 104);
        assertLong(Long.MIN_VALUE, 1, 0, 0);
        assertLong(1546300800000L, 1, 8, 40); // a date: 2019-01-01T00:00:00Z
    }

    public void testSeedIsPinned() {
        assertThat(ShufflePartitioner.SHUFFLE_HASH_SEED, equalTo(0x5f3759df_2b7e1516L));
    }

    private static void assertKeyword(String key, int p5, int p16, int p256) {
        assertThat(key, ShufflePartitioner.partition(new BytesRef(key), 5), equalTo(p5));
        assertThat(key, ShufflePartitioner.partition(new BytesRef(key), 16), equalTo(p16));
        assertThat(key, ShufflePartitioner.partition(new BytesRef(key), 256), equalTo(p256));
    }

    private static void assertLong(long key, int p5, int p16, int p256) {
        assertThat(ShufflePartitioner.partition(key, 5), equalTo(p5));
        assertThat(ShufflePartitioner.partition(key, 16), equalTo(p16));
        assertThat(ShufflePartitioner.partition(key, 256), equalTo(p256));
    }

    /** A BytesRef slice of a larger array must hash like the same bytes on their own. */
    public void testKeywordUsesOnlyTheSlice() {
        byte[] padded = "xxalicexx".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(ShufflePartitioner.partition(new BytesRef(padded, 2, 5), 256),
            equalTo(ShufflePartitioner.partition(new BytesRef("alice"), 256)));
    }

    public void testPartitionInRange() {
        for (int i = 0; i < 1000; i++) {
            int n = randomIntBetween(1, 512);
            int p = ShufflePartitioner.partition(new BytesRef(randomUnicodeOfLengthBetween(0, 20)), n);
            assertTrue(p >= 0 && p < n);
            int q = ShufflePartitioner.partition(randomLong(), n);
            assertTrue(q >= 0 && q < n);
        }
    }

    // ---- split ----

    private static StringTerms stringTerms(int keys, long otherDocCount) {
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        while (buckets.size() < keys) {
            String key = randomAlphaOfLengthBetween(1, 10);
            if (seen.add(key)) {
                buckets.add(new StringTerms.Bucket(new BytesRef(key), randomIntBetween(1, 100), InternalAggregations.EMPTY, false, 0,
                    DocValueFormat.RAW));
            }
        }
        return new StringTerms("t", BucketOrder.compound(BucketOrder.count(false)), 10, 1, Collections.emptyList(),
            Collections.singletonMap("meta", "data"), DocValueFormat.RAW, Integer.MAX_VALUE, false, otherDocCount, buckets, 0);
    }

    private static LongTerms longTerms(int keys) {
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        Set<Long> seen = new HashSet<>();
        while (buckets.size() < keys) {
            long key = randomLong();
            if (seen.add(key)) {
                buckets.add(new LongTerms.Bucket(key, randomIntBetween(1, 100), InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));
            }
        }
        return new LongTerms("t", BucketOrder.key(true), 10, 0, Collections.emptyList(), null, DocValueFormat.RAW, Integer.MAX_VALUE,
            false, 0, buckets, 0);
    }

    public void testSplitStringTermsPreservesEveryBucketOnce() {
        int numPartitions = randomIntBetween(1, 20);
        long other = randomBoolean() ? 0 : randomIntBetween(1, 100);
        StringTerms terms = stringTerms(randomIntBetween(0, 200), other);
        List<InternalAggregation> parts = ShufflePartitioner.split(terms, numPartitions);
        assertThat(parts.size(), equalTo(numPartitions));

        Map<String, Long> seen = new HashMap<>();
        long otherSum = 0;
        for (int p = 0; p < numPartitions; p++) {
            StringTerms part = (StringTerms) parts.get(p);
            assertThat(part.getName(), equalTo("t"));
            assertThat(part.order, equalTo(terms.order));
            assertThat(part.requiredSize, equalTo(terms.requiredSize));
            assertThat(part.minDocCount, equalTo(terms.minDocCount));
            assertThat(part.getMetaData(), equalTo(terms.getMetaData()));
            assertThat(part.getShardSize(), equalTo(Integer.MAX_VALUE));
            assertThat(part.getDocCountError(), equalTo(0L));
            otherSum += part.getSumOfOtherDocCounts();
            for (StringTerms.Bucket bucket : part.getBuckets()) {
                assertThat(ShufflePartitioner.partition(bucket.termBytes, numPartitions), equalTo(p));
                assertNull("bucket in two partitions", seen.put(bucket.getKeyAsString(), bucket.getDocCount()));
            }
        }
        Map<String, Long> expected = new HashMap<>();
        for (StringTerms.Bucket bucket : terms.getBuckets()) {
            expected.put(bucket.getKeyAsString(), bucket.getDocCount());
        }
        assertThat(seen, equalTo(expected));
        assertThat(otherSum, equalTo(other));
    }

    public void testSplitLongTermsPreservesEveryBucketOnce() {
        int numPartitions = randomIntBetween(1, 20);
        LongTerms terms = longTerms(randomIntBetween(0, 200));
        List<InternalAggregation> parts = ShufflePartitioner.split(terms, numPartitions);
        Set<Long> seen = new HashSet<>();
        for (int p = 0; p < numPartitions; p++) {
            for (LongTerms.Bucket bucket : ((LongTerms) parts.get(p)).getBuckets()) {
                assertThat(ShufflePartitioner.partition(bucket.term, numPartitions), equalTo(p));
                assertTrue(seen.add(bucket.term));
            }
        }
        assertThat(seen.size(), equalTo(terms.getBuckets().size()));
    }

    public void testEmptyPartitionsExist() {
        // more partitions than keys: some parts are empty, but every partition gets a part
        StringTerms terms = stringTerms(2, 0);
        List<InternalAggregation> parts = ShufflePartitioner.split(terms, 16);
        assertThat(parts.size(), equalTo(16));
        int empty = 0;
        for (InternalAggregation part : parts) {
            if (((StringTerms) part).getBuckets().isEmpty()) {
                empty++;
            }
        }
        assertThat(empty, equalTo(14 + (sameReducer(terms) ? 1 : 0)));
    }

    private static boolean sameReducer(StringTerms terms) {
        return ShufflePartitioner.partition(terms.getBuckets().get(0).termBytes, 16)
            == ShufflePartitioner.partition(terms.getBuckets().get(1).termBytes, 16);
    }

    public void testSplitGroupsPartitionsByOwner() {
        // 8 partitions on 3 reducers: every key goes to the reducer owning its partition
        int[] owner = {0, 1, 2, 0, 1, 2, 0, 1};
        StringTerms terms = stringTerms(100, 0);
        List<InternalAggregation> parts = ShufflePartitioner.split(terms, 8, 3, p -> owner[p]);
        assertThat(parts.size(), equalTo(3));
        int total = 0;
        for (int r = 0; r < 3; r++) {
            for (StringTerms.Bucket bucket : ((StringTerms) parts.get(r)).getBuckets()) {
                assertThat(owner[ShufflePartitioner.partition(bucket.termBytes, 8)], equalTo(r));
                total++;
            }
        }
        assertThat(total, equalTo(100));
    }

    public void testSplitUnmapped() {
        UnmappedTerms unmapped = new UnmappedTerms("t", BucketOrder.count(false), 10, 1, Collections.emptyList(), null);
        List<InternalAggregation> parts = ShufflePartitioner.split(unmapped, 3);
        assertThat(parts.size(), equalTo(3));
        for (InternalAggregation part : parts) {
            assertThat(part, sameInstance(unmapped));
        }
    }

    public void testSplitRejectsDoubleTerms() {
        DoubleTerms doubles = new DoubleTerms("t", BucketOrder.count(false), 10, 1, Collections.emptyList(), null, DocValueFormat.RAW,
            10, false, 0, Collections.emptyList(), 0);
        expectThrows(IllegalArgumentException.class, () -> ShufflePartitioner.split(doubles, 3));
    }
}
