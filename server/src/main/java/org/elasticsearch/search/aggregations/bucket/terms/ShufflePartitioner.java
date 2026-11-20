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

import com.carrotsearch.hppc.BitMixer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntUnaryOperator;

/**
 * The shuffle partition function and the shard-side split of a terms result (docs/shuffle/PROPOSAL.md sections 5.3 and 6.4).
 * <p>
 * <b>The partition function is part of the wire protocol.</b> Changing {@link #SHUFFLE_HASH_SEED} or either hash changes which
 * reducer owns a key; nodes disagreeing on it would silently produce wrong results. Any change must bump
 * {@code ShuffleSettings.PROTOCOL_VERSION}. {@code ShufflePartitionerTests} pins golden values.
 */
public final class ShufflePartitioner {

    /** Differs from the document routing seed (Murmur3HashFunction uses 0). Never change within a protocol version. */
    public static final long SHUFFLE_HASH_SEED = 0x5f3759df_2b7e1516L;

    private ShufflePartitioner() {}

    /** Partition of a keyword term. */
    public static int partition(BytesRef key, int numPartitions) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(key.bytes, key.offset, key.length, SHUFFLE_HASH_SEED, new MurmurHash3.Hash128());
        return (int) Math.floorMod(hash.h1, (long) numPartitions);
    }

    /** Partition of a long (integral or date) term. */
    public static int partition(long key, int numPartitions) {
        return (int) Math.floorMod(BitMixer.mix64(key ^ SHUFFLE_HASH_SEED), (long) numPartitions);
    }

    /**
     * Splits one shard's terms result into {@code numPartitions} results of the same type, name, order, size, min_doc_count,
     * pipelines and metadata. Every bucket lands in exactly one partition; partitions may be empty. Each part has
     * {@code docCountError = 0}; the shard's {@code otherDocCount} (normally 0, since shards collect every bucket) is carried
     * by partition 0 only, so the sum over partitions is unchanged.
     * <p>
     * An {@link UnmappedTerms} result has no buckets and is returned as-is for every partition.
     */
    public static List<InternalAggregation> split(InternalAggregation shardResult, int numPartitions) {
        return split(shardResult, numPartitions, numPartitions, IntUnaryOperator.identity());
    }

    /**
     * Like {@link #split(InternalAggregation, int)}, but groups partitions by owner: the key's partition is mapped through
     * {@code groupOfPartition} to one of {@code numGroups} groups (reducers). Used when partitions outnumber reducers (M8).
     */
    public static List<InternalAggregation> split(InternalAggregation shardResult, int numPartitions, int numGroups,
                                                  IntUnaryOperator groupOfPartition) {
        if (numPartitions < 1 || numGroups < 1) {
            throw new IllegalArgumentException("numPartitions and numGroups must be at least 1, got [" + numPartitions + "] and ["
                + numGroups + "]");
        }
        List<InternalAggregation> parts = new ArrayList<>(numGroups);
        if (shardResult instanceof UnmappedTerms) {
            for (int g = 0; g < numGroups; g++) {
                parts.add(shardResult);
            }
        } else if (shardResult instanceof StringTerms) {
            StringTerms terms = (StringTerms) shardResult;
            List<List<StringTerms.Bucket>> groups = groups(numGroups);
            for (StringTerms.Bucket bucket : terms.getBuckets()) {
                groups.get(groupOfPartition.applyAsInt(partition(bucket.termBytes, numPartitions))).add(bucket);
            }
            for (int g = 0; g < numGroups; g++) {
                parts.add(terms.create(terms.getName(), groups.get(g), 0, g == 0 ? terms.getSumOfOtherDocCounts() : 0));
            }
        } else if (shardResult instanceof LongTerms) {
            LongTerms terms = (LongTerms) shardResult;
            List<List<LongTerms.Bucket>> groups = groups(numGroups);
            for (LongTerms.Bucket bucket : terms.getBuckets()) {
                groups.get(groupOfPartition.applyAsInt(partition(bucket.term, numPartitions))).add(bucket);
            }
            for (int g = 0; g < numGroups; g++) {
                parts.add(terms.create(terms.getName(), groups.get(g), 0, g == 0 ? terms.getSumOfOtherDocCounts() : 0));
            }
        } else {
            throw new IllegalArgumentException("[shuffle] does not support aggregation [" + shardResult.getName() + "] of type ["
                + shardResult.getClass().getSimpleName() + "]");
        }
        return parts;
    }

    private static <B> List<List<B>> groups(int numPartitions) {
        List<List<B>> groups = new ArrayList<>(numPartitions);
        for (int p = 0; p < numPartitions; p++) {
            groups.add(new ArrayList<>());
        }
        return groups;
    }

    /** The order of a terms result (a protected field outside this package). */
    public static BucketOrder order(InternalTerms<?, ?> terms) {
        return terms.order;
    }

    /** The requested {@code size} of a terms result. */
    public static int requiredSize(InternalTerms<?, ?> terms) {
        return terms.requiredSize;
    }

    /** The {@code min_doc_count} of a terms result. */
    public static long minDocCount(InternalTerms<?, ?> terms) {
        return terms.minDocCount;
    }

    /** Number of buckets in a terms result (0 for unmapped). */
    public static int bucketCount(InternalAggregation result) {
        return result instanceof InternalTerms ? ((InternalTerms<?, ?>) result).getBuckets().size() : 0;
    }
}
