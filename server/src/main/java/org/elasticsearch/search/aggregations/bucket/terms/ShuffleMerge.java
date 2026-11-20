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

import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * The coordinator merge of a shuffled terms aggregation (docs/shuffle/PROPOSAL.md section 6.9).
 * <p>
 * Every input is one reducer's final, exact result, and the reducers' key sets are disjoint, so taking the first
 * {@code size} buckets of the concatenation under the aggregation's (total) order is exact for any order. This deliberately
 * does NOT call {@code reduce}: parent pipelines already ran on the reducers (code-map.md D5) and must not run twice.
 */
public final class ShuffleMerge {

    private ShuffleMerge() {}

    /**
     * Merges reducer results.
     *
     * @param name         the aggregation name
     * @param order        the aggregation's order, used to sort and as the shape of an empty result
     * @param requiredSize the aggregation's {@code size}
     * @param minDocCount  the aggregation's {@code min_doc_count}, used for an empty result
     * @param metaData     the aggregation's metadata, used for an empty result
     * @param reducerResults one entry per reducer; {@code null} entries (reducers that kept no shard) are ignored
     */
    public static InternalAggregation merge(String name, BucketOrder order, int requiredSize, long minDocCount,
                                            Map<String, Object> metaData, List<InternalAggregation> reducerResults) {
        List<InternalMappedTerms<?, ?>> mapped = new ArrayList<>();
        long otherDocCount = 0;
        for (InternalAggregation result : reducerResults) {
            if (result == null || result instanceof UnmappedTerms) {
                continue;
            }
            if (result instanceof StringTerms == false && result instanceof LongTerms == false) {
                throw new IllegalStateException("[shuffle] cannot merge aggregation [" + name + "] of type ["
                    + result.getClass().getSimpleName() + "]");
            }
            if (mapped.isEmpty() == false && mapped.get(0).getClass() != result.getClass()) {
                throw new IllegalStateException("[shuffle] cannot merge aggregation [" + name + "]: reducers returned ["
                    + mapped.get(0).getClass().getSimpleName() + "] and [" + result.getClass().getSimpleName() + "]");
            }
            InternalMappedTerms<?, ?> terms = (InternalMappedTerms<?, ?>) result;
            if (terms.getName().equals(name) == false) {
                throw new IllegalStateException("[shuffle] expected aggregation [" + name + "] but a reducer returned ["
                    + terms.getName() + "]");
            }
            mapped.add(terms);
            otherDocCount += terms.getSumOfOtherDocCounts();
        }
        if (mapped.isEmpty()) {
            return new UnmappedTerms(name, order, requiredSize, minDocCount, Collections.<PipelineAggregator>emptyList(), metaData);
        }
        return mergeMapped(mapped, order, requiredSize, otherDocCount);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static InternalAggregation mergeMapped(List<InternalMappedTerms<?, ?>> mapped, BucketOrder order, int requiredSize,
                                                   long otherDocCount) {
        List<InternalTerms.Bucket> all = new ArrayList<>();
        for (InternalMappedTerms<?, ?> terms : mapped) {
            all.addAll(terms.getBuckets());
        }
        Comparator comparator = order.comparator(null);
        all.sort(comparator);
        int keep = Math.min(requiredSize, all.size());
        List top = new ArrayList<>(all.subList(0, keep));
        for (int i = keep; i < all.size(); i++) {
            otherDocCount += all.get(i).getDocCount();
        }
        InternalMappedTerms first = mapped.get(0);
        // same type, name, order, size, min_doc_count, format and metadata as the reducers' results; exact by construction
        return first.create(first.getName(), top, 0, otherDocCount);
    }
}
