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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ShuffleMerge;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.shuffle.InternalShufflePlaceholder;
import org.elasticsearch.search.shuffle.ShuffleMessages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Inserted before the fetch phase of a shuffled search (docs/shuffle/PROPOSAL.md section 6.8): finalizes every reducer with
 * the winning attempt of each successful shard, then runs the next phase over a view of the query results whose
 * {@code reduce()} substitutes the merged result for the {@link InternalShufflePlaceholder}.
 */
final class ShuffleFinalizePhase extends SearchPhase {

    private final SearchPhaseContext context;
    private final ShuffleCoordinator shuffle;
    private final InitialSearchPhase.SearchPhaseResults<SearchPhaseResult> queryResults;
    private final Function<InitialSearchPhase.SearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory;

    ShuffleFinalizePhase(SearchPhaseContext context, ShuffleCoordinator shuffle,
                         InitialSearchPhase.SearchPhaseResults<SearchPhaseResult> queryResults,
                         Function<InitialSearchPhase.SearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory) {
        super("shuffle_finalize");
        this.context = context;
        this.shuffle = shuffle;
        this.queryResults = queryResults;
        this.nextPhaseFactory = nextPhaseFactory;
    }

    @Override
    public void run() {
        // later phases must not inherit a shard request's shuffle headers (see ShuffleCoordinator#withCleanContext)
        try (Releasable ignored = shuffle.withCleanContext()) {
            Exception rejection = shuffle.rejection();
            if (rejection != null) {
                // a reducer rejected data: its state is poisoned, the search cannot be exact (section 6.3)
                shuffle.freeAll();
                context.onPhaseFailure(this, "shuffle rejected by a reducer", rejection);
                return;
            }
            // only shards that succeeded; failed shards (partial results) and can-match skipped shards are left out
            Map<ShardId, Integer> keep = new HashMap<>();
            queryResults.getSuccessfulResults().forEach(r -> {
                ShardId shardId = r.getSearchShardTarget().getShardId();
                keep.put(shardId, shuffle.attempt(shardId));
            });
            shuffle.finalizeAll(keep, context.getTask(), ActionListener.wrap(
                responses -> context.executeNextPhase(this, nextPhaseFactory.apply(
                    new SubstitutingResults(queryResults, shuffle.plan().shuffleId(), responses))),
                e -> context.onPhaseFailure(this, "shuffle finalize failed", e)));
        }
    }

    /**
     * Delegates to the real query results, except that {@link #reduce()} replaces the placeholder with the merge of the
     * reducers' results. Substitution happens after sibling pipelines ran, which v1 allows because no sibling pipeline may
     * reference the shuffled aggregation (section 4.1).
     */
    static final class SubstitutingResults extends InitialSearchPhase.SearchPhaseResults<SearchPhaseResult> {
        private final InitialSearchPhase.SearchPhaseResults<SearchPhaseResult> delegate;
        private final String shuffleId;
        private final List<ShuffleCoordinator.ReducerResult> reducerResults;

        SubstitutingResults(InitialSearchPhase.SearchPhaseResults<SearchPhaseResult> delegate, String shuffleId,
                            List<ShuffleCoordinator.ReducerResult> reducerResults) {
            super(delegate.getNumShards());
            this.delegate = delegate;
            this.shuffleId = shuffleId;
            this.reducerResults = reducerResults;
        }

        @Override
        Stream<SearchPhaseResult> getSuccessfulResults() {
            return delegate.getSuccessfulResults();
        }

        @Override
        void consumeResult(SearchPhaseResult result) {
            delegate.consumeResult(result);
        }

        @Override
        boolean hasResult(int shardIndex) {
            return delegate.hasResult(shardIndex);
        }

        @Override
        void consumeShardFailure(int shardIndex) {
            delegate.consumeShardFailure(shardIndex);
        }

        @Override
        AtomicArray<SearchPhaseResult> getAtomicArray() {
            return delegate.getAtomicArray();
        }

        @Override
        SearchPhaseController.ReducedQueryPhase reduce() {
            SearchPhaseController.ReducedQueryPhase reduced = delegate.reduce();
            return substitute(reduced, shuffleId, reducerResults);
        }
    }

    static SearchPhaseController.ReducedQueryPhase substitute(SearchPhaseController.ReducedQueryPhase reduced, String shuffleId,
                                                              List<ShuffleCoordinator.ReducerResult> results) {
        List<InternalAggregation> reducerResults = new ArrayList<>(results.size());
        for (ShuffleCoordinator.ReducerResult result : results) {
            reducerResults.add(result.response.result());
        }
        if (reduced.aggregations == null) {
            throw new IllegalStateException("[shuffle] the reduced query phase has no aggregations to substitute");
        }
        List<InternalAggregation> aggs = new ArrayList<>();
        boolean found = false;
        for (Aggregation aggregation : reduced.aggregations.asList()) {
            if (aggregation instanceof InternalShufflePlaceholder) {
                if (found) {
                    throw new IllegalStateException("[shuffle] more than one shuffle placeholder in the reduced aggregations");
                }
                found = true;
                InternalShufflePlaceholder placeholder = (InternalShufflePlaceholder) aggregation;
                aggs.add(ShuffleMerge.merge(placeholder.getName(), placeholder.order(), placeholder.requiredSize(),
                    placeholder.minDocCount(), placeholder.getMetaData(), reducerResults));
            } else {
                aggs.add((InternalAggregation) aggregation);
            }
        }
        if (found == false) {
            throw new IllegalStateException("[shuffle] no shuffle placeholder found in the reduced aggregations");
        }
        SearchProfileShardResults profile = reduced.shardResults == null ? null : withShuffleProfile(reduced.shardResults, shuffleId,
            results);
        return new SearchPhaseController.ReducedQueryPhase(reduced.totalHits, reduced.fetchHits, reduced.maxScore, reduced.timedOut,
            reduced.terminatedEarly, reduced.suggest, new InternalAggregations(aggs), profile, reduced.scoreDocs,
            reduced.sortField, reduced.sortValueFormats, reduced.numReducePhases, reduced.isSortedByField, reduced.size,
            reduced.from, reduced.isEmptyResult);
    }

    /** Prefix of the profile "shard" ids that carry one reducer's shuffle stats. */
    static final String PROFILE_ID_PREFIX = "[shuffle][";

    /**
     * Adds one profile entry per reducer (M7), using only existing profile structures so that the response wire format and
     * rendering stay unchanged: the entry id is {@code [shuffle][<reducer node id>]}, and it holds one aggregation profile
     * result of type {@code shuffle_reducer}. Its breakdown has {@code merge} (time the reducer spent decoding and reducing) and
     * {@code transport_and_queue} (the rest of the coordinator-measured finalize round trip), so {@code time_in_nanos} is the
     * finalize round trip. Partials, buckets and bytes received are in the description.
     */
    static SearchProfileShardResults withShuffleProfile(SearchProfileShardResults shardProfiles, String shuffleId,
                                                        List<ShuffleCoordinator.ReducerResult> results) {
        Map<String, ProfileShardResult> merged = new TreeMap<>(shardProfiles.getShardResults());
        for (ShuffleCoordinator.ReducerResult result : results) {
            ShuffleMessages.FinalizeResponse r = result.response;
            Map<String, Long> breakdown = new HashMap<>();
            breakdown.put("merge", r.mergeTimeNanos());
            breakdown.put("transport_and_queue", Math.max(0L, result.roundTripNanos - r.mergeTimeNanos()));
            String description = "reducer [" + result.nodeId + "] shuffle [" + shuffleId + "] partials [" + r.partialsReceived()
                + "] buckets [" + r.bucketsReceived() + "] bytes [" + r.bytesReceived() + "]";
            ProfileResult profile = new ProfileResult("shuffle_reducer", description, breakdown, Collections.emptyList());
            merged.put(PROFILE_ID_PREFIX + result.nodeId + "]", new ProfileShardResult(Collections.emptyList(),
                new AggregationProfileShardResult(Collections.singletonList(profile))));
        }
        return new SearchProfileShardResults(merged);
    }
}
