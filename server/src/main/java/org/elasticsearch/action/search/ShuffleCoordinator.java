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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.shuffle.ShuffleMessages;
import org.elasticsearch.search.shuffle.ShuffleRejectedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinator side of one shuffled search (docs/shuffle/PROPOSAL.md sections 6.2, 6.3 and 6.8): the plan, the per-shard
 * attempt counters, the thread-context headers for each shard request, and finalize / free towards the reducers.
 */
final class ShuffleCoordinator {

    private static final Logger logger = LogManager.getLogger(ShuffleCoordinator.class);

    private final ShufflePlan plan;
    private final String encodedPlan;
    private final DiscoveryNodes nodes;
    private final TransportService transportService;
    private final ThreadContext threadContext;
    /** The context of the original request, before any shuffle header was set (code-map.md D6). */
    private final ThreadContext.StoredContext baseContext;
    private final Map<ShardId, AtomicInteger> attempts = new ConcurrentHashMap<>();
    private final AtomicReference<Exception> rejection = new AtomicReference<>();
    private final AtomicBoolean freed = new AtomicBoolean();

    ShuffleCoordinator(ShufflePlan plan, DiscoveryNodes nodes, TransportService transportService) {
        this.plan = plan;
        this.encodedPlan = plan.encode();
        this.nodes = nodes;
        this.transportService = transportService;
        this.threadContext = transportService.getThreadPool().getThreadContext();
        this.baseContext = threadContext.newStoredContext(false);
    }

    /**
     * Plans a shuffle for this search, or returns {@code null} if it does not use shuffle or does not need it: when at most one
     * shard executes after the can-match phase, the normal path is already exact (section 4.1).
     */
    static ShuffleCoordinator create(SearchRequest request, GroupShardsIterator<SearchShardIterator> shardIterators,
                                     DiscoveryNodes nodes, TransportService transportService, Random random) {
        TermsAggregationBuilder terms = ShuffleRequestValidator.findShuffled(request);
        if (terms == null) {
            return null;
        }
        int executing = 0;
        for (SearchShardIterator iterator : shardIterators) {
            if (iterator.skip() == false) {
                executing++;
            }
        }
        if (executing <= 1) {
            return null;
        }
        List<String> reducers = selectReducers(nodes, terms.shuffle().reducers(), random);
        ShufflePlan plan = ShufflePlan.identity(UUIDs.randomBase64UUID(), terms.getName(), reducers, nodes.getLocalNodeId());
        return new ShuffleCoordinator(plan, nodes, transportService);
    }

    /**
     * Reducer selection v1 (section 5.2): coordinating-only nodes first, then data nodes; each group is rotated by a random
     * offset so that concurrent searches spread over different nodes. Returns at most {@code wanted} node ids.
     */
    static List<String> selectReducers(DiscoveryNodes nodes, int wanted, Random random) {
        List<String> coordinatingOnly = new ArrayList<>();
        List<String> data = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            if (node.isDataNode()) {
                data.add(node.getId());
            } else if (node.isMasterNode() == false && node.isIngestNode() == false) {
                coordinatingOnly.add(node.getId());
            }
        }
        List<String> selected = new ArrayList<>(wanted);
        addRotated(selected, coordinatingOnly, wanted, random);
        addRotated(selected, data, wanted, random);
        if (selected.isEmpty()) {
            throw new IllegalStateException("[shuffle] found no eligible reducer nodes (coordinating-only or data nodes)");
        }
        return selected;
    }

    private static void addRotated(List<String> selected, List<String> candidates, int wanted, Random random) {
        if (candidates.isEmpty() || selected.size() >= wanted) {
            return;
        }
        candidates.sort(Comparator.naturalOrder());
        int offset = random.nextInt(candidates.size());
        for (int i = 0; i < candidates.size() && selected.size() < wanted; i++) {
            selected.add(candidates.get((offset + i) % candidates.size()));
        }
    }

    ShufflePlan plan() {
        return plan;
    }

    /**
     * Puts the plan and this shard's attempt into a fresh copy of the original request context, for the duration of one
     * synchronous send. {@code newAttempt} increments the attempt (initial phase, including retries on another copy); the DFS
     * query phase reuses the attempt that won the DFS phase. Close the returned releasable right after the send.
     */
    Releasable withShardHeaders(ShardId shardId, boolean newAttempt) {
        AtomicInteger counter = attempts.computeIfAbsent(shardId, s -> new AtomicInteger());
        int attempt = newAttempt ? counter.incrementAndGet() : Math.max(1, counter.get());
        ThreadContext.StoredContext previous = threadContext.stashContext();
        baseContext.restore();
        threadContext.putHeader(ShufflePlan.PLAN_HEADER, encodedPlan);
        threadContext.putHeader(ShufflePlan.ATTEMPT_HEADER, Integer.toString(attempt));
        return previous::restore;
    }

    /**
     * Runs the next steps in the original request context without shuffle headers, so that later phases and any requests they
     * send (fetch, expand) are never mistaken for shuffle shard requests.
     */
    Releasable withCleanContext() {
        ThreadContext.StoredContext previous = threadContext.stashContext();
        baseContext.restore();
        return previous::restore;
    }

    /** The attempt currently recorded for a shard; shard retries are sequential, so for a successful shard it is the winner. */
    int attempt(ShardId shardId) {
        AtomicInteger counter = attempts.get(shardId);
        return counter == null ? 1 : Math.max(1, counter.get());
    }

    /** Reducer rejections (breaker, poisoned state) are not retried on another copy (section 6.3). */
    boolean isRetryable(Exception e) {
        return ExceptionsHelper.unwrapCause(e) instanceof ShuffleRejectedException == false;
    }

    void onShardFailure(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (cause instanceof ShuffleRejectedException) {
            rejection.compareAndSet(null, (ShuffleRejectedException) cause);
        }
    }

    /** The first reducer rejection seen by any shard, or {@code null}. */
    Exception rejection() {
        return rejection.get();
    }

    /** One reducer's finalize result, with the coordinator-measured round trip (for the profile API, M7). */
    static final class ReducerResult {
        final String nodeId;
        final ShuffleMessages.FinalizeResponse response;
        final long roundTripNanos;

        ReducerResult(String nodeId, ShuffleMessages.FinalizeResponse response, long roundTripNanos) {
            this.nodeId = nodeId;
            this.response = response;
            this.roundTripNanos = roundTripNanos;
        }
    }

    /** Sends finalize to every reducer; on any failure frees all reducers and fails {@code listener}. */
    void finalizeAll(Map<ShardId, Integer> keepAttempts, Task task, ActionListener<List<ReducerResult>> listener) {
        final List<String> reducers = plan.reducers();
        ActionListener<Collection<ReducerResult>> grouped = ActionListener.wrap(
            responses -> listener.onResponse(new ArrayList<>(responses)),
            e -> {
                freeAll();
                listener.onFailure(e);
            });
        GroupedActionListener<ReducerResult> group = new GroupedActionListener<>(grouped, reducers.size(), Collections.emptyList());
        ShuffleMessages.FinalizeRequest request = new ShuffleMessages.FinalizeRequest(plan.shuffleId(), keepAttempts);
        try (Releasable ignored = withCleanContext()) {
            for (String reducerId : reducers) {
                DiscoveryNode node = nodes.get(reducerId);
                if (node == null) {
                    group.onFailure(new IllegalStateException("[shuffle] reducer [" + reducerId + "] is not in the cluster"));
                    continue;
                }
                final long start = System.nanoTime();
                ActionListener<ShuffleMessages.FinalizeResponse> timed = ActionListener.wrap(
                    response -> group.onResponse(new ReducerResult(reducerId, response, System.nanoTime() - start)),
                    group::onFailure);
                try {
                    transportService.sendChildRequest(transportService.getConnection(node), ShuffleMessages.FINALIZE_ACTION_NAME,
                        request, task, new ActionListenerResponseHandler<>(timed, ShuffleMessages.FinalizeResponse::new));
                } catch (Exception e) {
                    group.onFailure(e);
                }
            }
        }
    }

    /** Frees the shuffle on every reducer, once. Best effort: reducers also free on timeout and when the coordinator leaves. */
    void freeAll() {
        if (freed.compareAndSet(false, true) == false) {
            return;
        }
        ShuffleMessages.FreeRequest request = new ShuffleMessages.FreeRequest(plan.shuffleId());
        try (Releasable ignored = withCleanContext()) {
            for (String reducerId : plan.reducers()) {
                DiscoveryNode node = nodes.get(reducerId);
                if (node == null) {
                    continue;
                }
                try {
                    transportService.sendRequest(node, ShuffleMessages.FREE_ACTION_NAME, request,
                        new ActionListenerResponseHandler<>(ActionListener.wrap(r -> {}, e -> logger.debug(
                            new ParameterizedMessage("failed to free shuffle [{}] on [{}]", plan.shuffleId(), reducerId), e)),
                            () -> TransportResponse.Empty.INSTANCE));
                } catch (Exception e) {
                    logger.debug(new ParameterizedMessage("failed to free shuffle [{}] on [{}]", plan.shuffleId(), reducerId), e);
                }
            }
        }
    }

    /** Frees the reducers whenever the search fails, whatever the phase (planning, shard phase, finalize, fetch, cancellation). */
    ActionListener<SearchResponse> wrapListener(ActionListener<SearchResponse> listener) {
        return new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                freeAll();
                listener.onFailure(e);
            }
        };
    }
}
