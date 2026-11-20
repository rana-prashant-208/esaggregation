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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.search.ShufflePlan;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ShufflePartitioner;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shard side of the shuffle (docs/shuffle/PROPOSAL.md section 6.4, steps 3-6): splits the shuffled terms result by
 * partition, sends one partial (possibly empty) to every reducer, waits asynchronously for every acknowledgement, and then
 * replaces the aggregation in the query result with an {@link InternalShufflePlaceholder}.
 */
public class ShardShuffleSender {

    private final TransportService transportService;
    private final ClusterService clusterService;

    public ShardShuffleSender(TransportService transportService, ClusterService clusterService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
    }

    /**
     * Sends {@code result}'s shuffled aggregation to the reducers and substitutes the placeholder. {@code listener} is completed
     * after every reducer acknowledged, or with the first failure. A {@link ShuffleRejectedException} is passed on unwrapped so
     * that the coordinator can recognize it and skip retries.
     */
    public void send(ShufflePlan plan, int attempt, ShardId shardId, QuerySearchResult result, Task task,
                     ActionListener<Void> listener) {
        final List<SendTarget> targets;
        try {
            targets = prepare(plan, attempt, shardId, result, clusterService.state().nodes());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        final CountDown countDown = new CountDown(targets.size());
        final AtomicBoolean failed = new AtomicBoolean();
        for (SendTarget target : targets) {
            ActionListener<TransportResponse.Empty> ack = new ActionListener<TransportResponse.Empty>() {
                @Override
                public void onResponse(TransportResponse.Empty empty) {
                    if (countDown.countDown() && failed.get() == false) {
                        listener.onResponse(null);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (failed.compareAndSet(false, true)) {
                        listener.onFailure(unwrapRejection(e));
                    }
                    countDown.countDown();
                }
            };
            try {
                ActionListenerResponseHandler<TransportResponse.Empty> handler =
                    new ActionListenerResponseHandler<>(ack, () -> TransportResponse.Empty.INSTANCE);
                if (task == null) {
                    transportService.sendRequest(target.node, ShuffleMessages.PARTIAL_ACTION_NAME, target.request, handler);
                } else {
                    transportService.sendChildRequest(transportService.getConnection(target.node), ShuffleMessages.PARTIAL_ACTION_NAME,
                        target.request, task, handler);
                }
            } catch (Exception e) {
                ack.onFailure(e);
            }
        }
    }

    /**
     * Splits the aggregation and swaps in the placeholder. Visible for tests: everything except the actual sends.
     */
    static List<SendTarget> prepare(ShufflePlan plan, int attempt, ShardId shardId, QuerySearchResult result, DiscoveryNodes nodes) {
        if (result.hasAggs() == false) {
            throw new IllegalStateException("shard " + shardId + " has no aggregations to shuffle for [" + plan.aggName() + "]");
        }
        InternalAggregations aggs = (InternalAggregations) result.consumeAggs();
        List<InternalAggregation> replaced = new ArrayList<>();
        InternalAggregation shuffled = null;
        for (org.elasticsearch.search.aggregations.Aggregation a : aggs.asList()) {
            InternalAggregation agg = (InternalAggregation) a;
            if (agg.getName().equals(plan.aggName())) {
                shuffled = agg;
            } else {
                replaced.add(agg);
            }
        }
        if (shuffled == null) {
            result.aggregations(aggs);
            throw new IllegalStateException("shard " + shardId + " has no aggregation [" + plan.aggName() + "] to shuffle");
        }
        final int numReducers = plan.reducers().size();
        List<InternalAggregation> parts = ShufflePartitioner.split(shuffled, plan.numPartitions(), numReducers,
            plan::reducerForPartition);
        List<SendTarget> targets = new ArrayList<>(numReducers);
        for (int r = 0; r < numReducers; r++) {
            String nodeId = plan.reducers().get(r);
            DiscoveryNode node = nodes.get(nodeId);
            if (node == null) {
                throw new IllegalStateException("shuffle [" + plan.shuffleId() + "] reducer [" + nodeId + "] is not in the cluster");
            }
            InternalAggregation part = parts.get(r);
            targets.add(new SendTarget(node, new ShuffleMessages.PartialRequest(plan.shuffleId(), plan.aggName(), shardId, attempt, r,
                plan.coordinatorNodeId(), ShufflePartitioner.bucketCount(part), ShuffleMessages.serializePartial(part))));
        }
        replaced.add(placeholder(plan.shuffleId(), shuffled));
        result.aggregations(new InternalAggregations(replaced));
        return targets;
    }

    static InternalShufflePlaceholder placeholder(String shuffleId, InternalAggregation terms) {
        if (terms instanceof InternalTerms == false) {
            throw new IllegalArgumentException("[shuffle] does not support aggregation [" + terms.getName() + "] of type ["
                + terms.getClass().getSimpleName() + "]");
        }
        InternalTerms<?, ?> t = (InternalTerms<?, ?>) terms;
        return new InternalShufflePlaceholder(t.getName(), shuffleId, ShufflePartitioner.order(t), ShufflePartitioner.requiredSize(t),
            ShufflePartitioner.minDocCount(t), t.getMetaData());
    }

    private static Exception unwrapRejection(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        return cause instanceof ShuffleRejectedException ? (ShuffleRejectedException) cause : e;
    }

    static final class SendTarget {
        final DiscoveryNode node;
        final ShuffleMessages.PartialRequest request;

        SendTarget(DiscoveryNode node, ShuffleMessages.PartialRequest request) {
            this.node = node;
            this.request = request;
        }
    }
}
