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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.shuffle.ShuffleMessages.FinalizeRequest;
import org.elasticsearch.search.shuffle.ShuffleMessages.FinalizeResponse;
import org.elasticsearch.search.shuffle.ShuffleMessages.FreeRequest;
import org.elasticsearch.search.shuffle.ShuffleMessages.PartialRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * M2: the shuffle transport actions work between real nodes (remote and local delivery), and rejections keep their type.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, transportClientRatio = 0)
public class ShuffleTransportIT extends ESIntegTestCase {

    private static StringTerms partial(String key, long count) {
        StringTerms.Bucket bucket = new StringTerms.Bucket(new BytesRef(key), count, InternalAggregations.EMPTY, false, 0,
            DocValueFormat.RAW);
        return new StringTerms("agg", BucketOrder.compound(BucketOrder.count(false)), 10, 1, Collections.emptyList(), null,
            DocValueFormat.RAW, Integer.MAX_VALUE, false, 0, Collections.singletonList(bucket), 0);
    }

    private static <R extends TransportResponse> R send(TransportService from, DiscoveryNode to, String action,
                                                        TransportRequest request, Supplier<R> response) {
        PlainActionFuture<R> future = PlainActionFuture.newFuture();
        from.sendRequest(to, action, request, new ActionListenerResponseHandler<>(future, response));
        return future.actionGet();
    }

    public void testPartialFinalizeFree() {
        String[] nodes = internalCluster().getNodeNames();
        TransportService sender = internalCluster().getInstance(TransportService.class, nodes[0]);
        String reducerName = randomFrom(nodes); // remote or local delivery
        DiscoveryNode reducer = internalCluster().getInstance(TransportService.class, reducerName).getLocalNode();
        ShuffleService reducerService = internalCluster().getInstance(ShuffleService.class, reducerName);

        ShardId s0 = new ShardId("events", "_na_", 0);
        ShardId s1 = new ShardId("events", "_na_", 1);
        String coordinator = sender.getLocalNode().getId();
        send(sender, reducer, ShuffleMessages.PARTIAL_ACTION_NAME,
            new PartialRequest("sh", "agg", s0, 1, 0, coordinator, 1, ShuffleMessages.serializePartial(partial("alice", 40))),
            () -> TransportResponse.Empty.INSTANCE);
        send(sender, reducer, ShuffleMessages.PARTIAL_ACTION_NAME,
            new PartialRequest("sh", "agg", s1, 1, 0, coordinator, 1, ShuffleMessages.serializePartial(partial("alice", 10))),
            () -> TransportResponse.Empty.INSTANCE);
        assertThat(reducerService.activeStates(), equalTo(1));

        Map<ShardId, Integer> keep = new HashMap<>();
        keep.put(s0, 1);
        keep.put(s1, 1);
        FinalizeResponse response = send(sender, reducer, ShuffleMessages.FINALIZE_ACTION_NAME, new FinalizeRequest("sh", keep),
            FinalizeResponse::new);
        assertThat(((StringTerms) response.result()).getBucketByKey("alice").getDocCount(), equalTo(50L));
        assertThat(reducerService.activeStates(), equalTo(0));

        // free is idempotent over the wire too
        send(sender, reducer, ShuffleMessages.FREE_ACTION_NAME, new FreeRequest("sh"), () -> TransportResponse.Empty.INSTANCE);
        send(sender, reducer, ShuffleMessages.FREE_ACTION_NAME, new FreeRequest("sh"), () -> TransportResponse.Empty.INSTANCE);

        // a late partial is rejected, and the rejection keeps its type across the wire
        Exception e = expectThrows(Exception.class, () -> send(sender, reducer, ShuffleMessages.PARTIAL_ACTION_NAME,
            new PartialRequest("sh", "agg", s0, 2, 0, coordinator, 1, ShuffleMessages.serializePartial(partial("alice", 1))),
            () -> TransportResponse.Empty.INSTANCE));
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(ShuffleRejectedException.class));

        for (String node : nodes) {
            assertThat(internalCluster().getInstance(ShuffleService.class, node).activeStates(), equalTo(0));
            assertThat(internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST).getUsed(),
                equalTo(0L));
        }
    }
}
