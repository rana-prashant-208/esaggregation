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
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShufflePlan;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * M3: the shard side, driven by plan headers set directly on the client (the coordinator only starts planning in M4).
 * Shards collect every bucket, send one partial to every reducer, and return a placeholder; finalizing on the reducers
 * gives exact per-key totals.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, transportClientRatio = 0)
public class ShuffleShardIT extends ESIntegTestCase {

    private static final int SHARDS = 3;

    private void createEvents() throws Exception {
        assertAcked(prepareCreate("events")
            .setSettings(Settings.builder().put("index.number_of_shards", SHARDS).put("index.number_of_replicas", 0)
                .put("index.requests.cache.enable", true))
            .addMapping("_doc", "user", "type=keyword", "bytes", "type=long", "score", "type=double"));
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int n = randomIntBetween(50, 300);
        for (int i = 0; i < n; i++) {
            docs.add(client().prepareIndex("events", "_doc").setSource("user", "user_" + randomIntBetween(0, 40),
                "bytes", randomIntBetween(0, 1000), "score", randomDouble()));
        }
        indexRandom(true, docs);
        ensureGreen("events");
    }

    private TermsAggregationBuilder topUsers() {
        return AggregationBuilders.terms("top_users").field("user").size(1000).subAggregation(AggregationBuilders.sum("bytes")
            .field("bytes"));
    }

    private Client withPlan(ShufflePlan plan, Integer attempt) {
        Map<String, String> headers = new HashMap<>();
        headers.put(ShufflePlan.PLAN_HEADER, plan.encode());
        if (attempt != null) {
            headers.put(ShufflePlan.ATTEMPT_HEADER, Integer.toString(attempt));
        }
        return client().filterWithHeader(headers);
    }

    private List<DiscoveryNode> nodes() {
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (DiscoveryNode node : clusterService().state().nodes()) {
            nodes.add(node);
        }
        return nodes;
    }

    private Map<ShardId, Integer> allShards(int attempt) {
        String uuid = clusterService().state().metaData().index("events").getIndexUUID();
        Map<ShardId, Integer> keep = new HashMap<>();
        for (int s = 0; s < SHARDS; s++) {
            keep.put(new ShardId("events", uuid, s), attempt);
        }
        return keep;
    }

    private ShuffleMessages.FinalizeResponse finalizeOn(DiscoveryNode reducer, String shuffleId, Map<ShardId, Integer> keep) {
        TransportService transport = internalCluster().getInstance(TransportService.class);
        PlainActionFuture<ShuffleMessages.FinalizeResponse> future = PlainActionFuture.newFuture();
        transport.sendRequest(reducer, ShuffleMessages.FINALIZE_ACTION_NAME, new ShuffleMessages.FinalizeRequest(shuffleId, keep),
            new ActionListenerResponseHandler<>(future, ShuffleMessages.FinalizeResponse::new));
        return future.actionGet();
    }

    private void assertNoShuffleStateAndBreakersReleased() throws Exception {
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertThat(internalCluster().getInstance(ShuffleService.class, node).activeStates(), equalTo(0));
                assertThat(internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST)
                    .getUsed(), equalTo(0L));
            }
        });
    }

    public void testShardsSendEveryBucketAndReducersAreExact() throws Exception {
        createEvents();
        // the reference: a normal terms aggregation that sees every key (single coordinator, large size)
        SearchResponse reference = client().prepareSearch("events").setSize(0).addAggregation(topUsers()).get();
        assertSearchResponse(reference);
        Terms expected = reference.getAggregations().get("top_users");

        List<DiscoveryNode> nodes = nodes();
        List<String> reducerIds = new ArrayList<>();
        for (DiscoveryNode node : randomSubsetOf(randomIntBetween(1, nodes.size()), nodes)) {
            reducerIds.add(node.getId());
        }
        String coordinator = internalCluster().getInstance(TransportService.class).getLocalNode().getId();
        ShufflePlan plan = ShufflePlan.identity("sh-" + randomAlphaOfLength(8), "top_users", reducerIds, coordinator);

        long missesBefore = requestCacheMisses();
        SearchResponse response = withPlan(plan, 1).prepareSearch("events").setSize(0).addAggregation(topUsers())
            .setRequestCache(true).get();
        assertThat(response.getSuccessfulShards(), equalTo(SHARDS));
        assertThat(response.getAggregations().get("top_users"), instanceOf(InternalShufflePlaceholder.class));
        assertThat("the request cache is bypassed with a plan", requestCacheMisses(), equalTo(missesBefore));

        // every reducer holds a state with one partial per shard (empty ones included)
        long bucketsSeen = 0;
        Map<String, Long> counts = new HashMap<>();
        Map<String, Double> sums = new HashMap<>();
        for (String reducerId : reducerIds) {
            DiscoveryNode reducer = clusterService().state().nodes().get(reducerId);
            ShuffleMessages.FinalizeResponse fin = finalizeOn(reducer, plan.shuffleId(), allShards(1));
            assertThat("one partial per shard", fin.partialsReceived(), equalTo(SHARDS));
            StringTerms result = (StringTerms) fin.result();
            assertThat(result.getDocCountError(), equalTo(0L));
            for (StringTerms.Bucket bucket : result.getBuckets()) {
                assertNull("keys are disjoint across reducers", counts.put(bucket.getKeyAsString(), bucket.getDocCount()));
                sums.put(bucket.getKeyAsString(), ((InternalSum) bucket.getAggregations().get("bytes")).getValue());
            }
            bucketsSeen += fin.bucketsReceived();
        }
        assertThat(bucketsSeen, greaterThan(0L));
        assertThat(counts.size(), equalTo(expected.getBuckets().size()));
        for (Terms.Bucket bucket : expected.getBuckets()) {
            assertThat(bucket.getKeyAsString(), counts.get(bucket.getKeyAsString()), equalTo(bucket.getDocCount()));
            assertThat(sums.get(bucket.getKeyAsString()), equalTo(((Sum) bucket.getAggregations().get("bytes")).getValue()));
        }
        assertNoShuffleStateAndBreakersReleased();
    }

    public void testFieldTypeIsCheckedOnTheShards() throws Exception {
        createEvents();
        ShufflePlan plan = ShufflePlan.identity("sh-double", "by_score", nodeIds(),
            internalCluster().getInstance(TransportService.class).getLocalNode().getId());
        Exception e = expectThrows(Exception.class, () -> withPlan(plan, 1).prepareSearch("events").setSize(0)
            .addAggregation(AggregationBuilders.terms("by_score").field("score")).get());
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("only supports keyword, long, integer, short, byte and date fields"));
        assertNoShuffleStateAndBreakersReleased();
    }

    public void testMissingAttemptHeaderFailsTheShards() throws Exception {
        createEvents();
        ShufflePlan plan = ShufflePlan.identity("sh-noattempt", "top_users", nodeIds(),
            internalCluster().getInstance(TransportService.class).getLocalNode().getId());
        Exception e = expectThrows(Exception.class,
            () -> withPlan(plan, null).prepareSearch("events").setSize(0).addAggregation(topUsers()).get());
        assertThat(ExceptionsHelper.stackTrace(e), containsString(ShufflePlan.ATTEMPT_HEADER));
        assertNoShuffleStateAndBreakersReleased();
    }

    public void testWithoutPlanNothingChanges() throws Exception {
        createEvents();
        SearchResponse response = client().prepareSearch("events").setSize(0).addAggregation(topUsers()).get();
        assertSearchResponse(response);
        assertThat(response.getAggregations().get("top_users"), instanceOf(StringTerms.class));
        for (String node : internalCluster().getNodeNames()) {
            assertThat(internalCluster().getInstance(ShuffleService.class, node).activeStates(), equalTo(0));
        }
    }

    private List<String> nodeIds() {
        List<String> ids = new ArrayList<>();
        for (DiscoveryNode node : nodes()) {
            ids.add(node.getId());
        }
        return ids;
    }

    private long requestCacheMisses() {
        IndicesStatsResponse stats = client().admin().indices().prepareStats("events").setRequestCache(true).get();
        return stats.getTotal().getRequestCache().getMissCount();
    }
}
