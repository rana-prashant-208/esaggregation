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
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ShuffleConfig;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.shuffle.ShuffleMessages.PartialRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * M6: failures, disruption and leaks (docs/shuffle/PROPOSAL.md sections 8 and 9). After every test no shuffle state and no
 * request-breaker reservation may remain on any surviving node.
 * <p>
 * Topology: one master-only node, three data-only nodes, and two coordinating-only nodes: {@code coordinator} receives the
 * searches, and {@code reducer} holds no shards, so taking it away affects only the shuffle. Reducer selection prefers
 * coordinating-only nodes, so with 4 reducers both coordinating-only nodes and two data nodes are reducers.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0,
    supportsDedicatedMasters = false, autoMinMasterNodes = true)
public class ShuffleFailureIT extends ESIntegTestCase {

    private static final int SHARDS = 3;

    private String coordinator;
    private String reducer;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    private void startCluster() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(3);
        coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        reducer = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(ShuffleSettings.SHUFFLE_ENABLED_SETTING.getKey(), true)));
        assertAcked(prepareCreate("events").setSettings(Settings.builder().put("index.number_of_shards", SHARDS)
            .put("index.number_of_replicas", 1)).addMapping("_doc", "user", "type=keyword"));
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(200, 800); i++) {
            docs.add(client().prepareIndex("events", "_doc").setSource("user", "u" + randomIntBetween(0, 60)));
        }
        indexRandom(true, docs);
        ensureGreen("events");
    }

    @After
    public void clearRules() {
        for (TransportService ts : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) ts).clearAllRules();
        }
    }

    private static TermsAggregationBuilder shuffled() {
        return AggregationBuilders.terms("top").field("user").size(10).shuffle(new ShuffleConfig(4));
    }

    private SearchRequestBuilder search() {
        return internalCluster().client(coordinator).prepareSearch("events").setSize(0)
            .setSearchType(SearchType.QUERY_THEN_FETCH).addAggregation(shuffled());
    }

    private Terms reference(String preference) {
        SearchRequestBuilder builder = client().prepareSearch("events").setSize(0)
            .addAggregation(AggregationBuilders.terms("top").field("user").size(10).shardSize(10_000));
        if (preference != null) {
            builder.setPreference(preference);
        }
        return builder.get().getAggregations().get("top");
    }

    private static void assertSameTerms(Terms expected, Terms actual) {
        assertThat(actual.getBuckets().size(), equalTo(expected.getBuckets().size()));
        for (int i = 0; i < expected.getBuckets().size(); i++) {
            assertThat(actual.getBuckets().get(i).getKeyAsString(), equalTo(expected.getBuckets().get(i).getKeyAsString()));
            assertThat(actual.getBuckets().get(i).getDocCount(), equalTo(expected.getBuckets().get(i).getDocCount()));
        }
        assertThat(actual.getSumOfOtherDocCounts(), equalTo(expected.getSumOfOtherDocCounts()));
        assertThat(actual.getDocCountError(), equalTo(0L));
    }

    private void addSendBehaviorOnAllNodes(StubbableTransport.SendRequestBehavior behavior) {
        for (TransportService ts : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) ts).addSendBehavior(behavior);
        }
    }

    private MockTransportService transport(String node) {
        return (MockTransportService) internalCluster().getInstance(TransportService.class, node);
    }

    /** No shuffle state and no request-breaker reservation on any (surviving) node. */
    private void assertNoLeaks() throws Exception {
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertThat(node, internalCluster().getInstance(ShuffleService.class, node).activeStates(), equalTo(0));
                assertThat(node, internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST)
                    .getUsed(), equalTo(0L));
            }
        });
    }

    // ------------------------------------------------------------------ shard failures

    /** A shard copy fails after sending some (possibly zero) partials; the retry on the other copy keeps the result exact. */
    public void testShardFailsMidSend() throws Exception {
        startCluster();
        Terms expected = reference(null);
        for (int round = 0; round < 3; round++) {
            final int failingShard = randomIntBetween(0, SHARDS - 1);
            final int passBeforeFailing = randomIntBetween(0, 3);
            AtomicInteger firstAttempt = new AtomicInteger();
            AtomicInteger retries = new AtomicInteger();
            addSendBehaviorOnAllNodes((connection, requestId, action, request, options) -> {
                if (ShuffleMessages.PARTIAL_ACTION_NAME.equals(action) && ((PartialRequest) request).shardId().id() == failingShard) {
                    if (((PartialRequest) request).attempt() == 1 && firstAttempt.incrementAndGet() > passBeforeFailing) {
                        throw new ConnectTransportException(connection.getNode(), "simulated mid-send failure");
                    } else if (((PartialRequest) request).attempt() > 1) {
                        retries.incrementAndGet();
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
            SearchResponse response = search().get();
            clearRules();
            logger.info("round {}: shard {} failed after {} sends, retry sends {}", round, failingShard, passBeforeFailing, retries.get());
            assertThat(response.getFailedShards(), equalTo(0));
            assertSameTerms(expected, response.getAggregations().get("top"));
            assertNoLeaks();
        }
    }

    /** Every copy of a shard fails: with partial results the shard is left out of finalize; without, the search fails. */
    public void testShardFailsOnAllCopies() throws Exception {
        startCluster();
        final int failingShard = randomIntBetween(0, SHARDS - 1);
        addSendBehaviorOnAllNodes((connection, requestId, action, request, options) -> {
            if (ShuffleMessages.PARTIAL_ACTION_NAME.equals(action) && ((PartialRequest) request).shardId().id() == failingShard) {
                throw new ConnectTransportException(connection.getNode(), "simulated permanent failure");
            }
            connection.sendRequest(requestId, action, request, options);
        });

        SearchResponse partial = search().setAllowPartialSearchResults(true).get();
        assertThat(partial.getFailedShards(), equalTo(1));
        List<String> others = new ArrayList<>();
        for (int s = 0; s < SHARDS; s++) {
            if (s != failingShard) {
                others.add(Integer.toString(s));
            }
        }
        clearRules();
        assertSameTerms(reference("_shards:" + String.join(",", others)), partial.getAggregations().get("top"));
        assertNoLeaks();

        addSendBehaviorOnAllNodes((connection, requestId, action, request, options) -> {
            if (ShuffleMessages.PARTIAL_ACTION_NAME.equals(action) && ((PartialRequest) request).shardId().id() == failingShard) {
                throw new ConnectTransportException(connection.getNode(), "simulated permanent failure");
            }
            connection.sendRequest(requestId, action, request, options);
        });
        expectThrows(Exception.class, () -> search().setAllowPartialSearchResults(false).get());
        assertNoLeaks();
    }

    // ------------------------------------------------------------------ reducer failures

    /** The reducer leaves the cluster after the shards sent their data, before finalize: the search fails, nothing leaks. */
    public void testReducerDiesBeforeFinalize() throws Exception {
        startCluster();
        String reducerId = transport(reducer).getLocalNode().getId();
        CountDownLatch intercepted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        transport(coordinator).addSendBehavior((connection, requestId, action, request, options) -> {
            if (ShuffleMessages.FINALIZE_ACTION_NAME.equals(action) && connection.getNode().getId().equals(reducerId)) {
                intercepted.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        ActionFuture<SearchResponse> future = search().execute();
        assertTrue("finalize reached the reducer", intercepted.await(30, TimeUnit.SECONDS));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(reducer));
        release.countDown();

        Exception e = expectThrows(Exception.class, future::actionGet);
        logger.info("search failed as expected: {}", e.toString());
        assertNoLeaks();
    }

    /** The reducer is unreachable while shards send: every shard fails on every copy, the search fails, nothing leaks. */
    public void testReducerUnreachableDuringShardPhase() throws Exception {
        startCluster();
        String reducerId = transport(reducer).getLocalNode().getId();
        addSendBehaviorOnAllNodes((connection, requestId, action, request, options) -> {
            if (ShuffleMessages.PARTIAL_ACTION_NAME.equals(action) && connection.getNode().getId().equals(reducerId)) {
                throw new NodeDisconnectedException(connection.getNode(), action);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        expectThrows(Exception.class, () -> search().get());
        assertThat("the unreachable reducer never got anything",
            internalCluster().getInstance(ShuffleService.class, reducer).partialsAccepted(), equalTo(0L));
        assertNoLeaks();
    }

    // ------------------------------------------------------------------ coordinator failure and cancellation

    /** The coordinator leaves after the shards sent their data: the reducers free its states via the cluster-state listener. */
    public void testCoordinatorDies() throws Exception {
        startCluster();
        CountDownLatch finalizing = new CountDownLatch(1);
        // the coordinator neither finalizes nor frees: only its departure can free the reducers
        transport(coordinator).addSendBehavior((connection, requestId, action, request, options) -> {
            if (ShuffleMessages.FINALIZE_ACTION_NAME.equals(action) || ShuffleMessages.FREE_ACTION_NAME.equals(action)) {
                finalizing.countDown();
                return;
            }
            connection.sendRequest(requestId, action, request, options);
        });
        search().execute();
        assertTrue("all shards sent their data", finalizing.await(30, TimeUnit.SECONDS));
        long statesBefore = 0;
        for (String node : internalCluster().getNodeNames()) {
            if (node.equals(coordinator) == false) {
                statesBefore += internalCluster().getInstance(ShuffleService.class, node).activeStates();
            }
        }
        assertThat("remote reducers hold state", statesBefore, greaterThan(0L));

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(coordinator));
        assertNoLeaks();
        for (String node : internalCluster().getNodeNames()) {
            assertThat("freed by the listener, not by timeout", internalCluster().getInstance(ShuffleService.class, node).timeouts(),
                equalTo(0L));
        }
    }

    /** A search cancelled while shards are sending leaves nothing behind, whatever its outcome. */
    public void testCancelledSearchLeavesNothing() throws Exception {
        startCluster();
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        addSendBehaviorOnAllNodes((connection, requestId, action, request, options) -> {
            if (ShuffleMessages.PARTIAL_ACTION_NAME.equals(action)) {
                blocked.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        ActionFuture<SearchResponse> future = search().execute();
        assertTrue(blocked.await(30, TimeUnit.SECONDS));
        client().admin().cluster().prepareCancelTasks().setActions(SearchAction.NAME).get();
        release.countDown();
        try {
            SearchResponse response = future.actionGet(30, TimeUnit.SECONDS);
            logger.info("cancelled search completed: failed shards [{}]", response.getFailedShards());
        } catch (Exception e) {
            logger.info("cancelled search failed: {}", e.toString());
        }
        assertNoLeaks();
    }

    // ------------------------------------------------------------------ breaker

    /** A reducer's breaker trips: the search fails fast, no shard is retried on another copy, nothing leaks. */
    public void testBreakerTripFailsFastWithoutRetries() throws Exception {
        startCluster();
        // every partial's heap estimate exceeds the request breaker
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
            .put(ShuffleSettings.SHUFFLE_HEAP_ESTIMATE_FACTOR_SETTING.getKey(), 1e12)));
        AtomicInteger retries = new AtomicInteger();
        addSendBehaviorOnAllNodes((connection, requestId, action, request, options) -> {
            if (ShuffleMessages.PARTIAL_ACTION_NAME.equals(action) && ((PartialRequest) request).attempt() > 1) {
                retries.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });
        try {
            Exception e = expectThrows(Exception.class, () -> search().get());
            assertThat(ExceptionsHelper.stackTrace(e), containsString("ShuffleRejectedException"));
            assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertThat("rejections are never retried on another copy", retries.get(), equalTo(0));
            long rejections = 0;
            for (String node : internalCluster().getNodeNames()) {
                rejections += internalCluster().getInstance(ShuffleService.class, node).rejections();
            }
            assertThat(rejections, greaterThanOrEqualTo(1L));
        } finally {
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putNull(ShuffleSettings.SHUFFLE_HEAP_ESTIMATE_FACTOR_SETTING.getKey())));
        }
        assertNoLeaks();
    }
}
