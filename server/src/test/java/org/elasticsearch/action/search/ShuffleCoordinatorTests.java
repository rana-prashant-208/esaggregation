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

import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.bucket.terms.ShuffleConfig;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.shuffle.ShuffleRejectedException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.nullValue;

public class ShuffleCoordinatorTests extends ESTestCase {

    private ThreadPool threadPool;
    private TransportService transportService;

    @Before
    public void setUpTransport() {
        threadPool = new TestThreadPool(getTestName());
        transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null);
    }

    @After
    public void tearDownTransport() throws Exception {
        transportService.close();
        terminate(threadPool);
    }

    private static DiscoveryNode node(String id, DiscoveryNode.Role... roles) {
        Set<DiscoveryNode.Role> set = roles.length == 0 ? Collections.emptySet() : EnumSet.of(roles[0], roles);
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), Collections.emptyMap(), set, Version.CURRENT);
    }

    private static DiscoveryNodes cluster() {
        return DiscoveryNodes.builder()
            .add(node("master", DiscoveryNode.Role.MASTER))
            .add(node("coord1"))
            .add(node("coord2"))
            .add(node("ingest", DiscoveryNode.Role.INGEST))
            .add(node("data1", DiscoveryNode.Role.DATA))
            .add(node("data2", DiscoveryNode.Role.DATA, DiscoveryNode.Role.MASTER))
            .add(node("data3", DiscoveryNode.Role.DATA, DiscoveryNode.Role.INGEST))
            .localNodeId("coord1").build();
    }

    public void testSelectPrefersCoordinatingOnlyThenData() {
        Random random = random();
        assertThat(ShuffleCoordinator.selectReducers(cluster(), 2, random), containsInAnyOrder("coord1", "coord2"));

        List<String> three = ShuffleCoordinator.selectReducers(cluster(), 3, random);
        assertThat(three.size(), equalTo(3));
        assertThat(three.subList(0, 2), containsInAnyOrder("coord1", "coord2"));
        assertThat(three.get(2), isIn(new HashSet<>(java.util.Arrays.asList("data1", "data2", "data3"))));

        // more than eligible: all eligible nodes, never master-only or ingest-only nodes
        List<String> all = ShuffleCoordinator.selectReducers(cluster(), 16, random);
        assertThat(all, containsInAnyOrder("coord1", "coord2", "data1", "data2", "data3"));
    }

    public void testSelectRotates() {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node("d1", DiscoveryNode.Role.DATA)).add(node("d2", DiscoveryNode.Role.DATA))
            .add(node("d3", DiscoveryNode.Role.DATA)).localNodeId("d1").build();
        Set<String> firsts = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            firsts.add(ShuffleCoordinator.selectReducers(nodes, 1, random()).get(0));
        }
        assertThat("concurrent searches start at different nodes", firsts, containsInAnyOrder("d1", "d2", "d3"));
    }

    public void testSelectNoEligibleNode() {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node("m", DiscoveryNode.Role.MASTER)).localNodeId("m").build();
        expectThrows(IllegalStateException.class, () -> ShuffleCoordinator.selectReducers(nodes, 2, random()));
    }

    private static SearchRequest shuffledRequest(int reducers) {
        return new SearchRequest("events").source(new SearchSourceBuilder().size(0).aggregation(
            new TermsAggregationBuilder("top_users", null).field("user").shuffle(new ShuffleConfig(reducers))));
    }

    private static GroupShardsIterator<SearchShardIterator> shards(int executing, int skipped) {
        List<SearchShardIterator> its = new ArrayList<>();
        for (int i = 0; i < executing + skipped; i++) {
            SearchShardIterator it = new SearchShardIterator(null, new ShardId("events", "uuid", i), Collections.emptyList(),
                OriginalIndices.NONE);
            if (i >= executing) {
                it.resetAndSkip();
            }
            its.add(it);
        }
        return new GroupShardsIterator<>(its);
    }

    public void testCreatePlansWithShuffleAndSeveralShards() {
        ShuffleCoordinator shuffle = ShuffleCoordinator.create(shuffledRequest(2), shards(3, 0), cluster(), transportService,
            random());
        assertNotNull(shuffle);
        ShufflePlan plan = shuffle.plan();
        assertThat(plan.aggName(), equalTo("top_users"));
        assertThat(plan.coordinatorNodeId(), equalTo("coord1"));
        assertThat(plan.reducers().size(), equalTo(2));
        assertThat(plan.numPartitions(), equalTo(2));
    }

    public void testSingleShardFallsBack() {
        assertThat(ShuffleCoordinator.create(shuffledRequest(2), shards(1, 0), cluster(), transportService, random()), nullValue());
        // several shards, but can-match skipped all but one
        assertThat(ShuffleCoordinator.create(shuffledRequest(2), shards(1, randomIntBetween(1, 5)), cluster(), transportService,
            random()), nullValue());
        assertNotNull(ShuffleCoordinator.create(shuffledRequest(2), shards(2, randomIntBetween(1, 5)), cluster(), transportService,
            random()));
    }

    public void testNoShuffleNoPlan() {
        SearchRequest plain = new SearchRequest("events").source(new SearchSourceBuilder().aggregation(
            new TermsAggregationBuilder("top_users", null).field("user")));
        assertThat(ShuffleCoordinator.create(plain, shards(5, 0), cluster(), transportService, random()), nullValue());
    }

    public void testAttemptsAndHeaders() {
        ThreadContext threadContext = threadPool.getThreadContext();
        threadContext.putHeader("user-header", "kept");
        ShuffleCoordinator shuffle = ShuffleCoordinator.create(shuffledRequest(2), shards(3, 0), cluster(), transportService, random());
        ShardId s0 = new ShardId("events", "uuid", 0);
        ShardId s1 = new ShardId("events", "uuid", 1);
        assertThat(shuffle.attempt(s0), equalTo(1));

        try (Releasable ignored = shuffle.withShardHeaders(s0, true)) {
            assertThat(ShufflePlan.fromThreadContext(threadContext), equalTo(shuffle.plan()));
            assertThat(ShufflePlan.attemptFromThreadContext(threadContext), equalTo(1));
            assertThat(threadContext.getHeader("user-header"), equalTo("kept"));

            // a retry runs inside the failed send's restored context, which already has the headers (code-map.md D6)
            try (Releasable retry = shuffle.withShardHeaders(s0, true)) {
                assertThat(ShufflePlan.attemptFromThreadContext(threadContext), equalTo(2));
                try (Releasable other = shuffle.withShardHeaders(s1, true)) {
                    assertThat(ShufflePlan.attemptFromThreadContext(threadContext), equalTo(1));
                }
                try (Releasable clean = shuffle.withCleanContext()) {
                    assertThat(threadContext.getHeader(ShufflePlan.PLAN_HEADER), nullValue());
                    assertThat(threadContext.getHeader("user-header"), equalTo("kept"));
                }
            }
            // DFS query phase: reuse the attempt, do not increment
            try (Releasable dfs = shuffle.withShardHeaders(s0, false)) {
                assertThat(ShufflePlan.attemptFromThreadContext(threadContext), equalTo(2));
            }
        }
        assertThat("headers are restored after the send", threadContext.getHeader(ShufflePlan.PLAN_HEADER), nullValue());
        assertThat(shuffle.attempt(s0), equalTo(2));
        assertThat(shuffle.attempt(s1), equalTo(1));
    }

    public void testRejectionIsNotRetryableAndRecorded() {
        ShuffleCoordinator shuffle = ShuffleCoordinator.create(shuffledRequest(2), shards(3, 0), cluster(), transportService, random());
        ShuffleRejectedException rejected = new ShuffleRejectedException("breaker", null);
        Exception wrapped = new RemoteTransportException("remote", rejected);
        assertFalse(shuffle.isRetryable(wrapped));
        assertTrue(shuffle.isRetryable(new IllegalStateException("node left")));
        assertThat(shuffle.rejection(), nullValue());
        shuffle.onShardFailure(new IllegalStateException("other"));
        assertThat(shuffle.rejection(), nullValue());
        shuffle.onShardFailure(wrapped);
        assertThat(shuffle.rejection(), equalTo(rejected));
        assertThat(Collections.singletonList(shuffle.rejection()), everyItem(equalTo(rejected)));
    }
}
