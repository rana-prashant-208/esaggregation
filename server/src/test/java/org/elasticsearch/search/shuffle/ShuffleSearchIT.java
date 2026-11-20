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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ShuffleConfig;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * M4: end-to-end shuffled searches through the coordinator. Every shuffled response must equal a normal terms aggregation
 * whose shard_size covers every key, and leave no reducer state or breaker reservation behind.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, transportClientRatio = 0)
public class ShuffleSearchIT extends ESIntegTestCase {

    private static final int KEYS = 150;

    @Before
    public void enableShuffle() {
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(ShuffleSettings.SHUFFLE_ENABLED_SETTING.getKey(), true)));
    }

    private int createEvents(String index, int shards) throws Exception {
        assertAcked(prepareCreate(index)
            .setSettings(Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", randomIntBetween(0, 1)))
            .addMapping("_doc", "user", "type=keyword", "uid", "type=long", "bytes", "type=long", "ts", "type=date"));
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int n = randomIntBetween(300, 1500);
        for (int i = 0; i < n; i++) {
            // skewed: a few heavy keys and a long tail, so stock top-k with small shard_size would be wrong
            int key = randomBoolean() ? randomIntBetween(0, 10) : randomIntBetween(0, KEYS);
            docs.add(client().prepareIndex(index, "_doc").setSource("user", "user_" + key, "uid", key, "bytes", randomIntBetween(0, 100),
                "ts", "2019-01-0" + randomIntBetween(1, 9)));
        }
        indexRandom(true, docs);
        ensureGreen(index);
        return n;
    }

    private static TermsAggregationBuilder terms(String field, int size, BucketOrder order, long minDocCount) {
        return AggregationBuilders.terms("top").field(field).size(size).order(order).minDocCount(minDocCount)
            .subAggregation(AggregationBuilders.sum("bytes").field("bytes"));
    }

    private static BucketOrder randomOrder() {
        switch (randomIntBetween(0, 5)) {
            case 0: return BucketOrder.count(false);
            case 1: return BucketOrder.count(true);
            case 2: return BucketOrder.key(true);
            case 3: return BucketOrder.key(false);
            case 4: return BucketOrder.aggregation("bytes", false);
            default: return BucketOrder.aggregation("bytes", true);
        }
    }

    private void assertSameTerms(Terms expected, Terms actual) {
        assertThat(Strings.toString(actual), actual.getBuckets().size(), equalTo(expected.getBuckets().size()));
        for (int i = 0; i < expected.getBuckets().size(); i++) {
            Terms.Bucket e = expected.getBuckets().get(i);
            Terms.Bucket a = actual.getBuckets().get(i);
            assertThat("bucket " + i, a.getKeyAsString(), equalTo(e.getKeyAsString()));
            assertThat(a.getKeyAsString(), a.getDocCount(), equalTo(e.getDocCount()));
            assertThat(((Sum) a.getAggregations().get("bytes")).getValue(), equalTo(((Sum) e.getAggregations().get("bytes")).getValue()));
        }
        assertThat(actual.getSumOfOtherDocCounts(), equalTo(expected.getSumOfOtherDocCounts()));
        assertThat(actual.getDocCountError(), equalTo(0L));
    }

    private void assertNoLeaks() throws Exception {
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertThat(node, internalCluster().getInstance(ShuffleService.class, node).activeStates(), equalTo(0));
                assertThat(node, internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST)
                    .getUsed(), equalTo(0L));
            }
        });
    }

    private long partialsAccepted() {
        long total = 0;
        for (String node : internalCluster().getNodeNames()) {
            total += internalCluster().getInstance(ShuffleService.class, node).partialsAccepted();
        }
        return total;
    }

    public void testEquivalentToExactTerms() throws Exception {
        int shards = randomIntBetween(2, 6);
        createEvents("events", shards);
        if (randomBoolean()) {
            internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        }
        for (int round = 0; round < 10; round++) {
            String field = randomBoolean() ? "user" : "uid";
            int size = randomIntBetween(1, 30);
            BucketOrder order = randomOrder();
            long minDocCount = randomIntBetween(1, 4);
            SearchType searchType = randomFrom(SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH);

            SearchResponse reference = client().prepareSearch("events").setSize(0).setSearchType(searchType)
                .addAggregation(terms(field, size, order, minDocCount).shardSize(10_000)).get();
            assertSearchResponse(reference);

            long before = partialsAccepted();
            int reducers = randomIntBetween(1, 5);
            SearchResponse shuffled = client().prepareSearch("events").setSize(0).setSearchType(searchType)
                .addAggregation(terms(field, size, order, minDocCount).shuffle(new ShuffleConfig(reducers))).get();
            assertSearchResponse(shuffled);
            assertThat("the search really shuffled", partialsAccepted() - before, greaterThan(0L));

            String context = "round " + round + " field " + field + " size " + size + " order " + order + " min_doc_count "
                + minDocCount + " reducers " + reducers + " " + searchType;
            logger.info("checking {}", context);
            assertSameTerms(reference.getAggregations().get("top"), shuffled.getAggregations().get("top"));
            // the shuffled response renders like a normal terms response
            assertThat(Strings.toString(shuffled).contains("doc_count_error_upper_bound\":0"), equalTo(true));
            assertNoLeaks();
        }
    }

    public void testSingleShardFallsBackToNormalPath() throws Exception {
        createEvents("single", 1);
        long before = partialsAccepted();
        SearchResponse reference = client().prepareSearch("single").setSize(0)
            .addAggregation(terms("user", 5, BucketOrder.count(false), 1)).get();
        SearchResponse shuffled = client().prepareSearch("single").setSize(0)
            .addAggregation(terms("user", 5, BucketOrder.count(false), 1).shuffle(new ShuffleConfig(3))).get();
        assertSearchResponse(shuffled);
        assertThat("one shard: no shuffle", partialsAccepted(), equalTo(before));
        assertSameTerms(reference.getAggregations().get("top"), shuffled.getAggregations().get("top"));
        assertNoLeaks();
    }

    public void testCanMatchSkippedShardsAreExcluded() throws Exception {
        assertAcked(prepareCreate("logs-2018").setSettings(Settings.builder().put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)).addMapping("_doc", "user", "type=keyword", "bytes", "type=long", "ts", "type=date"));
        createEvents("logs-2019", 2);
        List<IndexRequestBuilder> old = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            old.add(client().prepareIndex("logs-2018", "_doc").setSource("user", "user_" + i, "bytes", 1, "ts", "2018-06-01"));
        }
        indexRandom(true, old);

        long before = partialsAccepted();
        // can-match only runs for query_then_fetch (the test client otherwise picks a random search type)
        SearchResponse shuffled = client().prepareSearch("logs-*").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
            .setPreFilterShardSize(1)
            .setQuery(QueryBuilders.rangeQuery("ts").gte("2019-01-01"))
            .addAggregation(terms("user", 10, BucketOrder.count(false), 1).shuffle(new ShuffleConfig(2))).get();
        assertSearchResponse(shuffled);
        assertThat("the 2018 shards were skipped by can-match", shuffled.getSkippedShards(), equalTo(2));
        // 2 executing shards x 2 reducers; the skipped shards never sent anything
        assertThat(partialsAccepted() - before, equalTo(4L));

        SearchResponse reference = client().prepareSearch("logs-*").setSize(0)
            .setQuery(QueryBuilders.rangeQuery("ts").gte("2019-01-01"))
            .addAggregation(terms("user", 10, BucketOrder.count(false), 1).shardSize(10_000)).get();
        assertSameTerms(reference.getAggregations().get("top"), shuffled.getAggregations().get("top"));
        assertNoLeaks();
    }

    public void testOtherAggregationsAreUntouched() throws Exception {
        createEvents("events", 3);
        SearchResponse shuffled = client().prepareSearch("events").setSize(0)
            .addAggregation(terms("user", 5, BucketOrder.count(false), 1).shuffle(new ShuffleConfig(2)))
            .addAggregation(AggregationBuilders.sum("total_bytes").field("bytes"))
            .addAggregation(AggregationBuilders.terms("plain").field("user").size(3)).get();
        assertSearchResponse(shuffled);
        SearchResponse reference = client().prepareSearch("events").setSize(0)
            .addAggregation(AggregationBuilders.sum("total_bytes").field("bytes"))
            .addAggregation(AggregationBuilders.terms("plain").field("user").size(3)).get();
        assertThat(((Sum) shuffled.getAggregations().get("total_bytes")).getValue(),
            equalTo(((Sum) reference.getAggregations().get("total_bytes")).getValue()));
        assertThat(Strings.toString((Terms) shuffled.getAggregations().get("plain")),
            equalTo(Strings.toString((Terms) reference.getAggregations().get("plain"))));
        assertNoLeaks();
    }

    public void testHitsAreFetchedNormally() throws Exception {
        int docs = createEvents("events", 3);
        SearchResponse shuffled = client().prepareSearch("events").setSize(5)
            .addAggregation(terms("user", 5, BucketOrder.count(false), 1).shuffle(new ShuffleConfig(2))).get();
        assertSearchResponse(shuffled);
        assertThat(shuffled.getHits().getHits().length, equalTo(5));
        assertThat(shuffled.getHits().getTotalHits(), equalTo((long) docs));
        assertNoLeaks();
    }
}
