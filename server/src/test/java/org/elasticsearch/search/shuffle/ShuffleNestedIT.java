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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
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
 * Nested shuffle, option A (code-map.md D36): a shuffled top-level terms with terms sub-aggregations. Each reducer owns a
 * top-level key with its whole subtree, and shards send every bucket at every level, so every level must equal a stock
 * terms tree whose shard_size covers every key at every level.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, transportClientRatio = 0)
public class ShuffleNestedIT extends ESIntegTestCase {

    private static final String[] FIELDS = {"l1", "l2", "l3"};

    @Before
    public void enableShuffle() {
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(ShuffleSettings.SHUFFLE_ENABLED_SETTING.getKey(), true)));
    }

    private void index(int perLevel) throws Exception {
        for (int i = 1; i <= 3; i++) {
            assertAcked(prepareCreate("lv-" + i).setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 2))
                .put("index.number_of_replicas", randomIntBetween(0, 1)))
                .addMapping("_doc", "l1", "type=keyword", "l2", "type=keyword", "l3", "type=keyword", "bytes", "type=long"));
        }
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int n = randomIntBetween(1000, 4000);
        for (int d = 0; d < n; d++) {
            // skewed at every level, so that stock trees with a small shard_size would be wrong
            int a = (int) (Math.pow(randomDouble(), 2) * perLevel);
            int b = (int) (Math.pow(randomDouble(), 2) * perLevel);
            int c = (int) (Math.pow(randomDouble(), 2) * perLevel);
            docs.add(client().prepareIndex("lv-" + randomIntBetween(1, 3), "_doc")
                .setSource("l1", "a" + a, "l2", "b" + b, "l3", "c" + c, "bytes", randomIntBetween(0, 100)));
        }
        indexRandom(true, docs);
        ensureGreen("lv-*");
    }

    /** level 0..2; a positive exactShardSize builds the stock reference, otherwise the top level shuffles. */
    private static TermsAggregationBuilder tree(int level, int[] sizes, BucketOrder[] orders, long[] minDocCounts,
                                                int exactShardSize, int reducers) {
        TermsAggregationBuilder t = AggregationBuilders.terms(FIELDS[level]).field(FIELDS[level]).size(sizes[level])
            .order(orders[level]).minDocCount(minDocCounts[level]);
        t.subAggregation(AggregationBuilders.sum("bytes").field("bytes"));
        if (exactShardSize > 0) {
            t.shardSize(exactShardSize);
        } else if (level == 0) {
            t.shuffle(new ShuffleConfig(reducers));
        }
        if (level < 2) {
            t.subAggregation(tree(level + 1, sizes, orders, minDocCounts, exactShardSize, reducers));
        }
        return t;
    }

    private static void assertSameTree(String path, Terms expected, Terms actual, int level) {
        assertThat(path + " buckets", actual.getBuckets().size(), equalTo(expected.getBuckets().size()));
        assertThat(path + " sum_other_doc_count", actual.getSumOfOtherDocCounts(), equalTo(expected.getSumOfOtherDocCounts()));
        assertThat(path + " doc_count_error_upper_bound", actual.getDocCountError(), equalTo(0L));
        for (int i = 0; i < expected.getBuckets().size(); i++) {
            Terms.Bucket e = expected.getBuckets().get(i);
            Terms.Bucket a = actual.getBuckets().get(i);
            String p = path + "/" + e.getKeyAsString();
            assertThat(p + " key at " + i, a.getKeyAsString(), equalTo(e.getKeyAsString()));
            assertThat(p + " doc_count", a.getDocCount(), equalTo(e.getDocCount()));
            assertThat(p + " bytes", ((Sum) a.getAggregations().get("bytes")).getValue(),
                equalTo(((Sum) e.getAggregations().get("bytes")).getValue()));
            if (level < 2) {
                assertSameTree(p, e.getAggregations().get(FIELDS[level + 1]), a.getAggregations().get(FIELDS[level + 1]), level + 1);
            }
        }
    }

    private static BucketOrder randomOrder() {
        switch (randomIntBetween(0, 3)) {
            case 0: return BucketOrder.count(false);
            case 1: return BucketOrder.count(true);
            case 2: return BucketOrder.key(true);
            default: return BucketOrder.aggregation("bytes", false);
        }
    }

    private long partialsAccepted() {
        long total = 0;
        for (String node : internalCluster().getNodeNames()) {
            total += internalCluster().getInstance(ShuffleService.class, node).partialsAccepted();
        }
        return total;
    }

    public void testThreeLevelsExactAtEveryLevel() throws Exception {
        int perLevel = randomFrom(5, 20, 60);
        index(perLevel);
        for (int round = 0; round < 6; round++) {
            int[] sizes = {randomIntBetween(1, 15), randomIntBetween(1, 15), randomIntBetween(1, 15)};
            BucketOrder[] orders = {randomOrder(), randomOrder(), randomOrder()};
            long[] minDocCounts = {randomIntBetween(1, 2), randomIntBetween(1, 2), randomIntBetween(1, 2)};
            int reducers = randomIntBetween(1, 4);
            SearchType searchType = randomFrom(SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH);
            logger.info("round {}: per level {} sizes {}/{}/{} orders {}/{}/{} reducers {} {}", round, perLevel, sizes[0], sizes[1],
                sizes[2], orders[0], orders[1], orders[2], reducers, searchType);

            SearchResponse reference = client().prepareSearch("lv-*").setSize(0).setSearchType(searchType)
                .addAggregation(tree(0, sizes, orders, minDocCounts, 100_000, 0)).get();
            long before = partialsAccepted();
            SearchResponse shuffled = client().prepareSearch("lv-*").setSize(0).setSearchType(searchType)
                .addAggregation(tree(0, sizes, orders, minDocCounts, 0, reducers)).get();
            assertSearchResponse(reference);
            assertSearchResponse(shuffled);
            if (shuffled.getTotalShards() - shuffled.getSkippedShards() > 1) {
                assertThat("the search really shuffled", partialsAccepted() - before, greaterThan(0L));
            }
            assertSameTree("round " + round, reference.getAggregations().get("l1"), shuffled.getAggregations().get("l1"), 0);
        }
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertThat(internalCluster().getInstance(ShuffleService.class, node).activeStates(), equalTo(0));
                assertThat(internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST)
                    .getUsed(), equalTo(0L));
            }
        });
    }
}
