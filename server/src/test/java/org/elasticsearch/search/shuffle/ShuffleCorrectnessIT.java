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

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ShuffleConfig;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.SimpleValue;
import org.elasticsearch.search.shuffle.ShuffleMessages.PartialRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * M5: correctness of shuffled terms aggregations (docs/shuffle/PROPOSAL.md sections 9 and 10).
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, transportClientRatio = 0)
public class ShuffleCorrectnessIT extends ESIntegTestCase {

    /** Section 10: user to per-shard counts S0..S4. Totals: 660 docs; top 3 by count are carol 150, bob 95, alice 90. */
    private static final Map<String, int[]> CANONICAL = new LinkedHashMap<>();
    static {
        CANONICAL.put("alice", new int[] {40, 10, 0, 35, 5});
        CANONICAL.put("bob", new int[] {0, 50, 20, 0, 25});
        CANONICAL.put("carol", new int[] {30, 30, 30, 30, 30});
        CANONICAL.put("dave", new int[] {20, 0, 0, 60, 0});
        CANONICAL.put("erin", new int[] {10, 15, 10, 10, 40});
        CANONICAL.put("frank", new int[] {5, 5, 5, 5, 5});
        CANONICAL.put("grace", new int[] {0, 0, 70, 0, 0});
        CANONICAL.put("heidi", new int[] {15, 0, 10, 0, 0});
        CANONICAL.put("ivan", new int[] {0, 20, 0, 5, 0});
        CANONICAL.put("judy", new int[] {0, 0, 5, 0, 10});
    }

    public static class DoublingScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("double_it", vars -> ((Number) vars.get("_value0")).doubleValue() * 2);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, DoublingScriptPlugin.class);
    }

    @Before
    public void enableShuffle() {
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(ShuffleSettings.SHUFFLE_ENABLED_SETTING.getKey(), true)));
    }

    // ---------------------------------------------------------------- canonical fixture (section 10)

    /** Indexes the section 10 table with explicit routing so that each shard holds exactly its column. */
    private void indexCanonical(int replicas) {
        assertAcked(prepareCreate("canon").setSettings(Settings.builder().put("index.number_of_shards", 5)
            .put("index.number_of_replicas", replicas)).addMapping("_doc", "user", "type=keyword"));
        ensureGreen("canon");
        IndexMetaData meta = clusterService().state().metaData().index("canon");
        String[] routingForShard = new String[5];
        for (int i = 0; Arrays.asList(routingForShard).contains(null); i++) {
            String routing = "r" + i;
            int shard = OperationRouting.generateShardId(meta, null, routing);
            if (routingForShard[shard] == null) {
                routingForShard[shard] = routing;
            }
        }
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Map.Entry<String, int[]> e : CANONICAL.entrySet()) {
            for (int shard = 0; shard < 5; shard++) {
                for (int d = 0; d < e.getValue()[shard]; d++) {
                    bulk.add(client().prepareIndex("canon", "_doc").setRouting(routingForShard[shard]).setSource("user", e.getKey()));
                }
            }
        }
        assertThat(bulk.get().hasFailures(), equalTo(false));
        // the fixture really is the table: per-shard doc counts
        for (int shard = 0; shard < 5; shard++) {
            long expected = 0;
            for (int[] counts : CANONICAL.values()) {
                expected += counts[shard];
            }
            SearchResponse perShard = client().prepareSearch("canon").setSize(0).setRouting(routingForShard[shard]).get();
            assertThat("shard " + shard, perShard.getHits().getTotalHits(), equalTo(expected));
        }
    }

    private SearchResponse canonicalSearch() {
        return client().prepareSearch("canon").setSize(0).setSearchType(SearchType.QUERY_THEN_FETCH)
            .addAggregation(AggregationBuilders.terms("top_users").field("user").size(3).shuffle(new ShuffleConfig(5))).get();
    }

    private void assertCanonical(SearchResponse response) {
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("top_users");
        assertThat(terms.getBuckets().size(), equalTo(3));
        assertThat(terms.getBuckets().get(0).getKeyAsString(), equalTo("carol"));
        assertThat(terms.getBuckets().get(0).getDocCount(), equalTo(150L));
        assertThat(terms.getBuckets().get(1).getKeyAsString(), equalTo("bob"));
        assertThat(terms.getBuckets().get(1).getDocCount(), equalTo(95L));
        assertThat(terms.getBuckets().get(2).getKeyAsString(), equalTo("alice"));
        assertThat(terms.getBuckets().get(2).getDocCount(), equalTo(90L));
        assertThat(terms.getSumOfOtherDocCounts(), equalTo(660L - 335L));
        assertThat(terms.getDocCountError(), equalTo(0L));
    }

    public void testCanonicalExample() throws Exception {
        indexCanonical(0);
        assertCanonical(canonicalSearch());

        // documentation only (section 10): stock top-k with a small shard_size can miss or undercount keys
        SearchResponse stock = client().prepareSearch("canon").setSize(0)
            .addAggregation(AggregationBuilders.terms("top_users").field("user").size(3).shardSize(3)).get();
        Terms stockTerms = stock.getAggregations().get("top_users");
        StringBuilder sb = new StringBuilder();
        for (Terms.Bucket b : stockTerms.getBuckets()) {
            sb.append(b.getKeyAsString()).append('=').append(b.getDocCount()).append(' ');
        }
        logger.info("stock terms with shard_size=3: [{}] doc_count_error_upper_bound [{}]", sb.toString().trim(),
            stockTerms.getDocCountError());
        assertNoLeaks();
    }

    /**
     * Section 10 retry variant: S3's first attempt sends a partial to one reducer and then fails, so the coordinator retries S3
     * on its other copy with attempt 2. alice must still be 90 (not 125, which a double-counted S3 would give).
     */
    public void testCanonicalExampleWithRetry() throws Exception {
        indexCanonical(1);
        final int s3 = 3;
        AtomicInteger firstAttemptSends = new AtomicInteger();
        AtomicInteger firstAttemptFailures = new AtomicInteger();
        AtomicInteger secondAttemptSends = new AtomicInteger();
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService transport = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            transport.addSendBehavior((connection, requestId, action, request, options) -> {
                if (ShuffleMessages.PARTIAL_ACTION_NAME.equals(action)) {
                    PartialRequest partial = (PartialRequest) request;
                    if (partial.shardId().id() == s3 && partial.attempt() == 1) {
                        // let exactly one partial of the first attempt through, then fail the rest
                        if (firstAttemptSends.incrementAndGet() > 1) {
                            firstAttemptFailures.incrementAndGet();
                            throw new ConnectTransportException(connection.getNode(), "simulated failure of shard 3 attempt 1");
                        }
                    } else if (partial.shardId().id() == s3 && partial.attempt() == 2) {
                        secondAttemptSends.incrementAndGet();
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        try {
            assertCanonical(canonicalSearch());
            assertThat("the first attempt failed mid-send", firstAttemptFailures.get(), greaterThanOrEqualTo(1));
            assertThat("the shard was retried with attempt 2", secondAttemptSends.get(), greaterThan(0));
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                ((MockTransportService) internalCluster().getInstance(TransportService.class, node)).clearAllRules();
            }
        }
        assertNoLeaks();
    }

    // ---------------------------------------------------------------- randomized equivalence

    private int indexRandomData(String index, int shards, int keys) throws Exception {
        assertAcked(prepareCreate(index).setSettings(Settings.builder().put("index.number_of_shards", shards)
            .put("index.number_of_replicas", randomIntBetween(0, 1)))
            .addMapping("_doc", "user", "type=keyword", "uid", "type=long", "day", "type=date", "bytes", "type=long"));
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int n = randomIntBetween(200, 2000);
        int distribution = randomIntBetween(0, 2);
        for (int i = 0; i < n; i++) {
            final int key;
            if (distribution == 0) {
                key = randomIntBetween(0, keys - 1);                                         // uniform
            } else if (distribution == 1) {
                key = Math.min(keys - 1, (int) Math.floor(Math.pow(randomDouble(), 3) * keys)); // skewed, long tail
            } else {
                key = randomBoolean() ? 0 : randomIntBetween(0, keys - 1);                   // one heavy key
            }
            docs.add(client().prepareIndex(index, "_doc").setSource("user", "u" + key, "uid", key,
                "day", 1546300800000L + key * 86_400_000L, "bytes", randomIntBetween(0, 10_000)));
        }
        indexRandom(true, docs);
        ensureGreen(index);
        return n;
    }

    private static TermsAggregationBuilder withMetrics(TermsAggregationBuilder terms) {
        return terms
            .subAggregation(AggregationBuilders.sum("sum").field("bytes"))
            .subAggregation(AggregationBuilders.min("min").field("bytes"))
            .subAggregation(AggregationBuilders.max("max").field("bytes"))
            .subAggregation(AggregationBuilders.avg("avg").field("bytes"))
            .subAggregation(AggregationBuilders.count("count").field("bytes"))
            .subAggregation(AggregationBuilders.stats("stats").field("bytes"))
            .subAggregation(AggregationBuilders.extendedStats("ext").field("bytes"))
            .subAggregation(AggregationBuilders.cardinality("card").field("bytes"))
            .subAggregation(AggregationBuilders.percentiles("pct").field("bytes").percentiles(50))
            .subAggregation(PipelineAggregatorBuilders.bucketScript("doubled", Collections.singletonMap("_value0", "sum"),
                new Script(ScriptType.INLINE, MockScriptPlugin.NAME, "double_it", Collections.emptyMap())));
    }

    private static BucketOrder randomOrder() {
        switch (randomIntBetween(0, 6)) {
            case 0: return BucketOrder.count(false);
            case 1: return BucketOrder.count(true);
            case 2: return BucketOrder.key(true);
            case 3: return BucketOrder.key(false);
            case 4: return BucketOrder.aggregation("sum", false);
            case 5: return BucketOrder.aggregation("avg", true);
            default: return BucketOrder.aggregation("max", false);
        }
    }

    public void testRandomizedEquivalence() throws Exception {
        int keys = randomFrom(1, 5, 50, 500);
        int shards = randomIntBetween(2, 8);
        indexRandomData("rand", shards, keys);
        for (int round = 0; round < 8; round++) {
            String field = randomFrom("user", "uid", "day");
            int size = randomIntBetween(1, 40);
            BucketOrder order = randomOrder();
            long minDocCount = randomIntBetween(1, 3);
            int reducers = randomIntBetween(1, 6);
            SearchType searchType = randomFrom(SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH);
            String ctx = "keys " + keys + " shards " + shards + " field " + field + " size " + size + " order " + order
                + " min_doc_count " + minDocCount + " reducers " + reducers + " " + searchType;
            logger.info("round {}: {}", round, ctx);

            SearchResponse reference = client().prepareSearch("rand").setSize(0).setSearchType(searchType)
                .addAggregation(withMetrics(AggregationBuilders.terms("t").field(field).size(size).order(order)
                    .minDocCount(minDocCount).shardSize(100_000))).get();
            SearchResponse shuffled = client().prepareSearch("rand").setSize(0).setSearchType(searchType)
                .addAggregation(withMetrics(AggregationBuilders.terms("t").field(field).size(size).order(order)
                    .minDocCount(minDocCount).shuffle(new ShuffleConfig(reducers)))).get();
            assertSearchResponse(reference);
            assertSearchResponse(shuffled);
            assertEquivalent(ctx, reference.getAggregations().get("t"), shuffled.getAggregations().get("t"));
            assertNoLeaks();
        }
    }

    private static void assertEquivalent(String ctx, Terms expected, Terms actual) {
        assertThat(ctx, actual.getBuckets().size(), equalTo(expected.getBuckets().size()));
        assertThat(ctx + " sum_other_doc_count", actual.getSumOfOtherDocCounts(), equalTo(expected.getSumOfOtherDocCounts()));
        assertThat(ctx + " doc_count_error_upper_bound", actual.getDocCountError(), equalTo(0L));
        for (int i = 0; i < expected.getBuckets().size(); i++) {
            Terms.Bucket e = expected.getBuckets().get(i);
            Terms.Bucket a = actual.getBuckets().get(i);
            String b = ctx + " bucket " + i + " [" + e.getKeyAsString() + "]";
            assertThat(b, a.getKeyAsString(), equalTo(e.getKeyAsString()));
            assertThat(b, a.getDocCount(), equalTo(e.getDocCount()));
            // integer-valued inputs: these metrics are exact whatever the reduce order
            assertThat(b, ((Sum) a.getAggregations().get("sum")).getValue(), equalTo(((Sum) e.getAggregations().get("sum")).getValue()));
            assertThat(b, ((Min) a.getAggregations().get("min")).getValue(), equalTo(((Min) e.getAggregations().get("min")).getValue()));
            assertThat(b, ((Max) a.getAggregations().get("max")).getValue(), equalTo(((Max) e.getAggregations().get("max")).getValue()));
            assertThat(b, ((Avg) a.getAggregations().get("avg")).getValue(), equalTo(((Avg) e.getAggregations().get("avg")).getValue()));
            assertThat(b, ((ValueCount) a.getAggregations().get("count")).getValue(),
                equalTo(((ValueCount) e.getAggregations().get("count")).getValue()));
            Stats sa = a.getAggregations().get("stats");
            Stats se = e.getAggregations().get("stats");
            assertThat(b, sa.getCount(), equalTo(se.getCount()));
            assertThat(b, sa.getSum(), equalTo(se.getSum()));
            ExtendedStats xa = a.getAggregations().get("ext");
            ExtendedStats xe = e.getAggregations().get("ext");
            assertThat(b, xa.getSumOfSquares(), equalTo(xe.getSumOfSquares()));
            assertThat(b, xa.getVariance(), closeTo(xe.getVariance(), 1e-6 * (1 + Math.abs(xe.getVariance()))));
            // HyperLogLog++ merges are order-independent
            assertThat(b, ((Cardinality) a.getAggregations().get("card")).getValue(),
                equalTo(((Cardinality) e.getAggregations().get("card")).getValue()));
            // t-digest merges depend on order: same bucket data, approximately the same median
            double pe = ((Percentiles) e.getAggregations().get("pct")).percentile(50);
            double pa = ((Percentiles) a.getAggregations().get("pct")).percentile(50);
            double range = ((Max) e.getAggregations().get("max")).getValue() - ((Min) e.getAggregations().get("min")).getValue();
            assertThat(b + " p50", pa, closeTo(pe, 0.05 * range + 1e-9));
            // the parent pipeline ran exactly once, on the reducer (code-map.md D5)
            assertThat(b + " bucket_script", ((SimpleValue) a.getAggregations().get("doubled")).value(),
                equalTo(((SimpleValue) e.getAggregations().get("doubled")).value()));
        }
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
}
