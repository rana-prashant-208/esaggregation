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

import org.elasticsearch.action.admin.cluster.node.shuffle.NodeShuffleStats;
import org.elasticsearch.action.admin.cluster.node.shuffle.ShuffleStatsAction;
import org.elasticsearch.action.admin.cluster.node.shuffle.ShuffleStatsRequest;
import org.elasticsearch.action.admin.cluster.node.shuffle.ShuffleStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ShuffleConfig;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * M7: shuffle timings in the profile API and per-node shuffle stats.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, transportClientRatio = 0)
public class ShuffleObservabilityIT extends ESIntegTestCase {

    private static final int SHARDS = 3;
    private static final Pattern DESCRIPTION =
        Pattern.compile("reducer \\[(.+)] shuffle \\[(.+)] partials \\[(\\d+)] buckets \\[(\\d+)] bytes \\[(\\d+)]");

    @Before
    public void setUpIndex() throws Exception {
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(ShuffleSettings.SHUFFLE_ENABLED_SETTING.getKey(), true)));
        assertAcked(prepareCreate("events").setSettings(Settings.builder().put("index.number_of_shards", SHARDS)
            .put("index.number_of_replicas", 0)).addMapping("_doc", "user", "type=keyword"));
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            docs.add(client().prepareIndex("events", "_doc").setSource("user", "u" + randomIntBetween(0, 40)));
        }
        indexRandom(true, docs);
        ensureGreen("events");
    }

    private SearchResponse shuffledSearch(int reducers, boolean profile) {
        return client().prepareSearch("events").setSize(0).setSearchType(SearchType.QUERY_THEN_FETCH).setProfile(profile)
            .addAggregation(AggregationBuilders.terms("top").field("user").size(5).shuffle(new ShuffleConfig(reducers))).get();
    }

    public void testProfileHasOneEntryPerReducer() {
        int reducers = randomIntBetween(1, 3);
        SearchResponse response = shuffledSearch(reducers, true);
        assertSearchResponse(response);

        Map<String, ProfileShardResult> profiles = response.getProfileResults();
        Set<String> reducerIds = new HashSet<>();
        long buckets = 0;
        for (Map.Entry<String, ProfileShardResult> entry : profiles.entrySet()) {
            if (entry.getKey().startsWith("[shuffle][") == false) {
                continue;
            }
            List<ProfileResult> aggs = entry.getValue().getAggregationProfileResults().getProfileResults();
            assertThat(aggs.size(), equalTo(1));
            ProfileResult reducer = aggs.get(0);
            assertThat(reducer.getQueryName(), equalTo("shuffle_reducer"));
            Matcher m = DESCRIPTION.matcher(reducer.getLuceneDescription());
            assertTrue(reducer.getLuceneDescription(), m.matches());
            reducerIds.add(m.group(1));
            assertThat(entry.getKey(), equalTo("[shuffle][" + m.group(1) + "]"));
            assertThat("one partial per shard", Integer.parseInt(m.group(3)), equalTo(SHARDS));
            assertThat("bytes received", Long.parseLong(m.group(5)), greaterThan(0L));
            buckets += Long.parseLong(m.group(4));
            assertThat(reducer.getTimeBreakdown().get("merge"), greaterThanOrEqualTo(0L));
            assertThat(reducer.getTimeBreakdown().get("transport_and_queue"), greaterThanOrEqualTo(0L));
            assertThat("time is the finalize round trip", reducer.getTime(), greaterThan(0L));
        }
        assertThat(reducerIds.size(), equalTo(reducers));
        assertThat("every shard bucket was received by some reducer", buckets, greaterThan(0L));
        // normal shard profiles are still there
        assertThat(profiles.size(), equalTo(SHARDS + reducers));
        assertThat(Strings.toString(response), containsString("\"type\":\"shuffle_reducer\""));
    }

    public void testNoProfileNoEntries() {
        SearchResponse response = shuffledSearch(2, false);
        assertSearchResponse(response);
        assertTrue(response.getProfileResults().isEmpty());
    }

    private ShuffleStatsResponse stats() {
        return client().execute(ShuffleStatsAction.INSTANCE, new ShuffleStatsRequest()).actionGet();
    }

    public void testNodeStats() throws Exception {
        ShuffleStatsResponse before = stats();
        assertThat(before.getNodes().size(), equalTo(internalCluster().size()));
        assertThat(before.failures().size(), equalTo(0));

        int reducers = randomIntBetween(1, 3);
        int searches = randomIntBetween(1, 3);
        for (int i = 0; i < searches; i++) {
            assertSearchResponse(shuffledSearch(reducers, false));
        }

        assertBusy(() -> {
            ShuffleStatsResponse after = stats();
            long partials = 0;
            long finalizations = 0;
            long bytes = 0;
            for (NodeShuffleStats node : after.getNodes()) {
                assertThat(node.getActiveStates(), equalTo(0));
                assertThat(node.getReservedBytes(), equalTo(0L));
                assertThat(node.getRejections(), equalTo(0L));
                assertThat(node.getTimeouts(), equalTo(0L));
                partials += node.getPartialsAccepted();
                finalizations += node.getFinalizations();
                bytes += node.getBytesAccepted();
            }
            assertThat(partials, equalTo((long) searches * SHARDS * reducers));
            assertThat(finalizations, equalTo((long) searches * reducers));
            assertThat(bytes, greaterThan(0L));
            assertThat(after.toString().isEmpty(), equalTo(false));
        });

        // a breaker trip shows up as a rejection
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
            .put(ShuffleSettings.SHUFFLE_HEAP_ESTIMATE_FACTOR_SETTING.getKey(), 1e12)));
        try {
            expectThrows(Exception.class, () -> shuffledSearch(2, false));
        } finally {
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putNull(ShuffleSettings.SHUFFLE_HEAP_ESTIMATE_FACTOR_SETTING.getKey())));
        }
        assertBusy(() -> {
            long rejections = 0;
            for (NodeShuffleStats node : stats().getNodes()) {
                rejections += node.getRejections();
                assertThat(node.getActiveStates(), equalTo(0));
            }
            assertThat(rejections, greaterThan(0L));
        });
    }
}
