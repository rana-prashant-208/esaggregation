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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.ShuffleConfig;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.shuffle.ShuffleSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ShuffleRequestValidatorTests extends ESTestCase {

    private static DiscoveryNodes nodes(boolean allSupport) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        int count = randomIntBetween(1, 5);
        int legacy = allSupport ? -1 : randomIntBetween(0, count - 1);
        for (int i = 0; i < count; i++) {
            Map<String, String> attributes = new HashMap<>();
            if (i != legacy) {
                attributes.put(ShuffleSettings.PROTOCOL_ATTRIBUTE, ShuffleSettings.PROTOCOL_VERSION);
            } else if (randomBoolean()) {
                attributes.put(ShuffleSettings.PROTOCOL_ATTRIBUTE, "0"); // a different protocol version is not compatible
            }
            builder.add(new DiscoveryNode("node_" + i, buildNewFakeTransportAddress(), attributes,
                EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT));
        }
        return builder.localNodeId("node_0").build();
    }

    private static ClusterSettings settings(boolean enabled, int maxReducers) {
        Settings settings = Settings.builder()
            .put(ShuffleSettings.SHUFFLE_ENABLED_SETTING.getKey(), enabled)
            .put(ShuffleSettings.SHUFFLE_MAX_REDUCERS_SETTING.getKey(), maxReducers)
            .build();
        return new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    private static TermsAggregationBuilder shuffled(int reducers) {
        return new TermsAggregationBuilder("top_users", null).field("user").size(100).shuffle(new ShuffleConfig(reducers));
    }

    private static SearchRequest request(AggregationBuilder... aggs) {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0);
        for (AggregationBuilder agg : aggs) {
            source.aggregation(agg);
        }
        return new SearchRequest("events").source(source);
    }

    private static TermsAggregationBuilder validate(SearchRequest request) {
        return ShuffleRequestValidator.validate(request, false, nodes(true), settings(true, 16));
    }

    private static void assertRejected(SearchRequest request, String message) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validate(request));
        assertThat(e.getMessage(), containsString(message));
    }

    public void testValidRequest() {
        TermsAggregationBuilder terms = shuffled(5);
        terms.subAggregation(new SumAggregationBuilder("bytes").field("bytes"));
        terms.subAggregation(new CardinalityAggregationBuilder("hosts", ValueType.STRING).field("host"));
        Map<String, String> paths = Collections.singletonMap("b", "bytes");
        terms.subAggregation(PipelineAggregatorBuilders.bucketScript("double_bytes", paths, new Script("params.b * 2")));
        terms.subAggregation(PipelineAggregatorBuilders.bucketSelector("big", paths, new Script("params.b > 10")));
        SearchRequest request = request(terms, new AvgAggregationBuilder("avg_bytes").field("bytes"));
        assertThat(validate(request), sameInstance(terms));
    }

    public void testNoShuffleIsIgnored() {
        // even when shuffle is disabled and nodes lack the attribute, normal requests pass untouched
        SearchRequest request = request(new TermsAggregationBuilder("t", null).field("user"));
        assertThat(ShuffleRequestValidator.validate(request, true, nodes(false), settings(false, 16)), nullValue());
        assertThat(ShuffleRequestValidator.validate(new SearchRequest(), true, nodes(false), settings(false, 16)), nullValue());
        SearchRequest noAggs = new SearchRequest().source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        assertThat(ShuffleRequestValidator.validate(noAggs, true, nodes(false), settings(false, 16)), nullValue());
    }

    public void testDisabled() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> ShuffleRequestValidator.validate(request(shuffled(2)), false, nodes(true), settings(false, 16)));
        assertThat(e.getMessage(), containsString("search.shuffle.enabled"));
    }

    public void testProtocolGate() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> ShuffleRequestValidator.validate(request(shuffled(2)), false, nodes(false), settings(true, 16)));
        assertThat(e.getMessage(), containsString("shuffle protocol"));
        assertNotNull(ShuffleRequestValidator.validate(request(shuffled(2)), false, nodes(true), settings(true, 16)));
    }

    public void testScroll() {
        SearchRequest request = request(shuffled(2));
        request.scroll(new Scroll(TimeValue.timeValueMinutes(1)));
        assertRejected(request, "scroll");
    }

    public void testCrossCluster() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> ShuffleRequestValidator.validate(request(shuffled(2)), true, nodes(true), settings(true, 16)));
        assertThat(e.getMessage(), containsString("cross-cluster"));
    }

    public void testNotTopLevel() {
        FilterAggregationBuilder filter = new FilterAggregationBuilder("f", QueryBuilders.matchAllQuery());
        filter.subAggregation(shuffled(2));
        assertRejected(request(filter), "top-level");

        TermsAggregationBuilder outer = new TermsAggregationBuilder("outer", null).field("x");
        FilterAggregationBuilder middle = new FilterAggregationBuilder("middle", QueryBuilders.matchAllQuery());
        middle.subAggregation(shuffled(2));
        outer.subAggregation(middle);
        assertRejected(request(outer), "top-level");
    }

    public void testOnlyOneShuffle() {
        assertRejected(request(shuffled(2), new TermsAggregationBuilder("b", null).field("y").shuffle(new ShuffleConfig(2))),
            "only one");
    }

    public void testSiblingPipelineReference() {
        SearchRequest request = request(shuffled(2));
        request.source().aggregation(PipelineAggregatorBuilders.maxBucket("max_users", "top_users>_count"));
        assertRejected(request, "sibling pipeline aggregation [max_users]");

        SearchRequest other = request(shuffled(2), new TermsAggregationBuilder("other", null).field("z"));
        other.source().aggregation(PipelineAggregatorBuilders.maxBucket("max_other", "other>_count"));
        assertNotNull(validate(other));
    }

    public void testScript() {
        TermsAggregationBuilder terms = new TermsAggregationBuilder("t", null).script(new Script("doc['user'].value"))
            .shuffle(new ShuffleConfig(2));
        assertRejected(request(terms), "scripts");
    }

    public void testFieldRequired() {
        assertRejected(request(new TermsAggregationBuilder("t", null).shuffle(new ShuffleConfig(2))), "requires a [field]");
    }

    public void testValueType() {
        assertRejected(request(new TermsAggregationBuilder("t", null).field("f").valueType(ValueType.DOUBLE)
                .shuffle(new ShuffleConfig(2))), "value_type");
        assertNotNull(validate(request(new TermsAggregationBuilder("t", null).field("f").valueType(ValueType.LONG)
            .shuffle(new ShuffleConfig(2)))));
    }

    public void testShardSize() {
        assertRejected(request(shuffled(2).shardSize(randomIntBetween(1, 1000))), "[shard_size]");
    }

    public void testShardMinDocCount() {
        assertRejected(request(shuffled(2).shardMinDocCount(randomIntBetween(1, 10))), "[shard_min_doc_count]");
        assertNotNull(validate(request(shuffled(2).shardMinDocCount(0))));
    }

    public void testMaxReducers() {
        int max = randomIntBetween(1, 16);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> ShuffleRequestValidator.validate(request(shuffled(max + 1)), false, nodes(true), settings(true, max)));
        assertThat(e.getMessage(), containsString("at most [" + max + "]"));
        assertNotNull(ShuffleRequestValidator.validate(request(shuffled(max)), false, nodes(true), settings(true, max)));
    }

    public void testBucketSubAggregation() {
        TermsAggregationBuilder terms = shuffled(2);
        terms.subAggregation(new TermsAggregationBuilder("inner", null).field("x"));
        assertRejected(request(terms), "only allows metric sub-aggregations");
    }

    public void testNonAllowedMetricSubAggregation() {
        TermsAggregationBuilder terms = shuffled(2);
        terms.subAggregation(new TopHitsAggregationBuilder("hits"));
        assertRejected(request(terms), "[top_hits]");
    }

    public void testPipelineSubAggregation() {
        TermsAggregationBuilder terms = shuffled(2);
        terms.subAggregation(new SumAggregationBuilder("bytes").field("bytes"));
        terms.subAggregation(PipelineAggregatorBuilders.bucketSort("sort", Collections.emptyList()));
        assertRejected(request(terms), "only allows pipeline sub-aggregations");
    }
}
