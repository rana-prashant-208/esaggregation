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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.terms.ShuffleConfig;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * M1: every node advertises the shuffle protocol, and the coordinator gate honours {@code search.shuffle.enabled}.
 */
// transport clients would drop the coordinator-only shuffle option (code-map.md D3)
@ESIntegTestCase.ClusterScope(transportClientRatio = 0)
public class ShuffleGateIT extends ESIntegTestCase {

    public void testNodesAdvertiseProtocol() {
        for (DiscoveryNode node : client().admin().cluster().prepareState().get().getState().nodes()) {
            assertThat(node.getAttributes().get(ShuffleSettings.PROTOCOL_ATTRIBUTE), equalTo(ShuffleSettings.PROTOCOL_VERSION));
        }
    }

    public void testEnabledSettingGatesRequests() {
        createIndex("events");
        client().prepareIndex("events", "_doc", "1").setSource("user", "alice").setRefreshPolicy("true").get();
        TermsAggregationBuilder terms = new TermsAggregationBuilder("top_users", null).field("user.keyword")
            .shuffle(new ShuffleConfig(2));

        Exception e = expectThrows(Exception.class, () -> client().prepareSearch("events").setSize(0).addAggregation(terms).get());
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ExceptionsHelper.unwrapCause(e).getMessage(), containsString("search.shuffle.enabled"));

        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(ShuffleSettings.SHUFFLE_ENABLED_SETTING.getKey(), true)));
        try {
            // passes the gate; shard/reducer execution arrives in later milestones
            SearchResponse response = client().prepareSearch("events").setSize(0).addAggregation(terms).get();
            assertSearchResponse(response);

            Exception tooMany = expectThrows(Exception.class, () -> client().prepareSearch("events").setSize(0)
                .addAggregation(new TermsAggregationBuilder("t", null).field("user.keyword").shuffle(new ShuffleConfig(1000))).get());
            assertThat(ExceptionsHelper.status(tooMany), equalTo(RestStatus.BAD_REQUEST));
        } finally {
            assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(ShuffleSettings.SHUFFLE_ENABLED_SETTING.getKey())));
        }
    }
}
