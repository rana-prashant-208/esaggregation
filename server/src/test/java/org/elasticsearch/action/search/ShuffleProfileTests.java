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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.shuffle.ShuffleMessages;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ShuffleProfileTests extends ESTestCase {

    private static SearchProfileShardResults shardProfiles() {
        Map<String, Long> breakdown = new HashMap<>();
        breakdown.put("build_aggregation", 10L);
        ProfileResult agg = new ProfileResult("StringTermsAggregator", "top", breakdown, Collections.emptyList());
        Map<String, ProfileShardResult> shards = new HashMap<>();
        shards.put("[node1][events][0]", new ProfileShardResult(Collections.emptyList(),
            new AggregationProfileShardResult(Collections.singletonList(agg))));
        return new SearchProfileShardResults(shards);
    }

    public void testOneEntryPerReducer() throws IOException {
        List<ShuffleCoordinator.ReducerResult> results = Arrays.asList(
            new ShuffleCoordinator.ReducerResult("r1", new ShuffleMessages.FinalizeResponse(null, 3, 120, 4096, 1_000), 5_000),
            new ShuffleCoordinator.ReducerResult("r2", new ShuffleMessages.FinalizeResponse(null, 3, 80, 2048, 7_000), 6_000));
        SearchProfileShardResults profile = ShuffleFinalizePhase.withShuffleProfile(shardProfiles(), "sh-1", results);

        Map<String, ProfileShardResult> byId = profile.getShardResults();
        assertThat(byId.size(), equalTo(3));
        assertTrue("shard profiles are kept", byId.containsKey("[node1][events][0]"));

        ProfileResult r1 = byId.get("[shuffle][r1]").getAggregationProfileResults().getProfileResults().get(0);
        assertThat(r1.getQueryName(), equalTo("shuffle_reducer"));
        assertThat(r1.getLuceneDescription(), equalTo("reducer [r1] shuffle [sh-1] partials [3] buckets [120] bytes [4096]"));
        assertThat(r1.getTimeBreakdown().get("merge"), equalTo(1_000L));
        assertThat(r1.getTimeBreakdown().get("transport_and_queue"), equalTo(4_000L));
        assertThat("time is the finalize round trip", r1.getTime(), equalTo(5_000L));

        // clock skew between measurements never produces a negative component
        ProfileResult r2 = byId.get("[shuffle][r2]").getAggregationProfileResults().getProfileResults().get(0);
        assertThat(r2.getTimeBreakdown().get("transport_and_queue"), equalTo(0L));

        // the extra entries use only stock profile structures: they serialize and render like any shard profile
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            profile.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(new SearchProfileShardResults(in).getShardResults().keySet(), equalTo(byId.keySet()));
            }
        }
        String json = Strings.toString(profile);
        assertThat(json, containsString("\"id\":\"[shuffle][r1]\""));
        assertThat(json, containsString("\"type\":\"shuffle_reducer\""));
    }
}
