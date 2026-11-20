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
package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.shuffle.ShuffleStatsAction;
import org.elasticsearch.action.admin.cluster.node.shuffle.ShuffleStatsRequest;
import org.elasticsearch.action.admin.cluster.node.shuffle.ShuffleStatsResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * {@code GET /_shuffle/stats} and {@code GET /_shuffle/{nodeId}/stats}: reducer-side shuffle stats per node
 * (docs/shuffle/PROPOSAL.md, M7). Not under {@code _nodes/...}, where {@code _nodes/{nodeId}/stats} would capture the path.
 */
public class RestShuffleStatsAction extends BaseRestHandler {

    public RestShuffleStatsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_shuffle/stats", this);
        controller.registerHandler(GET, "/_shuffle/{nodeId}/stats", this);
    }

    @Override
    public String getName() {
        return "shuffle_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        ShuffleStatsRequest statsRequest = new ShuffleStatsRequest(Strings.splitStringByCommaToArray(request.param("nodeId")));
        statsRequest.timeout(request.param("timeout"));
        return channel -> client.execute(ShuffleStatsAction.INSTANCE, statsRequest, new RestBuilderListener<ShuffleStatsResponse>(channel) {
            @Override
            public RestResponse buildResponse(ShuffleStatsResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                RestActions.buildNodesHeader(builder, channel.request(), response);
                builder.field("cluster_name", response.getClusterName().value());
                response.toXContent(builder, channel.request());
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
