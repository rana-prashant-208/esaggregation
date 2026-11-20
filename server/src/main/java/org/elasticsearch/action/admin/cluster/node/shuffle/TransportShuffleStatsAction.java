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
package org.elasticsearch.action.admin.cluster.node.shuffle;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.shuffle.ShuffleService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportShuffleStatsAction extends TransportNodesAction<ShuffleStatsRequest, ShuffleStatsResponse,
        TransportShuffleStatsAction.NodeShuffleStatsRequest, NodeShuffleStats> {

    private final ShuffleService shuffleService;

    @Inject
    public TransportShuffleStatsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                       TransportService transportService, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver, ShuffleService shuffleService) {
        super(settings, ShuffleStatsAction.NAME, threadPool, clusterService, transportService, actionFilters,
            indexNameExpressionResolver, ShuffleStatsRequest::new, NodeShuffleStatsRequest::new, ThreadPool.Names.MANAGEMENT,
            NodeShuffleStats.class);
        this.shuffleService = shuffleService;
    }

    @Override
    protected ShuffleStatsResponse newResponse(ShuffleStatsRequest request, List<NodeShuffleStats> responses,
                                               List<FailedNodeException> failures) {
        return new ShuffleStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeShuffleStatsRequest newNodeRequest(String nodeId, ShuffleStatsRequest request) {
        return new NodeShuffleStatsRequest(nodeId);
    }

    @Override
    protected NodeShuffleStats newNodeResponse() {
        return new NodeShuffleStats();
    }

    @Override
    protected NodeShuffleStats nodeOperation(NodeShuffleStatsRequest request) {
        return NodeShuffleStats.of(clusterService.localNode(), shuffleService);
    }

    public static class NodeShuffleStatsRequest extends BaseNodeRequest {
        public NodeShuffleStatsRequest() {
        }

        NodeShuffleStatsRequest(String nodeId) {
            super(nodeId);
        }
    }
}
