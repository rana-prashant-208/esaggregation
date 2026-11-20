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

import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.admin.cluster.RestShuffleStatsAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ShuffleStatsTests extends ESTestCase {

    private static DiscoveryNode node() {
        return new DiscoveryNode("n1", buildNewFakeTransportAddress(), Collections.emptyMap(), EnumSet.allOf(DiscoveryNode.Role.class),
            Version.CURRENT);
    }

    public void testSerializationAndRendering() throws IOException {
        NodeShuffleStats stats = new NodeShuffleStats(node(), 2, 1024, 30, 9000, 7, 1, 3);
        NodeShuffleStats copy;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                copy = NodeShuffleStats.readNodeShuffleStats(in);
            }
        }
        assertThat(copy.getNode().getId(), equalTo("n1"));
        assertThat(copy.getActiveStates(), equalTo(2));
        assertThat(copy.getReservedBytes(), equalTo(1024L));
        assertThat(copy.getPartialsAccepted(), equalTo(30L));
        assertThat(copy.getBytesAccepted(), equalTo(9000L));
        assertThat(copy.getFinalizations(), equalTo(7L));
        assertThat(copy.getRejections(), equalTo(1L));
        assertThat(copy.getTimeouts(), equalTo(3L));

        ShuffleStatsResponse response = new ShuffleStatsResponse(new ClusterName("c"), Collections.singletonList(copy),
            Collections.emptyList());
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        response.toXContent(builder, null);
        String json = Strings.toString(builder.endObject());
        assertThat(json, equalTo("{\"nodes\":{\"n1\":{\"name\":\"" + copy.getNode().getName() + "\",\"shuffle\":{\"active_states\":2,"
            + "\"reserved_bytes\":1024,\"partials_accepted\":30,\"bytes_accepted\":9000,\"finalizations\":7,\"rejections\":1,"
            + "\"timeouts\":3}}}}"));
    }

    public void testRestHandler() throws Exception {
        RestController controller = mock(RestController.class);
        RestShuffleStatsAction handler = new RestShuffleStatsAction(Settings.EMPTY, controller);
        verify(controller).registerHandler(RestRequest.Method.GET, "/_shuffle/stats", handler);
        verify(controller).registerHandler(RestRequest.Method.GET, "/_shuffle/{nodeId}/stats", handler);

        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            AtomicReference<ActionRequest> executed = new AtomicReference<>();
            NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse,
                        RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>>
                void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
                    assertThat(action.name(), equalTo(ShuffleStatsAction.NAME));
                    executed.set(request);
                }
            };
            FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_shuffle/n1,n2/stats")
                .withParams(new java.util.HashMap<>(Collections.singletonMap("nodeId", "n1,n2"))).build();
            handler.handleRequest(request, new FakeRestChannel(request, false, 1), client);
            ShuffleStatsRequest statsRequest = (ShuffleStatsRequest) executed.get();
            assertThat(statsRequest.nodesIds(), arrayContaining("n1", "n2"));
            assertThat(handler.getName(), containsString("shuffle"));
        } finally {
            terminate(threadPool);
        }
    }
}
