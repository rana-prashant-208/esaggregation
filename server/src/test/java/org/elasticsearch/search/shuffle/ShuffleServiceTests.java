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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageTester;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.shuffle.ShuffleMessages.FinalizeRequest;
import org.elasticsearch.search.shuffle.ShuffleMessages.FinalizeResponse;
import org.elasticsearch.search.shuffle.ShuffleMessages.FreeRequest;
import org.elasticsearch.search.shuffle.ShuffleMessages.PartialRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

public class ShuffleServiceTests extends ESTestCase {

    private static final String AGG = "top_users";

    private ThreadPool threadPool;
    private NamedWriteableRegistry registry;
    private final AtomicLong clock = new AtomicLong();

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
        registry = new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedWriteables());
        clock.set(randomLongBetween(0, 1_000_000));
    }

    @After
    public void tearDownThreadPool() throws InterruptedException {
        terminate(threadPool);
    }

    private static ClusterSettings clusterSettings(Settings settings) {
        return new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    private static CircuitBreakerService breakerService(String requestLimit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), requestLimit)
            .build();
        return new HierarchyCircuitBreakerService(settings, clusterSettings(settings));
    }

    private ShuffleService service(CircuitBreakerService breakers) {
        return new ShuffleService(Settings.EMPTY, clusterSettings(Settings.EMPTY), threadPool, breakers, registry,
            finalReduce -> new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, finalReduce), clock::get);
    }

    private static long requestBreakerUsed(CircuitBreakerService breakers) {
        return breakers.getBreaker(CircuitBreaker.REQUEST).getUsed();
    }

    // ---- fake partials ----

    /** A shard's data: key to doc count. The sum sub-aggregation is doc count * 10 so it is checkable. */
    private static Map<String, Long> randomShardData(int keySpace) {
        Map<String, Long> data = new HashMap<>();
        int keys = randomIntBetween(0, keySpace);
        for (int i = 0; i < keys; i++) {
            data.put("user_" + randomIntBetween(0, keySpace), (long) randomIntBetween(1, 100));
        }
        return data;
    }

    private static StringTerms partial(Map<String, Long> data, int size) {
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        for (Map.Entry<String, Long> e : data.entrySet()) {
            InternalAggregations subs = new InternalAggregations(Collections.singletonList(
                new InternalSum("bytes", e.getValue() * 10d, DocValueFormat.RAW, Collections.emptyList(), null)));
            buckets.add(new StringTerms.Bucket(new BytesRef(e.getKey()), e.getValue(), subs, false, 0, DocValueFormat.RAW));
        }
        return new StringTerms(AGG, BucketOrder.compound(BucketOrder.count(false)), size, 1, Collections.emptyList(), null,
            DocValueFormat.RAW, Integer.MAX_VALUE, false, 0, buckets, 0);
    }

    private static PartialRequest request(String shuffleId, ShardId shardId, int attempt, String coordinator, StringTerms partial) {
        return new PartialRequest(shuffleId, AGG, shardId, attempt, 0, coordinator, partial.getBuckets().size(),
            ShuffleMessages.serializePartial(partial));
    }

    private InternalAggregation decode(BytesReference bytes) throws IOException {
        try (StreamInput in = new NamedWriteableAwareStreamInput(bytes.streamInput(), registry)) {
            return in.readNamedWriteable(InternalAggregation.class);
        }
    }

    /** The reference result: a plain final reduce over fresh copies of the same partials. */
    private InternalAggregation expectedReduce(List<StringTerms> partials) throws IOException {
        List<InternalAggregations> copies = new ArrayList<>();
        for (StringTerms partial : partials) {
            copies.add(new InternalAggregations(Collections.singletonList(decode(ShuffleMessages.serializePartial(partial)))));
        }
        InternalAggregation.ReduceContext context =
            new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, true);
        return (InternalAggregation) InternalAggregations.reduce(copies, context).get(AGG);
    }

    private static ShardId shard(int id) {
        return new ShardId("events", "_na_", id);
    }

    // ---- tests ----

    public void testFinalizeMatchesReduce() throws IOException {
        CircuitBreakerService breakers = breakerService("100mb");
        ShuffleService service = service(breakers);
        int shards = randomIntBetween(1, 8);
        int size = randomIntBetween(1, 50);
        List<StringTerms> partials = new ArrayList<>();
        Map<ShardId, Integer> keep = new HashMap<>();
        for (int s = 0; s < shards; s++) {
            StringTerms partial = partial(randomShardData(100), size);
            partials.add(partial);
            service.onPartial(request("sh1", shard(s), 1, "coord", partial));
            keep.put(shard(s), 1);
        }
        assertThat(service.activeStates(), equalTo(1));
        assertThat(requestBreakerUsed(breakers), greaterThan(0L));
        assertThat(service.reservedBytes(), equalTo(requestBreakerUsed(breakers)));

        FinalizeResponse response = service.onFinalize(new FinalizeRequest("sh1", keep));
        InternalAggregation expected = expectedReduce(partials);
        assertEquals(expected, response.result());
        assertThat(((StringTerms) response.result()).getDocCountError(), equalTo(0L));
        assertThat(response.partialsReceived(), equalTo(shards));
        long buckets = partials.stream().mapToLong(p -> p.getBuckets().size()).sum();
        assertThat(response.bucketsReceived(), equalTo(buckets));

        assertThat(service.activeStates(), equalTo(0));
        assertThat(requestBreakerUsed(breakers), equalTo(0L));
    }

    public void testAttemptSelection() throws IOException {
        CircuitBreakerService breakers = breakerService("100mb");
        ShuffleService service = service(breakers);
        StringTerms s0 = partial(Collections.singletonMap("alice", 40L), 3);
        StringTerms s3attempt1 = partial(Collections.singletonMap("alice", 35L), 3);
        StringTerms s3attempt2 = partial(Collections.singletonMap("alice", 35L), 3);
        StringTerms failedShard = partial(Collections.singletonMap("alice", 1000L), 3);

        service.onPartial(request("sh", shard(0), 1, "coord", s0));
        service.onPartial(request("sh", shard(3), 1, "coord", s3attempt1));
        service.onPartial(request("sh", shard(3), 2, "coord", s3attempt2));
        service.onPartial(request("sh", shard(4), 1, "coord", failedShard)); // shard 4 later failed for good

        Map<ShardId, Integer> keep = new HashMap<>();
        keep.put(shard(0), 1);
        keep.put(shard(3), 2);
        FinalizeResponse response = service.onFinalize(new FinalizeRequest("sh", keep));
        StringTerms result = (StringTerms) response.result();
        assertThat(result.getBuckets().size(), equalTo(1));
        assertThat(result.getBucketByKey("alice").getDocCount(), equalTo(75L)); // not 110 (double count) and not 1075
        assertThat(((InternalSum) result.getBucketByKey("alice").getAggregations().get("bytes")).getValue(), equalTo(750d));
        assertThat(response.partialsReceived(), equalTo(4));
        assertThat(requestBreakerUsed(breakers), equalTo(0L));
    }

    public void testFinalizeFailsWhenShardMissing() {
        CircuitBreakerService breakers = breakerService("100mb");
        ShuffleService service = service(breakers);
        service.onPartial(request("sh", shard(0), 1, "coord", partial(randomShardData(10), 5)));

        Map<ShardId, Integer> keep = new HashMap<>();
        keep.put(shard(0), 1);
        keep.put(shard(1), 1);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> service.onFinalize(new FinalizeRequest("sh", keep)));
        assertThat(e.getMessage(), containsString("[events][1]"));
        assertThat(service.activeStates(), equalTo(0));
        assertThat(requestBreakerUsed(breakers), equalTo(0L));
    }

    public void testFinalizeFailsWhenAttemptMissing() {
        ShuffleService service = service(breakerService("100mb"));
        service.onPartial(request("sh", shard(0), 1, "coord", partial(randomShardData(10), 5)));
        IllegalStateException e = expectThrows(IllegalStateException.class,
            () -> service.onFinalize(new FinalizeRequest("sh", Collections.singletonMap(shard(0), 2))));
        assertThat(e.getMessage(), containsString("attempt [2]"));
    }

    public void testFinalizeWithoutStateAndNoShards() throws IOException {
        ShuffleService service = service(breakerService("100mb"));
        FinalizeResponse response = service.onFinalize(new FinalizeRequest("never-seen", Collections.emptyMap()));
        assertThat(response.result(), nullValue());
        expectThrows(IllegalStateException.class,
            () -> service.onFinalize(new FinalizeRequest("never-seen-2", Collections.singletonMap(shard(0), 1))));
    }

    public void testFinalizeTwiceFails() throws IOException {
        ShuffleService service = service(breakerService("100mb"));
        service.onPartial(request("sh", shard(0), 1, "coord", partial(randomShardData(10), 5)));
        service.onFinalize(new FinalizeRequest("sh", Collections.singletonMap(shard(0), 1)));
        IllegalStateException e = expectThrows(IllegalStateException.class,
            () -> service.onFinalize(new FinalizeRequest("sh", Collections.singletonMap(shard(0), 1))));
        assertThat(e.getMessage(), containsString("already finalized or freed"));
    }

    public void testPartialAfterFinalizeOrFreeIsRejected() throws IOException {
        CircuitBreakerService breakers = breakerService("100mb");
        ShuffleService service = service(breakers);
        service.onPartial(request("a", shard(0), 1, "coord", partial(randomShardData(10), 5)));
        service.onFinalize(new FinalizeRequest("a", Collections.singletonMap(shard(0), 1)));
        expectThrows(ShuffleRejectedException.class,
            () -> service.onPartial(request("a", shard(0), 2, "coord", partial(randomShardData(10), 5))));

        service.free("b");
        expectThrows(ShuffleRejectedException.class,
            () -> service.onPartial(request("b", shard(0), 1, "coord", partial(randomShardData(10), 5))));
        assertThat(service.activeStates(), equalTo(0));
        assertThat(requestBreakerUsed(breakers), equalTo(0L));
    }

    public void testAggNameMismatch() {
        ShuffleService service = service(breakerService("100mb"));
        service.onPartial(request("sh", shard(0), 1, "coord", partial(randomShardData(10), 5)));
        PartialRequest other = new PartialRequest("sh", "other_agg", shard(1), 1, 0, "coord", 0,
            ShuffleMessages.serializePartial(partial(Collections.emptyMap(), 5)));
        expectThrows(IllegalArgumentException.class, () -> service.onPartial(other));
    }

    public void testBreakerTripPoisonsAndReleases() throws IOException {
        CircuitBreakerService breakers = breakerService("20kb");
        ShuffleService service = service(breakers);
        long baseline = requestBreakerUsed(breakers);
        ShuffleRejectedException rejected = null;
        int sent = 0;
        for (int s = 0; s < 1000 && rejected == null; s++) {
            Map<String, Long> data = new HashMap<>();
            for (int k = 0; k < 50; k++) {
                data.put("user_" + s + "_" + k, 1L);
            }
            try {
                service.onPartial(request("sh", shard(s), 1, "coord", partial(data, 10)));
                sent++;
            } catch (ShuffleRejectedException e) {
                rejected = e;
            }
        }
        assertNotNull("breaker never tripped", rejected);
        assertThat(sent, greaterThan(0));
        assertThat(rejected.getMessage(), containsString("circuit breaker"));
        assertThat(requestBreakerUsed(breakers), equalTo(baseline));
        assertThat(service.reservedBytes(), equalTo(0L));
        assertThat(service.rejections(), equalTo(1L));

        // every later partial and the finalize are rejected without touching the breaker
        ShuffleRejectedException again = expectThrows(ShuffleRejectedException.class,
            () -> service.onPartial(request("sh", shard(9999), 1, "coord", partial(Collections.singletonMap("x", 1L), 1))));
        assertThat(again.getMessage(), containsString("poisoned"));
        assertThat(requestBreakerUsed(breakers), equalTo(baseline));
        expectThrows(ShuffleRejectedException.class,
            () -> service.onFinalize(new FinalizeRequest("sh", Collections.singletonMap(shard(0), 1))));
        assertThat(service.activeStates(), equalTo(0));
        assertThat(requestBreakerUsed(breakers), equalTo(baseline));
    }

    public void testTimeoutCleanup() {
        CircuitBreakerService breakers = breakerService("100mb");
        ShuffleService service = service(breakers);
        service.onPartial(request("old", shard(0), 1, "coord", partial(randomShardData(10), 5)));
        clock.addAndGet(ShuffleSettings.SHUFFLE_STATE_TIMEOUT_SETTING.get(Settings.EMPTY).millis() / 2);
        service.onPartial(request("new", shard(0), 1, "coord", partial(randomShardData(10), 5)));

        clock.addAndGet(ShuffleSettings.SHUFFLE_STATE_TIMEOUT_SETTING.get(Settings.EMPTY).millis() / 2 + 1);
        service.reapExpired();
        assertThat(service.activeStates(), equalTo(1));
        assertThat(service.timeouts(), equalTo(1L));
        assertTrue(service.hasTombstone("old"));
        expectThrows(ShuffleRejectedException.class,
            () -> service.onPartial(request("old", shard(1), 1, "coord", partial(randomShardData(10), 5))));

        clock.addAndGet(ShuffleSettings.SHUFFLE_STATE_TIMEOUT_SETTING.get(Settings.EMPTY).millis() + 1);
        service.reapExpired();
        assertThat(service.activeStates(), equalTo(0));
        assertFalse("tombstones expire too", service.hasTombstone("old"));
        assertThat(requestBreakerUsed(breakers), equalTo(0L));
    }

    public void testCoordinatorLeftCleanup() {
        CircuitBreakerService breakers = breakerService("100mb");
        ShuffleService service = service(breakers);
        service.onPartial(request("from_c1", shard(0), 1, "c1", partial(randomShardData(10), 5)));
        service.onPartial(request("from_c2", shard(0), 1, "c2", partial(randomShardData(10), 5)));

        DiscoveryNode c1 = node("c1");
        DiscoveryNode c2 = node("c2");
        ClusterState before = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(c1).add(c2).localNodeId("c2").build()).build();
        ClusterState after = ClusterState.builder(before)
            .nodes(DiscoveryNodes.builder().add(c2).localNodeId("c2").build()).build();
        service.clusterChanged(new ClusterChangedEvent("test", after, before));

        assertThat(service.activeStates(), equalTo(1));
        assertTrue(service.hasTombstone("from_c1"));
        assertFalse(service.hasTombstone("from_c2"));
        service.free("from_c2");
        assertThat(requestBreakerUsed(breakers), equalTo(0L));
    }

    public void testFreeIsIdempotent() {
        CircuitBreakerService breakers = breakerService("100mb");
        ShuffleService service = service(breakers);
        service.onPartial(request("sh", shard(0), 1, "coord", partial(randomShardData(10), 5)));
        int times = randomIntBetween(2, 5);
        for (int i = 0; i < times; i++) {
            service.free("sh");
            service.free("unknown");
        }
        assertThat(service.activeStates(), equalTo(0));
        assertThat(requestBreakerUsed(breakers), equalTo(0L));
    }

    public void testDuplicateDeliveryKeepsOneReservation() throws IOException {
        CircuitBreakerService breakers = breakerService("100mb");
        ShuffleService service = service(breakers);
        StringTerms partial = partial(Collections.singletonMap("alice", 5L), 3);
        service.onPartial(request("sh", shard(0), 1, "coord", partial));
        long once = requestBreakerUsed(breakers);
        service.onPartial(request("sh", shard(0), 1, "coord", partial));
        assertThat(requestBreakerUsed(breakers), equalTo(once));
        FinalizeResponse response = service.onFinalize(new FinalizeRequest("sh", Collections.singletonMap(shard(0), 1)));
        assertThat(((StringTerms) response.result()).getBucketByKey("alice").getDocCount(), equalTo(5L));
    }

    public void testCloseFreesEverything() {
        CircuitBreakerService breakers = breakerService("100mb");
        ShuffleService service = service(breakers);
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            service.onPartial(request("sh" + i, shard(0), 1, "coord", partial(randomShardData(10), 5)));
        }
        service.close();
        assertThat(service.activeStates(), equalTo(0));
        assertThat(requestBreakerUsed(breakers), equalTo(0L));
    }

    public void testMessagesSerialization() throws IOException {
        StringTerms partial = partial(randomShardData(20), 5);
        PartialRequest original = request("sh", shard(2), 3, "coord", partial);
        PartialRequest partialCopy = new PartialRequest(roundTrip(original::writeTo));
        assertThat(partialCopy.shuffleId(), equalTo("sh"));
        assertThat(partialCopy.shardId(), equalTo(shard(2)));
        assertThat(partialCopy.attempt(), equalTo(3));
        assertThat(partialCopy.coordinatorNodeId(), equalTo("coord"));
        assertThat(partialCopy.bucketCount(), equalTo(partial.getBuckets().size()));
        // compare decoded to decoded: a freshly built aggregation and a deserialized one differ in incidental fields
        InternalAggregation expected = decode(ShuffleMessages.serializePartial(partial));
        assertEquals(expected, decode(partialCopy.partial()));

        Map<ShardId, Integer> keep = new HashMap<>();
        keep.put(shard(0), 1);
        keep.put(shard(5), 4);
        FinalizeRequest finalizeCopy = new FinalizeRequest(roundTrip(new FinalizeRequest("sh", keep)::writeTo));
        assertThat(finalizeCopy.keepAttempts(), equalTo(keep));

        FinalizeResponse response = new FinalizeResponse(partial, 4, 10, 1000, 12345);
        FinalizeResponse responseCopy = new FinalizeResponse();
        responseCopy.readFrom(roundTrip(response::writeTo));
        assertEquals(expected, responseCopy.result());
        assertThat(responseCopy.partialsReceived(), equalTo(4));
        assertThat(responseCopy.bucketsReceived(), equalTo(10L));
        assertThat(responseCopy.bytesReceived(), equalTo(1000L));
        assertThat(responseCopy.mergeTimeNanos(), equalTo(12345L));

        assertThat(new FreeRequest(roundTrip(new FreeRequest("sh")::writeTo)).shuffleId(), equalTo("sh"));
    }

    /**
     * Measures heap / serialized size for realistic partials (keyword terms with a sum sub-aggregation) and checks the
     * default {@code search.shuffle.heap_estimate_factor} covers it. The measured ratio is logged; see code-map.md.
     */
    public void testHeapEstimateFactorCoversRealisticPartials() throws IOException {
        double factor = ShuffleSettings.SHUFFLE_HEAP_ESTIMATE_FACTOR_SETTING.get(Settings.EMPTY);
        for (int buckets : new int[] {1_000, 20_000}) {
            Map<String, Long> data = new HashMap<>();
            for (int i = 0; i < buckets; i++) {
                data.put(randomAlphaOfLengthBetween(5, 20) + i, (long) randomIntBetween(1, 1_000_000));
            }
            BytesReference bytes = ShuffleMessages.serializePartial(partial(data, 100));
            InternalAggregation decoded = decode(bytes);
            long heap = RamUsageTester.sizeOf(decoded);
            double ratio = (double) heap / bytes.length();
            logger.info("shuffle heap estimate: buckets [{}] serialized [{}] heap [{}] ratio [{}]", buckets, bytes.length(), heap,
                ratio);
            // tiny partials have a higher ratio (fixed object overhead) but are tiny in absolute terms; only check real sizes
            assertThat("heap/serialized ratio for " + buckets + " buckets", ratio, lessThan(factor));
        }
    }

    private static DiscoveryNode node(String id) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), Collections.emptyMap(), EnumSet.allOf(DiscoveryNode.Role.class),
            Version.CURRENT);
    }

    private interface Writer {
        void writeTo(org.elasticsearch.common.io.stream.StreamOutput out) throws IOException;
    }

    private StreamInput roundTrip(Writer writer) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            writer.writeTo(out);
            return new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        }
    }
}
