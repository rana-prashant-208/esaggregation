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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.shuffle.ShuffleMessages.FinalizeRequest;
import org.elasticsearch.search.shuffle.ShuffleMessages.FinalizeResponse;
import org.elasticsearch.search.shuffle.ShuffleMessages.FreeRequest;
import org.elasticsearch.search.shuffle.ShuffleMessages.PartialRequest;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Reducer side of the shuffle terms aggregation (docs/shuffle/PROPOSAL.md section 6.6).
 * <p>
 * Buffers serialized partial aggregations per {@code (shuffleId, shard, attempt)}, charges them to the REQUEST circuit
 * breaker, and on finalize reduces exactly the attempts the coordinator names. A breaker trip poisons the shuffle: everything
 * is released and every later partial or finalize for it is rejected with {@link ShuffleRejectedException}.
 * <p>
 * State is created by the first partial. Finalized and freed shuffles leave a tombstone for {@code search.shuffle.state_timeout}
 * so that late partials (for example from a failed attempt) are rejected instead of creating new state.
 */
public class ShuffleService implements ClusterStateListener, Closeable {

    private static final Logger logger = LogManager.getLogger(ShuffleService.class);

    /** How often expired states and tombstones are reaped. */
    static final TimeValue REAPER_INTERVAL = TimeValue.timeValueSeconds(10);

    private final ThreadPool threadPool;
    private final CircuitBreakerService circuitBreakerService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final Function<Boolean, ReduceContext> reduceContextFactory;
    private final LongSupplier relativeTimeMillis;

    private volatile TimeValue stateTimeout;
    private volatile double heapEstimateFactor;

    private final ConcurrentHashMap<String, ShuffleState> states = new ConcurrentHashMap<>();
    /** shuffleId to the relative time it was finalized or freed. */
    private final ConcurrentHashMap<String, Long> tombstones = new ConcurrentHashMap<>();

    private final AtomicLong partialsAccepted = new AtomicLong();
    private final AtomicLong bytesAccepted = new AtomicLong();
    private final AtomicLong finalizations = new AtomicLong();
    private final AtomicLong rejections = new AtomicLong();
    private final AtomicLong timeouts = new AtomicLong();

    private volatile Scheduler.Cancellable reaper;

    public ShuffleService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool,
                          CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                          Function<Boolean, ReduceContext> reduceContextFactory) {
        this(settings, clusterSettings, threadPool, circuitBreakerService, namedWriteableRegistry, reduceContextFactory,
            threadPool::relativeTimeInMillis);
    }

    /** Visible for tests that need to control the clock. */
    ShuffleService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool,
                   CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                   Function<Boolean, ReduceContext> reduceContextFactory, LongSupplier relativeTimeMillis) {
        this.threadPool = threadPool;
        this.circuitBreakerService = circuitBreakerService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.reduceContextFactory = reduceContextFactory;
        this.relativeTimeMillis = relativeTimeMillis;
        this.stateTimeout = ShuffleSettings.SHUFFLE_STATE_TIMEOUT_SETTING.get(settings);
        this.heapEstimateFactor = ShuffleSettings.SHUFFLE_HEAP_ESTIMATE_FACTOR_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(ShuffleSettings.SHUFFLE_STATE_TIMEOUT_SETTING, v -> this.stateTimeout = v);
        clusterSettings.addSettingsUpdateConsumer(ShuffleSettings.SHUFFLE_HEAP_ESTIMATE_FACTOR_SETTING,
            v -> this.heapEstimateFactor = v);
    }

    /** Registers the reducer-side transport handlers. */
    public void registerRequestHandlers(TransportService transportService) {
        transportService.registerRequestHandler(ShuffleMessages.PARTIAL_ACTION_NAME, ThreadPool.Names.SHUFFLE, PartialRequest::new,
            new Handler<PartialRequest>() {
                @Override
                TransportResponse handle(PartialRequest request) {
                    onPartial(request);
                    return TransportResponse.Empty.INSTANCE;
                }
            });
        transportService.registerRequestHandler(ShuffleMessages.FINALIZE_ACTION_NAME, ThreadPool.Names.SHUFFLE,
            FinalizeRequest::new, new Handler<FinalizeRequest>() {
                @Override
                TransportResponse handle(FinalizeRequest request) throws IOException {
                    return onFinalize(request);
                }
            });
        transportService.registerRequestHandler(ShuffleMessages.FREE_ACTION_NAME, ThreadPool.Names.SAME, FreeRequest::new,
            new Handler<FreeRequest>() {
                @Override
                TransportResponse handle(FreeRequest request) {
                    free(request.shuffleId());
                    return TransportResponse.Empty.INSTANCE;
                }
            });
    }

    /** Starts the periodic reaper of expired state. */
    public synchronized void start() {
        if (reaper == null) {
            reaper = threadPool.scheduleWithFixedDelay(this::reapExpired, REAPER_INTERVAL, ThreadPool.Names.GENERIC);
        }
    }

    @Override
    public synchronized void close() {
        if (reaper != null) {
            reaper.cancel();
            reaper = null;
        }
        for (String shuffleId : new ArrayList<>(states.keySet())) {
            free(shuffleId);
        }
    }

    /**
     * Stores one partial. Throws {@link ShuffleRejectedException} if the shuffle is poisoned, already finalized or freed, or the
     * breaker trips (which poisons it).
     */
    public void onPartial(PartialRequest request) {
        final String shuffleId = request.shuffleId();
        if (tombstones.containsKey(shuffleId)) {
            throw reject(shuffleId, "it was already finalized or freed", null);
        }
        final ShuffleState state = states.computeIfAbsent(shuffleId,
            id -> new ShuffleState(id, request.aggName(), request.coordinatorNodeId(), relativeTimeMillis.getAsLong()));
        synchronized (state) {
            if (state.closed || tombstones.containsKey(shuffleId)) {
                // raced with free/finalize; make sure we did not resurrect it
                states.remove(shuffleId, state);
                throw reject(shuffleId, "it was already finalized or freed", null);
            }
            if (state.poisoned) {
                throw reject(shuffleId, "it was poisoned by an earlier rejection", null);
            }
            if (state.aggName.equals(request.aggName()) == false) {
                throw new IllegalArgumentException("shuffle [" + shuffleId + "] is for aggregation [" + state.aggName
                    + "], got a partial for [" + request.aggName() + "]");
            }
            final long estimate = estimateHeap(request.partial());
            try {
                breaker().addEstimateBytesAndMaybeBreak(estimate, "<shuffle_partial[" + shuffleId + "]>");
            } catch (CircuitBreakingException e) {
                poison(state);
                throw reject(shuffleId, "the request circuit breaker tripped", e);
            }
            Map<Integer, Partial> attempts = state.partials.computeIfAbsent(request.shardId(), s -> new HashMap<>());
            Partial previous = attempts.put(request.attempt(),
                new Partial(request.partition(), request.bucketCount(), request.partial(), estimate));
            state.reservedBytes += estimate;
            state.partialsReceived++;
            partialsAccepted.incrementAndGet();
            bytesAccepted.addAndGet(request.partial().length());
            state.bytesReceived += request.partial().length();
            if (previous != null) {
                // a duplicate delivery of the same attempt; keep the latest and release the old reservation
                release(state, previous.reservedBytes);
            }
        }
    }

    /**
     * Reduces the partials of the named attempts and frees the shuffle, whatever the outcome.
     */
    public FinalizeResponse onFinalize(FinalizeRequest request) throws IOException {
        final String shuffleId = request.shuffleId();
        final Map<ShardId, Integer> keep = request.keepAttempts();
        final ShuffleState state = states.get(shuffleId);
        if (state == null) {
            boolean finished = tombstones.containsKey(shuffleId);
            free(shuffleId);
            if (finished) {
                throw new IllegalStateException("shuffle [" + shuffleId + "] was already finalized or freed");
            }
            if (keep.isEmpty()) {
                // every shard failed or was skipped: nothing was ever sent to this reducer
                return new FinalizeResponse(null, 0, 0, 0, 0);
            }
            throw new IllegalStateException("no shuffle state for [" + shuffleId + "], expected partials from shards " + keep.keySet());
        }
        try {
            synchronized (state) {
                if (state.closed) {
                    throw new IllegalStateException("shuffle [" + shuffleId + "] was already finalized or freed");
                }
                if (state.poisoned) {
                    throw reject(shuffleId, "it was poisoned by an earlier rejection", null);
                }
                final long start = System.nanoTime();
                List<Partial> kept = new ArrayList<>(keep.size());
                for (Map.Entry<ShardId, Integer> entry : keep.entrySet()) {
                    Map<Integer, Partial> attempts = state.partials.get(entry.getKey());
                    Partial partial = attempts == null ? null : attempts.get(entry.getValue());
                    if (partial == null) {
                        throw new IllegalStateException("shuffle [" + shuffleId + "] has no partial from shard " + entry.getKey()
                            + " attempt [" + entry.getValue() + "], received attempts "
                            + (attempts == null ? "[]" : attempts.keySet()));
                    }
                    kept.add(partial);
                }
                long buckets = 0;
                List<InternalAggregations> decoded = new ArrayList<>(kept.size());
                for (Partial partial : kept) {
                    buckets += partial.bucketCount;
                    List<InternalAggregation> single = new ArrayList<>(1);
                    single.add(decode(partial.bytes));
                    decoded.add(new InternalAggregations(single));
                }
                InternalAggregation result = null;
                if (decoded.isEmpty() == false) {
                    InternalAggregations reduced = InternalAggregations.reduce(decoded, reduceContextFactory.apply(true));
                    result = (InternalAggregation) reduced.get(state.aggName);
                }
                finalizations.incrementAndGet();
                return new FinalizeResponse(result, state.partialsReceived, buckets, state.bytesReceived,
                    System.nanoTime() - start);
            }
        } finally {
            free(shuffleId);
        }
    }

    /** Frees a shuffle's state, if any, and leaves a tombstone. Idempotent. */
    public void free(String shuffleId) {
        tombstones.put(shuffleId, relativeTimeMillis.getAsLong());
        ShuffleState state = states.remove(shuffleId);
        if (state != null) {
            synchronized (state) {
                closeState(state);
            }
        }
    }

    /** Frees every state older than {@code search.shuffle.state_timeout}, and drops tombstones of the same age. */
    void reapExpired() {
        final long now = relativeTimeMillis.getAsLong();
        final long timeoutMillis = stateTimeout.millis();
        for (ShuffleState state : states.values()) {
            if (now - state.createdAtMillis > timeoutMillis) {
                logger.debug("freeing shuffle [{}] after timeout [{}]", state.shuffleId, stateTimeout);
                timeouts.incrementAndGet();
                free(state.shuffleId);
            }
        }
        tombstones.entrySet().removeIf(e -> now - e.getValue() > timeoutMillis);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved() == false) {
            return;
        }
        Set<String> removed = new HashSet<>();
        for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
            removed.add(node.getId());
        }
        freeByCoordinators(removed);
    }

    /** Frees every shuffle coordinated by one of the given nodes. */
    void freeByCoordinators(Set<String> coordinatorNodeIds) {
        for (ShuffleState state : states.values()) {
            if (coordinatorNodeIds.contains(state.coordinatorNodeId)) {
                logger.debug("freeing shuffle [{}] because coordinator [{}] left the cluster", state.shuffleId,
                    state.coordinatorNodeId);
                free(state.shuffleId);
            }
        }
    }

    /** Number of live (not finalized, not freed) shuffle states. */
    public int activeStates() {
        return states.size();
    }

    /** Bytes currently reserved on the request breaker for all shuffle states. */
    public long reservedBytes() {
        long total = 0;
        for (ShuffleState state : states.values()) {
            synchronized (state) {
                total += state.reservedBytes;
            }
        }
        return total;
    }

    /** Total partials accepted since the node started. */
    public long partialsAccepted() {
        return partialsAccepted.get();
    }

    /** Total serialized bytes of partials accepted since the node started. */
    public long bytesAccepted() {
        return bytesAccepted.get();
    }

    /** Successful finalizations since the node started. */
    public long finalizations() {
        return finalizations.get();
    }

    public long rejections() {
        return rejections.get();
    }

    public long timeouts() {
        return timeouts.get();
    }

    boolean hasTombstone(String shuffleId) {
        return tombstones.containsKey(shuffleId);
    }

    private long estimateHeap(BytesReference bytes) {
        return (long) Math.ceil(bytes.length() * heapEstimateFactor);
    }

    private InternalAggregation decode(BytesReference bytes) throws IOException {
        try (StreamInput in = new NamedWriteableAwareStreamInput(bytes.streamInput(), namedWriteableRegistry)) {
            return in.readNamedWriteable(InternalAggregation.class);
        }
    }

    private CircuitBreaker breaker() {
        return circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
    }

    /** Must hold the state's lock. */
    private void poison(ShuffleState state) {
        state.poisoned = true;
        state.partials.clear();
        release(state, state.reservedBytes);
    }

    /** Must hold the state's lock. */
    private void closeState(ShuffleState state) {
        state.closed = true;
        state.partials.clear();
        release(state, state.reservedBytes);
    }

    /** Must hold the state's lock. */
    private void release(ShuffleState state, long bytes) {
        if (bytes > 0) {
            breaker().addWithoutBreaking(-bytes);
            state.reservedBytes -= bytes;
        }
    }

    private ShuffleRejectedException reject(String shuffleId, String reason, Exception cause) {
        rejections.incrementAndGet();
        return new ShuffleRejectedException("shuffle [{}] rejected: {}", cause, shuffleId, reason);
    }

    private static final class ShuffleState {
        final String shuffleId;
        final String aggName;
        final String coordinatorNodeId;
        final long createdAtMillis;
        final Map<ShardId, Map<Integer, Partial>> partials = new HashMap<>();
        long reservedBytes;
        int partialsReceived;
        long bytesReceived;
        boolean poisoned;
        boolean closed;

        ShuffleState(String shuffleId, String aggName, String coordinatorNodeId, long createdAtMillis) {
            this.shuffleId = shuffleId;
            this.aggName = aggName;
            this.coordinatorNodeId = coordinatorNodeId;
            this.createdAtMillis = createdAtMillis;
        }
    }

    private static final class Partial {
        final int partition;
        final int bucketCount;
        final BytesReference bytes;
        final long reservedBytes;

        Partial(int partition, int bucketCount, BytesReference bytes, long reservedBytes) {
            this.partition = partition;
            this.bucketCount = bucketCount;
            this.bytes = bytes;
            this.reservedBytes = reservedBytes;
        }
    }

    /** Sends the handler's response, or its failure, on the channel. */
    private abstract static class Handler<R extends TransportRequest> implements TransportRequestHandler<R> {
        abstract TransportResponse handle(R request) throws Exception;

        @Override
        public final void messageReceived(R request, TransportChannel channel) throws Exception {
            final TransportResponse response;
            try {
                response = handle(request);
            } catch (Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.warn(new ParameterizedMessage("failed to send shuffle failure response"), inner);
                }
                return;
            }
            channel.sendResponse(response);
        }
    }
}
