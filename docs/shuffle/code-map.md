# Shuffle aggregation — code map (ES 6.5.4)

Verified against tag `v6.5.4` (branch `shuffle-agg`). Base path: `server/src/main/java/org/elasticsearch/`.
Line numbers are from the stock checkout before fork edits and may shift by a few lines.

## Build notes (not in the proposal)

- **Version:** this fork is based on **6.5.4**, not 6.8. Every path in PROPOSAL.md section 6 exists in 6.5.4.
- **JDK:** compiler JDK 11 (`buildSrc/src/main/resources/minimumCompilerVersion`), runtime JDK 8 or later. Gradle 4.10 wrapper.
- **jcenter is shut down.** The build no longer resolves. `buildSrc/build.gradle` and `BuildPlugin.groovy` (`configureRepositories`)
  now use Maven Central and the Gradle plugin portal, with `https://repo.grails.org/grails/core` (a jcenter mirror) last,
  for artifacts that exist nowhere else (`nebula-core:3.0.0`, `p4java-jfrog`).
- Behind the local proxy, Java needs `-Dhttps.proxyHost/Port`. See `../gradle-env.sh` (outside the repo).

## Milestone status

| Milestone | Status |
|---|---|
| M0 | Done (this file). |
| M1 | Done. Tests: `ShuffleConfigTests`, `ShufflePlanTests`, `ShuffleRequestValidatorTests`, `ShuffleSettingsTests` (37 tests); `ShuffleGateIT` (2). Regression: `TermsTests`, `bucket.terms.*Tests`, `action.search.*Tests`, `common.settings.*Tests`, `SearchModuleTests`. `:server:precommit` passes. |
| M2 | Done. `ShuffleServiceTests` (16 tests, also run with `-Dtests.iters=5`); `ShuffleTransportIT` (real nodes, local and remote delivery). Regression: `ExceptionSerializationTests`, `ElasticsearchExceptionTests`, `threadpool.*`, `node.*`, `common.settings.*`, `action.search.*`, `bucket.terms.*`, `threadpool.*IT`, `bucket.*TermsIT`. `:server:precommit` passes. |
| M3 | Done. `ShufflePartitionerTests` (golden values, split), `ShardShuffleSenderTests` (every reducer gets a partial including empty ones, placeholder substitution, batched reduce, rendering refused). `ShuffleShardIT` drives real shards with plan headers set on the client: reducer totals equal a normal terms agg exactly, the request cache is bypassed, the field type is checked on the shards (400), and a missing attempt header fails. Also run with `-Dtests.iters=4`. |
| M4 | Done. `ShuffleMergeTests` (hand-computed, every order, `sum_other_doc_count`, null/unmapped/empty inputs). `ShuffleCoordinatorTests` (reducer selection, single-shard and can-match fallback, attempt headers inside an already-headered context, rejection not retryable). `ShuffleSearchIT` runs real `"shuffle"` searches: 10 random rounds per run (field keyword/long, size, all orders, min_doc_count, 1-5 reducers, QTF and DFS) equal a `shard_size: 10000` terms agg exactly. It also covers single-shard fallback, can-match-skipped shards excluded, other aggs and hits untouched, and no state or breaker leaks. Run with `-Dtests.iters=4`. |
| M5 | Done (tests only; no production code changed). `ShuffleCorrectnessIT`, also run with `-Dtests.iters=6`:<br>• **Canonical fixture:** the section 10 table is indexed with explicit routing per shard (verified per shard). The shuffled response is exactly carol 150, bob 95, alice 90, `sum_other_doc_count` 325, error 0. For documentation, stock ES with `shard_size: 3` returns carol 150, bob 95, **dave 80**: it misses alice, with `doc_count_error_upper_bound` 115.<br>• **Retry variant:** `MockTransportService` lets one partial of S3's attempt 1 through and fails the rest, so the coordinator retries S3 on its replica with attempt 2. The response is unchanged (alice 90, not 125).<br>• **Randomized equivalence:** random key count (1/5/50/500), distribution (uniform/skewed/one heavy key), 2-8 shards, keyword/long/date field, size, 7 orders, min_doc_count, 1-6 reducers, QTF/DFS. Metric sub-aggs are sum, min, max, avg, value_count, stats, extended_stats, cardinality and percentiles, plus a `bucket_script` parent pipeline. All are exact except percentiles (t-digest merge is order-dependent; checked within 5% of the bucket's range). `bucket_script` matching proves it ran once. `bucket_selector` is excluded (D5).<br>• **Non-shuffle regression:** existing terms tests pass unchanged (`bucket.terms.*Tests`, `bucket.*TermsIT`). |
| M6 | Done (tests only; no production code changed). `ShuffleFailureIT`, also run with `-Dtests.iters=5` (35 runs). Topology: 1 master, 3 data, 2 coordinating-only nodes; one coordinating-only node is the coordinator, and the other is a reducer holding no shards. Failures are injected with `MockTransportService` send behaviors. Every test ends by asserting no `ShuffleState` and request breaker = 0 on every surviving node.<br>• **Shard fails mid-send:** a random shard's attempt 1 fails after 0-3 partials and is retried on the replica. The result is exact.<br>• **Shard fails on all copies:** with `allow_partial_search_results`, the response has 1 failed shard and equals a reference over the other shards (`preference: _shards:...`). Without it, the search fails.<br>• **Reducer dies before finalize:** finalize to it is held, the node is stopped, and the search fails ("shuffle finalize failed").<br>• **Reducer unreachable during the shard phase:** every shard fails on every copy, the search fails, and the reducer never accepted a partial.<br>• **Coordinator dies:** it neither finalizes nor frees. The reducers free its states through the cluster-state listener, with 0 timeouts.<br>• **Cancellation while shards send:** observed outcome is that the search **still completes**, because the 6.5.4 coordinator does not abort on cancel. No leaks either way.<br>• **Breaker trip:** `heap_estimate_factor` is set to 1e12. The search fails with `ShuffleRejectedException` (HTTP 429), there are **0 retries on another copy**, and reducer `rejections() ≥ 1`. |
| M7 | Done. `ShuffleProfileTests`, `ShuffleStatsTests` (serialization, JSON, REST handler routes and node-id parsing) and `ShuffleObservabilityIT` (profile entries per reducer with sane values; stats totals match `searches × shards × reducers` partials and `searches × reducers` finalizations; a breaker trip shows up as a rejection). |
| M8 item 2 (option A) | Done: `terms` sub-aggregations under a shuffled terms (D36). `ShuffleRequestValidatorTests` (nested allowed, rules at every level), `ShuffleNestedIT` (random 3-level trees, every level equal to a stock tree with `shard_size` 100,000 at every level; `-Dtests.iters=3`). |
| M8 other items | Not started (each item is its own proposal update). |

Run one test class with: `./gradlew :server:test -Dtests.class=<pattern>` (one pattern only; `--tests` cannot be repeated), and ITs with `:server:integTest`.

## Differences from the proposal and the adjustments made

| # | Proposal says | 6.5.4 reality | Adjustment |
|---|---|---|---|
| D1 | Target 6.8 | 6.5.4 | None needed so far; all classes exist. |
| D2 | Request validation rejects non-keyword/integral fields at the coordinator (4.1) | The coordinator has no mapper service in 6.5.4, so it cannot see field types. | The coordinator rejects scripts, a missing `field`, and `value_type` hints other than string/long/date. The **mapped field type** check moves to the shard in M3 (`TermsAggregatorFactory`, where `config.fieldContext().fieldType()` is known). A shard-side `IllegalArgumentException` fails every shard, so the search returns 400. |
| D3 | `shuffle` is parsed at the REST layer and is not serialized | Correct, and the request reaches `TransportSearchAction` without serialization only when it comes in over REST/NodeClient. | A **transport client** request loses `shuffle` silently and runs as a normal terms aggregation. Documented, not fixed in v1. |
| D4 | "If `shard_size`/`shard_min_doc_count` are set explicitly, return an error" | The builder can't tell an explicit default from an unset value: `shardSize` is -1 when unset, `shardMinDocCount` is 0. | Error when `shard_size != -1` or `shard_min_doc_count != 0`. An explicit `shard_min_doc_count: 0` is accepted (it's harmless). |
| D5 | Parent pipelines (`bucket_script`, `bucket_selector`) run on the reducer (4.2) | `bucket_selector` runs after the cut to `size` inside `InternalAggregation.reduce` (`InternalAggregation.java:134-142`). If each reducer runs it, a reducer's top-`size` is filtered *before* the global cut, and the result differs from stock ES. | **Decided (2026-11-19): parent pipelines run on the reducers, as the proposal says.** Partials keep their pipelines, and each reducer's final reduce runs them; the coordinator merge (6.9) does not run them again. **Documented semantic difference:** with `bucket_selector`, a reducer filters its own top-`size` *before* the coordinator's global cut, so the buckets returned can differ from stock ES (stock filters the global top-`size`). In practice shuffle may return *more* surviving buckets, up to `size`, than stock. `bucket_script` is unaffected. `bucket_sort` and all other pipelines stay rejected. M5's randomized equivalence test must leave `bucket_selector` out of the comparison, or compare against "filter, then cut" semantics. |
| D6 | Retry headers: "stash and restore the context around each shard send" | `ContextRestoreResponseHandler` restores the **send-time** context in the response callbacks (`TransportService.sendRequestInternal`, ~636). So a retry that `onShardFailure` triggers already carries the failed attempt's `_shuffle_attempt`, and `putHeader` throws on duplicate keys (`ThreadContext.java:431`). | In M4, capture `StoredContext base = threadContext.newStoredContext(false)` before any shuffle header is set. For every send: `try (StoredContext ignored = tc.stashContext()) { base.restore(); putHeader(PLAN); putHeader(ATTEMPT); executePhaseOnShard(...); }`. |
| D7 | DFS variant: hook `getNextPhase` | In DFS mode the shard query runs in `DfsQueryPhase` through `QUERY_ID_ACTION_NAME` with **no retry on another copy**, using a different `SearchService.executeQueryPhase(QuerySearchRequest, ...)` overload. | M3/M4 must hook both `SearchService.executeQueryPhase` overloads. Attempts are always 1 in the DFS query phase. |
| D8 | Cancellation hook on `SearchTask` | `SearchTask` has no listener API. Coordinator phases never check cancellation; only shard-side `QueryPhase` and `sendChildRequest` do. | Add a listener list to `SearchTask` and fire it from a `CancellableTask.onCancelled()` override (M4). |
| D9 | `canCache` returns false with the plan header | `IndicesService.canCache(ShardSearchRequest, SearchContext)` (1157) has no ThreadContext access. | Add the check in `SearchService.loadOrExecuteQueryPhase` (324-332), which owns `threadPool`, before calling `canCache` (M3). |
| D10 | `ShuffleService` created "next to SearchService" | `Node` start/stop only iterates `pluginLifecycleComponents`. | Construct it after `newSearchService` (~535) and bind it in Guice (~573). Start its timers lazily, or add explicit start/close calls (M2). |
| D11 | Placeholder `doReduce` returns one input | `InternalAggregation.reduce` runs **parent** pipelines on the final reduce. | The placeholder carries an empty `pipelineAggregators` list (M3). |

### M2 decisions (differences from proposal 6.6)

| # | Proposal says | Implemented | Why |
|---|---|---|---|
| D12 | The partial request carries `InternalAggregation partial` as a NamedWriteable | It carries `BytesReference partial`: the sender serializes with `ShuffleMessages.serializePartial`, and the reducer buffers the bytes and decodes them only at finalize. | (a) The breaker estimate uses the exact serialized size even for **local** delivery, which skips transport serialization. (b) `InternalTerms.doReduce` mutates its inputs, and decoding fresh objects at finalize makes that harmless. (c) Discarded attempts are never decoded. |
| D13 | "Partial arrives after finalize or free: no state exists, so it is rejected" | State is created by the first partial, so "no state" can't be the signal. Finalize and free leave a **tombstone** for `state_timeout`. A partial for a tombstoned id is rejected with `ShuffleRejectedException`, and so is one that races with free (re-checked under the state lock). | Without tombstones, a late partial would resurrect a state that leaks until timeout. |
| D14 | The finalize error paths leave cleanup to the coordinator's `free` | Finalize is **terminal**: it frees the state whatever the outcome (success, missing shard, poisoned). | It removes one round trip from every failure path. A `free` from the coordinator is still harmless (idempotent). |
| D15 | Finalize with no state returns an error | It returns an empty response (`result == null`) when `keepAttempts` is empty, because every shard failed or was skipped. It is still an error when shards were expected. | All shards failing with partial results allowed is a legal outcome. |
| D16 | `ShuffleRejectedException` | Registered in `ElasticsearchException` with fork-local id **10_000**, `Version.V_6_5_0`. Status 429. | It has to keep its type across the wire so the coordinator can skip retries. The high id avoids clashing with upstream ids on rebase. |
| D17 | `heap_estimate_factor` "to be measured" | Default **8.0**. Measured with `RamUsageTester` on keyword terms partials with a sum sub-aggregation: 6.9× at 20k buckets, 7.1× at 1k, 8.0× at 10. | The proposal's provisional 3.0 would under-count heap by more than 2×. |

M2 wiring:
- `search/shuffle/ShuffleService.java` is created in `Node` after `newSearchService`, with `searchService::createReduceContext`. Handlers are registered on the `TransportService`, and it is added with `clusterService.addListener`. It is bound in Guice, `start()`ed next to `SearchService` (which schedules a reaper every 10s on GENERIC), and closed next to it.
- `search/shuffle/ShuffleMessages.java`: action names `internal:search/shuffle/{partial,finalize,free}`. Partial and finalize run on the new fixed `shuffle` pool (`ThreadPool.Names.SHUFFLE`, size = processors, queue 1000). Free runs on SAME.
- Node-stats counters for M7 already exist: `activeStates()`, `reservedBytes()`, `rejections()`, `timeouts()`.
- D5 is decided: partials carry their parent pipelines, and the reducer runs them in its final reduce. This is already how M2/M3 behave, so no code change is needed.

### M3 decisions and wiring

| # | Topic | Implemented |
|---|---|---|
| D18 | How the terms factory learns about the plan | `SearchContext` has a new non-abstract `shufflePlan()` / `shufflePlan(ShufflePlan)` (FilteredSearchContext delegates). `SearchService.createContext` sets it from the `_shuffle_plan` header **before `parseSource`**. `TermsAggregatorFactory.isShuffled()` is `plan != null && parent == null && plan.aggName().equals(name)`, so only the top-level aggregation with that name is affected. Mockito mocks of `SearchContext` return null, so aggregation unit tests are unaffected. |
| D19 | Partition-to-reducer mapping | `ShufflePartitioner.split(result, numPartitions, numGroups, groupOfPartition)` groups buckets **per reducer**, so a reducer gets exactly one partial per shard even when partitions outnumber reducers (M8). This matters because the reducer stores partials by `(shard, attempt)`. `PartialRequest.partition` carries the reducer index. |
| D20 | The shard's `otherDocCount` | Normally 0, because shards collect every bucket. If it isn't, partition 0 carries it, so the sum is preserved. |
| D21 | Placeholder contents | `InternalShufflePlaceholder` carries name, `shuffleId`, order, `requiredSize`, `minDocCount` and metadata, and **no pipeline aggregators**. The last three let M4 build an empty result when no reducer returns one. `doReduce` rejects mixing placeholders from different shuffles. |
| D22 | Failure after the query phase | If the send fails, `SearchService` frees the shard's search context: the coordinator never sees the result, so it can't free it. `ShuffleRejectedException` is unwrapped from `RemoteTransportException` before it fails the shard. |
| D23 | Parent task | Partials go out with `sendChildRequest(…, task, …)`, so cancelling the search also cancels in-flight sends. |

Hooks:
- `SearchService.executeQueryPhase(ShardSearchRequest, …)` calls `maybeShuffle(result, task, listener)` after the synchronous query phase.
- `executeQueryPhase(QuerySearchRequest, …)` (DFS, D7) wraps its listener the same way.
- `maybeShuffle` reads the plan and attempt from the ThreadContext (preserved across the SEARCH fork) and calls `ShardShuffleSender.send`.
- `ShardShuffleSender` is created in `Node` with the `TransportService` and `ClusterService` and handed over through `SearchService.setShardShuffleSender`. It is null in tests that build `SearchService` without transport, and a plan then fails the shard.
- Request cache: `loadOrExecuteQueryPhase` uses `canCache = context.shufflePlan() == null && indicesService.canCache(...)` (D9).
- Field type check (D2, shard half): `TermsAggregatorFactory.validateShuffleField()` allows keyword, date, and number types long/integer/short/byte. It rejects scripts. Its `IllegalArgumentException` gives a 400.
- Partition function: `ShufflePartitioner.SHUFFLE_HASH_SEED = 0x5f3759df_2b7e1516L`. `partition(BytesRef)` is `floorMod(MurmurHash3.hash128(…).h1, n)`; `partition(long)` is `floorMod(BitMixer.mix64(v ^ seed), n)`. Golden values are pinned in `ShufflePartitionerTests`.

### M4 decisions and wiring

| # | Topic | Implemented |
|---|---|---|
| D24 | Where planning happens | In `TransportSearchAction.searchAsyncAction`, in the non-prefilter branch. The can-match phase calls this branch again with its filtered iterators, so the plan always sees the **post-can-match** shard set. `ShuffleCoordinator.create` returns null (normal path) when no shuffle is requested or ≤ 1 shard executes. Validation (M1) already ran in `executeSearch`. |
| D25 | Coordinator state | `action/search/ShuffleCoordinator` holds the plan, per-`ShardId` attempt counters, the first reducer rejection, and finalize/free. It is attached with `AbstractSearchAsyncAction.setShuffle` before `start()`, so **no constructor changes** and existing tests/mocks are unaffected. |
| D26 | Headers per attempt (D6 implemented) | The new hook `InitialSearchPhase.prepareShardExecution(shardIt)` wraps each synchronous `executePhaseOnShard`, retries included. `ShuffleCoordinator.withShardHeaders` stashes the context, restores the **original request context captured at planning time**, puts `_shuffle_plan` and the incremented `_shuffle_attempt`, and restores the caller's context after the send. DFS query phase: `SearchPhaseContext.withShuffleHeaders(shardId)` (default no-op) reuses the DFS-phase attempt. |
| D27 | No retry on rejection | The new hook `InitialSearchPhase.isRetryableOnAnotherCopy(e)` returns false for `ShuffleRejectedException`. The remaining copies are counted as done (`totalOps += remaining`) and never contacted. The failure is recorded on the coordinator. `ShuffleFinalizePhase` then fails the search even if partial results are allowed, because a poisoned reducer cannot be exact. |
| D28 | Which attempts finalize keeps | Derived from the **successful query results** (`SearchPhaseResults.getSuccessfulResults()`), mapped to the shard's current attempt. Shard retries are sequential, so the current attempt of a successful shard is the one that succeeded. Failed shards (partial results) and can-match-skipped shards are therefore excluded. This works the same for QTF and DFS, where a shard can succeed in DFS and fail in the query phase. |
| D29 | Substitution hook (open question 2) | `ShuffleFinalizePhase` hands the fetch phase a `SubstitutingResults` wrapper whose `reduce()` calls the real reduce, then rebuilds `ReducedQueryPhase` with the placeholder replaced by `ShuffleMerge.merge(...)`. This needs **no change to `SearchPhaseController`**. Substitution happens after sibling pipelines, as allowed by v1 (moving it earlier is M8 item 8). |
| D30 | Header leakage into later phases | The finalize phase and everything after it (fetch, expand, response) run under `withCleanContext()`. Otherwise fetch or expand requests sent from a shard callback would carry `_shuffle_plan`, and an expand sub-search could be mistaken for a shuffle. |
| D31 | Freeing on failure / cancellation (D8 resolved) | The search listener is wrapped (`ShuffleCoordinator.wrapListener`): **any** failure sends `free` to all reducers, once. Cancelling a `SearchTask` cancels its child requests (shard queries, partial sends, finalize), so the search fails and the wrapped listener frees. **No `SearchTask` change was needed.** Reducer timeouts and coordinator-left cleanup remain the backstop. |
| D32 | `search.max_buckets` on the coordinator merge | Not enforced in M4. The merged result has at most `size` buckets, and each reducer enforces `max_buckets` in its own final reduce. Open question 3 is answered on the reducer side only. Revisit in M7 if needed. |

Files: `action/search/{ShuffleCoordinator,ShuffleFinalizePhase}.java`; `search/aggregations/bucket/terms/ShuffleMerge.java`. Hooks touched: `InitialSearchPhase` (2 hooks + no-retry accounting), `AbstractSearchAsyncAction` (shuffle field + overrides + failure recording), `SearchPhaseContext` (default `withShuffleHeaders`), `DfsQueryPhase` (header around each send), `SearchQueryThenFetchAsyncAction` / `SearchDfsQueryThenFetchAsyncAction` (`getNextPhase`), `TransportSearchAction` (plan + listener wrap).

Testing note: `ESIntegTestCase` clients pick a **random search type**. Tests that need can-match must set `QUERY_THEN_FETCH` explicitly, because DFS never pre-filters.

### M7 decisions

| # | Topic | Implemented |
|---|---|---|
| D33 | Profile API output | No new profile classes and **no wire-format change**. `ShuffleFinalizePhase.withShuffleProfile` adds one entry per reducer to `SearchProfileShardResults`, with id `[shuffle][<reducer node id>]`. Each entry holds one aggregation profile result: type `shuffle_reducer`, description `reducer [id] shuffle [id] partials [n] buckets [n] bytes [n]`, breakdown `merge` (reducer decode+reduce time) and `transport_and_queue` (rest of the coordinator-measured finalize round trip). `time_in_nanos` is therefore the finalize round trip. Counts go in the description because `ProfileResult` sums every breakdown value into its time. Old transport clients and the stock renderer handle these entries; tools that parse shard ids as `[node][index][shard]` will see an unfamiliar id. |
| D34 | Node stats | **Not a new `_nodes/stats` section.** Its positional wire format is exchanged between nodes, and an extra section would break mixed stock/fork clusters during a rolling upgrade. Instead there is a new nodes action `cluster:monitor/nodes/shuffle_stats` with REST endpoints `GET /_shuffle/stats` and `GET /_shuffle/{nodeId}/stats`. Per node it reports `active_states`, `reserved_bytes`, `partials_accepted`, `bytes_accepted`, `finalizations`, `rejections` and `timeouts`. Stock nodes in a mixed cluster show up as node failures in that response; nothing else is affected. The path is not under `_nodes/` because `_nodes/{nodeId}/stats` would capture it. |
| D35 | Finalize round trip | `ShuffleCoordinator.finalizeAll` now returns `ReducerResult(nodeId, response, roundTripNanos)` per reducer, timed around each finalize send. |

Not verified over real HTTP: the server module's test cluster has no HTTP transport. The REST handler is unit-tested (routes, node-id parsing, action dispatch), and the action itself is tested in `ShuffleObservabilityIT`.

### Nested terms (M8 item 2, option A)

| # | Topic | Implemented |
|---|---|---|
| D36 | Bucket sub-aggregations | `terms` is allowed at any depth under the shuffled top-level terms, each level with the same sub-aggregation rules (metrics, `terms`, `bucket_script`, `bucket_selector`); `shard_size` / `shard_min_doc_count` on a nested level are rejected; a nested `shuffle` is still rejected (only the top level is partitioned). Other bucket aggregations stay rejected. Only the **top-level key** is partitioned: a reducer owns a top-level key together with its whole subtree, so it merges every shard's complete subtree and every level is exact. Shard side: `TermsAggregatorFactory.isUnderShuffled()` (walks `getParent()` to the root and asks it `isShuffled()`) makes nested levels collect every bucket too, otherwise shards would cut nested lists short. Reducer final reduce, coordinator merge and wire format are unchanged: nested buckets already travel inside their parent bucket. `bucket_selector` at a nested level behaves exactly like stock (the reducer holds the complete parent bucket); only top-level `bucket_selector` keeps the D5 difference. Balance follows the top-level keys: a few top keys or one key with a huge subtree concentrates work (option B, per-path partitioning, would address that). |

## Item-by-item map (proposal section 6)

### 6.1 REST and parsing — done in M1
- `search/aggregations/bucket/terms/TermsAggregationBuilder.java`
  - Parser: `ObjectParser<TermsAggregationBuilder, Void> PARSER`. `shuffle` is declared with `declareObject`.
  - Clone constructor `TermsAggregationBuilder(TermsAggregationBuilder clone, Builder, Map)` is used by `shallowCopy`, which `rewrite()` calls. It **must** copy `shuffle`, and it does.
  - `innerWriteTo(StreamOutput)` / `TermsAggregationBuilder(StreamInput)` are **unchanged**.
  - `doXContentBody`, `innerHashCode`, `innerEquals` include `shuffle`.
- New: `search/aggregations/bucket/terms/ShuffleConfig.java`.
- Validation hook: `action/search/TransportSearchAction.executeSearch(...)` (the private method, ~290), right after the global READ block check. This is where cluster state, cross-cluster status (`remoteShardIterators`) and settings are all known. It runs inside `ActionListener.wrap`, so the `IllegalArgumentException` reaches the listener and becomes a 400.
- New: `action/search/ShuffleRequestValidator.java`.

### Settings, attribute, gate — done in M1
- New: `search/shuffle/ShuffleSettings.java`. The settings are registered in `common/settings/ClusterSettings.BUILT_IN_CLUSTER_SETTINGS`.
- Node attribute: `node/Node.java` constructor, `this.settings = ShuffleSettings.withProtocolAttribute(pluginsService.updatedSettings())`. `DiscoveryNode.createLocal` reads `node.attr.*` from these settings, through `LocalNodeFactory`.
- Gate: `ShuffleSettings.firstNodeWithoutProtocol(DiscoveryNodes)`.

### 6.2 Planning
- New: `action/search/ShufflePlan.java`. The headers `_shuffle_plan` (Base64url of a versioned stream encoding) and `_shuffle_attempt` (decimal int) are final.
- Single-shard: `TransportSearchAction` forces QTF when `shardIterators.size()==1` (321-324). The shard inlines fetch when `request.numberOfShards()==1` (`SearchService.java:386-388`, which returns `QueryFetchSearchResult`).
- Can-match: `TransportSearchAction.shouldPreFilterSearchShards` (362-367). Pre-filtering applies only with QTF, a source that can rewrite to match-none, and `pre_filter_shard_size` (default 128) < shard count. Skipped shards have `SearchShardIterator.skip() == true` and go through `InitialSearchPhase.skipShard`. Planning goes in `AbstractSearchAsyncAction` after can-match, i.e. in the action the can-match phase factory creates.

### 6.3 Attempts and retries — `action/search/InitialSearchPhase.java`
- `performPhaseOnShard(int shardIndex, SearchShardIterator shardIt, ShardRouting shard)` (200-233) calls `executePhaseOnShard(...)` synchronously. Increment the attempt counter here, keyed by `ShardId`, not `shardIndex`.
- `onShardFailure(int, ShardRouting, String, SearchShardIterator, Exception)` (84-123) retries through `shardIt.nextOrNull()` (103) → `performPhaseOnShard` (110). Skip the retry here when the cause is `ShuffleRejectedException`.
- The request is built by `AbstractSearchAsyncAction.buildShardSearchRequest(SearchShardIterator)` (321-331) and sent by `SearchTransportService.sendExecuteQuery(Connection, ShardSearchTransportRequest, SearchTask, SearchActionListener)` (144-154) → `TransportService.sendChildRequest`.
- **Headers reach the shard:** remote sends go through `TcpTransport` → `ThreadContext.writeTo`. Local sends go through `localNodeConnection` → `sendLocalRequest`, and the QUERY handler runs on SAME with the caller's context. Shard-side forks (`rewriteShardRequest` → SEARCH) preserve the context. **Open question 1 is answered: yes**, but see D6 for retries.
- `allow_partial_search_results` exists: `SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS`, and it is checked in `InitialSearchPhase.run`, `AbstractSearchAsyncAction.executeNextPhase` and `buildSearchResponse`.

### 6.4 Shard side — `search/SearchService.java`
- `executeQueryPhase(ShardSearchRequest, SearchTask, ActionListener<SearchPhaseResult>)` (334-350) → `rewriteShardRequest` → the private sync `executeQueryPhase(ShardSearchRequest, SearchTask)` (366-407). Wrap the listener here.
- DFS: `executeQueryPhase(QuerySearchRequest, SearchTask, ActionListener<QuerySearchResult>)` (453). See D7.
- `QuerySearchResult.consumeAggs()` (156) returns the aggregations and nulls them (single use). `aggregations(InternalAggregations)` (165) sets them. For one shard, use `QueryFetchSearchResult.queryResult()`.

### 6.5 Collect all buckets — `TermsAggregatorFactory.doCreateInternal` (113-192)
- Override the shard size after `bucketCountThresholds.ensureValidity()` (127): `setShardSize(Integer.MAX_VALUE)`, `setShardMinDocCount(0)`.
- Queue sizing is capped everywhere, as assumed: `StringTermsAggregator:132` and `LongTermsAggregator:127` use `Math.min(bucketOrds.size(), shardSize)`. `GlobalOrdinalsStringTermsAggregator:178-183` uses `Math.min(valueCount | maxBucketOrd(), shardSize)`. `DoubleTermsAggregator` delegates to Long.
- `subAggCollectionMode(Integer.MAX_VALUE, …)` gives DEPTH_FIRST.

### 6.6 Reducer — `ShuffleService` (new)
- Transport registration pattern: `SearchTransportService.registerRequestHandler(TransportService, SearchService)` (308) uses `transportService.registerRequestHandler(name, Request::new, executor, handler)`. `internal:` names are valid.
- Breaker: `circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)`; `addEstimateBytesAndMaybeBreak(long, String)`; `addWithoutBreaking(-bytes)`.
- ReduceContext: `SearchService.createReduceContext(boolean finalReduce)` (1094-1096), with `MultiBucketConsumer`. So `search.max_buckets` applies per reducer automatically. That answers open question 3 for the reducer half.
- Thread pool: add `Names.SHUFFLE`, put it in `THREAD_POOL_TYPES` as FIXED, and add a builder in the `ThreadPool` constructor: `new FixedExecutorBuilder(settings, Names.SHUFFLE, availableProcessors, 1000)`.
- `InternalTerms.doReduce` **mutates its inputs** (`setDocCountError`). Never reduce the same partial objects twice.

### 6.7 Placeholder
- Register in `SearchModule.registerAggregations` (338) with `namedWriteables.add(new NamedWriteableRegistry.Entry(InternalAggregation.class, NAME, InternalShufflePlaceholder::new))`.

### 6.8 Finalize phase and substitution
- `SearchQueryThenFetchAsyncAction.getNextPhase(SearchPhaseResults, SearchPhaseContext)` (59-61) returns a new `FetchSearchPhase`.
- `SearchDfsQueryThenFetchAsyncAction.getNextPhase` (61-64) returns `DfsQueryPhase(…, queryResults -> new FetchSearchPhase(…))`.
- Substitution: `SearchPhaseController.reduceAggs(List<InternalAggregations>, List<SiblingPipelineAggregator>, ReduceContext)` (523-537). Substitute right after `InternalAggregations.reduce` (525) and before the sibling pipelines. That position already allows M8 item 8. **Open question 2 is answered.**
- Batched reduce: `QueryPhaseResultConsumer` (617+) is only used when `batched_reduce_size` (default 512) < shards.

### 6.9 Merge
- Concrete types: `StringTerms`, `LongTerms`, `DoubleTerms` (shuffle forbids double), and `UnmappedTerms` (a reducer that got only unmapped partials).
- Use `InternalMappedTerms`'s protected `create(String name, List<B> buckets, long docCountError, long otherDocCount)` (String 144 / Long 141). It is reachable from the same package, so the merge code lives in `search/aggregations/bucket/terms/`.

### Hashing
- `MurmurHash3.hash128(byte[], int, int, long seed, Hash128)` (66); `Hash128.h1`.
- `com.carrotsearch.hppc.BitMixer.mix64` is on the classpath (hppc 0.7.1).

## Open questions (section 11) status
1. Headers reach shards: **yes** (see 6.3). Retries need a stored base context (D6).
2. Substitution hook: `SearchPhaseController.reduceAggs`, after line 525.
3. `search.max_buckets`: it applies per reducer for free through `createReduceContext`. The coordinator merge must call `consumeBucketsAndMaybeBreak(top.size())` itself.
4. `heap_estimate_factor`: provisional default 3.0, to be measured in M2.
5. Coordinator as reducer: deferred to M7.
6. Cancellation: there is no hook in 6.5.4 (D8).
