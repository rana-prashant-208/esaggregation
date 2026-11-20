# Proposal: Shuffle-Based Terms Aggregation for an Elasticsearch 6.8 Fork

**Status:** Draft for implementation
**Target:** Elasticsearch 6.8.x fork (server module)
**Audience:** Engineers and Claude Code working in the fork's repository

---

## 0. How to use this document with Claude Code

Put this file in the repository at `docs/shuffle/PROPOSAL.md` and add a short `CLAUDE.md` at the repo root that points to it:

```markdown
# CLAUDE.md
- The current project is the shuffle aggregation described in docs/shuffle/PROPOSAL.md. Read it before any change.
- Work one milestone at a time (section 9). Do not start a milestone until the previous one's acceptance criteria pass.
- Class and method names in the proposal were written from knowledge of ES 6.8 and MUST be verified against this checkout before editing. Record verified names and signatures in docs/shuffle/code-map.md.
- Keep changes minimal and local. Do not refactor unrelated code.
- Run `./gradlew precommit` and the tests named in the milestone before declaring it done.
```

Suggested prompt per milestone:

> Read docs/shuffle/PROPOSAL.md and docs/shuffle/code-map.md. Implement milestone M<n>. Verify every class and method you touch against the source first. Write the tests listed under M<n>, run them, and report what passed, what you changed, and anything in the proposal that didn't match the code.

**Rule for every milestone:** if the code differs from what this document assumes, stop, record the difference in `code-map.md`, and propose an adjustment rather than forcing the design.

---

## 1. Summary

Today, a `terms` aggregation in Elasticsearch is reduced on a single coordinating node. Each shard returns its local top `shard_size` buckets, and the coordinator merges them. This causes two problems:

1. **Inaccuracy.** Keys that rank moderately on every shard but never in a shard's local top list are undercounted or dropped (`doc_count_error_upper_bound > 0`).
2. **A single-node bottleneck.** Getting exact results requires a very large `shard_size`, which sends every shard's buckets to one node. Coordinator heap and CPU scale with shards × buckets.

This proposal adds an opt-in **shuffle** mode to the `terms` aggregation:

1. Shards compute all their buckets.
2. Each shard partitions its buckets by `hash(key)` and sends each partition to its owning **reducer** node.
3. Each reducer merges only its keys. It sees every shard's data for those keys, so its totals are exact. It returns its top `size`.
4. The coordinator merges N small, disjoint top-k lists.

Results are exact, and reduce memory and CPU are spread across N nodes.

---

## 2. Goals and non-goals

### Goals (v1)

- **Exact results:** a `terms` aggregation that is exact for any `order`, with `doc_count_error_upper_bound = 0`.
- **Spread reduce cost:** reduce memory and CPU distributed across N reducer nodes. The coordinator handles at most `N × size` buckets for the shuffled aggregation.
- **Unchanged response format:** responses look exactly like a normal `terms` aggregation.
- **Opt-in and safe:** disabled by default, gated by a cluster setting, and only usable when every node runs the fork.
- **Safe failures:** shard retries never double-count, no reducer state leaks, and memory is charged to the request circuit breaker.

### Non-goals (v1)

- `shuffle` on a `terms` aggregation that is nested under another bucket aggregation.
- Bucket sub-aggregations under a shuffled `terms`. Only metric sub-aggregations are allowed (section 4.2).
- Sibling pipeline aggregations that reference a shuffled aggregation.
- Scroll, cross-cluster search, `significant_terms`, script-based terms.
- Recovering from a reducer failure. v1 fails the search and the client retries.
- Threshold mode (TPUT), partitions greater than reducers, hot-key splitting, automatic selection. These are planned in milestone M8.

---

## 3. User-facing API

```json
GET events/_search
{
  "size": 0,
  "aggs": {
    "top_users": {
      "terms": {
        "field": "user",
        "size": 100,
        "shuffle": { "reducers": 5 }
      },
      "aggs": {
        "bytes": { "sum": { "field": "bytes" } }
      }
    }
  }
}
```

**`shuffle.reducers`** is an integer, at least 1 and at most `search.shuffle.max_reducers`. If it's larger than the number of eligible nodes, use all eligible nodes.

When `shuffle` is present, `shard_size` and `shard_min_doc_count` are ignored, because shards always send every bucket. If either is set explicitly, return a validation error, so users don't think they still have an effect.

**Response:** identical in shape to a normal `terms` response.
- `doc_count_error_upper_bound` is always 0.
- `sum_other_doc_count` is exact.
- The Profile API, when enabled, includes shuffle timings and sizes per reducer (milestone M7).

### Cluster settings (new, dynamic)

| Setting | Default | Meaning |
|---|---|---|
| `search.shuffle.enabled` | `false` | Master switch. When off, requests with `shuffle` fail with a clear error. |
| `search.shuffle.max_reducers` | `16` | Upper bound on `shuffle.reducers`. |
| `search.shuffle.state_timeout` | `5m` | Reducer state not finalized within this time is freed. |
| `search.shuffle.heap_estimate_factor` | to be measured in M2 | Multiplier from serialized partial size to estimated heap, for breaker accounting. |

---

## 4. Validation rules (v1)

### 4.1 Request-level

Reject the request (HTTP 400, clear message) when any of these is true:
- `search.shuffle.enabled` is false.
- Not every node in the cluster advertises shuffle protocol support (section 7.1).
- It is a scroll request or a cross-cluster search.
- The shuffled `terms` is not a top-level aggregation.
- A sibling pipeline aggregation's `buckets_path` references the shuffled aggregation.
- The field is not a keyword or long/integer/short/byte/date field, or the aggregation uses a script.

**One shard:** if the search resolves to exactly one shard after the can-match phase, silently run the normal path. Shuffling is pointless with one shard, and ES 6 has a single-shard query-and-fetch optimization that would conflict.

### 4.2 Allowed sub-aggregations (v1)

Metric aggregations only: `sum`, `min`, `max`, `avg`, `value_count`, `stats`, `extended_stats`, `cardinality`, `percentiles`. Reject any bucket sub-aggregation, and any pipeline sub-aggregation other than parent pipelines such as `bucket_script` and `bucket_selector`.

Parent pipelines run on the reducer, where each bucket is complete.

**Ordering:** every `order` is supported. In full mode, each reducer's buckets are final and exact, and the reducers' key sets are disjoint. So taking the top `size` of the merged lists under any total order is exact.

---

## 5. Design

### 5.1 Roles

- **Coordinator:** the node that receives the search request. It plans the shuffle, tracks shard attempts, finalizes, and merges.
- **Shard:** each target shard copy that executes the query phase.
- **Reducer:** a node selected by the coordinator to own a slice of the key space. The coordinator and data nodes can also be reducers.

### 5.2 The shuffle plan

Created once per search by the coordinator:

| Field | Meaning |
|---|---|
| `shuffleId` | Random UUID. |
| `aggName` | The name of the shuffled top-level aggregation. v1 allows one per request. |
| `reducers` | Ordered list of node IDs. |
| `numPartitions` | Equals `reducers.size()` in v1. Kept as a separate field so M8 can raise it. |
| `partitionToReducer` | `int[numPartitions]` mapping partitions to reducers. Identity in v1. |
| `coordinatorNodeId` | Used by reducers to free state if the coordinator leaves the cluster. |

**Reducer selection (v1):**
1. Prefer coordinating-only nodes, meaning no master, data or ingest role.
2. If there are fewer of those than requested, add data nodes.
3. Rotate the starting node with a per-search random offset, so concurrent searches don't always pick the same nodes.

### 5.3 Partition function

```
partition = floorMod(hash(key), numPartitions)
```

- **Keyword keys:** `MurmurHash3.hash128(bytes, offset, length, SHUFFLE_HASH_SEED, new Hash128()).h1`, using `org.elasticsearch.common.hash.MurmurHash3`.
- **Long keys:** a 64-bit mixer, for example `BitMixer.mix64(value ^ SHUFFLE_HASH_SEED)`.
- `SHUFFLE_HASH_SEED` is a constant that differs from any seed used for document routing. The partition function is part of the wire protocol and must never change within a protocol version.

### 5.4 End-to-end flow

```
Client ──► Coordinator
             │ 1. plan (shuffleId, reducers); attempt=1 per shard
             ├──────────────► Shard S0..Sn: query phase + full terms buckets
             │                  2. split buckets by partition
             │                  3. send partial (possibly empty) to EVERY reducer
             │                     ──► Reducer R0..Rk: store by (shard, attempt), ack
             │                  4. after all acks, reply to coordinator with placeholder
             │ 5. all shards done (or permanently failed)
             ├──────────────► Reducers: finalize(shuffleId, {shard → attempt to keep})
             │                  6. discard other attempts, final reduce, top `size`
             │ ◄────────────── reducer results
             │ 7. k-way merge, substitute placeholder, continue to fetch phase
Client ◄────┘
```

**Why every shard sends to every reducer**, even with no buckets: at finalize, a reducer must be able to tell "this shard had no keys for me" apart from "this shard's data hasn't arrived."

### 5.5 Carrying the plan to shards without changing the wire format

**Recommended approach:** carry the plan in `ThreadContext` request headers, not in new fields on `ShardSearchTransportRequest` or `TermsAggregationBuilder`.

Rationale:
- Thread context headers are already serialized with every transport request. Nodes that don't know a header ignore it.
- Normal, non-shuffle requests stay byte-for-byte identical to stock ES, which keeps rolling upgrades into and out of the fork safe.
- The fork shares stock version IDs, so version-gated serialization is not available.

Headers, with names to be finalized in M1:

| Header | Value |
|---|---|
| `_shuffle_plan` | Compact encoding of the plan from section 5.2. |
| `_shuffle_attempt` | The attempt number for this specific shard request. |

**How it's set:** the coordinator sets `_shuffle_plan` once for the search and `_shuffle_attempt` per shard request. `putHeader` cannot overwrite an existing header, so stash and restore the context around each shard send.

**The `shuffle` option itself:** `TermsAggregationBuilder` parses `shuffle` at the REST layer, but its `writeTo` / `StreamInput` serialization is NOT changed. Shards learn which aggregation to shuffle from `aggName` in the plan header.

**Alternative**, if headers prove unworkable: add serialized fields gated on all nodes advertising the protocol attribute. This needs careful reasoning about stock-to-fork streams. Document the reason in `code-map.md` if you switch.

The new transport actions and `InternalShufflePlaceholder` are only ever sent between fork nodes, after the gate in section 7.1 passes, so they need no compatibility handling.

---

## 6. Components and changes

> **Every path and name below is unverified.** Verify each one in M0 and record the actual names and signatures in `docs/shuffle/code-map.md`.

Base path: `server/src/main/java/org/elasticsearch/`

### 6.1 REST and request parsing

**`search/aggregations/bucket/terms/TermsAggregationBuilder.java`**
- Add a `ShuffleConfig` field (`reducers`), parsed from `"shuffle": { "reducers": N }`.
- Include it in `toXContent`, `equals` and `hashCode`.
- Do NOT add it to `writeTo` or the stream constructor (section 5.5).

**`search/aggregations/bucket/terms/ShuffleConfig.java`** (new): a small value class with parsing and validation.

**Request validation:** run the checks in section 4 at the coordinator before planning. The best hook is likely `TransportSearchAction`, or wherever `SearchRequest` validation happens in this version.

### 6.2 Coordinator: planning

**`action/search/TransportSearchAction.java`** / **`AbstractSearchAsyncAction.java`**
- After resolving shard iterators and after the can-match pre-filter phase, detect a shuffled aggregation in the request's source.
- If the target shard count is 1, fall back to the normal path.
- Build the `ShufflePlan` and register it in the search's phase context.

**`action/search/ShufflePlan.java`** (new): the plan from section 5.2, with header encoding and decoding.

### 6.3 Coordinator: attempts and retries

**`action/search/InitialSearchPhase.java`** (or wherever 6.8 implements `performPhaseOnShard` and `onShardFailure`):
- Keep a per-shard attempt counter. Increment it before each try, including retries on another copy.
- Set `_shuffle_attempt` on each shard request.
- On success, record the winning attempt for that shard.
- If a shard fails on all copies and `allow_partial_search_results` is true, exclude it from finalize and mark the response as partial. If partial results are not allowed, fail the search, as stock ES does.
- **Do not retry** shard failures caused by a reducer rejection (`ShuffleRejectedException`, section 6.6). Fail the search immediately, so replicas don't keep resending to a reducer that is out of memory.

**Shards skipped by can-match** never execute, so they are excluded from the finalize list from the start.

### 6.4 Shard side: split and send

**`search/SearchService.java`**, in the async `executeQueryPhase(ShardSearchRequest, SearchTask, ActionListener<SearchPhaseResult>)` path:
- If `_shuffle_plan` is present and `aggName` matches a top-level aggregation:
  1. Make the terms aggregator collect all buckets (section 6.5).
  2. After the query phase, take the `InternalTerms` for `aggName` from `QuerySearchResult`.
  3. Split its buckets into `numPartitions` groups with the partition function (section 5.3). Each group is a new `InternalTerms` of the same type, with the same `name`, `order`, `requiredSize` (= `size`), `minDocCount` and metadata, and its own `otherDocCount = 0` and `docCountError = 0`.
  4. Send one `ShufflePartialRequest` to each reducer, including empty groups. The send to the local node goes through `TransportService` like any other, which handles local delivery.
  5. Wait asynchronously for all acknowledgements. Do not block the search thread.
  6. Replace the aggregation in `QuerySearchResult` with `InternalShufflePlaceholder`, then complete the listener.
- If any send fails, complete the listener with that failure. A `ShuffleRejectedException` must propagate unchanged, so the coordinator can recognize it (section 6.3).

**`search/shuffle/ShardShuffleSender.java`** (new): steps 3–5.

**`indices/IndicesService.java`, `canCache(...)`:** return false when `_shuffle_plan` is present.

### 6.5 Collecting all buckets on the shard

**`search/aggregations/bucket/terms/TermsAggregatorFactory.java`**: when this aggregation is the shuffled one, as identified by the plan header in the shard's thread context:
- Set the shard size to `Integer.MAX_VALUE`.
- Set `shardMinDocCount` to 0.

Verify that the string, global-ordinals and long terms aggregators cap their priority queue at the actual number of distinct values, as in `Math.min(valueCount, shardSize)`. If any aggregator allocates by `shardSize` directly, fix that path.

### 6.6 Reducer side: `ShuffleService`

**`search/shuffle/ShuffleService.java`** (new): a node-level component, created in `node/Node.java` next to `SearchService`.

**State:**
```
Map<shuffleId, ShuffleState>

ShuffleState {
  String aggName
  String coordinatorNodeId
  Map<ShardId, Map<attempt, InternalAggregation>> partials
  long reservedBytes
  boolean poisoned
  long createdAt
}
```

**Transport actions,** registered alongside the existing search actions in `action/search/SearchTransportService.java`:

**`internal:search/shuffle/partial`** — request: `shuffleId`, `aggName`, `shardId`, `attempt`, `partition`, `coordinatorNodeId`, `InternalAggregation partial` (as a NamedWriteable). On receipt:
1. If the state is poisoned, reject immediately with `ShuffleRejectedException`.
2. Estimate heap as serialized size × `heap_estimate_factor` and reserve it on the REQUEST breaker. If the breaker trips, poison the state, release everything already reserved, and reject with `ShuffleRejectedException`.
3. Store the partial under `(shardId, attempt)` and acknowledge.

v1 buffers partials and merges at finalize, because a shard's attempt can't be trusted until finalize names it. A possible later optimization: merge eagerly while a shard has only one attempt.

**`internal:search/shuffle/finalize`** — request: `shuffleId`, `Map<ShardId, Integer> keepAttempts`. On receipt:
1. If the state is poisoned or missing, return an error.
2. Check that every shard in `keepAttempts` has a partial for exactly that attempt. If one is missing, return an error.
3. Discard partials from all other attempts, and from shards not in `keepAttempts`.
4. Run a final reduce over the kept partials with the aggregation's own `reduce`, using a `ReduceContext` built the same way `SearchPhaseController` builds it. That includes `BigArrays`, `ScriptService` and the multi-bucket consumer, with `isFinalReduce = true`. This applies `order`, `min_doc_count`, the cut to `size` and parent pipelines.
5. Respond with the reduced aggregation, plus stats: buckets received, bytes received, merge time.
6. Free the state and release breaker memory.

**`internal:search/shuffle/free`** — request: `shuffleId`. Frees state if present. Idempotent.

**Lifecycle:**
- **Timeout:** a periodic task via `ThreadPool.scheduleWithFixedDelay` frees any state older than `search.shuffle.state_timeout`.
- **Coordinator departure:** implement `ClusterStateListener`. When a coordinator node leaves the cluster, free all states it owns.
- **Thread pool:** run partial and finalize handlers on a new fixed `shuffle` thread pool, registered in `threadpool/ThreadPool.java`, with a size around the number of processors and a bounded queue. Merges must not starve the `search` pool.

**`search/shuffle/ShuffleRejectedException.java`** (new): rejection due to breaker or poisoned state. Not retryable.

### 6.7 Placeholder aggregation

**`search/shuffle/InternalShufflePlaceholder.java`** (new): extends `InternalAggregation`. It carries `shuffleId` and the aggregation's `name`, `size`, `order`, `minDocCount` and metadata, and is registered as a NamedWriteable in `search/SearchModule.java`.
- `doReduce(...)` returns one of its inputs, so batched partial reduces (`batched_reduce_size`) keep working.
- `doXContentBody` throws `IllegalStateException`. Rendering a placeholder means substitution failed and must never happen silently.
- `getProperty` throws the same way.

### 6.8 Coordinator: finalize phase and substitution

**`action/search/ShuffleFinalizePhase.java`** (new): a `SearchPhase` inserted before `FetchSearchPhase`.
- **Hook:** `SearchQueryThenFetchAsyncAction.getNextPhase(...)` and the DFS variant return `new ShuffleFinalizePhase(context, plan, () -> new FetchSearchPhase(...))` when a plan exists.
- **`run()`:**
  1. Send finalize to every reducer, with `keepAttempts` from section 6.3.
  2. Collect the responses. On any failure, send `free` to all reducers and fail the search.
  3. Merge the results (section 6.9) into the final `InternalTerms`.
  4. Store it in a holder on the phase context, keyed by aggregation name.
  5. Run the next phase.

**Substitution:** after `SearchPhaseController.reducedQueryPhase(...)` reduces the top-level aggregations, replace `InternalShufflePlaceholder` with the stored result before the response is built. Add an explicit hook for this; the exact place is decided in M4 from the real code.

Because v1 rejects sibling pipelines that reference the shuffled aggregation (section 4.1), substitution can happen after sibling pipelines run. Supporting those pipelines later means moving substitution to before sibling pipeline execution.

**Cancellation:** when the `SearchTask` is cancelled, send `free` to all reducers.

### 6.9 Coordinator merge algorithm

Input: one `InternalTerms` per reducer, each already final and exact, with disjoint keys.

```
all     = concatenate the buckets of every reducer result
sorted  = sort(all, order comparator of the aggregation)
top     = first `size` of sorted
other   = sum(reducer.otherDocCount) + sum(doc_count of all buckets not in top)
result  = same InternalTerms type, built with buckets = top, docCountError = 0, otherDocCount = other
```

Do NOT call `reduce` here. Parent pipelines already ran on the reducers, and running reduce again would repeat them. Build the result with the type's own constructor or factory method; verify which one in M0.

---

## 7. Compatibility and rollout

### 7.1 Protocol gate

Every fork node automatically advertises the node attribute `search_shuffle_protocol: 1`. The coordinator allows shuffle only when every node in the current cluster state has it, and `search.shuffle.enabled` is true.

**Rolling upgrade into the fork:** shuffle stays unusable until the last node is upgraded. Normal searches are unaffected throughout.

**Rolling back:** set `search.shuffle.enabled: false` first, then downgrade.

### 7.2 Behavior without shuffle

With no `shuffle` option, behavior and wire format are unchanged from stock 6.8. Test this explicitly (M5).

---

## 8. Failure handling summary

| Failure | Behavior |
|---|---|
| Shard fails before or after sending some partials | Coordinator retries on the next copy with `attempt + 1`. Finalize keeps only the successful attempt. |
| Shard fails on all copies, partial results allowed | Shard is excluded from finalize. Response is marked partial. |
| Reducer breaker trips | State is poisoned. The shard request fails with `ShuffleRejectedException`. Coordinator fails the search without retrying and frees all reducers. |
| Reducer node leaves the cluster | Any send or finalize to it fails. Coordinator fails the search and frees the surviving reducers. Client may retry. |
| Coordinator node leaves the cluster | Reducers free its states through the cluster-state listener, or at the latest through the timeout. |
| Search cancelled | Coordinator frees all reducers. Shard sends in flight fail harmlessly. |
| Partial arrives after finalize or free | No state exists, so it is rejected. |
| Search timeout (`timeout` parameter) | Shards send what they collected, as they do today. Response has `timed_out: true`. |

---

## 9. Milestones

Each milestone ends with `./gradlew precommit` passing and its tests passing. Integration tests named `*IT` run in the task the branch uses for them (check `server/build.gradle`). Use the branch's test conventions, such as `-Dtests.class=...` for randomized testing.

### M0 — Reconnaissance and code map

- Build the branch and run a baseline of the search and aggregation unit tests.
- For every class and method in section 6, open the source and record the real name, package, signature and the relevant lines in `docs/shuffle/code-map.md`.
- Answer the open questions that can be answered from code (section 11).

**Acceptance:** `code-map.md` covers every item in section 6 and lists every place where the proposal is wrong about the code, with a suggested adjustment.

### M1 — API, validation and gating

- Parse `shuffle` and add `ShuffleConfig`.
- Add the settings, the node attribute and the protocol gate.
- Add the request validation from section 4.
- Add `ShufflePlan` with header encoding and decoding.

**Tests:**
- Parsing round-trips through XContent.
- Every validation rule in section 4 has a failing case and a passing case.
- The gate rejects shuffle when any node lacks the attribute.
- A plan survives header encode and decode.
- `TermsAggregationBuilder` stream serialization is unchanged: serialize with the fork, deserialize with the original wire layout.

### M2 — `ShuffleService` and protocol, tested alone

- Transport actions, state, breaker accounting, poisoning, timeouts, the cluster-state listener and the thread pool.

**Tests (unit, with fake partials):**
- Merging partials gives the same result as `reduce` over the same inputs.
- Attempt selection keeps only the named attempt.
- Finalize fails when an expected shard is missing.
- The breaker trips, poisons the state and releases everything.
- Timeout cleanup works.
- Coordinator-left cleanup works.
- `free` is idempotent.

**Measure:** the ratio of heap to serialized size on realistic partials, and set a default for `heap_estimate_factor`.

### M3 — Shard side

- Collect all buckets for the shuffled aggregation (section 6.5).
- Split and send with `ShardShuffleSender`, including empty partitions.
- Substitute the placeholder and disable the request cache.

**Tests:**
- Partition function determinism: golden values for fixed keys, so the protocol can never drift unnoticed.
- The split preserves every bucket exactly once.
- Empty partitions are sent.
- The placeholder survives batched reduce.
- `canCache` returns false with the plan header.

### M4 — Coordinator

- Planning, reducer selection, per-shard attempts, the retry exception for `ShuffleRejectedException`.
- `ShuffleFinalizePhase`, the merge algorithm (section 6.9), the substitution hook, freeing on cancellation and failure.

**Tests:**
- The merge algorithm against hand-computed cases, including every `order` and the `sum_other_doc_count` calculation.
- Single-shard fallback.
- Shards skipped by can-match are excluded from finalize.

### M5 — Correctness integration tests

- **Canonical fixture:** encode the 5-shard example from section 10 and assert the exact response.
- **Randomized equivalence:** for random data (key count, distribution, shard count, reducer count, `size`, `order`, `min_doc_count`, metric sub-aggregations), the shuffle response must equal the response with `shard_size` set larger than the number of distinct keys. Compare buckets, counts, metrics and `sum_other_doc_count`. `doc_count_error_upper_bound` must be 0.
- **Non-shuffle regression:** existing terms aggregation tests pass unchanged.

### M6 — Failure, disruption and leak tests

- Use network disruption to fail a shard mid-send and check the result is still correct.
- Kill a reducer: the search fails cleanly and the other reducers are freed.
- Kill the coordinator: reducers are freed.
- Cancel a search: reducers are freed.
- Trip the breaker: the search fails fast with no replica retries.
- **After every test,** assert that no `ShuffleState` remains on any node and that request breakers return to their baseline.

### M7 — Observability

- Profile API output: per-reducer buckets received, bytes, merge time, and finalize round-trip time.
- A node stats section: active shuffle states, reserved bytes, rejections and timeouts.

**Tests:** fields appear and have sane values.

### M8 — Later work (each its own proposal update)

1. **Partitions greater than reducers:** for example 64 or 256 partitions with weighted `partitionToReducer` assignment.
2. **Bucket sub-aggregations** under the shuffled terms, with memory guidance.
3. **Shuffle on terms nested under small bucket aggregations:** partition by the full bucket path, merge per path at the coordinator.
4. **Threshold (TPUT) mode** for descending count or non-negative sum ordering. Shard-side bucket state is kept between phases under the shuffle ID, and subtrees are fetched only for the final winners. Fall back to full mode when phase 2 shows that most keys pass the threshold.
5. **Hot-key splitting** for heavy subtrees, by hashing `(key, sub_key)` for known hot keys.
6. **Automatic selection** of shuffle from estimated cardinality.
7. **Load-aware reducer selection,** using node heap and search queue depth.
8. **Sibling pipelines** referencing shuffled aggregations, by moving substitution before sibling pipeline execution.

---

## 10. Canonical example (test fixture for M5)

5 shards, `shuffle.reducers: 5`, `size: 3`, ordered by count descending.

| User | S0 | S1 | S2 | S3 | S4 | Total |
|---|---|---|---|---|---|---|
| alice | 40 | 10 | 0 | 35 | 5 | 90 |
| bob | 0 | 50 | 20 | 0 | 25 | 95 |
| carol | 30 | 30 | 30 | 30 | 30 | 150 |
| dave | 20 | 0 | 0 | 60 | 0 | 80 |
| erin | 10 | 15 | 10 | 10 | 40 | 85 |
| frank | 5 | 5 | 5 | 5 | 5 | 25 |
| grace | 0 | 0 | 70 | 0 | 0 | 70 |
| heidi | 15 | 0 | 10 | 0 | 0 | 25 |
| ivan | 0 | 20 | 0 | 5 | 0 | 25 |
| judy | 0 | 0 | 5 | 0 | 10 | 15 |

Total documents: 660.

**Expected response:**
- Buckets: carol 150, bob 95, alice 90.
- `sum_other_doc_count`: 660 − 335 = **325**.
- `doc_count_error_upper_bound`: 0.

Index the documents with explicit routing so each shard gets exactly the counts above. The test must pass for any partition assignment, since real hash values will differ from the illustrative assignment used in discussion.

**Retry variant:** fail S3 after it has sent to some reducers, so it retries with attempt 2. The expected response is unchanged; in particular alice must still be 90, not 125.

**Stock comparison:** with stock ES, `size: 3` and the default `shard_size`, carol can be missed, because she is at most third on every shard. Use this as documentation of the problem, not as an assertion.

---

## 11. Open questions

1. **Thread context headers:** do they reliably reach the shard-level code paths for local shards, remote shards and retries in 6.8? (M0) If not, use the alternative from section 5.5.
2. **Substitution hook:** where exactly in `SearchPhaseController` / `QueryPhaseResultConsumer` should it go? (M0/M4)
3. **`search.max_buckets`:** how should it apply? Proposal: enforce it per reducer during the reducer's final reduce, and on the coordinator for the merged result. Confirm the semantics are acceptable.
4. **`heap_estimate_factor` default:** measured in M2.
5. **Coordinator as reducer:** should the coordinator be excluded from the reducer set when there are enough other nodes? Decide from M7 measurements.
6. **Cancellation hooks:** confirm how `SearchTask` cancellation propagates in 6.8, and the right place to register for it.

---

## 12. Risks

- **Fork maintenance:** changes touch the search phases, `SearchService` and the aggregation factories, which are areas that churn between versions. Keep every change behind clearly named, minimal hooks to ease rebasing.
- **Latency:** the shuffle adds one round trip, so it's slower than the normal path for small aggregations. That's why it is opt-in.
- **Hot keys with large subtrees:** not an issue in v1, which allows metric sub-aggregations only. Addressed in M8.
- **Breaker estimates:** inaccurate estimates could allow excessive heap use. Mitigated by measuring in M2 and by poisoning on the first trip.
