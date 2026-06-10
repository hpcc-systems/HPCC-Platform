# Thor Within-Graph Parallelism and Pipeline Execution

This document covers how Thor executes activities within a single subgraph, including thread boundaries, the strand model, pipeline parallelism via lookahead buffers, and per-row overhead.

## 1. Execution Model Within a Subgraph

Thor activities within a single subgraph execute via **demand-driven pull**: sink activities call `nextRow()` upstream, which cascades through the activity chain. This means:

- Activities are **not independently scheduled** — there is no thread pool or task queue within a graph.
- Multiple sinks in a subgraph execute sequentially unless explicitly async: [../../thorlcr/graph/thgraphslave.cpp#L1154](../../thorlcr/graph/thgraphslave.cpp#L1154).
- Pipeline parallelism (concurrent producer/consumer) is achieved **only** via smart buffers (lookahead).

## 2. Strand-Based Multi-Threading

### Current State: Only PROJECT Uses Strands

The strand model (`CThorStrandedActivity`) exists at [../../thorlcr/graph/thgraphslave.hpp#L473](../../thorlcr/graph/thgraphslave.hpp#L473) but is used by **only one activity**: PROJECT at [../../thorlcr/activities/project/thprojectslave.cpp#L79](../../thorlcr/activities/project/thprojectslave.cpp#L79).

All other activities (FILTER, DEGROUP, AGGREGATE, GROUP, SAMPLE, FIRSTN, CATCH, etc.) derive from single-threaded `CSlaveActivity`.

### How Strands Work

- `numStrands` controlled by the ECL `PARALLEL()` hint.
- Creates `IStrandJunction` splitter/recombiner to distribute rows across independent worker threads.
- Each strand has its own allocator (`roxiemem::RHFunique`) and helper instance.
- Block size (default 512 rows) controls batch granularity per strand.
- Strand logic: [../../thorlcr/graph/thgraphslave.cpp#L750](../../thorlcr/graph/thgraphslave.cpp#L750).

### Expansion Opportunities

Activities that are embarrassingly parallel and could benefit from stranding:
- **FILTER**: stateless predicate evaluation, no cross-row state
- **NORMALIZE**: per-row expansion, no cross-row dependency
- **PARSE**: per-row pattern matching

Activities that **cannot** be stranded (cross-row state):
- **ITERATE/PROCESS**: carries state between rows
- **ROLLUP/DEDUP**: depends on adjacent row comparison
- **GROUP**: depends on group boundaries

## 3. `isFastThrough()` and Lookahead Suppression

### What `isFastThrough()` Means

[../../thorlcr/graph/thgraphslave.cpp#L291](../../thorlcr/graph/thgraphslave.cpp#L291): An activity is "fast-through" if:
1. Its `ThorDataLinkMetaInfo` has `fastThrough = true` (lightweight transform, no blocking)
2. It cannot stall (`canStall = false`)
3. **All** of its inputs are also fast-through (recursive check)

Activities marked `fastThrough = true`:
- PROJECT, FILTER, CATCH, COUNTPROJECT, DEGROUP, GROUP, AGGREGATE (grouped), SAMPLE, FIRSTN, and ~10 others.

### Effect

When an entire chain is fast-through, the downstream activity does **not** install a lookahead smart buffer between itself and the chain. This means:
- No extra thread boundary between the activities.
- No smart buffer queue/dequeue overhead.
- The consumer's `nextRow()` directly calls the producer's `nextRow()` in the same thread.

Decision point: [../../thorlcr/graph/thgraphslave.cpp#L265](../../thorlcr/graph/thgraphslave.cpp#L265) — `ensureStartFTLookAhead()`.

### What It Does NOT Do

`isFastThrough()` does **not** enable:
- Transform fusion (combining two activities into one tight loop)
- Elimination of virtual dispatch (`IEngineRowStream::nextRow()` is still virtual per-row)
- Removal of per-row ref-count overhead (`OwnedConstThorRow` still links/releases)

## 4. Per-Row Overhead in Activity Chains

Each row crossing an activity boundary pays:

| Cost | Source |
|------|--------|
| Virtual dispatch | `IEngineRowStream::nextRow()` — one virtual call per row per activity |
| Ref-count increment | `LinkThorRow()` via `OwnedConstThorRow` — atomic operation |
| Ref-count decrement | `ReleaseThorRow()` on previous row — atomic operation |
| Helper virtual call | `helper->isValid()` or `helper->transform()` — one virtual per row |

For a chain of N lightweight activities (e.g., FILTER→PROJECT→FILTER), this means 4N atomic/virtual operations per row, even when the chain could theoretically execute as a single fused loop.

### Row Allocation Patterns

- **FILTER**: returns same row object via `row.getClear()` — no allocation.
- **PROJECT**: allocates new row via `RtlDynamicRowBuilder(allocator)` — allocation per surviving row.
- `OwnedConstThorRow` ref-counting: [../../thorlcr/thorutil/thmem.hpp#L87](../../thorlcr/thorutil/thmem.hpp#L87).

## 5. Lookahead (Smart Buffers) and Thread Boundaries

When `isFastThrough()` is false (activity can stall or block), Thor installs a lookahead smart buffer that:
- Creates a **separate producer thread** that prefetches rows into a queue.
- Consumer pulls from the queue without blocking on producer I/O.
- Enables pipeline parallelism between blocking producers and fast consumers.

Key parameters:
- Buffer sizes defined in [../../thorlcr/thorutil/thbufdef.hpp](../../thorlcr/thorutil/thbufdef.hpp): `ITERATE_SMART_BUFFER_SIZE`, `COUNTPROJECT_SMART_BUFFER_SIZE` (12 MB), etc.
- Persistent vs temporary lookaheads: persistent installed before input started, temporary after.

### Stop Propagation Through Smart Buffers

When downstream calls stop:
- Smart buffer sets `stopped = true` flag.
- Producer thread checks flag between batches (not per-row).
- Estimated stop latency: 1-10ms under normal conditions; higher under buffer pressure.

## 6. `concurrentSubGraphs` — Between-Graph Parallelism

This is the **only** lever for running multiple graphs simultaneously:

- Configuration: [../../thorlcr/graph/thgraph.cpp#L2513](../../thorlcr/graph/thgraph.cpp#L2513) — reads `concurrentSubGraphs` from workunit or globals.
- **Default: 1** (serial subgraph execution).
- Thread pool size = `limit * 2` for async cleanup.
- Dependency tracking: [../../thorlcr/graph/thgraph.cpp#L2620](../../thorlcr/graph/thgraph.cpp#L2620) — hard dependencies completely block parallelism.

### Why Default Is 1

1. **Memory explosion risk**: multiple concurrent graphs with large intermediates easily exceed row-manager budget.
2. **Most real graphs have dependencies**: truly independent subgraphs are rare.
3. **No adaptive feedback**: no mechanism to increase parallelism when memory headroom exists.

### Potential Improvements

- **Adaptive `concurrentSubGraphs`**: start at 1, raise when memory pressure is low, reduce on OOM risk.
- **Opportunistic small-graph fusion**: merge tiny independent subgraphs into parent graph to reduce scheduling overhead.

## 7. Limit Propagation and Predictive Short-Circuit

### Current: Reactive Stop Only

FIRSTN/CHOOSEN call `stopInput()` after receiving enough rows. Stop cascades upstream:
- [../../thorlcr/activities/firstn/thfirstnslave.cpp#L44](../../thorlcr/activities/firstn/thfirstnslave.cpp#L44)
- [../../thorlcr/activities/limit/thlimitslave.cpp#L205](../../thorlcr/activities/limit/thlimitslave.cpp#L205)

### Gap: No Proactive Limit Hints

Sources (INDEX, DATASET) don't know about downstream limits upfront. They may fetch far more rows than needed before receiving the stop signal.

**Potential improvements:**
- Compile-time limit inference: annotate sources with "consumer only needs N rows".
- Push limit hints to remote read paths (dafilesrv, index server) for early termination.

### Gap: No Predictive Empty Detection

No metadata channel exists for "this branch will produce 0 rows." Empty-result detection is purely reactive (read all rows, discover count is 0).

**Potential improvements:**
- Use dataset statistics (min/max cardinality) from compilation to prune known-empty branches before execution.
- Memoize dependency execution results to avoid re-executing dependencies that already produced empty.

## 8. Transform Fusion — Why It Doesn't Exist and What It Would Take

Thor currently has no mechanism to fuse adjacent lightweight activities into a single execution loop. `FILTER(PROJECT(x))` always executes as two separate activity loops with virtual dispatch between them.

### What Would Enable Fusion

1. **Compile-time code generation**: fuse multiple helper calls into a single generated function.
2. **Inline helper logic**: remove virtual dispatch by specializing at graph compilation time.
3. **Eliminate smart buffers for provably-safe chains**: linear chains with no stall risk need no buffering.
4. **Stack allocation for pass-through rows**: avoid heap allocation for rows that survive a filter unchanged.

### Practical Constraints

- Helpers are `IHThorArg*` interfaces — cannot inline at runtime without JIT.
- All activity communication is via `IEngineRowStream` (virtual dispatch).
- The current architecture fundamentally requires one `nextRow()` virtual call per activity per row.

## 9. Relationship to Other KB Notes

- Shared spill infrastructure and row buffering: see [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md)
- Graph startup and master→slave init cost: see [graph-startup-and-init.md](graph-startup-and-init.md)
- Graph-level scheduling and child-graph evaluation: see [graph-runtime-and-results.md](graph-runtime-and-results.md)
- Cross-cutting patterns including centralized coordination: see [cross-cutting-optimization-themes.md](cross-cutting-optimization-themes.md)
