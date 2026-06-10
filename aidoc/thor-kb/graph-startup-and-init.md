# Thor Graph Startup and Initialization

This document covers the cost of preparing and initializing a Thor graph before any rows flow. For large clusters with many subgraphs, initialization overhead can dominate elapsed time on short-running subgraphs.

## 1. Startup Phases

Thor graph execution proceeds through three serial phases before rows are produced:

### Phase 1: `prepareContext()` — Activity Creation and Wiring

[../../thorlcr/graph/thgraph.cpp#L657](../../thorlcr/graph/thgraph.cpp#L657) performs recursive depth-first traversal of the graph:

- **Activity factory lookup and creation**: each activity is created lazily during prepare.
- **`onCreate()` and `onStart()` called during prepare for conditional activities** (IF/CASE/FILTER): [../../thorlcr/graph/thgraph.cpp#L719](../../thorlcr/graph/thgraph.cpp#L719), [../../thorlcr/graph/thgraph.cpp#L751](../../thorlcr/graph/thgraph.cpp#L751). This evaluates `getCondition()`, `getBranch()`, or `canMatchAny()` to determine which branch to execute.
- **`connectInput()` wires upstream outputs**: [../../thorlcr/graph/thgraph.cpp#L839](../../thorlcr/graph/thgraph.cpp#L839).
- **Write-sink idempotence check**: [../../thorlcr/graph/thgraph.cpp#L709](../../thorlcr/graph/thgraph.cpp#L709) — `checkUpdate()` can short-circuit the entire graph if output already exists.
- **Internal sinks with 0 dependents are skipped**: [../../thorlcr/graph/thgraph.cpp#L785](../../thorlcr/graph/thgraph.cpp#L785).

### Phase 2: Master→Slave Init Data Exchange

[../../thorlcr/graph/thgraphmaster.cpp#L2735](../../thorlcr/graph/thgraphmaster.cpp#L2735): Master serializes per-activity init data for each slave **sequentially**:

- The payload typically includes file names, partition mappings, and layout metadata.
- Serialization is per-slave: slave N blocks until slaves 0..(N-1) are served.
- Slave-side receive: [../../thorlcr/graph/thgraphslave.cpp#L1007](../../thorlcr/graph/thgraphslave.cpp#L1007) blocks with `MEDIUMTIMEOUT` retries.

### Phase 3: `doExecute()` — Start Activities

[../../thorlcr/graph/thgraph.cpp#L1516](../../thorlcr/graph/thgraph.cpp#L1516):

- Loop over all connected activities: `onStart()` + `initActivity()` called sequentially.
- `preStart()` called recursively on all connected activities: [../../thorlcr/graph/thgraph.cpp#L1523](../../thorlcr/graph/thgraph.cpp#L1523).
- `start()` invoked at graph level: [../../thorlcr/graph/thgraph.cpp#L1524](../../thorlcr/graph/thgraph.cpp#L1524).
- Each activity's `start()` calls `dataLinkStart()` and `startInput(0)`: [../../thorlcr/graph/thgraphslave.cpp#L417](../../thorlcr/graph/thgraphslave.cpp#L417).

## 2. Known Overhead and Optimization Gaps

### Serial Master→Slave Exchange

The master processes slaves sequentially. For 400 workers × many activities, the Nth slave waits for (N-1) serialization rounds before receiving its data. Partition metadata is often identical across slaves but serialized redundantly per-slave.

**Potential improvements:**
- Serialize common partition metadata once; send per-slave deltas only.
- Pipeline slave init so early slaves start executing while later slaves still receive data.
- Non-blocking request model where slaves pull init data on-demand.

### Speculative `onCreate()` for Empty Graphs

`onCreate()` is called per-activity even for branches that will short-circuit or produce zero rows. For graphs that end up empty, all creation work is wasted.

**Potential improvements:**
- Defer `onCreate()` to actual execution commit time.
- Mark stateless activities to skip `onCreate()` on graph re-run (currently the `onCreateCalled` flag is cleared by `reset()` at [../../thorlcr/graph/thgraph.cpp#L399](../../thorlcr/graph/thgraph.cpp#L399)).

### Dead-Branch Traversal

Non-taken conditional branches are still traversed with `connectOnly=true` at [../../thorlcr/graph/thgraph.cpp#L812](../../thorlcr/graph/thgraph.cpp#L812). This recurses the full subgraph structure even though the branch will never execute.

**Potential improvements:**
- Skip recursive `prepareContext()` entirely for known-dead branches.
- Estimated savings: 5-20% of prepare time for conditional-heavy graphs.

### No Proactive Upstream Cancellation

Activities are started before `stopInput()` can prevent upstream work. The graph waits with a timeout rather than cancelling mid-flow: [../../thorlcr/graph/thgraph.cpp#L1525](../../thorlcr/graph/thgraph.cpp#L1525).

### Dependency Re-Execution

`executeDependencies()` at [../../thorlcr/graph/thgraph.cpp#L644](../../thorlcr/graph/thgraph.cpp#L644) has no result caching. The same dependency graph can re-execute without memoization when multiple activities or branches depend on it.

## 3. Source Comments and TODOs

| Location | Comment |
|----------|---------|
| [../../thorlcr/graph/thgraph.cpp#L1880](../../thorlcr/graph/thgraph.cpp#L1880) | `// JCSMORE - could do in parallel, they can take some time to timeout` (abort calls) |
| [../../thorlcr/graph/thgraph.cpp#L2113](../../thorlcr/graph/thgraph.cpp#L2113) | `// JCSMORE - would need to re-think how this is done if these sibling child queries could be executed in parallel` |
| [../../thorlcr/graph/thgraph.cpp#L772](../../thorlcr/graph/thgraph.cpp#L772) | Comment on parallel/sequential activities: "should be removed, once we are positive there are no issues" |

## 4. Relationship to Other KB Notes

- Graph-level result creation and child-graph evaluation overhead: see [graph-runtime-and-results.md](graph-runtime-and-results.md)
- `concurrentSubGraphs` and inter-graph scheduling: see [within-graph-parallelism.md](within-graph-parallelism.md)
- Activity-level start/stop overhead and row streaming: see [within-graph-parallelism.md](within-graph-parallelism.md)
