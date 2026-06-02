# Resource Allocator (`hqlresource.cpp`)

> Last verified: 2026-05

## 1. Purpose and Role

"Resourcing" in the eclcc pipeline refers to the process of converting a logical, declarative expression graph into an **execution graph** of subgraphs that respects the physical constraints of the target engine. Specifically, it:

- Splits the expression graph into **subgraphs** (each becoming a runtime graph/activity graph)
- Determines where **spill points** are needed when resource limits are exceeded
- Inserts **splitters** when a dataset is consumed by multiple downstream activities
- Marks splitters as **balanced** (limited read-ahead, more efficient but can deadlock) or **unbalanced** (unlimited read-ahead, safe but uses more memory)
- Handles **conditional execution** (IF branches) to avoid unnecessary evaluation
- Merges subgraphs that can share resources without introducing circular dependencies
- Manages **dependencies** between subgraphs (file read/write ordering, result sequencing)

### Pipeline Position

The resource allocator runs **after** optimization passes (constant folding, CSE, compound activity merging, filter hoisting, field projection) and **before** final C++ code generation. The idealized call order within `resourceThorGraph()` (the main entry point) is:

1. **`ActivityInvariantHoister`** — Hoists child-query-invariant expressions out of activities
2. **`ensureActivitiesAreUnique`** — Tags each action with a unique ID
3. **`EclResourcer::resourceGraph()`** — The main multi-pass resourcing algorithm
4. **`hoistNestedCompound`** — Post-processing of compound actions
5. **`SpillActivityTransformer`** — Converts abstract spill nodes into concrete read/write activities

The resourcing is called separately for:
- Top-level Thor graphs (`resourceThorGraph`)
- Library graphs (`resourceLibraryGraph`)
- Loop body graphs (`resourceLoopGraph`)
- Child queries (`resourceNewChildGraph`)
- Remote/hthor graphs (`resourceRemoteGraph`)

---

## 2. Key Classes

### `EclResourcer` ([hqlresource.ipp](../../ecl/hqlcpp/hqlresource.ipp))

The **main orchestrator** class. It coordinates the entire multi-pass resourcing algorithm. Key responsibilities:
- Manages the collection of `ResourceGraphInfo` (subgraphs) and `ResourceGraphLink` (edges between them)
- Executes passes 1–9 in sequence
- Holds the resource limit configuration (`CResources * resourceLimit`)
- Maintains the transform mutex for expression-graph traversal

### `ResourcerInfo` ([hqlresource.ipp](../../ecl/hqlcpp/hqlresource.ipp))

Per-expression metadata attached via `setTransformExtra`. Extends `SpillerInfo`. Tracks:
- **Graph assignment** (`graph`) — which `ResourceGraphInfo` this expression belongs to
- **Usage counts** (`numUses`, `numExternalUses`) — how many times the expression is referenced internally vs across graph boundaries
- **Conditional state** (`pathToExpr`, `conditions`) — whether the expression is unconditionally or conditionally executed
- **Split/spill decisions** (`isSpillPoint`, `balanced`, `neverSplit`)
- **Aggregation through-pass** (`aggregates`)
- **Balanced splitter links** (`balancedLinks`, `curBalanceLink`)

### `ResourceGraphInfo` ([hqlresource.ipp](../../ecl/hqlcpp/hqlresource.ipp))

Represents a **single subgraph** in the execution plan. Contains:
- `sources` / `sinks` — Links to/from other subgraphs (typed as `GraphLinkArray`)
- `dependsOn` — Dependency links (e.g., file write must precede file read)
- `resources` — Accumulated resource usage (`CResources`)
- `conditions` — Which conditional branches lead to this graph
- `isUnconditional` — Whether this graph is always executed
- `hasConditionSource` / `mergedConditionSource` — Conditional branching tracking
- `hasSequentialSource` — Whether this graph is part of a SEQUENTIAL chain
- `balancedExternalSources` / `unbalancedExternalSources` — External splitter classification

Key methods: `allocateResources()`, `mergeInSource()`, `mergeInSibling()`, `isDependentOn()`, `getDepth()`

### `ResourceGraphLink` ([hqlresource.ipp](../../ecl/hqlcpp/hqlresource.ipp))

An **edge** between two subgraphs. Contains:
- `sourceGraph` / `sinkGraph` — The two connected graphs
- `sourceNode` / `sinkNode` — The expression nodes at each end
- `linkKind` — `UnconditionalLink` or `SequenceLink`

### `ResourceGraphDependencyLink` ([hqlresource.ipp](../../ecl/hqlcpp/hqlresource.ipp))

A specialized link for **side-effect dependencies** (file read-after-write, result dependencies). Contains an additional `dependency` expression identifying the dependency reason.

### `CResourceOptions` ([hqlresource.ipp](../../ecl/hqlcpp/hqlresource.ipp))

Configuration holder for the resourcing pass. Encapsulates:
- Target cluster type and size
- Boolean flags controlling behavior (e.g., `allowThroughSpill`, `noConditionalLinks`, `minimiseSpills`, `combineSiblings`, `optimizeSharedInputs`, `newBalancedSpotter`)
- Spill naming via `UniqueSequenceCounter`
- Child query state

Key predicates:
- `canSplit()` — returns false for HThor (no splitter support)
- `checkResources()` — returns true only for Thor top-level graphs (resource splitting only applies there)
- `targetRoxie()` / `targetThor()`

### `CResources` ([hqlresource.ipp](../../ecl/hqlcpp/hqlresource.ipp))

A **resource budget vector** with slots for each `ResourceType`:
- `RESslavememory` — Memory on slave nodes
- `RESslavesocket` — Sockets on slave nodes
- `RESmastermemory` — Memory on master
- `RESmastersocket` — Sockets on master
- `REShashdist` — Hash distribute operations
- `RESheavy` — Heavyweight activities (sorts, hash joins)
- `RESactivities` — Total activity count

Provides `add()`, `addExceeds()`, `exceeds()`, `maximize()`, `sub()` for budget arithmetic.

### `SpillerInfo` ([hqlresource.ipp](../../ecl/hqlcpp/hqlresource.ipp))

Base class for spill generation. Handles:
- Creating spill read expressions (`createSpilledRead`)
- Creating spill write expressions (`createSpilledWrite`)
- Choosing spill medium: graph result, global (workunit) result, or disk file
- Adding appropriate attributes (compressed, grouped, etc.)

### `ActivityInvariantHoister` ([hqlresource.cpp](../../ecl/hqlcpp/hqlresource.cpp))

A **pre-pass transformer** that runs before `EclResourcer`. It identifies expressions within child queries (or activity arguments) that are **invariant** with respect to the activity and hoists them to be evaluated once at the parent level. This prevents redundant re-evaluation within iterative contexts.

### `CSplitterInfo` ([hqlresource.ipp](../../ecl/hqlcpp/hqlresource.ipp))

Helper class for the balanced/unbalanced splitter analysis. Gathers potential splitter nodes into a graph structure of `CSplitterLink` edges, then drives the cycle-detection walk.

### `EclResourceDependencyGatherer` ([hqlresource.cpp](../../ecl/hqlcpp/hqlresource.cpp))

Helper class for Pass 5. Walks expressions to discover file/result read-write dependencies and creates `ResourceGraphDependencyLink` objects between the affected graphs.

### `SpillActivityTransformer` ([hqlresource.cpp](../../ecl/hqlcpp/hqlresource.cpp))

A post-processing transformer that converts abstract spill nodes (`no_writespill`, `no_readspill`, `no_commonspill`) into concrete activities (`no_output`/`no_table` for disk, or `no_setgraphresult`/`no_getgraphresult` for graph results). Also collapses nested splitters.

---

## 3. Graph Splitting Logic

The resource allocator uses a **multi-pass** approach to determine where to split the expression graph into subgraphs:

### Pass 1: Find Split Points (`findSplitPoints`, `deriveUsageCounts`)

Walks the expression tree to identify:
- Which expressions are **activities** (dataset/row-producing operations)
- Which are containers (non-activity nodes that contain activities)
- **Usage counts** — how many times each activity is referenced

An activity with `numUses > 1` becomes a natural split point because it must be shared between multiple consumers.

Certain activities are marked `neverSplit`:
- Compound disk/index reads (cannot be broken apart)
- Keyed filters and stepped operations (splitting would lose optimization)

### Pass 2: Create Initial Graphs (`createInitialGraphs`)

Walks the tree again, assigning each activity to a `ResourceGraphInfo`:
- If an activity has `numUses > 1` (shared), it gets its **own** graph
- If a link is conditional (`SequenceLink`), it forces a new graph
- If `forceNewGraph` is true (e.g., for sequential actions, `no_executewhen`), it gets its own graph
- Otherwise, the activity is placed in its **owner's graph**

Connections between graphs are recorded as `ResourceGraphLink` edges.

### Pass 4: Resource-Based Splitting (`resourceSubGraphs`)

Only applies when `options.checkResources()` is true (Thor top-level graphs):

1. **`calculateResourceSpillPoints`** — Walks each graph's activity tree, accumulating resources. When adding the next activity would exceed the limit, it marks that activity as a **spill point**. It prefers spilling at points that reduce data (filters, aggregates, projects) and handles conditional branches by taking the maximum of branch resources.

2. **`insertResourceSpillPoints`** — Actually creates new subgraphs at the marked spill points by calling `createResourceSplit`.

### Criteria That Drive Splits

| Criterion | Effect |
|-----------|--------|
| Activity referenced more than once | Always split (shared) |
| Resource limit exceeded | Split at best spill point |
| Conditional (IF) branches | May force new graph |
| Sequential actions | Each action in its own graph |
| `no_executewhen` / `no_compound` | Dependency forced to separate graph |
| `neverSplit` flag | Prevents splitting within compound reads |
| Action in `actionLinkInNewGraph` mode | Forces new graph for dependency actions |

---

## 4. Engine Targeting

The resource allocator itself does **not** reassign activities between engines — by the time resourcing runs, the target engine is already determined by the compilation context. However, it **adapts its behavior** based on `targetClusterType`:

### ClusterType-Dependent Behavior

| Aspect | Thor (`ThorLCRCluster`) | Roxie (`RoxieCluster`) | HThor (`HThorCluster`) |
|--------|------------------------|------------------------|------------------------|
| Resource checking | Yes (splits on limits) | No (unlimited) | No (unlimited) |
| Splitter support | Yes (balanced/unbalanced) | Yes (balanced/unbalanced) | **No** (cannot split) |
| Graph results | Preferred for spills | Preferred for spills | Global results for child |
| Conditional links | Configurable (`noConditionalLinks`) | Always no conditional links | Has conditional links |
| Sibling merging | Yes (`combineSiblings`) | No | No |
| Balanced splitter scope | Per-graph (one graph at a time) | Global (all graphs) | N/A |
| `clusterSize` | User-configured (affects resource scaling) | 1 | 1 |
| Expand vs spill | Balance disk I/O cost | `expandRatherThanSplit()` | Expand when possible |
| `spillSharedConditionals` | No | Yes | No |
| `shareDontExpand` | No | Yes | No |

### Properties That Drive Engine-Specific Behavior

- `isLocalActivity(expr)` — If an activity is local, it uses fewer resources (no cross-node communication)
- `isGroupedActivity(expr)` — Grouped operations typically use less memory
- `isKeyedJoin(expr)` — Keyed joins are lightweight
- `expr->hasAttribute(lookupAtom)` — Lookup joins have specific resource profiles
- `expr->hasAttribute(hashAtom)` — Hash distribution activities
- `expr->hasAttribute(fewAtom)` — Hint that data volume is small

---

## 5. Resource Constraints

### Resource Types (`ResourceType` enum)

```
RESslavememory   — Memory per slave node
RESslavesocket   — Network sockets per slave
RESmastermemory  — Memory on master node
RESmastersocket  — Network sockets on master
REShashdist      — Number of hash distribute operations
RESheavy         — Number of heavyweight operations (sorts, hash joins)
RESactivities    — Total number of activities
```

### Default Limits

| Resource | Default Limit |
|----------|---------------|
| Total memory | 1800 MB (`DEFAULT_TOTAL_MEMORY`) |
| Max sockets | 2000 (`DEFAULT_MAX_SOCKETS`) |
| Max activities | 100 (`DEFAULT_MAX_ACTIVITIES`) |
| Max heavy | Configurable (`resourceMaxHeavy`) |
| Max distributes | Configurable (`resourceMaxDistribute`) |

These can be overridden via `HqlCppOptions` (`resourceMaxMemory`, `resourceMaxSockets`, `resourceMaxActivities`, `resourceMaxHeavy`, `resourceMaxDistribute`).

Setting `unlimitedResources = true` sets all limits to `0xffffffff` (effectively unlimited).

For non-Thor clusters, all limits are set to `0xffffffff` since splitting is not relevant.

### Activity Resource Costs

The `getResources()` function maps each activity type to its resource cost:

- **Lightweight** (0x10000 slave memory, 1 activity): filters, projects, choosen, topn, etc.
- **Heavyweight** (0x100000 slave memory, 1 heavy, 1 activity): global sorts, hash joins, hash dedup
- **Hash distribute cost**: `MEM_Const_Minimal + DISTRIBUTE_RESMEM(clusterSize)` plus 1 hashdist
- **Special cases**: Joins with different strategies (lookup, hash, smart, local) have varying costs

### How Resources Affect Splitting

When `calculateResourceSpillPoints` accumulates activity costs and finds that adding the next activity would exceed the limit (`addExceeds` returns true), it:

1. If a "good spill point" exists upstream (a lightweight activity that reduces data), it backtracks and spills there
2. Otherwise, it marks the current activity as a spill point
3. If a single activity alone exceeds the limit, it throws `HQLERR_CannotResourceActivity`

The `minimizeSkewBeforeSpill` option additionally prefers splitting before heavyweight activities (like distributes) to reduce data skew before spilling.

---

## 6. Spill Points

### Spill Decisions

The `expandRatherThanSpill()` method on `ResourcerInfo` determines whether to **duplicate** (expand) the activity in multiple graphs or **spill** (write to disk/result and read back). The decision considers:

1. **Source type**: Disk reads, index reads, inline tables, and `STEPPED` sources are cheap to re-read → expand
2. **Filtering**: If a filter sits between the shared point and the source, it may reduce data enough to justify spilling. Uses a cost model:
   - Cost of spill: `r + rp + npr` (read + filter + write + n reads)
   - Cost of expand: `nr` (n full reads)
   - Spill when: `p < (n-1)/(n+1)` where p is filter probability
3. **Processing**: If projections/limits are applied, the data is processed → may justify spilling
4. **Child query access** (`linkedFromChild`): If used from a child query, always spill (don't expand)
5. **`neverSplit`** flag: Prevents spilling for compound reads and keyed activities

### Spill Medium Selection

Three spill mechanisms are available, selected by `SpillerInfo`:

| Medium | When Used | Mechanism |
|--------|-----------|-----------|
| **Graph result** | Child queries, `useGraphResults` enabled | `no_setgraphresult` / `no_getgraphresult` |
| **Global (workunit) result** | Child query access when graph results unavailable | `no_output(named)` / `no_workunit_dataset` |
| **Disk file** | Default for Thor top-level | `no_output(file)` / `no_table(file)` |

The `outputToUseForSpill` optimization reuses an existing output file (e.g., a `BUILD INDEX` or named `OUTPUT`) instead of creating a separate spill file.

### Spill Optimization

- **`moveExternalSpillPoints`** (Pass 8): If all external consumers of a spill point apply lightweight data-reducing operations (filter, project), those operations are moved inside the source graph to reduce the spill file size.
- **`optimizeAggregates`** (Pass 7): Merges simple aggregate results into "through aggregates" that compute during the main data flow, eliminating separate subgraphs for trivial aggregations.
- **`minimizeSpillSize`** option: Controls how aggressively to move filtering into source graphs.
- **`filteredSpillThreshold`**: If the source is a filtered disk read and `numExternalUses >= threshold`, forces a spill rather than expanding.

---

## 7. Child Query Handling

Child queries (nested execution contexts within an activity, e.g., the body of a `PROJECT` transform, or `LOOP` bodies) are resourced differently:

### Key Differences

1. **`isChildQuery = true`**: Disables resource-based splitting. Child graphs are assumed lightweight.
2. **`useGraphResults = true`**: Spills within child queries use graph results (in-memory) rather than disk files.
3. **`unlimitedResources`**: Loop bodies may use unlimited resources to avoid artificial splits.
4. **Active cursors**: The parent's active row selectors are passed to the resourcing so that references to parent rows aren't incorrectly treated as activities.
5. **No sibling merging**: `combineSiblings` is disabled for child queries.
6. **No balanced splitter analysis for HThor**: Since HThor doesn't support splitters within child queries.

### `ActivityInvariantHoister`

Before the main resourcing pass, the `ActivityInvariantHoister` identifies expressions within activity arguments that can be evaluated **once** at the parent level:
- Dataset expressions that are independent of the activity's input rows
- Scalar expressions that reference hoistable datasets
- `GLOBAL` / `EVALONCE` expressions

These are extracted as separate activities with spilled results, and the original references are replaced with reads from those results.

### Child-Dependent Tracking

`CChildDependent` objects track expressions hoisted from child queries:
- `original` — The original expression within the child
- `hoisted` — The expression to evaluate at the parent level
- `projectedHoisted` — Optimized version with field projection
- `alwaysHoist` / `isSingleNode` / `forceHoist` — Hoisting strategy flags

---

## 8. Conditional Handling

### Pass 3: Mark Conditions (`markConditions`)

This pass tags each activity and graph with its conditional execution context:

1. **`markAsUnconditional`** — Recursively walks from root expressions, marking activities as either `PathUnconditional` or `PathConditional`
2. **Condition tags** — Each branch of an IF/CHOOSE gets a unique condition tag (e.g., `createAttribute(trueAtom, LINK(expr), LINK(condition))`)
3. **Graph-level unconditional** — If any path to a graph is unconditional, the graph is marked `isUnconditional`

### Conditional Split Points

When `noConditionalLinks` is **false** (Thor mode):
- Each branch of an IF/CHOOSE/conditional filter is forced into its own subgraph
- Activities within conditional branches are tagged with their condition set
- The `conditionSourceCount` tracks how many conditions feed into an activity

When `noConditionalLinks` is **true** (Roxie mode):
- Conditional branches are NOT forced into separate graphs
- The runtime engine handles conditional execution directly

### Spill Counting for Conditionals

`calcNumConditionalUses()` computes the effective number of times a conditional expression will be read. For Thor (where conditional graphs are cloned):
- Strips the true/false tag to find unique condition origins
- Counts the maximum occurrences within the same branch
- Sums across unique conditions

### Conditional Merging Rules

When merging graphs (`mergeInSource`):
- If the source is unconditional and the sink is conditional, merging is restricted unless:
  - The graph is "very cheap" (e.g., a single simple aggregate)
  - The source has only one sink
- If both have different conditions, merging is prevented to avoid unnecessary evaluation
- The `spillSharedConditionals` option (Roxie) allows shared conditionals to be spilled rather than duplicated

### `spillMultiCondition`

When an activity is used from multiple different conditional branches on Roxie, this option allows marking its graph as unconditional (ensuring it's always evaluated) rather than duplicating it in each conditional context.

---

## 9. Key Algorithms

### Overall Algorithm (`resourceGraph`)

```
1. findSplitPoints(exprs)         — Pass 1: Identify activities and usage counts
2. createInitialGraphs(exprs)     — Pass 2: Assign activities to initial subgraphs
3. addDependencies(exprs)         — Pass 5: Link subgraphs with read/write dependencies
4. markConditions(exprs)          — Pass 3: Tag conditional/unconditional paths
5. resourceSubGraphs(exprs)       — Pass 4: Split subgraphs exceeding resource limits
6. mergeSubGraphs()               — Pass 6: Merge compatible subgraphs
7. spotUnbalancedSplitters(exprs) — Pass 6b/6c: Detect and mark unbalanced splitters
8. optimizeAggregates()           — Pass 7: Merge simple aggregates as through-aggregates
9. moveExternalSpillPoints()      — Pass 8: Optimize spill data volume
10. createResourced(transformed)  — Pass 9: Generate final expression tree
```

### Graph Merging Algorithm (Pass 6)

Two-pass bottom-up merge:
- **Pass 0**: Merge sources that would NOT be expanded (normal spill points) into their sinks
- **Pass 1**: Merge sources that WOULD be expanded into their sinks

For each graph at each depth level:
1. Iterate over source links
2. Check if merge is valid: `queryMergeGraphLink()` verifies no dependency cycles, no conditional conflicts
3. Check if resources allow: `allocateResources()` verifies combined resources don't exceed limits
4. If valid: `mergeGraph()` combines the two graphs, `replaceGraphReferences()` updates all pointers
5. Dead graphs are marked `isDead`

After source merging, **sibling merging** (`mergeSiblings`) combines graphs that share a common source and have compatible conditions.

### Balanced Splitter Detection (Pass 6b — New Algorithm)

The problem: A "balanced" splitter (limited read-ahead) creates implicit dependencies between its outputs. If two balanced splitters share inputs, they can create **deadlock cycles**.

The algorithm:
1. **`gatherPotentialSplitters`** — Build a mini-graph of splitter nodes and their connections (`CSplitterLink`)
2. **`optimizeConditionalLinks`** — Remove duplicate links from activities that pull inputs independently (e.g., unordered `APPEND`)
3. **`walkPotentialSplitters`** — Depth-first traversal looking for cycles:
   - Mark nodes as `balancedVisiting` during traversal
   - If a node is reached that is already being visited → **cycle found**
   - The node causing the cycle is marked `balanced = false` (becomes unbalanced)
   - Backtracking propagates until the cycle is resolved

This is essentially computing a **feedback arc set** to break all cycles, specialized for the splitter graph structure.

### Dependency Graph (Pass 5)

`EclResourceDependencyGatherer` identifies ordering constraints:
- File writes create dependencies for subsequent file reads of the same file
- Result writes (`SET RESULT`) create dependencies for result reads (`GET RESULT`)
- Graph result writes depend on graph result reads

Dependencies are tracked per normalized filename/result-name. When a source-sink pair spans different subgraphs, a `ResourceGraphDependencyLink` is created, ensuring execution ordering.

### Depth Calculation and Caching

`ResourceGraphInfo::getDepth()` computes the longest path from roots in the dependency/source graph. It uses sequence-number-based caching (`depthSequence` vs `options->state.updateSequence`) to avoid redundant recomputation after graph merges.

`isDependentOn()` similarly caches its result with `cachedDependent`, invalidated when `updateSequence` changes (after any merge).

### Resource Spill Point Selection

The `calculateResourceSpillPoints` algorithm uses a **greedy** approach:
1. Walk the activity tree accumulating resources
2. When limits are exceeded:
   - If a "good spill point" exists (a lightweight data-reducing activity was seen), backtrack to it
   - Otherwise, mark the current node as a spill point
3. For conditional branches (IF/CHOOSE), take the **maximum** resource usage across branches (since only one executes, but we must account for the worst case)
4. The `minimizeSkewBeforeSpill` heuristic prefers splitting before heavyweight distribute operations to ensure data is balanced before being written to disk

---

## 10. Entry Points (Public API)

| Function | Purpose |
|----------|---------|
| `resourceThorGraph()` | Resource a top-level Thor/HThor graph |
| `resourceLibraryGraph()` | Resource a library instance graph |
| `resourceLoopGraph()` | Resource the body of a LOOP |
| `resourceNewChildGraph()` | Resource a new child query graph |
| `resourceRemoteGraph()` | Resource a remote execution graph |
| `convertSpillsToActivities()` | Post-process: convert spill nodes to read/write activities |

---

## 11. Important Invariants

1. **Graph immutability**: The resourcing pass does NOT modify existing expression nodes. It creates new nodes (splitters, spills) and builds a new output tree.
2. **Single-graph activities**: Each activity belongs to exactly one `ResourceGraphInfo` at any time (stored in `ResourcerInfo::graph`).
3. **No circular dependencies**: After all merges, `isDependentOn()` must return false for any pair in both directions (enforced by `checkRecursion` in debug builds).
4. **Link integrity**: When a graph is merged into another, ALL links pointing to the dead graph are redirected via `replaceGraphReferences()`.
5. **Unique activities**: Actions are tagged with unique IDs (`ensureActivitiesAreUnique`) so the runtime can distinguish multiple instances.
6. **Pass ordering**: Passes must execute in order because later passes depend on state set by earlier ones (e.g., Pass 4 needs Pass 2's graph assignments, Pass 6 needs Pass 5's dependencies).
