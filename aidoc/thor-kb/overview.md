# Thor Knowledge Base

> **Note**: This file is maintained by engineers and the `thor` AI agent. It tracks critical facts, high-level architecture, performance bottlenecks, and execution mechanics for the Thor component.

---

## 1. System Priorities
Thor is the HPCC Platform's large-scale batch processing engine. The primary engineering goals are:

- **Correctness at scale**: preserve grouping, ordering, and distribution semantics across large clusters.
- **Elapsed-time reduction**: remove skew, unnecessary repartitioning, and spill-heavy behavior.
- **Graceful behavior under memory pressure**: Thor must keep running by spilling safely when memory is constrained.

## 2. Execution Model
- Thor work is coordinated through the workflow/eclagent pipeline described in [../../devdoc/Workunits.md](../../devdoc/Workunits.md).
- Thor and hThor execute one subgraph at a time, while Roxie executes a complete graph as one. This makes graph boundaries, child-graph evaluation, and per-subgraph overhead important optimization surfaces for Thor.
- Graph activities are identified by `ThorActivityKind` values and paired with helper interfaces derived from `IHThorArg`, as documented in [../../devdoc/Workunits.md](../../devdoc/Workunits.md).

## 3. Memory and Spilling
- Thor's memory behavior differs fundamentally from Roxie: when memory pressure rises, Thor is expected to spill and continue rather than terminate the query. See [../../devdoc/MemoryManager.md](../../devdoc/MemoryManager.md).
- Buffered-row owners can register spill callbacks with priorities. Correct callback behavior and lock ordering are part of Thor correctness, not just performance.
- Multi-channel Thor execution relies on shared/global row-manager behavior so constant data can be shared safely across channels.

## 4. Observed Workload Profile
- Typical production clusters discussed so far use roughly 100, 200, or 400 workers.
- A representative worker has about 4 GB of row memory, 5 CPUs, and 200 GB of local disk.
- Real jobs span a huge range: some process only hundreds of rows, while others process billions.
- Graph complexity also spans widely, from roughly 100 activities to more than 40,000.
- Small jobs are common, but the largest graphs dominate total elapsed time and cost. That biases optimization value toward graph-wide coordination, spill timing, distribution preservation, and high-fanout stages.

## 5. Hot Optimization Surfaces
- **Distribution and skew**: bad partitioning, single-node merges, and skewed joins often dominate total runtime.
- **No-spill exceptions**: some Thor paths are intentionally whole-scope and all-memory, so OOM-before-spill behavior needs separate treatment.
- **Materialization boundaries**: extra result packages, temp files, and master/slave collation often dominate elapsed time more than the inner algorithm.
- **Centralized coordination**: master barriers, first-node modes, and rank-to-rank chains can serialize otherwise parallel work.
- **Graph shaping**: early reduction, preserving distribution, and placing spill or materialization before explosive stages can matter more than activity-local tuning.
- **Sort/group/funnel pipelines**: grouped sorts, funnels, and spill-heavy row collectors are recurring bottlenecks.
- **Child graph execution**: repeated graph evaluation and temporary result setup can multiply quickly inside child queries.
- **Instrumentation quality**: missing total times or misleading activity timings make optimization much harder.

## 6. Detailed Documentation Index
- [Execution Model](execution-model.md) - workflow, Thor queues, subgraphs, helper interfaces, and child-graph execution.
- [Activity Family Sweep](activity-family-sweep.md) - staged small-scope analyses of specific Thor activity families and their optimization surfaces.
- [Cross-Cutting Optimization Themes](cross-cutting-optimization-themes.md) - no-spill exceptions, materialization boundaries, centralized coordination, and observability blockers that cut across many families.
- [Top 10 Priority Actions](top-10-priority-actions.md) - ranked next actions tied to the current production workload, slow-job evidence, and expected impact.
- [Graph Shaping and ECL Feedback](graph-shaping-and-ecl-feedback.md) - graph-level and ECL-level opportunities to reduce rows earlier, preserve distribution, and improve spill timing before Thor pays the full cost.
- [Graph Runtime and Results](graph-runtime-and-results.md) - graph-side execution boundaries, result ownership, child-graph evaluation, and master/slave result materialization.
- [Memory and Spilling](memory-and-spilling.md) - spill callbacks, row buffering, large allocations, and channel-aware memory behavior.
- [Storage and Spill I/O](storage-and-spill-io.md) - temp-file creation, spill-file lifetime, merge versus concat behavior, and current spill observability gaps.
- [Shared Buffering and Spilling](shared-buffering-and-spilling.md) - `thmem.cpp` shared infrastructure for row arrays, spill callbacks, collectors, and stream handoff.
- [Activities Using Buffering and Spilling](activities-using-buffering-and-spilling.md) - how Thor activity families consume row loaders, row collectors, and spill-aware replay streams.
- [Graph Startup and Initialization](graph-startup-and-init.md) - prepareContext(), master→slave init exchange, onCreate() lifecycle, dead-branch traversal, and dependency re-execution overhead.
- [Within-Graph Parallelism](within-graph-parallelism.md) - strands, isFastThrough(), lookahead/smart buffers, per-row overhead, transform fusion gaps, limit propagation, and concurrentSubGraphs.
- [Optimizations Backlog](optimizations.md) - confirmed hotspots, workload-driven leads, and current refactoring ideas.