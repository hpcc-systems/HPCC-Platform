# Thor Cross-Cutting Optimization Themes

This note captures the engine-level patterns that became clear after the activity-family sweep. The family notes remain the detailed source of truth. This document exists to rank work across families and to highlight conclusions that only become obvious when many slices are compared together.

## 1. Why This Note Exists
The active family sweep is now broad enough that the next useful step is synthesis rather than another family note.

Four patterns dominate the current Thor optimization picture:

- hard no-spill memory walls
- extra materialization, replay, and serialization boundaries
- centralized synchronization and single-node coordination
- observability gaps that block reliable ranking

See [activity-family-sweep.md](activity-family-sweep.md) for family coverage and [optimizations.md](optimizations.md) for the reorganized backlog.

## 2. No-Spill Exceptions Are First-Class Risks
Thor is usually described as an engine that spills under memory pressure. That remains true for much of the runtime, but later family work showed that some important operators are intentionally all-memory.

The clearest examples are:

- [activity-family-project-filter-countproject-apply.md](activity-family-project-filter-countproject-apply.md), where `FILTERGROUP` requires whole-group validation in memory
- [activity-family-rollup-dedup-aggregate.md](activity-family-rollup-dedup-aggregate.md), where `DEDUP,ALL` and `ROLLUP GROUP` both materialize whole scopes through `rc_allMem`
- [activity-family-hash-distribute.md](activity-family-hash-distribute.md), where BEST-mode hash dedup still returns an unspillable stream

This changes how memory work should be prioritized. For these paths, generic spill tuning is not enough. They need either algorithmic redesign, tighter guardrails, or better user-facing warnings because they can fail before spill relief becomes available.

## 3. Materialization Boundaries Often Dominate More Than The Inner Algorithm
Many of the highest-cost Thor behaviors are not inside a transform loop. They happen where rows are copied, parked, spilled, reread, collated, or turned into result packages.

The strongest recurring examples are:

- [graph-runtime-and-results.md](graph-runtime-and-results.md), where distributed result promotion and master collation can duplicate buffering above the original activity result stream
- [activity-family-result-materialization-write-sinks.md](activity-family-result-materialization-write-sinks.md), where result shipping and sink paths add extra row movement and part-creation overhead
- [activity-family-loop.md](activity-family-loop.md), where regular loop recreates results every iteration and graph-loop retains iteration outputs as graph results
- [storage-and-spill-io.md](storage-and-spill-io.md), where spill-file lifetime and collector handoff determine when temporary state really goes away

This is the main cross-family lesson from the sweep: when a workload is unexpectedly expensive, the first question should often be “how many materialization or serialization boundaries does this rowset cross?” rather than “is the transform itself slow?”

## 4. Centralized Coordination Keeps Reappearing
Later family work sharpened the earlier graph-runtime conclusions by showing how often Thor performance is limited by coordination, not just computation.

Examples include:

- rank-to-rank state transfer in [activity-family-iterate-process.md](activity-family-iterate-process.md)
- master-mediated sync on every global loop iteration in [activity-family-loop.md](activity-family-loop.md)
- first-node-only row service-call modes in [activity-family-soapcall-httpcall.md](activity-family-soapcall-httpcall.md)
- child-graph serialization and result recreation in [graph-runtime-and-results.md](graph-runtime-and-results.md)
- explicit full-input barriers in [activity-family-limit-firstn-sample-selectnth-choosesets.md](activity-family-limit-firstn-sample-selectnth-choosesets.md)

The cross-family pattern is that Thor has many semantically correct but centrally coordinated paths. Those paths are often easy to miss if one looks only at local row transforms.

## 5. Observability Is A Prerequisite, Not Cleanup
At the start of the sweep, missing counters looked like cleanup work. By the end, the repeated gaps across joins, loops, structured reads, sinks, hash distribute, and service calls showed that instrumentation is a primary blocker to ranking engine work correctly.

The clearest symptoms are:

- [storage-and-spill-io.md](storage-and-spill-io.md), where visibility is much stronger for spill writes than for spill rereads, reopen counts, or merge fan-in
- [activity-family-loop.md](activity-family-loop.md), where loop only exports iteration count even though sync wait and retained results dominate behavior
- [activity-family-soapcall-httpcall.md](activity-family-soapcall-httpcall.md), where request-level spans know more than the family counters expose
- [optimizations.md](optimizations.md), where the longest section is now observability and timing gaps rather than algorithmic ideas

This means measurement work should not be postponed until after algorithm changes. In several areas it is the work that makes algorithm changes rankable at all.

## 6. Current Workload Profile Changes The Ranking
The current workload evidence matters because it is not a toy profile.

- Typical production environments here use roughly 100, 200, or 400 workers.
- A representative worker has about 4 GB of row memory, 5 CPUs, and 200 GB of local disk.
- Some jobs process only hundreds of rows, but the expensive end of the workload processes billions of rows across graphs with hundreds to tens of thousands of activities.
- Small jobs are common, but the largest jobs dominate total runtime and cost.

That workload pushes optimization value toward engine behaviors that multiply across the whole graph:

- preserving distribution so rows do not keep collapsing onto one node
- reducing rows before explosive joins, selfjoins, funnels, and iterative graph stages
- making spill happen earlier and on smaller intermediates
- reducing graph-wide coordination, replay, and repeated result packaging
- instrumenting the boundaries that decide where the real cost sits

In this environment, a modest improvement on a billion-row, 10,000-activity graph is often more valuable than a large percentage win on a tiny local operator.

## 7. Graph Shaping Is A Real Optimization Surface
The workunit notes add an important conclusion that is only partly inside Thor itself: sometimes the most useful optimization is not a faster activity implementation, but a better graph shape entering Thor.

The clearest example is a transitive-closure style pattern: spilling or reducing 100M rows before a self join is dramatically better than spilling 100B rows after the join has exploded the dataset.

That implies the KB should explicitly track graph-shaping opportunities such as:

- introducing an earlier reduction or dedup boundary before a known fanout stage
- using `MANY` or similar ECL-level reductions before distribute, aggregate, selfjoin, or funnel stages
- preserving distribution with `LOOKUP`-style or local paths instead of turning an already-distributed flow into a keyed or single-node pattern
- splitting one logical activity into two stages if that enables earlier spill, reduction, or cheaper materialization
- treating ordered funnel from child datasets as a graph-shape warning, not just a funnel implementation issue

Thor may not always be able to rewrite these cases automatically, but they are still high-value optimization knowledge because they guide both engine work and user-facing feedback.

See [graph-shaping-and-ecl-feedback.md](graph-shaping-and-ecl-feedback.md) for the dedicated graph-shaping note.

## 8. Practical Prioritization Order
Given the current KB, the most defensible order for implementation work is:

1. Add counters and timers for the missing boundaries that prevent ranking: sync wait, result creation, spill rereads, retained results, and family-specific control-path states.
2. Add graph-shaping and ECL-feedback guidance for the cases where earlier reduction, preserved distribution, or activity splitting would avoid the worst blowups entirely.
3. Address hard no-spill cliffs or add stronger guardrails around them.
4. Remove extra materialization and serialization boundaries where semantics already allow a cheaper path.
5. Only then spend time on hot-loop micro-optimizations, buffer-size knob cleanup, and reuse tweaks.

That order follows the current evidence better than starting with isolated per-row transform tuning.

See [top-10-priority-actions.md](top-10-priority-actions.md) for the ranked action list that applies this ordering to the current workload.

## 9. Reading Map
When revisiting Thor optimization work, the shortest high-value path is:

1. [overview.md](overview.md)
2. [cross-cutting-optimization-themes.md](cross-cutting-optimization-themes.md)
3. [top-10-priority-actions.md](top-10-priority-actions.md)
4. [graph-shaping-and-ecl-feedback.md](graph-shaping-and-ecl-feedback.md)
5. [optimizations.md](optimizations.md)
6. [graph-runtime-and-results.md](graph-runtime-and-results.md)
7. [memory-and-spilling.md](memory-and-spilling.md)
8. [activity-family-sweep.md](activity-family-sweep.md)