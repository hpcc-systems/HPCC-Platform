# Thor Top 10 Priority Actions

This note turns the Thor KB into a ranked, code-derived action list. Workunit observations are useful validation signals, but they are secondary here.

## 1. Ranking Basis
Primary ranking source:

- code-level hotspots and invariants already confirmed in the Thor family sweep and cross-cutting notes
- breadth of impact across activity families and graph/runtime boundaries
- expected multiplicative effect on large distributed graphs

Secondary ranking source:

- workload notes used to validate or break ties, not to define the list

The ranking still assumes the current production profile described in the KB:

- roughly 100, 200, or 400 workers
- about 4 GB of row memory, 5 CPUs, and 200 GB of local disk per worker
- workloads ranging from tiny jobs to billion-row graphs with hundreds to tens of thousands of activities
- the largest jobs dominate elapsed time and cost

That means the best next actions are the ones that either:

- improve ranking accuracy for the expensive jobs
- cut row volume before explosive stages
- preserve distribution and avoid centralized execution
- reduce graph-wide replay, spill, and synchronization costs

## 2. Ranked Actions

### 1. Close observability gaps at Thor's major cost boundaries
Why this ranks first from code: across join, loop, graph results, spill I/O, funnel, structured reads, and service calls, the code repeatedly exposes weak or incomplete counters. This blocks reliable ranking and often hides where elapsed time is really spent.

Current evidence:

- [cross-cutting-optimization-themes.md](cross-cutting-optimization-themes.md) and [optimizations.md](optimizations.md) both show observability gaps spanning many families.
- [../../devdoc/thoropt.md](../../devdoc/thoropt.md) aligns with the same gaps (spill size, project totals, helper timing, suspicious distribute and funnel timings).

Focus:

- spill byte counts and reread counts
- total time for `PROJECT`-like chains
- helper and library timing attribution
- sanity checks for distribute, funnel, and similar stage totals

### 2. Remove avoidable materialization and replay boundaries
Why this ranks here from code: the sweep repeatedly shows forced temp-file cycles, overflow-buffer replay, extra result packaging, and rematerialization across graph/runtime boundaries.

Current evidence:

- [optimizations.md](optimizations.md) section 2 (materialization, replay, result boundaries)
- [graph-runtime-and-results.md](graph-runtime-and-results.md) (slave fetch and master collation boundaries)

Focus:

- fewer spill write/read cycles when semantics permit
- fewer duplicate result materializations across graph layers
- cheaper replay and handoff boundaries

### 3. Preserve distribution and cut centralized execution paths
Why this ranks here from code: many expensive paths are structurally serialized or collapse to single-node behavior even when upstream work is distributed.

Current evidence:

- [optimizations.md](optimizations.md) section 3 (distribution, skew, centralized coordination)
- [graph-runtime-and-results.md](graph-runtime-and-results.md) and loop/iterate family notes on coordinated barriers and rank chains

Focus:

- avoid single-node merge and similar collapse patterns
- keep useful distribution information longer
- reduce master-mediated and first-node-only bottlenecks

### 4. Address hard no-spill memory cliffs
Why this ranks here from code: several paths are intentionally all-memory (`rc_allMem` and related behavior), making them OOM-before-spill risks rather than regular spill-tuning candidates.

Current evidence:

- [optimizations.md](optimizations.md) section 1 (no-spill watchlist)
- rollup/dedup/filtergroup family notes documenting whole-scope materialization behavior

Focus:

- guardrails and diagnostics for no-spill paths
- algorithmic alternatives where semantics allow
- clearer user-facing guidance for memory-risk shapes

### 5. Reduce skew amplification in join and selfjoin families
Why this ranks here from code: join helpers and selfjoin paths still contain known skew-sensitive behaviors that become multiplicative at scale.

Current evidence:

- join and selfjoin family notes plus section 3 of [optimizations.md](optimizations.md)
- helper behavior in `thsortu.cpp` and related join paths already flags missed shortcuts and group-scan inefficiencies

Focus:

- reduce skew-driven group blowups
- avoid unnecessary RHS-only scans and trailing scans
- improve skew diagnostics to detect bad shapes earlier

### 6. Optimize shared spill infrastructure overhead for tiny-group and reset-heavy patterns
Why this ranks here from code: multiple activity families repeatedly pay loader reset, stream handoff, and shared spill callback overhead.

Current evidence:

- [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md)
- grouped sort and shared spill sections in [optimizations.md](optimizations.md)

Focus:

- reduce per-group setup and reset churn
- preserve useful allocation state where safe
- measure callback and stream-setup cost directly

### 7. Improve graph-runtime and child-query execution efficiency
Why this ranks here from code: graph-level orchestration (`evaluateCrit`, result recreation, async overlap boundaries) can dominate activity-local improvements.

Current evidence:

- [graph-runtime-and-results.md](graph-runtime-and-results.md)
- loop and iterate family notes

Focus:

- reduce repeated child-result setup and packaging
- tune overlap to avoid synchronized memory-pressure spikes
- minimize graph-layer duplication above activity outputs

### 8. Prioritize graph-shaping and ECL feedback for multiplicative bad patterns
Why this ranks here from code plus architecture: several highest-cost paths are best mitigated by changing graph shape before execution rather than only optimizing inner loops.

Current evidence:

- [graph-shaping-and-ecl-feedback.md](graph-shaping-and-ecl-feedback.md)
- cross-family conclusions in [cross-cutting-optimization-themes.md](cross-cutting-optimization-themes.md)

Focus:

- early reduction before fanout-heavy stages
- preserve distribution and choose cheaper join kinds
- split logical phases when it creates better spill boundaries

### 9. Improve source-path and sink-path efficiency where setup cost dominates
Why this ranks here from code: structured-source and write/sink paths still show repeated setup and serialization-heavy behavior that can dominate local compute.

Current evidence:

- storage/source/sink sections in [optimizations.md](optimizations.md)
- structured-source and result-sink family notes

Focus:

- cut repeated source-path setup and translation work
- reduce sink-side rematerialization and transfer overhead
- improve diagnostics for read-path and publish-path behavior

### 10. Reduce graph initialization latency for large clusters
Why this ranks here from code: for 400-worker clusters with many subgraphs, the serial master→slave init exchange and speculative onCreate() overhead can dominate elapsed time on short-running subgraphs.

Current evidence:

- [graph-startup-and-init.md](graph-startup-and-init.md)
- serial master→slave exchange at [../../thorlcr/graph/thgraphmaster.cpp#L2735](../../thorlcr/graph/thgraphmaster.cpp#L2735)
- dead-branch traversal at [../../thorlcr/graph/thgraph.cpp#L812](../../thorlcr/graph/thgraph.cpp#L812)
- dependency re-execution without memoization at [../../thorlcr/graph/thgraph.cpp#L644](../../thorlcr/graph/thgraph.cpp#L644)

Focus:

- pipeline or parallelize master→slave init data delivery
- defer onCreate() for activities in branches that may short-circuit
- skip dead-branch traversal entirely
- memoize dependency execution results

### 11. Enable within-graph pipeline parallelism and reduce per-row overhead
Why this ranks here from code: only PROJECT uses multi-threaded strands; all other activities are single-threaded. Per-row virtual dispatch and ref-count overhead compounds across chains of lightweight activities.

Current evidence:

- [within-graph-parallelism.md](within-graph-parallelism.md)
- strand model limited to PROJECT at [../../thorlcr/activities/project/thprojectslave.cpp#L79](../../thorlcr/activities/project/thprojectslave.cpp#L79)
- per-row overhead from OwnedConstThorRow at [../../thorlcr/thorutil/thmem.hpp#L87](../../thorlcr/thorutil/thmem.hpp#L87)
- no proactive limit propagation to sources

Focus:

- extend stranding to FILTER, NORMALIZE, and other stateless activities
- investigate transform fusion for provably-safe linear chains
- push limit hints upstream to data sources for early termination
- adaptive concurrentSubGraphs with memory-pressure feedback

### 12. Tackle helper-heavy compute overhead after attribution is reliable
Why this ranks here from code perspective: helper-heavy hotspots are real but should be sequenced after broader graph and infrastructure multipliers unless instrumentation proves they dominate.

Current evidence:

- helper timing and attribution gaps in [optimizations.md](optimizations.md)
- additional helper-heavy symptoms in [../../devdoc/thoropt.md](../../devdoc/thoropt.md)

Focus:

- better helper attribution and visibility
- avoid repeated helper setup where possible
- revisit thread reuse only after timing signals are trustworthy

## 3. How To Use This List
Use this note as a code-first ranking layer above [optimizations.md](optimizations.md):

- if a new idea affects only a narrow hot loop, compare it against the top 10 before prioritizing it
- if a change improves graph shape, distribution preservation, or pre-fanout reduction, it should probably rank higher than its local code size suggests
- if a hotspot cannot be ranked because timing is ambiguous, the real action is usually still number 1

## 4. Notes On Workunit Evidence
Workunit observations remain valuable for validating priority and choosing examples, but they are intentionally not the primary basis for this list.

For workunit-driven follow-ups and concrete examples, see [../../devdoc/thoropt.md](../../devdoc/thoropt.md).