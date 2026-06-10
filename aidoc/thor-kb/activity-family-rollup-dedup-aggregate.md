# Thor Activity Family: Rollup / Dedup / Aggregate

This document covers the family rooted in [../../thorlcr/activities/rollup](../../thorlcr/activities/rollup) and [../../thorlcr/activities/aggregate](../../thorlcr/activities/aggregate). The main split inside this family is between streaming adjacency-based operators and the exceptional paths that fully materialize a local scope or group.

## 1. Family Summary
This family has two distinct execution styles.

- Standard `DEDUP` and `ROLLUP` are streaming boundary operators. They keep boundary state and, in global mode, hand the final kept state to the next slave with one MP message.
- `DEDUP,ALL` and `ROLLUP GROUP` are materially different. They load a full local scope or group into a row array through `IThorRowLoader(..., rc_allMem)` before processing.
- The aggregate variants are mostly bounded and streaming. Ungrouped global aggregate emits one partial row per slave and merges those rows on slave 1. Group aggregate reduces one group to one row. Through aggregate holds mutable state until `stop()`.

The critical shared-infrastructure fact is that `rc_allMem` disables spill in the loader implementation, so the materializing paths fail by memory exhaustion rather than degrading into standard spill behavior.

## 2. Main Anchors
- [../../thorlcr/activities/rollup/throllup.cpp#L30](../../thorlcr/activities/rollup/throllup.cpp#L30) global rollup/dedup master and global `DEDUP,ALL` rejection
- [../../thorlcr/activities/rollup/throllupslave.cpp#L26](../../thorlcr/activities/rollup/throllupslave.cpp#L26) `CDedupAllHelper`
- [../../thorlcr/activities/rollup/throllupslave.cpp#L152](../../thorlcr/activities/rollup/throllupslave.cpp#L152) `CDedupRollupBaseActivity`
- [../../thorlcr/activities/rollup/throllupslave.cpp#L207](../../thorlcr/activities/rollup/throllupslave.cpp#L207) global boundary restore in `checkFirstRow()`
- [../../thorlcr/activities/rollup/throllupslave.cpp#L240](../../thorlcr/activities/rollup/throllupslave.cpp#L240) boundary handoff in `putNextKept()`
- [../../thorlcr/activities/rollup/throllupslave.cpp#L317](../../thorlcr/activities/rollup/throllupslave.cpp#L317) streaming `CDedupSlaveActivity`
- [../../thorlcr/activities/rollup/throllupslave.cpp#L442](../../thorlcr/activities/rollup/throllupslave.cpp#L442) streaming `CRollupSlaveActivity`
- [../../thorlcr/activities/rollup/throllupslave.cpp#L540](../../thorlcr/activities/rollup/throllupslave.cpp#L540) `CRollupGroupSlaveActivity`
- [../../thorlcr/activities/aggregate/thaggregateslave.cpp#L32](../../thorlcr/activities/aggregate/thaggregateslave.cpp#L32) `AggregateSlaveBase`
- [../../thorlcr/activities/aggregate/thaggregateslave.cpp#L45](../../thorlcr/activities/aggregate/thaggregateslave.cpp#L45) rank-1 global merge in `getResult()`
- [../../thorlcr/activities/aggregate/thaggregateslave.cpp#L125](../../thorlcr/activities/aggregate/thaggregateslave.cpp#L125) `AggregateSlaveActivity`
- [../../thorlcr/activities/aggregate/thaggregateslave.cpp#L206](../../thorlcr/activities/aggregate/thaggregateslave.cpp#L206) `ThroughAggregateSlaveActivity`
- [../../thorlcr/activities/aggregate/thgroupaggregateslave.cpp#L20](../../thorlcr/activities/aggregate/thgroupaggregateslave.cpp#L20) `GroupAggregateSlaveActivity`
- [../../thorlcr/thorutil/thmem.cpp#L1858](../../thorlcr/thorutil/thmem.cpp#L1858) `rc_allMem` disables spilling in the shared collector/loader base

## 3. Confirmed Optimization Opportunities
### A. Replace `DEDUP,ALL` with a spill-capable or chunked algorithm
This is the clearest confirmed hotspot in the family.

`CDedupAllHelper` explicitly notes that chunking is missing at [../../thorlcr/activities/rollup/throllupslave.cpp#L111](../../thorlcr/activities/rollup/throllupslave.cpp#L111), then loads the full scope through `createThorRowLoader(..., rc_allMem)` at [../../thorlcr/activities/rollup/throllupslave.cpp#L88](../../thorlcr/activities/rollup/throllupslave.cpp#L88) and [../../thorlcr/activities/rollup/throllupslave.cpp#L114](../../thorlcr/activities/rollup/throllupslave.cpp#L114). The shared loader disables spilling for that mode at [../../thorlcr/thorutil/thmem.cpp#L1858](../../thorlcr/thorutil/thmem.cpp#L1858) and [../../thorlcr/thorutil/thmem.cpp#L1953](../../thorlcr/thorutil/thmem.cpp#L1953).

That makes `DEDUP,ALL` a confirmed OOM-before-spill path rather than a normal spill-heavy path.

### B. Replace `ROLLUP GROUP` whole-group materialization with a spill-capable or chunked path
`CRollupGroupSlaveActivity` has the same structural issue at group granularity. It loads a full group with `loadGroup()` through the all-memory loader at [../../thorlcr/activities/rollup/throllupslave.cpp#L554](../../thorlcr/activities/rollup/throllupslave.cpp#L554) and [../../thorlcr/activities/rollup/throllupslave.cpp#L574](../../thorlcr/activities/rollup/throllupslave.cpp#L574), then only improves the failure mode by wrapping OOM with better context at [../../thorlcr/activities/rollup/throllupslave.cpp#L597](../../thorlcr/activities/rollup/throllupslave.cpp#L597).

This is the second confirmed materialization hotspot in the family.

### C. Export scope-size and fan-in metrics that the runtime already knows
The family currently exposes too little information for these exceptional paths.

- `DEDUP,ALL` logs loaded row count in debug output at [../../thorlcr/activities/rollup/throllupslave.cpp#L124](../../thorlcr/activities/rollup/throllupslave.cpp#L124), but does not publish scope size, survivor count, or OOM-near capacity signals.
- Global aggregate already receives one partial row per slave in [../../thorlcr/activities/aggregate/thaggregateslave.cpp#L62](../../thorlcr/activities/aggregate/thaggregateslave.cpp#L62) through [../../thorlcr/activities/aggregate/thaggregateslave.cpp#L91](../../thorlcr/activities/aggregate/thaggregateslave.cpp#L91), but it does not export wait time, merge count, or first-node merge cost.

## 4. Plausible But Unverified Opportunities
### A. Revisit the rank-1 serial merge in global aggregate
The global aggregate structure is simple and bounded, but all partial rows still merge serially on slave 1 in [../../thorlcr/activities/aggregate/thaggregateslave.cpp#L45](../../thorlcr/activities/aggregate/thaggregateslave.cpp#L45) through [../../thorlcr/activities/aggregate/thaggregateslave.cpp#L91](../../thorlcr/activities/aggregate/thaggregateslave.cpp#L91). A tree reduction may help if slave counts are high or `mergeAggregate()` is expensive.

### B. Revisit stop-phase accounting in through aggregate
`ThroughAggregateSlaveActivity` drains remaining input and finalizes the aggregate in `stop()` at [../../thorlcr/activities/aggregate/thaggregateslave.cpp#L248](../../thorlcr/activities/aggregate/thaggregateslave.cpp#L248) through [../../thorlcr/activities/aggregate/thaggregateslave.cpp#L265](../../thorlcr/activities/aggregate/thaggregateslave.cpp#L265). If early-stop downstream patterns are common, some cost may be hidden in a place that is harder to interpret from activity timings.

### C. Revisit repeated row-array allocation churn in materializing paths
Both `DEDUP,ALL` and `ROLLUP GROUP` clear and rebuild row-array state per scope at [../../thorlcr/activities/rollup/throllupslave.cpp#L109](../../thorlcr/activities/rollup/throllupslave.cpp#L109) and [../../thorlcr/activities/rollup/throllupslave.cpp#L584](../../thorlcr/activities/rollup/throllupslave.cpp#L584). If pointer-table reuse is poor, that can add churn beyond the algorithmic pair work itself.

## 5. Areas That Need Measurement
- local scope size and row width for `DEDUP,ALL`
- group size distribution for `ROLLUP GROUP`
- frequency of the OOM-context paths in both all-memory variants
- `matches()` call count and survivor count for `DEDUP,ALL`
- first-node wait time and merge cost for global aggregate fan-in
- `ThroughAggregate::stop()` drain time versus steady-state `nextRow()` time
- early-out hit rate for ungrouped `existsaggregate`

## 6. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/rollup/throllup.cpp](../../thorlcr/activities/rollup/throllup.cpp)
2. [../../thorlcr/activities/rollup/throllupslave.cpp](../../thorlcr/activities/rollup/throllupslave.cpp)
3. [../../thorlcr/activities/aggregate/thaggregateslave.cpp](../../thorlcr/activities/aggregate/thaggregateslave.cpp)
4. [../../thorlcr/activities/aggregate/thgroupaggregateslave.cpp](../../thorlcr/activities/aggregate/thgroupaggregateslave.cpp)
5. [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md)