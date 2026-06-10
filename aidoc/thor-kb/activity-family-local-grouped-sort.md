# Thor Activity Family: Local and Grouped Sort

This document covers the local and grouped sort path implemented in [../../thorlcr/activities/msort/thgroupsortslave.cpp](../../thorlcr/activities/msort/thgroupsortslave.cpp). It intentionally excludes distributed/global merge sort.

## 1. Family Summary
`CLocalSortSlaveActivity` is a thin wrapper over the shared row-loader path. Ungrouped local sort loads the entire input once during `start()`. Grouped local sort loads one group, drains it, emits an end-of-group marker, then reloads the next group.

That means most real cost is inherited from the shared loader/collector path rather than activity-specific logic. The main local/grouped-sort question is therefore how often the activity forces the shared loader to reset, spill, sort, and rebuild its output stream.

## 2. Main Anchors
- [../../thorlcr/activities/msort/thgroupsortslave.cpp#L33](../../thorlcr/activities/msort/thgroupsortslave.cpp#L33) `CLocalSortSlaveActivity`
- [../../thorlcr/activities/msort/thgroupsortslave.cpp#L52](../../thorlcr/activities/msort/thgroupsortslave.cpp#L52) loader creation with mixed spill policy
- [../../thorlcr/activities/msort/thgroupsortslave.cpp#L67](../../thorlcr/activities/msort/thgroupsortslave.cpp#L67) grouped `loadGroup()` at start
- [../../thorlcr/activities/msort/thgroupsortslave.cpp#L69](../../thorlcr/activities/msort/thgroupsortslave.cpp#L69) ungrouped whole-input `load()` at start
- [../../thorlcr/activities/msort/thgroupsortslave.cpp#L77](../../thorlcr/activities/msort/thgroupsortslave.cpp#L77) active stats publication via `setStats()`
- [../../thorlcr/activities/msort/thgroupsortslave.cpp#L97](../../thorlcr/activities/msort/thgroupsortslave.cpp#L97) grouped reload after end-of-group
- [../../thorlcr/thorutil/thmem.cpp#L1999](../../thorlcr/thorutil/thmem.cpp#L1999) `CThorRowLoader`
- [../../thorlcr/thorutil/thmem.cpp#L2025](../../thorlcr/thorutil/thmem.cpp#L2025) loader returning through `getStream()`
- [../../thorlcr/thorutil/thmem.cpp#L1735](../../thorlcr/thorutil/thmem.cpp#L1735) final stream handoff path
- [../../thorlcr/thorutil/thmem.cpp#L1846](../../thorlcr/thorutil/thmem.cpp#L1846) loader reset clearing rows, spill files, and counters

## 3. Confirmed Optimization Opportunities
### A. Reuse grouped-loader state across groups
Grouped local sort reloads through `loadGroup()` on every group boundary at [../../thorlcr/activities/msort/thgroupsortslave.cpp#L97](../../thorlcr/activities/msort/thgroupsortslave.cpp#L97). The default loader path resets first, and reset clears row state, spill files, and counters at [../../thorlcr/thorutil/thmem.cpp#L1846](../../thorlcr/thorutil/thmem.cpp#L1850).

For workloads with very many small groups, this guarantees setup and teardown churn that is unrelated to comparison work. A reusable grouped-loader mode that preserves row-table capacity and avoids full reset between fully drained groups is a confirmed optimization surface.

### B. Avoid rebuilding output stream machinery for tiny no-spill groups
Each `loadGroup()` returns through `getStream()` at [../../thorlcr/thorutil/thmem.cpp#L2025](../../thorlcr/thorutil/thmem.cpp#L2025), which deactivates callbacks, sorts the final in-memory run, and constructs a fresh stream/merger at [../../thorlcr/thorutil/thmem.cpp#L1735](../../thorlcr/thorutil/thmem.cpp#L1839).

In grouped mode, this repeats once per group. For tiny groups that never spill, the stream-construction overhead is a confirmed target for reduction.

### C. Fix grouped spill-stat accumulation before trusting measurements
The grouped local sort activity publishes active stats with `setStats()` at [../../thorlcr/activities/msort/thgroupsortslave.cpp#L77](../../thorlcr/activities/msort/thgroupsortslave.cpp#L77), while loader reset clears counters at [../../thorlcr/thorutil/thmem.cpp#L1849](../../thorlcr/thorutil/thmem.cpp#L1850).

That means grouped-sort spill behavior is not accumulated across groups in a trustworthy way. This is a confirmed observability defect and should be fixed before using grouped-sort benchmarks to drive optimization decisions.

## 4. Plausible But Unverified Opportunities
### A. Spill priority may be too eager for grouped local sorts
Grouped local sort selects `SPILL_PRIORITY_GROUPSORT` at [../../thorlcr/activities/msort/thgroupsortslave.cpp#L51](../../thorlcr/activities/msort/thgroupsortslave.cpp#L51). It is plausible that grouped sorts spill earlier than equivalent ungrouped locals under contention, but that may be deliberate prioritization rather than a bug.

### B. Stable sort defaults may be heavier than necessary
Unless the helper requests unstable sort, the loader uses `stableSort_earlyAlloc` at [../../thorlcr/activities/msort/thgroupsortslave.cpp#L52](../../thorlcr/activities/msort/thgroupsortslave.cpp#L52). There may be room for a cheaper grouped-local path when stability is not valuable in practice, but the semantic risk has not been audited here.

### C. `loaderCs` may indicate unfinished synchronization work
[../../thorlcr/activities/msort/thgroupsortslave.cpp#L42](../../thorlcr/activities/msort/thgroupsortslave.cpp#L42) declares `loaderCs` with a comment about loader lifetime during stats reads, but the active path does not use it. This may be dead code, or it may point to an unfinished safety concern.

## 5. Areas That Need Measurement
- split tiny-group runtime into compare cost versus loader reset and stream rebuild cost
- compare grouped and ungrouped spill behavior under concurrent Thor memory pressure
- measure spill-file fragmentation for a single very large group under pressure
- verify how misleading grouped-sort live stats are before and after any stat accumulation fix

## 6. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/msort/thgroupsortslave.cpp](../../thorlcr/activities/msort/thgroupsortslave.cpp)
2. [../../thorlcr/thorutil/thmem.cpp](../../thorlcr/thorutil/thmem.cpp) around `CThorRowLoader` and `CThorRowCollectorBase::getStream()`
3. [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md)
4. [storage-and-spill-io.md](storage-and-spill-io.md)