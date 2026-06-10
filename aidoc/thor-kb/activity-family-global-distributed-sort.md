# Thor Activity Family: Global and Distributed Sort

This document covers the distributed merge-sort path used by `SORT` when work cannot remain local. It includes the Thor activity wrappers in [../../thorlcr/activities/msort/thmsort.cpp](../../thorlcr/activities/msort/thmsort.cpp) and [../../thorlcr/activities/msort/thmsortslave.cpp](../../thorlcr/activities/msort/thmsortslave.cpp), plus the underlying distributed sorter in [../../thorlcr/msort](../../thorlcr/msort).

## 1. Family Summary
This family is a two-phase distributed sort. Each slave first gathers and locally sorts its input. If it spills, it keeps samples for partitioning and writes sorted overflow through the dedicated msort spill path. The master then computes or reuses split points, adjusts them for spilled nodes, checks skew, and instructs each slave to merge its local slice with zero or more remote streams.

There is also a small-data `MiniSort` path that bypasses the full split-map plus socket-merge workflow when all nodes stayed in memory and the aggregate memory estimate is below the configured threshold.

## 2. Main Anchors
- [../../thorlcr/activities/msort/thmsort.cpp#L127](../../thorlcr/activities/msort/thmsort.cpp#L127) `CMSortActivityMaster::process()`
- [../../thorlcr/activities/msort/thmsortslave.cpp#L81](../../thorlcr/activities/msort/thmsortslave.cpp#L81) `MSortSlaveActivity::start()`
- [../../thorlcr/activities/msort/thmsortslave.cpp#L148](../../thorlcr/activities/msort/thmsortslave.cpp#L148) `MSortSlaveActivity::stop()`
- [../../thorlcr/activities/msort/thmsortslave.cpp#L163](../../thorlcr/activities/msort/thmsortslave.cpp#L163) `MSortSlaveActivity::reset()`
- [../../thorlcr/msort/tsortm.cpp#L1147](../../thorlcr/msort/tsortm.cpp#L1147) master `Sort()` partition planning
- [../../thorlcr/msort/tsortm.cpp#L1290](../../thorlcr/msort/tsortm.cpp#L1290) overflow adjustment phase
- [../../thorlcr/msort/tsorts.cpp#L1329](../../thorlcr/msort/tsorts.cpp#L1329) `CThorSorter::Gather()` spill-aware local gather
- [../../thorlcr/msort/tsorts.cpp#L788](../../thorlcr/msort/tsorts.cpp#L788) `AdjustOverflow()`
- [../../thorlcr/msort/tsorts1.cpp#L551](../../thorlcr/msort/tsorts1.cpp#L551) per-target merge reader set
- [../../thorlcr/msort/tsorts.cpp#L482](../../thorlcr/msort/tsorts.cpp#L482) `MiniSort` fast path

## 3. Confirmed Optimization Opportunities
### A. Fix the `MiniSort` primary-node re-sort path
The `MiniSort` fast path already documents that the primary side is not optimal: it receives already sorted remote data, but the current path still resorts the combined data rather than merging presorted runs at [../../thorlcr/msort/tsorts.cpp#L482](../../thorlcr/msort/tsorts.cpp#L482), [../../thorlcr/msort/tsorts.cpp#L526](../../thorlcr/msort/tsorts.cpp#L526), and [../../thorlcr/msort/tsorts.cpp#L560](../../thorlcr/msort/tsorts.cpp#L560).

That is a direct, code-confirmed fast-path optimization opportunity.

### B. Reduce linear spill-boundary correction work
After sample-based partitioning, `AdjustOverflow()` walks backward row by row through spill data until it finds the first row below the split key at [../../thorlcr/msort/tsorts.cpp#L788](../../thorlcr/msort/tsorts.cpp#L788) and [../../thorlcr/msort/tsorts.cpp#L801](../../thorlcr/msort/tsorts.cpp#L801).

For duplicate-heavy keys or coarse sample intervals, that is guaranteed extra spill-file work. Any more direct boundary lookup or coarse index would attack a confirmed hotspot.

### C. Reuse the distributed sorter across reset when possible
`MSortSlaveActivity::reset()` recreates the sorter and explicitly notes that reuse would be better at [../../thorlcr/activities/msort/thmsortslave.cpp#L167](../../thorlcr/activities/msort/thmsortslave.cpp#L167). That implies repeated sorter and transfer-server setup even when the activity shape has not changed.

If lifecycle constraints allow it, sorter reuse is a confirmed efficiency opportunity.

## 4. Plausible But Unverified Opportunities
### A. Transport batching may be conservative for current networks
The merge transport uses fixed-size buffering and fairness-oriented send behavior in [../../thorlcr/msort/tsorts.cpp#L58](../../thorlcr/msort/tsorts.cpp#L58), [../../thorlcr/msort/tsorts1.cpp#L196](../../thorlcr/msort/tsorts1.cpp#L196), and [../../thorlcr/msort/tsortl.cpp#L294](../../thorlcr/msort/tsortl.cpp#L294). That may be under-batching on modern links, but it needs throughput measurements before any change.

### B. Reader fan-in may dominate on wider clusters
Each target partition merges one reader per contributing source at [../../thorlcr/msort/tsorts1.cpp#L537](../../thorlcr/msort/tsorts1.cpp#L537) and [../../thorlcr/msort/tsorts.cpp#L1232](../../thorlcr/msort/tsorts.cpp#L1232). A hierarchical merge or stronger source-side coalescing may help, but only if cluster-size scaling data shows local fan-in dominates.

### C. Gather metadata could expose stronger planning signals
For spilled gathers, the master sees sample-oriented information rather than exact local row count in [../../thorlcr/msort/tsorts.cpp#L895](../../thorlcr/msort/tsorts.cpp#L895) and [../../thorlcr/msort/tsortm.cpp#L1147](../../thorlcr/msort/tsortm.cpp#L1147). Better metadata could improve partitioning decisions, but this pass does not prove it would materially reduce elapsed time.

## 5. Areas That Need Measurement
- partition-build time versus merge time on real global-sort workloads
- how often `AdjustOverflow()` backtracks only a few rows versus long duplicate-heavy runs
- merge read-side and network costs, which are less visible than spill-write statistics today
- merge fan-in behavior on large clusters and under TLS
- `MiniSort` hit rate and sorter-reset frequency in production workflows

## 6. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/msort/thmsort.cpp](../../thorlcr/activities/msort/thmsort.cpp)
2. [../../thorlcr/activities/msort/thmsortslave.cpp](../../thorlcr/activities/msort/thmsortslave.cpp)
3. [../../thorlcr/msort/tsortm.cpp](../../thorlcr/msort/tsortm.cpp)
4. [../../thorlcr/msort/tsorts.cpp](../../thorlcr/msort/tsorts.cpp)
5. [../../thorlcr/msort/tsorts1.cpp](../../thorlcr/msort/tsorts1.cpp)