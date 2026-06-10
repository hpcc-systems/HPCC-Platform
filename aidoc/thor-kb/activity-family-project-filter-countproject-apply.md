# Thor Activity Family: Project / Filter / CountProject / Apply

This document covers the unary row-shaping family rooted in [../../thorlcr/activities/project](../../thorlcr/activities/project), [../../thorlcr/activities/filter](../../thorlcr/activities/filter), [../../thorlcr/activities/countproject](../../thorlcr/activities/countproject), and [../../thorlcr/activities/apply](../../thorlcr/activities/apply). Most of this family is row-at-a-time and single-input. The interesting outliers are the stranded `PROJECT` runtime, whole-group `FILTERGROUP`, and distributed `COUNTPROJECT`.

## 1. Family Summary
This family splits into a few distinct execution styles.

- Plain `FILTER`, `FILTERPROJECT`, local `COUNTPROJECT`, and much of `PROJECT` are straightforward single-row loops over one input stream.
- `PROJECT` is the multicore outlier because its parallelism comes from the strand runtime rather than from local buffering.
- `PREFETCH PROJECT` is the queueing outlier: it optionally precomputes `preTransform`, evaluates a child graph, and coordinates producer/consumer state through a bounded queue.
- `FILTERGROUP` is the whole-group outlier. It materializes one full group, evaluates a group predicate, then replays accepted rows.
- Global `COUNTPROJECT` is the distributed outlier. It installs lookahead, computes local row counts, and forwards prefix counts rank-to-rank before transforming rows.
- `APPLY` is adjacent but sink-like: it consumes rows for helper side effects and emits nothing.

The strongest confirmed optimization surface in this family is `FILTERGROUP`, because the current helper contract forces all-memory whole-group validation.

## 2. Main Anchors
- [../../thorlcr/activities/project/thprojectslave.cpp#L8](../../thorlcr/activities/project/thprojectslave.cpp#L8) `CProjecStrandProcessor`
- [../../thorlcr/activities/project/thprojectslave.cpp#L66](../../thorlcr/activities/project/thprojectslave.cpp#L66) `CProjectSlaveActivity`
- [../../thorlcr/activities/project/thprojectslave.cpp#L91](../../thorlcr/activities/project/thprojectslave.cpp#L91) `CPrefetchProjectSlaveActivity`
- [../../thorlcr/activities/project/thprojectslave.cpp#L109](../../thorlcr/activities/project/thprojectslave.cpp#L109) `PrefetchInfo`
- [../../thorlcr/activities/project/thprojectslave.cpp#L125](../../thorlcr/activities/project/thprojectslave.cpp#L125) `CPrefetcher`
- [../../thorlcr/graph/thgraphslave.cpp#L760](../../thorlcr/graph/thgraphslave.cpp#L760) strand splitter/recombiner setup in `CThorStrandedActivity::getOutputStreams()`
- [../../thorlcr/activities/filter/thfilterslave.cpp#L18](../../thorlcr/activities/filter/thfilterslave.cpp#L18) `CFilterSlaveActivityBase`
- [../../thorlcr/activities/filter/thfilterslave.cpp#L43](../../thorlcr/activities/filter/thfilterslave.cpp#L43) `CFilterSlaveActivity`
- [../../thorlcr/activities/filter/thfilterslave.cpp#L245](../../thorlcr/activities/filter/thfilterslave.cpp#L245) `CFilterGroupSlaveActivity`
- [../../thorlcr/activities/countproject/thcountprojectslave.cpp#L18](../../thorlcr/activities/countproject/thcountprojectslave.cpp#L18) `BaseCountProjectActivity`
- [../../thorlcr/activities/countproject/thcountprojectslave.cpp#L31](../../thorlcr/activities/countproject/thcountprojectslave.cpp#L31) `LocalCountProjectActivity`
- [../../thorlcr/activities/countproject/thcountprojectslave.cpp#L81](../../thorlcr/activities/countproject/thcountprojectslave.cpp#L81) `CountProjectActivity`
- [../../thorlcr/activities/apply/thapplyslave.cpp#L7](../../thorlcr/activities/apply/thapplyslave.cpp#L7) `CApplySlaveActivity`

## 3. Confirmed Optimization Opportunities
### A. Replace `FILTERGROUP` whole-group materialization with a streaming validation path
`FILTERGROUP` constructs its loader with `rc_allMem` at [../../thorlcr/activities/filter/thfilterslave.cpp#L258](../../thorlcr/activities/filter/thfilterslave.cpp#L258), fully materializes each group at [../../thorlcr/activities/filter/thfilterslave.cpp#L298](../../thorlcr/activities/filter/thfilterslave.cpp#L298), and the source explicitly says a stream-based `isValid` contract would avoid keeping the whole group in memory at [../../thorlcr/activities/filter/thfilterslave.cpp#L309](../../thorlcr/activities/filter/thfilterslave.cpp#L309).

This is the clearest confirmed hotspot in the family.

### B. Export prefetch, filter-hit, and prefix-count counters that the runtime already knows
The family already tracks important control-path state but does not surface it.

- Plain filter increments an internal `matched` counter at [../../thorlcr/activities/filter/thfilterslave.cpp#L57](../../thorlcr/activities/filter/thfilterslave.cpp#L57) without exporting it.
- Prefetch project tracks queue fullness and blocked readers around [../../thorlcr/activities/project/thprojectslave.cpp#L153](../../thorlcr/activities/project/thprojectslave.cpp#L153) through [../../thorlcr/activities/project/thprojectslave.cpp#L211](../../thorlcr/activities/project/thprojectslave.cpp#L211), but no queue-depth or wait counters are published.
- Global countproject has explicit send, receive, and wait points at [../../thorlcr/activities/countproject/thcountprojectslave.cpp#L90](../../thorlcr/activities/countproject/thcountprojectslave.cpp#L90) through [../../thorlcr/activities/countproject/thcountprojectslave.cpp#L140](../../thorlcr/activities/countproject/thcountprojectslave.cpp#L140), but no exported prefix-latency counters.

### C. Revisit global `COUNTPROJECT` lookahead cost once it is measurable
Global countproject always installs lookahead before it can start prefix-count transformation at [../../thorlcr/activities/countproject/thcountprojectslave.cpp#L123](../../thorlcr/activities/countproject/thcountprojectslave.cpp#L123) through [../../thorlcr/activities/countproject/thcountprojectslave.cpp#L156](../../thorlcr/activities/countproject/thcountprojectslave.cpp#L156). The buffer size is fixed at 12 MB by `COUNTPROJECT_SMART_BUFFER_SIZE` in [../../thorlcr/thorutil/thbufdef.hpp#L33](../../thorlcr/thorutil/thbufdef.hpp#L33).

This is a confirmed extra buffering surface, although it still needs measurement before treating it as a regression.

## 4. Plausible But Unverified Opportunities
### A. Revisit strand overhead for trivial `PROJECT` transforms
`PROJECT` parallelism comes from the strand runtime in [../../thorlcr/activities/project/thprojectslave.cpp#L8](../../thorlcr/activities/project/thprojectslave.cpp#L8) and [../../thorlcr/graph/thgraphslave.cpp#L760](../../thorlcr/graph/thgraphslave.cpp#L760). For tiny rows or very cheap transforms, split/recombine overhead may dominate, but this slice did not measure that cost.

### B. Revisit fixed preload in `PREFETCH PROJECT`
`CPrefetcher` uses queue and semaphore control with a default preload configured in the same file. The control points are at [../../thorlcr/activities/project/thprojectslave.cpp#L153](../../thorlcr/activities/project/thprojectslave.cpp#L153) through [../../thorlcr/activities/project/thprojectslave.cpp#L211](../../thorlcr/activities/project/thprojectslave.cpp#L211). An adaptive preload may fit better than a fixed one, but that remains a tuning lead.

### C. Revalidate stepped filter semantics before optimizing them
The stepped filter path still contains explicit uncertainty comments about group handling and mismatch return behavior at [../../thorlcr/activities/filter/thfilterslave.cpp#L98](../../thorlcr/activities/filter/thfilterslave.cpp#L98) and [../../thorlcr/activities/filter/thfilterslave.cpp#L123](../../thorlcr/activities/filter/thfilterslave.cpp#L123). The stepped `FILTERGROUP` fallback also carries a direct “doesn't look right” note at [../../thorlcr/activities/filter/thfilterslave.cpp#L393](../../thorlcr/activities/filter/thfilterslave.cpp#L393).

## 5. Areas That Need Measurement
- strand split/recombine wait and skew for cheap `PROJECT` transforms
- `PREFETCH PROJECT` queue depth, full waits, and `preTransform` rejection rate
- filter hit ratio and grouped rejection rate
- `FILTERGROUP` group size distribution and OOM-near behavior
- global `COUNTPROJECT` prefix-count latency and lookahead spill behavior
- helper start/apply/end time split for `APPLY`

## 6. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/project/thprojectslave.cpp](../../thorlcr/activities/project/thprojectslave.cpp)
2. [../../thorlcr/graph/thgraphslave.cpp](../../thorlcr/graph/thgraphslave.cpp)
3. [../../thorlcr/activities/filter/thfilterslave.cpp](../../thorlcr/activities/filter/thfilterslave.cpp)
4. [../../thorlcr/activities/countproject/thcountprojectslave.cpp](../../thorlcr/activities/countproject/thcountprojectslave.cpp)
5. [../../thorlcr/activities/apply/thapplyslave.cpp](../../thorlcr/activities/apply/thapplyslave.cpp)