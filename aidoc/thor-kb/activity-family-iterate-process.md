# Thor Activity Family: Iterate and Process

This document covers Thor's iterate and process activities implemented by [../../thorlcr/activities/iterate/thiterateslave.cpp](../../thorlcr/activities/iterate/thiterateslave.cpp), [../../thorlcr/activities/iterate/thgroupiterateslave.cpp](../../thorlcr/activities/iterate/thgroupiterateslave.cpp), and the master setup in [../../thorlcr/activities/iterate/thiterate.cpp](../../thorlcr/activities/iterate/thiterate.cpp). It intentionally excludes `loop`, which is queued as a separate, more complex control-flow slice.

## 1. Family Summary
This family carries row state across a stream rather than building large buffered collections.

- `ITERATE` keeps one previous row and transforms the next input row against it.
- `PROCESS` keeps a right-side state row and emits a left output row plus the next right-state row.
- Global variants serialize state across slave boundaries using an `mpTag` handoff.
- Grouped variants reset state at group boundaries and stay local to one slave.

That makes this family simpler than `loop`, but it still has real hot-path costs in row finalization, right-state handling, and global cross-rank serialization.

## 2. Main Anchors
- [../../thorlcr/activities/iterate/thiterateslave.cpp#L25](../../thorlcr/activities/iterate/thiterateslave.cpp#L25) `IterateSlaveActivityBase`
- [../../thorlcr/activities/iterate/thiterateslave.cpp#L102](../../thorlcr/activities/iterate/thiterateslave.cpp#L102) ungrouped `IterateSlaveActivity`
- [../../thorlcr/activities/iterate/thiterateslave.cpp#L181](../../thorlcr/activities/iterate/thiterateslave.cpp#L181) ungrouped `CProcessSlaveActivity`
- [../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L24](../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L24) grouped `GroupIterateSlaveActivity`
- [../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L92](../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L92) grouped `GroupProcessSlaveActivity`
- [../../thorlcr/activities/iterate/thiterate.cpp#L21](../../thorlcr/activities/iterate/thiterate.cpp#L21) master `mpTag` allocation for global variants
- [../../thorlcr/thorutil/thbufdef.hpp#L11](../../thorlcr/thorutil/thbufdef.hpp#L11) dedicated iterate and process smart-buffer constants

## 3. Confirmed Optimization Opportunities
### A. Global iterate and process still use the wrong smart-buffer knob
The shared global start path installs lookahead with `ENTH_SMART_BUFFER_SIZE` at [../../thorlcr/activities/iterate/thiterateslave.cpp#L78](../../thorlcr/activities/iterate/thiterateslave.cpp#L78) through [../../thorlcr/activities/iterate/thiterateslave.cpp#L85](../../thorlcr/activities/iterate/thiterateslave.cpp#L85).

But Thor defines `ITERATE_SMART_BUFFER_SIZE` and `PROCESS_SMART_BUFFER_SIZE` separately at [../../thorlcr/thorutil/thbufdef.hpp#L11](../../thorlcr/thorutil/thbufdef.hpp#L11) through [../../thorlcr/thorutil/thbufdef.hpp#L14](../../thorlcr/thorutil/thbufdef.hpp#L14), and the resource estimator already uses the iterate-specific constant at [../../ecl/hqlcpp/hqlresource.cpp#L1879](../../ecl/hqlcpp/hqlresource.cpp#L1879) through [../../ecl/hqlcpp/hqlresource.cpp#L1882](../../ecl/hqlcpp/hqlresource.cpp#L1882).

Today the constants happen to share the same size, so this is mostly a confirmed tuning and maintainability defect. It prevents independent adjustment when iterate and process workloads want different buffering.

### B. Ungrouped process still pays redundant left-output size discovery
`helper->transform()` already returns `thisSize` for the emitted left row at [../../thorlcr/activities/iterate/thiterateslave.cpp#L232](../../thorlcr/activities/iterate/thiterateslave.cpp#L232) through [../../thorlcr/activities/iterate/thiterateslave.cpp#L240](../../thorlcr/activities/iterate/thiterateslave.cpp#L240).

But the code recomputes the output size with `queryOutputMeta()->getRecordSize(nextl.getSelf())` at [../../thorlcr/activities/iterate/thiterateslave.cpp#L239](../../thorlcr/activities/iterate/thiterateslave.cpp#L239) before finalizing. The grouped process path does not do that; it finalizes the left row directly with `thisSize` at [../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L163](../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L163) through [../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L169](../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L169).

That makes the ungrouped process path a concrete hot-loop cleanup candidate.

### C. Process variants keep right-state size discovery in the row hot path
Ungrouped process computes the next right-state size with `helper->queryRightRecordSize()->getRecordSize(nextr.getSelf())` at [../../thorlcr/activities/iterate/thiterateslave.cpp#L236](../../thorlcr/activities/iterate/thiterateslave.cpp#L236), and grouped process computes it through output metadata at [../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L164](../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L164).

The ungrouped code explicitly tags this spot with `better TBD?`, so this is a confirmed per-row tuning target for fixed-size or metadata-stable workloads.

### D. Global iterate and process are serial across slaves but under-report that cost
The global chain is explicit in `getFirst()` and `putNext()` at [../../thorlcr/activities/iterate/thiterateslave.cpp#L39](../../thorlcr/activities/iterate/thiterateslave.cpp#L39) through [../../thorlcr/activities/iterate/thiterateslave.cpp#L76](../../thorlcr/activities/iterate/thiterateslave.cpp#L76), and the source comment already says the global path is serial at [../../thorlcr/activities/iterate/thiterateslave.cpp#L78](../../thorlcr/activities/iterate/thiterateslave.cpp#L78).

But `getMetaInfo()` never marks the activity sequential in [../../thorlcr/activities/iterate/thiterateslave.cpp#L153](../../thorlcr/activities/iterate/thiterateslave.cpp#L153) through [../../thorlcr/activities/iterate/thiterateslave.cpp#L166](../../thorlcr/activities/iterate/thiterateslave.cpp#L166), unlike global rollup at [../../thorlcr/activities/rollup/throllupslave.cpp#L192](../../thorlcr/activities/rollup/throllupslave.cpp#L192) through [../../thorlcr/activities/rollup/throllupslave.cpp#L197](../../thorlcr/activities/rollup/throllupslave.cpp#L197). The family also lacks dedicated counters for rank-to-rank handoff wait versus row-transform time.

## 4. Plausible But Unverified Opportunities
### A. Revisit local iterate and process without lookahead on stall-prone inputs
The shared start path installs lookahead only for global variants at [../../thorlcr/activities/iterate/thiterateslave.cpp#L78](../../thorlcr/activities/iterate/thiterateslave.cpp#L78) through [../../thorlcr/activities/iterate/thiterateslave.cpp#L85](../../thorlcr/activities/iterate/thiterateslave.cpp#L85). That may be worth measuring on local inputs with expensive upstream stalls, but the current code suggests the choice was deliberate.

### B. Revisit grouped boundary control flow before carrying it forward
Both grouped variants keep `eogNext` state in [../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L32](../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L32) through [../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L64](../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L64) and [../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L110](../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L110) through [../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L145](../../thorlcr/activities/iterate/thgroupiterateslave.cpp#L145), but this pass did not find a local write to `true`. That looks suspicious, but grouped end-of-group semantics are sensitive enough that it belongs in the plausible bucket until traced more deeply.

## 5. Areas That Need Measurement
- realized wait time in the global rank-to-rank handoff path
- frequency and cost of ungrouped process output-size recomputation
- fixed-size versus variable-size right-state behavior in `PROCESS`
- whether local iterate/process benefit from optional lookahead on real workloads
- grouped iterate/process branch behavior around end-of-group transitions

## 6. Observability Notes
This family is light on explicit family-specific counters.

- the visible stats are mostly generic activity timing and row counts
- the global serial handoff is not exported as a dedicated wait metric
- metadata does not advertise the global sequential shape as clearly as other serial families do

## 7. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/iterate/thiterateslave.cpp](../../thorlcr/activities/iterate/thiterateslave.cpp)
2. [../../thorlcr/activities/iterate/thgroupiterateslave.cpp](../../thorlcr/activities/iterate/thgroupiterateslave.cpp)
3. [../../thorlcr/activities/iterate/thiterate.cpp](../../thorlcr/activities/iterate/thiterate.cpp)
4. [../../thorlcr/thorutil/thbufdef.hpp](../../thorlcr/thorutil/thbufdef.hpp)
5. [../../ecl/hqlcpp/hqlresource.cpp](../../ecl/hqlcpp/hqlresource.cpp)