# Thor Activity Family: Group / Degroup / Rollover

This document covers the small family rooted in [../../thorlcr/activities/group](../../thorlcr/activities/group) and [../../thorlcr/activities/degroup](../../thorlcr/activities/degroup). The interesting cost surface here is global group rollover; degroup itself is intentionally thin.

## 1. Family Summary
This family is asymmetric.

- `GroupSlaveActivity` handles both local regrouping and global boundary stitching.
- `CDegroupSlaveActivity` is largely a flattening adapter with stepping passthrough.

In local mode, group reconstructs group boundaries from flattened input using one-row lookahead. In global mode, each non-first node peels off its first group, materializes it through a spill-capable collector, and serves that group backward to the previous node so the previous node can emit a logically continuous group.

There is no row-loader usage in this family. The shared buffering/spill surface here is the global-group rollover collector plus its request/reply row transport.

## 2. Main Anchors
- [../../thorlcr/activities/group/thgroup.cpp#L30](../../thorlcr/activities/group/thgroup.cpp#L30) global group master mpTag setup
- [../../thorlcr/activities/group/thgroupslave.cpp#L23](../../thorlcr/activities/group/thgroupslave.cpp#L23) `GroupSlaveActivity`
- [../../thorlcr/activities/group/thgroupslave.cpp#L37](../../thorlcr/activities/group/thgroupslave.cpp#L37) `getNext()` flattening plus rollover switch
- [../../thorlcr/activities/group/thgroupslave.cpp#L87](../../thorlcr/activities/group/thgroupslave.cpp#L87) global rollover setup in `start()`
- [../../thorlcr/activities/group/thgroupslave.cpp#L143](../../thorlcr/activities/group/thgroupslave.cpp#L143) grouped output reconstruction in `nextRow()`
- [../../thorlcr/activities/group/thgroupslave.cpp#L172](../../thorlcr/activities/group/thgroupslave.cpp#L172) end-of-group accounting
- [../../thorlcr/activities/degroup/thdegroupslave.cpp#L20](../../thorlcr/activities/degroup/thdegroupslave.cpp#L20) `CDegroupSlaveActivity`
- [../../thorlcr/activities/degroup/thdegroupslave.cpp#L59](../../thorlcr/activities/degroup/thdegroupslave.cpp#L59) stepped degroup path
- [../../thorlcr/thorutil/thmem.hpp#L563](../../thorlcr/thorutil/thmem.hpp#L563) collector API used for rollover buffering
- [../../thorlcr/thorutil/thormisc.cpp#L1241](../../thorlcr/thorutil/thormisc.cpp#L1241) rollover row transport

## 3. Confirmed Optimization Opportunities
### A. Remove redundant degroup earlier in planning when input is already ungrouped
The activity explicitly detects already-ungrouped input at [../../thorlcr/activities/degroup/thdegroupslave.cpp#L33](../../thorlcr/activities/degroup/thdegroupslave.cpp#L33) through [../../thorlcr/activities/degroup/thdegroupslave.cpp#L37](../../thorlcr/activities/degroup/thdegroupslave.cpp#L37), and then the runtime path becomes a plain passthrough flatten/read adapter.

That is a confirmed case where earlier graph simplification could remove an activity without changing behavior.

### B. Export rollover-specific counters that the runtime already knows
Global group already knows how many rows are buffered and sent backward through the rollover path at [../../thorlcr/activities/group/thgroupslave.cpp#L127](../../thorlcr/activities/group/thgroupslave.cpp#L127) through [../../thorlcr/activities/group/thgroupslave.cpp#L129](../../thorlcr/activities/group/thgroupslave.cpp#L129), and the collector API exposes spill-related state.

But current activity stats only publish `StNumGroups` and `StNumGroupMax` at [../../thorlcr/activities/group/thgroupslave.cpp#L196](../../thorlcr/activities/group/thgroupslave.cpp#L196) through [../../thorlcr/activities/group/thgroupslave.cpp#L200](../../thorlcr/activities/group/thgroupslave.cpp#L200). This is a confirmed observability defect for rollover-heavy workloads.

### C. Export stitched-boundary counts
The global path already knows where cross-row boundaries are decided at [../../thorlcr/activities/group/thgroupslave.cpp#L154](../../thorlcr/activities/group/thgroupslave.cpp#L154) through [../../thorlcr/activities/group/thgroupslave.cpp#L157](../../thorlcr/activities/group/thgroupslave.cpp#L157). Reporting how often global stitching actually happened would make tuning much less speculative.

## 4. Plausible But Unverified Opportunities
### A. Replace the generic collector with a lighter contiguous-group rollover buffer
The current path uses the generic collector append/spill/getStream machinery at [../../thorlcr/activities/group/thgroupslave.cpp#L102](../../thorlcr/activities/group/thgroupslave.cpp#L102) through [../../thorlcr/activities/group/thgroupslave.cpp#L129](../../thorlcr/activities/group/thgroupslave.cpp#L129). Group rollover does not need sorting or random access, so a narrower structure may be cheaper.

### B. Revisit rollover transport granularity
The request/reply transport is pull-based and batches into fixed-size chunks in [../../thorlcr/thorutil/thormisc.cpp#L1267](../../thorlcr/thorutil/thormisc.cpp#L1267) through [../../thorlcr/thorutil/thormisc.cpp#L1380](../../thorlcr/thorutil/thormisc.cpp#L1380). That may be too chatty or too coarse depending on row width and first-group size.

### C. Revisit start-to-first-row latency on non-first nodes
Non-first global-group nodes complete rollover setup before yielding output at [../../thorlcr/activities/group/thgroupslave.cpp#L87](../../thorlcr/activities/group/thgroupslave.cpp#L87) through [../../thorlcr/activities/group/thgroupslave.cpp#L129](../../thorlcr/activities/group/thgroupslave.cpp#L129). Some of that work may be deferrable or overlap-friendly.

## 5. Areas That Need Measurement
- first-group size histogram per slave and what fraction of total rows goes through rollover
- collector spill count, bytes, and elapsed time for rollover buffering
- rollover message count, payload size, and blocked wait time in the row transport
- non-first-node start-to-first-row delay for global group versus local group
- frequency of redundant degroup on already-ungrouped input
- stepped degroup selectivity versus plain flattening cost

## 6. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/group/thgroup.cpp](../../thorlcr/activities/group/thgroup.cpp)
2. [../../thorlcr/activities/group/thgroupslave.cpp](../../thorlcr/activities/group/thgroupslave.cpp)
3. [../../thorlcr/activities/degroup/thdegroupslave.cpp](../../thorlcr/activities/degroup/thdegroupslave.cpp)
4. [../../thorlcr/thorutil/thormisc.cpp](../../thorlcr/thorutil/thormisc.cpp)
5. [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md)