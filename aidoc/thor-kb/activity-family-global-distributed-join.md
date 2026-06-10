# Thor Activity Family: Global and Distributed Join

This document covers the distributed sort-merge join path implemented by [../../thorlcr/activities/join/thjoin.cpp](../../thorlcr/activities/join/thjoin.cpp) and [../../thorlcr/activities/join/thjoinslave.cpp](../../thorlcr/activities/join/thjoinslave.cpp) when the activity is not routed through the local/grouped branch.

## 1. Family Summary
This family is mostly a join-specific wrapper around the distributed sorter. The master chooses the partition side and phase order, each slave gathers and merges one or both sides through the sorter, and the resulting prepared streams are handed to the same join helper used by the local path.

The dominant costs are orchestration costs rather than row matching itself:

- repartition planning
- barrier synchronization
- primary-stream parking between phases
- sorter spill plus one additional join-local spill surface

## 2. Main Anchors
- [../../thorlcr/activities/join/thjoin.cpp#L98](../../thorlcr/activities/join/thjoin.cpp#L98) MP-tag and barrier setup
- [../../thorlcr/activities/join/thjoin.cpp#L156](../../thorlcr/activities/join/thjoin.cpp#L156) partition-side selection and sliding-match detection
- [../../thorlcr/activities/join/thjoin.cpp#L230](../../thorlcr/activities/join/thjoin.cpp#L230) primary-first two-phase distributed protocol
- [../../thorlcr/activities/join/thjoin.cpp#L289](../../thorlcr/activities/join/thjoin.cpp#L289) presorted-partition-side branch
- [../../thorlcr/activities/join/thjoinslave.cpp#L181](../../thorlcr/activities/join/thjoinslave.cpp#L181) distributed slave init and sorter creation
- [../../thorlcr/activities/join/thjoinslave.cpp#L264](../../thorlcr/activities/join/thjoinslave.cpp#L264) async secondary startup and lookahead
- [../../thorlcr/activities/join/thjoinslave.cpp#L486](../../thorlcr/activities/join/thjoinslave.cpp#L486) `doglobaljoin()`
- [../../thorlcr/activities/join/thjoinslave.cpp#L559](../../thorlcr/activities/join/thjoinslave.cpp#L559) primary gather, barrier, and rematerialization path
- [../../thorlcr/activities/join/thjoinslave.cpp#L587](../../thorlcr/activities/join/thjoinslave.cpp#L587) secondary gather and final merge handoff
- [../../thorlcr/thorutil/thbuf.cpp#L1264](../../thorlcr/thorutil/thbuf.cpp#L1264) overflowable buffer implementation used for primary parking

## 3. Confirmed Optimization Opportunities
### A. Remove or reduce primary-side rematerialization
After the primary side has been globally gathered and merged, the slave copies that merged stream into `createOverflowableBuffer()` at [../../thorlcr/activities/join/thjoinslave.cpp#L572](../../thorlcr/activities/join/thjoinslave.cpp#L572) through [../../thorlcr/activities/join/thjoinslave.cpp#L575](../../thorlcr/activities/join/thjoinslave.cpp#L575), then later replays it into the join helper.

That is a confirmed extra full-row pass. Because the parked stream is spill-capable, it is also a confirmed second spill surface beyond the sorter gather path.

### B. Add a distributed empty-side shortcut analogous to the local path
The local join path can discard the RHS for empty-LHS inner joins at [../../thorlcr/activities/join/thjoinslave.cpp#L464](../../thorlcr/activities/join/thjoinslave.cpp#L464) through [../../thorlcr/activities/join/thjoinslave.cpp#L469](../../thorlcr/activities/join/thjoinslave.cpp#L469).

By contrast, the global path only disables partition reuse when the primary global count is zero at [../../thorlcr/activities/join/thjoinslave.cpp#L582](../../thorlcr/activities/join/thjoinslave.cpp#L582) through [../../thorlcr/activities/join/thjoinslave.cpp#L583](../../thorlcr/activities/join/thjoinslave.cpp#L583), then still proceeds into secondary gather at [../../thorlcr/activities/join/thjoinslave.cpp#L587](../../thorlcr/activities/join/thjoinslave.cpp#L587) through [../../thorlcr/activities/join/thjoinslave.cpp#L589](../../thorlcr/activities/join/thjoinslave.cpp#L589).

For non-right-outer shapes, that is confirmed wasted distributed work.

## 4. Plausible But Unverified Opportunities
### A. Revisit a join-specific small-data fast path
The join master passes `0` as the small-sort threshold in the distributed join sort calls at [../../thorlcr/activities/join/thjoin.cpp#L237](../../thorlcr/activities/join/thjoin.cpp#L237), [../../thorlcr/activities/join/thjoin.cpp#L264](../../thorlcr/activities/join/thjoin.cpp#L264), and [../../thorlcr/activities/join/thjoin.cpp#L296](../../thorlcr/activities/join/thjoin.cpp#L296), while the generic sorter has a `MiniSort` gate. A join-specific fast path may exist, but the secondary cosort dependence on partition information makes this a design question, not a simple knob change.

### B. Reduce serialized phase boundaries if overlap can be preserved safely
The master and slave both serialize primary sort, primary merge completion, secondary setup, and secondary sort through the barrier cadence in [../../thorlcr/activities/join/thjoin.cpp#L230](../../thorlcr/activities/join/thjoin.cpp#L280) and [../../thorlcr/activities/join/thjoinslave.cpp#L559](../../thorlcr/activities/join/thjoinslave.cpp#L604). More overlap may be possible, but this pass does not prove it would help net throughput.

### C. Revisit the spill policy of the parked primary stream
The parked primary stream uses the default overflowable-buffer behavior at [../../thorlcr/activities/join/thjoinslave.cpp#L572](../../thorlcr/activities/join/thjoinslave.cpp#L572). The code alone does not show whether that spill priority is helping or hurting under pressure.

## 5. Areas That Need Measurement
- spill bytes and elapsed time for the parked primary stream
- barrier wait breakdown across the distributed join phases
- frequency of eligible empty-side distributed joins
- how much useful overlap remains after async secondary startup
- presorted partition-side quality under skew and duplicate-heavy keys

## 6. Observability Notes
The current path already contains a direct observability gap: `doglobaljoin()` leaves a `MORE` note for missing statistics from spilling the parked primary stream at [../../thorlcr/activities/join/thjoinslave.cpp#L591](../../thorlcr/activities/join/thjoinslave.cpp#L592).

That should be fixed before trying to rank distributed-join spill optimizations confidently.

## 7. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/join/thjoin.cpp](../../thorlcr/activities/join/thjoin.cpp)
2. [../../thorlcr/activities/join/thjoinslave.cpp](../../thorlcr/activities/join/thjoinslave.cpp)
3. [../../thorlcr/msort/tsortm.cpp](../../thorlcr/msort/tsortm.cpp)
4. [../../thorlcr/msort/tsorts.cpp](../../thorlcr/msort/tsorts.cpp)
5. [storage-and-spill-io.md](storage-and-spill-io.md)