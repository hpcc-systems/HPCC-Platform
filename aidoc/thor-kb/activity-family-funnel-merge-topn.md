# Thor Activity Family: Funnel / Merge / TopN

This document covers the fan-in family rooted in [../../thorlcr/activities/funnel](../../thorlcr/activities/funnel), [../../thorlcr/activities/merge](../../thorlcr/activities/merge), and [../../thorlcr/activities/topn](../../thorlcr/activities/topn). These activities all combine multiple ordered or unordered sources, but their buffering and materialization behavior is very different.

## 1. Family Summary
This family splits into three distinct behaviors.

- Funnel is the only operator here with an explicitly unordered path. The ungrouped unordered case uses one reader thread per input plus a bounded shared queue, while ordered or grouped funneling drains one active stream at a time.
- Merge is always ordered at the activity boundary. The local path is a straight ordered fan-in, but the global path is much heavier: it gathers, writes a temp file, samples and partitions that file, then rereads partition slices locally and remotely.
- TopN is an ordered selection operator rather than a full merge. It keeps only the best `N` rows, reuses row-table allocation across groups, and in global mode merges partial tops through a fixed-arity network tree.

The strongest confirmed optimization surface in this family is global merge. Funnel and TopN both show plausible opportunities, but they need measurement first.

## 2. Main Anchors
- [../../thorlcr/activities/funnel/thfunnelslave.cpp#L34](../../thorlcr/activities/funnel/thfunnelslave.cpp#L34) `CParallelFunnel`
- [../../thorlcr/activities/funnel/thfunnelslave.cpp#L346](../../thorlcr/activities/funnel/thfunnelslave.cpp#L346) `FunnelSlaveActivity`
- [../../thorlcr/thorutil/thbufdef.hpp#L47](../../thorlcr/thorutil/thbufdef.hpp#L47) funnel queue thresholds
- [../../thorlcr/activities/merge/thmerge.cpp#L36](../../thorlcr/activities/merge/thmerge.cpp#L36) `GlobalMergeActivityMaster`
- [../../thorlcr/activities/merge/thmergeslave.cpp#L36](../../thorlcr/activities/merge/thmergeslave.cpp#L36) `GlobalMergeSlaveActivity`
- [../../thorlcr/activities/merge/thmergeslave.cpp#L158](../../thorlcr/activities/merge/thmergeslave.cpp#L158) remote reply reserialization in `cProvider::run()`
- [../../thorlcr/activities/merge/thmergeslave.cpp#L200](../../thorlcr/activities/merge/thmergeslave.cpp#L200) `createPartitionMerger()`
- [../../thorlcr/activities/merge/thmergeslave.cpp#L319](../../thorlcr/activities/merge/thmergeslave.cpp#L319) global merge gather and temp-file creation
- [../../thorlcr/activities/merge/thmergeslave.cpp#L375](../../thorlcr/activities/merge/thmergeslave.cpp#L375) `getRows()` temp-file reread path
- [../../thorlcr/activities/merge/thmergeslave.cpp#L428](../../thorlcr/activities/merge/thmergeslave.cpp#L428) `LocalMergeSlaveActivity`
- [../../thorlcr/activities/merge/thmergeslave.cpp#L533](../../thorlcr/activities/merge/thmergeslave.cpp#L533) `CNWayMergeActivity`
- [../../thorlcr/activities/topn/thtopn.cpp#L8](../../thorlcr/activities/topn/thtopn.cpp#L8) fixed global `MERGE_GRANULARITY`
- [../../thorlcr/activities/topn/thtopnslave.cpp#L112](../../thorlcr/activities/topn/thtopnslave.cpp#L112) `TopNSlaveActivity::getNextSortGroup()`
- [../../thorlcr/activities/topn/thtopnslave.cpp#L128](../../thorlcr/activities/topn/thtopnslave.cpp#L128) TopN admit/reject path

## 3. Confirmed Optimization Opportunities
### A. Eliminate the deserialize plus reserialize bounce in global merge replies
The global merge reply path materializes rows with `getRows()`, then serializes them back into a message in `cProvider::run()` at [../../thorlcr/activities/merge/thmergeslave.cpp#L158](../../thorlcr/activities/merge/thmergeslave.cpp#L158) through [../../thorlcr/activities/merge/thmergeslave.cpp#L161](../../thorlcr/activities/merge/thmergeslave.cpp#L161).

The source itself calls this out as wasteful. This is a confirmed extra CPU and memory-copy cost in the remote partition-serving path.

### B. Reduce the guaranteed temp-file write/read cycle in global merge
Global merge always locally merges all inputs, writes every row to a temp file, then reopens slices from that same file for partition serving and final merge at [../../thorlcr/activities/merge/thmergeslave.cpp#L319](../../thorlcr/activities/merge/thmergeslave.cpp#L319) through [../../thorlcr/activities/merge/thmergeslave.cpp#L356](../../thorlcr/activities/merge/thmergeslave.cpp#L356), plus [../../thorlcr/activities/merge/thmergeslave.cpp#L375](../../thorlcr/activities/merge/thmergeslave.cpp#L375).

That is a confirmed write/read/rematerialization cycle even before remote consumers are considered.

### C. Export queue, reread, and admit/reject counters
The family already knows key runtime facts but does not publish them.

- Parallel funnel tracks shared queue state around [../../thorlcr/activities/funnel/thfunnelslave.cpp#L164](../../thorlcr/activities/funnel/thfunnelslave.cpp#L164) through [../../thorlcr/activities/funnel/thfunnelslave.cpp#L206](../../thorlcr/activities/funnel/thfunnelslave.cpp#L206), but it does not export queue depth or wait time.
- Global merge knows tmpfile partition boundaries and serves local versus remote slices in [../../thorlcr/activities/merge/thmergeslave.cpp#L200](../../thorlcr/activities/merge/thmergeslave.cpp#L200) through [../../thorlcr/activities/merge/thmergeslave.cpp#L220](../../thorlcr/activities/merge/thmergeslave.cpp#L220), but it does not expose reread bytes or per-partition fan-in.
- TopN decides which rows are inserted or rejected at [../../thorlcr/activities/topn/thtopnslave.cpp#L128](../../thorlcr/activities/topn/thtopnslave.cpp#L128) through [../../thorlcr/activities/topn/thtopnslave.cpp#L133](../../thorlcr/activities/topn/thtopnslave.cpp#L133), but it does not export rows seen, kept, replaced, or rejected.

## 4. Plausible But Unverified Opportunities
### A. Revisit ordered funnel startup behavior
Ordered funnel starts every input up front with `std::async` in [../../thorlcr/activities/funnel/thfunnelslave.cpp#L402](../../thorlcr/activities/funnel/thfunnelslave.cpp#L402) and [../../thorlcr/activities/funnel/thfunnelslave.cpp#L417](../../thorlcr/activities/funnel/thfunnelslave.cpp#L417), even though serial drain later consumes one input at a time at [../../thorlcr/activities/funnel/thfunnelslave.cpp#L462](../../thorlcr/activities/funnel/thfunnelslave.cpp#L462). That may be wasted startup work for expensive sources.

### B. Revisit funnel queue sizing and throttling
The unordered path uses fixed thresholds from [../../thorlcr/thorutil/thbufdef.hpp#L47](../../thorlcr/thorutil/thbufdef.hpp#L47) and single-waiter throttling around [../../thorlcr/activities/funnel/thfunnelslave.cpp#L174](../../thorlcr/activities/funnel/thfunnelslave.cpp#L174) and [../../thorlcr/activities/funnel/thfunnelslave.cpp#L206](../../thorlcr/activities/funnel/thfunnelslave.cpp#L206). The right tradeoff depends on row width, input count, and upstream pacing.

### C. Revisit TopN data structure and merge-tree shape
TopN currently uses stable `binaryInsert()` into an in-memory row array at [../../thorlcr/activities/topn/thtopnslave.cpp#L128](../../thorlcr/activities/topn/thtopnslave.cpp#L128) and [../../thorlcr/activities/topn/thtopnslave.cpp#L133](../../thorlcr/activities/topn/thtopnslave.cpp#L133), and global TopN uses a fixed fan-in of 4 at [../../thorlcr/activities/topn/thtopn.cpp#L8](../../thorlcr/activities/topn/thtopn.cpp#L8). A heap or a different merge-tree shape may win for larger `N` or different cluster sizes, but that is not proven here.

## 5. Areas That Need Measurement
- funnel queue depth, blocked writer time, and per-input startup cost
- global merge tmpfile bytes written versus reread
- global merge local-versus-remote served bytes and partition skew
- merge stepping usefulness in `nextRowGE()` paths
- TopN rows seen versus inserted versus rejected
- grouped TopN cardinality distribution and high-water allocation behavior
- real upstream savings from TopN early `stopInput(0)`

## 6. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/funnel/thfunnelslave.cpp](../../thorlcr/activities/funnel/thfunnelslave.cpp)
2. [../../thorlcr/activities/merge/thmerge.cpp](../../thorlcr/activities/merge/thmerge.cpp)
3. [../../thorlcr/activities/merge/thmergeslave.cpp](../../thorlcr/activities/merge/thmergeslave.cpp)
4. [../../thorlcr/activities/topn/thtopn.cpp](../../thorlcr/activities/topn/thtopn.cpp)
5. [../../thorlcr/activities/topn/thtopnslave.cpp](../../thorlcr/activities/topn/thtopnslave.cpp)