# Thor Activity Family: Result / Materialization / Write Sinks

This document covers the sink and result-materialization family rooted in [../../thorlcr/activities/result](../../thorlcr/activities/result), [../../thorlcr/activities/diskwrite](../../thorlcr/activities/diskwrite), [../../thorlcr/activities/pipewrite](../../thorlcr/activities/pipewrite), [../../thorlcr/activities/indexwrite](../../thorlcr/activities/indexwrite), [../../thorlcr/activities/spill](../../thorlcr/activities/spill), and [../../thorlcr/activities/temptable](../../thorlcr/activities/temptable), plus the graph-side result containers in [../../thorlcr/graph](../../thorlcr/graph).

## 1. Family Summary
The common pattern in this family is slave-heavy execution with lightweight master aggregation.

- Explicit remote result activities are narrow: each slave sends at most one serialized row, and the master enforces exactly one non-empty result.
- Broader graph-result materialization is graph-owned rather than activity-owned. Distributed results are fetched, collated, and repacked through graph result containers.
- Physical write sinks mostly share the disk-write base and differ in formatting or target medium.
- `spill` is sink-adjacent rather than a pure sink: it writes each row to a temp file and still forwards the row downstream.
- `temptable` is also adjacent rather than persisted: it generates inline rows but does not own a durable sink path.

The strongest confirmed optimization surfaces in this family are distributed graph-result double materialization, single-part indexwrite serialization through node 1, per-row pipe recreation, and blank-part creation in the shared publish path.

## 2. Main Anchors
- [../../thorlcr/activities/result/thresultslave.cpp#L14](../../thorlcr/activities/result/thresultslave.cpp#L14) `CResultSlaveActivity`
- [../../thorlcr/activities/result/thresult.cpp#L18](../../thorlcr/activities/result/thresult.cpp#L18) `CResultActivityMaster`
- [../../thorlcr/graph/thgraphmaster.cpp#L2378](../../thorlcr/graph/thgraphmaster.cpp#L2378) `CCollatedResult`
- [../../thorlcr/graph/thgraphslave.cpp#L1478](../../thorlcr/graph/thgraphslave.cpp#L1478) slave distributed-result caching
- [../../thorlcr/activities/thdiskbase.cpp#L279](../../thorlcr/activities/thdiskbase.cpp#L279) shared write publish path
- [../../thorlcr/activities/pipewrite/thpwslave.cpp#L168](../../thorlcr/activities/pipewrite/thpwslave.cpp#L168) pipewrite row loop
- [../../thorlcr/activities/indexwrite/thindexwriteslave.cpp#L345](../../thorlcr/activities/indexwrite/thindexwriteslave.cpp#L345) single-part index transfer path
- [../../thorlcr/activities/spill/thspillslave.cpp#L18](../../thorlcr/activities/spill/thspillslave.cpp#L18) `SpillSlaveActivity`
- [../../thorlcr/activities/temptable/thtmptableslave.cpp#L53](../../thorlcr/activities/temptable/thtmptableslave.cpp#L53) inline table generation

## 3. Confirmed Optimization Opportunities
### A. Reduce double materialization in distributed graph-result collation
`CCollatedResult::ensure()` deserializes slave replies into per-slave `CThorExpandingRowArray` instances at [../../thorlcr/graph/thgraphmaster.cpp#L2406](../../thorlcr/graph/thgraphmaster.cpp#L2406), then appends those rows again into a new result buffer at [../../thorlcr/graph/thgraphmaster.cpp#L2461](../../thorlcr/graph/thgraphmaster.cpp#L2461).

That is a confirmed extra copy and extra spill boundary for distributed results.

### B. Remove the serial node-1 transfer bottleneck in single-part indexwrite
The single-part key path explicitly says nodes 2..N could or should start pushing data earlier at [../../thorlcr/activities/indexwrite/thindexwriteslave.cpp#L347](../../thorlcr/activities/indexwrite/thindexwriteslave.cpp#L347). The current implementation still pulls rows from each peer in sequence at [../../thorlcr/activities/indexwrite/thindexwriteslave.cpp#L366](../../thorlcr/activities/indexwrite/thindexwriteslave.cpp#L366) through [../../thorlcr/activities/indexwrite/thindexwriteslave.cpp#L378](../../thorlcr/activities/indexwrite/thindexwriteslave.cpp#L378).

This is a confirmed serialized transfer hotspot.

### C. Avoid per-row process lifecycle when pipewrite recreate semantics are unnecessary
The recreate path in `CPipeWriteSlaveActivity::write()` opens, closes, and verifies the pipe around every row at [../../thorlcr/activities/pipewrite/thpwslave.cpp#L168](../../thorlcr/activities/pipewrite/thpwslave.cpp#L168) through [../../thorlcr/activities/pipewrite/thpwslave.cpp#L181](../../thorlcr/activities/pipewrite/thpwslave.cpp#L181).

When helper semantics do not require per-row isolation, that cost is guaranteed overhead.

### D. Avoid blank-part creation in the shared write publish path when possible
The shared master publish path creates empty parts when the file descriptor is wider than the active cluster at [../../thorlcr/activities/thdiskbase.cpp#L319](../../thorlcr/activities/thdiskbase.cpp#L319), and the source explicitly says it would be far preferable to avoid that at [../../thorlcr/activities/thdiskbase.cpp#L334](../../thorlcr/activities/thdiskbase.cpp#L334).

## 4. Plausible But Unverified Opportunities
### A. Lazy-open spill and write sinks only after the first row arrives
Spill and file sinks open output resources during process setup, before they know whether any row will arrive. That may be worth revisiting for empty-output cases, but file and metadata semantics need checking first.

### B. Avoid full rowset materialization for result consumers that only need streaming access
Graph results currently support rowset materialization paths as a general mechanism. That may be more than some consumers need, but this slice did not trace the caller mix deeply enough to call it confirmed waste.

### C. Revisit blocked-I/O tuning symmetry between raw and row-writer outputs
The shared write base carries a comment questioning why blocked-I/O sizing is only handled on one path. That looks worth measuring, but it remains a lead rather than a confirmed issue.

## 5. Areas That Need Measurement
- distributed result receive bytes, repack bytes, and spill behavior on the master
- single-part index transfer time and bytes by source node
- pipewrite recreate count, subprocess startup latency, and stderr volume
- blank-part creation frequency in real publish workflows
- spill compression and encryption overhead versus raw temp writes
- empty-output frequency for spill and write sinks

## 6. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/result/thresultslave.cpp](../../thorlcr/activities/result/thresultslave.cpp)
2. [../../thorlcr/activities/result/thresult.cpp](../../thorlcr/activities/result/thresult.cpp)
3. [../../thorlcr/graph/thgraphmaster.cpp](../../thorlcr/graph/thgraphmaster.cpp)
4. [../../thorlcr/activities/thdiskbase.cpp](../../thorlcr/activities/thdiskbase.cpp)
5. [../../thorlcr/activities/pipewrite/thpwslave.cpp](../../thorlcr/activities/pipewrite/thpwslave.cpp)
6. [../../thorlcr/activities/indexwrite/thindexwriteslave.cpp](../../thorlcr/activities/indexwrite/thindexwriteslave.cpp)
7. [../../thorlcr/activities/spill/thspillslave.cpp](../../thorlcr/activities/spill/thspillslave.cpp)