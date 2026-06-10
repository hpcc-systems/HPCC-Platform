# Thor Activity Family: Loop

This document covers Thor's `LOOP` family as implemented by [../../thorlcr/activities/loop/thloopslave.cpp](../../thorlcr/activities/loop/thloopslave.cpp) and [../../thorlcr/activities/loop/thloop.cpp](../../thorlcr/activities/loop/thloop.cpp), plus the graph/result plumbing that directly controls loop-result materialization in [../../thorlcr/graph/thgraph.cpp](../../thorlcr/graph/thgraph.cpp), [../../thorlcr/graph/thgraph.hpp](../../thorlcr/graph/thgraph.hpp), and [../../thorlcr/graph/thgraphslave.cpp](../../thorlcr/graph/thgraphslave.cpp). It intentionally excludes `ITERATE` and `PROCESS`, which are covered separately.

## 1. Family Summary
This family has two distinct runtime shapes.

- Regular row, dataset, and count loops shuttle rows through `loopPending`, execute the child graph for each iteration, and materialize a fresh three-result package for loop output, loop input, and optional counter or loop-again state.
- Graph-loop mode snapshots the full input into a graph result, executes child graphs repeatedly, and retains iteration outputs inside a long-lived graph-loop result set until the final result is read.

That makes `LOOP` more control-plane-heavy than most Thor activities. Its main costs come from per-iteration result packaging, global synchronization, retained loop results, and stop behavior rather than a single steady-state row transform.

## 2. Main Anchors
- [../../thorlcr/activities/loop/thloopslave.cpp#L31](../../thorlcr/activities/loop/thloopslave.cpp#L31) `CLoopSlaveActivityBase`
- [../../thorlcr/activities/loop/thloopslave.cpp#L130](../../thorlcr/activities/loop/thloopslave.cpp#L130) `CLoopSlaveActivity`
- [../../thorlcr/activities/loop/thloopslave.cpp#L298](../../thorlcr/activities/loop/thloopslave.cpp#L298) regular loop iteration body in `getNextRow(bool stopping)`
- [../../thorlcr/activities/loop/thloopslave.cpp#L475](../../thorlcr/activities/loop/thloopslave.cpp#L475) `CGraphLoopSlaveActivity`
- [../../thorlcr/activities/loop/thloop.cpp#L30](../../thorlcr/activities/loop/thloop.cpp#L30) `CLoopActivityMasterBase`
- [../../thorlcr/activities/loop/thloop.cpp#L41](../../thorlcr/activities/loop/thloop.cpp#L41) master-side global loop synchronization
- [../../thorlcr/graph/thgraph.cpp#L241](../../thorlcr/graph/thgraph.cpp#L241) `CThorBoundLoopGraph`
- [../../thorlcr/graph/thgraph.cpp#L2150](../../thorlcr/graph/thgraph.cpp#L2150) `CGraphBase::executeChild(..., results, graphLoopResults)`
- [../../thorlcr/graph/thgraph.hpp#L1289](../../thorlcr/graph/thgraph.hpp#L1289) `CThorGraphResults`
- [../../thorlcr/activities/loop/thloopslave.cpp#L1297](../../thorlcr/activities/loop/thloopslave.cpp#L1297) `CGraphLoopResultReadSlaveActivity`

## 3. Confirmed Optimization Opportunities
### A. Regular loops rebuild their result package and loop buffer every iteration
The regular slave path recreates a three-slot graph-results bundle at [../../thorlcr/activities/loop/thloopslave.cpp#L389](../../thorlcr/activities/loop/thloopslave.cpp#L389), re-prepares loop input and output through [../../thorlcr/graph/thgraph.cpp#L275](../../thorlcr/graph/thgraph.cpp#L275), and then allocates a fresh overflowable `loopPending` buffer at [../../thorlcr/activities/loop/thloopslave.cpp#L450](../../thorlcr/activities/loop/thloopslave.cpp#L450).

The master global path mirrors the same three-result recreation at [../../thorlcr/activities/loop/thloop.cpp#L220](../../thorlcr/activities/loop/thloop.cpp#L220). For high-iteration loops, this is a confirmed allocator, result-materialization, and buffer-handoff hotspot.

### B. Global loops pay a master-mediated sync round trip on every iteration
Slaves send iteration progress in [../../thorlcr/activities/loop/thloopslave.cpp#L46](../../thorlcr/activities/loop/thloopslave.cpp#L46) and may block waiting for the master reply at [../../thorlcr/activities/loop/thloopslave.cpp#L61](../../thorlcr/activities/loop/thloopslave.cpp#L61).

The master waits for all slaves in [../../thorlcr/activities/loop/thloop.cpp#L41](../../thorlcr/activities/loop/thloop.cpp#L41) and only emits the “Still waiting” watchdog log at [../../thorlcr/activities/loop/thloop.cpp#L57](../../thorlcr/activities/loop/thloop.cpp#L57). That is a confirmed latency surface and a confirmed diagnostics gap for rank skew or barrier delay.

### C. Downstream early-stop does not promptly short-circuit loop execution
The feeder stop path explicitly says it discards further rows but keeps looping until `finishedLooping` at [../../thorlcr/activities/loop/thloopslave.cpp#L224](../../thorlcr/activities/loop/thloopslave.cpp#L224). The outer activity stop path documents that it will block until the loop reaches EOF at [../../thorlcr/activities/loop/thloopslave.cpp#L468](../../thorlcr/activities/loop/thloopslave.cpp#L468).

For loops whose consumer stops early, that is confirmed wasted child-graph execution, buffering, and synchronization work.

### D. Graph-loop mode snapshots full input and retains iteration outputs as graph results
The graph-loop slave path creates an initial graph-results set at [../../thorlcr/activities/loop/thloopslave.cpp#L505](../../thorlcr/activities/loop/thloopslave.cpp#L505), copies the full input stream into graph-loop result 0 at [../../thorlcr/activities/loop/thloopslave.cpp#L519](../../thorlcr/activities/loop/thloopslave.cpp#L519), and keeps iteration outputs in a long-lived graph-loop result set wired through [../../thorlcr/graph/thgraph.cpp#L2150](../../thorlcr/graph/thgraph.cpp#L2150) and stored by [../../thorlcr/graph/thgraph.hpp#L1289](../../thorlcr/graph/thgraph.hpp#L1289).

The master graph-loop path similarly pre-allocates loop results at [../../thorlcr/activities/loop/thloop.cpp#L300](../../thorlcr/activities/loop/thloop.cpp#L300). This is the confirmed memory and temp-disk pressure point for graph-loop workloads.

### E. Loop-family observability is too thin for serious optimization work
The family stats mapping only exports `StNumIterations` at [../../thorlcr/thorutil/thormisc.cpp#L103](../../thorlcr/thorutil/thormisc.cpp#L103), and the slave active-stats path only sets that one counter at [../../thorlcr/activities/loop/thloopslave.cpp#L126](../../thorlcr/activities/loop/thloopslave.cpp#L126).

There are no dedicated counters for sync wait, empty-iteration streaks, `loopPending` bytes or spills, per-iteration result creation, graph-loop retained-result volume, or graph-loop result rereads.

## 4. Plausible But Unverified Opportunities
### A. Revisit how loop child graphs are classified as global for synchronization purposes
The graph-control seam already carries an explicit note that there should probably be a better way to decide whether an activity is global at [../../thorlcr/graph/thgraph.cpp#L885](../../thorlcr/graph/thgraph.cpp#L885).

Because that decision feeds loop child-graph behavior, it is a plausible source of over-synchronization or conservative loop execution. This pass did not trace a concrete misclassified graph shape, so it stays in the plausible bucket.

## 5. Areas That Need Measurement
- per-iteration time split across sync wait, child-graph execute, result creation, and buffer handoff
- `loopPending` row volume, spill bytes, and spill reread rate across long loops
- incidence of downstream early-stop and the amount of work done after the consumer has stopped
- retained graph-loop result volume versus iteration count
- distributed graph-loop result reread and collation cost when results are read back later

## 6. Observability Notes
Loop behavior is rich, but the exported view is minimal.

- timeout logging exists for stuck global loops, but not quantitative sync-wait counters
- the family does not export empty-loop streaks or loop-again decisions
- graph-loop retained-result growth is invisible in loop-specific statistics

## 7. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/loop/thloopslave.cpp](../../thorlcr/activities/loop/thloopslave.cpp)
2. [../../thorlcr/activities/loop/thloop.cpp](../../thorlcr/activities/loop/thloop.cpp)
3. [../../thorlcr/graph/thgraph.cpp](../../thorlcr/graph/thgraph.cpp)
4. [../../thorlcr/graph/thgraphslave.cpp](../../thorlcr/graph/thgraphslave.cpp)
5. [../../thorlcr/graph/thgraph.hpp](../../thorlcr/graph/thgraph.hpp)