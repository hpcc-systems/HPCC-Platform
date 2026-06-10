# Thor Activity Family: SelfJoin

This document covers Thor selfjoin as implemented by [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp) and the shared matching helper in [../../thorlcr/activities/msort/thsortu.cpp](../../thorlcr/activities/msort/thsortu.cpp). It is separate from the regular join families because selfjoin has one ordered input stream, one equal-key group buffer, and its own local, global, and lightweight wrappers.

## 1. Family Summary
Selfjoin has three runtime shapes.

- Lightweight selfjoin bypasses loader or sorter setup and feeds the incoming stream directly into the helper.
- Local selfjoin materializes and sorts the input through `IThorRowLoader`, then hands the resulting stream to the helper.
- Global selfjoin gathers the ordered stream through `IThorSorter`, waits on a barrier, and then starts a merge stream before the helper runs.

All three shapes funnel into `SelfJoinHelper`, which reads one ordered stream, builds one equal-key group in memory, and emits matches from that group. That makes equal-key group width and duplicate skew the dominant cost driver in this family.

## 2. Main Anchors
- [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L59](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L59) local selfjoin loader path
- [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L69](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L69) global selfjoin gather, barrier, and merge path
- [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L159](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L159) helper selection in `start()`
- [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L231](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L231) active-stat rollup
- [../../thorlcr/activities/msort/thsortu.cpp#L1017](../../thorlcr/activities/msort/thsortu.cpp#L1017) `SelfJoinHelper` state and equal-key group buffer
- [../../thorlcr/activities/msort/thsortu.cpp#L1244](../../thorlcr/activities/msort/thsortu.cpp#L1244) large preliminary-match warning path
- [../../thorlcr/activities/msort/thsortu.cpp#L2093](../../thorlcr/activities/msort/thsortu.cpp#L2093) multicore selfjoin helper factory
- [../../thorlcr/activities/join/thjoin.cpp#L334](../../thorlcr/activities/join/thjoin.cpp#L334) master-side selfjoin warning throttle

## 3. Confirmed Optimization Opportunities
### A. Duplicate-heavy selfjoins still materialize one full equal-key group before useful work can finish
`SelfJoinHelper` builds `curgroup` for the full equal-key run in [../../thorlcr/activities/msort/thsortu.cpp#L1017](../../thorlcr/activities/msort/thsortu.cpp#L1017) through [../../thorlcr/activities/msort/thsortu.cpp#L1243](../../thorlcr/activities/msort/thsortu.cpp#L1243), then records and warns about large groups only after they are already built at [../../thorlcr/activities/msort/thsortu.cpp#L1244](../../thorlcr/activities/msort/thsortu.cpp#L1244) and [../../thorlcr/activities/msort/thsortu.cpp#L1245](../../thorlcr/activities/msort/thsortu.cpp#L1245).

This is a confirmed skew hotspot: duplicate-heavy keys push memory, extend elapsed time, and only surface via warning once the expensive group already exists.

### B. Local selfjoin spill accounting is explicitly uncertain
The local path merges row-loader statistics immediately after loading at [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L64](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L64), and the code comments directly that the best policy is unclear if the returned row stream spills later.

That is a confirmed observability defect for local selfjoin tuning.

### C. Global selfjoin has distinct gather, barrier, and merge stages without dedicated active counters
The global path gathers at [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L72](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L72), waits at [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L79](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L79), and only then starts the merged stream at [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L85](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L85).

But active stats only roll up helper and sorter totals at [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L231](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L231) through [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L241](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L241). That blocks stage-level diagnosis of whether the family is gather-bound, wait-bound, or merge-bound.

### D. Multicore selfjoin fallback reasons are not exported
`createSelfJoinHelper()` returns the single-thread helper whenever parallel match is disabled, a keep-limit is present, or sliding-match is requested at [../../thorlcr/activities/msort/thsortu.cpp#L2102](../../thorlcr/activities/msort/thsortu.cpp#L2102). The only explicit signal on the positive path is a debug log with thread count at [../../thorlcr/activities/msort/thsortu.cpp#L2107](../../thorlcr/activities/msort/thsortu.cpp#L2107).

That is a confirmed diagnostics gap for tuning multicore selfjoin behavior.

## 4. Plausible But Unverified Opportunities
### A. Revisit sorter lifetime across loop-driven selfjoin reuse
`reset()` contains an explicit note that the sorter should not need to be recreated between loop iterations at [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L135](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp#L135). This slice did not prove a hot recreate path in real workloads, but the source suggests reuse is important.

### B. Revisit multicore crossover for many tiny equal-key groups
The multicore path adds intercept and worker overhead once enabled at [../../thorlcr/activities/msort/thsortu.cpp#L2107](../../thorlcr/activities/msort/thsortu.cpp#L2107) through [../../thorlcr/activities/msort/thsortu.cpp#L2109](../../thorlcr/activities/msort/thsortu.cpp#L2109). For very small groups, the crossover may be unfavorable.

## 5. Areas That Need Measurement
- equal-key group-size distribution for selfjoin-heavy workloads
- local selfjoin spill bytes and later reread behavior
- global selfjoin time split across gather, barrier wait, merge, and helper match phases
- frequency and cause of multicore fallback in `createSelfJoinHelper()`
- warning incidence and actual elapsed-time growth once `INITIAL_SELFJOIN_MATCH_WARNING_LEVEL` is crossed

## 6. Observability Notes
Selfjoin already knows when a match group becomes concerning, but the exported view is still coarse.

- the helper emits a warning threshold only after building the full group
- the master warning path in [../../thorlcr/activities/join/thjoin.cpp#L334](../../thorlcr/activities/join/thjoin.cpp#L334) through [../../thorlcr/activities/join/thjoin.cpp#L347](../../thorlcr/activities/join/thjoin.cpp#L347) throttles messages by time and threshold doubling rather than exporting richer counters
- active stats do not separate gather, wait, merge, helper, and spill phases cleanly

## 7. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/selfjoin/thselfjoinslave.cpp](../../thorlcr/activities/selfjoin/thselfjoinslave.cpp)
2. [../../thorlcr/activities/msort/thsortu.cpp](../../thorlcr/activities/msort/thsortu.cpp)
3. [../../thorlcr/activities/join/thjoin.cpp](../../thorlcr/activities/join/thjoin.cpp)
4. [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md)
5. [activity-family-local-grouped-join.md](activity-family-local-grouped-join.md)