# Thor Activity Family: Keyed Join

This document covers the live keyed-join family in [../../thorlcr/activities/keyedjoin](../../thorlcr/activities/keyedjoin), plus the fact that legacy keyed join remains selectable and therefore still matters as maintenance context.

## 1. Family Summary
The current keyed-join implementation is a keyed index-probe pipeline with optional fetch.

- the master maps index/data parts to slaves and decides local versus remote handling
- the slave turns each LHS row into a `CJoinGroup`
- lookup handlers add RHS rows or fetch placeholders into that group
- completed groups move from pending to done while preserving ordering or grouping semantics
- memory pressure can spill complete or incomplete join-group RHS state

This makes keyed join a queueing and memory-management problem as much as an index-lookup problem.

## 2. Main Anchors
- [../../thorlcr/activities/keyedjoin/thkeyedjoin.cpp#L32](../../thorlcr/activities/keyedjoin/thkeyedjoin.cpp#L32) `CKeyedJoinMaster`
- [../../thorlcr/activities/keyedjoin/thkeyedjoin.cpp#L81](../../thorlcr/activities/keyedjoin/thkeyedjoin.cpp#L81) part-mapping logic
- [../../thorlcr/activities/keyedjoin/thkeyedjoincommon.hpp#L28](../../thorlcr/activities/keyedjoin/thkeyedjoincommon.hpp#L28) keyed-join wire headers
- [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L188](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L188) `CJoinGroup`
- [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L580](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L580) partial-group spill path
- [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L960](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L960) handler framework
- [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L1348](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L1348) local key lookup
- [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L1737](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L1737) remote reply handling
- [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L2598](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L2598) fetch routing
- [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L2663](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L2663) `readAhead()`
- [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3429](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3429) `onComplete()`
- [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3497](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3497) RHS bucket allocation
- [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3669](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3669) keyed-join spill callback

## 3. Confirmed Optimization Opportunities
### A. Remove redundant `fpos` serialization from remote reply paths
Both remote reply paths carry explicit `fpos` vectors at [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L1737](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L1737) and [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L1759](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L1759), and the code itself calls this out as wasted work.

That is a direct payload-reduction target.

### B. Avoid dense per-part RHS bucket allocation when preserve-order fan-out is sparse
Ordered mode allocates `RowArray` storage sized to `totalIndexParts` for each join group at [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3503](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3503) and [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3504](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3504).

That is a confirmed keyed-join-specific memory cost when actual hit fan-out is sparse.

### C. Reduce $O(n)$ spill-callback rescans
`freeBufferedRows()` rescans done and pending join groups to recompute `totalInUse` and spill candidates at [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3689](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3689), [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3713](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3713), and [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3789](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3789).

That is a confirmed memory-pressure overhead that grows with queued groups.

### D. Reduce preserve-order spill metadata overhead
Ordered partial-group spilling writes bucket-offset metadata and chained spill markers at [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L622](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L622) and replays that structure later at [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3529](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3529).

That is a confirmed extra CPU and disk surface unique to preserve-order spilling.

## 4. Plausible But Unverified Opportunities
### A. Revisit the merged local-key path versus true per-part locality
The source suggests the merged local-key path is not ideal around [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L2894](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L2894), but the gain from changing it is not proven here.

### B. Rebalance thread budgets across lookup and fetch work
The current split limiters may underuse or overconstrain concurrency for some workloads, but this is only suggested as a possibility around [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L2996](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L2996).

### C. Revisit fetch routing and prefetch policy on fetch-heavy joins
`queueFetchLookup()` and the local fetch prefetch path may be paying repeated routing and batching costs, but this pass does not prove those costs dominate real workloads.

## 5. Areas That Need Measurement
- stage timings for `readAhead()`, lookup, fetch, `onComplete()`, `nextRow()`, and `freeBufferedRows()`
- queue depth and block time for pending-lookup and done-group limiters
- complete-versus-incomplete spill counts, bytes, and replay reads
- local-versus-remote routing ratios for both probes and fetches
- fetch prefetch effectiveness on sparse/random `fpos` access
- network payload sizes for remote replies before and after any `fpos` payload changes

## 6. Observability Notes
Instrumentation needs work before keyed join can be tuned confidently.

- merged local-key file stats are already known to be misattributed
- remote fetch file stats are not tracked
- the live family still coexists with a separate legacy implementation, which raises the cost of leaving instrumentation inconsistent across paths

## 7. Legacy Context
The legacy keyed-join factories remain selectable at [../../thorlcr/activities/keyedjoin/thkeyedjoin.cpp#L599](../../thorlcr/activities/keyedjoin/thkeyedjoin.cpp#L599) through [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3849](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp#L3849).

That means cleanup or optimization work on the live path should either justify leaving legacy untouched or explicitly account for duplicated maintenance burden.

## 8. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/keyedjoin/thkeyedjoin.cpp](../../thorlcr/activities/keyedjoin/thkeyedjoin.cpp)
2. [../../thorlcr/activities/keyedjoin/thkeyedjoincommon.hpp](../../thorlcr/activities/keyedjoin/thkeyedjoincommon.hpp)
3. [../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp](../../thorlcr/activities/keyedjoin/thkeyedjoinslave.cpp)
4. [../../thorlcr/activities/keyedjoin/thkeyedjoinslave-legacy.cpp](../../thorlcr/activities/keyedjoin/thkeyedjoinslave-legacy.cpp)
5. [storage-and-spill-io.md](storage-and-spill-io.md)