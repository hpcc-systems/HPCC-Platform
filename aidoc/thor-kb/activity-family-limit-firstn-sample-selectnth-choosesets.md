# Thor Activity Family: Limit / FirstN / Sample / SelectNth / ChooseSets

This document covers the pruning and selection family rooted in [../../thorlcr/activities/limit](../../thorlcr/activities/limit), [../../thorlcr/activities/firstn](../../thorlcr/activities/firstn), [../../thorlcr/activities/sample](../../thorlcr/activities/sample), [../../thorlcr/activities/selectnth](../../thorlcr/activities/selectnth), and [../../thorlcr/activities/choosesets](../../thorlcr/activities/choosesets). The family shares a common goal of position- or category-based pruning, but the implementations split sharply between streaming filters and prefix-state or materialization outliers.

## 1. Family Summary
This family has three main cost profiles.

- `LIMIT`, local or grouped `FIRSTN`, and `SAMPLE` are mostly streaming. They count rows, emit or drop them in place, and stop early when enough information is available.
- Global `FIRSTN`, global `SELECTNTH`, and plain global `CHOOSESETS` become sequential across nodes because they depend on prefix state from earlier nodes.
- `SKIPLIMIT`, `ROWLIMIT`, and `CHOOSESETS LAST` or `CHOOSESETS ENTH` are the heavy end. They buffer or pre-count input before they can decide what to emit.

The strongest confirmed optimization surfaces in this family are the `SKIPLIMIT` overflowable-buffer replay path and the full-input pre-count barrier in `CHOOSESETS LAST` and `CHOOSESETS ENTH`.

## 2. Main Anchors
- [../../thorlcr/activities/limit/thlimitslave.cpp#L26](../../thorlcr/activities/limit/thlimitslave.cpp#L26) `CLimitSlaveActivityBase`
- [../../thorlcr/activities/limit/thlimitslave.cpp#L83](../../thorlcr/activities/limit/thlimitslave.cpp#L83) streaming `CLimitSlaveActivity`
- [../../thorlcr/activities/limit/thlimitslave.cpp#L181](../../thorlcr/activities/limit/thlimitslave.cpp#L181) `CSkipLimitSlaveActivity::gather()`
- [../../thorlcr/activities/firstn/thfirstnslave.cpp#L215](../../thorlcr/activities/firstn/thfirstnslave.cpp#L215) `CFirstNSlaveGlobal`
- [../../thorlcr/activities/firstn/thfirstn.cpp#L36](../../thorlcr/activities/firstn/thfirstn.cpp#L36) global `FIRSTN` master coordination
- [../../thorlcr/activities/sample/thsampleslave.cpp#L21](../../thorlcr/activities/sample/thsampleslave.cpp#L21) `SampleSlaveActivity`
- [../../thorlcr/activities/selectnth/thselectnthslave.cpp#L8](../../thorlcr/activities/selectnth/thselectnthslave.cpp#L8) `CSelectNthSlaveActivity`
- [../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L233](../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L233) `ChooseSetsPlusActivity`
- [../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L358](../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L358) `ChooseSetsLastActivity`
- [../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L443](../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L443) `ChooseSetsEnthActivity`

## 3. Confirmed Optimization Opportunities
### A. Reduce the `SKIPLIMIT` and `ROWLIMIT` buffer replay path
`CSkipLimitSlaveActivity::gather()` writes candidate rows into an overflowable buffer, stops the input, then either clears that buffer or reopens it through a reader at [../../thorlcr/activities/limit/thlimitslave.cpp#L181](../../thorlcr/activities/limit/thlimitslave.cpp#L181) through [../../thorlcr/activities/limit/thlimitslave.cpp#L214](../../thorlcr/activities/limit/thlimitslave.cpp#L214).

If the buffer spills, that path adds a real write/read cycle by construction. The source also leaves a direct note about missing excessive-buffering signaling at [../../thorlcr/activities/limit/thlimitslave.cpp#L199](../../thorlcr/activities/limit/thlimitslave.cpp#L199).

### B. Reduce the full-input pre-count barrier in `CHOOSESETS LAST` and `CHOOSESETS ENTH`
`ChooseSetsPlusActivity` installs lookahead over a counting wrapper at [../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L279](../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L279) with an explicit “read all input” comment, then waits for global counts before `LAST` or `ENTH` selection can begin.

This is a confirmed two-phase cost center in the current design, surfaced again in `ChooseSetsLastActivity` and `ChooseSetsEnthActivity` at [../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L358](../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L358) and [../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L443](../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L443).

### C. Replace the `SELECTNTH` spinlock with an atomic as already noted in source
`CSelectNthSlaveActivity` keeps `lookaheadN` behind a `SpinLock`, and the declaration itself explicitly says it should be replaced with an atomic at [../../thorlcr/activities/selectnth/thselectnthslave.cpp#L17](../../thorlcr/activities/selectnth/thselectnthslave.cpp#L17).

This is the clearest local code-commented micro-optimization in the family.

## 4. Plausible But Unverified Opportunities
### A. Revisit master-serialized control in global `FIRSTN`
Global `FIRSTN` is coordinated through a master send/receive loop at [../../thorlcr/activities/firstn/thfirstn.cpp#L36](../../thorlcr/activities/firstn/thfirstn.cpp#L36) and [../../thorlcr/activities/firstn/thfirstnslave.cpp#L296](../../thorlcr/activities/firstn/thfirstnslave.cpp#L296). That may be perfectly fine at modest scale, but it is the obvious place to look if cluster-size latency becomes visible.

### B. Revisit node-to-node tally chaining in plain global `CHOOSESETS`
The plain global `CHOOSESETS` path also forwards tally state serially between nodes. The relevant state transitions are at [../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L113](../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L113) through [../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L200](../../thorlcr/activities/choosesets/thchoosesetsslave.cpp#L200).

### C. Revisit fixed smart-buffer sizing across the family
`FIRSTN`, `SELECTNTH`, and `CHOOSESETS` rely on fixed smart-buffer sizes. That may be fine, but the right tradeoff likely depends on row width and cluster shape rather than on one fixed constant per operator.

## 5. Areas That Need Measurement
- `SKIPLIMIT` spill count, buffered bytes, and replay time
- global `FIRSTN` master coordination latency and stop-input savings
- `SELECTNTH` lookahead avoidance rate and default-row fallback frequency
- `CHOOSESETS` category distribution skew and pre-count wait time
- grouped `FIRSTN` discard-tail cost after the limit is reached
- sample input skip ratio and per-group reset behavior

## 6. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/limit/thlimit.cpp](../../thorlcr/activities/limit/thlimit.cpp)
2. [../../thorlcr/activities/limit/thlimitslave.cpp](../../thorlcr/activities/limit/thlimitslave.cpp)
3. [../../thorlcr/activities/firstn/thfirstn.cpp](../../thorlcr/activities/firstn/thfirstn.cpp)
4. [../../thorlcr/activities/firstn/thfirstnslave.cpp](../../thorlcr/activities/firstn/thfirstnslave.cpp)
5. [../../thorlcr/activities/selectnth/thselectnthslave.cpp](../../thorlcr/activities/selectnth/thselectnthslave.cpp)
6. [../../thorlcr/activities/choosesets/thchoosesets.cpp](../../thorlcr/activities/choosesets/thchoosesets.cpp)
7. [../../thorlcr/activities/choosesets/thchoosesetsslave.cpp](../../thorlcr/activities/choosesets/thchoosesetsslave.cpp)
8. [../../thorlcr/activities/sample/thsampleslave.cpp](../../thorlcr/activities/sample/thsampleslave.cpp)