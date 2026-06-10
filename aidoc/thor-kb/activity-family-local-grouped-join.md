# Thor Activity Family: Local and Grouped Join

This document covers the local and grouped join path implemented by [../../thorlcr/activities/join/thjoinslave.cpp](../../thorlcr/activities/join/thjoinslave.cpp) when the master routes the activity through the local/grouped branch. It intentionally excludes distributed/global join orchestration.

## 1. Family Summary
The local/grouped join activity is a two-input wrapper around two distinct cost centers:

1. local stream preparation, where unsorted inputs are fully materialized through spill-capable row loaders
2. `CJoinHelper`, where matching equal-key groups are walked in memory after the streams are prepared

If an input is already locally sorted but grouped, the activity strips group markers before joining. If an input is unsorted, it loads and sorts the full side through `IThorRowLoader` before handing the resulting streams to the helper.

That means spill protection is present in the loader phase, but once the helper starts matching, the current equal-key RHS group is buffered in memory inside the helper rather than using a spill-aware structure.

## 2. Main Anchors
- [../../thorlcr/activities/join/thjoin.cpp#L362](../../thorlcr/activities/join/thjoin.cpp#L362) local/grouped master selection
- [../../thorlcr/activities/join/thjoinslave.cpp#L204](../../thorlcr/activities/join/thjoinslave.cpp#L204) local loader creation with join spill priority
- [../../thorlcr/activities/join/thjoinslave.cpp#L264](../../thorlcr/activities/join/thjoinslave.cpp#L264) start-time secondary-input startup
- [../../thorlcr/activities/join/thjoinslave.cpp#L288](../../thorlcr/activities/join/thjoinslave.cpp#L288) local versus global branch in `start()`
- [../../thorlcr/activities/join/thjoinslave.cpp#L441](../../thorlcr/activities/join/thjoinslave.cpp#L441) local join preparation path
- [../../thorlcr/thorutil/thormisc.cpp#L1403](../../thorlcr/thorutil/thormisc.cpp#L1403) `createUngroupStream()` group-flattening adapter
- [../../thorlcr/activities/msort/thsortu.cpp#L288](../../thorlcr/activities/msort/thsortu.cpp#L288) `CJoinHelper` state and counters
- [../../thorlcr/activities/msort/thsortu.cpp#L844](../../thorlcr/activities/msort/thsortu.cpp#L844) RHS group build and match bitmap setup
- [../../thorlcr/activities/msort/thsortu.cpp#L952](../../thorlcr/activities/msort/thsortu.cpp#L952) main match loop and `JSrightgrouponly` pass
- [../../thorlcr/activities/msort/thsortu.cpp#L2071](../../thorlcr/activities/msort/thsortu.cpp#L2071) multicore join-helper factory

## 3. Confirmed Optimization Opportunities
### A. Defer secondary startup when the local path can cheaply prove the primary is empty
The activity starts the secondary side, and may also install lookahead, before local emptiness is known at [../../thorlcr/activities/join/thjoinslave.cpp#L264](../../thorlcr/activities/join/thjoinslave.cpp#L264) through [../../thorlcr/activities/join/thjoinslave.cpp#L307](../../thorlcr/activities/join/thjoinslave.cpp#L307).

But the local path can later discard the RHS entirely for empty-LHS inner joins at [../../thorlcr/activities/join/thjoinslave.cpp#L464](../../thorlcr/activities/join/thjoinslave.cpp#L464) through [../../thorlcr/activities/join/thjoinslave.cpp#L469](../../thorlcr/activities/join/thjoinslave.cpp#L469). That is fixed startup and buffering cost with no benefit on that workload shape.

### B. Skip the RHS-only sweep for non-right-outer joins
After a completed match group, the helper moves into `JSrightgrouponly` at [../../thorlcr/activities/msort/thsortu.cpp#L965](../../thorlcr/activities/msort/thsortu.cpp#L999). The source explicitly notes that inner and left-only joins should avoid that extra walk at [../../thorlcr/activities/msort/thsortu.cpp#L989](../../thorlcr/activities/msort/thsortu.cpp#L994).

That is a confirmed per-group overhead independent of skew.

### C. Stop scanning trailing RHS once LHS is exhausted for joins that do not need unmatched right rows
When the left side ends first, the helper still scans trailing RHS rows and explicitly notes that it could stop earlier when right-outer semantics are not required at [../../thorlcr/activities/msort/thsortu.cpp#L928](../../thorlcr/activities/msort/thsortu.cpp#L931).

For inner and left-only joins, that is pure post-useful-work cost.

## 4. Plausible But Unverified Opportunities
### A. Reuse RHS-group buffers across groups instead of rebuilding them each time
The helper kills and rebuilds `rightgroup` and its match bitmap around [../../thorlcr/activities/msort/thsortu.cpp#L847](../../thorlcr/activities/msort/thsortu.cpp#L857) and [../../thorlcr/activities/msort/thsortu.cpp#L902](../../thorlcr/activities/msort/thsortu.cpp#L906). This looks like churn for many tiny groups, but this pass does not prove it dominates elapsed time.

### B. Add spill-aware handling for very large equal-key RHS groups inside the helper
The loader phase can spill, but the helper then buffers the current RHS match group in memory via `rightgroup` at [../../thorlcr/activities/msort/thsortu.cpp#L288](../../thorlcr/activities/msort/thsortu.cpp#L323) and [../../thorlcr/activities/msort/thsortu.cpp#L844](../../thorlcr/activities/msort/thsortu.cpp#L906). Current safeguards are abort and at-most limits rather than spill-aware continuation.

### C. Revisit multicore-helper crossover for workloads with many tiny groups
`createJoinHelper()` can choose multicore wrappers at [../../thorlcr/activities/msort/thsortu.cpp#L2071](../../thorlcr/activities/msort/thsortu.cpp#L2090), but those wrappers add queueing and writer fan-out. The break-even point for tiny match groups is not established here.

## 5. Areas That Need Measurement
- split local-join elapsed time into loader work versus helper work
- peak helper memory per equal-key match group
- frequency and cost of wasted RHS startup on empty-LHS workloads
- frequency and cost of RHS-only scans that produce no rows
- single-thread versus multicore local-join crossover by group-size distribution

## 6. Observability Notes
The current join statistics are not strong enough to drive fine-grained local-join tuning with confidence.

- published counters focus on group sizes and candidate counts rather than helper memory footprint
- the single-thread helper already notes blind spots for some large trailing groups
- the multicore helper currently records the left group size as both left and right at [../../thorlcr/activities/msort/thsortu.cpp#L1619](../../thorlcr/activities/msort/thsortu.cpp#L1623)

## 7. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/join/thjoin.cpp](../../thorlcr/activities/join/thjoin.cpp)
2. [../../thorlcr/activities/join/thjoinslave.cpp](../../thorlcr/activities/join/thjoinslave.cpp)
3. [../../thorlcr/activities/msort/thsortu.cpp](../../thorlcr/activities/msort/thsortu.cpp)
4. [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md)
5. [activities-using-buffering-and-spilling.md](activities-using-buffering-and-spilling.md)