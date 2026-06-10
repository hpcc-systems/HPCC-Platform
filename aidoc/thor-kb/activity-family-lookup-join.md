# Thor Activity Family: Lookup Join

This document covers the lookup-join family rooted in [../../thorlcr/activities/lookupjoin](../../thorlcr/activities/lookupjoin). It includes plain lookup join, lookup-many, and the smart variants that can degrade first to distributed local lookup and then to standard join behavior.

## 1. Family Summary
`CLookupJoinActivityBase` manages a multi-stage RHS lifetime:

1. broadcast or gather RHS rows into spillable row arrays
2. optionally collate and mark unique-key boundaries
3. build a lookup table when memory holds
4. degrade to distributed local lookup if global smart lookup spills
5. degrade again to standard join if local lookup preparation still spills or cannot size the HT

That means the family is not just a fast in-memory lookup path. It is also a staged failover pipeline with repeated hashing, collector handoff, and spill-sensitive cleanup logic.

## 2. Main Anchors
- [../../thorlcr/activities/lookupjoin/thlookupjoin.cpp#L25](../../thorlcr/activities/lookupjoin/thlookupjoin.cpp#L25) master MP-tag setup
- [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L94](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L94) `CBroadcaster`
- [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L580](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L580) `CMarker`
- [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L819](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L819) spillable RHS row array with flush marker
- [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L1115](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L1115) `processRHSRows()`
- [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L1739](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L1739) `CLookupJoinActivityBase`
- [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L1898](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L1898) smart global-to-local cleanup path
- [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2018](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2018) gathered-RHS stream reconstruction
- [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2070](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2070) local HT preparation
- [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2111](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2111) global RHS handling
- [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2320](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2320) distributed-local failover via `CChannelDistributor`
- [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2500](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2500) standard-join failover setup

## 3. Confirmed Optimization Opportunities
### A. Eliminate repeated RHS hash work across failover stages
The code already flags wasted hash calls in `processRHSRows()` at [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L1115](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L1115).

The same RHS rows are then hashed again during node-local pruning, channel redistribution, late keep-local filtering, and HT insertion. That is a confirmed structural cost in the current implementation.

### B. Reuse or pool `CMarker` workers instead of recreating them per large RHS preparation
`CMarker::calculate()` allocates fresh worker objects around [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L714](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L714), [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L752](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L752), and [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L760](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L760), and the source explicitly notes that a shared lightweight pool would be better.

That is a confirmed setup/teardown target for larger RHS preparations.

### C. Reduce the smart-local overflow round-trip
When late local rows cannot append in memory, the path writes them to a temp file at [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L3048](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L3048) through [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L3054](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L3054), then reopens that file during gathered-RHS reconstruction at [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2061](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2061).

That is a confirmed extra disk round-trip in the failover path.

### D. Avoid repeated rescans after row-array save/reset cycles
Smart failover cleanup uses `flushMarker` for incremental local-row filtering, but after a row-array save the marker is reset at [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L1960](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L1960). That means later callbacks can revisit surviving rows from the beginning.

This is a confirmed repeated-work surface inside spill cleanup.

## 4. Plausible But Unverified Opportunities
### A. Parallelize receive-side expand and deserialize work
The receive side currently uses a single processing queue around [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L873](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L873) and [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L897](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L897). The code itself wonders about a small worker pool, but the benefit needs measurement.

### B. Narrow or remove the `CChannelDistributor` callback critical section
The in-source comment at [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2403](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2403) questions why the critical section exists. That may be a contention target, but spill-callback concurrency needs proof before changing it.

### C. Revisit broadcaster batching and queue sizing
Packet size, queue depth, and sender throttling defaults are fixed near [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L40](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L40), [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L41](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L41), and [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L179](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L179). They may be conservative, but this is measurement-first work.

## 5. Areas That Need Measurement
- stage-level timing for broadcast, expand/deserialization, marking, pruning, redistribution, and standard-join setup
- hash invocations per RHS row across the full failover pipeline
- prevalence and size of overflow-file writes in smart lookup
- cause-specific failover rates rather than only local-vs-standard totals
- cross-channel spill-callback contention and skew during smart failover

## 6. Observability Notes
Current exported stats are not strong enough to rank lookup-join stages confidently.

- failover counters collapse distinct causes into coarse local-versus-standard degradation totals
- `overflowWriteCount` is tracked internally but not exported
- stage-level timings for broadcast, marker build, redistribution, and HT preparation are missing

## 7. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/lookupjoin/thlookupjoin.cpp](../../thorlcr/activities/lookupjoin/thlookupjoin.cpp)
2. [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp)
3. [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp)
4. [activities-using-buffering-and-spilling.md](activities-using-buffering-and-spilling.md)
5. [storage-and-spill-io.md](storage-and-spill-io.md)