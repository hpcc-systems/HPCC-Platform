# Thor Activity Family: Hash Distribute and Variants

This document covers the close family rooted in [../../thorlcr/activities/hashdistrib](../../thorlcr/activities/hashdistrib): plain hash distribute, merge distribute, proportional redistribute, keyed distribute, global/local hash dedup, hash aggregate with distributed merge, and the folder-local hash-join setup. It stays inside the folder-local transport and buffering logic rather than opening a full join review.

## 1. Family Summary
The family shares one transport core. Rows are hashed to destination slaves, accumulated into per-target buckets, written locally through spill-capable row buffering when the destination is self, and serialized for remote transfer otherwise.

There are two main transport modes:
- a push path for normal hash distribute
- a pull/merge path when ordered fan-in is required

The variants mostly change the hasher, the destination policy, or the downstream consumer. The common optimization question is therefore not "which helper is used" but how much skew, spill, lock contention, and extra disk traffic the shared transport skeleton introduces.

## 2. Main Anchors
- [../../thorlcr/activities/hashdistrib/thhashdistrib.cpp#L36](../../thorlcr/activities/hashdistrib/thhashdistrib.cpp#L36) `HashDistributeMasterBase`
- [../../thorlcr/activities/hashdistrib/thhashdistrib.cpp#L161](../../thorlcr/activities/hashdistrib/thhashdistrib.cpp#L161) `ReDistributeActivityMaster::process()`
- [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L80](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L80) `CDistributorBase`
- [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L1545](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L1545) `CRowDistributor`
- [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L1668](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L1668) `CRowPullDistributor`
- [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2130](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2130) `HashDistributeSlaveBase`
- [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2287](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2287) proportional redistribute hasher
- [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2553](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2553) keyed distribute TLK lookup
- [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2901](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2901) `CBucketHandler` spill controller for hash dedup
- [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L3103](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L3103) hash dedup driver
- [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L3871](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L3871) global hash dedup wrapper
- [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L4158](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L4158) `CAggregateHT`
- [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L4412](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L4412) distributed aggregate merge entry point

## 3. Confirmed Optimization Opportunities
### A. Make BEST-mode hash dedup output spillable
The code explicitly says the in-memory bucket output should really be spillable at [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L3092](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L3092).

As written, once that stream is returned, further spill relief is blocked until the bucket is consumed. That is a direct memory-pressure optimization target.

### B. Drain resident hash-dedup buckets before spilled buckets
The BEST-mode path also explicitly says the non-spilled buckets should be processed first at [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L3076](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L3076).

That would free live memory earlier and shrink the overlap window between resident and spilled state.

### C. Avoid full redistribute spill just to discover byte totals
When redistribute lacks byte-size metadata, it can spill the entire input to disk at [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2356](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2356), then still decide the original distribution is acceptable and passthrough at [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2532](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2532).

That is a confirmed extra full write/read cycle.

### D. Remove the hot-path critical section from global hash dedup input fetches
The global hash dedup path explicitly notes that the current async `stopInput` design forces a critical section around input fetches at [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L3942](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L3942).

That is a direct code-confirmed contention surface.

### E. Export pre-send dedup effectiveness as counters
The family has a real pre-send dedup heuristic, but the decision points are only logged at [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L588](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L588) and [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L594](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L594). Exported runtime stats cover remote rows, bytes, and temp-buffer activity, but not whether the heuristic helped at [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L954](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L954) and [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L1274](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L1274).

That is a confirmed observability gap on a performance-relevant behavior.

## 4. Plausible But Unverified Opportunities
### A. Sender bucket-release heuristics may underperform under skew
Once the sender buffer fills, bucket choice is heuristic-driven around [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L751](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L751). This may be leaving throughput on the table for skewed targets, but it needs measurement.

### B. Pull-distributor disk caching may dominate merge-distribute workloads
The ordered pull path can spill pending outbound buffers to disk at [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L1903](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L1903). Whether that is a major elapsed-time cost depends on real merge-distribute and aggregate-merge workloads.

### C. Keyed distribute may benefit from stronger locality/caching around TLK lookups
The TLK-driven destination path at [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2553](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L2553) may have room for better locality behavior, but this pass does not prove it is hot enough.

### D. `CAggregateHT` may eventually need spill-aware fallback
The aggregate table is purely in-memory at [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L4158](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L4158). A spill or partition fallback could help some workloads, but that is a larger design change and needs profile evidence first.

## 5. Areas That Need Measurement
- per-destination skew after the actual hash-to-node decision
- time blocked on sender backpressure, writer-pool saturation, and candidate selection
- volume and frequency of pull-cache spills versus local temp-buffer spills
- how often pre-send dedup stays enabled and how many rows it actually removes
- hash-dedup recursion depth, spill count, and post-spill flush costs
- frequency of redistribute's unknown-size fallback
- aggregate-table load factor, expansion cost, and failure behavior on large groups

## 6. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/hashdistrib/thhashdistrib.cpp](../../thorlcr/activities/hashdistrib/thhashdistrib.cpp)
2. [../../thorlcr/activities/hashdistrib/thhashdistribslave.ipp](../../thorlcr/activities/hashdistrib/thhashdistribslave.ipp)
3. [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp)
4. [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md)
5. [storage-and-spill-io.md](storage-and-spill-io.md)