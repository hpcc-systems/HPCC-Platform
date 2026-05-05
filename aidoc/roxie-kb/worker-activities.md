# Roxie Worker Activities (`ccdactivities.cpp`)

This document explores the architectural footprint, execution modeling, and performance characteristics of Roxie worker configurations, primarily housed in [roxie/ccd/ccdactivities.cpp](../../roxie/ccd/ccdactivities.cpp) and related source files.

## 1. Architectural Overview

Roxie implements query execution using a segmented model across **Server Activities** (orchestration) and **Worker/Agent Activities** (data extraction/index processing limiters). On worker nodes, [CRoxieAgentActivity](../../roxie/ccd/ccdactivities.cpp#L302) serves as the primary base execution wrapper.

Whenever a parsed query subgraph runs, it delegates chunked work via `IRoxieQueryPacket` dispatches to agents.
Key subclasses include:
* **Disk Operations**: [CRoxieDiskReadBaseActivity](../../roxie/ccd/ccdactivities.cpp#L850) governs planar disk files (Thor distributed). Includes CSV, XML, and binary fetches.
* **Index Processing**: [CRoxieIndexActivity](../../roxie/ccd/ccdactivities.cpp#L2560) and [CRoxieIndexReadActivity](../../roxie/ccd/ccdactivities.cpp#L2822) are the primary engines for scanning B-tree indexes generated via `jhtree`.
* **Distributed Joins/Aggregations**: Implementations like `CRoxieDiskAggregateActivity` and `CRoxieKeyedJoinIndexActivity` handle complex relational mappings closest to the physical hardware rather than proxying raw data back to the server.

## 2. Concurrency and Threading

Workers process requests through a constrained thread pool instantiated as `CRoxieWorker` in [roxie/ccd/ccdqueue.cpp](../../roxie/ccd/ccdqueue.cpp#L1537). This prevents multi-threading context thrashing under high UDP query burst loads.

* **Thread Limits**: Initialized in [roxie/ccd/ccdmain.cpp](../../roxie/ccd/ccdmain.cpp#L72), the default `numAgentThreads` is historically 30, meaning a worker instance is heavily optimized for fast throughput per core.
* **Synchronization Primitives**:
    * `CRoxieWorker` handles request lifecycle polling using lightweight mechanisms `std::atomic<bool>` for tight loops.
    * Internal activities utilize `CriticalSection pcrit` and `parCrit` (Parallel CriticalBlock) dynamically. E.g., inside [CRoxieDiskGroupAggregateActivity](../../roxie/ccd/ccdactivities.cpp#L2309), contention is actively documented as a bottleneck needing spinlocks over full critical sections to reduce core parking delays.

## 3. Reliability and Orchestration

* **Packet Continuations:** Because Roxie functions on a stateless UDP backend, queries requiring >60KB memory or paginated disk reads will packetize and suspend output. Variables like `resentInfo` and constraints on `allowFieldTranslation` guarantee that aborted/stranded queries are defensively discarded without memory corruption.
* **Aborts & Race Conditions:** Critical Blocks prominently guard against race conditions associated with remote client termination (`abortLaunch` tracking). A remote UDP kill signal needs to gracefully teardown index walks instantly.

## 4. Memory Management (roxiemem vs heap)

Standard heap allocations are bypassed for actual streaming payload logic. Roxie optimizes Row lifecycle tracking using a dedicated custom slab allocator namespace `roxiemem`.

* **Row Allocators:** Operations that transform, aggregate, or fetch instantiate row blocks via `IEngineRowAllocator`.
* **Row Builders:** Worker threads constantly marshal data into `OptimizedRowBuilder` ([ccdactivities.cpp#L671](../../roxie/ccd/ccdactivities.cpp#L671)) or `RtlDynamicRowBuilder`. This minimizes heap fragmentation and guarantees thread-local safety without taxing global `jemalloc/malloc` arenas when iterating millions of strings during filtering.
* **Opt-in Memory Penalties:** Activities override `needsRowAllocator()` ([ccdactivities.cpp#L340](../../roxie/ccd/ccdactivities.cpp#L340)) to skip initialization loops if query meta indicates dynamic sizing isn't structurally necessary for the current subgraph, netting nanosecond improvements multiplied across gigabytes of volume.

## 5. Performance Bottlenecks & Optimizations

* **JHTree "Smart Stepping"**: The bulk of Roxie's execution lies in traversing index leaves. [CRoxieIndexReadActivity](../../roxie/ccd/ccdactivities.cpp#L2822) integrates aggressive "Smart Stepping" bounds. Through `steppingOffset` and `advanceToNextSeek()`, the core JHTree walker skips evaluation of rows that don't match the current seek tuple—achieving tremendous disk I/O reduction versus linear B-tree scanning.
* **Disk/Index Cache Hits**: Heavy use of `IInMemoryFileProcessor` mitigates rotational or SSD physical IO delay, though reading memory pointers across cache boundaries must be cleanly memory mapped.
* If throughput exhibits regressions, tracking atomic locks inside the `roxiemem::IRowManager` instances and ensuring `CriticalBlock` contentions in Aggregate nodes remain low are the primary diagnostic tasks.
