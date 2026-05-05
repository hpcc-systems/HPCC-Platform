# Worker-to-Server Communication & Result Aggregation

This document details the data path of execution results returning from Worker (Agent) nodes back to the orchestrating Roxie Server.

## Worker-Side Data Transmit
When an activity executes on a worker node (e.g., `CRoxieKeyedJoinIndexActivity` in `roxie/ccd/ccdactivities.cpp`), it processes the subset graph (reads index structures) and serializes the result rows.
1. The activity calls `ROQ->createOutputStream(...)` to obtain an `IMessagePacker`.
2. As rows are returned (or matches found), they are natively allocated via `roxiemem` and processed.
3. They are handed over to the `IMessagePacker` (sometimes via helpers like `OptimizedKJRowBuilder`).
4. If a row exceeds optimal MTU limits or `roxiemem::DATA_ALIGNMENT_SIZE`, the packer fragments it into a multi-part payload.
5. The packet sequences are dispatched out natively using `ISendManager` (and the UDP/TCP send managers handle backpressure to the OS).

## Server-Side Reception (`CRemoteResultAdaptor`)
When a Server orchestrates a distributed query via `CRoxieServerRemoteActivity` or `CRoxieServerKeyedJoinActivity` (in `roxie/ccd/ccdactivities.cpp`), it waits for the split data streams via the `CRemoteResultAdaptor` (defined in `roxie/ccd/ccdserver.cpp`).
`CRemoteResultAdaptor` intercepts UDP/TCP messages and merges them back into a cohesive dataset using `CRemoteResultMerger`.

### The Result Merger (`CRemoteResultMerger`)
`CRemoteResultMerger` is responsible for aggregating partial asynchronous remote results from multiple workers.
* It maintains a list of `HeapEntry` structs mapping sequence packets to memory block chunks.
* It implements a local min-heap (`makeHeap()` and `remakeHeap()`) based on index comparisons to yield sequential rows in strictly sorted order.
* `nextRowGE()` and `skipRows()` handle Smart-Stepping skipping over unneeded sequence boundaries without full deserialization.

## Bottleneck Points and Contention (Worker-to-Server)

1. **Agent Message Packing Overhead**:
   Every result row generated on the Worker forces memory packing calls before transmission (`output->getBuffer()`, `memcpy()`). High-throughput result generation inherently bottlenecked by memory-copy mechanics to fragment the data.

2. **Server-Side Result Aggregation (Heap Sorting)**:
   The `CRemoteResultMerger` uses `makeHeap()` which demands CPU overhead on the central Server. Rebuilding or sliding tree heaps dynamically on varying UDP arrival sequences scales poorly with high Worker-fanout counts. In scenarios like massive Keyed Joins, yielding sequential `next()` rows from many workers simultaneously requires recurrent heap property restorations, eating memory bandwidth and CPU cycles.

3. **Limits Checking & Concurrency Locks**:
   In `CRemoteResultAdaptor::processRow()`, rows are individually evaluated against global `rowLimit` and `keyedLimit`. Furthermore, `ChannelBuffer` allocations use strict mutual exclusion (`CriticalSection crit`). Consequently, reading merged data is fiercely synchronized, capping single-thread retrieval speeds even when the network buffer has ample bytes.

4. **Incast Flooding**:
   During Worker-to-Server consolidation, if many leaf nodes match on a distributed key simultaneously, they all blast UDP/TCP messages back to the originating server. While RTS/CTS partially throttles UDP, large synchronized queries can cause temporary packet queues to spike, exacerbating jitter for concurrent queries.
