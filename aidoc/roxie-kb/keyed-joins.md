# Server-Side Keyed Joins in Roxie

## Overview
This document covers the architectural and code-level details of server-side keyed joins, specifically focusing on **Half Keyed Joins**. The primary implementation resides in `roxie/ccd/ccdserver.cpp`.

### Core Classes
* `CRoxieServerKeyedJoinBase` (`roxie/ccd/ccdserver.cpp`): The base class handling data lifecycle, group aggregation (`CJoinGroup`), limits (e.g., `abortLimit`, `keepLimit`), and transformation handling (`doJoinGroup`, `doTransform`).
* `CRoxieServerHalfKeyedJoinActivity` (`roxie/ccd/ccdserver.cpp`): Inherits from `CRoxieServerKeyedJoinBase`. Bypasses data file disk fetches because all required fields are present within the matched index payload (`!helper->diskAccessRequired()`).

## 1. Existing Optimizations for Half Keyed Joins

* **Blocked Memory Allocation (`roxiemem::RHFblocked`)**:
  * Because records (like `CJoinGroup` and intermediate index fields) are constructed serially by the puller thread, the classes instantiate their allocators (e.g., `joinGroupAllocator` and `indexReadAllocator`) with the `RHFblocked` flag. This avoids hitting `roxiemem` global locks/critical sections repeatedly.
* **Simple Mode Fast Path (`isSimple`)**:
  * In locally-optimized or partitioned layouts, `CRoxieServerHalfKeyedJoinActivity` sets `isSimple = true`. When enabled, matched records eagerly `enqueue()` onto `remote.injected` queues rather than dynamically building large compound `CRowArrayMessageResult` messages.
* **TopLevelKey (TLK) Distributed Hashing**:
  * In `CRoxieServerHalfKeyedJoinActivity::processRow`, it evaluates `thisKey->isTopLevelKey()`. If distributed, it quickly extracts the partition (`tlk->getPartition()` or `extractFpos()`) and copies lookups directly to `remote.getMem` to ship out to remote worker parts efficiently, rather than resolving locally.
* **Preloading**:
  * Takes advantage of `keyedJoinPreload` to bootstrap the index lookups without waiting for downstream pulling.

## 2. Intra-Query Contention (Single Query Bottlenecks)

* **The `groupsCrit` Critical Section (`CRoxieServerKeyedJoinBase`)**:
  * The most glaring contention inside a single executing query involves `CriticalBlock c(groupsCrit)` in `processCompletedGroups()`.
  * `processCompletedGroups()` holds this lock while iterating through `groups.dequeue()` and heavily processing `doJoinGroup(head)`.
  * `doJoinGroup` encapsulates the user-defined `helper.transform(...)` and `remote.addResult`, mapping the extracted keys into row formats.
  * *Concurrency Fault*: `processCompletedGroups()` is frequently triggered asynchronously via `noteEndReceived()` (when remote workers finish answering the index sub-read). Because it stalls under `groupsCrit`, it blocks the main puller thread from instantiating new groups via `createJoinGroup(const void *row)`, creating severe pipeline stalls during high-fanout index matches.

## 3. Inter-Query Contention (Multiple Concurrent Queries)

* **JHTree Node Cache Contention (`CTreeCache`)**:
  * While `CRoxieServerHalfKeyedJoinActivity` creates an isolated `tlk` (`IKeyManager`) instance per query, the underlying `IKeyIndex` objects and JHTree node layouts are shared globally across queries.
  * In heavy load scenarios, multiple concurrent queries performing complex Half Keyed Joins against the same physical indices repeatedly contend on internal JHTree node read locks/cache locks when fetching blocks (since half keyed joins often read many internal branches of indexes).
* **Reference Counting on Shared Metadata**:
  * Concurrent instances leverage the exact same immutable `IResolvedFile` (`varFileInfo`) and `ITranslatorSet`. Any hidden dynamic allocations or shared atomic reference counters hit simultaneously during `tlk->setKey(...)` loop operations (e.g. switching key parts) can introduce false sharing.
