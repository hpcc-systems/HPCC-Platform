# Roxie Optimization Backlog

> **Note**: This document tracks significant architectural and performance optimizations discovered during analysis. It is explicitly heavily weighted toward Index Operations, Half Keyed Joins, and Splitter contention.

**General Concurrency Context**:
* Typically, each Roxie server handles **10-20 concurrent queries**. 
* The **number of activities in a query varies significantly**: simple queries might have around 100 activities, while complex queries can have **tens of thousands**.
* The **index operations** resulting from these queries can lead to exponentially more active worker threads. 
* The **optimum number of worker threads** remains a critical topic for investigation and is heavily dependent on three key factors:
  1. The effectiveness of the internal node caches.
  2. The time required to decompress nodes.
  3. The speed of the underlying storage (e.g., NVME vs. Remote Storage).

---

## 1. CRITICAL: Half Keyed Joins & Splitters
**Context**: Half Keyed joins are the most critical join type in Roxie. Contention within these joins heavily impacts concurrent scale. Splitters are similarly foundational and contention hurts many queries.
* **Refactor `groupsCrit` in Half Keyed Joins**:
  * **Current State**: `processCompletedGroups()` in `CRoxieServerKeyedJoinBase` holds `groupsCrit` during user-defined transformations (`doJoinGroup()`). This heavily stalls the main puller thread.
  * **Action**: Move execution of `doJoinGroup()` strictly outside the critical section in `processCompletedGroups()`. Dequeue ready groups into a thread-isolated list while holding `groupsCrit`, release the lock, and then perform the data parsing/serialization sequentially.
* **Splitters Analysis (`CRoxieServerThroughSpillActivity`) Lock Contention**:
  * **Current State**: Splitters buffer rows for downstream pipelines using a global `QueueOf` wrapper. Every single downstream thread making a `nextRow()` call contends on two locks (`crit` and `crit2`). Worse, determining if a row can be discarded involves an $O(N)$ scan of all outputs (`isLastTailReader()` and `minIndex()`) that executes *while holding the `crit` lock*. While splitters typically have < 10 outputs, large numbers (massive fan-outs) are not impossible, making this $O(N)$ scan within a lock a severe tail-risk. In a Single-VM Topology B limit, this creates compounding OS-level lock stalls across channels.
  * **Action - RefCount Buffers**: Wrap buffered `RoxieRow` allocations in atomic reference counts `std::atomic<uint32_t> refCount = NumOutputs`. Let downstream pipelines pull atomically without locking (`crit`), decrementing the refCount and cleanly discarding the row when it reaches 0.
  * **Action - O(1) Tail Resolution**: Replace the linear N-scan in `minIndex` with a Min-Heap tracker sorted by reader offset, immediately transforming the lock period from $O(N)$ to $O(1)$. 
  * **Action - Push Model**: Follow the newer `IStrandJunction` architecture. Refactor splitters directly to async Push execution where the parent thread dispatches rows down the multiple pipes unblocked, rather than N parallel downstream consumers blocking each other waiting to Pull.

## 2. VERY IMPORTANT: Index Reads & Execution (`jhtree`)
**Context**: Index operations are the primary driver of Roxie performance. File-based reads (CSV/XML) are rarely used and should be ignored for optimizations.
* **Asynchronous `jhtree` Decompression**:
  * **Current State**: Index reads halt the query thread when a compressed block must be read and deflated from disk. 
  * **Action**: Dispatch block decompression to a background CPU thread-pool in `jhtree`. Leave the primary query threads purely focused on choreography and graph traversal.

## 3. VERY IMPORTANT: Memory & Allocation (`roxiemem` & `jemalloc`)
**Context**: Standard allocations during hot paths cause Tcache thrashing and bin-lock contention across concurrent joins.
* **Thread-Local Caching for `roxiemem`**:
  * **Current State**: High-concurrency allocations serialize heavily onto `heapletLock` in `CChunkedHeap::doAllocateRow`.
  * **Action**: Introduce a thread-local small queue (like TCMalloc/Jemalloc) of pre-allocated `roxiemem` chunks. Drain from thread-local without the `heapletLock`, falling back to `heapletLock` only on a cache-miss.
* **Lock-Free `activeBufferCS` Pools**:
  * **Action**: Replace `CriticalSection activeBufferCS` in `CChunkingRowManager` with a lock-free ring-buffer since data payload lifecycles are identical and constant size.

## 4. IMPORTANT: `soapcall` and Query Invariants
**Context**: While standard XML/Parse activities are rarely used, `soapcall` is heavily used and relies on XML/JSON translation.
* **Hoist `CXMLMaker` Compilation**:
  * **Current State**: XML parsing paths repetitively parse XPath schemas per record inside execution loops, dumping the state machine afterward.
  * **Action**: For heavily used `soapcall` flows (and similar XML abstractions), hoist the `CXMLMaker` template generation into the Factory constructor. Ensure the layout map is calculated once at query-load, allowing execution threads to simply stream data through the invariant state machine.

## 5. NETWORK: Cluster Transport Mechanics
**Context**: Server-to-Worker dispatch and Worker-to-Server Result merging.
* **TCP Message Collating Simplification**:
  * **Action**: Bypass the complex gap-checking (`PacketSequencer`) and background collator threads cleanly when the topology operates natively over TCP. Delegate back-pressure heavily to the OS limits via `CSocketTarget::writeAsync`, stripping legacy UDP network throttles.
* **Parallelize Outbound `ROQ` Serialization**:
  * **Action**: The background thread `RoxieThrottledPacketSender` single-threads payload serialization. Move `.serialize()` execution into the concurrent dispatch threads *before* the throttled network loop.
* **Remote Result Merge Heap Trashing (`CRemoteResultMerger`)**:
  * **Action**: Batch the `makeHeap()`/`remakeHeap()` ops when assembling unsequenced UDP packets on the server side to bypass crippling min-heap thrashing during high-incast remote execution.
* **Eliminate Application-Level Fragmentation & Reassembly**:
  * **Current State**: Roxie uses `UdpPacketHeader` masking to chunk large data streams at the software tier (which even bleeds over into TCP bridging via `CTcpPacketCollator`). 
  * **Action**: Rely on TCP's reliable byte-stream abstraction to cleanly stream unchunked memory structures, deleting the sequencing maps and collator overhead on the receivers. 
* **Strip RTS/CTS Throttling Paradigms**:
  * **Current State**: UDP incast requires extreme lock-bound Request-to-Send (RTS) and Clear-to-Send (CTS) mechanisms to throttle dispatch.
  * **Action**: Let TCP kernel window sizes naturally provide backpressure. Remove the RTS/CTS permit queue layer entirely to drop mutex/CAS contention on dispatch threads. 
* **Event-Driven Socket Death Detection**:
  * **Current State**: Network failures are detected by a polling thread that tracks custom UDP ping/pong elapsed timestamps across the topology.
  * **Action**: Use socket-level `epoll` (`EPOLLRDHUP`/`EPOLLERR`) and `SO_KEEPALIVE` on persistent TCP connections. Turn node crash resolution from a delayed ping-timeout event into an instantaneous OS-level interrupt.
* **Zero-Copy Network Serialization (Scatter-Gather)**:
  * **Current State**: Data maps into heavily managed discrete `DataBuffer` payloads in memory to fit UDP envelopes.
  * **Action**: Leverage continuous sockets with `writev()` (Scatter-Gather I/O) to write discrete header structs and payload memory pointers straight into the OS kernel without an intermediate chunking `memcpy`.
## 6. CHILD QUERIES: Subgraph Execution (`CActivityGraph`)
**Context**: Activities like `PROJECT`, `LOOP`, `NORMALIZE` or child-datasets often trigger child subgraphs *per-row*. High-volume queries will execute tens of thousands of child graph instances rapidly.
* **Remove Redundant `evaluateCrit` in `CActivityGraph`**:
  * **Current State**: Every time `CActivityGraph::evaluate` fires, it locks `evaluateCrit`. However, `CProxyActivityGraph` already enforces thread-exclusivity per child instance, and serial queries run single-threaded.
  * **Action**: Remove the lock (or conditionally bypass it), saving thousands of atomic context switches per second on the hot path.
* **Pool Volatile `CGraphResults` and OS `CriticalSection` Instantiations**:
  * **Current State**: `evaluate()` dynamically allocates a `new CGraphResults` per query. This object contains a Windows/POSIX `CriticalSection` member. Dynamically allocating and destroying OS-level lock primitives per execution absolutely thrashes `jemalloc`.
  * **Action**: 
    1. Pool `CGraphResults` via a lock-free cache. Override `Release()` to wipe internal state and recycle the envelope instead of destructing.
    2. Check if the `CriticalSection` inside `CGraphResults` is strictly necessary on read paths; if the parent sequence consumes data iteratively unshared, elide the lock.
* **Lock-Free Instance Popping in `CProxyActivityGraph`**:
  * **Current State**: When running parallel subgraphs, `CProxyActivityGraph::evaluate` takes a global `graphCrit` lock *twice per row*: once to `stack.popGet()` an execution graph, and again to `stack.append()` it back. Under a Single-VM Topology B limit, this crushes parallelism because hundreds of threads block on a single lock just to check out an execution context.
  * **Action**: Replace the `CIArrayOf stack` guarded by a mutex with an `std::atomic` lock-free Treiber stack. Furthermore, aggressively pre-warm/pre-allocate these graph contexts during `onCreate` initialization rather than lazily allocating them inside the hot execution loop.

## 7. FILES AND CACHING: Multi-part Logic, OS Page Caching, and Eviction
**Context**: File resolution and caching heuristics heavily dictate max throughput. Because of compression and physical distribution, file sizes and mapping structures can create request avalanches and severe cache pollution.

* **Agent Request Avalanche (Logical to Physical Mapping)**:
  * **Current State**: A single abstract file "x" inside a query may map dynamically to multiple underlying logical files. A keyed join targeting that abstract file triggers index requests against *each* underlying logical file individually. 
  * **Action - Mitigate Agent Thread Walls**: The maximum throughput and resilience of a Roxie cluster under load is fundamentally capped by the *number of generated Agent Requests*. We need to investigate ways to batch, group, or intelligently consolidate logical-to-physical traversal mechanisms ahead of dispatch so complex joins don't trigger cascading agent request explosions.
* **Internal Node Cache vs. Linux Page Cache Interference**:
  * **Current State**: The Linux Page Cache holds natively compressed nodes straight off disk, while the Roxie internal node cache (`roxiemem`) holds fully decompressed pages. 
    * A larger Linux Page Cache holds more historical nodes (avoiding catastrophic 150µs disk I/O penalties).
    * A larger Internal Cache reduces CPU load by avoiding constant re-decompression but sacrifices raw density footprint.
  * **Action - Tuning Overlap**: If index blocks are stored uncompressed (Size On Disk == Size In Memory), half the system's memory is wasted duplicating blocks between the OS and Roxie. Track block statuses and instruct Roxie to dynamically minimize the internal cache for 1:1 footprint indexes.
* **"Noisy Neighbor" Cache Eviction Pollution**:
  * **Current State**: Heavy, long-running queries that touch a vast number of files aggressively cycle memory blocks. In a mixed workload environment, these sweeps flush the "hot" data used by fast, highly concurrent interactive queries out of both the Linux Page Cache and the internal Roxie Node Cache.
  * **Action - Partitioning & Priority Caching**: Future architectures must implement priority-tiering (Quality of Service) for cache domains. Either partition the `roxiemem` cache blocks per-query-class or implement a strict MRU/LRU isolation tier where "Low Priority" operations are explicitly barred from evicting cached blocks belonging to "High Priority" endpoints.
  * Make writes to the disk cache asynchronous
  * Ensure reads and writes to the disk cache do not lock each other.

## 8. MEMORY ARCHITECTURE: `roxiemem` Sub-Allocation & Alignment
**Context**: `roxiemem` operates by carving up large OS pages into smaller, sub-allocated row chunks to avoid OS-level allocation overhead. While efficient for density, the strict topological alignment of these pages introduces insidious CPU-level bottlenecks under extreme concurrency (Topology A & B).

* **Cache Set Thrashing (Alignment Collisions)**:
  * **Current State**: Large OS pages (e.g., 2MB) mathematically share the exact same lower-order bit patterns. When `roxiemem` sub-allocates structures at identical offsets within these pages, it guarantees that concurrent accesses hit the *exact same Cache Sets* in the CPU's N-way associative L1/L2 caches. This forces threads to aggressively evict each other's hot data, even though they aren't touching the same actual bytes.
  * **Action - Cache Coloring**: Introduce randomized padding offsets (Cache Coloring) at the start of each large page. By shifting the initial allocation boundaries by differing cache-line widths (e.g., +64, +128 bytes), memory naturally distributes across the CPU cache sets, stopping the eviction loop.
* **False Sharing on Small Chunks**:
  * **Current State**: If `roxiemem` allocates small payload structures (e.g., 16 or 32 bytes) contiguously, different worker threads might end up owning rows that reside on the exact same 64-byte L1 CPU cache line. When Thread A writes to its row, it hardware-invalidates Thread B's CPU cache line, creating invisible but catastrophic memory stalls across cores.
  * **Action - Padding & Bulk Dealing**: Enforce strict `alignas(64)` boundaries (or `CACHE_LINE_SIZE` padding) for heavily mutated structures. To save memory, deal allocations to threads in bulk contiguous chunks so a single thread exclusively owns the entire cache-line neighborhood avoiding cross-thread overlaps.
* **Sub-Allocator Lock Contention**:
  * **Current State**: As multiple threads attempt to carve rows out of the same large page pool, they inevitably collide on central allocator locks (`heapletLock`) controlling that specific page manager map.
  * **Action - Strict TLABs**: Enforce strict **Thread-Local Allocation Buffers (TLABs)**. Assign entire large pages to specific threads. The thread sub-allocates from its private page completely lock-free until exhausted, only hitting the central spinlock to request a fresh page from the OS.