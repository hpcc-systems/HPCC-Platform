# Roxie Knowledge Base

> **Note**: This file is maintained by engineers and the `Roxie Developer` AI agent. It tracks critical facts, high-level architecture, performance bottlenecks, and execution mechanics specifically for the Roxie component.

---

## 1. System Priorities
Roxie is the rapid data delivery engine for the HPCC platform. It supports millions of dollars in revenue so its engineering focus revolves entirely around **reliability** and **performance**.

## 2. Memory Management
*   **Dual Allocators**: Avoid allocations on critical execution paths to secure predictable latency.
*   **Custom Memory**: `roxie/roxiemem` contains a highly specialized memory manager. If making modifications here, consult both inline comments and `MemoryManager.md`.
*   **Standard Heap**: For the standard C++ heap (used heavily for structures like indexes, buffers, and temporary strings), the standard system allocator is replaced with `jemalloc` globally.

## 3. Concurrency & Contention
*   **Bottlenecks**: Multi-threading bottlenecks (thread contention, locking) dictate maximum throughput under load.
*   **jhtree Node Cache Architecture**: The index node cache implementation found in [`system/jhtree/jhtree.cpp`](../../system/jhtree/jhtree.cpp) is fiercely optimized. Any change touching it requires extreme scrutiny as it is a major contention point.
    *   **Subcaching**: To reduce thread contention under heavy query load, the `CNodeCache` partitions memory across multiple MRU subcaches (`cacheBuckets`), assigning keys via a shift hash (`cacheBits`). See [`system/jhtree/jhtree.cpp`](../../system/jhtree/jhtree.cpp).
    *   **Separation of Concerns**: Caching limits are enforced strictly by node type via a three-pronged `CacheMax` approach: Branches (`maxNodeMem`), Leaves (`maxLeaveMem`), and Blobs (`maxBlobMem`).

## 4. Query Execution Pipeline
*   **Index Reading (I/O)**: Roxie spends >50% of its lifespan searching and reading indexes. Improvements to I/O logic, buffering, caching, or read layout directly drive up overall throughput.
*   **"Hybrid" Index Processing**: Roxie indexes act mostly in the `hybrid` format (`BlockCompression = 5` for leaves, `InplaceCompression = 2` for branches). This aims to achieve an optimal balance - minimizing disk I/O through block-compressed leaves while retaining fast, in-memory CPU traversal speeds for inplace-compressed branches. See `HybridIndexCompressor` in [`system/jhtree/jhblockcompressed.cpp`](../../system/jhtree/jhblockcompressed.cpp#L391).
*   **Activity Choreography**: The way internal server activities interact can be a massive point of friction. Inefficiencies here drag down every query sequentially.

## 5. Startup & Scaling
*   **Query Loading**: The methodology by which Roxie loads queries at startup dictates how rapidly dynamic clusters can spin up and scale sideways to meet user demand.

## 6. Network Boundaries
*   **Worker/Server Communication**: Pushing data between the central Roxie server nodes and Roxie workers impacts latency. Watch cross-chatter closely.
*   **UDP / TCP Alternatives**: Roxie relies heavily on either UDP (`roxie/udplib/`) or TCP abstractions (`system/security/securesocket/socketutils.hpp`) for internal communication, as they are alternatives rather than used alongside each other. Historically, UDP has been used and remains the default for production systems. TCP is believed to have advantages but has not been used in anger in production environments yet.

## 7. Detailed Documentation Index
For deeper dives into specific architectural areas and to help direct AI agents, refer to the following specialized knowledge base documents:

*   [Memory Management](memory.md) - Deep dive into `roxiemem`, caching, and memory limits.
*   [Networking & Transport](networking.md) - Details on UDP vs TCP, IP multicasting, and messaging.
*   [Topologies & Deployment](topologies.md) - Constraints for Massively Parallel, Single-VM, and Ephemeral deployments.
*   [Server Logic & Flow](server-logic.md) - Core Roxie server mechanics and query dispatching.
*   [Worker Activities](worker-activities.md) - Execution nodes and graph execution inside workers.
*   [Server-to-Worker Communication](server-to-worker.md) - Request dispatching mechanism and packet management.
*   [Worker-to-Server Communication](worker-to-server.md) - Results routing and telemetry back to the server.
*   [Query Invariants and Parsing](query-invariants-and-parsing.md) - Pre-execution parsing and subgraph preparation.
*   [Startup & Configuration](startup-and-config.md) - Initial query loading, dynamic topology, and hot-swapping.
*   [Parallel Execution](parallel-execution.md) - Multi-threading, strands, and contention optimization.
*   [Keyed Joins & Splitters](keyed-joins.md) - Specific optimization characteristics for distributed joins and data correlation.
*   [Optimizations Backlog](optimizations.md) - The main repository for all architectural bottlenecks, proposed fixes, and ongoing investigations.

