# Roxie Memory Architecture & Analysis

**Overview**:
The roxiemem component provides custom, highly-performant heap allocation schemes (CChunkingRowManager) designed around query lifetime limits and block allocators optimized for Roxie's execution model.

## Allocation Hierarchy and Structures
1. **Row Manager (CChunkingRowManager in roxie/roxiemem/roxiemem.cpp)**:
   - Manages an array of size-classified heaps (normalHeaps) of type CFixedChunkedHeap.
   - Also dynamically manages per-activity blocks, and huge allocations (CHugeHeap). All are governed by severe memory footprint tracking (callbacks, OOM handling).
2. **Chunked Heaps (CChunkedHeap and derivatives)**:
   - Contains a pool of ChunkedHeaplet (or FixedSizeHeaplet/PackedFixedSizeHeaplet) instances.
   - Handles the fixed-size row layout allocating chunks aligned to HEAP_ALIGNMENT_SIZE boundaries.
   - CHeap::heapletLock (a CriticalSection) gates the allocation process.
3. **Heaplet (ChunkedHeaplet)**:
   - Represents an exact 256kb block (by default) HEAP_ALIGNMENT_SIZE which serves equally-sized requests.
   - Frees rows via inlineReleasePointer using a Lock-Free logic.

## Concurrency and Deallocation (The Fast-Path & ABA Solution)
- **Lock-Free Deallocations**: 
  - While allocations are guarded by heapletLock (an OS CriticalSection / Spinlock), freeing memory via FixedSizeHeaplet::noteReleased (which calls inlineReleasePointer) operates entirely lock-free using std::atomic operations. 
  - This severely limits contention when thousands of threads tear down datasets.
- **ABA Problem Prevention**: 
  - Adding rows back to the chunk free-list (r_blocks linked structure) via compare_exchange_efficient(r_blocks...) in inlineReleasePointer suffers inherently from the ABA problem. 
  - roxiemem defends against this by masking the top portion of the 32-bit r_blocks value as an incrementing CAS tag (RBLOCKS_CAS_TAG_MASK).

## Noteworthy Options
- **Scanning Allocator (ALWAYS_USE_SCAN_HEAP)**:
  Currently commented out, but there exists a lock-free linear scan allocator alternative. It attempts to scan tags linearly instead of using the r_blocks free list, but suffers pathological costs when fragmented memory forces massive numbers of cache-miss scans ("Excessive scans").

## Bottlenecks & Contention Points
1. **Allocation Serialization (heapletLock)**: 
   Since CChunkedHeap::doAllocateRow takes a lock before checking its activeHeaplet list, highly concurrent identical-size queries strictly serializing on heapletLock.
2. **Buffer States (activeBufferCS)**:
   CChunkingRowManager::activeBuffs has a critical section activeBufferCS explicitly marked // Potentially slow. 
3. **Dynamic Heaps (fixedSpinLock)**:
   CChunkingRowManager::fixedSpinLock synchronizes fixedHeaps list access and reportEmptyHeaps. Marked "possibly be a ReadWriteLock".

## Standard Heap vs. Roxiemem (Jemalloc Fallback analysis)
Many peripheral objects in Roxie execution paths rely on the standard C++ heap (backed by `jemalloc`). While `jemalloc` is generally performant, short-lived, frequently resizing objects bypass `roxiemem` and incur overhead:
- **StringBuffer/SCMStringBuffer**: Heavily utilized for string manipulation, query parameter packing/unpacking, and generating result labels (e.g., in `roxie/ccd/ccdquery.cpp` and `roxie/ccd/ccdserver.cpp`). Successive appends cause `realloc` operations, triggering tcache thrashing, fragmentation, and potential bin-lock contention for medium-sized buffers.
- **MemoryBuffer**: Used extensively for network data serialization (e.g., `MemoryBuffer` metadata inside `roxie/udplib/udpmsgpk.cpp` and packet collating). As data chunks are read from indexes across nodes, arrays re-size aggressively.
- **IPropertyTree (`jptree.hpp`)**: Standard properties tree components used for XML/JSON payload processing parse inputs into numerous intermediate heap-allocated strings, lists, and property nodes, none of which utilize `roxiemem`'s chunked fast-release.

**Inefficiencies Explained**:
- **Tcache Thrashing**: Creating thousands of temporary strings and intermediate parsing nodes pollutes `jemalloc`'s thread-local cache. When the tcache overflows, memory gets evicted to the central arena requiring locks.
- **Lock Contention**: While `jemalloc` employs arenas to limit contention, high concurrency across all threads competing for string allocations simultaneously on hot boundary paths natively induces lock stalls compared to `roxiemem`'s tailored lock-free deallocations (`inlineReleasePointer`).
- **TLB Misses & Fragmentation**: Rapid recycling of unpredictable array sizes (growing via `MemoryBuffer` limits) leads to heavy virtual memory un-mappings/re-mappings. TLB entries get invalidated, resulting in higher latency over multi-gigabyte heap traversals per query.
