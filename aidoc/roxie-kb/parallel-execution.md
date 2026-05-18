# Roxie Parallel Execution Opportunities

## Overview
This document outlines potential areas within the Roxie codebase where operations that currently execute sequentially, block synchronously, or wait on I/O could be parallelized to improve overall throughput and reduce query latency.

## Disk Read Activities (`roxie/ccd/ccdactivities.cpp`)
- **Key Classes**: `CRoxieDiskReadBaseActivity`, `CRoxieDiskReadActivity`, `CRoxieCsvReadActivity`, `CRoxieXmlReadActivity`
- **Current State**: Row fetching relies heavily on sequential record processors (`ReadRecordProcessor`, `CsvRecordProcessor`). I/O reads within these activities typically block the calling thread.
- **Optimization Potential**: 
  - Overlap disk I/O requests with processing. We can dispatch multiple asynchronous I/O requests for subsequent chunks of file data while the current chunk is deserialized and passed into the `RowBuilder` (e.g., `OptimizedRowBuilder`).
  - Introduce an asynchronous pre-fetch queue for `RecordProcessor`s, maintaining a buffer of fetched records from worker nodes or disk before the upper-activity loop demands them.

## Remote Result Merging (`roxie/ccd/ccdserver.cpp`)
- **Key Classes**: `CRemoteResultMerger` (and associated server components)
- **Current State**: Server gathers results from multiple nodes over UDP/TCP. Synchronous locks/waits are often required to merge row sets deterministically.
- **Optimization Potential**: 
  - Utilize parallel reduction or lock-free concurrent queues when capturing incoming packets from `udplib` instead of locking a centralized merge structure.
  - Implement speculative parallel parsing of inbound network packets before the deterministic merge phase.

## Index Reads / Node Cache (`system/jhtree/`)
- **Files**: `system/jhtree/jhtree.cpp`, `system/jhtree/jhcache.cpp`
- **Current State**: Index reads (>50% of Roxie's time) often block while waiting for a node to be fetched from disk and decompressed via `hlzw.cpp`. The node cache (`jhcache.hpp`) uses synchronization to handle concurrent tree traversals.
- **Optimization Potential**: 
  - **Concurrent Decompression**: When multiple independent leaf nodes need fetching for a branch, their decompression (`jhtree` fetching block compressed layouts) can be assigned to thread-pool workers rather than blocking the main traversal thread.
  - **Prefetch Hinting**: Inject prefetch instructions for index branches early in the query evaluation tree so the OS or Roxie I/O layer brings nodes into the memory-mapped cache before traversal hits them.

## Next Steps
- Implement PoC for asynchronous read-ahead in `CRoxieCsvReadActivity`.
- Profile `CRemoteResultMerger` contention under 10k QPS to validate parallel reduction viability.
