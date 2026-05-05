# Roxie Deployment Topologies

> **Note**: Roxie must perform reliably across vastly different physical footprints. When optimizing or tracing code, always consider the impact against these three primary production configurations.

## Topology A: Massive Scale Interactive (Hot)
- **Scale**: 100 nodes, 50 channels, 2 replicas per channel.
- **Storage**: Each worker manages access to ~10TB of index data stored on local NVME.
- **Profile**: **Interactive / Hot Cache**.
- **Bottleneck Risks**: Network incast (Server-to-Worker and Worker-to-Server scaling), cross-node message sorting, locking on globally shared memory mechanisms like `roxiemem`, and NVME I/O saturation.

## Topology B: Single-VM Interactive (Hot)
- **Scale**: 20 nodes, 10 channels, 2 replicas per channel. *All nodes run on the same virtual machine.*
- **Storage**: 10-20TB total index size stored on local NVME.
- **Profile**: **Interactive / Hot Cache**.
- **Bottleneck Risks**: Extreme OS context-switching, CPU cache trashing, and hypervisor limits. Since all 20 nodes share the exact same physical memory bus and CPU threads, synchronization locks (like `groupsCrit` or `heapletLock`) or thread-heavy operations will severely throttle the entire VM simultaneously. 

## Topology C: Ephemeral Remote / Batch (Cold)
- **Scale**: 10 nodes, 10 channels, 1 replica.
- **Storage**: ~200TB total data stored heavily on Remote Storage (e.g., S3, Azure Blob, etc.).
- **Profile**: **Batch / Cold Cache**. 
- **Bottleneck Risks**: The cluster is dynamically provisioned, executes from cold-start, and quickly tears down. Startup speeds (e.g. `lazyOpen`, phase-split parallel query loading), massive initial I/O latency, and caching warm-up are the limiting factors. CPU bounds are less of an issue compared to remote storage block-read latencies.
