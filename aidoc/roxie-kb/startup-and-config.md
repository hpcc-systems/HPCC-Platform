# Roxie Startup and Query Loading Architecture

This document describes the initialization sequence and scaling bottlenecks for the Roxie component, specifically analyzing `roxie/ccd/ccdmain.cpp` and `roxie/ccd/ccdstate.cpp`.

## Entry Sequence (`roxie_main`)

The execution begins in `roxie_main`, executing several critical initializations that dictate engine topology, networking capabilities, and memory boundaries.

1. **Topology & Configuration Binding**: Settings are aggregated from `loadConfiguration` (falling back to YAML or XML topology). It dynamically injects workunit-level configurations if executed directly upon a WorkUnit, allowing testing settings to override persistent configs.
2. **Mode Switch (One-Shot vs. Daemon)**:
   - If loading a standalone query DLL or specific WorkUnit (`--selftest` or WUID argument), `oneShotRoxie` / `runOnce` modes apply.
   - Single-use mode skips heavy file-cache workers, omits large agent loop setups, and bypasses plugin preloads (`queryFileCache().start()` and `loadPlugins()` are bypassed).
3. **Hardware / Resource Probing**: Allocates logical constraints (`totalMemoryLimit`, huge-pages rules, `numAgentThreads`, `numServerThreads`). Computes concurrency settings and bounds I/O chunking (`indexReadChunkSize`, `mtu_size` for UDP/TCP max limits).
4. **Memory Structures**: Sets limits for custom memory managers (`roxiemem::setTotalMemoryLimit`), binds `nodeCacheMem` and `leafCacheMem` tightly to `jhtree` bounds which inherently affect scaling. 
5. **Networking Pre-flight**: Resolves multicast listeners (`ccdMulticastPort`). Determines worker network topology structure based on cluster layout.

## Query Loading and Concurrency

The ultimate bottleneck dictates how fast a Roxie pod or node can service traffic: **Loading the Query Set and Binding File Indexes**. 

When `globalPackageSetManager->load()` executes, it dives into `ccdstate.cpp`, which manages query set deployment. The speed at which queries bind relies on two fundamental concurrency levels:
1. `parallelQueryLoadThreads` (Default: 1, scaled in config)
2. `numResolveFilenameThreads` (Default: `parallelQueryLoadThreads * 4` if using remote storage)

### Execution Flow of Query Loading
To prevent blocking initialization indefinitely against cloud storage architectures (e.g. Blob/S3 or remote NFS queries):
1. **Phase 1: WorkUnit Expansion (`parallelQueryLoadThreads`)**
   - Ingests the query XML via `createQueryDll`.
   - Iterates the WorkUnit components. Instead of resolving file locations inline, it gathers all `SummaryType::ReadIndex` and `SummaryType::ReadFile` dependencies and generates logical paths (`expandLogicalFilename`).
   - Paths are tracked in a unified package `SummaryMap` inside a lightweight lock `CriticalSection filenameCrit`.
2. **Phase 2: I/O File Resolution (`numResolveFilenameThreads`)**
   - Iterates physical names generated in Phase 1 asynchronously.
   - For remote environments, retrieving file sizes from remote storage happens heavily in parallel. 
   - Files are cached via `package->lookupExpandedFileName(...)` so subsequent lookups are O(1).
3. **Phase 3: Index Pre-Opening (The Chokepoint)**
   - If `preopenActiveIndexes` is true, eagerly checks `resolvedfile->getKeyArray()` and loops all active file parts to call `thisKey->ensureReady()`. This pulls node roots into memory. Pre-opening prevents the *first query penalty* but slows down container scale-outs.
4. **Phase 4: Factory Instantiation (`parallelQueryLoadThreads`)**
   - Assembles the validated dll resources calling `loadQueryFromDll`, binding execution state graphs, and finalizing query insertion to Roxie's active map.

## Scaling Time Optimization Details

Several techniques and toggles govern Roxie's startup latency constraints:

- **Lazy Opening (`@lazyOpen`)**: The default is `smart`. If `restarts > 0` (meaning a crash or container restart), `lazyOpen` switches to `true`. This skips index validation entirely at startup to bring the server immediately online, passing the latency inherently down to the subsequent first query that tries hitting the missing/unverified storage nodes.
- **Dependency Execution**: Options such as `defaultExecuteDependenciesSequentially` dictate if loaded sub-queries delay initialization.
- **File Timestamp / Hash Validation**: Verifying that `alwaysTrustFormatCrcs`, `ignoreFileDateMismatches`, and `ignoreFileSizeMismatches` are safely bypassed inside the resolution loop minimizes Dali calls or FileSystem metadata probing.
- **Node Hash Caching**: During file verification, the query set computes a hash string of all dependencies. If the hashes match, DLLs and factory instances can bypass parsing WorkUnit components (`addQuery`). 

## Key Learnings for Architectural Enhancements
- Modifying `preopenActiveIndexes` in containerized environments allows orchestrators to register readiness faster.
- Modifying `parallelQueryLoadThreads` is essential on cores with high count but must be weighed against CPU starvation. 
- Overhauling `roxie_main`'s initialization sequencing requires acknowledging single-mode (`oneShotRoxie`) vs farm processes; memory must initialize before topologies load DLLs, and caching must initiate only after DLL factory binding is determined parallel-safe.