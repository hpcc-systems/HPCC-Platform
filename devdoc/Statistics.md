# HPCC Platform Statistics System

This document provides comprehensive information about the statistics system in HPCC Platform, including which statistics are captured, how they are aggregated, their update frequencies, and guidance for interpreting them.

## Overview

The HPCC Platform statistics system captures detailed performance and operational metrics across all platform components (Thor, Roxie, ECL Compiler, ESP, etc.). Statistics are organized into hierarchical scopes (graphs, activities, edges) and can be aggregated across multiple nodes and time periods.

## Statistics Architecture

### Core Components

- **StatisticKind**: Enumeration defining all available statistic types
- **StatisticMeasure**: Units of measurement (time, size, count, etc.)
- **StatsMergeAction**: How statistics are combined when aggregated
- **StatisticsMapping**: Defines which statistics are collected for each activity type
- **CRuntimeStatisticCollection**: Runtime collection and storage of statistics

### Scope Types

Statistics are collected within different scope types:

| Scope Type | Description | Scope Prefix |
|------------|-------------|--------------|
| SSTglobal | Root scope for entire workunit | - |
| SSTgraph | Individual graph execution | `graph` |
| SSTsubgraph | Subgraph within a graph | `sg` |
| SSTactivity | Individual activity execution | `a` |
| SSTedge | Data flow between activities | `e` |
| SSTfunction | Function call statistics | `f` |
| SSTworkflow | Workflow execution | `w` |
| SSTfile | File operation statistics | `p` |
| SSTchannel | Roxie channel statistics | `x` |

### Automatic Aggregation

Certain statistics are automatically aggregated from subgraph scope level to parent scopes (activity, graph, global) by the StatisticsAggregator. The following statistics are included in the standard aggregation set:

- **StCostFileAccess** - File access costs are summed across all subgraphs
- **StSizeGraphSpill** - Graph spill sizes are aggregated (using max merge)
- **StSizeSpillFile** - Spill file sizes are summed across subgraphs

This aggregation ensures that resource usage metrics are properly rolled up to provide accurate totals at higher scope levels.

#### Merge Actions and Aggregation Logic

Each statistic has a defined `StatsMergeAction` that determines how values are combined during aggregation:

| Merge Action | Behavior | Use Cases |
|--------------|----------|-----------|
| **StatsMergeSum** | Add values together | Counts (rows processed, disk reads), sizes (bytes read), costs |
| **StatsMergeMax** | Take maximum value | Peak memory usage, maximum group sizes, largest data structures |
| **StatsMergeMin** | Take minimum value | Best-case timings, minimum resource requirements |
| **StatsMergeReplace** | Use latest value | Status indicators, timestamps, configuration values |
| **StatsMergeAppend** | Concatenate text values | Error messages, debug information |
| **StatsMergeKeepNonZero** | Keep existing non-zero value, or use new value if existing is zero | Initialization patterns, fallback values |
| **StatsMergeFirst** | Keep first value encountered | Initial state preservation |
| **StatsMergeLast** | Use last value encountered | Final state tracking |

#### Multi-Node Aggregation

Statistics are collected independently on each node and then aggregated using statistical variants:

**Multi-Node Statistical Variants:**
```cpp
// Example: Execution time across 10 Thor slave nodes
StTimeElapsed      = 45000000000ns    // Sum of all node times
StTimeMinElapsed   = 4200000000ns     // Fastest node completion
StTimeMaxElapsed   = 4800000000ns     // Slowest node completion  
StTimeAvgElapsed   = 4500000000ns     // Average per node
StSkewElapsed      = 12.5%            // Load imbalance indicator
```

**Skew Calculation:** Skew measures load balance across nodes:
- `Skew = (Max - Min) / Average * 100`
- 0% = Perfect balance, >50% indicates significant imbalance
- High skew often indicates data distribution problems or resource contention


## Complete StatisticKind Reference

### Core Interpretation Principles

#### Understanding Measurement Units

**Time Statistics (nanoseconds):**
- Values are in nanoseconds for precision, but typically displayed in human-readable units
- `StTimeElapsed = 45000000000` = 45 seconds
- Use for identifying bottlenecks, comparing operation durations
- **Caveat:** Time statistics may include wait time, not just active processing

**Size Statistics (bytes):**
- Memory and disk measurements in bytes
- `StSizePeakMemory = 2147483648` = 2GB peak memory usage
- Use for capacity planning, memory optimization
- **Caveat:** Peak measurements don't indicate sustained usage patterns

**Count Statistics:**
- Simple counters for occurrences, iterations, operations
- `StNumRowsProcessed = 1000000` = 1 million rows processed
- Use for throughput analysis, operation frequency
- **Caveat:** Counts don't indicate processing complexity per item

**Cost Statistics (currency × 10^6):**
- Internal representation multiplied by 1,000,000 for precision
- `StCostExecute = 1500000` = $1.50 execution cost
- Use for resource optimization
- **Caveat:** Cost models may not reflect actual cloud billing

#### Statistical Variants and Their Meanings

**Distribution Analysis:**
```
StTimeElapsed variants across 8 Thor nodes:
- StTimeElapsed      = 360s  (total CPU time across all nodes)
- StTimeMinElapsed   = 42s   (fastest node completed in 42s)
- StTimeMaxElapsed   = 48s   (slowest node took 48s)
- StTimeAvgElapsed   = 45s   (average per node: 360s ÷ 8 nodes)
- StSkewElapsed      = 13%   (load imbalance: (48-42)/45 * 100)
```

### Known Limitations and Caveats

#### Timing Accuracy

**Cycle vs Time Statistics:**
- Cycle statistics (StCycle*) provide higher precision than time statistics
- Time conversion introduces small errors (~1μs granularity)

**Elapsed Time Interpretation:**
- `StTimeElapsed` includes all wait time (I/O, network, blocking)
- `StTimeLocalExecute` excludes input processing, shows actual work (but may not account for read-ahead thread execution time; parallel processing in activities like parallel projects makes timing for current and upstream activities challenging)
- `StTimeTotalExecute` includes input dependencies

#### Memory Measurements

**Peak vs Current Usage:**
- Peak statistics show high-water marks, not sustained usage
- Memory may be temporarily allocated then released
- Use for capacity planning, not average consumption estimation

**Memory Scope Variations:**
- `StSizeMemory`: Total process memory allocation
- `StSizeRowMemory`: Row buffer memory only  
- `StSizePeakMemory`: Maximum memory ever allocated




This section provides a comprehensive reference of all 236 base StatisticKind values defined in the HPCC Platform. Each statistic can also have variants (Min/Max/Avg/Skew) that provide additional aggregation information across distributed nodes.

### Quick Reference by Category

#### Time Statistics (time in nanoseconds)
Time measurements in nanoseconds - used for duration and elapsed time tracking.

#### Timestamp Statistics (timestamp in microseconds) 
Point-in-time measurements in microseconds - used for tracking when events occur.

#### Count Statistics (count)
Integer counters - used for tracking occurrences, iterations, and quantities.

#### Size Statistics (size in bytes)
Memory and disk size measurements in bytes.

#### Cycle Statistics (cpu cycles)
CPU cycle measurements for low-level performance analysis. **Note**: StCycle* statistics are used internally for precise time tracking and are automatically converted to equivalent StTime* statistics for reporting. Cycle statistics are never stored permanently or reported directly.

#### Cost Statistics (cost)
Resource cost measurements for billing and optimization.

**Storage Format**: Cost statistics are stored internally using the **currency × 10^6** format, meaning all values are multiplied by 1,000,000 before storage. This allows storing fractional currency values as integers while maintaining 6 decimal places of precision.

**Type Conversion and Usage**:
- **Internal Processing**: Always use `cost_type` for storing, processing, and manipulating cost values within the system
- **Presentation Only**: Only convert to `double` (money type) when displaying costs to users or in reports
- **Conversion Functions**: Use the provided conversion functions to ensure consistency:
  - `money2cost_type(double money)` - Convert from display currency to internal cost_type
  - `cost_type2money(cost_type cost)` - Convert from internal cost_type to display currency
- **Data Integrity**: This approach prevents accidental mixing of internal cost_type values with external money values, ensuring mathematical operations and comparisons work correctly throughout the codebase

### Statistics Reference

| StatisticKind | Measure | Description | Update Frequency | Developer Notes |
|---------------|---------|-------------|------------------|----------------|
| StKindNone | n/a | Base/null statistic kind | N/A | Used as placeholder |
| StKindAll | n/a | Wildcard for all statistics | N/A | Used in filters |
| StWhenGraphStarted | timestamp (μs) | Graph start time | At milestone | **Deprecated** - use StWhenStarted |
| StWhenGraphFinished | timestamp (μs) | Graph finish time | At milestone | **Deprecated** - use StWhenFinished |
| StWhenFirstRow | timestamp (μs) | First row processed timestamp | At milestone | Critical for latency analysis |
| StWhenQueryStarted | timestamp (μs) | Query start time | At milestone | **Deprecated** - use StWhenStarted |
| StWhenQueryFinished | timestamp (μs) | Query finish time | At milestone | **Deprecated** - use StWhenFinished |
| StWhenCreated | timestamp (μs) | Item creation timestamp | At milestone | - |
| StWhenCompiled | timestamp (μs) | Compilation start time | At milestone | - |
| StWhenWorkunitModified | timestamp (μs) | Workunit modification time | At milestone | **Unused** |
| StTimeElapsed | time (ns) | Total elapsed time | On completion | |
| StTimeLocalExecute | time (ns) | Local execution time | On completion | Time spent in activity processing.  It excludes any input processing time |
| StTimeTotalExecute | time (ns) | Total execution time | On completion | Time spent in activity processing including time spent in all input activities |
| StTimeRemaining | time (ns) | Remaining time estimate | Every 30 seconds (abort check) | **Unused** |
| StSizeGeneratedCpp | size (bytes) | Generated C++ file size | On completion | Compiler metric |
| StSizePeakMemory | size (bytes) | Peak memory usage | At graph end | Critical for memory analysis |
| StSizeMaxRowSize | size (bytes) | Maximum row memory size | At graph end | RoxieMemory high-water mark |
| StNumRowsProcessed | count | Rows processed count | Every 30 seconds (Thor progress) | Core throughput metric |
| StNumSlaves | count | Parallel process count | On start | Scale indicator |
| StNumStarts | count | Activity start count | On start/stop | Should match StNumStops |
| StNumStops | count | Activity stop count | On start/stop | Activity lifecycle tracking |
| StNumIndexSeeks | count | Index keyed lookups | Every 30 seconds (Thor progress) | KEYED() filter operations |
| StNumIndexScans | count | Index scan operations | Every 30 seconds (Thor progress) | Post-seek sequential reads |
| StNumIndexWildSeeks | count | WILD() filter seeks | Every 30 seconds (Thor progress) | Pattern matching operations |
| StNumIndexSkips | count | Smart-stepping increments | Every 30 seconds (Thor progress) | Index optimization metric |
| StNumIndexNullSkips | count | Failed smart-steps | Every 30 seconds (Thor progress) | Indicates poor priority tuning |
| StNumIndexMerges | count | Smart-step merge setups | Every 30 seconds (Thor progress) | - |
| StNumIndexMergeCompares | count | Smart-step comparisons | Every 30 seconds (Thor progress) | - |
| StNumPreFiltered | count | Pre-lookup filtering | Every 30 seconds (Thor progress) | LEFT rows filtered |
| StNumPostFiltered | count | Post-match filtering | Every 30 seconds (Thor progress) | Transform/filter rejections |
| StNumBlobCacheHits | count | Blob cache hits | Per access (at graph end) | Cache efficiency metric |
| StNumLeafCacheHits | count | Leaf node cache hits | Per access (at graph end) | Index performance |
| StNumNodeCacheHits | count | Branch node cache hits | Per access (at graph end) | Critical for performance |
| StNumBlobCacheAdds | count | Blob cache misses | Per access (at graph end) | Disk reads required |
| StNumLeafCacheAdds | count | Leaf cache misses | Per access (at graph end) | Consider cache tuning |
| StNumNodeCacheAdds | count | Branch cache misses | Per access (at graph end) | High impact on performance |
| StNumPreloadCacheHits | count | Preload cache hits | Per access (at graph end) | **Unused** |
| StNumPreloadCacheAdds | count | Preload cache misses | Per access (at graph end) | **Unused** |
| StNumServerCacheHits | count | Server cache hits | Per access (at graph end) | **Unused** |
| StNumIndexAccepted | count | KEYED JOIN matches | Every 30 seconds (Thor progress) | Transform returned result |
| StNumIndexRejected | count | KEYED JOIN skips | Every 30 seconds (Thor progress) | Transform filtered out |
| StNumAtmostTriggered | count | ATMOST failures | Every 30 seconds (Thor progress) | JOIN limit exceeded |
| StNumDiskSeeks | count | Disk FETCH operations | Every 30 seconds (Thor progress) | File system seeks |
| StNumIterations | count | LOOP iterations | Every 30 seconds (Thor progress) | Control flow metric |
| StLoadWhileSorting | cpu load | Sort CPU load | Every 30 seconds (abort check) | **Unused** |
| StNumLeftRows | count | LEFT rows in joins | Every 30 seconds (Thor progress) | Join cardinality |
| StNumRightRows | count | RIGHT rows in joins | Every 30 seconds (Thor progress) | Join cardinality |
| StPerReplicated | percentage | Replication progress % | On progress | Data distribution status |
| StNumDiskRowsRead | count | File rows read | Every 30 seconds (Thor progress) | File I/O metric |
| StNumIndexRowsRead | count | Index rows read | Every 30 seconds (Thor progress) | Index I/O metric |
| StNumDiskAccepted | count | Disk rows accepted | Every 30 seconds (Thor progress) | Transform results |
| StNumDiskRejected | count | Disk rows rejected | Every 30 seconds (Thor progress) | Transform filtering |
| StTimeSoapcall | time (ns) | SOAPCALL duration | Per call (at graph end) | External service calls |
| StTimeFirstExecute | time (ns) | First row latency | On completion | Startup overhead |
| StTimeDiskReadIO | time (ns) | Disk read time | Per I/O (30s progress) | I/O performance |
| StTimeDiskWriteIO | time (ns) | Disk write time | Per I/O (30s progress) | I/O performance |
| StSizeDiskRead | size (bytes) | Disk bytes read | Every 30 seconds (Thor progress) | I/O volume |
| StSizeDiskWrite | size (bytes) | Disk bytes written | Every 30 seconds (Thor progress) | I/O volume |
| StCycleDiskReadIOCycles | cpu cycles | Read I/O CPU cycles | Per I/O (30s progress) | For internal time tracking. Should not be reported. Convert to StTimeDiskReadIO to report. |
| StCycleDiskWriteIOCycles | cpu cycles | Write I/O CPU cycles | Per I/O (30s progress) | For internal time tracking. Should not be reported. Convert to StTimeDiskWriteIO to report. |
| StNumDiskReads | count | Disk read operations | Every 30 seconds (Thor progress) | I/O operation count |
| StNumDiskWrites | count | Disk write operations | Every 30 seconds (Thor progress) | I/O operation count |
| StNumSpills | count | Memory spill count | Per spill (30s progress) | Memory pressure indicator |
| StTimeSpillElapsed | time (ns) | Spill operation time | Per spill (30s progress) | Memory management overhead |
| StTimeSortElapsed | time (ns) | Sort operation time | Per sort (30s progress) | Sorting performance |
| StNumGroups | count | Groups processed | Every 30 seconds (Thor progress) | GROUP operation metric |
| StNumGroupMax | count | Largest group size | On completion | Skew detection |
| StSizeSpillFile | size (bytes) | Spill file size | Per spill (30s progress) | **Auto-aggregated**<sup>1</sup> - Disk usage from spills |
| StCycleSpillElapsedCycles | cpu cycles | Spill CPU cycles | Per spill (30s progress) | Internal timing - converted to StTimeSpillElapsed |
| StCycleSortElapsedCycles | cpu cycles | Sort CPU cycles | Per sort (30s progress) | Internal timing - converted to StTimeSortElapsed |
| StNumStrands | count | Parallel strands | On start | **Partially implemented** |
| StCycleTotalExecuteCycles | cpu cycles | Total execution cycles | On completion | Internal timing - converted to StTimeTotalExecute |
| StNumExecutions | count | Graph executions | Per execution (30s progress) | Execution frequency |
| StTimeTotalNested | time (ns) | Total nested time | On completion | **Unused** |
| StCycleLocalExecuteCycles | cpu cycles | Local execution cycles | On completion | Internal timing - converted to StTimeLocalExecute |
| StNumCompares | count | Comparison operations | Every 30 seconds (Thor progress) | **Unused** |
| StNumScansPerRow | count | Scans per row | Per row (30s progress) | **Unused** |
| StNumAllocations | count | Memory allocations | Per allocation (at graph end) | Memory management |
| StNumAllocationScans | count | Allocation scans | Per scan (30s progress) | Heap fragmentation indicator |
| StNumDiskRetries | count | I/O retry operations | Per retry (30s progress) | Storage reliability issue |
| StCycleElapsedCycles | cpu cycles | Elapsed CPU cycles | On completion | For internal time tracking. Should not be reported. Convert to StTimeElapsed to report. |
| StCycleRemainingCycles | cpu cycles | Remaining cycles | Every 30 seconds | **Unused** |
| StCycleSoapcallCycles | cpu cycles | SOAPCALL CPU cycles | Per call (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeSoapcall to report. |
| StCycleFirstExecuteCycles | cpu cycles | First execute cycles | On completion | For internal time tracking. Should not be reported. Convert to StTimeFirstExecute to report. |
| StCycleTotalNestedCycles | cpu cycles | Nested operation cycles | On completion | **Unused** |
| StTimeGenerate | time (ns) | Code generation time | On completion | Compiler performance |
| StCycleGenerateCycles | cpu cycles | Generation CPU cycles | On completion | For internal time tracking. Should not be reported. Convert to StTimeGenerate to report. |
| StWhenStarted | timestamp (μs) | Start timestamp | At milestone | Activity lifecycle |
| StWhenFinished | timestamp (μs) | Finish timestamp | At milestone | Activity lifecycle |
| StNumAnalyseExprs | count | Analyse expressions | On completion | Compiler internal metric |
| StNumTransformExprs | count | Transform expressions | On completion | Compiler internal metric |
| StNumUniqueAnalyseExprs | count | Unique analyse expressions | On completion | Code complexity |
| StNumUniqueTransformExprs | count | Unique transform expressions | On completion | Code complexity |
| StNumDuplicateKeys | count | Index duplicate keys | Every 30 seconds (Thor progress) | Data quality metric |
| StNumAttribsProcessed | count | Attributes processed | Per attribute (at graph end) | ECL parsing metric |
| StNumAttribsSimplified | count | Simplified attributes | Per attribute (at graph end) | **Unused** |
| StNumAttribsFromCache | count | Cached attributes | Per access (at graph end) | **Unused** |
| StNumSmartJoinDegradedToLocal | count | Smart join degradations | Per degradation (30s progress) | JOIN optimization failure |
| StNumSmartJoinSlavesDegradedToStd | count | Smart join slave degradations | Per degradation (30s progress) | JOIN performance issue |
| StNumAttribsSimplifiedTooComplex | count | Complex attributes | Per attribute (at graph end) | **Unused** |
| StNumSysContextSwitches | count | System context switches | At graph end | System-wide scheduling |
| StTimeOsUser | time (ns) | OS user time | At graph end | System-level timing |
| StTimeOsSystem | time (ns) | OS system time | At graph end | Kernel time usage |
| StTimeOsTotal | time (ns) | OS total time | At graph end | Complete system time |
| StCycleOsUserCycles | cpu cycles | OS user cycles | At graph end | For internal time tracking. Should not be reported. Convert to StTimeOsUser to report. |
| StCycleOsSystemCycles | cpu cycles | OS system cycles | At graph end | For internal time tracking. Should not be reported. Convert to StTimeOsSystem to report. |
| StCycleOsTotalCycles | cpu cycles | OS total cycles | At graph end | For internal time tracking. Should not be reported. Convert to StTimeOsTotal to report. |
| StNumContextSwitches | count | Process context switches | At graph end | Process-level scheduling |
| StTimeUser | time (ns) | Process user time | At graph end | User-space execution |
| StTimeSystem | time (ns) | Process system time | At graph end | Kernel time for process |
| StTimeTotal | time (ns) | Process total time | At graph end | Complete process time |
| StCycleUserCycles | cpu cycles | Process user cycles | At graph end | For internal time tracking. Should not be reported. Convert to StTimeUser to report. |
| StCycleSystemCycles | cpu cycles | Process system cycles | At graph end | For internal time tracking. Should not be reported. Convert to StTimeSystem to report. |
| StCycleTotalCycles | cpu cycles | Process total cycles | At graph end | For internal time tracking. Should not be reported. Convert to StTimeTotal to report. |
| StSizeOsDiskRead | size (bytes) | OS disk read size | Per I/O (30s progress) | **Unused** |
| StSizeOsDiskWrite | size (bytes) | OS disk write size | Per I/O (30s progress) | **Unused** |
| StTimeBlocked | time (ns) | Blocked time | On block | Resource contention |
| StCycleBlockedCycles | cpu cycles | Blocked CPU cycles | On block | For internal time tracking. Should not be reported. Convert to StTimeBlocked to report. |
| StCostExecute | cost | Execution cost | On completion | Resource costing |
| StSizeAgentReply | size (bytes) | Agent reply size | Per response | Roxie communication |
| StTimeAgentWait | time (ns) | Agent wait time | Per request | Roxie performance |
| StCycleAgentWaitCycles | cpu cycles | Agent wait cycles | Per request (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeAgentWait to report. |
| StCostFileAccess | cost | File access cost | Per access (30s progress) | **Auto-aggregated**<sup>1</sup> - I/O costing |
| StNumPods | count | Kubernetes pods | On deployment | K8s resource usage |
| StCostCompile | cost | Compilation cost | On completion | Compiler resource usage |
| StTimeNodeLoad | time (ns) | Branch node load time | Per load (at graph end) | Index performance |
| StCycleNodeLoadCycles | cpu cycles | Node load cycles | Per load (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeNodeLoad to report. |
| StTimeLeafLoad | time (ns) | Leaf node load time | Per load (at graph end) | Index decompression |
| StCycleLeafLoadCycles | cpu cycles | Leaf load cycles | Per load (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeLeafLoad to report. |
| StTimeBlobLoad | time (ns) | Blob load time | Per load (at graph end) | Large data handling |
| StCycleBlobLoadCycles | cpu cycles | Blob load cycles | Per load (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeBlobLoad to report. |
| StTimeDependencies | time (ns) | Dependency time | On startup | Activity startup overhead |
| StCycleDependenciesCycles | cpu cycles | Dependency cycles | On startup | For internal time tracking. Should not be reported. Convert to StTimeDependencies to report. |
| StTimeStart | time (ns) | Start time | On startup | Activity initialization |
| StCycleStartCycles | cpu cycles | Start cycles | On startup | For internal time tracking. Should not be reported. Convert to StTimeStart to report. |
| StEnumActivityCharacteristics | enum value | Activity characteristics | On setup | Bitfield: urgentStart=0x01, hasRowLatency=0x02, hasDependencies=0x04, slowDependencies=0x08 |
| StTimeNodeRead | time (ns) | Branch read time | Per access (at graph end) | Includes page cache |
| StCycleNodeReadCycles | cpu cycles | Node read cycles | Per access (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeNodeRead to report. |
| StTimeLeafRead | time (ns) | Leaf read time | Per access (at graph end) | Includes page cache |
| StCycleLeafReadCycles | cpu cycles | Leaf read cycles | Per access (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeLeafRead to report. |
| StTimeBlobRead | time (ns) | Blob read time | Per access (at graph end) | Includes page cache |
| StCycleBlobReadCycles | cpu cycles | Blob read cycles | Per access (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeBlobRead to report. |
| StNumNodeDiskFetches | count | Branch disk fetches | Per miss (at graph end) | Page cache misses |
| StNumLeafDiskFetches | count | Leaf disk fetches | Per miss (at graph end) | Cache tuning indicator |
| StNumBlobDiskFetches | count | Blob disk fetches | Per miss (at graph end) | Large data cache misses |
| StTimeNodeFetch | time (ns) | Branch fetch time | Per fetch (at graph end) | Excludes page cache |
| StCycleNodeFetchCycles | cpu cycles | Node fetch cycles | Per fetch (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeNodeFetch to report. |
| StTimeLeafFetch | time (ns) | Leaf fetch time | Per fetch (at graph end) | Excludes page cache |
| StCycleLeafFetchCycles | cpu cycles | Leaf fetch cycles | Per fetch (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeLeafFetch to report. |
| StTimeBlobFetch | time (ns) | Blob fetch time | Per fetch (at graph end) | Excludes page cache |
| StCycleBlobFetchCycles | cpu cycles | Blob fetch cycles | Per fetch (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeBlobFetch to report. |
| StSizeGraphSpill | size (bytes) | Graph spill size | Per spill (30s progress) | **Auto-aggregated**<sup>1</sup> - Inter-subgraph spilling |
| StTimeAgentQueue | time (ns) | Agent queue time | Per request | Roxie load balancing |
| StCycleAgentQueueCycles | cpu cycles | Agent queue cycles | Per request (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeAgentQueue to report. |
| StTimeIBYTIDelay | time (ns) | IBYTI delay time | Per failover | Channel failover delay |
| StCycleIBYTIDelayCycles | cpu cycles | IBYTI delay cycles | Per failover (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeIBYTIDelay to report. |
| StWhenQueued | timestamp (μs) | Queue timestamp | At milestone | Request queuing |
| StWhenDequeued | timestamp (μs) | Dequeue timestamp | At milestone | Request processing start |
| StWhenK8sLaunched | timestamp (μs) | K8s job launch time | At milestone | Container orchestration |
| StWhenK8sStarted | timestamp (μs) | K8s job start time | At milestone | Container startup |
| StWhenK8sReady | timestamp (μs) | Thor ready time | At milestone | Distributed startup |
| StNumSocketWrites | count | Socket write operations | Per operation (at graph end) | Network I/O |
| StSizeSocketWrite | size (bytes) | Socket write size | Per write (at graph end) | Network bandwidth |
| StTimeSocketWriteIO | time (ns) | Socket write time | Per write (5s socket monitoring) | Network latency |
| StCycleSocketWriteIOCycles | cpu cycles | Socket write cycles | Per write (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeSocketWriteIO to report. |
| StNumSocketReads | count | Socket read operations | Per operation (at graph end) | Network I/O |
| StSizeSocketRead | size (bytes) | Socket read size | Per read (at graph end) | Network bandwidth |
| StTimeSocketReadIO | time (ns) | Socket read time | Per read (5s socket monitoring) | Network latency |
| StCycleSocketReadIOCycles | cpu cycles | Socket read cycles | Per read (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeSocketReadIO to report. |
| StSizeMemory | size (bytes) | Total memory allocated | At graph end | System memory usage |
| StSizeRowMemory | size (bytes) | Row memory size | At graph end | Data structure memory |
| StSizePeakRowMemory | size (bytes) | Peak row memory | At graph end | Memory high-water mark |
| StSizeAgentSend | size (bytes) | Agent send size | Per request (at graph end) | Roxie request size |
| StTimeIndexCacheBlocked | time (ns) | Index cache blocked time | On contention | Cache contention |
| StCycleIndexCacheBlockedCycles | cpu cycles | Cache blocked cycles | On contention | For internal time tracking. Should not be reported. Convert to StTimeIndexCacheBlocked to report. |
| StTimeAgentProcess | time (ns) | Agent processing time | Per request | Roxie worker time |
| StCycleAgentProcessCycles | cpu cycles | Agent process cycles | Per request (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeAgentProcess to report. |
| StNumAckRetries | count | Acknowledgment retries | Per retry (at graph end) | Network reliability |
| StSizeContinuationData | size (bytes) | Continuation data size | Per continuation (at graph end) | Partial result state |
| StNumContinuationRequests | count | Continuation requests | Per request (at graph end) | Multi-part responses |
| StNumFailures | count | Query failures | Per failure (at graph end) | Error rate |
| StNumLocalRows | count | Local row count | Every 30 seconds (Thor progress) | Distribution efficiency |
| StNumRemoteRows | count | Remote row count | Every 30 seconds (Thor progress) | Network overhead |
| StSizeRemoteWrite | size (bytes) | Remote write size | Every 30 seconds (Thor progress) | Distribution cost |
| StSizePeakTempDisk | size (bytes) | Peak temp disk usage | At graph end | Temporary storage |
| StSizePeakEphemeralDisk | size (bytes) | Peak ephemeral storage | At graph end | Container storage |
| StNumMatchLeftRowsMax | count | Max left rows in group | On completion | Join skew detection |
| StNumMatchRightRowsMax | count | Max right rows in group | On completion | Join skew detection |
| StNumMatchCandidates | count | Join candidate pairs | Every 30 seconds (Thor progress) | Join complexity |
| StNumMatchCandidatesMax | count | Max candidates in group | On completion | Join hotspot detection |
| StNumParallelExecute | count | Parallel execution paths | On startup | Parallelism degree |
| StNumAgentRequests | count | Agent request count | Per request| Roxie request volume |
| StSizeAgentRequests | size (bytes) | Agent request size | Per request (at graph end) | Roxie bandwidth |
| StTimeSoapcallDNS | time (ns) | SOAPCALL DNS time | Per call (at graph end) | Network resolution |
| StTimeSoapcallConnect | time (ns) | SOAPCALL connect time | Per call (at graph end) | Connection establishment |
| StCycleSoapcallDNSCycles | cpu cycles | DNS CPU cycles | Per call (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeSoapcallDNS to report. |
| StCycleSoapcallConnectCycles | cpu cycles | Connect CPU cycles | Per call (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeSoapcallConnect to report. |
| StNumSoapcallConnectFailures | count | SOAPCALL failures | Per failure (at graph end) | Connection reliability |
| StTimeLookAhead | time (ns) | Lookahead time | Every 30 seconds (Thor progress) | Prefetching overhead |
| StCycleLookAheadCycles | cpu cycles | Lookahead cycles | Every 30 seconds (Thor progress) | For internal time tracking. Should not be reported. Convert to StTimeLookAhead to report. |
| StNumCacheHits | count | Generic cache hits | Per access (at graph end) | Cache effectiveness |
| StNumCacheAdds | count | Generic cache adds | Per miss (at graph end) | Cache misses |
| StSizePeakCacheObjects | count | Peak cache objects | Every 120 seconds (cost check) | Cache sizing |
| StNumCacheDuplicates | count | Cache duplicates | Per duplicate (at graph end) | Race condition indicator |
| StNumCacheEvictions | count | Cache evictions | Per eviction (at graph end) | Memory pressure |
| StSizeOffsetBranches | size (bytes) | Branch offset position | On completion | Index structure |
| StSizeBranchMemory | size (bytes) | Branch memory size | At graph end | Index memory usage |
| StSizeLeafMemory | size (bytes) | Leaf memory size | At graph end | Index leaf storage |
| StSizeLargestExpandedLeaf | size (bytes) | Largest leaf size | On completion | Index optimization |
| StTimeDelayed | time (ns) | Delay time | On delay | Minimum query time wait |
| StCycleDelayedCycles | cpu cycles | Delay cycles | On delay | For internal time tracking. Should not be reported. Convert to StTimeDelayed to report. |
| StTimePostMortemCapture | time (ns) | Post-mortem time | On error | Debugging overhead |
| StCyclePostMortemCaptureCycles | cpu cycles | Post-mortem cycles | On error | For internal time tracking. Should not be reported. Convert to StTimePostMortemCapture to report. |
| StNumBloomAccepts | count | Bloom filter accepts | Every 30 seconds (Thor progress) | Filter effectiveness |
| StNumBloomRejects | count | Bloom filter rejects | Every 30 seconds (Thor progress) | False positive rate |
| StNumBloomSkips | count | Bloom filter skips | Every 30 seconds (Thor progress) | Filter bypass rate |
| StNumAccepts | count | Items accepted | Every 30 seconds (Thor progress) | Processing acceptance |
| StNumWaits | count | Queue waits | Per wait| Resource contention |
| StTimeProvision | time (ns) | Provisioning time | On provision | Resource allocation |
| StCycleProvisionCycles | cpu cycles | Provision cycles | On provision | For internal time tracking. Should not be reported. Convert to StTimeProvision to report. |
| StCostStart | cost | Start cost | On startup | Initialization cost |
| StTimeWaitSuccess | time (ns) | Successful wait time | Per success | Queue service time |
| StCycleWaitSuccessCycles | cpu cycles | Success wait cycles | Per success (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeWaitSuccess to report. |
| StTimeWaitFailure | time (ns) | Failed wait time | Per failure | Timeout duration |
| StCycleWaitFailureCycles | cpu cycles | Failure wait cycles | Per failure (at graph end) | For internal time tracking. Should not be reported. Convert to StTimeWaitFailure to report. |
| StCostWait | cost | Wait cost | On wait | Idle resource cost |
| StNumAborts | count | Abort count | Per abort (30s abort check) | Cancellation rate |
| StCostAbort | cost | Abort cost | Per abort (30s abort check) | Cancellation overhead |
| StNumRowsRead | count | Input rows read | Every 30 seconds (Thor progress) | Data ingestion |
| StNumRowsWritten | count | Output rows written | Every 30 seconds (Thor progress) | Data production |
| StTimeQueryConsume | time (ns) | Query consumption time | On completion | Input processing |
| StCycleQueryConsumeCycles | cpu cycles | Consume cycles | On completion | For internal time tracking. Should not be reported. Convert to StTimeQueryConsume to report. |
| StNumSuccesses | count | Success count | Per success | Success rate |
| StNumSoapcallRetries | count | SOAPCALL retries | Per retry (30s progress) | External service reliability |
| StTimeQueryPreparation | time (ns) | Query prep time | On startup | Setup overhead |
| StCycleQueryPreparationCycles | cpu cycles | Prep cycles | On startup | For internal time tracking. Should not be reported. Convert to StTimeQueryPreparation to report. |

<sup>1</sup> **Auto-aggregated**: This statistic is automatically aggregated from subgraph scope level to graph, workflow, and global scopes by the StatisticsAggregator component.

### Statistical Variants and Modifiers

Each base statistic can be combined with modifiers to create additional variants:

```cpp
// Example: Creating variant statistics
StatisticKind baseKind = StTimeElapsed;
StatisticKind minKind = (StatisticKind)(baseKind | StMinX);    // StTimeMinElapsed  
StatisticKind maxKind = (StatisticKind)(baseKind | StMaxX);    // StTimeMaxElapsed
StatisticKind avgKind = (StatisticKind)(baseKind | StAvgX);    // StTimeAvgElapsed
StatisticKind skewKind = (StatisticKind)(baseKind | StSkew);   // StSkewElapsed
```

**Available Modifiers:**

| Modifier | Value | Purpose | Measure Type |
|----------|-------|---------|--------------|
| StMinX | 0x10000 | Minimum value across all nodes | Same as base |
| StMaxX | 0x20000 | Maximum value across all nodes | Same as base |
| StAvgX | 0x30000 | Average value across all nodes | Same as base |
| StSkew | Skew measure (0 = perfectly balanced) | skew |
| StSkewMin | Minimum skew across nodes | skew |
| StSkewMax | Maximum skew across nodes | skew |
| StNodeMin | Node number containing minimum | node |
| StNodeMax | Node number containing maximum | node |
| StDeltaX | 0x90000 | Difference in values | Same as base |
| StStdDevX | 0xa0000 | Standard deviation of values | Same as base |

### Developer Usage Patterns

#### Adding New Statistics

When adding a new statistic to the system:

1. **Add to StatisticKind enum** in `jstatcodes.h`:
```cpp
enum StatisticKind
{
    // ... existing statistics
    StMyNewStatistic,               // Add before StMax
    StMax,
}
```

2. **Add metadata entry** in `jstats.cpp` statsMetaData array:
```cpp
{ TIMESTAT(MyNew), "Description of my new statistic" },
```

3. **Update StatisticsMapping** for relevant activities:
```cpp
const StatisticsMapping myActivityStatistics({StMyNewStatistic}, basicActivityStatistics);
```

#### Common Usage Examples

**Checking if zero values should be reported:**
```cpp
if (value || includeStatisticIfZero(kind))
{
    // Report the statistic
}
```

**Converting between measures:**
```cpp
unsigned __int64 nanoseconds = convertMeasure(CpuCycles, TimeNs, cycles);
```

**Getting statistic metadata:**
```cpp
const char* name = queryStatisticName(kind);
const char* description = queryStatisticDescription(kind);
StatisticMeasure measure = queryMeasure(kind);
StatsMergeAction mergeMode = queryMergeMode(kind);
```

**Creating statistic gatherers:**
```cpp
Owned<IStatisticGatherer> gatherer = createStatisticsGatherer(SCTthor, "thor", rootScope);
```

#### Performance Considerations

**High-frequency statistics:** Use atomic operations sparingly
```cpp
// Preferred for high-frequency updates
stats.addStatistic(StNumRowsProcessed, batchSize);

// Avoid for high-frequency (causes memory synchronization)
stats.addStatisticAtomic(StNumRowsProcessed, 1);  
```

**Memory overhead:** Each CRuntimeStatisticCollection allocates space for all mapped statistics
```cpp
// Minimize mappings for memory-constrained activities
const StatisticsMapping lightweightStats({StTimeElapsed, StNumRowsProcessed});
```

**Cycle vs Time statistics:** Use cycles for high-precision, low-overhead measurements
```cpp
cycle_t startCycles = get_cycles_now();
// ... work ...
cycle_t elapsed = get_cycles_now() - startCycles;
stats.addStatistic(StCycleLocalExecuteCycles, elapsed);
```
