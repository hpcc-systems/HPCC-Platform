/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */


#ifndef JSTATCODES_H
#define JSTATCODES_H

#define ActivityScopePrefix "a"
#define EdgeScopePrefix "e"
#define SubGraphScopePrefix "sg"
#define GraphScopePrefix "graph"
#define FunctionScopePrefix "f"

#define CONST_STRLEN(x) (sizeof(x)-1)       // sizeof(const-string) = strlen(const-string) + 1 byte for the \0 terminator
#define MATCHES_CONST_PREFIX(search, prefix) (strncmp(search, prefix, CONST_STRLEN(prefix)) == 0)

enum CombineStatsAction
{
    MergeStats,
    ReplaceStats,
    AppendStats,
};

enum StatisticCreatorType
{
    SCTnone,
    SCTall,
    SCTunknown,
    SCThthor,
    SCTroxie,
    SCTroxieSlave,
    SCTthor,
    SCTthorMaster,
    SCTthorSlave,
    SCTeclcc,
    SCTesp,
    SCTsummary,                         // used to maintain the summary time over all thors (mainly for sorting)
    SCTmax,
};

enum StatisticScopeType
{
    SSTnone,
    SSTall,
    SSTglobal,                          // root scope
    SSTgraph,                           // identifies a graph
    SSTsubgraph,
    SSTactivity,
    SSTallocator,                       // identifies an allocator
    SSTsection,                         // A section within the query - not a great differentiator
    SSTcompilestage,                    // a stage within the compilation process
    SSTdfuworkunit,                     // a reference to an executing dfu workunit
    SSTedge,
    SSTfunction,                        // a function call
    SSTmax
};

enum StatisticMeasure
{
    SMeasureNone,
    SMeasureAll,
    SMeasureTimeNs,                     // Elapsed time in nanoseconds
    SMeasureTimestampUs,                // timestamp/when - a point in time (to the microsecond)
    SMeasureCount,                      // a count of the number of occurrences
    SMeasureSize,                       // a quantity of memory (or disk) measured in bytes
    SMeasureLoad,                       // measure of cpu activity (stored as 1/1000000 core)
    SMeasureSkew,                       // a measure of skew. 0 = perfectly balanced, range [-10000..infinity]
    SMeasureNode,                       // A node number within a cluster (0 = master)
    SMeasurePercent,                    // actually stored as parts per million, displayed as a percentage
    SMeasureIPV4,
    SMeasureCycle,
    SMeasureMax,
};

//This macro can be used to generate multiple variations of a statistics kind, but probably not needed any more
//e.g.,     DEFINE_SKEW_STAT(Time, Elapsed)

#define DEFINE_SKEW_STAT(x, y) \
    St ## x ## Min ## y = (St ## x ## y | StMinX), \
    St ## x ## Max ## y = (St ## x ## y | StMaxX), \
    St ## x ## Ave ## y = (St ## x ## y | StAvgX), \
    St ## Skew ## y = (St ## x ## y | StSkew), \
    St ## SkewMin ## y = (St ## x ## y | StSkewMin), \
    St ## SkewMax ## y = (St ## x ## y | StSkewMax), \
    St ## NodeMin ## y = (St ## x ## y | StNodeMin), \
    St ## NodeMax ## y = (St ## x ## y | StNodeMax),

//The values in this enumeration are stored persistently.  The associated values must not be changed.
//If you add an entry here you must also update statsMetaData
//NOTE: All statistic names should be unique with the type prefix removed. Since the prefix is replaced with Skew/Min/etc.
enum StatisticKind
{
    StKindNone,
    StKindAll,
    StWhenGraphStarted,                 // When a graph starts
    StWhenGraphFinished,                // When a graph stopped
    StWhenFirstRow,                     // When the first row is processed by slave activity
    StWhenQueryStarted,
    StWhenQueryFinished,
    StWhenCreated,
    StWhenCompiled,
    StWhenWorkunitModified,             // Not sure this is very useful
    StTimeElapsed,                      // Elapsed wall time between first row and last row
    StTimeLocalExecute,                 // Time spend processing just this activity
    StTimeTotalExecute,                 // Time executing this activity and all inputs
    StTimeRemaining,
    StSizeGeneratedCpp,
    StSizePeakMemory,
    StSizeMaxRowSize,
    StNumRowsProcessed,                 // on edge
    StNumSlaves,                        // on edge
    StNumStarted,                       // on edge
    StNumStopped,                       // on edge
    StNumIndexSeeks,
    StNumIndexScans,
    StNumIndexWildSeeks,
    StNumIndexSkips,
    StNumIndexNullSkips,
    StNumIndexMerges,
    StNumIndexMergeCompares,
    StNumPreFiltered,
    StNumPostFiltered,
    StNumBlobCacheHits,
    StNumLeafCacheHits,
    StNumNodeCacheHits,
    StNumBlobCacheAdds,
    StNumLeafCacheAdds,
    StNumNodeCacheAdds,
    StNumPreloadCacheHits,
    StNumPreloadCacheAdds,
    StNumServerCacheHits,
    StNumIndexAccepted,
    StNumIndexRejected,
    StNumAtmostTriggered,
    StNumDiskSeeks,
    StNumIterations,
    StLoadWhileSorting,                 // Average load while processing a sort?
    StNumLeftRows,
    StNumRightRows,
    StPerReplicated,
    StNumDiskRowsRead,
    StNumIndexRowsRead,
    StNumDiskAccepted,
    StNumDiskRejected,
    StTimeSoapcall,                     // Time spent waiting for soapcalls
    StTimeFirstExecute,                 // Time waiting for first record from this activity
    StTimeDiskReadIO,
    StTimeDiskWriteIO,
    StSizeDiskRead,
    StSizeDiskWrite,
    StCycleDiskReadIOCycles,
    StCycleDiskWriteIOCycles,
    StNumDiskReads,
    StNumDiskWrites,
    StNumSpills,
    StTimeSpillElapsed,
    StTimeSortElapsed,
    StNumGroups,
    StNumGroupMax,
    StSizeSpillFile,
    StCycleSpillElapsedCycles,
    StCycleSortElapsedCycles,
    StNumStrands,                       // Stranding stats - on edge
    StCycleTotalExecuteCycles,
    StNumExecutions,
    StTimeTotalNested,
    StCycleLocalExecuteCycles,
    StNumCompares,
    StNumScansPerRow,
    StNumAllocations,
    StNumAllocationScans,
    StNumDiskRetries,
    StCycleElapsedCycles,
    StCycleRemainingCycles,
    StCycleSoapcallCycles,
    StCycleFirstExecuteCycles,
    StCycleTotalNestedCycles,

    StMax,

    //For any quantity there is potentially the following variants.
    //These modifiers ORd with the values above to form a compound type.
    StKindMask                          = 0x0ffff,
    StVariantScale                      = (StKindMask+1),
    StMinX                              = 0x10000,  // the minimum value
    StMaxX                              = 0x20000,  // the maximum value
    StAvgX                              = 0x30000,  // the average value
    StSkew                              = 0x40000,  // the skew on a particular node
    StSkewMin                           = 0x50000,  // the minimum skew
    StSkewMax                           = 0x60000,  // the maximum skew
    StNodeMin                           = 0x70000,  // the node containing the minimum
    StNodeMax                           = 0x80000,  // the node containing the maximum
    StDeltaX                            = 0x90000,  // a difference in the value of X
    StStdDevX                           = 0xa0000,  // standard deviation in the value of X
    StNextModifier                      = 0xb0000,

};

#endif
