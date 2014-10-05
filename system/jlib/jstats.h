/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


#ifndef JSTATS_H
#define JSTATS_H

#include "jlib.hpp"

#define ActivityScopePrefix "a"
#define EdgeScopePrefix "e"
#define SubGraphScopePrefix "sg"
#define GraphScopePrefix "graph"
#define CONST_STRLEN(x) (sizeof(x)-1)       // sizeof(const-string) = strlen(const-string) + 1 byte for the \0 terminator
#define MATCHES_CONST_PREFIX(search, prefix) (strncmp(search, prefix, CONST_STRLEN(prefix)) == 0)

enum OldStatsKind
{
    STATS_INDEX_SEEKS,
    STATS_INDEX_SCANS,
    STATS_INDEX_WILDSEEKS,
    STATS_INDEX_SKIPS,
    STATS_INDEX_NULLSKIPS,
    STATS_INDEX_MERGES,

    STATS_BLOBCACHEHIT,
    STATS_LEAFCACHEHIT,
    STATS_NODECACHEHIT,
    STATS_BLOBCACHEADD,
    STATS_LEAFCACHEADD,
    STATS_NODECACHEADD,

    STATS_INDEX_MERGECOMPARES,

    STATS_PRELOADCACHEHIT,
    STATS_PRELOADCACHEADD,

    STATS_SERVERCACHEHIT,

    STATS_ACCEPTED,
    STATS_REJECTED,
    STATS_ATMOST,

    STATS_DISK_SEEKS,
    STATS_SOAPCALL_LATENCY,

    STATS_SIZE
};

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
//If you add an entry here you must also update queryMeasure(), queryStatisticName() and queryTreeTag()
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
    StSizeMaxRowSize,                   // Is measurement in K appropriate?

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
    StNextModifier                      = 0xa0000,

};

//---------------------------------------------------------------------------------------------------------------------

interface IStatistic : extends IInterface
{
public:
    virtual StatisticKind queryKind() const = 0;
    virtual unsigned __int64 queryValue() const = 0;
};

interface IStatisticFilter
{
    virtual bool matches(IStatistic * stats) = 0;
    virtual void getFilter(StringBuffer & out) = 0;
};

interface IStatisticIterator : public IIteratorOf<IStatistic>
{
};

//Represents a single level of a scope
class jlib_decl StatsScopeId
{
public:
    StatsScopeId() : id(0), extra(0), scopeType(SSTnone) {}
    StatsScopeId(StatisticScopeType _scopeType, unsigned _id, unsigned _extra = 0)
        : id(_id), extra(_extra), scopeType(_scopeType)
    {
    }

    StatisticScopeType queryScopeType() const { return scopeType; }
    StringBuffer & getScopeText(StringBuffer & out) const;

    unsigned getHash() const;
    bool matches(const StatsScopeId & other) const;
    unsigned queryActivity() const;

    void deserialize(MemoryBuffer & in, unsigned version);
    void serialize(MemoryBuffer & out) const;

    bool setScopeText(const char * text);
    void setId(StatisticScopeType _scopeType, unsigned _id, unsigned _extra = 0);
    void setActivityId(unsigned _id);
    void setEdgeId(unsigned _id, unsigned _output);
    void setSubgraphId(unsigned _id);

    bool operator == (const StatsScopeId & other) const { return matches(other); }

protected:
    //If any more items are added then this could become a union...
    unsigned id;
    unsigned extra;
    StatisticScopeType scopeType;
};

interface IStatisticCollectionIterator;
interface IStatisticCollection : public IInterface
{
public:
    virtual StatisticScopeType queryScopeType() const = 0;
    virtual StringBuffer & getFullScope(StringBuffer & str) const = 0;
    virtual StringBuffer & getScope(StringBuffer & str) const = 0;
    virtual unsigned __int64 queryStatistic(StatisticKind kind) const = 0;
    virtual unsigned getNumStatistics() const = 0;
    virtual void getStatistic(StatisticKind & kind, unsigned __int64 & value, unsigned idx) const = 0;
    virtual IStatisticCollectionIterator & getScopes(const char * filter) = 0;
    virtual void getMinMaxScope(IStringVal & minValue, IStringVal & maxValue, StatisticScopeType searchScopeType) const = 0;
    virtual void getMinMaxActivity(unsigned & minValue, unsigned & maxValue) const = 0;
    virtual void serialize(MemoryBuffer & out) const = 0;
    virtual unsigned __int64 queryWhenCreated() const = 0;
};

interface IStatisticCollectionIterator : public IIteratorOf<IStatisticCollection>
{
};

enum StatsMergeAction
{
    StatsMergeKeep,
    StatsMergeReplace,
    StatsMergeSum,
    StatsMergeMin,
    StatsMergeMax,
    StatsMergeAppend,
};

interface IStatisticGatherer : public IInterface
{
public:
    virtual void beginScope(const StatsScopeId & id) = 0;
    virtual void beginSubGraphScope(unsigned id) = 0;
    virtual void beginActivityScope(unsigned id) = 0;
    virtual void beginEdgeScope(unsigned id, unsigned oid) = 0;
    virtual void endScope() = 0;
    virtual void addStatistic(StatisticKind kind, unsigned __int64 value) = 0;
    virtual void updateStatistic(StatisticKind kind, unsigned __int64 value, StatsMergeAction mergeAction) = 0;
    virtual IStatisticCollection * getResult() = 0;
};

//All filtering should go through this interface - so we can extend and allow AND/OR filters at a later date.
interface IStatisticsFilter : public IInterface
{
public:
    virtual bool matches(StatisticCreatorType curCreatorType, const char * curCreator, StatisticScopeType curScopeType, const char * curScope, StatisticMeasure curMeasure, StatisticKind curKind) const = 0;
    //These are a bit arbitrary...
    virtual bool queryMergeSources() const = 0;
    virtual const char * queryScope() const = 0;

};

class StatsScopeBlock
{
public:
    inline StatsScopeBlock(IStatisticGatherer & _gatherer) : gatherer(_gatherer)
    {
    }
    inline ~StatsScopeBlock()
    {
        gatherer.endScope();
    }

protected:
    IStatisticGatherer & gatherer;
};

//---------------------------------------------------------------------------------------------------------------------

class StatsSubgraphScope : public StatsScopeBlock
{
public:
    inline StatsSubgraphScope(IStatisticGatherer & _gatherer, unsigned id) : StatsScopeBlock(_gatherer)
    {
        gatherer.beginSubGraphScope(id);
    }
};

class StatsActivityScope : public StatsScopeBlock
{
public:
    inline StatsActivityScope(IStatisticGatherer & _gatherer, unsigned id) : StatsScopeBlock(_gatherer)
    {
        gatherer.beginActivityScope(id);
    }
};

class StatsEdgeScope : public StatsScopeBlock
{
public:
    inline StatsEdgeScope(IStatisticGatherer & _gatherer, unsigned id, unsigned oid) : StatsScopeBlock(_gatherer)
    {
        gatherer.beginEdgeScope(id, oid);
    }
};

//---------------------------------------------------------------------------------------------------------------------

class ScopedItemFilter
{
public:
    ScopedItemFilter() : minDepth(0), maxDepth(0), hasWildcard(false) {}

    bool match(const char * search) const;
    bool matchDepth(unsigned low, unsigned high) const;

    const char * queryValue() const { return value ? value.get() : "*"; }

    void set(const char * value);
    void setDepth(unsigned _minDepth);
    void setDepth(unsigned _minDepth, unsigned _maxDepth);

protected:
    unsigned minDepth;
    unsigned maxDepth;
    StringAttr value;
    bool hasWildcard;
};

class jlib_decl StatisticsFilter : public CInterfaceOf<IStatisticsFilter>
{
public:
    StatisticsFilter();
    StatisticsFilter(const char * filter);
    StatisticsFilter(StatisticCreatorType _creatorType, StatisticScopeType _scopeType, StatisticMeasure _measure, StatisticKind _kind);
    StatisticsFilter(const char * _creatorType, const char * _scopeType, const char * _kind);
    StatisticsFilter(const char * _creatorTypeText, const char * _creator, const char * _scopeTypeText, const char * _scope, const char * _measureText, const char * _kindText);
    StatisticsFilter(StatisticCreatorType _creatorType, const char * _creator, StatisticScopeType _scopeType, const char * _scope, StatisticMeasure _measure, StatisticKind _kind);

    virtual bool matches(StatisticCreatorType curCreatorType, const char * curCreator, StatisticScopeType curScopeType, const char * curScope, StatisticMeasure curMeasure, StatisticKind curKind) const;
    virtual bool queryMergeSources() const { return mergeSources && scopeFilter.matchDepth(2,0); }
    virtual const char * queryScope() const { return scopeFilter.queryValue(); }

    void set(const char * _creatorTypeText, const char * _scopeTypeText, const char * _kindText);
    void set(const char * _creatorTypeText, const char * _creator, const char * _scopeTypeText, const char * _scope, const char * _measureText, const char * _kindText);

    void setCreatorDepth(unsigned _minCreatorDepth, unsigned _maxCreatorDepth);
    void setCreator(const char * _creator);
    void setCreatorType(StatisticCreatorType _creatorType);
    void setFilter(const char * filter);
    void setScopeDepth(unsigned _minScopeDepth);
    void setScopeDepth(unsigned _minScopeDepth, unsigned _maxScopeDepth);
    void setScope(const char * _scope);
    void setScopeType(StatisticScopeType _scopeType);
    void setKind(StatisticKind _kind);
    void setKind(const char * _kind);
    void setMeasure(StatisticMeasure _measure);
    void setMergeSources(bool _value);      // set to false for legacy timing semantics

protected:
    void init();

protected:
    StatisticCreatorType creatorType;
    StatisticScopeType scopeType;
    StatisticMeasure measure;
    StatisticKind kind;
    ScopedItemFilter creatorFilter;
    ScopedItemFilter scopeFilter;
    bool mergeSources;
};

//---------------------------------------------------------------------------------------------------------------------

class StatisticsMapping
{
public:
    //Takes a list of StatisticKind terminated by StKindNone
    StatisticsMapping(StatisticKind kind, ...);

    inline unsigned getIndex(StatisticKind kind) const
    {
        dbgassertex(kind >= StKindNone && kind < StMax);
        return kindToIndex.item(kind);
    }
    inline StatisticKind getKind(unsigned index) const { return (StatisticKind)indexToKind.item(index); }
    inline unsigned numStatistics() const { return indexToKind.ordinality(); }

protected:
    void createMappings();

protected:
    UnsignedArray kindToIndex;
    UnsignedArray indexToKind;
};

//---------------------------------------------------------------------------------------------------------------------

//MORE: We probably want to have functions that peform the atomic equivalents
class CRuntimeStatistic
{
public:
    inline void add(unsigned __int64 delta) { value += delta; }
    inline void addAtomic(unsigned __int64 delta) { value += delta; }
    inline unsigned __int64 get() const { return value; }
    inline unsigned __int64 getClear()
    {
        unsigned __int64 ret = value;
        value -= ret;
        return ret;
    }
    inline unsigned __int64 getClearAtomic()
    {
        unsigned __int64 ret = value;
        value -= ret; // should be atomic dec...
        return ret;
    }
    inline void clear() { set(0); }
    inline void set(unsigned __int64 delta) { value = delta; }

protected:
    unsigned __int64 value;
};

//This class is used to gather statistics for an activity - it has no notion of scope.
interface IContextLogger;
class CRuntimeStatisticCollection
{
public:
    CRuntimeStatisticCollection(const StatisticsMapping & _mapping) : mapping(_mapping)
    {
        unsigned num = mapping.numStatistics();
        values = new CRuntimeStatistic[num+1]; // extra entry is to gather unexpected stats
    }
    ~CRuntimeStatisticCollection()
    {
        delete [] values;
    }

    inline CRuntimeStatistic & queryStatistic(StatisticKind kind)
    {
        unsigned index = queryMapping().getIndex(kind);
        return values[index];
    }
    inline const CRuntimeStatistic & queryStatistic(StatisticKind kind) const
    {
        unsigned index = queryMapping().getIndex(kind);
        return values[index];
    }

    void addStatistic(StatisticKind kind, unsigned __int64 value)
    {
        queryStatistic(kind).add(value);
    }
    void setStatistic(StatisticKind kind, unsigned __int64 value)
    {
        queryStatistic(kind).set(value);
    }
    unsigned __int64 getStatisticValue(StatisticKind kind) const
    {
        return queryStatistic(kind).get();
    }
    void reset()
    {
        unsigned num = mapping.numStatistics();
        for (unsigned i = 0; i <= num; i++)
            values[i].clear();
        memset(values, 0, sizeof(unsigned __int64) * num);
    }

    inline const StatisticsMapping & queryMapping() const { return mapping; };
    inline unsigned ordinality() const { return mapping.numStatistics(); }
    inline StatisticKind getKind(unsigned i) const { return mapping.getKind(i); }

    void rollupStatistics(IContextLogger * target) { rollupStatistics(1, &target); }
    void rollupStatistics(unsigned num, IContextLogger * const * targets) const;

    void recordStatistics(IStatisticGatherer & target, StatsMergeAction mergeAction) const;

protected:
    void reportIgnoredStats() const;

private:
    const StatisticsMapping & mapping;
    CRuntimeStatistic * values;
};


//---------------------------------------------------------------------------------------------------------------------

//A class for minimizing the overhead of collecting timestamps.
class jlib_decl OptimizedTimestamp
{
public:
    OptimizedTimestamp();

    unsigned __int64 getTimeStampNowValue();

protected:
    cycle_t lastCycles;
    unsigned __int64 lastTimestamp;
};

class IpAddress;

extern jlib_decl unsigned __int64 getTimeStampNowValue();
extern jlib_decl unsigned __int64 getIPV4StatsValue(const IpAddress & ip);
extern jlib_decl void formatStatistic(StringBuffer & out, unsigned __int64 value, StatisticMeasure measure);
extern jlib_decl void formatStatistic(StringBuffer & out, unsigned __int64 value, StatisticKind kind);
extern jlib_decl unsigned __int64 mergeStatistic(StatisticMeasure measure, unsigned __int64 value, unsigned __int64 otherValue);
extern jlib_decl unsigned __int64 mergeStatisticValue(unsigned __int64 prevValue, unsigned __int64 newValue, StatsMergeAction mergeAction);

extern jlib_decl StatisticMeasure queryMeasure(StatisticKind kind);
extern jlib_decl const char * queryStatisticName(StatisticKind kind);
extern jlib_decl void queryLongStatisticName(StringBuffer & out, StatisticKind kind);
extern jlib_decl const char * queryTreeTag(StatisticKind kind);
extern jlib_decl const char * queryCreatorTypeName(StatisticCreatorType sct);
extern jlib_decl const char * queryScopeTypeName(StatisticScopeType sst);
extern jlib_decl const char * queryMeasureName(StatisticMeasure measure);
extern jlib_decl StatsMergeAction queryMergeMode(StatisticMeasure measure);

extern jlib_decl StatisticMeasure queryMeasure(const char *  measure);
extern jlib_decl StatisticKind queryStatisticKind(const char *  kind);
extern jlib_decl StatisticCreatorType queryCreatorType(const char * sct);
extern jlib_decl StatisticScopeType queryScopeType(const char * sst);

extern jlib_decl IStatisticGatherer * createStatisticsGatherer(StatisticCreatorType creatorType, const char * creator, const StatsScopeId & rootScope);
extern jlib_decl void serializeStatisticCollection(MemoryBuffer & out, IStatisticCollection * collection);
extern jlib_decl IStatisticCollection * createStatisticCollection(MemoryBuffer & in);


extern jlib_decl StatisticKind mapRoxieStatKind(unsigned i); // legacy
extern jlib_decl StatisticMeasure getStatMeasure(unsigned i);
inline unsigned __int64 milliToNano(unsigned __int64 value) { return value * 1000000; } // call avoids need to upcast values
inline unsigned __int64 nanoToMilli(unsigned __int64 value) { return value / 1000000; }

extern jlib_decl StatisticCreatorType queryStatisticsComponentType();
extern jlib_decl const char * queryStatisticsComponentName();
extern jlib_decl void setStatisticsComponentName(StatisticCreatorType processType, const char * processName, bool appendIP);

extern jlib_decl void verifyStatisticFunctions();

//This interface is primarily here to reduce the dependency between the different components.
interface IStatisticTarget
{
    virtual void addStatistic(StatisticScopeType scopeType, const char * scope, StatisticKind kind, char * description, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction) = 0;
};

#endif
