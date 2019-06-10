/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include "jmutex.hpp"
#include <vector>
#include <initializer_list>

#include "jstatcodes.h"

typedef unsigned __int64 stat_type;
const unsigned __int64 MaxStatisticValue = (unsigned __int64)0-1U;
const unsigned __int64 AnyStatisticValue = MaxStatisticValue; // Use the maximum value to also represent unknown, since it is unlikely to ever occur.

inline constexpr stat_type seconds2StatUnits(stat_type secs) { return secs * 1000000000; }
inline constexpr stat_type msecs2StatUnits(stat_type ms) { return ms * 1000000; }
inline constexpr stat_type statUnits2seconds(stat_type stat) {return stat / 1000000000; }
inline constexpr stat_type statUnits2msecs(stat_type stat) {return stat / 1000000; }

inline constexpr stat_type statSkewPercent(int value) { return (stat_type)value * 100; }            // Since 1 = 0.01% skew
inline constexpr stat_type statSkewPercent(long double value) { return (stat_type)(value * 100); }
inline constexpr stat_type statSkewPercent(stat_type  value) { return (stat_type)(value * 100); }

inline StatisticKind queryStatsVariant(StatisticKind kind) { return (StatisticKind)(kind & ~StKindMask); }

//---------------------------------------------------------------------------------------------------------------------

//Represents a single level of a scope
class jlib_decl StatsScopeId
{
public:
    StatsScopeId() {}
    StatsScopeId(StatisticScopeType _scopeType, unsigned _id, unsigned _extra = 0)
        : id(_id), extra(_extra), scopeType(_scopeType)
    {
    }
    StatsScopeId(StatisticScopeType _scopeType, const char * _name)
        : name(_name), scopeType(_scopeType)
    {
    }
    StatsScopeId(const char * _scope)
    {
        setScopeText(_scope);
    }

    StatisticScopeType queryScopeType() const { return scopeType; }
    StringBuffer & getScopeText(StringBuffer & out) const;
    bool isWildcard() const;

    unsigned getHash() const;
    bool matches(const StatsScopeId & other) const;
    unsigned queryActivity() const;
    void describe(StringBuffer & description) const;

    void deserialize(MemoryBuffer & in, unsigned version);
    void serialize(MemoryBuffer & out) const;

    bool extractScopeText(const char * text, const char * * next);
    bool setScopeText(const char * text, const char * * next = nullptr);
    void setId(StatisticScopeType _scopeType, unsigned _id, unsigned _extra = 0);
    void setActivityId(unsigned _id);
    void setEdgeId(unsigned _id, unsigned _output);
    void setFunctionId(const char * _name);
    void setSubgraphId(unsigned _id);
    void setWorkflowId(unsigned _id);
    void setChildGraphId(unsigned _id);

    int compare(const StatsScopeId & other) const;

    bool operator == (const StatsScopeId & other) const { return matches(other); }

protected:
    //If any more items are added then this could become a union...
    unsigned id = 0;
    unsigned extra = 0;
    StringAttr name;
    StatisticScopeType scopeType = SSTnone;
};

interface IStatisticCollectionIterator;
interface IStatisticGatherer;
interface IStatisticCollection : public IInterface
{
public:
    virtual StatisticScopeType queryScopeType() const = 0;
    virtual StringBuffer & getFullScope(StringBuffer & str) const = 0;
    virtual StringBuffer & getScope(StringBuffer & str) const = 0;
    virtual unsigned __int64 queryStatistic(StatisticKind kind) const = 0;
    virtual unsigned getNumStatistics() const = 0;
    virtual bool getStatistic(StatisticKind kind, unsigned __int64 & value) const = 0;
    virtual void getStatistic(StatisticKind & kind, unsigned __int64 & value, unsigned idx) const = 0;
    virtual IStatisticCollectionIterator & getScopes(const char * filter, bool sorted) = 0;
    virtual void getMinMaxScope(IStringVal & minValue, IStringVal & maxValue, StatisticScopeType searchScopeType) const = 0;
    virtual void getMinMaxActivity(unsigned & minValue, unsigned & maxValue) const = 0;
    virtual void serialize(MemoryBuffer & out) const = 0;
    virtual unsigned __int64 queryWhenCreated() const = 0;
    virtual void mergeInto(IStatisticGatherer & target) const = 0;
};

interface IStatisticCollectionIterator : public IIteratorOf<IStatisticCollection>
{
};

enum StatsMergeAction
{
    StatsMergeKeepNonZero,
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
    virtual void beginChildGraphScope(unsigned id) = 0;
    virtual void endScope() = 0;
    virtual void addStatistic(StatisticKind kind, unsigned __int64 value) = 0;
    virtual void updateStatistic(StatisticKind kind, unsigned __int64 value, StatsMergeAction mergeAction) = 0;
    virtual IStatisticCollection * getResult() = 0;
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


class jlib_decl StatsAggregation
{
public:
    void noteValue(stat_type value);

    stat_type getCount() const { return count; }
    stat_type getMin() const { return minValue; }
    stat_type getMax() const { return maxValue; }
    stat_type getSum() const { return sumValue; }
    stat_type getAve() const;
    //MORE: StDev would require a sum of squares.

protected:
    stat_type count = 0;
    stat_type sumValue = 0;
    stat_type minValue = 0;
    stat_type maxValue = 0;
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

class StatsChildGraphScope : public StatsScopeBlock
{
public:
    inline StatsChildGraphScope(IStatisticGatherer & _gatherer, unsigned id) : StatsScopeBlock(_gatherer)
    {
        gatherer.beginChildGraphScope(id);
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

class StatsScope : public StatsScopeBlock
{
public:
    inline StatsScope(IStatisticGatherer & _gatherer, const StatsScopeId & id) : StatsScopeBlock(_gatherer)
    {
        gatherer.beginScope(id);
    }
};

class StatsOptScope
{
public:
    inline StatsOptScope(IStatisticGatherer & _gatherer, const StatsScopeId & _id) : gatherer(_gatherer), id(_id)
    {
        if (id.queryScopeType() != SSTnone)
            gatherer.beginScope(id);
    }
    inline ~StatsOptScope()
    {
        if (id.queryScopeType() != SSTnone)
            gatherer.endScope();
    }

protected:
    IStatisticGatherer & gatherer;
    const StatsScopeId & id;
};

//---------------------------------------------------------------------------------------------------------------------

class ScopedItemFilter
{
public:
    ScopedItemFilter() : minDepth(0), maxDepth(0), hasWildcard(false) {}

    bool match(const char * search) const;
    bool matchDepth(unsigned low, unsigned high) const;
    bool recurseChildScopes(const char * curScope) const;

    const char * queryValue() const { return value ? value.get() : "*"; }

    void set(const char * value);
    void setDepth(unsigned _depth);
    void setDepth(unsigned _minDepth, unsigned _maxDepth);

protected:
    unsigned minDepth;
    unsigned maxDepth;
    StringAttr value;
    bool hasWildcard;
};

class jlib_decl StatisticValueFilter
{
public:
    StatisticValueFilter(StatisticKind _kind, stat_type _minValue, stat_type _maxValue) :
        kind(_kind), minValue(_minValue), maxValue(_maxValue)
    {
    }

    bool matches(stat_type value) const
    {
        return ((value >= minValue) && (value <= maxValue));
    }

    StatisticKind queryKind() const { return kind; }
    StringBuffer & describe(StringBuffer & out) const;

protected:
    StatisticKind kind;
    stat_type minValue;
    stat_type maxValue;
};

enum ScopeCompare : unsigned
{
    SCunknown   = 0x0000,   //
    SCparent    = 0x0001,   // is a parent of: w1, w1:g1
    SCchild     = 0x0002,   // is a child of: w1:g1, w1
    SCequal     = 0x0004,   // w1:g1, w1:g1 - may extend to wildcards later.
    SCrelated   = 0x0008,   // w1:g1, w1:g2 - some shared relationship
    SCunrelated = 0x0010,   // no connection
};
BITMASK_ENUM(ScopeCompare);


/*
 * compare two scopes, and return a value indicating their relationship
 */

extern jlib_decl ScopeCompare compareScopes(const char * scope, const char * key);

class jlib_decl ScopeFilter
{
public:
    ScopeFilter() = default;
    ScopeFilter(const char * scopeList);

    void addScope(const char * scope);
    void addScopes(const char * scope);
    void addScopeType(StatisticScopeType scopeType);
    void addId(const char * id);
    void setDepth(unsigned low, unsigned high);
    void setDepth(unsigned value) { setDepth(value, value); }

    /*
     * Return a mask containing information about whether the scope will match the filter
     * It errs on the side of false positives - e.g. SCparent is set if it might be the parent of a match
     */
    ScopeCompare compare(const char * scope) const;

    int compareDepth(unsigned depth) const; // -1 too shallow, 0 a match, +1 too deep
    bool hasSingleMatch() const;
    bool canAlwaysPreFilter() const;
    void finishedFilter();

    const StringArray & queryScopes() const { return scopes; }
    bool matchOnly(StatisticScopeType scopeType) const;
    StringBuffer & describe(StringBuffer & out) const;

protected:
    void intersectDepth(unsigned _minDepth, unsigned _maxDepth);

protected:
    UnsignedArray scopeTypes;
    StringArray scopes;
    StringArray ids;
    unsigned minDepth = 0;
    unsigned maxDepth = UINT_MAX;
};


class jlib_decl StatisticsFilter : public CInterface
{
public:
    StatisticsFilter();
    StatisticsFilter(const char * filter);
    StatisticsFilter(StatisticCreatorType _creatorType, StatisticScopeType _scopeType, StatisticMeasure _measure, StatisticKind _kind);
    StatisticsFilter(const char * _creatorType, const char * _scopeType, const char * _kind);
    StatisticsFilter(const char * _creatorTypeText, const char * _creator, const char * _scopeTypeText, const char * _scope, const char * _measureText, const char * _kindText);
    StatisticsFilter(StatisticCreatorType _creatorType, const char * _creator, StatisticScopeType _scopeType, const char * _scope, StatisticMeasure _measure, StatisticKind _kind);

    bool matches(StatisticCreatorType curCreatorType, const char * curCreator, StatisticScopeType curScopeType, const char * curScope, StatisticMeasure curMeasure, StatisticKind curKind, unsigned __int64 value) const;
    bool recurseChildScopes(StatisticScopeType curScopeType, const char * curScope) const;
    const char * queryScope() const { return scopeFilter.queryValue(); }

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
    void setValueRange(unsigned __int64 minValue, unsigned __int64 _maxValue);
    void setKind(StatisticKind _kind);
    void setKind(const char * _kind);
    void setMeasure(StatisticMeasure _measure);

protected:
    void addFilter(const char * filter);
    void init();

protected:
    StatisticCreatorType creatorType;
    StatisticScopeType scopeType;
    StatisticMeasure measure;
    StatisticKind kind;
    ScopedItemFilter creatorFilter;
    ScopedItemFilter scopeFilter;
    unsigned __int64 minValue = 0;
    unsigned __int64 maxValue = (unsigned __int64)(-1);
};

//---------------------------------------------------------------------------------------------------------------------

class jlib_decl StatisticsMapping
{
public:
    //Takes a list of StatisticKind
    StatisticsMapping(const std::initializer_list<StatisticKind> &kinds);
    //Takes an existing Mapping, and extends it with a list of StatisticKind
    StatisticsMapping(const StatisticsMapping * from, const std::initializer_list<StatisticKind> &kinds);
    //Accepts all StatisticKind values
    StatisticsMapping();

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

extern const jlib_decl StatisticsMapping allStatistics;
extern const jlib_decl StatisticsMapping heapStatistics;
extern const jlib_decl StatisticsMapping diskLocalStatistics;
extern const jlib_decl StatisticsMapping diskRemoteStatistics;
extern const jlib_decl StatisticsMapping diskReadRemoteStatistics;
extern const jlib_decl StatisticsMapping diskWriteRemoteStatistics;

//---------------------------------------------------------------------------------------------------------------------

class jlib_decl CRuntimeStatistic
{
public:
    CRuntimeStatistic() : value(0) {}
    inline void add(unsigned __int64 delta)
    {
        //load and store default to relaxed - so this has no atomic synchronization
        value.store(value.load() + delta);
    }
    inline void addAtomic(unsigned __int64 delta)
    {
        value.fetch_add(delta);
    }
    inline unsigned __int64 get() const { return value; }
    inline unsigned __int64 getClear()
    {
        unsigned __int64 ret = value;
        value.store(0);
        return ret;
    }
    inline unsigned __int64 getClearAtomic()
    {
        unsigned __int64 ret = value;
        value.fetch_sub(ret);
        return ret;
    }
    inline void clear() { set(0); }
    void merge(unsigned __int64 otherValue, StatsMergeAction mergeAction);
    inline void set(unsigned __int64 _value) { value = _value; }

protected:
    RelaxedAtomic<unsigned __int64> value;
};

//This class is used to gather statistics for an activity - it has no notion of scope.
interface IContextLogger;
class CNestedRuntimeStatisticMap;

class jlib_decl CRuntimeStatisticCollection
{
public:
    CRuntimeStatisticCollection(const StatisticsMapping & _mapping) : mapping(_mapping)
    {
        unsigned num = mapping.numStatistics();
        values = new CRuntimeStatistic[num+1]; // extra entry is to gather unexpected stats
    }
    CRuntimeStatisticCollection(const CRuntimeStatisticCollection & _other) : mapping(_other.mapping)
    {
        unsigned num = mapping.numStatistics();
        values = new CRuntimeStatistic[num+1];
        for (unsigned i=0; i <= num; i++)
            values[i].set(_other.values[i].get());
    }
    virtual ~CRuntimeStatisticCollection();

    inline CRuntimeStatistic & queryStatistic(StatisticKind kind)
    {
        unsigned index = queryMapping().getIndex(kind);
        dbgassertex(index < mapping.numStatistics());
        return values[index];
    }
    inline const CRuntimeStatistic & queryStatistic(StatisticKind kind) const
    {
        unsigned index = queryMapping().getIndex(kind);
        dbgassertex(index < mapping.numStatistics());
        return values[index];
    }

    void addStatistic(StatisticKind kind, unsigned __int64 value)
    {
        queryStatistic(kind).add(value);
    }
    void addStatisticAtomic(StatisticKind kind, unsigned __int64 value)
    {
        queryStatistic(kind).addAtomic(value);
    }
    virtual void mergeStatistic(StatisticKind kind, unsigned __int64 value);
    void setStatistic(StatisticKind kind, unsigned __int64 value)
    {
        queryStatistic(kind).set(value);
    }
    unsigned __int64 getStatisticValue(StatisticKind kind) const
    {
        return queryStatistic(kind).get();
    }
    unsigned __int64 getSerialStatisticValue(StatisticKind kind) const;
    void reset();
    void reset(const StatisticsMapping & toClear);

    CRuntimeStatisticCollection & registerNested(const StatsScopeId & scope, const StatisticsMapping & mapping);

    inline const StatisticsMapping & queryMapping() const { return mapping; };
    inline unsigned ordinality() const { return mapping.numStatistics(); }
    inline StatisticKind getKind(unsigned i) const { return mapping.getKind(i); }
    inline unsigned __int64 getValue(unsigned i) const { return values[i].get(); }

    void merge(const CRuntimeStatisticCollection & other);
    void updateDelta(CRuntimeStatisticCollection & target, const CRuntimeStatisticCollection & source);
    void rollupStatistics(IContextLogger * target) { rollupStatistics(1, &target); }
    void rollupStatistics(unsigned num, IContextLogger * const * targets) const;


    virtual void recordStatistics(IStatisticGatherer & target) const;
    void getNodeProgressInfo(IPropertyTree &node) const;

    // Print out collected stats to string
    StringBuffer &toStr(StringBuffer &str) const;
    // Print out collected stats to string as XML
    StringBuffer &toXML(StringBuffer &str) const;
    // Serialize/deserialize
    virtual bool serialize(MemoryBuffer & out) const;  // Returns true if any non-zero
    virtual void deserialize(MemoryBuffer & in);
    virtual void deserializeMerge(MemoryBuffer& in);


protected:
    virtual CNestedRuntimeStatisticMap *createNested() const;
    CNestedRuntimeStatisticMap & ensureNested();
    CNestedRuntimeStatisticMap *queryNested() const;
    void reportIgnoredStats() const;
    void mergeStatistic(StatisticKind kind, unsigned __int64 value, StatsMergeAction mergeAction)
    {
        queryStatistic(kind).merge(value, mergeAction);
    }
    const CRuntimeStatistic & queryUnknownStatistic() const { return values[mapping.numStatistics()]; }

protected:
    const StatisticsMapping & mapping;
    CRuntimeStatistic * values;
    std::atomic<CNestedRuntimeStatisticMap *> nested {nullptr};
    static CriticalSection nestlock;
};

//NB: Serialize and deserialize are not currently implemented.
class jlib_decl CRuntimeSummaryStatisticCollection : public CRuntimeStatisticCollection
{
public:
    CRuntimeSummaryStatisticCollection(const StatisticsMapping & _mapping);
    ~CRuntimeSummaryStatisticCollection();

    virtual void mergeStatistic(StatisticKind kind, unsigned __int64 value) override;
    virtual void recordStatistics(IStatisticGatherer & target) const override;
    virtual bool serialize(MemoryBuffer & out) const override;  // Returns true if any non-zero
    virtual void deserialize(MemoryBuffer & in) override;
    virtual void deserializeMerge(MemoryBuffer& in) override;

protected:
    struct DerivedStats
    {
    public:
        void mergeStatistic(unsigned __int64 value, unsigned node);
    public:
        unsigned __int64 max = 0;
        unsigned __int64 min = 0;
        unsigned __int64 count = 0;
        double sumSquares = 0;
        unsigned minNode = 0;
        unsigned maxNode = 0;
    };

protected:
    virtual CNestedRuntimeStatisticMap *createNested() const override;

protected:
    DerivedStats * derived;
};

class CNestedRuntimeStatisticCollection : public CInterface
{
public:
    CNestedRuntimeStatisticCollection(const StatsScopeId & _scope, CRuntimeStatisticCollection * _stats)
    : scope(_scope), stats(_stats)
    {
    }
    CNestedRuntimeStatisticCollection(const CNestedRuntimeStatisticCollection & _other) = delete;
    ~CNestedRuntimeStatisticCollection() { delete stats; }

    bool matches(const StatsScopeId & otherScope) const;
    inline const StatisticsMapping & queryMapping() const { return stats->queryMapping(); };
    inline CRuntimeStatisticCollection & queryStats() { return *stats; }
    inline const CRuntimeStatisticCollection & queryStats() const { return *stats; }

    bool serialize(MemoryBuffer & out) const;  // Returns true if any non-zero
    void deserialize(MemoryBuffer & in);
    void deserializeMerge(MemoryBuffer& in);
    void merge(const CNestedRuntimeStatisticCollection & other);
    void recordStatistics(IStatisticGatherer & target) const;
    StringBuffer & toStr(StringBuffer &str) const;
    StringBuffer & toXML(StringBuffer &str) const;
    void updateDelta(CNestedRuntimeStatisticCollection & target, const CNestedRuntimeStatisticCollection & source);

public:
    StatsScopeId scope;
    CRuntimeStatisticCollection * stats;
};

class CNestedRuntimeStatisticMap
{
public:
    virtual ~CNestedRuntimeStatisticMap() = default;

    CNestedRuntimeStatisticCollection & addNested(const StatsScopeId & scope, const StatisticsMapping & mapping);

    bool serialize(MemoryBuffer & out) const;  // Returns true if any non-zero
    void deserialize(MemoryBuffer & in);
    void deserializeMerge(MemoryBuffer& in);
    void merge(const CNestedRuntimeStatisticMap & other);
    void recordStatistics(IStatisticGatherer & target) const;
    StringBuffer & toStr(StringBuffer &str) const;
    StringBuffer & toXML(StringBuffer &str) const;
    void updateDelta(CNestedRuntimeStatisticMap & target, const CNestedRuntimeStatisticMap & source);


protected:
    virtual CRuntimeStatisticCollection * createStats(const StatisticsMapping & mapping);

protected:
    CIArrayOf<CNestedRuntimeStatisticCollection> map;
    mutable ReadWriteLock lock;
};

class CNestedSummaryRuntimeStatisticMap : public CNestedRuntimeStatisticMap
{
protected:
    virtual CRuntimeStatisticCollection * createStats(const StatisticsMapping & mapping) override;
};

//---------------------------------------------------------------------------------------------------------------------

//Some template helper classes for merging statistics from external sources.

template <class INTERFACE>
void mergeStats(CRuntimeStatisticCollection & stats, INTERFACE * source)
{
    if (!source)
        return;

    ForEachItemIn(iStat, stats)
    {
        StatisticKind kind = stats.getKind(iStat);
        stats.mergeStatistic(kind, source->getStatistic(kind));
    }
}

template <class INTERFACE>
void mergeStats(CRuntimeStatisticCollection & stats, Shared<INTERFACE> source) { mergeStats(stats, source.get()); }

template <class INTERFACE>
void mergeStat(CRuntimeStatisticCollection & stats, INTERFACE * source, StatisticKind kind)
{
    if (source)
        stats.mergeStatistic(kind, source->getStatistic(kind));
}

template <class INTERFACE>
void mergeStat(CRuntimeStatisticCollection & stats, Shared<INTERFACE> source, StatisticKind kind) { mergeStat(stats, source.get(), kind); }

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
extern jlib_decl StringBuffer & formatStatistic(StringBuffer & out, unsigned __int64 value, StatisticMeasure measure);
extern jlib_decl StringBuffer & formatStatistic(StringBuffer & out, unsigned __int64 value, StatisticKind kind);
extern jlib_decl void formatTimeStampAsLocalTime(StringBuffer & out, unsigned __int64 value);
extern jlib_decl stat_type readStatisticValue(const char * cur, const char * * end, StatisticMeasure measure);

extern jlib_decl unsigned __int64 mergeStatistic(StatisticMeasure measure, unsigned __int64 value, unsigned __int64 otherValue);
extern jlib_decl unsigned __int64 mergeStatisticValue(unsigned __int64 prevValue, unsigned __int64 newValue, StatsMergeAction mergeAction);

extern jlib_decl StatisticMeasure queryMeasure(StatisticKind kind);
extern jlib_decl const char * queryStatisticName(StatisticKind kind);
extern jlib_decl void queryLongStatisticName(StringBuffer & out, StatisticKind kind);
extern jlib_decl const char * queryTreeTag(StatisticKind kind);
extern jlib_decl const char * queryCreatorTypeName(StatisticCreatorType sct);
extern jlib_decl const char * queryScopeTypeName(StatisticScopeType sst);
extern jlib_decl const char * queryMeasureName(StatisticMeasure measure);
extern jlib_decl const char * queryMeasurePrefix(StatisticMeasure measure);
extern jlib_decl StatsMergeAction queryMergeMode(StatisticMeasure measure);
extern jlib_decl StatsMergeAction queryMergeMode(StatisticKind kind);

extern jlib_decl StatisticMeasure queryMeasure(const char *  measure, StatisticMeasure dft);
extern jlib_decl StatisticKind queryStatisticKind(const char *  kind, StatisticKind dft);
extern jlib_decl StatisticCreatorType queryCreatorType(const char * sct, StatisticCreatorType dft);
extern jlib_decl StatisticScopeType queryScopeType(const char * sst, StatisticScopeType dft);

extern jlib_decl IStatisticGatherer * createStatisticsGatherer(StatisticCreatorType creatorType, const char * creator, const StatsScopeId & rootScope);
extern jlib_decl void serializeStatisticCollection(MemoryBuffer & out, IStatisticCollection * collection);
extern jlib_decl IStatisticCollection * createStatisticCollection(MemoryBuffer & in);

inline unsigned __int64 milliToNano(unsigned __int64 value) { return value * 1000000; } // call avoids need to upcast values
inline unsigned __int64 nanoToMilli(unsigned __int64 value) { return value / 1000000; }

extern jlib_decl unsigned __int64 convertMeasure(StatisticMeasure from, StatisticMeasure to, unsigned __int64 value);
extern jlib_decl unsigned __int64 convertMeasure(StatisticKind from, StatisticKind to, unsigned __int64 value);

extern jlib_decl StatisticCreatorType queryStatisticsComponentType();
extern jlib_decl const char * queryStatisticsComponentName();
extern jlib_decl void setStatisticsComponentName(StatisticCreatorType processType, const char * processName, bool appendIP);

extern jlib_decl void verifyStatisticFunctions();
extern jlib_decl void formatTimeCollatable(StringBuffer & out, unsigned __int64 value, bool nano);
extern jlib_decl unsigned __int64 extractTimeCollatable(const char *s, bool nano);

extern jlib_decl void validateScopeId(const char * idText);
extern jlib_decl void validateScope(const char * scopeText);

//Scopes need to be processed in a consistent order so they can be merged.
//activities are in numeric order
//edges must come before activities.
extern jlib_decl int compareScopeName(const char * left, const char * right);
extern jlib_decl unsigned queryScopeDepth(const char * text);
extern jlib_decl const char * queryScopeTail(const char * scope);
extern jlib_decl bool getParentScope(StringBuffer & parent, const char * scope);
extern jlib_decl void describeScope(StringBuffer & description, const char * scope);

//This interface is primarily here to reduce the dependency between the different components.
interface IStatisticTarget
{
    virtual void addStatistic(StatisticScopeType scopeType, const char * scope, StatisticKind kind, char * description, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction) = 0;
};

class jlib_decl NullStatisticTarget : implements IStatisticTarget
{
public:
    virtual void addStatistic(StatisticScopeType scopeType, const char * scope, StatisticKind kind, char * description, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction)
    {
    }
};


#endif
