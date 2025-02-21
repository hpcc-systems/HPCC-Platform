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
#include "jexcept.hpp"
#include "jmutex.hpp"
#include <vector>
#include <initializer_list>
#include <map>

#include "jstatcodes.h"

typedef unsigned __int64 stat_type;
typedef unsigned __int64 cost_type; // Decimal currency amount multiplied by 10^6
const unsigned __int64 MaxStatisticValue = (unsigned __int64)0-1U;
const unsigned __int64 AnyStatisticValue = MaxStatisticValue; // Use the maximum value to also represent unknown, since it is unlikely to ever occur.

inline constexpr stat_type seconds2StatUnits(stat_type secs) { return secs * 1000000000; }
inline constexpr stat_type msecs2StatUnits(stat_type ms) { return ms * 1000000; }
inline constexpr double statUnits2seconds(stat_type stat) {return ((double)stat) / 1000000000; }
inline constexpr double statUnits2msecs(stat_type stat) {return ((double)stat) / 1000000; }

inline constexpr stat_type statPercent(int value) { return (stat_type)value * 100; }            // Since 1 = 0.01% skew
inline constexpr stat_type statPercent(double value) { return (stat_type)(value * 100); }
inline constexpr stat_type statPercent(stat_type  value) { return (stat_type)(value * 100); }
inline constexpr stat_type statPercentageOf(stat_type value, stat_type per) { return value * per / 10000; }

inline StatisticKind queryStatsVariant(StatisticKind kind) { return (StatisticKind)(kind & ~StKindMask); }
constexpr cost_type money2cost_type(const double money) { return money * 1E6; }
constexpr double cost_type2money(cost_type cost) { return ((double) cost) / 1E6; }

extern jlib_decl void formatTime(StringBuffer & out, unsigned __int64 value);
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
    void setFileId(const char * _name);
    void setChannelId(unsigned id);
    void setSubgraphId(unsigned _id);
    void setWorkflowId(unsigned _id);
    void setChildGraphId(unsigned _id);
    void setDfuWorkunitId(const char * _name);
    void setSectionId(const char * _name);
    void setOperationId(const char * _name);

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
interface IStatisticVisitor;
interface ISpan;

class jlib_decl StatisticsMapping;
typedef std::function<void(const char * scope, StatisticScopeType sst, StatisticKind kind, stat_type value)> AggregateUpdatedCallBackFunc;

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
    virtual bool setStatistic(const char *scope, StatisticKind kind, unsigned __int64 value) = 0;
    virtual void serialize(MemoryBuffer & out) const = 0;
    virtual unsigned __int64 queryWhenCreated() const = 0;
    virtual void mergeInto(IStatisticGatherer & target) const = 0;
    virtual StringBuffer &toXML(StringBuffer &out) const = 0;
    virtual void visit(IStatisticVisitor & target) const = 0;
    virtual void visitChildren(IStatisticVisitor & target) const = 0;
    virtual void refreshAggregates(const StatisticsMapping & mapping, AggregateUpdatedCallBackFunc & fWhenAggregateUpdated) = 0;
    virtual stat_type aggregateStatistic(StatisticKind kind) const = 0;
    virtual void recordStats(const StatisticsMapping & mapping, IStatisticCollection * statsCollection, std::initializer_list<const StatsScopeId> path) = 0;
};

interface IStatisticCollectionIterator : public IIteratorOf<IStatisticCollection>
{
};

interface IStatisticVisitor
{
    virtual bool visitScope(const IStatisticCollection & cur) = 0;        // return true to iterate through children
};

enum StatsMergeAction
{
    StatsMergeKeepNonZero,
    StatsMergeReplace,
    StatsMergeSum,
    StatsMergeMin,
    StatsMergeMax,
    StatsMergeAppend,
    StatsMergeFirst,
    StatsMergeLast,
};

interface IStatisticGatherer : public IInterface
{
public:
    virtual void beginScope(const StatsScopeId & id) = 0;
    virtual void beginSubGraphScope(unsigned id) = 0;
    virtual void beginActivityScope(unsigned id) = 0;
    virtual void beginEdgeScope(unsigned id, unsigned oid) = 0;
    virtual void beginChildGraphScope(unsigned id) = 0;
    virtual void beginChannelScope(unsigned id) = 0;
    virtual void endScope() = 0;
    virtual void addStatistic(StatisticKind kind, unsigned __int64 value) = 0; // use updateStatistic() if kind could already be defined for the active scope
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

class ChannelActivityScope : public StatsScopeBlock
{
public:
    inline ChannelActivityScope(IStatisticGatherer & _gatherer, unsigned id) : StatsScopeBlock(_gatherer)
    {
        gatherer.beginChannelScope(id);
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
    //Takes a list of StatisticKind and a variable number of existing mappings and combines
    template <typename... Mappings>
    StatisticsMapping(const std::initializer_list<StatisticKind> &kinds, const Mappings &... mappings) : StatisticsMapping(&mappings...)
    {
        for (auto kind : kinds)
        {
            assert((kind != StKindNone) && (kind != StKindAll) && (kind < StMax));
            assert(!indexToKind.contains(kind));
            indexToKind.append(kind);
        }
        createMappings();
    }
    StatisticsMapping(StatisticKind kind)
    {
        if (StKindAll == kind)
        {
            for (int i = StKindAll+1; i < StMax; i++)
                indexToKind.append(i);
        }
        else
        {
            assert(kind != StKindNone && kind < StMax);
            indexToKind.append(kind);
        }
        createMappings();
    }
    inline unsigned getIndex(StatisticKind kind) const
    {
        dbgassertex(kind >= StKindNone && kind < StMax);
        return kindToIndex.item(kind);
    }
    inline bool hasKind(StatisticKind kind) const
    {
        return kindToIndex.item(kind) != numStatistics();
    }
    inline StatisticKind getKind(unsigned index) const { return (StatisticKind)indexToKind.item(index); }
    inline unsigned numStatistics() const { return indexToKind.ordinality(); }
    inline unsigned getUniqueHash() const { return hashcode; }

protected:
    StatisticsMapping() { }
    template <typename Mapping>
    StatisticsMapping(const Mapping *mapping)
    {
        ForEachItemIn(idx, mapping->indexToKind)
            indexToKind.append(mapping->indexToKind.item(idx));
    }
    template <typename Mapping, typename... Mappings>
    StatisticsMapping(const Mapping *mapping, const Mappings * ... mappings) : StatisticsMapping(mappings...)
    {
        ForEachItemIn(idx, mapping->indexToKind)
            indexToKind.append(mapping->indexToKind.item(idx));
    }
    void createMappings();
    bool equals(const StatisticsMapping & other);

protected:
    UnsignedArray kindToIndex;
    UnsignedArray indexToKind;
    unsigned hashcode = 0;          // Used to uniquely define the StatisticsMapping class required for nested scopes (do not persist)
private:
    StatisticsMapping& operator=(const StatisticsMapping&) =delete;
};

extern const jlib_decl StatisticsMapping noStatistics;
extern const jlib_decl StatisticsMapping allStatistics;
extern const jlib_decl StatisticsMapping heapStatistics;
extern const jlib_decl StatisticsMapping diskLocalStatistics;
extern const jlib_decl StatisticsMapping diskRemoteStatistics;
extern const jlib_decl StatisticsMapping diskReadRemoteStatistics;
extern const jlib_decl StatisticsMapping diskWriteRemoteStatistics;
extern const jlib_decl StatisticsMapping jhtreeCacheStatistics;
extern const jlib_decl StatisticsMapping stdAggregateKindStatistics;

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
        if (likely(ret))
            value.fetch_sub(ret);
        return ret;
    }
    inline void clear() { set(0); }
    void merge(unsigned __int64 otherValue, StatsMergeAction mergeAction);
    void sum(unsigned __int64 otherValue) { if (otherValue) addAtomic(otherValue); }
    inline void set(unsigned __int64 _value) { value = _value; }

protected:
    RelaxedAtomic<unsigned __int64> value;
};

class CNestedRuntimeStatisticMap;

//The CRuntimeStatisticCollection  used to gather statistics for an activity - it has no notion of its scope, but can contain nested scopes.
//Some of the functions have node parameters which have no meaning for the base implementation, but are used by the derived class
//CRuntimeSummaryStatisticCollection which is used fro summarising stats from multiple different worker nodes.
class jlib_decl CRuntimeStatisticCollection
{
public:
    CRuntimeStatisticCollection(const StatisticsMapping & _mapping, bool _ignoreUnknown = false) : mapping(_mapping)
#ifdef _TESTING
    ,ignoreUnknown(_ignoreUnknown)
#endif
    {
        unsigned num = mapping.numStatistics();
        values = new CRuntimeStatistic[num+1]; // extra entry is to gather unexpected stats and avoid tests when accumulating
    }
    CRuntimeStatisticCollection(const CRuntimeStatisticCollection & _other);
    virtual ~CRuntimeStatisticCollection();

    inline CRuntimeStatistic & queryStatistic(StatisticKind kind)
    {
        unsigned index = queryMapping().getIndex(kind);
#ifdef _TESTING
        if (!ignoreUnknown && (index >= mapping.numStatistics()))
        {
            VStringBuffer errMsg("Unknown mapping kind: %u", (unsigned)kind);
            throwUnexpectedX(errMsg.str());
        }
#endif
        return values[index];
    }
    inline CRuntimeStatistic * queryOptStatistic(StatisticKind kind) const
    {
        unsigned index = queryMapping().getIndex(kind);
        if (index == mapping.numStatistics())
            return nullptr;
        return &values[index];
    }
    inline const CRuntimeStatistic & queryStatistic(StatisticKind kind) const
    {
        unsigned index = queryMapping().getIndex(kind);
#ifdef _TESTING
        if (!ignoreUnknown && (index >= mapping.numStatistics()))
        {
            VStringBuffer errMsg("Unknown mapping kind: %u", (unsigned)kind);
            throwUnexpectedX(errMsg.str());
        }
#endif
        return values[index];
    }
    inline const CRuntimeStatistic & queryStatisticByIndex(unsigned index) const
    {
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
    void mergeStatistic(StatisticKind kind, unsigned __int64 value);
    void sumStatistic(StatisticKind kind, unsigned __int64 value);      // Special more efficient version of mergeStatistic useful in time critical sections
    void setStatistic(StatisticKind kind, unsigned __int64 value)
    {
        queryStatistic(kind).set(value);
    }
    unsigned __int64 getStatisticValue(StatisticKind kind) const
    {
        CRuntimeStatistic * stat = queryOptStatistic(kind);
        return stat ? stat->get() : 0;
    }
    unsigned __int64 getSerialStatisticValue(StatisticKind kind) const;
    void reset();
    void reset(const StatisticsMapping & toClear);

    CRuntimeStatisticCollection & registerNested(const StatsScopeId & scope, const StatisticsMapping & mapping);

    inline const StatisticsMapping & queryMapping() const { return mapping; };
    inline unsigned ordinality() const { return mapping.numStatistics(); }
    inline StatisticKind getKind(unsigned i) const { return mapping.getKind(i); }
    inline unsigned __int64 getValue(unsigned i) const { return values[i].get(); }

    void set(const CRuntimeStatisticCollection & other, unsigned node = 0);
    void merge(const CRuntimeStatisticCollection & other, unsigned node = 0);
    void updateDelta(CRuntimeStatisticCollection & target, const CRuntimeStatisticCollection & source);

    // Add the statistics to a span
    void exportToSpan(ISpan * span, StringBuffer & prefix) const;

    // Print out collected stats to string
    StringBuffer &toStr(StringBuffer &str) const;
    // Print out collected stats to string as XML
    StringBuffer &toXML(StringBuffer &str) const;

// The following functions are re-implemented in the derived CRuntimeSummaryStatisticCollection class
    virtual void recordStatistics(IStatisticGatherer & target, bool clear) const;
    virtual void setStatistic(StatisticKind kind, unsigned __int64 value, unsigned node);
    virtual void mergeStatistic(StatisticKind kind, unsigned __int64 value, unsigned node);
    virtual bool serialize(MemoryBuffer & out) const;  // Returns true if any non-zero
    virtual void deserialize(MemoryBuffer & in);
    virtual void deserializeMerge(MemoryBuffer& in);

    // Nested statistics need to be protected before merging, non nested are merged atomically
    inline bool isThreadSafeMergeSource() const { return nested == nullptr; }

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
#ifdef _TESTING
    bool ignoreUnknown = false;
#endif
};

/*
CRuntimeSummaryStatisticCollection
I can think of 3 different implementations of this class, each of which have advantages and disadvantages
1) Current scheme - specialized derived classes.
Derived versions of CRuntimeStatisticCollection, CNestedRuntimeStatisticCollection and CNestedRuntimeStatisticMap.
The individual results from the different nodes are not stored, but a summary result is stored and updated as stats
are updated.  The disadvantage with this approach is some functions in CRuntimeStatisticCollection which aren't
necessary.

2) Use template classes instead of derived classes.
Collection<CRuntimeStatistic> and Collection<DerivedStats:CRuntimeStatistic>.  It might be slightly more efficient,
but would require node to be passed to CRuntimeStatistic functions that ignore it.

3) Implement as completely separate classes.
Would provide a cleaner interface to the two classes, but would duplicate a substantial amount of code (all the nested
scope processing in addition to main class).  Unlikely to be better.

4) Have an array of stats, one for each node.
In this model the derived stats would only be calculated when required.  It would probably produce a cleaner interface
but would use more memory.  Perhaps more significantly it would be hard to merge the nested scopes e.g. for function
timings.  Only some of the stats would have them, so you would need to perform a union of all stat scopes.

So although HPCC-26541 was opened to refactor these classes, none of the alternatives are preferrable.
*/


//NB: Serialize and deserialize are not currently implemented.
class jlib_decl CRuntimeSummaryStatisticCollection : public CRuntimeStatisticCollection
{
public:
    CRuntimeSummaryStatisticCollection(const StatisticsMapping & _mapping);
    ~CRuntimeSummaryStatisticCollection();

    virtual void recordStatistics(IStatisticGatherer & target, bool clear = false) const override;
    virtual bool serialize(MemoryBuffer & out) const override;  // Returns true if any non-zero
    virtual void deserialize(MemoryBuffer & in) override;
    virtual void deserializeMerge(MemoryBuffer& in) override;

    void mergeStatistic(StatisticKind kind, unsigned __int64 value, unsigned node);
    void setStatistic(StatisticKind kind, unsigned __int64 value, unsigned node);
    double queryStdDevInfo(StatisticKind kind, unsigned __int64 &_min, unsigned __int64 &_max, unsigned &_minNode, unsigned &_maxNode) const;
protected:
    struct DerivedStats
    {
    public:
        void mergeStatistic(unsigned __int64 value, unsigned node);
        void setStatistic(unsigned __int64 value, unsigned node);
        double queryStdDevInfo(unsigned __int64 &_min, unsigned __int64 &_max, unsigned &_minNode, unsigned &_maxNode) const;
    public:
        unsigned __int64 max = 0;
        unsigned __int64 min = 0;
        unsigned __int64 count = 0;
        double sum = 0;
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
    void set(const CNestedRuntimeStatisticCollection & other, unsigned node);
    void merge(const CNestedRuntimeStatisticCollection & other, unsigned node);
    void recordStatistics(IStatisticGatherer & target, bool clear) const;
    void exportToSpan(ISpan * span, StringBuffer & prefix) const;
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
    void merge(const CNestedRuntimeStatisticMap & other, unsigned node);
    void set(const CNestedRuntimeStatisticMap & other, unsigned node);
    void recordStatistics(IStatisticGatherer & target, bool clear) const;
    void exportToSpan(ISpan * span, StringBuffer & prefix) const;
    StringBuffer & toStr(StringBuffer &str) const;
    StringBuffer & toXML(StringBuffer &str) const;
    void updateDelta(CNestedRuntimeStatisticMap & target, const CNestedRuntimeStatisticMap & source);


protected:
    virtual CRuntimeStatisticCollection * createStats(const StatisticsMapping & mapping);

protected:
    CIArrayOf<CNestedRuntimeStatisticCollection> map;
    mutable ReadWriteLock lock;
};

class CNestedRuntimeSummaryStatisticMap : public CNestedRuntimeStatisticMap
{
protected:
    virtual CRuntimeStatisticCollection * createStats(const StatisticsMapping & mapping) override;
};

//---------------------------------------------------------------------------------------------------------------------

//Some template helper classes for merging statistics from external sources.

template <class INTERFACE>
void mergeStats(CRuntimeStatisticCollection & stats, INTERFACE * source, const StatisticsMapping & mapping)
{
    if (!source)
        return;

    unsigned max = mapping.numStatistics();
    for (unsigned i=0; i < max; i++)
    {
        StatisticKind kind = mapping.getKind(i);
        stats.mergeStatistic(kind, source->getStatistic(kind));
    }
}

template <class INTERFACE>
void mergeStats(CRuntimeStatisticCollection & stats, const Shared<INTERFACE> & source, const StatisticsMapping & mapping) { mergeStats(stats, source.get(), mapping); }

template <class INTERFACE>
void mergeStats(CRuntimeStatisticCollection & stats, INTERFACE * source)       { mergeStats(stats, source, stats.queryMapping()); }

template <class INTERFACE>
void mergeStats(CRuntimeStatisticCollection & stats, const Shared<INTERFACE> & source) { mergeStats(stats, source.get(), stats.queryMapping()); }

template <class INTERFACE>
void mergeStat(CRuntimeStatisticCollection & stats, INTERFACE * source, StatisticKind kind)
{
    if (source)
        stats.mergeStatistic(kind, source->getStatistic(kind));
}

template <class INTERFACE>
void mergeStat(CRuntimeStatisticCollection & stats, const Shared<INTERFACE> & source, StatisticKind kind) { mergeStat(stats, source.get(), kind); }

// helper templates that add delta of previous vs current (from source) to tgtStats (and update prevStats)
template <class INTERFACE>
void updateStatsDelta(CRuntimeStatisticCollection & tgtStats, CRuntimeStatisticCollection & prevStats, INTERFACE * source)
{
    CRuntimeStatisticCollection curStats(tgtStats.queryMapping());
    mergeStats(curStats, source);
    prevStats.updateDelta(tgtStats, curStats); // NB: adds delta to tgtStats, and updates prevStats
}

template <class INTERFACE>
void updateStatsDelta(CRuntimeStatisticCollection & tgtStats, CRuntimeStatisticCollection & prevStats, const Shared<INTERFACE> & source)
{
    updateStatsDelta(tgtStats, prevStats, source.get());
}


//Some template helper classes for overwriting/setting statistics from external sources.

template <class INTERFACE>
void setStats(CRuntimeStatisticCollection & stats, INTERFACE * source, const StatisticsMapping & mapping)
{
    if (!source)
        return;

    unsigned max = mapping.numStatistics();
    for (unsigned i=0; i < max; i++)
    {
        StatisticKind kind = mapping.getKind(i);
        stats.setStatistic(kind, source->getStatistic(kind));
    }
}

template <class INTERFACE>
void setStats(CRuntimeStatisticCollection & stats, const Shared<INTERFACE> & source, const StatisticsMapping & mapping) { setStats(stats, source.get(), mapping); }

template <class INTERFACE>
void setStats(CRuntimeStatisticCollection & stats, INTERFACE * source)       { setStats(stats, source, stats.queryMapping()); }

template <class INTERFACE>
void setStats(CRuntimeStatisticCollection & stats, const Shared<INTERFACE> & source) { setStats(stats, source.get(), stats.queryMapping()); }

template <class INTERFACE>
void setStat(CRuntimeStatisticCollection & stats, INTERFACE * source, StatisticKind kind)
{
    if (source)
        stats.setStatistic(kind, source->getStatistic(kind));
}

template <class INTERFACE>
void setStat(CRuntimeStatisticCollection & stats, const Shared<INTERFACE> & source, StatisticKind kind) { setStat(stats, source.get(), kind); }


typedef std::map<StatisticKind, StatisticKind> StatKindMap;

template <class INTERFACE>
void mergeRemappedStats(CRuntimeStatisticCollection & stats, INTERFACE * source, const StatisticsMapping & mapping, const StatKindMap & remaps)
{
    if (!source)
        return;
    unsigned max = mapping.numStatistics();
    for (unsigned i=0; i < max; i++)
    {
        StatisticKind kind = mapping.getKind(i);
        if (remaps.find(kind) == remaps.end())
            stats.mergeStatistic(kind, source->getStatistic(kind));
    }
    for (auto remap: remaps)
    {
        if (mapping.hasKind(remap.second))
            stats.mergeStatistic(remap.second, source->getStatistic(remap.first));
    }
}

template <class INTERFACE>
void mergeRemappedStats(CRuntimeStatisticCollection & stats, INTERFACE * source, const StatKindMap & remaps)
{
    mergeRemappedStats(stats, source, stats.queryMapping(), remaps);
}

template <class INTERFACE>
void mergeRemappedStats(CRuntimeStatisticCollection & stats, const Shared<INTERFACE> & source, const StatKindMap & remaps)
{
    mergeRemappedStats(stats, source.get(), stats.queryMapping(), remaps);
}

template <class INTERFACE>
void updateRemappedStatsDelta(CRuntimeStatisticCollection & tgtStats, CRuntimeStatisticCollection & prevStats, INTERFACE * source, const StatKindMap & remap)
{
    CRuntimeStatisticCollection curStats(tgtStats.queryMapping());
    ::mergeRemappedStats(curStats, source, remap);
    prevStats.updateDelta(tgtStats, curStats); // NB: adds delta to tgtStats, and updates prevStats
}

template <class INTERFACE>
void updateRemappedStatsDelta(CRuntimeStatisticCollection & tgtStats, CRuntimeStatisticCollection & prevStats, const Shared<INTERFACE> & source, const StatKindMap & remap)
{
    updateRemappedStatsDelta(tgtStats, prevStats, source.get(), remap);
}


//---------------------------------------------------------------------------------------------------------------------

//A class for minimizing the overhead of collecting timestamps.
class IpAddress;

extern jlib_decl unsigned __int64 getTimeStampNowValue();
extern jlib_decl unsigned __int64 getIPV4StatsValue(const IpAddress & ip);
extern jlib_decl StringBuffer & formatStatistic(StringBuffer & out, unsigned __int64 value, StatisticMeasure measure);
extern jlib_decl StringBuffer & formatStatistic(StringBuffer & out, unsigned __int64 value, StatisticKind kind);
extern jlib_decl void formatTimeStampAsLocalTime(StringBuffer & out, unsigned __int64 value);
extern jlib_decl stat_type readStatisticValue(const char * cur, const char * * end, StatisticMeasure measure);
extern jlib_decl stat_type normalizeTimestampToNs(stat_type value);

extern jlib_decl unsigned __int64 mergeStatisticValue(unsigned __int64 prevValue, unsigned __int64 newValue, StatsMergeAction mergeAction);

extern jlib_decl StatisticMeasure queryMeasure(StatisticKind kind);
extern jlib_decl const char * queryStatisticName(StatisticKind kind);
extern jlib_decl void queryLongStatisticName(StringBuffer & out, StatisticKind kind);
extern jlib_decl const char * queryStatisticDescription(StatisticKind kind);
extern jlib_decl const char * queryTreeTag(StatisticKind kind);
extern jlib_decl const char * queryCreatorTypeName(StatisticCreatorType sct);
extern jlib_decl const char * queryScopeTypeName(StatisticScopeType sst);
extern jlib_decl const char * queryMeasureName(StatisticMeasure measure);
extern jlib_decl const char * queryMeasurePrefix(StatisticMeasure measure);
extern jlib_decl StatsMergeAction queryMergeMode(StatisticKind kind);

extern jlib_decl StatisticMeasure queryMeasure(const char *  measure, StatisticMeasure dft);
extern jlib_decl StatisticKind queryStatisticKind(const char *  kind, StatisticKind dft);
extern jlib_decl StatisticCreatorType queryCreatorType(const char * sct, StatisticCreatorType dft);
extern jlib_decl StatisticScopeType queryScopeType(const char * sst, StatisticScopeType dft);

extern jlib_decl IStatisticGatherer * createStatisticsGatherer(StatisticCreatorType creatorType, const char * creator, const StatsScopeId & rootScope);
extern jlib_decl void serializeStatisticCollection(MemoryBuffer & out, IStatisticCollection * collection);
extern jlib_decl IStatisticCollection * createStatisticCollection(MemoryBuffer & in);

extern jlib_decl IStatisticCollection * createStatisticCollection(const StatsScopeId & scopeId);
inline unsigned __int64 milliToNano(unsigned __int64 value) { return value * 1000000; } // call avoids need to upcast values
inline unsigned __int64 nanoToMilli(unsigned __int64 value) { return value / 1000000; }

extern jlib_decl unsigned __int64 convertMeasure(StatisticMeasure from, StatisticMeasure to, unsigned __int64 value);
extern jlib_decl unsigned __int64 convertMeasure(StatisticKind from, StatisticKind to, unsigned __int64 value);

extern jlib_decl StatisticCreatorType queryStatisticsComponentType();
extern jlib_decl const char * queryStatisticsComponentName();
extern jlib_decl void setStatisticsComponentName(StatisticCreatorType processType, const char * processName, bool appendIP);

extern jlib_decl void verifyStatisticFunctions();
extern jlib_decl void formatTimeCollatable(StringBuffer & out, unsigned __int64 value, bool nano);
extern jlib_decl unsigned __int64 extractTimeCollatable(const char *s, const char * * end);

extern jlib_decl void validateScopeId(const char * idText);
extern jlib_decl void validateScope(const char * scopeText);
extern jlib_decl StatisticScopeType getScopeType(const char * scope);

//Scopes need to be processed in a consistent order so they can be merged.
//activities are in numeric order
//edges must come before activities.
extern jlib_decl int compareScopeName(const char * left, const char * right);
extern jlib_decl unsigned queryScopeDepth(const char * text);
extern jlib_decl const char * queryScopeTail(const char * scope);
extern jlib_decl bool getParentScope(StringBuffer & parent, const char * scope);
extern jlib_decl bool isParentScope(const char *parent, const char *scope);
extern jlib_decl void describeScope(StringBuffer & description, const char * scope);

//This interface is primarily here to reduce the dependency between the different components.
interface IStatisticTarget
{
    virtual void addStatistic(StatisticScopeType scopeType, const char * scope, StatisticKind kind, char * description, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction) = 0;
};

class jlib_decl NullStatisticTarget : implements IStatisticTarget
{
public:
    virtual void addStatistic(StatisticScopeType scopeType, const char * scope, StatisticKind kind, char * description, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction) override
    {
    }
};

class jlib_decl RuntimeStatisticTarget : implements IStatisticTarget
{
public:
    RuntimeStatisticTarget(CRuntimeStatisticCollection & _target) : target(_target) {}

    virtual void addStatistic(StatisticScopeType scopeType, const char * scope, StatisticKind kind, char * description, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction) override
    {
        target.addStatistic(kind, value);
    }
protected:
    CRuntimeStatisticCollection & target;
};

extern jlib_decl StringBuffer & formatMoney(StringBuffer &out, unsigned __int64 value);

#endif
