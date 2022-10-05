/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#include "jliball.hpp"

#include "workunit.hpp"
#include "anacommon.hpp"
#include "anarule.hpp"
#include "anawu.hpp"
#include "thorcommon.hpp"
#include "commonext.hpp"


struct RoxieOptions
{
// The following are set from options in the query
    bool onlyActive = true;
    double thresholdPercent = 1.0;
    bool onlyCriticalPath = false;

//The following are computed:
    stat_type minTime = 0;
    stat_type maxTime = (stat_type)-1;
    stat_type timeThreshold = 0;
    PointerArrayOf<WaThread> threadFilter;  // Which "threads of execution" are on the crticial path.
};

template <class T>
inline int starShip(T x, T y)
{
    if (x < y)
        return -1;
    if (x > y)
        return +1;
    return 0;
}


//-----------------------------------------------------------------------------------------------------------
enum  WutOptValueType
{
    wutOptValueTypeMSec,
    wutOptValueTypeSeconds,
    wutOptValueTypePercent,
    wutOptValueTypeCount,
    wutOptValueTypeBool,
    wutOptValueTypeMax,
};

struct WuOption
{
    WutOptionType option;
    const char * name;
    stat_type defaultValue;
    WutOptValueType type;
};

constexpr struct WuOption wuOptionsDefaults[watOptMax]
= { {watOptMinInterestingTime, "minInterestingTime", 1000, wutOptValueTypeMSec},
    {watOptMinInterestingCost, "minInterestingCost", 30000, wutOptValueTypeMSec},
    {watOptSkewThreshold, "skewThreshold", 20, wutOptValueTypePercent},
    {watOptMinRowsPerNode, "minRowsPerNode", 1000, wutOptValueTypeCount},
    {watPreFilteredKJThreshold, "preFilteredKJThreshold", 50, wutOptValueTypePercent},
};

constexpr bool checkWuOptionsDefaults(int i = watOptMax)
{
    return ((wuOptionsDefaults[i-1].name != nullptr && (wuOptionsDefaults[i-1].option == i-1) && wuOptionsDefaults[i-1].type < wutOptValueTypeMax ) &&
           (i==1 || checkWuOptionsDefaults(i-1)));
}
static_assert(checkWuOptionsDefaults(), "wuOptionsDefaults[] not populated correctly");

class WuAnalyserOptions : public IAnalyserOptions
{
public:
   WuAnalyserOptions()
    {
        for (int opt = watOptFirst; opt < watOptMax; opt++)
            setOptionValue(static_cast<WutOptionType>(opt), wuOptionsDefaults[opt].defaultValue);
    }

    void setOptionValue(WutOptionType opt, __int64 val)
    {
        assertex(opt<watOptMax);
        switch(wuOptionsDefaults[opt].type)
        {
            case wutOptValueTypeMSec:
                wuOptions[opt] = msecs2StatUnits(val);
                break;
            case wutOptValueTypeSeconds:
                wuOptions[opt] = seconds2StatUnits(val);
                break;
            case wutOptValueTypePercent:
                wuOptions[opt] = statPercent((stat_type)val);
                break;
            case wutOptValueTypeCount:
                wuOptions[opt] = (stat_type) val;
                break;
            case wutOptValueTypeBool:
                wuOptions[opt] = (stat_type) val;
                break;
            default:
                throw MakeStringException(-1, "WuAnalyserOptions::setOptionValue - unknown wuOptionsDefaults[%d].type=%d", (int) opt, (int) wuOptionsDefaults[opt].type);
        }
    }

    void applyConfig(IPropertyTree *options)
    {
        if (!options) return;
        for (int opt = watOptFirst; opt < watOptMax; opt++)
        {
            StringBuffer wuOptionName("@");
            wuOptionName.append(wuOptionsDefaults[opt].name);
            __int64 val =  options->getPropInt64(wuOptionName, -1);
            if (val!=-1)
                setOptionValue(static_cast<WutOptionType>(opt), val);
        }
    }

    void applyConfig(IConstWorkUnit * wu)
    {
        for (int opt = watOptFirst; opt < watOptMax; opt++)
        {
            StringBuffer wuOptionName("analyzer_");
            wuOptionName.append(wuOptionsDefaults[opt].name);
            __int64 val = wu->getDebugValueInt64(wuOptionName, -1);
            if (val!=-1)
                setOptionValue(static_cast<WutOptionType>(opt), val);
        }
    }
    stat_type queryOption(WutOptionType opt) const override { return wuOptions[opt]; }
private:
    stat_type wuOptions[watOptMax];
};

//-----------------------------------------------------------------------------------------------------------

//MORE: Split this in two - for new code and old code.
class WorkunitAnalyserBase
{
public:
    WorkunitAnalyserBase();

    void analyse(IConstWorkUnit * wu);
    WuScope * getRootScope() { return LINK(root); }

protected:
    void collateWorkunitStats(IConstWorkUnit * workunit, const WuScopeFilter & filter);
    WuScope * selectFullScope(const char * scope);
    WuScope * resolveActivity(const char * name);

protected:
    Owned<WuScope> root;
    stat_type minTimestamp = 0;
};

//-----------------------------------------------------------------------------------------------------------

class WorkunitRuleAnalyser : public WorkunitAnalyserBase
{
public:
    WorkunitRuleAnalyser();

    void applyConfig(IPropertyTree *cfg, IConstWorkUnit * wu);

    void applyRules();
    void check(const char * scope, IWuActivity & activity);
    void print();
    void update(IWorkUnit *wu, double costRate);

protected:
    CIArrayOf<AActivityRule> rules;
    CIArrayOf<PerformanceIssue> issues;
    WuAnalyserOptions options;
};


class WorkunitStatsAnalyser : public WorkunitAnalyserBase
{
public:
    void applyOptions(IPropertyTree * cfg);

    void calcDependencies();
    void findActiveActivities(const StringArray & args);
    void findHotspots(const char * rootScope, stat_type & totalTime, CIArrayOf<WuHotspotResult> & results);
    void findHotspotsOld(const StringArray & args);
    void spotCommonPath(const StringArray & args);
    void reportActivity(const StringArray & args);
    void traceCriticalPaths(const StringArray & args);
    void traceDependencies();
    void traceWhyWaiting(const StringArray & args);
    void adjustTimestamps();
    void walkStartupTimes(const StringArray & args);

    double getThresholdPercent() const { return opts.thresholdPercent; }

protected:
//dependency processing
    WaThread * createThread(WuScope * creator, WaThreadType type);
    WuScope * queryLongestRootActivity() const;
    void processRootActivity(WuScope & scope, WaThread * thread, WaThreadType type);
    void processActivities(WuScope & scope, WaThread * thread, bool isStart);

protected:
    CIArrayOf<WaThread> threads;
    RoxieOptions opts;
};

//-----------------------------------------------------------------------------------------------------------

void WuScopeHashTable::onRemove(void *et)
{
    WuScope * elem = reinterpret_cast<WuScope *>(et);
    elem->Release();
}
unsigned WuScopeHashTable::getHashFromElement(const void *et) const
{
    const WuScope * elem = reinterpret_cast<const WuScope *>(et);
    return hashScope(elem->queryName());
}

unsigned WuScopeHashTable::getHashFromFindParam(const void *fp) const
{
    const char * search = reinterpret_cast<const char *>(fp);
    return hashScope(search);
}

const void * WuScopeHashTable::getFindParam(const void *et) const
{
    const WuScope * elem = reinterpret_cast<const WuScope *>(et);
    return elem->queryName();
}

bool WuScopeHashTable::matchesFindParam(const void *et, const void *key, unsigned fphash) const
{
    const WuScope * elem = reinterpret_cast<const WuScope *>(et);
    const char * search = reinterpret_cast<const char *>(key);
    return streq(elem->queryName(), search);
}

bool WuScopeHashTable::matchesElement(const void *et, const void *searchET) const
{
    const WuScope * elem = reinterpret_cast<const WuScope *>(et);
    const WuScope * searchElem = reinterpret_cast<const WuScope *>(searchET);
    return streq(elem->queryName(), searchElem->queryName());
}

//-----------------------------------------------------------------------------------------------------------

void WaThread::extractAllDependencies(PointerArrayOf<WaThread> & result)
{
    if (type == WaThreadType::Dependency)
    {
        if (!result.contains(this))
            result.append(this);
    }
    if (creator)
        creator->extractAllDependencies(result);
}

bool WaThread::isBlockedBy(WaThread * thread)
{
    if (this == thread)
        return true;
    if (!creator)
        return false;
    return creator->isBlockedBy(thread);
}

unsigned WaThread::queryDepth() const
{
    if (creator)
        return creator->queryDepth()+1;
    return 1;
}


//-----------------------------------------------------------------------------------------------------------

WuHotspotResult::WuHotspotResult(WuScope * _activity, const char * _sink, bool _isRoot, double _totalPercent, double _startPercent, double _myStartPercent, double _runPercent, bool _startSignificant, bool _runSignificant)
: activity(_activity), sink(_sink), isRoot(_isRoot), totalPercent(_totalPercent), startPercent(_startPercent), myStartPercent(_myStartPercent), runPercent(_runPercent), startSignificant(_startSignificant), runSignificant(_runSignificant)
{
}


int WuHotspotResult::compareTime(const WuHotspotResult & other) const
{
    if (runSignificant || other.runSignificant)
    {
        if (runPercent != other.runPercent)
            return starShip(other.runPercent, runPercent);
    }
    if (startSignificant || other.startSignificant)
    {
        if (startPercent != other.startPercent)
            return starShip(other.startPercent, startPercent);
    }
    return starShip(other.totalPercent, totalPercent);
}

int WuHotspotResult::compareStart(const WuHotspotResult & other) const
{
    stat_type thisStarted = activity->getBeginTimestamp();
    stat_type otherStarted = other.activity->getBeginTimestamp();
    if (thisStarted != otherStarted)
        return starShip(thisStarted, otherStarted);
    stat_type thisEnded = activity->getEndTimestamp();
    stat_type otherEnded = other.activity->getEndTimestamp();
    if (thisEnded != otherEnded)
        return starShip(thisEnded, otherEnded);
    return compareTime(other);
}

void WuHotspotResult::report()
{
    const char * prefix = isRoot ? "R" : " ";
    ThorActivityKind kind = activity->queryThorActivityKind();
    StringBuffer fullName;
    activity->getFullScopeName(fullName);
    printf("%-6s %s(%-6s %6.2f%%,%6.2f%%,%6.2f%%,%6.2f%%) %s\n", fullName.str(), prefix, sink.str(), totalPercent, startPercent, myStartPercent, runPercent, activityKindStr(kind));
}

void WuHotspotResult::reportTime()
{
    const char * prefix = isRoot ? "R" : " ";
    ThorActivityKind kind = activity->queryThorActivityKind();
    const char * critical = activity->  isOnCriticalPath() ? "*" : " ";
    printf("%s%-6s {%.9f..%.9f,%.9f..%.9f} %s(%-6s %6.2f%%,%6.2f%%,%6.2f%%) %s\n",
            critical, activity->queryName(),
            activity->getBeginTimestamp() / 1E9, activity->getEndStartTimestamp() / 1E9, activity->getBeginRunTimestamp() / 1E9, activity->getEndTimestamp() / 1E9,
            prefix, sink.str(), totalPercent, startPercent, runPercent, activityKindStr(kind));
/*
    printf("%-6s {%10llu..%10llu,%10llu..%10llu} %s(%-6s %6.2f%%,%6.2f%%,%6.2f%%) %s\n",
            activity->queryName(),
            activity->getBeginTimestamp(), activity->getEndStartTimestamp(), activity->getBeginRunTimestamp(), activity->getEndTimestamp(),
            prefix, sink.str(), totalPercent, startPercent, runPercent, activityKindStr(kind));
*/
}

//-----------------------------------------------------------------------------------------------------------

void WuScope::applyRules(WorkunitRuleAnalyser & analyser)
{
    for (auto & cur : scopes)
    {
        if (cur.queryScopeType() == SSTactivity)
            analyser.check(cur.queryName(), cur);
        cur.applyRules(analyser);
    }
}

void WuScope::connectActivities()
{
    //Yuk - scopes can be added to while they are being iterated, so need to create a list first
    CICopyArrayOf<WuScope> toWalk;
    for (auto & cur : scopes)
        toWalk.append(cur);

    ForEachItemIn(i, toWalk)
    {
        WuScope & cur = toWalk.item(i);

        if ((cur.queryScopeType() == SSTedge) && !cur.getAttr(WaIsChildGraph))
        {
            WuScope * source = cur.querySource();
            WuScope * sink = cur.queryTarget();
            //Sometimes there are spurious stats recorded e.g., output number for sinks - ignore them
            if (!source || !sink)
                continue;

            if (sink->parent != source->parent)
            {
                //Ensure that dependencies from subqueries are linked to the parent activity at the same depth
                unsigned sinkParents = sink->numParents();
                unsigned sourceParents = source->numParents();
                while (sinkParents > sourceParents)
                {
                    sink = sink->parent;
                    sinkParents--;
                }
            }
            IPropertyTree * attrs = cur.queryAttrs();

            bool isDependency = false;
            if (cur.getAttr(WaIsDependency))
            {
                isDependency = true;
            }

            if (isDependency)
            {
                if (source && sink)
                {
                    sink->addDependency(source);
                    source->addDependent(sink);
                }
            }
            else
            {
                if (source)
                    source->setOutput(cur.getAttr(WaSourceIndex), &cur);
                if (sink)
                    sink->setInput(cur.getAttr(WaTargetIndex), &cur);
            }
        }
        cur.connectActivities();
    }
}

void WuScope::gatherRootActivities(ScopeVector & result) const
{
    for (auto & cur : scopes)
    {
        if (cur.queryScopeType() == SSTactivity)
        {
            if (cur.isSink())
                result.push_back(&cur);
        }
        else
            cur.gatherRootActivities(result);
    }
}

const char * WuScope::getFullScopeName(StringBuffer & fullScopeName) const
{
    // Building full scope name dynamically is good enough as long as there's not too many calls here
    if (parent)
    {
        parent->getFullScopeName(fullScopeName);
        if (fullScopeName.length())
            return fullScopeName.append(":").append(name);
        else
            return fullScopeName.set(name);
    }
    else
        return fullScopeName.set(name);
}

void WuScope::extractAllDependencies(PointerArrayOf<WaThread> & result)
{
    //MORE: Need to avoid combinatorial explosion walking parents.
    for (auto cur : callers)
    {
        if (cur->creator != this)
            cur->extractAllDependencies(result);
    }
}


unsigned WuScope::queryDepth() const
{
    if (!depth)
    {
        //depth = UINT_MAX;
        for (auto cur : callers)
        {
            if (std::find(childThreads.begin(), childThreads.end(), cur) != childThreads.end())
                continue;
            unsigned callerDepth = cur->queryDepth();
            if (callerDepth > depth)
                depth = callerDepth;
        }
    }
    return depth;
}

unsigned WuScope::numParents() const
{
    unsigned result = 0;
    const WuScope * scope = this;
    for (;;)
    {
        scope = scope->parent;
        if (!scope)
            break;
        result++;
    }
    return result;
}

WuScope * WuScope::resolve(const char * scope, bool createMissing)
{
    assertex(scope);
    WuScope * match = scopes.find(scope);
    if (match)
        return match;
    for (auto & cur : scopes)
    {
        WuScope * match = cur.resolve(scope, false);
        if (match)
            return match;
    }

    if (!createMissing)
        return nullptr;

    match = new WuScope(scope, this);
    scopes.addNew(match);
    return match;
}

WuScope * WuScope::select(const char * scope)
{
    assertex(scope);
    WuScope * match = scopes.find(scope);
    if (match)
        return match;

    match = new WuScope(scope, this);
    scopes.addNew(match);
    return match;
}

void WuScope::showLargeDependencies(PointerArrayOf<WuScope> & visited, stat_type startTime, stat_type searchTime, stat_type interesting)
{
    if (visited.contains(this))
        return;
    visited.append(this);
    for (auto caller : callers)
    {
        WuScope * creator = caller->creator;
        ForEachItemIn(i, caller->dependencies)
        {
            WuScope * dependency = caller->dependencies.item(i);
            if (visited.contains(dependency))
                continue;
            visited.append(dependency);
            stat_type beginTime = dependency->getBeginTimestamp();
            stat_type endTime = dependency->getEndTimestamp();
            if (beginTime < startTime || endTime >= searchTime)
                continue;
            stat_type elapsed = endTime - beginTime;
            if (elapsed >= interesting)
            {
                printf("%s: %llu [%llu..%llu]\n", dependency->queryName(), elapsed, beginTime, endTime);
            }
        }

        if (creator)
            creator->showLargeDependencies(visited, startTime, searchTime, interesting);
    }
}

void WuScope::addDependency(WuScope * scope)
{
    dependencies.push_back(scope);
}

void WuScope::addDependent(WuScope * scope)
{
    dependents.push_back(scope);
}

void WuScope::adjustTimestamps(stat_type minTimestamp)
{
    for (auto & cur : scopes)
    {
        cur.adjustTimestamps(minTimestamp);
    }

    Owned<IAttributeIterator> iter = attrs->getAttributes();
    ForEach(*iter)
    {
        const char * name = iter->queryName();
        StatisticKind kind = queryStatisticKind(name+1, StKindNone);
        if ((kind != StKindNone) && (queryMeasure(kind) == SMeasureTimestampUs))
            attrs->setPropInt64(name, _atoi64(iter->queryValue()) - minTimestamp);
    }
}

void WuScope::gatherSelfAndInputs(PointerArrayOf<WuScope> & activities)
{
    if (activities.contains(this))
        return;
    activities.append(this);
    for (auto input : inputs)
        input->querySource()->gatherSelfAndInputs(activities);
}


void WuScope::walkStartupTimes(stat_type minTimestamp)
{
    stat_type startupTime = getStartTime();
    if (startupTime == 0)
        return;
    stat_type endTime = getBeginTimestamp() + startupTime;
    if (endTime < minTimestamp)
        return;
    printf("  %s %llu..%llu %" I64F "u [%llu]\n", queryName(), getBeginTimestamp(), endTime, startupTime, getStatRaw(StTimeDependencies));

    for (auto input : inputs)
    {
        WuScope * source = input->querySource();
        stat_type childTime = source->getStartTime();
        //ChildTime is the total time this activity spends starting.  If the any input takes at least 10% of the total
        //for this activity then it is worth walking.
        if (childTime * 10 > startupTime)
        {
            source->walkStartupTimes(minTimestamp);
        }
    }
}


void WaThread::expandAllDependencies(StringBuffer & result)
{
    ForEachItemIn(iDep, dependencies)
    {
        WuScope * cur = dependencies.item(iDep);
        if (result.length())
            result.append(" ");
        result.append(cur->queryName());
    }
    for (auto activity: activities)
    {
        for(auto child : activity->childThreads)
        {
            if (child != this)
                child->expandAllDependencies(result);
        }
    }
}

bool WuScope::isRootActivity() const
{
    for (auto caller : callers)
    {
        WuScope * creator = caller->creator;
        if (!creator)
            return true;
    }
    return false;
}

bool WuScope::gatherCriticalPaths(StringArray & paths, WaThread * childThread, WuScope * childDependency, stat_type maxDependantTime, unsigned indent, RoxieOptions & options)
{
    if (checkedCriticalPath && !onCriticalPath)
        return false;
    unsigned prevPaths = paths.ordinality();
    StringBuffer acText, dependText;
    char kind = isRootActivity() ? 'P' : executionThread ? 'R' : ' ';
    const char * prevMatch = checkedCriticalPath ? "...." : "";

    acText.pad(indent).appendf("%s %c{%llu..%llu}%s", queryName(), kind, getBeginTimestamp(), getEndTimestamp(), prevMatch);
    paths.append(acText);

    if (checkedCriticalPath)
        return true;
    checkedCriticalPath = true;

    bool matchedRoot = false;
    if (executionThread)
    {
        //This activity is a dependency.  Check all the places the activity is used from
        for (auto caller : callers)
        {
            WuScope * creator = caller->creator;
            if (creator)
            {
                if (creator->waitsForDependency(this))
                {
                    bool minimizeDependencies = false;
                    //Any activity that uses this dependency before the target activity is executed is theoretically
                    //a dependency, but in practice if the activity was run after this dependency ran it is not likely to be interesting.
                    stat_type maxTime = minimizeDependencies ? std::min(maxDependantTime, getEndTimestamp()) : maxDependantTime;
                    if (creator->gatherCriticalPaths(paths, nullptr, this, maxDependantTime, indent+1, options))
                        matchedRoot = true;
                }
            }
            else
                matchedRoot = true;
        }
    }
    else
    {
        //Walk through each of the activities that use this activity as an input.
        //Exclude inputs that are not on the critical path.  Either they are started after the first input finishes,
        //or they are started after the child dependency we are tracking finishes.  (The dependency will always be lower)
        stat_type maxTime = maxDependantTime;
        for (auto caller : callers)
        {
            WuScope * creator = caller->creator;
            assertex(creator);
            //MORE: Add a check on times.  Should it be based on start or end time?  Depends if following a dependency
            //=> need a flag
            if (maxTime)
            {
                if (creator->getBeginTimestamp() > maxTime)
                    continue;
            }
            if (creator->gatherCriticalPaths(paths, caller, childDependency, maxDependantTime, indent+1, options))
                matchedRoot = true;
        }
    }
    if (!matchedRoot)
        paths.popn(paths.ordinality() - prevPaths);
    else
        onCriticalPath = true;
    return matchedRoot;
}


bool WuScope::isBlockedBy(WaThread * thread)
{
    if (lastBlockedCheck == thread)
        return cachedBlocked;
    lastBlockedCheck = thread;
    cachedBlocked = true;
    for (auto & cur : callers)
    {
        if (!cur->isBlockedBy(thread))
        {
            cachedBlocked = false;
            break;
        }
    }
    return cachedBlocked;
}

bool WuScope::isRoot() const
{
    return !getAttr(WaIsInternal);
}


bool WuScope::mustExecuteInternalSink() const
{
    unsigned dependentCount = dependents.size();
    if (dependentCount == 0)
        return true;

    //If an internal result has as many dependencies (within the graph) as uses (which includes from outside the graph) then don't execute it unconditionally.
    unsigned usageCount = getAttr(WaNumGlobalUses);
    bool internalSpillAllUsesWithinGraph = (dependentCount==usageCount);
    return !internalSpillAllUsesWithinGraph;
}

bool WuScope::isInternalSequence() const
{
    return false; // can't tell from the graph at the moment...
}

bool WuScope::isSink() const
{
    if (!isRoot())
        return false;

    switch (getAttr(WaKind))
    {
    case TAKlocalresultwrite:
    case TAKdictionaryresultwrite:
    case TAKgraphloopresultwrite:
        return mustExecuteInternalSink();
    case TAKapply:
    case TAKdistribution:
    case TAKindexwrite:
    case TAKexternalsink:
    case TAKifaction:
    case TAKparallel:
    case TAKsequential:
    case TAKwhen_action:
    case TAKsoap_datasetaction:
    case TAKsoap_rowaction:
    case TAKdatasetresult:
    case TAKrowresult:
        return true;
    case TAKsideeffect:
        return outputs.size() == 0;
    case TAKworkunitwrite:
        //if (sequence-() != internal) return false;
        if (!isInternalSequence())
            return true;
        return mustExecuteInternalSink();
    case TAKremoteresult:
        if (!isInternalSequence())
            return true;
        return mustExecuteInternalSink();  // Codegen normally optimizes these away, but if it doesn't we need to treat as a (null) sink rather than a dependency or upstream activities are not stopped properly
    case TAKdiskwrite:
    case TAKcsvwrite:
    case TAKjsonwrite:
    case TAKxmlwrite:
        return true;
    }
    return false;
}

void WuScope::setInput(unsigned i, WuScope * scope)
{
    while (inputs.size() <= i)
        inputs.push_back(nullptr);
    inputs[i] = scope;
}

void WuScope::setOutput(unsigned i, WuScope * scope)
{
    while (outputs.size() <= i)
        outputs.push_back(nullptr);
    outputs[i] = scope;
}

WuScope * WuScope::querySource()
{
    const char * source = attrs->queryProp("@IdSource");
    if (!source)
        return nullptr;
    return parent->resolve(source, true);
}

WuScope * WuScope::queryTarget()
{
    const char * target = attrs->queryProp("@IdTarget");
    if (!target)
        return nullptr;
    return parent->resolve(target, true);
}

stat_type WuScope::getStatRaw(StatisticKind kind, StatisticKind variant) const
{
    StringBuffer name;
    name.append('@').append(queryStatisticName(kind | variant));
    return attrs->getPropInt64(name);
}

unsigned WuScope::getAttr(WuAttr attr) const
{
    StringBuffer name;
    name.append('@').append(queryWuAttributeName(attr));
    return attrs->getPropInt64(name);
}

void WuScope::getAttr(StringBuffer & result, WuAttr attr) const
{
    StringBuffer name;
    name.append('@').append(queryWuAttributeName(attr));
    attrs->getProp(name, result);
}


void WuScope::gatherCallers(PointerArrayOf<WaThread> & path)
{
    for (auto caller : callers)
    {
        if (!path.contains(caller))
            path.append(caller);
    }
}


void WuScope::noteCall(WaThread * thread)
{
    //Only add the same calling thread once (e.g. could be a dependency of multiple activities)
    //if (std::find(callers.begin(), callers.end(), thead) == callers.end())
    if (executionThread != thread)
    {
        callers.push_back(thread);
        callerSequence.push_back(++thread->sequence);
        thread->activities.push_back(this);
    }
}


void WuScope::noteStarted(WaThread * thead)
{
    childThreads.push_back(thead);
}

void WuScope::resetState()
{
    onCriticalPath = false;
    checkedCriticalPath = false;
    for (auto & cur : scopes)
        cur.resetState();
}

static int compareScopeId(CInterface * const * pLeft, CInterface * const * pRight)
{
    WuScope * left = (WuScope *)*pLeft;
    WuScope * right = (WuScope *)*pRight;
    return compareScopeName(left->queryName(), right->queryName());
}

void WuScope::trace(unsigned indent, bool skipEdges, unsigned maxLevels, unsigned scopeMask) const
{
    StatisticScopeType scopeType = queryScopeType();
    if (scopeMask && !(scopeMask & (1 << scopeType)))
        return;

    printf("%*s%s: \"%s\" (", indent, " ", queryScopeTypeName(), queryName());
    for (auto in : inputs)
        printf("%s ", skipEdges ? in->querySource()->queryName() : in->queryName());
    printf("->");
    for (auto out : outputs)
        printf(" %s",  skipEdges ? out->queryTarget()->queryName() : out->queryName());

    printf(") [");
    if (scopes.count())
    {
        printf("children: ");
        for(auto & scope: scopes)
        {
            printf("%s ", scope.queryName());
        }
    }
    if (dependencies.size())
    {
        printf("depends: ");
        for(auto & cur: dependencies)
        {
            printf("%s ", cur->queryName());
        }
    }
    if (callers.size())
    {
        printf("thread(");
        for (unsigned i=0; i < callers.size(); i++)
        {
            printf("%s:%u ", callers.at(i)->queryName(), callerSequence.at(i));
        }
        if (childThreads.size())
        {
            printf("*");
            for (auto & cur : childThreads)
            {
                printf("%s ", cur->queryName());
            }
        }

        if (executionThread)
            printf("$%s", executionThread->queryName());

        printf("@%u)", queryDepth());
    }
    printf("] {\n");

    printXML(queryAttrs(), indent);

    if (maxLevels > 1)
    {
        CICopyArrayOf<WuScope> toWalk;
        for (WuScope & cur : scopes)
        {
            toWalk.append(cur);
        }

        toWalk.sort(compareScopeId);

        ForEachItemIn(i, toWalk)
        {
            WuScope & cur = toWalk.item(i);
            cur.trace(indent+4, skipEdges, maxLevels-1, scopeMask);
        }
    }

    printf("%*s}\n",indent, " ");
}


stat_type WuScope::getBeginTimestamp() const
{
    stat_type acStarted = getStatRaw(StWhenStarted) * 1000;
    stat_type acFirstRow = getStatRaw(StWhenFirstRow) * 1000;
    if ((acStarted == 0) && (acFirstRow == 0))
    {
        if (queryThorActivityKind() == TAKlocalresultwrite)
            return inputs[0]->querySource()->getBeginTimestamp();
    }
    return acStarted ? acStarted : acFirstRow;
}

stat_type WuScope::getEndStartTimestamp() const
{
    stat_type acStarted = getStatRaw(StWhenStarted) * 1000;
    stat_type timeStart = getStartTime();
    if (acStarted || timeStart)
        return acStarted + timeStart;
    return getStatRaw(StWhenFirstRow) * 1000;
}

stat_type WuScope::getBeginRunTimestamp() const
{
    stat_type acFirstRow = getStatRaw(StWhenFirstRow) * 1000;
    if (acFirstRow == 0)
    {
        //outputs from libraries do not have begin/end times - so get them from the input activity
        if (queryThorActivityKind() == TAKlocalresultwrite)
            return inputs[0]->querySource()->getBeginRunTimestamp();
        return getBeginTimestamp();
    }
    return acFirstRow;
}

stat_type WuScope::getEndTimestamp() const
{
    stat_type acFirstRow = getStatRaw(StWhenFirstRow) * 1000;
    if (acFirstRow == 0)
    {
        stat_type whenFinished = getStatRaw(StWhenFinished) * 1000;
        if (whenFinished)
            return whenFinished;

        //outputs from libraries do not have begin/end times - so get them from the input activity
        if (queryThorActivityKind() == TAKlocalresultwrite)
            return inputs[0]->querySource()->getEndTimestamp();
        return getEndStartTimestamp();
    }

    stat_type acElapsed = getStatRaw(StTimeElapsed);
    return acFirstRow + acElapsed;
}

stat_type WuScope::getStartTime() const
{
    return getStatRaw(StTimeStart);
}

stat_type WuScope::getLocalStartTime() const
{
    stat_type acStart = getStartTime();
    stat_type inputStart = 0;
    for (auto & input : inputs)
    {
        inputStart += input->querySource()->getStartTime();
    }
    if (acStart > inputStart)
        return acStart - inputStart;
    else
        return 0;
}

stat_type WuScope::getLifetimeNs() const
{
    stat_type endTimestamp = getEndTimestamp();
    if (endTimestamp != 0)
        return endTimestamp - getBeginTimestamp();

    stat_type elapsed = getStatRaw(StTimeElapsed);
    if (!elapsed && (queryScopeType() != SSTactivity))
    {
        for (auto & cur : scopes)
        {
            stat_type childLifetime = cur.getLifetimeNs();
            if (childLifetime > elapsed)
                elapsed = childLifetime;
        }
    }
    return elapsed;
}

void WuScope::walkHotspots(CIArrayOf<WuHotspotResult> & results, stat_type totalTime, const char * parent, bool isRoot, const RoxieOptions & options)
{
    if (recursionCheck == 1)
        return;
    recursionCheck = 1;

    stat_type minTime = options.timeThreshold;
    switch (queryScopeType())
    {
    case SSTnone: // Global
    case SSTworkflow:
    case SSTgraph:
    case SSTsubgraph:
        for (auto & cur : scopes)
        {
            if (cur.getLifetimeNs() >= minTime)
                cur.walkHotspots(results, totalTime, nullptr, isRoot, options);
        }
        return;
    }
    if (!wasExecuted())
        return;

    stat_type myDependTime = getStatRaw(StTimeDependencies);
    stat_type localStartTime = getLocalStartTime();
    stat_type myStartTime = (localStartTime > myDependTime) ? localStartTime - myDependTime : 0;
    stat_type myLocalTime = getStatRaw(StTimeLocalExecute);

    //Only include if the time for the query overlaps the time range
    if ((getBeginTimestamp() <= options.maxTime) && (getEndTimestamp() >= options.minTime))
    {
        bool include = true;

        if (include)
        {
            if (isRoot || (localStartTime >= minTime) || (myLocalTime >= minTime))
            {
                const char * prefix = isRoot ? "R" : " ";
                double totalPercent = isRoot ? 100.0 * (double)getLifetimeNs() / totalTime : 0;
                double startPercent = 100.0 * (double)localStartTime / totalTime;
                double myStartPercent = 100.0 * (double)myStartTime / totalTime;
                double runPercent = 100.0 * (double)myLocalTime / totalTime;

                results.append(*new WuHotspotResult(this, parent, isRoot, totalPercent, startPercent, myStartPercent, runPercent, (localStartTime >= minTime), (myLocalTime >= minTime)));

                if (isRoot)
                    parent = queryName();
            }
        }
    }

    //Cannot short circuit because input may contain a splitter that had already been pulled by another thread
    //which will return results very quickly - likely not on the critical path.
    bool childCouldContainMatch = ((getStartTime() + getStatRaw(StTimeTotalExecute)) > minTime);
    if (!options.onlyCriticalPath || childCouldContainMatch)
    {
        for (auto & input : inputs)
        {
            input->querySource()->walkHotspots(results, totalTime, parent, false, options);
        }

        for (auto & depend : dependencies)
        {
            if (depend->getLifetimeNs() >= minTime)
                depend->walkHotspots(results, totalTime, queryName(), true, options);
        }
    }
}

void WuScope::traceActive(stat_type startTime, stat_type endTime)
{
    stat_type acBegin = getBeginTimestamp();
    stat_type acFirstRow = getStatRaw(StWhenFirstRow) * 1000;
    stat_type acEnd = getEndTimestamp();

    char lowerBound = (acBegin > startTime) ? '[' : ' ';
    char upperBound = (acEnd < endTime) ? ']' : ' ';
    printf("  %s: %c%" I64F "u (%" I64F "u) .. %" I64F "u%c  time %" I64F "u\n", queryName(), lowerBound, acBegin, acFirstRow, acEnd, upperBound, acEnd - acBegin);
}

void WuScope::gatherActive(PointerArrayOf<WuScope> & activities, stat_type startTime, stat_type endTime)
{
    if (queryScopeType() == SSTactivity)
    {
        stat_type acStarted = getStatRaw(StWhenStarted) * 1000;
        stat_type acFirstRow = getStatRaw(StWhenFirstRow) * 1000;
        stat_type acElapsed = getStatRaw(StTimeElapsed);
        stat_type acBegin = acStarted ? acStarted : acFirstRow;
        if (acBegin && acElapsed)
        {
            stat_type acEnd = acFirstRow + acElapsed;

            if ((acEnd >= startTime) && (acBegin <= endTime))
            {
                ThorActivityKind kind = queryThorActivityKind();
                if (isActivitySink(kind) && (kind != TAKspillwrite))
                    activities.append(this);
            }
        }
    }
    else
    {
        for (auto & cur : scopes)
        {
            cur.gatherActive(activities, startTime, endTime);
        }
    }
}

bool WuScope::waitsForDependency(WuScope * search) const
{
    if (!wasExecuted())
        return false;

    stat_type childEndTime = search->getEndTimestamp();
    stat_type beginTime = getBeginTimestamp();
    //If activity begins after dependency ends, it will not be waiting for it
    if (beginTime >= childEndTime)
        return false;

    //Likely to be dependent, but check if a previous dependency overlaps
    stat_type dependencyTime = beginTime + (getStartTime() - getStatRaw(StTimeDependencies));
    for (auto & dependency : dependencies)
    {
        if (dependencyTime >= childEndTime)
            return false;
        if (dependency == search)
            break;
    }

    return true;
}


bool WuScope::waitsForInput(WuScope * output, bool startOnly) const
{
    //Could be true for conditions...
    if (!wasExecuted())
        return false;

    //An output almost always waits for an input.  The exception is where a splitter is being read
    //from multiple outputs, and one output starts or completely reads the contents before the second
    //output starts.
    if (startOnly)
        return output->getBeginTimestamp() < getEndStartTimestamp();

    //How many rows were read by the output?
    stat_type numProcessed = 0;
    for (auto & outputEdge : outputs)
    {
        if (outputEdge->queryTarget() != output)
        {
            numProcessed = outputEdge->getStatRaw(StNumRowsProcessed);
            break;
        }
    }

    for (auto & outputEdge : outputs)
    {
        if (outputEdge->queryTarget() != output)
        {
            //Only check outputs that have processed at least as many rows
            if (outputEdge->getStatRaw(StNumRowsProcessed) >= numProcessed)
            {
                if (outputEdge->queryTarget()->getEndTimestamp() <= output->getBeginTimestamp())
                    return false;
            }
        }
    }
    return true;
}


bool WuScope::wasExecuted() const
{
    return (getLifetimeNs() != 0);
}


//-----------------------------------------------------------------------------------------------------------
/* Callback used to output scope properties as xml */
class StatsGatherer : public IWuScopeVisitor
{
public:
    StatsGatherer(IPropertyTree * _scope, stat_type & _minTimestamp) : scope(_scope), minTimestamp(_minTimestamp) {}

    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & cur) override
    {
        StringBuffer name;
        name.append('@').append(queryStatisticName(kind));
        scope->setPropInt64(name, value);
        if (queryMeasure(kind) == SMeasureTimestampUs)
        {
            if (!minTimestamp || (value < minTimestamp))
                minTimestamp = value;
        }
    }
    virtual void noteAttribute(WuAttr attr, const char * value)
    {
        StringBuffer name;
        name.append('@').append(queryWuAttributeName(attr));
        scope->setProp(name, value);
    }
    virtual void noteHint(const char * kind, const char * value)
    {
        throwUnexpected();
    }
    virtual void noteException(IConstWUException & exception)
    {
        throwUnexpected();
    }
    IPropertyTree * scope;
    stat_type & minTimestamp;
};


//-----------------------------------------------------------------------------------------------------------

WorkunitAnalyserBase::WorkunitAnalyserBase() : root(new WuScope("", nullptr))
{
}

void WorkunitAnalyserBase::analyse(IConstWorkUnit * wu)
{
    WuScopeFilter filter;
    filter.addOutputProperties(PTstatistics).addOutputProperties(PTattributes);
    filter.finishedFilter();
    collateWorkunitStats(wu, filter);
    root->connectActivities();
}

void WorkunitAnalyserBase::collateWorkunitStats(IConstWorkUnit * workunit, const WuScopeFilter & filter)
{
    Owned<IConstWUScopeIterator> iter = &workunit->getScopeIterator(filter);
    ForEach(*iter)
    {
        try
        {
            WuScope * scope = selectFullScope(iter->queryScope());

            StatsGatherer callback(scope->queryAttrs(), minTimestamp);
            scope->queryAttrs()->setPropInt("@stype", iter->getScopeType());
            iter->playProperties(callback);
        }
        catch (IException * e)
        {
            e->Release();
        }
    }
}

WuScope * WorkunitAnalyserBase::selectFullScope(const char * scope)
{
    StringBuffer temp;
    WuScope * resolved = root;
    for (;;)
    {
        if (!*scope)
            return resolved;
        const char * dot = strchr(scope, ':');
        if (!dot)
            return resolved->select(scope);

        temp.clear().append(dot-scope, scope);
        resolved = resolved->select(temp.str());
        scope = dot+1;
    }
}

WuScope * WorkunitAnalyserBase::resolveActivity(const char * name)
{
    WuScope * activity = root->resolve(name, false);
    if (!activity)
        throw MakeStringException(0, "Could not find activity %s", name);
    return activity;
}


//-----------------------------------------------------------------------------------------------------------

WorkunitRuleAnalyser::WorkunitRuleAnalyser()
{
    gatherRules(rules);
}

void WorkunitRuleAnalyser::applyConfig(IPropertyTree *cfg, IConstWorkUnit * wu)
{
    options.applyConfig(cfg);
    options.applyConfig(wu);
}


void WorkunitRuleAnalyser::check(const char * scope, IWuActivity & activity)
{
    if (activity.getStatRaw(StTimeLocalExecute, StMaxX) < options.queryOption(watOptMinInterestingTime))
        return;
    Owned<PerformanceIssue> highestCostIssue;
    ForEachItemIn(i, rules)
    {
        if (rules.item(i).isCandidate(activity))
        {
            Owned<PerformanceIssue> issue (new PerformanceIssue);
            if (rules.item(i).check(*issue, activity, options))
            {
                if (issue->getTimePenalityCost() >= options.queryOption(watOptMinInterestingCost))
                {
                    if (!highestCostIssue || highestCostIssue->getTimePenalityCost() < issue->getTimePenalityCost())
                        highestCostIssue.setown(issue.getClear());
                }
            }
        }
    }
    if (highestCostIssue)
    {
        StringBuffer fullScopeName;
        activity.getFullScopeName(fullScopeName);
        highestCostIssue->setScope(fullScopeName);
        issues.append(*highestCostIssue.getClear());
    }
}

void WorkunitRuleAnalyser::applyRules()
{
    root->applyRules(*this);
    issues.sort(compareIssuesCostOrder);
}

void WorkunitRuleAnalyser::print()
{
    ForEachItemIn(i, issues)
        issues.item(i).print();
}

void WorkunitRuleAnalyser::update(IWorkUnit *wu, double costRate)
{
    ForEachItemIn(i, issues)
        issues.item(i).createException(wu, costRate);
}



//-----------------------------------------------------------------------------------------------------------

void WorkunitStatsAnalyser::applyOptions(IPropertyTree * cfg)
{
    StringBuffer temp;

    opts.onlyActive = cfg->getPropBool("onlyActive", opts.onlyActive);
    if (cfg->getProp("threshold", temp.clear()))
        opts.thresholdPercent = atof(temp);
    opts.onlyCriticalPath = cfg->getPropBool("critical", opts.onlyCriticalPath);
}

void WorkunitStatsAnalyser::adjustTimestamps()
{
    //Adjust the first timestamp to 1 - don't set it to 0 so you can differentiate if it is missing
    root->adjustTimestamps(minTimestamp-1);
}

void WorkunitStatsAnalyser::calcDependencies()
{
    ScopeVector rootActivities;
    root->gatherRootActivities(rootActivities);

    WaThread * rootThread = createThread(nullptr, WaThreadType::Root);
    for (auto & cur : rootActivities)
    {
        processRootActivity(*cur, rootThread, WaThreadType::Parallel);
    }
}

void WorkunitStatsAnalyser::processRootActivity(WuScope & scope, WaThread * thread, WaThreadType type)
{
    scope.noteCall(thread);
    if (!scope.executed)
    {
        assertex(!scope.executionThread);
        WaThread * executionThread = createThread(&scope, type);
        scope.executionThread = executionThread;
        processActivities(scope, executionThread, true);
        scope.started = true;
        processActivities(scope, executionThread, false);
        scope.executed = true;
    }
}

void WorkunitStatsAnalyser::processActivities(WuScope & scope, WaThread * thread, bool isStart)
{
    bool onlyActive = true;
    if (onlyActive && !scope.wasExecuted())
    {
        scope.executed = true;
        return;
    }

    if (isStart)
    {
        if (!scope.started)
        {
            ThorActivityKind kind = scope.queryThorActivityKind();
            if ((kind == TAKsplit) || (kind == TAKspillwrite))
            {
                if (scope.outputs.size() > 1)
                {
                    thread = createThread(&scope, WaThreadType::Splitter);
                    scope.splitThread = thread;
                }
            }

            //Start input activities
            for (auto & input : scope.inputs)
            {
                processActivities(*input->querySource(), thread, true);
            }

            //Run all dependencies for this activity
            for (auto & depend : scope.dependencies)
            {
                processRootActivity(*depend, thread, WaThreadType::Dependency);
                thread->addDependency(depend);
            }
            scope.started = true;
        }
    }
    else
    {
        scope.noteCall(thread);
        if (!scope.executed)
        {
            if (scope.splitThread)
                thread = scope.splitThread;

            for (auto & input : scope.inputs)
            {
                processActivities(*input->querySource(), thread, false);
            }
            scope.executed = true;
        }
    }
}

WaThread * WorkunitStatsAnalyser::createThread(WuScope * creator, WaThreadType type)
{
    WaThread * thread = new WaThread(threads.ordinality(), creator, type);
    threads.append(*thread);
    if (creator)
        creator->noteStarted(thread);
    return thread;
}

static int compareThreadDepth(void * const * pLeft, void * const * pRight)
{
    WaThread * left = (WaThread *)*pLeft;
    WaThread * right = (WaThread *)*pRight;
    return (int)(left->queryDepth() - right->queryDepth());
}

static void gatherPath(PointerArrayOf<WaThread> & path, WuScope * resolved, bool indirect)
{
    resolved->gatherCallers(path);

    //The list of paths will expand as we gather more parent callers.
    for (unsigned i = 0; i < path.ordinality(); i++)
    {
        WuScope * creator = path.item(i)->creator;
        if (creator && (indirect || !creator->isDependency()))
            creator->gatherCallers(path);
    }

    path.sort(compareThreadDepth);
}


class WaActivityPath : public CInterface
{
public:
    WaActivityPath(const char * _id, WuScope * _scope) : id(_id), scope(_scope) {}

    bool isSingleThreadLevel(WaThread * search)
    {
        bool matched = false;
        ForEachItemIn(i, path)
        {
            if (path.item(i)->queryDepth() == search->queryDepth())
            {
                if (matched)
                    return false;
                matched = true;
            }
        }
        return matched;
    }

    bool allLaterPathsBlockedBy(WaThread * search)
    {
        bool matched = false;
        ForEachItemIn(i, path)
        {
            if (path.item(i)->queryDepth() > search->queryDepth())
            {
                if (!path.item(i)->isBlockedBy(search))
                    return false;
            }
        }
        return true;
    }

    void gatherDirectDependencies(PointerArrayOf<WaThread> & direct)
    {
        gatherPath(direct, scope, false);
    }

    unsigned getDirectDependency(WaActivityPath & other)
    {
        ForEachItemIn(j, path)
        {
            WaThread & thread = *path.item(j);
            if (&thread == other.scope->executionThread)
                return j;
            else
            {
                for (auto dependency : other.scope->dependencies)
                {
                    if (dependency->executionThread == &thread)
                        return j;
                }
            }
        }
        return UINT_MAX;
    }

    void trace(WaActivityPath * other, unsigned from)
    {
        PointerArrayOf<WaThread> direct;
        PointerArrayOf<WaThread> otherDirect;
        if (other)
        {
            gatherDirectDependencies(direct);
            other->gatherDirectDependencies(otherDirect);
        }

        for (unsigned j=from; j < path.ordinality(); j++)
        {
            WaThread & thread = *path.item(j);
            const char * creatorName = thread.creator ? thread.creator->queryName() : "<root>";
            //Do all paths include this thread?
            bool isSingle = isSingleThreadLevel(&thread);
            const char * single = isSingle ? "!" : " ";
            StringBuffer dependName;
            const char * marker = " ";
            const char * blocked = " ";
            if (isSingle && allLaterPathsBlockedBy(&thread))
                blocked = "=";

            if (other)
            {
                if (other->path.contains(&thread))
                {
                    marker = "*";
                    unsigned matchFirst = NotFound;
                    unsigned matchSecond = NotFound;

                    //For the current execution thread, which of the threads' dependencies are included in the path
                    PointerArrayOf<WuScope> & dependencies = thread.dependencies;
                    ForEachItemIn(iDep, dependencies)
                    {
                        WuScope * dependency = dependencies.item(iDep);
                        WaThread * thread = dependency->executionThread;
                        if (path.contains(thread) || direct.contains(thread))
                            matchFirst = iDep;
                        if (other->path.contains(thread) || otherDirect.contains(thread))
                            matchSecond = iDep;
                    }

                    if (matchFirst != NotFound)
                    {
                        if (matchSecond != NotFound)
                        {
                            if (matchFirst == matchSecond)
                                dependName = "=";
                            else if (matchFirst < matchSecond)
                                dependName.append("<").append(dependencies.item(matchFirst)->queryName()).append(",").append(dependencies.item(matchSecond)->queryName());
                            else
                                dependName.append(">").append(dependencies.item(matchSecond)->queryName()).append(",").append(dependencies.item(matchFirst)->queryName());
                        }
                        else
                        {
                            dependName.append("<").append(dependencies.item(matchFirst)->queryName());
                        }
                    }
                    else
                    {
                        if (matchSecond != NotFound)
                            dependName.append(">").append(dependencies.item(matchSecond)->queryName());
                        else
                            dependName = " ";
                    }
                }
                else
                {
                    //depends is the list of threads that call dependencies that depend on the 1st activity.
                    //If this thread matches one then that will cause this activity to be dependend on the 1st.
                    ForEachItemIn(iDep, thread.dependencies)
                    {
                        WuScope * dependency = thread.dependencies.item(iDep);
                        if (other->path.contains(dependency->executionThread))
                        {
                            dependName.set(dependency->queryName());
                            break;
                        }
                    }
                }
            }

            printf(" %s%s%s%4s %5s@%u %6s", single, marker, blocked, dependName.str(), thread.queryName(), thread.queryDepth(), creatorName);
            if (thread.creator)
                printf(" {%" I64F "u..%" I64F "u}", thread.creator->getBeginTimestamp(), thread.creator->getEndTimestamp());

            if (thread.creator)
            {
                printf(" [");
                for (auto cur : thread.creator->callers)
                {
                    printf(" %s", cur->queryName());
                    //If the caller
                }
                printf("]");
            }

            if (other)
            {
                if (&thread == other->scope->executionThread)
                    printf(" ****** match ******");
                else
                {
                    for (auto dependency : other->scope->dependencies)
                    {
                        if (dependency->executionThread == &thread)
                        {
                            printf(" ****** Dependency of %s ******", other->scope->queryName());
                        }
                    }
                }
            }


            //If this activity is a dependency, indicate if it is a dependency of the other activity
            printf("\n");
        }
    }
public:
    const char * id;
    WuScope * scope;
    PointerArrayOf<WaThread> path;
};

void WorkunitStatsAnalyser::spotCommonPath(const StringArray & args)
{
    /*
     Spot the common path between the first activity 'X' and each of the following activities 'Y'.

     gather all the path segments that lead to X and Y.
     Report if X is dependent on Y, X should preceed Y etc.
     */
    CIArrayOf<WaActivityPath> extracted;
    ForEachItemIn(i, args)
    {
        const char * arg = args.item(i);
        if (arg[0] != 'a')
            continue;
        WuScope * resolved = root->resolve(arg, false);
        if (!resolved)
            throw MakeStringException(0, "Could not find activity %s", arg);

        WaActivityPath & info = * new WaActivityPath(arg, resolved);
        extracted.append(info);
        gatherPath(info.path, resolved, true);
    }

    if (extracted.empty())
        return;

    printf(
        "Information about 1st activity, and dependence of second activity on the first:\n"
        "Format:  [!][*][=][Dnn] thread@depth activity [callers]\n"
        "!   only path with this depth.\n"
        "*   a common path between the first and second activities\n"
        "=   a path that all subsequent paths go through\n"
        "Dnn a dependency of the first activity on the second\n"
    );

    for (unsigned j=1; j < extracted.ordinality(); j++)
    {
        unsigned direct = extracted.item(0).getDirectDependency(extracted.item(j));
        if (direct != UINT_MAX)
        {
            extracted.item(0).trace(&extracted.item(j), direct);
        }
        else
        {
            extracted.item(0).trace(&extracted.item(j), 0);
            extracted.item(j).trace(&extracted.item(0), 0);
        }
    }
}


static int compareStartEndTime(void * const * pLeft, void * const * pRight)
{
    WuScope * left = (WuScope *)*pLeft;
    WuScope * right = (WuScope *)*pRight;
    stat_type leftBeginTime = left->getBeginTimestamp();
    stat_type rightBeginTime = right->getBeginTimestamp();
    if (leftBeginTime < rightBeginTime)
        return -1;
    if (leftBeginTime > rightBeginTime)
        return +1;
    stat_type leftEndTime = left->getEndTimestamp();
    stat_type rightEndTime = right->getEndTimestamp();
    return (rightEndTime > leftEndTime) ? +1 : (rightEndTime < leftEndTime) ? -1 : 0;
}


void WorkunitStatsAnalyser::findActiveActivities(const StringArray & args)
{
    CIArrayOf<WaActivityPath> extracted;
    ForEachItemIn(i, args)
    {
        const char * arg = args.item(i);
        if (arg[0] != '@')
            continue;
        //Syntax is @start,end
        char * next = nullptr;
        stat_type startTime = strtoul(arg+1, &next, 10) * 1000;
        stat_type endTime = startTime;
        if (next && *next == ',')
            endTime = strtoul(next+1, &next, 10) * 1000;

        if (startTime || endTime)
        {
            printf("%s\n", arg);
            PointerArrayOf<WuScope> activities;
            root->gatherActive(activities, startTime, endTime);
            activities.sort(compareStartEndTime);
            ForEachItemIn(i, activities)
            {
                WuScope * scope = activities.item(i);
                scope->traceActive(startTime, endTime);
            }
        }
    }
}


static int compareLocalTime(void * const * pLeft, void * const * pRight)
{
    WuScope * left = (WuScope *)*pLeft;
    WuScope * right = (WuScope *)*pRight;
    stat_type leftTime = left->getStatRaw(StTimeLocalExecute);
    stat_type rightTime = right->getStatRaw(StTimeLocalExecute);
    return (rightTime > leftTime) ? +1 : (rightTime < leftTime) ? -1 : 0;
}


void WorkunitStatsAnalyser::findHotspotsOld(const StringArray & args)
{
    CIArrayOf<WaActivityPath> extracted;
    ForEachItemIn(i, args)
    {
        const char * arg = args.item(i);
        if (arg[0] != 'H')
            continue;
        //Syntax is H<activity>[:n]
        arg++;
        const char * colon = strchr(arg, ':');
        unsigned max = 10;
        StringBuffer activity;
        if (colon)
        {
            activity.append(colon-arg, arg);
            max = strtoul(colon+1, nullptr, 10);
        }
        else
            activity.append(arg);

        WuScope * resolved = root->resolve(activity, false);
        if (!resolved)
            throw MakeStringException(0, "Could not find activity %s", activity.str());

        PointerArrayOf<WuScope> activities;
        resolved->gatherSelfAndInputs(activities);
        activities.sort(compareLocalTime);
        if (max > activities.ordinality())
            max = activities.ordinality();

        printf("%s\n", arg);
        for (unsigned i=0; i < max; i++)
        {
            WuScope * cur = activities.item(i);
            printf("  %s %" I64F "u\n", cur->queryName(), cur->getStatRaw(StTimeLocalExecute));
        }
    }
}


static int compareHotspots(CInterface * const * pLeft, CInterface * const * pRight)
{
    WuHotspotResult * left = static_cast<WuHotspotResult *>(*pLeft);
    WuHotspotResult * right = static_cast<WuHotspotResult *>(*pRight);
    return left->compareTime(*right);
}

static int compareHotspotStartTime(CInterface * const * pLeft, CInterface * const * pRight)
{
    WuHotspotResult * left = static_cast<WuHotspotResult *>(*pLeft);
    WuHotspotResult * right = static_cast<WuHotspotResult *>(*pRight);
    return left->compareStart(*right);
}


void WorkunitStatsAnalyser::reportActivity(const StringArray & args)
{
    stat_type minTime = (stat_type)-1;
    stat_type maxTime = 0;
    ScopeVector rootActivities;
    root->gatherRootActivities(rootActivities);

    for (auto scope : rootActivities)
    {
        if (scope->wasExecuted())
        {
            stat_type thisMin = scope->getBeginTimestamp();
            stat_type thisMax = scope->getEndTimestamp();
            if (minTime > thisMin)
                minTime = thisMin;
            if (maxTime < thisMax)
                maxTime = thisMax;
        }
    }

    WuScope * rootActivity = nullptr;
    WuScope * searchActivity = nullptr;

    ForEachItemIn(i, args)
    {
        const char * arg = args.item(i);
        if (arg[0] == '>')
        {
            if (arg[1] == 'a')
            {
                rootActivity = resolveActivity(arg+1);
                minTime = rootActivity->getBeginTimestamp();
            }
            else
                minTime = strtoll(arg+1, nullptr, 10);
        }
        else if (arg[0] == '<')
        {
            if (arg[1] == 'a')
            {
                searchActivity = resolveActivity(arg+1);
                maxTime = searchActivity->getBeginTimestamp();
            }
            else
                maxTime = strtoll(arg+1, nullptr, 10);
        }
    }

    //Note the activities that are reported in this, often don't overlap with the critical path - if the critical path
    //is a direct input to the root activity.
    stat_type totalTime = (maxTime - minTime);
    stat_type interesting = (stat_type)(totalTime * opts.thresholdPercent / 100.0);
    printf("Activity for (%llu..%llu %.2f%%):\n", minTime, maxTime, opts.thresholdPercent);
    printf("name {startTimeRange executeTimeRange} [R](parent total%% start%% run%%) type\n");

    opts.timeThreshold = interesting;
    opts.minTime = minTime;
    opts.maxTime = maxTime;
    if (searchActivity)
    {
        //Gather and tag the critical path for this activity, so it can be highlighted
        StringArray paths;
        searchActivity->gatherCriticalPaths(paths, nullptr, nullptr, 0, 0, opts);
        gatherPath(opts.threadFilter, searchActivity, true);
    }

    CIArrayOf<WuHotspotResult> results;
    if (rootActivity)
    {
        rootActivity->walkHotspots(results, totalTime, "", true, opts);
    }
    else
    {
        for (auto activity : rootActivities)
            activity->walkHotspots(results, totalTime, "", true, opts);
    }

    results.sort(compareHotspotStartTime);
    ForEachItemIn(iRes, results)
        results.item(iRes).reportTime();
}

void WorkunitStatsAnalyser::findHotspots(const char * rootScope, stat_type & totalTime, CIArrayOf<WuHotspotResult> & results)
{
    WuScope * activity = nullptr;
    if (!isEmptyString(rootScope))
    {
        activity = resolveActivity(rootScope);
        if (!activity)
            throw MakeStringException(0, "Could not find activity %s", rootScope);
    }

    if (!activity)
        activity = root;
    if (!activity)
        return;

    totalTime = activity->getLifetimeNs();

    stat_type interesting = (stat_type)(totalTime * opts.thresholdPercent / 100.0);
    opts.timeThreshold = interesting;
    activity->walkHotspots(results, totalTime, "", true, opts);
    results.sort(compareHotspots);
}

WuScope * WorkunitStatsAnalyser::queryLongestRootActivity() const
{
    ScopeVector rootActivities;
    root->gatherRootActivities(rootActivities);

    WuScope * best = nullptr;
    for (auto scope : rootActivities)
    {
        if (!best || scope->getLifetimeNs() > best->getLifetimeNs())
            best = scope;
    }
    return best;
}

void WorkunitStatsAnalyser::walkStartupTimes(const StringArray & args)
{
    ForEachItemIn(i, args)
    {
        const char * arg = args.item(i);
        if (arg[0] != 'S')
            continue;
        //Syntax is S<activity>
        WuScope * resolved = resolveActivity(arg+1);

        printf("%s\n", arg);
        printf("  activity when time\n");
        resolved->walkStartupTimes(resolved->getBeginTimestamp());
    }
}

void WorkunitStatsAnalyser::traceWhyWaiting(const StringArray & args)
{
    CIArrayOf<WaActivityPath> extracted;
    ForEachItemIn(i, args)
    {
        const char * arg = args.item(i);
        if (arg[0] != '?')
            continue;

        //Syntax is ?<activity>:startTime
        stat_type startTime = 0;
        StringBuffer activity;
        arg++;
        const char * colon = strchr(arg, ':');
        if (colon)
        {
            activity.append(colon-arg, arg);
            startTime = strtoul(colon+1, nullptr, 10) * 1000;
        }
        else
            activity.append(arg);

        WuScope * resolved = resolveActivity(activity);
        stat_type beginTime = resolved->getBeginTimestamp();
        stat_type interesting = (beginTime - startTime) / 400;
        printf("%s [%llu..%llu = %llu]\n", arg, startTime, beginTime, interesting);
        printf("  activity when time\n");
        PointerArrayOf<WuScope> visited;
        resolved->showLargeDependencies(visited, startTime, beginTime, interesting);
    }
}

void WorkunitStatsAnalyser::traceCriticalPaths(const StringArray & args)
{
    /*
     What is the critical path(s) of activities that are executed in ordered to execute a particular activity
     */
    ForEachItemIn(i, args)
    {
        const char * arg = args.item(i);
        if (arg[0] != 'a')
            continue;

        root->resetState();
        WuScope * resolved = resolveActivity(arg);
        StringArray paths;
        resolved->gatherCriticalPaths(paths, nullptr, nullptr, resolved->getEndTimestamp(), 0, opts);
        printf("Critical path for %s:\n", arg);
        ForEachItemIn(i, paths)
            puts(paths.item(i));
    }
}

void WorkunitStatsAnalyser::traceDependencies()
{
    unsigned mask = 1U | (1U << SSTworkflow) | (1U << SSTgraph) | (1U << SSTsubgraph) | (1U << SSTactivity);
    root->trace(0, true, 5, mask);
}

//---------------------------------------------------------------------------------------------------------------------

void WUANALYSIS_API analyseWorkunit(IWorkUnit * wu, IPropertyTree *options, double costPerMs)
{
    WorkunitRuleAnalyser analyser;
    analyser.applyConfig(options, wu);
    analyser.analyse(wu);
    analyser.applyRules();
    analyser.update(wu, costPerMs);
}

void WUANALYSIS_API analyseAndPrintIssues(IConstWorkUnit * wu, double costRate, bool updatewu)
{
    WorkunitRuleAnalyser analyser;
    analyser.applyConfig(nullptr, wu);
    analyser.analyse(wu);
    analyser.applyRules();
    analyser.print();
    if (updatewu)
    {
        Owned<IWorkUnit> lockedwu = &(wu->lock());
        lockedwu->clearExceptions("Workunit Analyzer");
        analyser.update(lockedwu, costRate);
    }
}

/*
Syntax:
aX aY               what are the common activities between aX and aY
@start..end         which activities are active between start and end
H<activity>[:n]     Display the top <n> local times that are an input to a given activity (ignore dependents)
*/

void analyseActivity(IConstWorkUnit * wu, IPropertyTree * cfg, const StringArray & args)
{
    WorkunitStatsAnalyser analyser;
    analyser.applyOptions(cfg);
    analyser.analyse(wu);
    analyser.adjustTimestamps();
    analyser.reportActivity(args);
}

void analyseDependencies(IConstWorkUnit * wu, IPropertyTree * cfg, const StringArray & args)
{
    WorkunitStatsAnalyser analyser;
    analyser.applyOptions(cfg);
    analyser.analyse(wu);
    analyser.adjustTimestamps();
    analyser.calcDependencies();
    analyser.spotCommonPath(args);
    analyser.findActiveActivities(args);
    analyser.findHotspotsOld(args);
    analyser.walkStartupTimes(args);
    analyser.traceWhyWaiting(args);     // ?<activity>:startTime.  What dependencies are there of the threads that call activity that could be causing the delay in loading.
}

void analyseOutputDependencyGraph(IConstWorkUnit * wu, IPropertyTree * cfg)
{
    WorkunitStatsAnalyser analyser;
    analyser.applyOptions(cfg);
    analyser.analyse(wu);
    analyser.adjustTimestamps();
    analyser.calcDependencies();
    analyser.traceDependencies();
}

void analyseCriticalPath(IConstWorkUnit * wu, IPropertyTree * cfg, const StringArray & args)
{
    WorkunitStatsAnalyser analyser;
    analyser.applyOptions(cfg);
    analyser.analyse(wu);
    analyser.adjustTimestamps();
    analyser.calcDependencies();
    analyser.traceCriticalPaths(args);
}

void analyseHotspots(IConstWorkUnit * wu, IPropertyTree * cfg, const StringArray & args)
{
    WorkunitStatsAnalyser analyser;
    analyser.applyOptions(cfg);
    analyser.analyse(wu);

    const char * rootScope = nullptr;
    if (args.ordinality())
        rootScope = args.item(0);

    stat_type totalTime = 0;
    CIArrayOf<WuHotspotResult> results;
    analyser.findHotspots(rootScope, totalTime, results);

    printf("Hotspots for %s (%lluns %.2f%%):\n", rootScope ? rootScope : "<wu>", totalTime, analyser.getThresholdPercent());
    printf("name [R](sink  total%%, start%%, mysta%%,   run%%)\n");
    ForEachItemIn(iRes, results)
        results.item(iRes).report();
}

void analyseHotspots(WuHotspotResults & results, IConstWorkUnit * wu, IPropertyTree * cfg)
{
    WorkunitStatsAnalyser analyser;
    analyser.applyOptions(cfg);
    analyser.analyse(wu);

    analyser.findHotspots(cfg->queryProp("@rootScope"), results.totalTime, results.hotspots);
    results.root.setown(analyser.getRootScope());
}

/*
Useful things to know:

Using wutool to investigate query performance.

- If using a workunit that was generated to store the stats, then use that workunit. E.g.
  wutool <command> wuid daliserver=<ip>

- If using a file generated as a result of control:querystats, use that file.  If it was generated by 8.6.x it needs to be edited in the following way:
  Add <Graphs>...</Graphs> around all the <Graph> tags.
  Change <Graph id='graphX'> to <Graph name='graphX' wfid="1">

  wutool <command> file daliserver=

1. Where is the time spent for this query

wutool hotspot file daliserver=

This displays a table like the following:

name [R](sink  total%, start%,  mysta%   run%)
a576    (a4239    0.00%,  0.04%,  0.04%, 16.51%) keyedjoin
a831    (a4239    0.00%, 15.48%,  0.00%,  0.00%) childif
a2007   (a4239    0.00%, 13.85%, 13.85%,  0.00%) librarycall
a4239  R(       100.00%,  0.00%,  0.00%,  0.00%) localresultwrite

R is included if this activity is the root of a dependency
total is the % of the time spent exectuing this activity (for sinks)
start is the % of the time spent in the on start processing - dependencies and time for this activity
mysta[rt] is the % time spend in time excluding dependencies
run is the % time spent in the on executing this activity, excluding time taken by inputs.

2. Where is the time spend for a particular activity?

wutool hotspot file daliserver= a12345

3. What is the critical path preventing a particular activity from starting?

wutool critical file daliserver= a970

The output is similar to the following:

Critical path for a970:
a970  {297499000..495368212}
 a972  {297271000..495367553}
  a973  {297560000..494056445}
   a975 R{297559000..494870583}
    a1005 R{297559000..495297543}
     a1094  {297181000..495438703}
      a1095  {359128000..623444259}
       a3508 R{359005000..595397308}
        a10565  {43642000..643648100}
         a10588  {46045000..643622451}
          a10603 R{46044000..643617963}
           a10652 P{43643000..644279236}
         a10604 P{43640000..643652174}
      a10413  {44667000..501101425}
       a10629  {43829000..501102365}
        a10645  {43681000..643726642}
         a10648 P{43680000..501106589}
  a1094  {297181000..495438703}....

The outout shows the key activities that use the output of that activity.
The first entry is the activity, indented by the number of steps from the search activity
There is a R before the brackets if the activity is a sink for a dependency
There is a P before the brackets if the activity is a sinke that is executed in parallel at the root of the graph.
The numbers in brackets are a range of times that activity was active in ns relative to the start of the query
An entry ending in ... indicates it is an alternative  path to an activity that has already been reported.

4. Extract useful executuion representation of the graphs

wutool depend file daliserver=

The activities have annotations that indicate the excecution paths:

               activity: "a30" (a29 ->) [thread(P1:1 R0:7 D14:1 *D5 $D5@4)] {

 It starts with a list of "threads" that the activity would have been executed on, with the sequence number on that thread following the colon.  After the '*' is a list if child threads for this activity (e.g. there may be several for an unordered funnel).  After the '$' is the name of a thead started by a sink, with the depth of the activity following the @

 In this case it is execute 3 ways
 - R0:7.  This indicates it is executed as a global (root) sink.
 - D14:1  This is the 1st activity executed when executing the activity that starts thread "D14".  (Search for $D14@)
          activity: "a58" (a57 ->) [thread(P13:1 R0:9 *D14 $D14@2)] {
 - P1:1   This is the 1st activity
          activity: "a36" (a35 ->) [thread(R0:1 S10:1 *P1 $P1@3)] {

5. Show the dependencies and common paths between two activities

wutool depend file daliserver= aX aY

6. Why is an activity waiting?

wutool depend file daliserver= ?aNNN[:startTime]

7. What activities were active in a range of times:

wutool depend file daliserver= @start-us,end-us

8. What are the interesting activities executed between a pair of activities/times.

wutool activity file daliserver= ">[activity|time]" "<[activity|time]" threshold=<percent>

If source activities specified then only activities with that thread as a path are included
If target activity specified then only predecessors for that path are included.
Only report activities that take up >= threshold% of the time.



Investigating a query:

a. Run wutool hostpot
b. For significant library calls, get timings for those libraries and repeat
c. For any soapcalls with large latencies, investigate if they start late
   wutool critical <activity>
d. [Why] is an activity waiting for another activity?
   wutool depend <other-activity> <main-activity>


Useful eclwatch functionality
- For a range of times or activities, highlight which activities are
  i) starting
  ii) active
  Allow a threshold for a percentage to be interesting.

- If the time range can be moved through the range it gives a picture of the execution order
- For range (0..activity->beginTime), the activities that are starting gives a picture of possible delay causes.

*/
