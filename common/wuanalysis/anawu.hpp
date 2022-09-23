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

#ifndef ANAWU_HPP
#define ANAWU_HPP

#ifdef WUANALYSIS_EXPORTS
    #define WUANALYSIS_API DECL_EXPORT
#else
    #define WUANALYSIS_API DECL_IMPORT
#endif

#include "anacommon.hpp"

void WUANALYSIS_API analyseWorkunit(IWorkUnit * wu, IPropertyTree *options, double costPerMs);
void WUANALYSIS_API analyseAndPrintIssues(IConstWorkUnit * wu, double costPerMs, bool updatewu);

//---------------------------------------------------------------------------------------------------------------------

class WuScope;
class WaThread;

class WuScopeHashTable : public SuperHashTableOf<WuScope, const char>
{
public:
    ~WuScopeHashTable() { _releaseAll(); }

    void addNew(WuScope * newWuScope) { SuperHashTableOf::addNew(newWuScope);}
    virtual void     onAdd(void *et) {};
    virtual void     onRemove(void *et);
    virtual unsigned getHashFromElement(const void *et) const;
    virtual unsigned getHashFromFindParam(const void *fp) const;
    virtual const void * getFindParam(const void *et) const;
    virtual bool matchesFindParam(const void *et, const void *key, unsigned fphash) const;
    virtual bool matchesElement(const void *et, const void *searchET) const;
};

inline unsigned hashScope(const char * name) { return hashcz((const byte *)name, 0); }

/*
The following is a logical thread of execution.  They are created in the following situations
- At a point where code can be executed in parallel.  Root graph, file append, prefetch.
- Any point which forces sequential execution. Examples are dependencies and spliter inputs.
- A conditional output

*/

enum class WaThreadType
{
    Root,
    Parallel,
    Sequential,
    Splitter,
    Dependency,
    Max
};
static constexpr char ThreadPrefix[(unsigned)WaThreadType::Max] = { 'R', 'P', 'Q', 'S', 'D' };

class WaThread : public CInterface
{
public:
    WaThread(unsigned _id, WuScope * _creator, WaThreadType _type) : creator(_creator), type(_type)
    {
        name.appendf("%c%d", ThreadPrefix[(unsigned)type], _id);
    }

    void addDependency(WuScope * dependency) { dependencies.append(dependency); }
    void expandAllDependencies(StringBuffer & result);
    void extractAllDependencies(PointerArrayOf<WaThread> & result);
    bool isBlockedBy(WaThread * thread);
    unsigned queryDepth() const;
    const char * queryName() const { return name.str(); }

public:
    StringBuffer name;
    PointerArrayOf<WuScope> dependencies;
    std::vector<WuScope *> activities;
    WuScope * creator;
    unsigned sequence = 0;
    WaThreadType type;
};

class WuHotspotResult : public CInterface
{
public:
    WuHotspotResult(WuScope * _activity, const char * _sink, bool _isRoot, double _totalPercent, double _startPercent, double _myStartPercent, double _runPercent, bool _startSignificant, bool _runSignificant);

    int compareTime(const WuHotspotResult & other) const;
    int compareStart(const WuHotspotResult & other) const;
    void report();
    void reportTime();

public:
    Linked<WuScope> activity;
    StringAttr sink;
    bool isRoot;
    bool startSignificant;
    bool runSignificant;
    double totalPercent;
    double startPercent;
    double myStartPercent;
    double runPercent;
};

struct WuHotspotResults
{
public:
    stat_type totalTime = 0;
    CIArrayOf<WuHotspotResult> hotspots;

    Owned<WuScope> root;            // To ensure scopes are not freed prematurely
};

class WuAnalyserOptions;
class RoxieOptions;
class WorkunitRuleAnalyser;
class WorkunitStatsAnalyser;

using ScopeVector = std::vector<WuScope *>;
class WuScope : public CInterface, implements IWuEdge, implements IWuActivity
{
    friend class WorkunitStatsAnalyser;
    friend class WaActivityPath;
    friend class WaThread;
public:
    WuScope(const char * _name, WuScope * _parent) : name(_name), parent(_parent)
    {
        attrs.setown(createPTree());
    }

//interface IWuScope
    virtual stat_type getStatRaw(StatisticKind kind, StatisticKind variant = StKindNone) const override;
    virtual unsigned getAttr(WuAttr kind) const override;
    virtual void getAttr(StringBuffer & result, WuAttr kind) const override;
    virtual const char * getFullScopeName(StringBuffer & fullScopeName) const override;

//interface IWuEdge
    virtual WuScope * querySource() override;
    virtual WuScope * queryTarget() override;

//interface IWuActivity
    virtual const char * queryName() const override { return name; }
    virtual IWuEdge * queryInput(unsigned idx) override { return (idx < inputs.size()) ? inputs[idx]:nullptr; }
    virtual IWuEdge * queryOutput(unsigned idx) override { return (idx < outputs.size()) ? outputs[idx]:nullptr; }

//primary processing functions
    void applyRules(WorkunitRuleAnalyser & analyser);
    void connectActivities();
    void trace(unsigned indent, bool skipEdges, unsigned maxLevels=UINT_MAX, unsigned scopeMask=0) const;

// Dependency tracking functions
    void extractAllDependencies(PointerArrayOf<WaThread> & result);
    void gatherRootActivities(ScopeVector & result) const;
    void gatherCallers(PointerArrayOf<WaThread> & path);
    void noteCall(WaThread * thead);
    void noteStarted(WaThread * thead);
    void resetState();

// creation helper functions
    void addDependency(WuScope * scope);
    void addDependent(WuScope * scope);
    void adjustTimestamps(stat_type minTimestamp);
    void gatherSelfAndInputs(PointerArrayOf<WuScope> & activities);
    void walkStartupTimes(stat_type minTime);
    void showLargeDependencies(PointerArrayOf<WuScope> & visited, stat_type startTime, stat_type searchTime, stat_type interesting);
    bool gatherCriticalPaths(StringArray & paths, WaThread * childThread, WuScope * childDependency, stat_type maxDependantTime, unsigned indent, RoxieOptions & options);

    bool isBlockedBy(WaThread * thread);
    bool isRootActivity() const;
    unsigned queryDepth() const;
    WuScope * resolve(const char * scope, bool createMissing); // return match, including search in children
    WuScope * select(const char * scope);  // Returns matching wuScope (create if no pre-existing)
    void setInput(unsigned i, WuScope * scope);  // Save i'th target in inputs
    void setOutput(unsigned i, WuScope * scope); // Save i'th source in output
    void traceActive(stat_type startTime, stat_type endTime);
    void gatherActive(PointerArrayOf<WuScope> & activities, stat_type startTime, stat_type endTime);
    void walkHotspots(CIArrayOf<WuHotspotResult> & results, stat_type totalTime, const char * parent, bool isRoot, const RoxieOptions & options);
    stat_type getBeginTimestamp() const;
    stat_type getEndStartTimestamp() const;
    stat_type getBeginRunTimestamp() const;
    stat_type getEndTimestamp() const;

    stat_type getStartTime() const;
    stat_type getLocalStartTime() const;
    stat_type getLifetimeNs() const;

    bool mustExecuteInternalSink() const;
    bool isInternalSequence() const;

    bool isRoot() const;
    bool isSink() const;
    bool isDependency() const { return executionThread != nullptr; }
    bool isOnCriticalPath() const { return onCriticalPath; }
    unsigned numParents() const;
    inline IPropertyTree * queryAttrs() const { return attrs; }
    StatisticScopeType queryScopeType() const { return (StatisticScopeType)attrs->getPropInt("@stype");}
    const char * queryScopeTypeName() const { return ::queryScopeTypeName(queryScopeType()); }
    bool waitsForDependency(WuScope * child) const;
    bool waitsForInput(WuScope * child, bool startOnly) const;
    bool wasExecuted() const;


protected:
    StringAttr name;
    WuScope * parent = nullptr;
    Owned<IPropertyTree> attrs;
    WuScopeHashTable scopes;
    std::vector<WuScope *> inputs;
    std::vector<WuScope *> outputs;
    std::vector<WuScope *> dependencies;
    std::vector<WuScope *> dependents;
    std::vector<WaThread *> callers;
    std::vector<unsigned> callerSequence;
    std::vector<WaThread *> childThreads;
    WaThread * executionThread = nullptr; // For an action (including dependency) which thread was this executed on
    WaThread * splitThread = nullptr; // For a splitter - which pseudo-thread is used for the children?
    mutable unsigned depth = 0;
    WaThread * lastBlockedCheck = nullptr;
    unsigned recursionCheck = 0;
    bool started = false;
    bool executed = false;
    bool cachedBlocked = false;
    bool onCriticalPath = false;
    bool checkedCriticalPath = false;
};


//---------------------------------------------------------------------------------------------------------------------

void WUANALYSIS_API analyseActivity(IConstWorkUnit * wu, IPropertyTree * cfg, const StringArray & args);
void WUANALYSIS_API analyseDependencies(IConstWorkUnit * wu, IPropertyTree * cfg, const StringArray & args);
void WUANALYSIS_API analyseCriticalPath(IConstWorkUnit * wu, IPropertyTree * cfg, const StringArray & args);
void WUANALYSIS_API analyseHotspots(IConstWorkUnit * wu, IPropertyTree * cfg, const StringArray & args);
void WUANALYSIS_API analyseHotspots(WuHotspotResults & results, IConstWorkUnit * wu, IPropertyTree * cfg);
void WUANALYSIS_API analyseOutputDependencyGraph(IConstWorkUnit * wu, IPropertyTree * cfg);

#endif
