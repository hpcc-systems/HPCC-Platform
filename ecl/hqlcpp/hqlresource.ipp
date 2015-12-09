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
#ifndef __HQLRESOURCE_IPP_
#define __HQLRESOURCE_IPP_

#include "hqlresource.hpp"

//#define TRACE_BALANCED

enum ResourceType { 
//Slave specific
    RESslavememory,
    RESslavesocket,
//Master specific
    RESmastermemory,
    RESmastersocket,
//General
    REShashdist, 
    RESheavy,
    RESactivities,
    RESmax
};

class CResourceOptions
{
public:
    CResourceOptions(ClusterType _targetClusterType, unsigned _clusterSize, const HqlCppOptions & _translatorOptions, UniqueSequenceCounter & _spillSequence);

    IHqlExpression * createResultSpillName();
    IHqlExpression * createDiskSpillName();
    IHqlExpression * createGlobalSpillName();

    void noteGraphsChanged() { state.updateSequence++; }
    void setChildQuery(bool value);
    void setNewChildQuery(IHqlExpression * graphIdExpr, unsigned numResults);
    void setUseGraphResults(bool _useGraphResults)
    {
        useGraphResults = _useGraphResults;
    }
    bool useGraphResult(bool linkedFromChild);
    bool useGlobalResult(bool linkedFromChild);

public:
    UniqueSequenceCounter & spillSequence;
    unsigned filteredSpillThreshold;
    unsigned minimizeSpillSize;
    bool     allowThroughSpill;
    bool     allowThroughResult;
    bool     cloneFilteredIndex;
    bool     spillSharedConditionals;
    bool     shareDontExpand;
    bool     useGraphResults;
    bool     noConditionalLinks;
    bool     minimiseSpills;
    bool     hoistResourced;
    bool     isChildQuery;
    bool     groupedChildIterators;
    bool     allowSplitBetweenSubGraphs;
    bool     preventKeyedSplit;
    bool     preventSteppedSplit;
    bool     minimizeSkewBeforeSpill;
    bool     expandSingleConstRow;
    bool     createSpillAsDataset;
    bool     optimizeSharedInputs;
    bool     combineSiblings;
    bool     actionLinkInNewGraph;
    bool     convertCompoundToExecuteWhen;
    bool     useResultsForChildSpills;
    bool     alwaysUseGraphResults;
    bool     newBalancedSpotter;
    bool     spillMultiCondition;
    bool     spotThroughAggregate;
    bool     alwaysReuseGlobalSpills;

    IHqlExpression * graphIdExpr;
    unsigned nextResult;
    unsigned clusterSize;
    ClusterType targetClusterType;

    //Used
    struct
    {
        unsigned updateSequence;
    } state;

    inline bool canSplit() const            { return targetClusterType != HThorCluster; }
    inline bool checkResources() const      { return isThorCluster(targetClusterType) && !isChildQuery; }
    inline bool targetRoxie() const         { return targetClusterType == RoxieCluster; }
    inline bool targetThor() const          { return targetClusterType == ThorLCRCluster; }
};

struct CResources : public CInterface
{
public:
    CResources(unsigned _clusterSize)   { clusterSize = _clusterSize; _clear(resource); }
    CResources(const CResources & other)    { clusterSize = other.clusterSize; set(other); }

    void add(const CResources & other);
    bool addExceeds(const CResources & other, const CResources & limit) const;
    void clear()                                            { _clear(resource); }
    bool exceeds(const CResources & limit) const;
    StringBuffer & getExceedsReason(StringBuffer & reasonText, const CResources & other, const CResources & limit) const;
    void maximize(const CResources & other);
    CResources & set(ResourceType kind, unsigned value)     { resource[kind] = value; return *this; }
    CResources & set(const CResources & other)              { memcpy(&resource, &other.resource, sizeof(resource)); return *this; }
    CResources & setLightweight();
    CResources & setHeavyweight();
    CResources & setManyToManySockets(unsigned num);
    CResources & setManyToMasterSockets(unsigned num);
    void sub(const CResources & other);

public:
    unsigned resource[RESmax];
    unsigned clusterSize;
};

enum LinkKind { UnconditionalLink, SequenceLink };

class ResourceGraphInfo;
class ResourceGraphLink : public CInterface
{
public:
    ResourceGraphLink(ResourceGraphInfo * _sourceGraph, IHqlExpression * _sourceNode, ResourceGraphInfo * _sinkGraph, IHqlExpression * _sinkNode, LinkKind _kind);
    ~ResourceGraphLink();

    virtual void changeSourceGraph(ResourceGraphInfo * newGraph);
    virtual void changeSinkGraph(ResourceGraphInfo * newGraph);
    virtual bool isRedundantLink();
    virtual bool isDependency() const { return false; }
    virtual IHqlExpression * queryDependency() const { return NULL; }

protected:
    void trace(const char * name);

public:
    Owned<ResourceGraphInfo> sourceGraph;
    Owned<ResourceGraphInfo> sinkGraph;
    OwnedHqlExpr sourceNode;
    OwnedHqlExpr sinkNode;
    byte linkKind;
};

class ResourceGraphDependencyLink : public ResourceGraphLink
{
public:
    ResourceGraphDependencyLink(ResourceGraphInfo * _sourceGraph, IHqlExpression * _sourceNode, ResourceGraphInfo * _sinkGraph, IHqlExpression * _sinkNode, IHqlExpression * _dependency);

    virtual void changeSourceGraph(ResourceGraphInfo * newGraph);
    virtual void changeSinkGraph(ResourceGraphInfo * newGraph);
    virtual bool isRedundantLink()          { return false; }
    virtual bool isDependency() const { return true; }
    virtual IHqlExpression * queryDependency() const { return dependency; }

protected:
    LinkedHqlExpr dependency;
};

typedef CIArrayOf<ResourceGraphInfo> ResourceGraphArray;
typedef CICopyArrayOf<ResourceGraphLink> GraphLinkArray;

class ResourceGraphInfo : public CInterface
{
public:
    ResourceGraphInfo(CResourceOptions * _options);
    ~ResourceGraphInfo();

    bool addCondition(IHqlExpression * condition);
    bool allocateResources(const CResources & value, const CResources & limit);
    bool canMergeActionAsSibling(bool sequential) const;
    bool containsActiveSinks() const;
    bool containsActionSink() const;
    unsigned getDepth();
    void getMergeFailReason(StringBuffer & reasonText, ResourceGraphInfo * otherGraph, const CResources & limit);
    bool hasSameConditions(ResourceGraphInfo & other);
    bool isDependentOn(ResourceGraphInfo & other, bool allowDirect);
    bool isVeryCheap();
    bool mergeInSibling(ResourceGraphInfo & other, const CResources & limit);
    bool mergeInSource(ResourceGraphInfo & other, const CResources & limit, bool ignoreConditional);
    void removeResources(const CResources & value);

    bool isSharedInput(IHqlExpression * expr);
    void addSharedInput(IHqlExpression * expr, IHqlExpression * mapped);
    IHqlExpression * queryMappedSharedInput(IHqlExpression * expr);

protected:
    void display();
    void mergeGraph(ResourceGraphInfo & other, bool mergeConditions);
    bool evalDependentOn(ResourceGraphInfo & other, bool ignoreSources);

public:
    OwnedHqlExpr createdGraph;
    CResourceOptions * options;
    GraphLinkArray dependsOn;           // NB: These do not link....
    GraphLinkArray sources;
    GraphLinkArray sinks;
    HqlExprArray conditions;
    HqlExprArray sharedInputs;
    HqlExprArray unbalancedExternalSources;
    HqlExprArray balancedExternalSources;
    CResources resources;
    unsigned depth;
    unsigned depthSequence;
    bool beenResourced:1;
    bool isUnconditional:1;
    bool mergedConditionSource:1;
    bool hasConditionSource:1;
    bool hasSequentialSource:1;
    bool hasRootActivity:1;
    bool isDead:1;
    bool startedGeneratingResourced:1;
    bool inheritedExpandedDependencies:1;
    struct
    {
        ResourceGraphInfo * other;
        unsigned updateSequence;
        bool ignoreSources;
        bool value;
    } cachedDependent;
};


class CChildDependent : public CInterface
{
public:
    CChildDependent(IHqlExpression * _original, IHqlExpression * _hoisted, bool _alwaysHoist, bool _isSingleNode, bool _forceHoist)
    : original(_original), hoisted(_hoisted), alwaysHoist(_alwaysHoist), isSingleNode(_isSingleNode), forceHoist(_forceHoist)
    {
        projectedHoisted.set(hoisted);
        projected = NULL;
    }

public:
    IHqlExpression * original;
    LinkedHqlExpr hoisted;
    LinkedHqlExpr projectedHoisted;
    bool forceHoist;
    bool alwaysHoist;
    bool isSingleNode;
    IHqlExpression * projected;
};

class ChildDependentArray : public CIArrayOf<CChildDependent>
{
public:
    unsigned findOriginal(IHqlExpression * expr);
};

class CSplitterLink : public CInterface
{
public:
    CSplitterLink(IHqlExpression * _source, IHqlExpression * _sink) : source(_source->queryBody()), sink(_sink->queryBody())
    {
    }

    IHqlExpression * queryOther(IHqlExpression * expr) const
    {
        if (expr->queryBody() == source)
            return sink;
        else
            return source;
    }

    bool hasSink(IHqlExpression * expr) const { return sink == expr->queryBody(); }
    bool hasSource(IHqlExpression * expr) const { return source == expr->queryBody(); }
    IHqlExpression * querySource() const { return source; }
    IHqlExpression * querySink() const { return sink; }

    void mergeSinkLink(CSplitterLink & sinkLink);

private:
    LinkedHqlExpr source;
    LinkedHqlExpr sink;
};

class EclResourcer;
class CSplitterInfo
{
public:
    CSplitterInfo(EclResourcer & _resourcer, bool _preserveBalanced, bool _ignoreExternalDependencies);
    ~CSplitterInfo();

    void addLink(IHqlExpression * source, IHqlExpression * sink, bool isExternal);
    static bool allInputsPulledIndependently(IHqlExpression * expr);
    void gatherPotentialSplitters(IHqlExpression * expr, IHqlExpression * sink, ResourceGraphInfo * graph, bool isDependency);
    bool isSplitOrBranch(IHqlExpression * expr) const;
    bool isBalancedSplitter(IHqlExpression * expr) const;
    void restoreBalanced();

private:
    CSplitterInfo();

    void gatherPotentialDependencySplitters(IHqlExpression * expr, IHqlExpression * sink, ResourceGraphInfo * graph);

public:
    EclResourcer & resourcer;
    HqlExprCopyArray externalSources;
    HqlExprCopyArray sinks;
    HqlExprCopyArray dependents;
    BoolArray wasBalanced;
    unsigned nextBalanceId;
    bool preserveBalanced;
    bool ignoreExternalDependencies;
};

class SpillerInfo : public NewTransformInfo
{
public:
    SpillerInfo(IHqlExpression * _original, CResourceOptions * _options);

    IHqlExpression * createSpilledRead(IHqlExpression * spillReason);
    IHqlExpression * createSpilledWrite(IHqlExpression * transformed, bool lazy);
    bool isUsedFromChild() const { return linkedFromChild; }
    IHqlExpression * queryOutputSpillFile() const { return outputToUseForSpill; }
    void setPotentialSpillFile(IHqlExpression * expr);


protected:
    void addSpillFlags(HqlExprArray & args, bool isRead);
    IHqlExpression * createSpillName();
    bool useGraphResult();
    bool useGlobalResult();
    IHqlExpression * wrapRowOwn(IHqlExpression * expr);

protected:
    CResourceOptions * options;
    HqlExprAttr spillName;
    HqlExprAttr spilledDataset;
    IHqlExpression * outputToUseForSpill;
    bool linkedFromChild; // could reuse a spare byte in the parent class
};

class ResourcerInfo : public SpillerInfo
{
public:
    enum { PathUnknown, PathConditional, PathUnconditional };

    ResourcerInfo(IHqlExpression * _original, CResourceOptions * _options);

    IHqlExpression * createTransformedExpr(IHqlExpression * expr);

    bool addCondition(IHqlExpression * condition);
    void addProjected(IHqlExpression * projected);
    bool alwaysExpand();
    unsigned calcNumConditionalUses();
    void clearProjected();
    bool expandRatherThanSpill(bool noteOtherSpills);
    bool expandRatherThanSplit();
    bool hasDependency() const;
    bool isExternalSpill();
    bool neverCommonUp();
    bool isSplit();
    bool isSpilledWrite();
    bool okToSpillThrough();
    void noteUsedFromChild(bool _forceHoist);
    unsigned numInternalUses();
    unsigned numSplitPaths();
    void resetBalanced();
    void setConditionSource(IHqlExpression * condition, bool isFirst);

    inline bool preventMerge()
    {
        //If we need to create a spill global result, but engine can't split then don't merge
        //Only required because hthor doesn't support splitters (or through-workunit results).
        return !options->canSplit() && options->useGlobalResult(linkedFromChild);
    }
    inline bool isUnconditional()       { return (pathToExpr == ResourcerInfo::PathUnconditional); }
    inline bool isConditionExpr()
    {
        switch (original->getOperator())
        {
        case no_if:
        case no_choose:
        case no_chooseds:
            return true;
        case no_output:
        case no_buildindex:
            return isUpdatedConditionally(original);
        case no_filter:
            return isConditionalFilter;
        }
        return false;
    }
    inline bool isResourcedActivity() const
    {
        return isActivity || containsActivity;
    }
    void setRootActivity();
    bool finishedWalkingSplitters() const
    {
        return (curBalanceLink == balancedLinks.ordinality());
    }
    const CSplitterLink * queryCurrentLink() const
    {
        if (balancedLinks.isItem(curBalanceLink))
            return &balancedLinks.item(curBalanceLink);
        return NULL;
    }

protected:
    bool spillSharesSplitter();
    IHqlExpression * createAggregation(IHqlExpression * expr);
    IHqlExpression * createSpiller(IHqlExpression * transformed, bool reuseSplitter);
    IHqlExpression * createSplitter(IHqlExpression * transformed);

public:
    Owned<ResourceGraphInfo> graph;
    HqlExprAttr pathToSplitter;
    HqlExprArray aggregates;
    HqlExprArray conditions;
    HqlExprAttr splitterOutput;
    HqlExprArray projected;
    HqlExprAttr projectedExpr;
    CIArrayOf<CSplitterLink> balancedLinks;
    GraphLinkArray dependsOn;           // NB: These do not link....

    unsigned numUses;
    unsigned numExternalUses;
    unsigned conditionSourceCount;
    unsigned currentSource;
    unsigned curBalanceLink;
    unsigned lastPass;
    unsigned balancedExternalUses;
    unsigned balancedInternalUses;
#ifdef TRACE_BALANCED
    unsigned balanceId;
#endif
    byte pathToExpr;
    bool containsActivity:1;
    bool isActivity:1;
    bool isRootActivity:1;
    bool gatheredDependencies:1;
    bool isSpillPoint:1;
    bool balanced:1;
    bool isAlreadyInScope:1;
    bool forceHoist:1;
    bool neverSplit:1;
    bool isConditionalFilter:1;
    bool projectResult:1;
    bool visited:1;
    bool balancedVisiting:1;
    bool removedParallelPullers:1;
};

class EclResourceDependencyGatherer;
class EclResourcer
{
    friend class SelectHoistTransformer;
    friend class CSplitterInfo;
public:
    EclResourcer(IErrorReceiver & _errors, IConstWorkUnit * _wu, const HqlCppOptions & _translatorOptions, CResourceOptions & _options);
    ~EclResourcer();

    void resourceGraph(IHqlExpression * expr, HqlExprArray & transformed);
    void resourceRemoteGraph(IHqlExpression * expr, HqlExprArray & transformed);
    void setSequential(bool _sequential) { sequential = _sequential; }
    void tagActiveCursors(HqlExprCopyArray * activeRows);
    inline unsigned numGraphResults() { return options.nextResult; }

protected:
    void changeGraph(IHqlExpression * expr, ResourceGraphInfo * newGraph);
    void connectGraphs(ResourceGraphInfo * sourceGraph, IHqlExpression * sourceNode, ResourceGraphInfo * sinkGraph, IHqlExpression * sinkNode, LinkKind linkKind);
    ResourceGraphInfo * createGraph();
    ResourcerInfo * queryCreateResourceInfo(IHqlExpression * expr);
    void removeLink(ResourceGraphLink & link, bool keepExternalUses);
    void replaceGraphReferences(IHqlExpression * expr, ResourceGraphInfo * oldGraph, ResourceGraphInfo * newGraph);
    void replaceGraphReferences(ResourceGraphInfo * oldGraph, ResourceGraphInfo * newGraph);

//Pass 1
    bool findSplitPoints(IHqlExpression * expr, bool isProjected);
    void findSplitPoints(HqlExprArray & exprs);
    void noteConditionalChildren(BoolArray & alwaysHoistChild);
    void deriveUsageCounts(IHqlExpression * expr);
    void deriveUsageCounts(const HqlExprArray & exprs);

//Pass 2
    void createInitialGraph(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerGraph, LinkKind linkKind, bool forceNewGraph);
    void createInitialGraphs(HqlExprArray & exprs);

    void createInitialRemoteGraph(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerGraph, bool forceNewGraph);
    void createInitialRemoteGraphs(HqlExprArray & exprs);

//Pass 3
    void markAsUnconditional(IHqlExpression * expr, ResourceGraphInfo * ownerGraph, IHqlExpression * condition);
    void markCondition(IHqlExpression * expr, IHqlExpression * condition, bool wasConditional);
    void markConditionBranch(unsigned childIndex, IHqlExpression * expr, IHqlExpression * condition, bool wasConditional);
    void markConditions(HqlExprArray & exprs);
    void markChildDependentsAsUnconditional(ResourcerInfo * info, IHqlExpression * condition);

//Pass 4
    void createResourceSplit(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerNewGraph, ResourceGraphInfo * originalGraph);
    void getResources(IHqlExpression * expr, CResources & exprResources);
    bool calculateResourceSpillPoints(IHqlExpression * expr, ResourceGraphInfo * graph, CResources & resourcesSoFar, bool hasGoodSpillPoint, bool canSpill);
    void insertResourceSpillPoints(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerOriginalGraph, ResourceGraphInfo * ownerNewGraph);
    void resourceSubGraph(ResourceGraphInfo * graph);
    void resourceSubGraphs(HqlExprArray & exprs);

//Pass 5
    void addDependencies(EclResourceDependencyGatherer & gatherer, IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr);
    void addDependencies(HqlExprArray & exprs);

//Pass 6
    bool queryMergeGraphLink(ResourceGraphLink & link);
    void mergeSubGraphs();
    void mergeSubGraphs(unsigned pass);
    void mergeSiblings();

//Pass 6b
    void oldSpotUnbalancedSplitters(IHqlExpression * expr, unsigned whichSource, IHqlExpression * path, ResourceGraphInfo * graph);
    void oldSpotUnbalancedSplitters(HqlExprArray & exprs);

//Pass 6c
    void spotSharedInputs(IHqlExpression * expr, ResourceGraphInfo * graph);
    void spotSharedInputs();

    bool removePassThrough(CSplitterInfo & connections, ResourcerInfo & info);
    void removeDuplicateIndependentLinks(CSplitterInfo & connections, ResourcerInfo & info);
    void optimizeIndependentLinks(CSplitterInfo & connections, ResourcerInfo & info);
    void optimizeConditionalLinks(CSplitterInfo & connections);
    IHqlExpression * walkPotentialSplitterLinks(CSplitterInfo & connections, IHqlExpression * expr, const CSplitterLink * link);
    IHqlExpression * walkPotentialSplitters(CSplitterInfo & connections, IHqlExpression * expr, const CSplitterLink & link);
    void walkPotentialSplitters(CSplitterInfo & connections);
    void extractSharedInputs(CSplitterInfo & connections, ResourceGraphInfo & graph);
    void spotUnbalancedSplitters(const HqlExprArray & exprs);

//Pass 7
    bool optimizeAggregate(IHqlExpression * expr);
    void optimizeAggregates();

//Pass 8
    IHqlExpression * findPredecessor(IHqlExpression * expr, IHqlExpression * search, IHqlExpression * prev);
    IHqlExpression * findPredecessor(ResourcerInfo * search);
    void moveExternalSpillPoints();

//Pass 9
    IHqlExpression * doCreateResourced(IHqlExpression * expr, ResourceGraphInfo * graph, bool expandInParent, bool defineSideEffect);
    IHqlExpression * createResourced(IHqlExpression * expr, ResourceGraphInfo * graph, bool expandInParent, bool defineSideEffect);
    void createResourced(HqlExprArray & transformed);
    void createResourced(ResourceGraphInfo * graph, HqlExprArray & transformed);
    void inheritRedundantDependencies(ResourceGraphInfo * thisGraph);

    void display(StringBuffer & out);
    void trace();

    void doCheckRecursion(ResourceGraphInfo * graph, PointerArray & visited);
    void checkRecursion(ResourceGraphInfo * graph, PointerArray & visited);
    void checkRecursion(ResourceGraphInfo * graph);
    unsigned getMaxDepth() const;
    bool checkAlreadyVisited(ResourcerInfo * info);
    void nextPass() { thisPass++; }

protected:
    Owned<IConstWorkUnit> wu;
    CIArrayOf<ResourceGraphInfo> graphs;
    CIArrayOf<ResourceGraphLink> links;
    ClusterType targetClusterType;
    CResources * resourceLimit;
    IErrorReceiver * errors;
    unsigned thisPass;
    bool spilled;
    bool insideNeverSplit;
    bool insideSteppedNeverSplit;
    bool sequential;
    CResourceOptions & options;
    HqlExprArray rootConditions;
    HqlExprCopyArray activeSelectors;
};

#endif
