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
#ifndef __HQLRESOURCE_IPP_
#define __HQLRESOURCE_IPP_

#include "hqlresource.hpp"

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
    CResourceOptions() { memset(this, 0, sizeof(*this)); state.updateSequence = 0; }

    IHqlExpression * createSpillName(bool isGraphResult);
    void noteGraphsChanged() { state.updateSequence++; }

public:
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
    virtual bool isDependency() { return false; }

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
    ResourceGraphDependencyLink(ResourceGraphInfo * _sourceGraph, IHqlExpression * _sourceNode, ResourceGraphInfo * _sinkGraph, IHqlExpression * _sinkNode);

    virtual void changeSourceGraph(ResourceGraphInfo * newGraph);
    virtual void changeSinkGraph(ResourceGraphInfo * newGraph);
    virtual bool isRedundantLink()          { return false; }
    virtual bool isDependency() { return true; }
};

typedef CIArrayOf<ResourceGraphInfo> ResourceGraphArray;
typedef CopyCIArrayOf<ResourceGraphLink> GraphLinkArray;

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
    bool mergeInSource(ResourceGraphInfo & other, const CResources & limit);
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
    GraphLinkArray dependsOn;           // NB: These do no link....
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

class ResourcerInfo : public CInterface, public IInterface
{
public:
    enum { PathUnknown, PathConditional, PathUnconditional };

    ResourcerInfo(IHqlExpression * _original, CResourceOptions * _options);
    IMPLEMENT_IINTERFACE

    IHqlExpression * createSpilledRead(IHqlExpression * spillReason);
    IHqlExpression * createTransformedExpr(IHqlExpression * expr);

    bool addCondition(IHqlExpression * condition);
    void addProjected(IHqlExpression * projected);
    bool alwaysExpand();
    unsigned calcNumConditionalUses();
    void clearProjected();
    bool expandRatherThanSpill(bool noteOtherSpills);
    bool expandRatherThanSplit();
    bool isExternalSpill();
    bool neverCommonUp();
    bool isSplit();
    bool isSpilledWrite();
    bool okToSpillThrough();
    void noteUsedFromChild(bool _forceHoist);
    unsigned numInternalUses();
    unsigned numSplitPaths();
    void setConditionSource(IHqlExpression * condition, bool isFirst);

    //hthor - don't merge anything to a global result because we don't allow splitters.
    inline bool preventMerge()          { return !options->canSplit() && useGlobalResult(); }
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

protected:
    bool spillSharesSplitter();
    bool useGraphResult();
    bool useGlobalResult();
    IHqlExpression * createAggregation(IHqlExpression * expr);
    IHqlExpression * createSpilledWrite(IHqlExpression * transformed);
    IHqlExpression * createSpiller(IHqlExpression * transformed, bool reuseSplitter);
    IHqlExpression * createSplitter(IHqlExpression * transformed);

protected:
    void addSpillFlags(HqlExprArray & args, bool isRead);
    IHqlExpression * createSpillName();
    IHqlExpression * wrapRowOwn(IHqlExpression * expr);

public:
    HqlExprAttr original;
    Owned<ResourceGraphInfo> graph;
    HqlExprAttr spillName;
    IHqlExpression * transformed;
    IHqlExpression * outputToUseForSpill;
    CResourceOptions * options;
    HqlExprAttr pathToSplitter;
    HqlExprArray aggregates;
    HqlExprArray conditions;
    ChildDependentArray childDependents;
    HqlExprAttr spilledDataset;
    HqlExprAttr splitterOutput;
    HqlExprArray projected;
    HqlExprAttr projectedExpr;

    unsigned numUses;
    unsigned numExternalUses;
    unsigned conditionSourceCount;
    unsigned currentSource;
    byte pathToExpr;
    bool containsActivity;
    bool isActivity;
    bool isRootActivity;
    bool gatheredDependencies;
    bool isSpillPoint;
    bool balanced;
    bool isAlreadyInScope;
    bool linkedFromChild;
    bool forceHoist;
    bool neverSplit;
    bool isConditionalFilter;
    bool projectResult;
    bool visited;
};

struct DependencySourceInfo
{
    HqlExprArray                    search;
    CIArrayOf<ResourceGraphInfo>    graphs;
    HqlExprArray                    exprs;
};


class EclResourcer
{
    friend class SelectHoistTransformer;
public:
    EclResourcer(IErrorReceiver * _errors, IConstWorkUnit * _wu, ClusterType _targetClusterType, unsigned _clusterSize, const HqlCppOptions & _translatorOptions);
    ~EclResourcer();

    void resourceGraph(IHqlExpression * expr, HqlExprArray & transformed);
    void resourceRemoteGraph(IHqlExpression * expr, HqlExprArray & transformed);
    void setChildQuery(bool value);
    void setNewChildQuery(IHqlExpression * graphIdExpr, unsigned numResults);
    void setSequential(bool _sequential) { sequential = _sequential; }
    void setUseGraphResults(bool _useGraphResults) 
    { 
        options.useGraphResults = _useGraphResults; 
    }
    void tagActiveCursors(HqlExprCopyArray & activeRows);
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
    void gatherChildSplitPoints(IHqlExpression * expr, ResourcerInfo * info, unsigned first, unsigned last);
    bool findSplitPoints(IHqlExpression * expr, bool isProjected);
    void findSplitPoints(HqlExprArray & exprs);
    void noteConditionalChildren(BoolArray & alwaysHoistChild);
    void deriveUsageCounts(IHqlExpression * expr);
    void deriveUsageCounts(const HqlExprArray & exprs);
    void extendSplitPoints();
    void projectChildDependents();
    IHqlExpression * projectChildDependent(IHqlExpression * expr);

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
    void addDependencySource(IHqlExpression * search, ResourceGraphInfo * curGraph, IHqlExpression * expr);
    void addDependencyUse(IHqlExpression * search, ResourceGraphInfo * curGraph, IHqlExpression * expr);
    bool addExprDependency(IHqlExpression * expr, ResourceGraphInfo * curGraph, IHqlExpression * activityExpr);
    void addRefExprDependency(IHqlExpression * expr, ResourceGraphInfo * curGraph, IHqlExpression * activityExpr);
    void doAddChildDependencies(IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr);
    void addChildDependencies(IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr);
    void addDependencies(IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr);
    void addDependencies(HqlExprArray & exprs);

//Pass 6
    bool queryMergeGraphLink(ResourceGraphLink & link);
    void mergeSubGraphs();
    void mergeSubGraphs(unsigned pass);
    void mergeSiblings();

//Pass 6b
    void spotUnbalancedSplitters(IHqlExpression * expr, unsigned whichSource, IHqlExpression * path, ResourceGraphInfo * graph);
    void spotUnbalancedSplitters(HqlExprArray & exprs);

//Pass 6c
    void spotSharedInputs(IHqlExpression * expr, ResourceGraphInfo * graph);
    void spotSharedInputs();

//Pass 7
    bool optimizeAggregate(IHqlExpression * expr);
    void optimizeAggregates();

//Pass 8
    IHqlExpression * findPredecessor(IHqlExpression * expr, IHqlExpression * search, IHqlExpression * prev);
    IHqlExpression * findPredecessor(ResourcerInfo * search);
    void moveExternalSpillPoints();

//Pass 9
    IHqlExpression * replaceResourcedReferences(ResourcerInfo * info, IHqlExpression * expr);
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

protected:
    Owned<IConstWorkUnit> wu;
    CIArrayOf<ResourceGraphInfo> graphs;
    CIArrayOf<ResourceGraphLink> links;
    ClusterType targetClusterType;
    DependencySourceInfo dependencySource;                  
    unsigned clusterSize;
    CResources * resourceLimit;
    IErrorReceiver * errors;
    bool spilled;
    bool spillMultiCondition;
    bool spotThroughAggregate;
    bool insideNeverSplit;
    bool insideSteppedNeverSplit;
    bool sequential;
    CResourceOptions options;
    HqlExprArray rootConditions;
    HqlExprCopyArray activeSelectors;
    ChildDependentArray allChildDependents;
};

#endif
