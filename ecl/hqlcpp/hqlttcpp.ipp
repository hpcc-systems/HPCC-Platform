/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
#ifndef __HQLTTCPP_IPP_
#define __HQLTTCPP_IPP_

#include "hqlcpp.hpp"
#include "hqlcpp.ipp"

#include "hqlhtcpp.ipp"
#include "hqltrans.ipp"

//---------------------------------------------------------------------------

class NewThorStoredReplacer : public QuickHqlTransformer
{
public:
    NewThorStoredReplacer(HqlCppTranslator & _translator, IWorkUnit * _wu, ICodegenContextCallback * _logger);

    virtual void doAnalyseBody(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    bool needToTransform();

protected:
    HqlCppTranslator & translator;
    IWorkUnit * wu;
    ICodegenContextCallback * ctxCallback;
    HqlExprArray commands;
    HqlExprArray storedNames;
    HqlExprArray storedValues;
    HqlExprCopyArray activeReplacements;
    BoolArray storedIsConstant;
    bool foldStored;
    bool seenMeta;
};

//---------------------------------------------------------------------------

enum YesNoOption { OptionUnknown, OptionNo, OptionMaybe, OptionSome, OptionYes };   //NB: last 3 IN ascending order of definiteness.

class HqlThorBoundaryInfo : public NewTransformInfo
{
public:
    HqlThorBoundaryInfo(IHqlExpression * _original) : NewTransformInfo(_original) { normalize = OptionUnknown; }

public:
    YesNoOption normalize;
};

class HqlThorBoundaryTransformer : public NewHqlTransformer
{
public:
    HqlThorBoundaryTransformer(IConstWorkUnit * wu, bool _isRoxie, unsigned _maxRootMaybes, bool _resourceConditionalActions, bool _resourceSequential);

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr) { return CREATE_NEWTRANSFORMINFO(HqlThorBoundaryInfo, expr); }
    inline HqlThorBoundaryInfo * queryBodyExtra(IHqlExpression * expr)      { return static_cast<HqlThorBoundaryInfo *>(queryTransformExtra(expr->queryBody())); }
    
    void transformRoot(const HqlExprArray & in, HqlExprArray & out);

protected:
    void transformCompound(HqlExprArray & result, node_operator compoundOp, const HqlExprArray & args, unsigned MaxMaybes);

protected:
    YesNoOption calcNormalizeThor(IHqlExpression * expr);
    YesNoOption normalizeThor(IHqlExpression * expr);

protected:
    IConstWorkUnit * wu;
    unsigned maxRootMaybes;
    bool isRoxie;
    bool resourceConditionalActions;
    bool resourceSequential;
};

//---------------------------------------------------------------------------

class ThorScalarInfo : public HoistingTransformInfo
{
public:
    ThorScalarInfo(IHqlExpression * _original) : HoistingTransformInfo(_original) { }

public:
    HqlExprAttr transformed[2];
    HqlExprAttr transformedSelector[2];
};

class ThorScalarTransformer : public HoistingHqlTransformer
{
public:
    ThorScalarTransformer(const HqlCppOptions & _options);

    virtual void doAnalyseExpr(IHqlExpression * expr);

    IHqlExpression * createTransformed(IHqlExpression * expr);

    inline bool needToTransform()                                   { return seenCandidate || containsUnknownIndependentContents; }

protected:
    void createHoisted(IHqlExpression * expr, SharedHqlExpr & setResultStmt, SharedHqlExpr & getResult, bool addWrapper);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr) { return CREATE_NEWTRANSFORMINFO(ThorScalarInfo, expr); }
    virtual IHqlExpression * queryAlreadyTransformed(IHqlExpression * expr);
    virtual IHqlExpression * queryAlreadyTransformedSelector(IHqlExpression * expr);
    virtual void setTransformed(IHqlExpression * expr, IHqlExpression * transformed);
    virtual void setTransformedSelector(IHqlExpression * expr, IHqlExpression * transformed);

    virtual IHqlExpression * transformIndependent(IHqlExpression * expr)
    {
        ThorScalarTransformer nested(options);
        nested.analyse(expr, 0);
        if (nested.needToTransform())
            return nested.transformRoot(expr);
        return LINK(expr);
    }

    //This is unusual that it is queryExtra() instead of queryBodyExtra() - since it is used to store the transformed
    //values, not any extra information.
    inline ThorScalarInfo * queryExtra(IHqlExpression * expr)       { return static_cast<ThorScalarInfo *>(queryTransformExtra(expr)); }
    inline bool isConditional()                                     { return isConditionalDepth != 0; }

protected:
    const HqlCppOptions & options;
    unsigned isConditionalDepth;
    bool seenCandidate;
};

//---------------------------------------------------------------------------

class SequenceNumberInfo : public NewTransformInfo
{
public:
    SequenceNumberInfo(IHqlExpression * expr) : NewTransformInfo(expr), getsSequence(false) {}
    void setGetsSequence() { getsSequence = true; }
    virtual IHqlExpression * queryTransformed() { if(getsSequence) return NULL; else return NewTransformInfo::queryTransformed(); } //must transform again if expression takes sequence number, as require different sequence number each time

private:
    bool getsSequence;
};

class SequenceNumberAllocator : public NewHqlTransformer
{
public:
    SequenceNumberAllocator();

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr) { return CREATE_NEWTRANSFORMINFO(SequenceNumberInfo, expr); }
    
    //queryExtra() instead of queryBodyExtra() because it is used to modify how queryTransform() acts.
    //NOTE: I'm not sure this really works e.g., if the transformed expression is a sequence list.
    SequenceNumberInfo * queryExtra(IHqlExpression * expr) { return dynamic_cast<SequenceNumberInfo *>(queryTransformExtra(expr)); }

    unsigned getMaxSequence() { return sequence; }

protected:
    void nextSequence(HqlExprArray & args, IHqlExpression * name, _ATOM overwriteAction, IHqlExpression * value, bool needAttr, bool * duplicate);
    virtual IHqlExpression * doTransformRootExpr(IHqlExpression * expr);
    IHqlExpression * attachSequenceNumber(IHqlExpression * expr);

protected:
    unsigned sequence;
    MapOwnedHqlToOwnedHql namedMap;
};

//---------------------------------------------------------------------------

class ThorHqlTransformer : public NewHqlTransformer
{
public:
    ThorHqlTransformer(HqlCppTranslator & _translator, ClusterType _targetClusterType, IConstWorkUnit * _wu);

protected:
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    IHqlExpression * getMergeTransform(IHqlExpression * dataset, IHqlExpression * transform);

    IHqlExpression * normalizeChooseN(IHqlExpression * expr);
    IHqlExpression * normalizeCoGroup(IHqlExpression * expr);
    IHqlExpression * normalizeDedup(IHqlExpression * expr);
//  IHqlExpression * normalizeIndexBuild(IHqlExpression * expr);
    IHqlExpression * normalizeGroup(IHqlExpression * expr);
    IHqlExpression * normalizeJoinOrDenormalize(IHqlExpression * expr);
    IHqlExpression * normalizeTableToAggregate(IHqlExpression * expr, bool canOptimizeCasts);
    IHqlExpression * normalizeTableGrouping(IHqlExpression * expr);
    IHqlExpression * normalizeMergeAggregate(IHqlExpression * expr);
    IHqlExpression * normalizePrefetchAggregate(IHqlExpression * expr);
    IHqlExpression * normalizeRollup(IHqlExpression * expr);
    IHqlExpression * normalizeScalarAggregate(IHqlExpression * expr);
    IHqlExpression * normalizeSelect(IHqlExpression * expr);
    IHqlExpression * normalizeSort(IHqlExpression * expr);
    IHqlExpression * normalizeSortSteppedIndex(IHqlExpression * expr, _ATOM attrName);
    IHqlExpression * normalizeTempTable(IHqlExpression * expr);

    IHqlExpression * skipGroupsWithinGroup(IHqlExpression * expr, bool isLocal);
    IHqlExpression * skipOverGroups(IHqlExpression * dataset, bool isLocal);

protected:
    typedef NewHqlTransformer PARENT;
    HqlCppTranslator &  translator;
    const HqlCppOptions & options;
    ClusterType         targetClusterType;
    unsigned            topNlimit;
    bool                groupAllDistribute;
};

//---------------------------------------------------------------------------

class CompoundSourceInfo : public NewTransformInfo
{
public:
    CompoundSourceInfo(IHqlExpression * _original); 

    bool canMergeLimit(IHqlExpression * expr, ClusterType targetClusterType);
    void ensureCompound();
    bool isAggregate();
    inline bool isShared() { return splitCount > 1; }
    bool inherit(const CompoundSourceInfo & other, node_operator newSourceOp = no_none);
    inline bool isNoteUsageFirst() 
    { 
        noteUsage();
        return (splitCount == 1);
    }
    inline void noteUsage() { if (splitCount < 10) splitCount++; }
    inline bool isBinary() const { return mode != no_csv && mode != no_xml; }
    void reset();

public:
    HqlExprAttr uid;
    node_operator sourceOp;
    node_operator mode;
    byte splitCount;
    bool forceCompound:1;
    bool isBoundary:1;
    bool isCloned:1;
    bool isLimited:1;
    bool isPreloaded:1;
    bool isFiltered:1;
    bool isPostFiltered:1;
    bool isCreateRowLimited:1;
};

enum
{
    CSFpreload      = 0x0001, 
    CSFindex        = 0x0002, 
    CSFignoreShared = 0x0010,
    CSFnewdisk      = 0x0020,
    CSFnewindex     = 0x0040,
    CSFnewchild     = 0x0080,
    CSFnewinline    = 0x0100,
    CSFcompoundSpill= 0x0200,
};


//MORE: Could remove dependency on insideCompound if it was ok to have compound operators scattered through the
//      contents of a compound item.  Probably would cause few problems, and would make life simpler
class CompoundSourceTransformer : public NewHqlTransformer
{
public:
    CompoundSourceTransformer(HqlCppTranslator & _translator, unsigned _flags);

    IHqlExpression * process(IHqlExpression * expr);

    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

protected:
    bool childrenAreShared(IHqlExpression * expr);
    bool createCompoundSource(IHqlExpression * expr);
    CompoundSourceInfo * queryBodyExtra(IHqlExpression * expr)  { return (CompoundSourceInfo *)queryTransformExtra(expr->queryBody()); }
    void analyseGatherInfo(IHqlExpression * expr);
    void analyseMarkBoundaries(IHqlExpression * expr);
    bool needToCloneLimit(IHqlExpression * expr, node_operator sourceOp);

protected:
    HqlCppTranslator &  translator;
    ClusterType         targetClusterType;
    unsigned            flags;
    bool                insideCompound;
    bool                candidate;
};


//---------------------------------------------------------------------------

class CompoundActivityTransformer : public NewHqlTransformer
{
public:
    CompoundActivityTransformer(ClusterType _targetClusterType);

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    ClusterType targetClusterType;
};


//---------------------------------------------------------------------------

class OptimizeActivityInfo : public NewTransformInfo
{
public:
    OptimizeActivityInfo(IHqlExpression * _original) : NewTransformInfo(_original) { setNumUses(0); }

    inline bool isShared() { return getNumUses() > 1; }
    inline void noteUsed() { setNumUses(getNumUses()+1); }
    inline void inherit(const OptimizeActivityInfo * other) 
    { 
        setNumUses(getNumUses() + other->getNumUses());
    }

private:
    inline byte getNumUses() const { return spareByte1; }
    inline void setNumUses(unsigned value) { spareByte1 = value < 100 ? value : 100; }
    using NewTransformInfo::spareByte1;     // used to hold a numUses()
};

//Various activity optimizations which can be done if nodes aren't shared
//sort(ds,x)[n]     -> topn(ds,x,n)[n]
//count(x) > n      -> count(choosen(x,n+1)) > n

class OptimizeActivityTransformer : public NewHqlTransformer
{
public:
    OptimizeActivityTransformer(bool _optimizeCountCompare, bool _optimizeNonEmpty);

    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr)  { return CREATE_NEWTRANSFORMINFO(OptimizeActivityInfo, expr); }
    
    IHqlExpression * doCreateTransformed(IHqlExpression * expr);
    IHqlExpression * insertChoosen(IHqlExpression * lhs, IHqlExpression * limit, __int64 limitDelta);
    IHqlExpression * optimizeCompare(IHqlExpression * lhs, IHqlExpression * rhs, node_operator op);

    OptimizeActivityInfo * queryBodyExtra(IHqlExpression * expr)    { return (OptimizeActivityInfo *)queryTransformExtra(expr->queryBody()); }
    inline bool isShared(IHqlExpression * expr)                 { return queryBodyExtra(expr)->isShared(); }

protected:
    bool optimizeCountCompare;
    bool optimizeNonEmpty;
};


//---------------------------------------------------------------------------

class WorkflowTransformInfo : public NewTransformInfo
{
public:
    WorkflowTransformInfo(IHqlExpression * expr) : NewTransformInfo(expr) { wfid = 0; lastWfid = 0; manyWorkflow = false; firstUseIsConditional = false; }
    UnsignedArray const &     queryDependencies() const { return dependencies; }
    void                      addDependency(unsigned dep)
    {
        if (dependencies.find(dep) == NotFound)
            dependencies.append(dep);
    }

    inline bool isCommonUpCandidate() { return manyWorkflow && !firstUseIsConditional; }

    inline bool noteWorkflow(unsigned wfid, bool isConditional)
    {
        if (lastWfid)
        {
            if (wfid != lastWfid)
            {
                manyWorkflow = true;
                lastWfid = wfid;
            }
            return true;
        }
        lastWfid = wfid;
        firstUseIsConditional = isConditional; 
        return false;
    }

private:
    UnsignedArray             dependencies;
    unsigned short            wfid;
    unsigned short            lastWfid;         // used during analysis
    bool                      firstUseIsConditional;
    bool                      manyWorkflow;
};

class ContingencyData
{
public:
    ContingencyData() : success(0), failure(0), recovery(0), retries(0), contingencyFor(0) {}
    unsigned                  success;
    unsigned                  failure;
    unsigned                  recovery;
    unsigned                  retries;
    unsigned                  contingencyFor;
};

class ScheduleData
{
public:
    ScheduleData() : independent(false), now(true), priority(50), counting(false), count(0) {}
    bool                      independent;
    bool                      now;
    StringBuffer              eventName;
    StringBuffer              eventText;
    int                       priority;
    bool                      counting;
    unsigned                  count;
};

class WorkflowTransformer : public NewHqlTransformer
{
public:
    WorkflowTransformer(IWorkUnit * _wu, HqlCppTranslator & _translator);

    void                      analyseAll(const HqlExprArray & in);
    void                      transformRoot(const HqlExprArray & in, WorkflowArray & out);

protected:
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr) { return CREATE_NEWTRANSFORMINFO(WorkflowTransformInfo, expr); }

    IWorkflowItem *           addWorkflowToWorkunit(unsigned wfid, WFType type, WFMode mode, IHqlExpression * cluster) 
    { 
        UnsignedArray dependencies; 
        ContingencyData conts; 
        return addWorkflowToWorkunit(wfid, type, mode, dependencies, conts, cluster);
    }
    IWorkflowItem *           addWorkflowToWorkunit(unsigned wfid, WFType type, WFMode mode, UnsignedArray const & dependencies, IHqlExpression * cluster) 
    { 
        ContingencyData conts;
        return addWorkflowToWorkunit(wfid, type, mode, dependencies, conts, cluster);
    }
    IWorkflowItem *           addWorkflowToWorkunit(unsigned wfid, WFType type, WFMode mode, UnsignedArray const & dependencies, ContingencyData const & conts, IHqlExpression * cluster);
    IWorkflowItem *           addWorkflowContingencyToWorkunit(unsigned wfid, WFType type, WFMode mode, UnsignedArray const & dependencies, IHqlExpression * cluster, unsigned wfidFor) { ContingencyData conts; conts.contingencyFor = wfidFor; return addWorkflowToWorkunit(wfid, type, mode, dependencies, conts, cluster); }

    void setWorkflowPersist(IWorkflowItem * wf, char const * persistName, unsigned persistWfid);
    void setWorkflowSchedule(IWorkflowItem * wf, ScheduleData const & sched);

    virtual IHqlExpression *  createTransformed(IHqlExpression * expr);
    void                      inheritDependencies(IHqlExpression * expr);
    void                      copyDependencies(WorkflowTransformInfo * source, WorkflowTransformInfo * dest);
    void                      copySetValueDependencies(WorkflowTransformInfo * source, IHqlExpression * expr);

    unsigned                  splitValue(IHqlExpression * value);
    IHqlExpression *          extractWorkflow(IHqlExpression * untransformed, IHqlExpression * transformed);
    IHqlExpression *          extractClusterWorkflow(IHqlExpression * expr);
    IHqlExpression *          extractCommonWorkflow(IHqlExpression * expr, IHqlExpression * transformed);

    IHqlExpression *          transformInternalCall(IHqlExpression * transformed);
    IHqlExpression *          transformInternalFunction(IHqlExpression * transformed);
    
    IHqlExpression *          createCompoundWorkflow(IHqlExpression * expr);
    IHqlExpression *          createIfWorkflow(IHqlExpression * expr);
    IHqlExpression *          createParallelWorkflow(IHqlExpression * expr);
    IHqlExpression *          createSequentialWorkflow(IHqlExpression * expr);
    IHqlExpression *          createWaitWorkflow(IHqlExpression * expr);
    IHqlExpression *          transformRootAction(IHqlExpression * expr);
    IHqlExpression *          transformSequentialEtc(IHqlExpression * expr);
    
    IWorkflowItem *           lookupWorkflowItem(unsigned wfid);
    IHqlExpression *          createWorkflowAction(unsigned wfid);
    unsigned                  ensureWorkflowAction(IHqlExpression * expr);
    void                      ensureWorkflowAction(UnsignedArray & dependencies, IHqlExpression * expr);
    WorkflowItem *            createWorkflowItem(IHqlExpression * expr, unsigned wfid, unsigned persistWfid = 0);
    void percolateScheduledIds(WorkflowArray & workflow);

    void                      cacheWorkflowDependencies(unsigned wfid, UnsignedArray & extra);

    inline bool               hasDependencies(IHqlExpression * expr) { return queryDirectDependencies(expr).ordinality() != 0; }
    bool                      hasStoredDependencies(IHqlExpression * expr);
    UnsignedArray const &     queryDependencies(unsigned wfid);
    UnsignedArray const &     queryDirectDependencies(IHqlExpression * expr);       // don't include children.
    void                      gatherIndirectDependencies(UnsignedArray & match, IHqlExpression * expr);

    inline WorkflowTransformInfo * queryBodyExtra(IHqlExpression * expr) { return static_cast<WorkflowTransformInfo *>(queryTransformExtra(expr->queryBody())); }

    inline unsigned           markDependencies() { return cumulativeDependencies.ordinality(); }
    inline bool               restoreDependencies(unsigned mark) 
    { 
        bool anyAdded = (mark == cumulativeDependencies.ordinality());
        cumulativeDependencies.trunc(mark); 
        return anyAdded;
    }
    
    bool                      haveSameDependencies(IHqlExpression * expr1, IHqlExpression * expr2);
    bool                      includesNewDependencies(IHqlExpression * prev, IHqlExpression * next);
    bool                      hasNonTrivialDependencies(IHqlExpression * expr);

    void                      transformSequential(HqlExprArray & transformed);

    void analyseExpr(IHqlExpression * expr);

protected:
    IWorkUnit *               wu;
    HqlCppTranslator &        translator;
    WorkflowArray *           workflowOut;
    unsigned                  wfidCount;
    HqlExprArray              alreadyProcessed;
    HqlExprArray              alreadyProcessedExpr;
    HqlExprArray              alreadyProcessedUntransformed;
    HqlExprCopyArray          activeLocations;
    unsigned                  trivialStoredWfid;
    unsigned                  onceWfid;
    unsigned                  nextInternalFunctionId;
    HqlExprArray              trivialStoredExprs;
    HqlExprArray              onceExprs;
    bool                      combineAllStored;
    bool                      combineTrivialStored;
    bool                      isRootAction;
    bool                      isRoxie;
    UnsignedArray             cumulativeDependencies;
    UnsignedArray             emptyDependencies;
    UnsignedArray             storedWfids;
    OwnedHqlExpr              rootCluster;  // currently unset, but if need to 
//used while analysing..
    unsigned                  activeWfid;
    bool                      isConditional;
    bool                      insideStored;
};

//------------------------------------------------------------------------

class ExplicitGlobalTransformer : public HoistingHqlTransformer
{
public:
    ExplicitGlobalTransformer(IWorkUnit * _wu, HqlCppTranslator & _translator);

    inline bool needToTransform() const { return seenGlobalScope || containsUnknownIndependentContents; }

protected:
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    virtual void doAnalyseExpr(IHqlExpression * expr);

    virtual IHqlExpression * transformIndependent(IHqlExpression * expr)
    {
        if (containsUnknownIndependentContents || seenLocalGlobalScope)
        {
            ExplicitGlobalTransformer nested(wu, translator);
            nested.analyse(expr, 0);
            if (nested.needToTransform())
                return nested.transformRoot(expr);
        }
        return LINK(expr);
    }

private:
    IWorkUnit *               wu;
    HqlCppTranslator &        translator;
    bool seenGlobalScope;
    bool seenLocalGlobalScope;
    bool isRoxie;
};

class ScalarGlobalExtra : public HoistingTransformInfo
{
public:
    ScalarGlobalExtra(IHqlExpression * _original) : HoistingTransformInfo(_original) { numUses = 0; alreadyGlobal = false; createGlobal = false; couldHoist = false; candidate = false; neverHoist = false; }

public:
    unsigned numUses;
    bool alreadyGlobal;
    bool createGlobal;
    bool couldHoist;
    bool candidate;
    bool neverHoist;
};

//Only does anything to scalar expressions => don't need a mergingTransformer
class ScalarGlobalTransformer : public HoistingHqlTransformer
{
public:
    ScalarGlobalTransformer(HqlCppTranslator & _translator);

protected:
    virtual void             analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr) { return CREATE_NEWTRANSFORMINFO(ScalarGlobalExtra, expr); }
    virtual void doAnalyseExpr(IHqlExpression * expr);
    bool isComplex(IHqlExpression * expr, bool checkGlobal);

    virtual IHqlExpression * transformIndependent(IHqlExpression * expr)
    {
        ScalarGlobalTransformer nested(translator);
        nested.analyse(expr, 0);
        return nested.transformRoot(expr);
    }

    inline ScalarGlobalExtra * queryBodyExtra(IHqlExpression * expr)        { return static_cast<ScalarGlobalExtra*>(queryTransformExtra(expr->queryBody())); }

protected:
    HqlCppTranslator & translator;
    bool okToHoist;
    bool neverHoist;
};


class ScopeMigrateInfo : public HoistingTransformInfo
{
public:
    ScopeMigrateInfo(IHqlExpression * _original) : HoistingTransformInfo(_original) { maxActivityDepth = 0; }//useCount = 0; condUseCount = 0; }

public:
    unsigned maxActivityDepth;
//  unsigned    useCount;
//  unsigned    condUseCount;
};

class NewScopeMigrateTransformer : public HoistingHqlTransformer
{
public:
    NewScopeMigrateTransformer(IWorkUnit * _wu, HqlCppTranslator & _translator);

protected:
    virtual void             analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr) { return CREATE_NEWTRANSFORMINFO(ScopeMigrateInfo, expr); }

    virtual IHqlExpression * transformIndependent(IHqlExpression * expr)
    {
        NewScopeMigrateTransformer nested(wu, translator);
        nested.analyse(expr, 0);
        return nested.transformRoot(expr);
    }

    IHqlExpression * hoist(IHqlExpression * expr, IHqlExpression * hoisted);
    IHqlExpression * transformCond(IHqlExpression * expr);

    inline unsigned extraIndex() { return activityDepth != 0 ? 0 : 1; }
    inline ScopeMigrateInfo * queryBodyExtra(IHqlExpression * expr)     { return static_cast<ScopeMigrateInfo *>(queryTransformExtra(expr->queryBody())); }

private:
    IWorkUnit *               wu;
    HqlCppTranslator &        translator;
    bool isRoxie;
    bool minimizeWorkunitTemporaries;
    unsigned activityDepth;
};

class AutoScopeMigrateInfo : public NewTransformInfo
{
public:
    AutoScopeMigrateInfo(IHqlExpression * _original) : NewTransformInfo(_original) { useCount = 0; condUseCount = 0; manyGraphs = false; firstUseIsConditional = false; firstUseIsSequential = false; globalInsideChild = false; lastGraph = 0; }

    bool addGraph(unsigned graph);
    bool doAutoHoist(IHqlExpression * transformed, bool minimizeWorkunitTemporaries);

public:
    unsigned    useCount;
    unsigned    condUseCount;
    unsigned    lastGraph;
    bool manyGraphs;
    bool firstUseIsConditional;
    bool firstUseIsSequential;
    bool globalInsideChild;
};

class AutoScopeMigrateTransformer : public NewHqlTransformer
{
public:
    AutoScopeMigrateTransformer(IWorkUnit * _wu, HqlCppTranslator & _translator);

    void transformRoot(const HqlExprArray & in, HqlExprArray & out);

    bool worthTransforming() const { return hasCandidate; }

protected:
    virtual void             analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr) { return CREATE_NEWTRANSFORMINFO(AutoScopeMigrateInfo, expr); }

    IHqlExpression * hoist(IHqlExpression * expr, IHqlExpression * hoisted);
    IHqlExpression * transformCond(IHqlExpression * expr);
    void doAnalyseExpr(IHqlExpression * expr);

    inline AutoScopeMigrateInfo * queryBodyExtra(IHqlExpression * expr)     { return static_cast<AutoScopeMigrateInfo *>(queryTransformExtra(expr->queryBody())); }

private:
    IWorkUnit *               wu;
    HqlCppTranslator &        translator;
    bool isRoxie;
    bool isConditional;
    bool hasCandidate;
    bool isSequential;
    unsigned curGraph;
    unsigned nextGraph;
    HqlExprArray graphActions;
    unsigned activityDepth;
    HqlExprArray * globalTarget;
};

//---------------------------------------------------------------------------
class TrivialGraphRemover : public NewHqlTransformer
{
public:
    TrivialGraphRemover();
    bool worthTransforming() const { return hasCandidate; }

protected:
    virtual void             analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    bool isTrivialGraph(IHqlExpression * expr);

private:
    bool hasCandidate;
};

//---------------------------------------------------------------------------

class ThorCountTransformer : public NewHqlTransformer
{
public:
    ThorCountTransformer(HqlCppTranslator & _translator, bool _countDiskFuncOk);

    IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    HqlCppTranslator &      translator;
    bool countDiskFuncOk;
};


//---------------------------------------------------------------------------

class FilteredIndexOptimizer : public NewHqlTransformer
{
public:
    FilteredIndexOptimizer(bool _processJoins, bool _processReads);

protected:
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    bool processJoins;
    bool processReads;
};


//---------------------------------------------------------------------------

class LocalUploadTransformer : public NewHqlTransformer
{
public:
    LocalUploadTransformer(IWorkUnit * _wu);

protected:
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    IWorkUnit * wu;
};


//---------------------------------------------------------------------------

class NestedSelectorInfo : public NewTransformInfo
{
public:
    NestedSelectorInfo(IHqlExpression * _original) : NewTransformInfo(_original) { isDenormalized = false; insertDenormalize = false; }

public:
    bool isDenormalized;
    bool insertDenormalize;
};

class NestedSelectorNormalizer : public NewHqlTransformer
{
public:
    NestedSelectorNormalizer();

    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    inline bool requiresTransforming() { return spottedCandidate; }

protected:
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr)  { return CREATE_NEWTRANSFORMINFO(NestedSelectorInfo, expr); }
    
    IHqlExpression * createNormalized(IHqlExpression * expr);
    NestedSelectorInfo * queryBodyExtra(IHqlExpression * expr)      { return (NestedSelectorInfo *)queryTransformExtra(expr->queryBody()); }

protected:
    bool spottedCandidate;
};


//---------------------------------------------------------------------------

class LeftRightSelectorNormalizer : public NewHqlTransformer
{
public:
    LeftRightSelectorNormalizer(bool _allowAmbiguity);

    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual IHqlExpression * createTransformedSelector(IHqlExpression * expr);

    inline bool containsAmbiguity() { return isAmbiguous; }

protected:
    void checkAmbiguity(const HqlExprCopyArray & inScope, IHqlExpression * selector);

protected:
    bool allowAmbiguity;
    bool isAmbiguous;
};


//---------------------------------------------------------------------------

class ForceLocalTransformInfo : public NewTransformInfo
{
    friend class ForceLocalTransformer;
public:
    ForceLocalTransformInfo(IHqlExpression * _expr) : NewTransformInfo(_expr) { }

public:
    //Theoretically this should be [insideForceLocal][allNodesDepth], but using insideAllNodes instead will just some obscure selfnodes in wrong place.
    HqlExprAttr         localTransformed[2][2];
    HqlExprAttr         localTransformedSelector[2][2];
};


//MORE: I think this might need to be a scoped transformer to cope with the general case
class ForceLocalTransformer : public NewHqlTransformer
{
public:
    ForceLocalTransformer(ClusterType _targetClusterType);

    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    virtual IHqlExpression * queryAlreadyTransformed(IHqlExpression * expr);
    virtual void setTransformed(IHqlExpression * expr, IHqlExpression * transformed);

    virtual IHqlExpression * queryAlreadyTransformedSelector(IHqlExpression * expr);
    virtual void setTransformedSelector(IHqlExpression * expr, IHqlExpression * transformed);

protected:
    bool queryAddLocal(IHqlExpression * expr);
    inline bool insideAllNodes() { return allNodesDepth > 0; }
    //Another exception
    inline ForceLocalTransformInfo * queryExtra(IHqlExpression * expr)  { return (ForceLocalTransformInfo *)queryTransformExtra(expr); }

protected:
    ClusterType targetClusterType;
    bool insideForceLocal;
    unsigned allNodesDepth;
};

class HqlLinkedChildRowTransformer : public QuickHqlTransformer
{
public:
    HqlLinkedChildRowTransformer(bool _implicitLinkedChildRows);

    virtual IHqlExpression * createTransformedBody(IHqlExpression * expr);

protected:
    IHqlExpression * ensureInputSerialized(IHqlExpression * expr);
    IHqlExpression * transformBuildIndex(IHqlExpression * expr);
    IHqlExpression * transformPipeThrough(IHqlExpression * expr);

protected:
    bool implicitLinkedChildRows;
};

class HqlScopeTaggerInfo : public MergingTransformInfo
{
public:
    HqlScopeTaggerInfo(IHqlExpression * _expr);
};

class HqlScopeTagger : public ScopedDependentTransformer
{
    typedef ScopedDependentTransformer Parent;
public:
    HqlScopeTagger(IErrorReceiver * _errors);

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

    void reportWarnings();

protected:
    void beginTableScope();
    void endTableScope(HqlExprArray & attrs, IHqlExpression * ds, IHqlExpression * newExpr);
    IHqlExpression * transformSelect(IHqlExpression * expr);

    IHqlExpression * transformAmbiguous(IHqlExpression * expr, bool isActiveOk);
    IHqlExpression * transformAmbiguousChildren(IHqlExpression * expr);
    IHqlExpression * transformNewDataset(IHqlExpression * expr, bool isActiveOk);
    IHqlExpression * transformRelated(IHqlExpression * expr);
    IHqlExpression * transformSelectorsAttr(IHqlExpression * expr);
    IHqlExpression * transformSizeof(IHqlExpression * expr);
    IHqlExpression * transformWithin(IHqlExpression * dataset, IHqlExpression * scope);

    bool isValidNormalizeSelector(IHqlExpression * expr);
    void reportError(const char * msg, bool warning = false);
    void reportSelectorError(IHqlExpression * selector, IHqlExpression * expr);

protected:
    IErrorReceiver * errors;
    WarningProcessor collector;
};

//---------------------------------------------------------------------------

class ContainsCompoundTransformer : public QuickHqlTransformer
{
public:
    ContainsCompoundTransformer();
    
protected:
    virtual void doAnalyseBody(IHqlExpression * expr);

public:
    bool containsCompound;
};
bool containsCompound(const HqlExprArray & exprs);
bool containsCompound(IHqlExpression * expr);

//---------------------------------------------------------------------------


class AnnotationTransformInfo : public NewTransformInfo
{
public:
    AnnotationTransformInfo(IHqlExpression * _expr) : NewTransformInfo(_expr) { }

    IHqlExpression * cloneAnnotations(IHqlExpression * body);
    void noteAnnotation(IHqlExpression * _annotation);
    
public:
    HqlExprCopyArray annotations;
};


class AnnotationNormalizerTransformer : public NewHqlTransformer
{
public:
    AnnotationNormalizerTransformer();
    
    virtual void analyseExpr(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

//  virtual void analyseSelector(IHqlExpression * expr);
//  virtual IHqlExpression * transformSelector(IHqlExpression * expr);

protected:
    AnnotationTransformInfo * queryCommonExtra(IHqlExpression * expr);
};

void normalizeAnnotations(HqlCppTranslator & translator, HqlExprArray & exprs);
void normalizeAnnotations(HqlCppTranslator & translator, WorkflowArray & workflow);

//---------------------------------------------------------------------------

class NestedCompoundTransformer : public HoistingHqlTransformer
{
public:
    NestedCompoundTransformer(HqlCppTranslator & _translator);
    
protected:
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    virtual IHqlExpression * transformIndependent(IHqlExpression * expr)
    {
        if (!containsCompound(expr))
            return LINK(expr);
        NestedCompoundTransformer nested(translator);
        nested.analyse(expr, 0);                    // analyse called so set up whether expressions are unconditional etc.
        return nested.transformRoot(expr);
    }

public:
    HqlCppTranslator & translator;
    const HqlCppOptions & translatorOptions;
};


class HqlTreeNormalizerInfo : public NewTransformInfo
{
public:
    HqlTreeNormalizerInfo(IHqlExpression * _expr) : NewTransformInfo(_expr) { symbol = NULL; }

    void noteSymbol(IHqlExpression * _symbol);
    
public:
    IHqlExpression * symbol;
};

class HqlTreeNormalizer : public NewHqlTransformer
{
typedef NewHqlTransformer Parent;

public:
    HqlTreeNormalizer(HqlCppTranslator & _translator);

    virtual void             analyseExpr(IHqlExpression * expr);

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

    bool querySeenForceLocal() const { return seenForceLocal; }
    bool querySeenLocalUpload() const { return seenLocalUpload; }

protected:
    virtual IHqlExpression * createTransformedSelector(IHqlExpression * expr);
    virtual ITypeInfo * transformType(ITypeInfo * type);

    inline IHqlExpression * createTransformedBody(IHqlExpression * expr);

    IHqlExpression * convertSelectToProject(IHqlExpression * newRecord, IHqlExpression * expr);
    void convertRecordToAssigns(HqlExprArray & assigns, IHqlExpression * oldRecord, IHqlExpression * targetSelector, bool canOmit, bool onlyConstant);
    IHqlExpression * convertRecordToAssigns(IHqlExpression * oldRecord, bool canOmit, bool onlyConstant);
    IHqlExpression * removeDefaultsFromExpr(IHqlExpression * expr, unsigned recordChildIndex, node_operator newOp);
    IHqlExpression * optimizeAssignSkip(HqlExprArray & children, IHqlExpression * expr, IHqlExpression * cond, unsigned depth);
    IHqlExpression * queryTransformPatternDefine(IHqlExpression * expr);
    IHqlExpression * transformActionList(IHqlExpression * expr);
    IHqlExpression * transformCase(IHqlExpression * expr);
    IHqlExpression * transformExecuteWhen(IHqlExpression * expr);
    IHqlExpression * transformEvaluate(IHqlExpression * expr);
    IHqlExpression * transformIfAssert(node_operator newOp, IHqlExpression * expr);
    IHqlExpression * transformKeyIndex(IHqlExpression * expr);
    IHqlExpression * transformMerge(IHqlExpression * expr);
    IHqlExpression * transformNewKeyIndex(IHqlExpression * expr);
    IHqlExpression * transformPatNamedUse(IHqlExpression * expr);
    IHqlExpression * transformPatCheckIn(IHqlExpression * expr);
    IHqlExpression * transformMap(IHqlExpression * expr);
    IHqlExpression * transformTempRow(IHqlExpression * expr);
    IHqlExpression * transformTempTable(IHqlExpression * expr);
    IHqlExpression * transformTable(IHqlExpression * untransformed);
    IHqlExpression * transformWithinFilter(IHqlExpression * expr);
    bool transformTransform(HqlExprArray & children, IHqlExpression * expr);
    IHqlExpression * transformTransform(IHqlExpression * expr);
    IHqlExpression * validateKeyedJoin(IHqlExpression * expr);

protected:
    IHqlExpression * makeRecursiveName(_ATOM searchModule, _ATOM searchName);
    HqlTreeNormalizerInfo * queryCommonExtra(IHqlExpression * expr);
    IHqlExpression * transformSimpleConst(IHqlExpression * expr)
    {
        if (expr->isConstant())
            return transform(expr);
        return LINK(expr);
    }

protected:
    HqlCppTranslator & translator;
    IErrorReceiver * errors;

    HqlExprArray forwardReferences;
    HqlExprArray defines;
    struct
    {
        bool removeAsserts;
        bool commonUniqueNameAttributes;
        bool simplifySelectorSequence;
        bool sortIndexPayload;
        bool allowSections;
        bool normalizeExplicitCasts;
        bool ensureRecordsHaveSymbols;
        bool outputRowsAsDatasets;
        bool constantFoldNormalize;
        bool allowActivityForKeyedJoin;
    } options;
    unsigned nextSequenceValue;
    bool seenForceLocal;
    bool seenLocalUpload;
};

void normalizeHqlTree(HqlCppTranslator & translator, HqlExprArray & exprs);
IHqlExpression * normalizeHqlTree(HqlCppTranslator & translator, IHqlExpression * expr);
// more: This really shouldn't need a translator argument - but it is a bit of a god class, and error reporting needs splitting from it.
IHqlExpression * normalizeRecord(HqlCppTranslator & translator, IHqlExpression * expr);     

IHqlExpression * removeNamedSymbols(IHqlExpression * expr);
void hoistNestedCompound(HqlCppTranslator & _translator, HqlExprArray & exprs);
void hoistNestedCompound(HqlCppTranslator & _translator, WorkflowArray & workflow);

//---------------------------------------------------------------------------
void expandGlobalDatasets(WorkflowArray & array, IWorkUnit * wu, HqlCppTranslator & translator);
void mergeThorGraphs(WorkflowArray & array, bool resourceConditionalActions, bool resourceSequential);
void migrateExprToNaturalLevel(WorkflowArray & array, IWorkUnit * wu, HqlCppTranslator & translator);
void removeTrivialGraphs(WorkflowArray & workflow);
void extractWorkflow(HqlCppTranslator & translator, HqlExprArray & exprs, WorkflowArray & out);
void optimizeActivities(HqlExprArray & exprs, bool optimizeCountCompare, bool optimizeNonEmpty);
void optimizeActivities(WorkflowArray & array, bool optimizeCountCompare, bool optimizeNonEmpty);
IHqlExpression * optimizeActivities(IHqlExpression * expr, bool optimizeCountCompare, bool optimizeNonEmpty);
IHqlExpression * insertImplicitProjects(HqlCppTranslator & translator, IHqlExpression * expr, bool optimizeSpills);
void insertImplicitProjects(HqlCppTranslator & translator, HqlExprArray & exprs);

//------------------------------------------------------------------------

#endif
