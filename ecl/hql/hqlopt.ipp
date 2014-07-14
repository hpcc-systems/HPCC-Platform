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
#ifndef __HQLOPT_IPP_
#define __HQLOPT_IPP_

#include "hqltrans.ipp"
#include "hqlopt.hpp"
#include "hqlpmap.hpp"
#include "hqlfold.ipp"
#include "hqlutil.hpp"

enum KeyCompType { SAME_KEY, PARTIAL_KEY, SUPER_KEY, DIFFERENT_KEY };

typedef Owned<TableProjectMapper> OwnedMapper;

class OptTransformInfo : public NewTransformInfo
{
public:
    OptTransformInfo(IHqlExpression * _expr) : NewTransformInfo(_expr) { useCount = 0; }

    bool getStopHoist() { return spareByte1; }
    void setStopHoist() { spareByte1 = true; }

public:
    unsigned useCount;
};

class DedupInfoExtractor;
class CTreeOptimizer : public NewHqlTransformer, public NullFolderMixin
{
    friend class ExpandMonitor;
public:
    CTreeOptimizer(IErrorReceiver & _errorProcessor, unsigned _options);

protected:
    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

protected:  
    virtual IHqlExpression * replaceWithNull(IHqlExpression * transformed);
    virtual IHqlExpression * removeParentNode(IHqlExpression * expr);
    virtual IHqlExpression * replaceWithNullRowDs(IHqlExpression * expr);
    virtual IHqlExpression * replaceWithNullRow(IHqlExpression * expr);
    virtual IHqlExpression * transformExpanded(IHqlExpression * expr);              

    IHqlExpression * doCreateTransformed(IHqlExpression * transformed, IHqlExpression * expr);
    IHqlExpression * extractFilterDs(HqlExprArray & conds, IHqlExpression * expr);
    IHqlExpression * removeChildNode(IHqlExpression * expr);
    IHqlExpression * forceSwapNodeWithChild(IHqlExpression * parent);
    IHqlExpression * swapNodeWithChild(IHqlExpression * parent);
    IHqlExpression * swapNodeWithChild(IHqlExpression * parent, unsigned childIndex);
    IHqlExpression * swapIntoAddFiles(IHqlExpression * expr, bool force=false);
    IHqlExpression * swapIntoIf(IHqlExpression * expr, bool force = false);
    TableProjectMapper * getMapper(IHqlExpression * expr);
    bool isShared(IHqlExpression * expr);
    bool isSharedOrUnknown(IHqlExpression * expr);
    bool childrenAreShared(IHqlExpression * expr);
    bool isWorthMovingProjectOverLimit(IHqlExpression * expr);

    IHqlExpression * moveFilterOverSelect(IHqlExpression * expr);
    IHqlExpression * queryMoveKeyedExpr(IHqlExpression * transformed);
    IHqlExpression * optimizeAggregateDataset(IHqlExpression * transformed);
    IHqlExpression * optimizeDatasetIf(IHqlExpression * transformed);
    IHqlExpression * moveProjectionOverSimple(IHqlExpression * transformed, bool ignoreIfFail, bool errorIfFail);
    IHqlExpression * moveProjectionOverLimit(IHqlExpression * transformed);
    IHqlExpression * hoistMetaOverProject(IHqlExpression * expr);

    IHqlExpression * replaceChild(IHqlExpression * expr, IHqlExpression * newChild);
    IHqlExpression * insertChild(IHqlExpression * expr, IHqlExpression * newChild);         // called if a child being inserted only
    void unwindReplaceChild(HqlExprArray & args, IHqlExpression * expr, IHqlExpression * newChild);

    IHqlExpression * expandFields(TableProjectMapper * mapper, IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IExpandCallback * _expandCallback = NULL);
    IHqlExpression * expandProjectedDataset(IHqlExpression * child, IHqlExpression * transform, IHqlExpression * childSelector, IHqlExpression * expr);

    IHqlExpression * defaultCreateTransformed(IHqlExpression * expr);
    inline OptTransformInfo * queryBodyExtra(IHqlExpression * expr) { return (OptTransformInfo *)queryTransformExtra(expr->queryBody()); }
    IHqlExpression * queryPromotedFilter(IHqlExpression * expr, node_operator side, unsigned childIndex);
    bool expandFilterCondition(HqlExprArray & expanded, HqlExprArray & unexpanded, IHqlExpression * filter, bool moveOver, bool onlyKeyed);
    IHqlExpression * hoistFilterOverProject(IHqlExpression * transformed, bool onlyKeyed);
    IHqlExpression * getOptimizedFilter(IHqlExpression * transformed, bool alwaysTrue);
    IHqlExpression * getOptimizedFilter(IHqlExpression * transformed, HqlExprArray const & filters);
    IHqlExpression * createHoistedFilter(IHqlExpression * expr, HqlExprArray & conditions, unsigned childIndex, unsigned maxConditions);
    IHqlExpression * getHoistedFilter(IHqlExpression * transformed, bool canHoistLeft, bool canMergeLeft, bool canHoistRight, bool canMergeRight, unsigned conditionIndex);
    bool isComplexTransform(IHqlExpression * transform);
    IHqlExpression * inheritSkips(IHqlExpression * newTransform, IHqlExpression * oldTransform, IHqlExpression * oldSelector, IHqlExpression * newSelector);


    bool incUsage(IHqlExpression * expr);
    bool decUsage(IHqlExpression * expr);
    bool noteUnused(IHqlExpression * expr);
    bool alreadyHasUsage(IHqlExpression * expr);
    void recursiveDecUsage(IHqlExpression * expr);
    void recursiveDecChildUsage(IHqlExpression * expr);
    IHqlExpression * inheritUsage(IHqlExpression * newExpr, IHqlExpression * oldExpr);

    bool extractSingleFieldTempTable(IHqlExpression * expr, SharedHqlExpr & retField, SharedHqlExpr & retValues);

    IHqlExpression * optimizeAggregateCompound(IHqlExpression * transformed);
    IHqlExpression * optimizeAggregateUnsharedDataset(IHqlExpression * expr, bool isSimpleCount);
    IHqlExpression * optimizeJoinCondition(IHqlExpression * expr);
    IHqlExpression * optimizeDistributeDedup(IHqlExpression * expr);
    IHqlExpression * optimizeIf(IHqlExpression * expr);
    IHqlExpression * optimizeProjectInlineTable(IHqlExpression * transformed, bool childrenAreShared);
        
    inline const char * queryNode0Text(IHqlExpression * expr) { return queryChildNodeTraceText(nodeText[0], expr); }
    inline const char * queryNode1Text(IHqlExpression * expr) { return queryChildNodeTraceText(nodeText[1], expr); }

    inline bool isAlwaysLocal() const { return (options & HOOalwayslocal) != 0; }

protected:
    typedef NewHqlTransformer PARENT;
    IErrorReceiver & errorProcessor;
    unsigned options;
    StringBuffer nodeText[2];
};


class ExpandMonitor : public IExpandCallback
{
public:
    ExpandMonitor(CTreeOptimizer & _optimizer) : optimizer(_optimizer) { complex = false; }
    ~ExpandMonitor();

    virtual IHqlExpression * onExpandSelector();
    virtual void onDatasetChanged(IHqlExpression * newValue, IHqlExpression * oldValue);
    virtual void onUnknown() { complex = true; }

    inline bool isComplex()                                     { return complex; }
    inline void setComplex() { complex = true; }

protected:
    CTreeOptimizer & optimizer;
    bool complex;
    HqlExprArray datasetsChanged;
};

//The following class checks whether the expansion will be illegal - e.g., if a selector in the base dataset has been expanded
class ExpandSelectorMonitor : public ExpandMonitor
{
public:
    ExpandSelectorMonitor(CTreeOptimizer & _optimizer) : ExpandMonitor(_optimizer) {}

    virtual void onExpand(IHqlExpression * select, IHqlExpression * newValue) {}
};



//
//Following class is used to work out if something is worth expanding/combining.
//It is stricter than the ExpandSelectMonitor above.
//
class ExpandComplexityMonitor : public ExpandMonitor
{
public:
    ExpandComplexityMonitor(CTreeOptimizer & _optimizer) : ExpandMonitor(_optimizer) { }

    void analyseTransform(IHqlExpression * tranform);

    virtual void onExpand(IHqlExpression * select, IHqlExpression * newValue);
};


#endif
