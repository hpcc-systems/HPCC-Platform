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
#include "platform.h"
#include "jlib.hpp"

#include "hqlexpr.hpp"
#include "hqlattr.hpp"
#include "hqlmeta.hpp"
#include "hqlutil.hpp"
#include "hqlcpputil.hpp"
#include "hqlthql.hpp"
#include "hqlcatom.hpp"
#include "hqlfold.hpp"
#include "hqlcerrors.hpp"
#include "hqltrans.ipp"
#include "hqlpmap.hpp"
#include "hqltcppc.ipp"
#include "hqlttcpp.ipp"

#include "hqlresource.ipp"
#include "../../thorlcr/thorutil/thbufdef.hpp"

// See comment at the end of the file for an overview of the process performed by these classes.

#define MINIMAL_CHANGES
#define MAX_INLINE_COMMON_COUNT 5
//#define TRACE_RESOURCING
//#define VERIFY_RESOURCING
//#define SPOT_UNCONDITIONAL_CONDITIONS

#define DEFAULT_MAX_SOCKETS 2000 // configurable by setting max_sockets in .ini
#define DEFAULT_TOTAL_MEMORY ((1024*1024*1800))
#define FIXED_CLUSTER_SIZE 400
#define MEM_Const_Minimal (1*1024*1024)
#define DEFAULT_MAX_ACTIVITIES  100

static IHqlExpression * backtrackPseudoExpr;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    backtrackPseudoExpr = createAttribute(retryAtom);
    return true;
}
MODULE_EXIT()
{
    ::Release(backtrackPseudoExpr);
}

//---------------------------------------------------------------------------------------------------------------------

inline bool isAffectedByResourcing(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_record:
    case no_constant:
    case no_attr:
        return false;
    }
    return true;
}


static node_operator expandLists(node_operator op, HqlExprArray & args, IHqlExpression * expr);
static void expandChildren(node_operator op, HqlExprArray & args, IHqlExpression * expr)
{
    ForEachChild(idx, expr)
        expandLists(op, args, expr->queryChild(idx));
}

static node_operator expandLists(node_operator op, HqlExprArray & args, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_sequential:
    case no_orderedactionlist:
        if (op != no_parallel)
        {
            expandChildren(no_sequential, args, expr);
            return no_sequential;
        }
        break;
    case no_comma:
    case no_compound:
    case no_actionlist:
    case no_parallel:
        if (op != no_sequential)
        {
            expandChildren(no_parallel, args, expr);
            return no_parallel;
        }
        break;
    case no_null:
        return op;
    }
    args.append(*LINK(expr));
    return op;
}


//---------------------------------------------------------------------------------------------------------------------

CResourceOptions::CResourceOptions(ClusterType _targetClusterType, unsigned _clusterSize, const HqlCppOptions & _translatorOptions, UniqueSequenceCounter & _spillSequence)
: spillSequence(_spillSequence)
{
    filteredSpillThreshold = 0;
    minimizeSpillSize = 0;
    allowThroughSpill = false;
    allowThroughResult = false;
    cloneFilteredIndex = false;
    spillSharedConditionals = false;
    shareDontExpand = false;
    useGraphResults = false;
    noConditionalLinks = false;
    minimiseSpills = false;
    hoistResourced = false;
    isChildQuery = false;
    groupedChildIterators = false;
    allowSplitBetweenSubGraphs = false;
    preventKeyedSplit = false;
    preventSteppedSplit = false;
    minimizeSkewBeforeSpill = false;
    expandSingleConstRow = false;
    createSpillAsDataset = false;
    optimizeSharedInputs = false;
    combineSiblings = false;
    actionLinkInNewGraph = false;
    convertCompoundToExecuteWhen = false;
    useResultsForChildSpills = false;
    alwaysUseGraphResults = false;
    newBalancedSpotter = false;
    graphIdExpr = NULL;
    nextResult = 0;
    state.updateSequence = 0;

    targetClusterType = _targetClusterType;
    clusterSize = _clusterSize ? _clusterSize : FIXED_CLUSTER_SIZE;
    minimizeSpillSize = _translatorOptions.minimizeSpillSize;

    switch (targetClusterType)
    {
    case ThorLCRCluster:
        break;
    default:
        clusterSize = 1;
        break;
    }

    isChildQuery = false;
    filteredSpillThreshold = _translatorOptions.filteredReadSpillThreshold;
    allowThroughSpill = (targetClusterType != RoxieCluster) && (targetClusterType != ThorLCRCluster) && _translatorOptions.allowThroughSpill;
    allowThroughResult = (targetClusterType != RoxieCluster) && (targetClusterType != ThorLCRCluster);
    cloneFilteredIndex = (targetClusterType != RoxieCluster);
    spillSharedConditionals = (targetClusterType == RoxieCluster);
    shareDontExpand = (targetClusterType == RoxieCluster);
    graphIdExpr = NULL;

    //MORE The following doesn't always work - it gets sometimes confused about spill files - see latestheaderbuild for an example.
    //Try again once cloneConditionals is false for thor
    minimiseSpills = _translatorOptions.minimiseSpills;
    spillMultiCondition = _translatorOptions.spillMultiCondition;
    spotThroughAggregate = _translatorOptions.spotThroughAggregate && (targetClusterType != RoxieCluster) && (targetClusterType != ThorLCRCluster);
    noConditionalLinks = (targetClusterType == RoxieCluster) || ((targetClusterType != HThorCluster) && _translatorOptions.noConditionalLinks);
    hoistResourced = _translatorOptions.hoistResourced;
    alwaysUseGraphResults = _translatorOptions.alwaysUseGraphResults;
    useGraphResults = false;        // modified by later call
    groupedChildIterators = _translatorOptions.groupedChildIterators;
    allowSplitBetweenSubGraphs = false;//(targetClusterType == RoxieCluster);
    preventKeyedSplit = _translatorOptions.preventKeyedSplit;
    preventSteppedSplit = _translatorOptions.preventSteppedSplit;
    minimizeSkewBeforeSpill = _translatorOptions.minimizeSkewBeforeSpill;
    expandSingleConstRow = true;
    createSpillAsDataset = _translatorOptions.optimizeSpillProject && (targetClusterType != HThorCluster);
    combineSiblings = _translatorOptions.combineSiblingGraphs && (targetClusterType != HThorCluster) && (targetClusterType != RoxieCluster);
    optimizeSharedInputs = _translatorOptions.optimizeSharedGraphInputs && combineSiblings;
    actionLinkInNewGraph = _translatorOptions.actionLinkInNewGraph || (targetClusterType == HThorCluster);
    convertCompoundToExecuteWhen = false;
    useResultsForChildSpills = _translatorOptions.useResultsForChildSpills;
    newBalancedSpotter = _translatorOptions.newBalancedSpotter;
    alwaysReuseGlobalSpills = _translatorOptions.alwaysReuseGlobalSpills;
}

void CResourceOptions::setChildQuery(bool value)
{
    isChildQuery = value;
}

void CResourceOptions::setNewChildQuery(IHqlExpression * _graphIdExpr, unsigned _numResults)
{
    graphIdExpr = _graphIdExpr;
    nextResult = _numResults;
}

bool CResourceOptions::useGraphResult(bool linkedFromChild)
{
    if (useResultsForChildSpills && linkedFromChild)
        return true;

    if (alwaysUseGraphResults)
        return true;

    if (!useGraphResults)
        return false;

    if (linkedFromChild)
        return true;

    //Roxie converts spills into splitters, so best to retain them
    if (targetClusterType == RoxieCluster)
        return false;
    return true;
}

bool CResourceOptions::useGlobalResult(bool linkedFromChild)
{
    return (linkedFromChild && !useGraphResult(linkedFromChild));
}


//---------------------------------------------------------------------------------------------------------------------

inline bool projectSelectorDatasetToField(IHqlExpression * row)
{
    return ((row->getOperator() == no_selectnth) && getFieldCount(row->queryRecord()) > 1);
}

static IHqlExpression * skipScalarWrappers(IHqlExpression * value)
{
    loop
    {
        node_operator op = value->getOperator();
        if ((op != no_globalscope) && (op != no_thisnode) && (op != no_evalonce))
            return value;
        value = value->queryChild(0);
    }
}

static HqlTransformerInfo eclHoistLocatorInfo("EclHoistLocator");
class EclHoistLocator : public NewHqlTransformer
{
public:
    EclHoistLocator(ChildDependentArray & _matched) : NewHqlTransformer(eclHoistLocatorInfo), matched(_matched)
    {
        alwaysSingle = true;
    }

    void analyseChild(IHqlExpression * expr, bool _alwaysSingle)
    {
        alwaysSingle = _alwaysSingle;
        analyse(expr, 0);
    }

    void noteDataset(IHqlExpression * expr, IHqlExpression * hoisted, bool alwaysHoist)
    {
        unsigned match = matched.findOriginal(expr);
        if (match == NotFound)
        {
            if (!hoisted)
                hoisted = expr;

            CChildDependent & depend = * new CChildDependent(expr, hoisted, alwaysHoist, alwaysSingle, false);
            matched.append(depend);
        }
        else
        {
            CChildDependent & prev = matched.item(match);
            if (alwaysHoist && !prev.alwaysHoist)
                prev.alwaysHoist = true;
            if (alwaysSingle && !prev.isSingleNode)
                prev.isSingleNode = true;
        }
    }

    void noteScalar(IHqlExpression * expr, IHqlExpression * value)
    {
        unsigned match = matched.findOriginal(expr);
        if (match == NotFound)
        {
            value = skipScalarWrappers(value);

            OwnedHqlExpr hoisted;
            IHqlExpression * projected = NULL;
            if (value->getOperator() == no_select)
            {
                bool isNew;
                IHqlExpression * row = querySelectorDataset(value, isNew);
                if(isNew || row->isDatarow())
                {
                    if (projectSelectorDatasetToField(row))
                        projected = value;

                    hoisted.set(row);
                }
                else
                {
                    //very unusual - possibly a thisnode(myfield) hoisted from an allnodes
                    //use a createrow of a single element in the default behavour below
                }
            }
            else if (value->getOperator() == no_createset)
            {
                IHqlExpression * ds = value->queryChild(0);
                IHqlExpression * selected = value->queryChild(1);

                OwnedHqlExpr field;
                //Project down to a single field.so implicit fields can still be optimized
                if (selected->getOperator() == no_select)
                    field.set(selected->queryChild(1));
                else
                    field.setown(createField(valueId, selected->getType(), NULL));
                OwnedHqlExpr record = createRecord(field);
                OwnedHqlExpr self = getSelf(record);
                OwnedHqlExpr assign = createAssign(createSelectExpr(LINK(self), LINK(field)), LINK(selected));
                OwnedHqlExpr transform = createValue(no_newtransform, makeTransformType(record->getType()), LINK(assign));
                hoisted.setown(createDataset(no_newusertable, LINK(ds), createComma(LINK(record), LINK(transform))));
            }

            if (!hoisted)
            {
                OwnedHqlExpr field = createField(valueId, value->getType(), NULL);
                OwnedHqlExpr record = createRecord(field);
                OwnedHqlExpr self = getSelf(record);
                OwnedHqlExpr assign = createAssign(createSelectExpr(LINK(self), LINK(field)), LINK(value));
                OwnedHqlExpr transform = createValue(no_transform, makeTransformType(record->getType()), LINK(assign));
                hoisted.setown(createRow(no_createrow, LINK(transform)));
            }

            CChildDependent & depend = * new CChildDependent(expr, hoisted, true, true, true);
            depend.projected = projected;
            matched.append(depend);
        }
    }

protected:
    ChildDependentArray & matched;
    bool alwaysSingle;
};


class EclChildSplitPointLocator : public EclHoistLocator
{
public:
    EclChildSplitPointLocator(IHqlExpression * _original, HqlExprCopyArray & _selectors, ChildDependentArray & _matches, bool _groupedChildIterators)
    : EclHoistLocator(_matches), selectors(_selectors), groupedChildIterators(_groupedChildIterators)
    {
        original = _original;
        okToSelect = false;
        gathered = false;
        conditionalDepth = 0;
        executedOnce = false;
        switch (original->getOperator())
        {
        case no_call:
        case no_externalcall:
        case no_libraryscopeinstance:
            okToSelect = true;
            break;
        }
    }

    void findSplitPoints(IHqlExpression * expr, unsigned from, unsigned to, bool _alwaysSingle, bool _executedOnce)
    {
        alwaysSingle = _alwaysSingle;
        for (unsigned i=from; i < to; i++)
        {
            IHqlExpression * cur = expr->queryChild(i);
            executedOnce = _executedOnce || cur->isAttribute();     // assume attributes are only executed once.
            findSplitPoints(cur);
        }
        alwaysSingle = false;
    }

protected:
    void findSplitPoints(IHqlExpression * expr)
    {
        //containsNonActiveDataset() would be nice - but that isn't percolated outside assigns etc.
        if (containsAnyDataset(expr) || containsMustHoist(expr) || !expr->isIndependentOfScope())
        {
            if (!gathered)
            {
                gatherAmbiguousSelectors(original);
                gathered = true;
            }
            analyse(expr, 0);
        }
    }

    bool queryHoistDataset(IHqlExpression * ds)
    {
        bool alwaysHoist = true;
        if (executedOnce)
        {
            if (conditionalDepth != 0)
                alwaysHoist = false;
        }
        return alwaysHoist;
    }

    bool queryNoteDataset(IHqlExpression * ds)
    {
        bool alwaysHoist = queryHoistDataset(ds);
        //MORE: It should be possible to remove this condition, but it causes problems with resourcing hsss.xhql amongst others -> disable for the moment
        if (alwaysHoist)
            noteDataset(ds, ds, alwaysHoist);
        return alwaysHoist;
    }


    virtual void analyseExpr(IHqlExpression * expr)
    {
        if (alreadyVisited(expr))
            return;

        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_select:
            {
                bool isNew;
                IHqlExpression * ds = querySelectorDataset(expr, isNew);
                if (isNew)
                {
                    if (isEvaluateable(ds))
                    {
                        //MORE: Following isn't a very nice test - stops implicit denormalize getting messed up
                        if (expr->isDataset())
                            break;

                        //Debtable.....
                        //Don't hoist counts on indexes or dataset - it may mean they are evaluated more frequently than need be.
                        //If dependencies and root graphs are handled correctly this could be deleted.
                        if (isCompoundAggregate(ds))
                            break;

                        if (!expr->isDatarow() && !expr->isDataset() && !expr->isDictionary())
                        {
                            if (queryHoistDataset(ds))
                            {
                                noteScalar(expr, expr);
                                return;
                            }
                        }
                        else
                        {
                            if (queryNoteDataset(ds))
                                return;
                        }
                    }
                }
                break;
            }
        case no_createset:
            {
                IHqlExpression * ds = expr->queryChild(0);
                if (isEvaluateable(ds))
                {
                    if (queryHoistDataset(ds))
                    {
                        noteScalar(expr, expr);
//??                    queryNoteDataset(ds);
                        return;
                    }
                }
                break;
            }
        case no_assign:
            {
                IHqlExpression * rhs = expr->queryChild(1);
                //if rhs is a new, evaluatable, dataset then we want to add it
                if ((rhs->isDataset() || rhs->isDictionary()) && isEvaluateable(rhs))
                {
                    if (queryNoteDataset(rhs))
                        return;
                }
                break;
            }
        case no_sizeof:
        case no_allnodes:
        case no_nohoist:
        case no_forcegraph:
            return;
        case no_globalscope:
        case no_evalonce:
            if (expr->hasAttribute(optAtom) && !expr->isIndependentOfScope())
                break;
            if (expr->isDataset() || expr->isDatarow() || expr->isDictionary())
                noteDataset(expr, expr->queryChild(0), true);
            else
                noteScalar(expr, expr->queryChild(0));
            return;
        case no_thisnode:
            throwUnexpected();
        case no_getgraphresult:
            if (expr->hasAttribute(_streaming_Atom) || expr->hasAttribute(_distributed_Atom))
            {
                noteDataset(expr, expr, true);
                return;
            }
            break;
        case no_getgraphloopresult:
            noteDataset(expr, expr, true);
            return;
        case no_createdictionary:
            if (isEvaluateable(expr) && !isConstantDictionary(expr))
                noteDataset(expr, expr, true);
            return;

        case no_selectnth:
            if (expr->queryChild(1)->isConstant())
            {
                IHqlExpression * ds = expr->queryChild(0);
                switch (ds->getOperator())
                {
                case no_getgraphresult:
                    if (!expr->hasAttribute(_streaming_Atom) && !expr->hasAttribute(_distributed_Atom))
                        break;
                    //fallthrough
                case no_getgraphloopresult:
                    noteDataset(expr, expr, true);
                    return;
                }
            }
            break;
        }

        bool wasOkToSelect = okToSelect;
        if (expr->isDataset())
        {
            switch (expr->getOperator())
            {
            case no_compound_diskread:
            case no_compound_disknormalize:
            case no_compound_diskaggregate:
            case no_compound_diskcount:
            case no_compound_diskgroupaggregate:
            case no_compound_indexread:
            case no_compound_indexnormalize:
            case no_compound_indexaggregate:
            case no_compound_indexcount:
            case no_compound_indexgroupaggregate:
            case no_compound_childread:
            case no_compound_childnormalize:
            case no_compound_childaggregate:
            case no_compound_childcount:
            case no_compound_childgroupaggregate:
            case no_compound_selectnew:
            case no_compound_inline:
            case no_newkeyindex:
            case no_keyindex:
            case no_table:
                okToSelect = false;
                break;
            }

            if (okToSelect && isEvaluateable(expr))
            {
                if (queryNoteDataset(expr))
                    return;
            }
        }
        else
            okToSelect = true;

        switch (op)
        {
        case no_if:
        case no_choose:
        case no_chooseds:
            {
                IHqlExpression * cond = expr->queryChild(0);
                analyseExpr(cond);
                if (expr->isDataset() || expr->isDatarow() || expr->isDictionary() || expr->isAction())
                    conditionalDepth++;
                doAnalyseChildren(expr, 1);
                if (expr->isDataset() || expr->isDatarow() || expr->isDictionary() || expr->isAction())
                    conditionalDepth--;
                break;
            }
        case no_mapto:
            {
                analyseExpr(expr->queryChild(0));
                if (expr->isDataset() || expr->isDatarow() || expr->isDictionary())
                    conditionalDepth++;
                analyseExpr(expr->queryChild(1));
                if (expr->isDataset() || expr->isDatarow() || expr->isDictionary())
                    conditionalDepth--;
                break;
            }
        case no_attr_expr:
            //Ignore internal tracking attributes e.g., _selectors_Atom
            if (!isInternalAttributeName(expr->queryName()))
            {
                //Default action for no_attr_expr is to not walk children, but we need to here.
                bool wasExecutedOnce = executedOnce;
                executedOnce = true;
                analyseChildren(expr);
                executedOnce = wasExecutedOnce;
            }
            break;
        default:
            NewHqlTransformer::analyseExpr(expr);
            break;
        }

        okToSelect = wasOkToSelect;
    }

    bool isCompoundAggregate(IHqlExpression * ds)
    {
        return false;
        //Generates worse code unless we take into account whether or not the newDisk operation flags are enabled.
        if (!isTrivialSelectN(ds))
            return false;
        IHqlExpression * agg = ds->queryChild(0);
        if (isSimpleCountAggregate(agg, true))
            return true;
        return false;
    }

    void gatherAmbiguousSelectors(IHqlExpression * expr)
    {
        //Horrible code to try and cope with ambiguous left selectors.
        //o Tree is ambiguous so same child expression can occur in different contexts - so can't depend on the context it is found in to work out if can hoist
        //o If any selector that is hidden within child expressions matches one in scope then can't hoist it.
        //If the current expression creates a selector, then can't hoist anything that depends on it [only add to hidden if in selectors to reduce searching]
        //o Want to hoist as much as possible.
        if (selectors.empty())
            return;

        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        unsigned max = expr->numChildren();
        unsigned i;
        HqlExprCopyArray hiddenSelectors;
        for (i = 0; i < first; i++)
            expr->queryChild(i)->gatherTablesUsed(&hiddenSelectors, NULL);
        for (i = last; i < max; i++)
            expr->queryChild(i)->gatherTablesUsed(&hiddenSelectors, NULL);

        ForEachItemIn(iSel, selectors)
        {
            IHqlExpression & cur = selectors.item(iSel);
            if (hiddenSelectors.contains(cur))
                ambiguousSelectors.append(cur);
        }

        switch (getChildDatasetType(expr))
        {
        case childdataset_datasetleft:
        case childdataset_left:
            {
                IHqlExpression * ds = expr->queryChild(0);
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
                if (selectors.contains(*left))
                    ambiguousSelectors.append(*left);
                break;
            }
        case childdataset_same_left_right:
        case childdataset_top_left_right:
        case childdataset_nway_left_right:
            {
                IHqlExpression * ds = expr->queryChild(0);
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
                OwnedHqlExpr right = createSelector(no_right, ds, selSeq);
                if (selectors.contains(*left))
                    ambiguousSelectors.append(*left);
                if (selectors.contains(*right))
                    ambiguousSelectors.append(*right);
                break;
            }
        case childdataset_leftright:
            {
                IHqlExpression * leftDs = expr->queryChild(0);
                IHqlExpression * rightDs = expr->queryChild(1);
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr left = createSelector(no_left, leftDs, selSeq);
                OwnedHqlExpr right = createSelector(no_right, rightDs, selSeq);
                if (selectors.contains(*left))
                    ambiguousSelectors.append(*left);
                if (selectors.contains(*right))
                    ambiguousSelectors.append(*right);
                break;
            }
        }
    }

    bool isEvaluateable(IHqlExpression * ds, bool ignoreInline = false)
    {
        //Don't hoist an alias - it could create unnecessary duplicate spills - hoist its input
        if (ds->getOperator() == no_dataset_alias)
            return false;

        //Not allowed to hoist
        if (isContextDependent(ds, (conditionalDepth == 0), true))
            return false;

        //MORE: Needs more work for child queries - need a GroupedChildIterator activity
        if (isGrouped(ds) && selectors.ordinality() && !groupedChildIterators)
            return false;

        //Check datasets are available
        HqlExprCopyArray scopeUsed;
        ds->gatherTablesUsed(NULL, &scopeUsed);
        ForEachItemIn(i, scopeUsed)
        {
            IHqlExpression & cur = scopeUsed.item(i);
            if (!selectors.contains(cur))
                return false;

            if (ambiguousSelectors.contains(cur))
                return false;
        }

        if (!isEfficientToHoistDataset(ds, ignoreInline))
            return false;

        return true;
    }

    bool isEfficientToHoistDataset(IHqlExpression * ds, bool ignoreInline) const
    {
        //MORE: This whole function could do with some significant improvements.  Whether it is inefficient to hoist
        //depends on at least the following...
        //a) cost of serializing v cost of re-evaluating (which can depend on the engine).
        //b) How many times it will be evaluated in the child context
        if (ds->getOperator() == no_createdictionary)
            return true;

        if (isInlineTrivialDataset(ds))
            return false;

#ifdef MINIMAL_CHANGES
        if (!ignoreInline)
        {
            //Generally this appears to be better to hoist since it involves calling a transform.
            if (ds->getOperator() == no_dataset_from_transform)
                return true;

            if (canProcessInline(NULL, ds))
                return false;
        }
#endif

        return true;
    }

protected:
    IHqlExpression * original;
    HqlExprCopyArray & selectors;
    HqlExprCopyArray ambiguousSelectors;
    unsigned conditionalDepth;
    bool okToSelect;
    bool gathered;
    bool groupedChildIterators;
    bool executedOnce;
};


class EclThisNodeLocator : public EclHoistLocator
{
public:
    EclThisNodeLocator(ChildDependentArray & _matches)
        : EclHoistLocator(_matches)
    {
        allNodesDepth = 0;
    }

protected:
    virtual void analyseExpr(IHqlExpression * expr)
    {
        //NB: This doesn't really work for no_thisnode occurring in multiple contexts.  We should probably hoist it from everywhere if it is hoistable from anywhere,
        //    although theoretically that gives us problems with ambiguous selectors.
        if (alreadyVisited(expr) || !containsThisNode(expr))
            return;

        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_allnodes:
            allNodesDepth++;
            NewHqlTransformer::analyseExpr(expr);
            allNodesDepth--;
            return;
        case no_thisnode:
            if (allNodesDepth == 0)
            {
                if (expr->isDataset() || expr->isDatarow() || expr->isDictionary())
                    noteDataset(expr, expr->queryChild(0), true);
                else
                    noteScalar(expr, expr->queryChild(0));
                return;
            }
            allNodesDepth--;
            NewHqlTransformer::analyseExpr(expr);
            allNodesDepth++;
            return;
        }
        NewHqlTransformer::analyseExpr(expr);
    }

protected:
    unsigned allNodesDepth;
};


static HqlTransformerInfo childDependentReplacerInfo("ChildDependentReplacer");
class ChildDependentReplacer : public MergingHqlTransformer
{
public:
    ChildDependentReplacer(const HqlExprCopyArray & _childDependents, const HqlExprArray & _replacements)
        : MergingHqlTransformer(childDependentReplacerInfo), childDependents(_childDependents), replacements(_replacements)
    {
    }

protected:
    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        unsigned match = childDependents.find(*expr);
        if (match != NotFound)
            return LINK(&replacements.item(match));

        return MergingHqlTransformer::createTransformed(expr);
    }

protected:
    const HqlExprCopyArray & childDependents;
    const HqlExprArray & replacements;
};


//---------------------------------------------------------------------------------------------------------------------

bool canUseResultInChildQuery(IHqlExpression * expr)
{
    return (expr->getOperator() == no_setgraphresult);
}

class ActivityInvariantInfo : public SpillerInfo
{
public:
    ActivityInvariantInfo(IHqlExpression * _original, CResourceOptions * _options) : SpillerInfo(_original, _options)
    {
        containsActivity = false;
        isActivity = false;
        visited = false;
        isAlreadyInScope = false;
        projectResult = true; // projected must also be non empty to actually project this dataset
    }

    void addProjected(IHqlExpression * next)
    {
        if (projectResult && !projected.contains(*next))
            projected.append(*LINK(next));
    }

    void clearProjected()
    {
        projectResult = false;
        projected.kill();
    }

    inline bool isResourcedActivity() const
    {
        return isActivity || containsActivity;
    }

    void noteUsedFromChild()
    {
        linkedFromChild = true;
        if (outputToUseForSpill && !canUseResultInChildQuery(outputToUseForSpill))
            outputToUseForSpill = NULL;
    }

public:
    HqlExprArray projected;
    HqlExprAttr projectedExpr;
    ChildDependentArray childDependents;
    HqlExprAttr hoistedRead;
    bool containsActivity:1;
    bool isActivity:1;
    bool visited:1;
    bool isAlreadyInScope:1;
    bool projectResult:1;
};

//Spot expressions that are invariant within an activity, and so would be more efficient if only executed once.
//MORE: Should there be an activity traversal base class?
static HqlTransformerInfo activityInvariantHoisterInfo("ActivityInvariantHoister");
class ActivityInvariantHoister : public NewHqlTransformer
{
public:
    ActivityInvariantHoister(CResourceOptions & _options)
    : NewHqlTransformer(activityInvariantHoisterInfo), options(_options)
    {
    }
    void tagActiveCursors(HqlExprCopyArray * activeRows);
    void transformRoot(const HqlExprArray & in, HqlExprArray & out);
    IHqlExpression * transformRoot(IHqlExpression * expr);

protected:
    void gatherChildSplitPoints(IHqlExpression * expr, ActivityInvariantInfo * info, unsigned first, unsigned last);
    bool findSplitPoints(IHqlExpression * expr, bool isProjected);
    void extendSplitPoints();
    IHqlExpression * projectChildDependent(IHqlExpression * expr);
    void projectChildDependents();
    void findSplitPoints(const HqlExprArray & exprs);

    IHqlExpression * createTransformed(IHqlExpression * expr);
    IHqlExpression * replaceResourcedReferences(ActivityInvariantInfo * info, IHqlExpression * expr);
    void doTransformRoot(const HqlExprArray & in);
    void ensureHoisted(IHqlExpression * expr, bool isSingleNode);

    static inline bool isResourcedActivity(IHqlExpression * expr)
    {
        if (!expr)
            return false;
        ActivityInvariantInfo * extra = queryOptBodyInfo(expr);
        return extra && extra->isResourcedActivity();
    }

    bool isWorthForcingHoist(IHqlExpression * expr)
    {
        loop
        {
            switch (expr->getOperator())
            {
            case no_selectnth:
            case no_filter:
            case no_newaggregate:
                if (isResourcedActivity(expr->queryChild(0)))
                    return true;
                break;
            default:
                return false;
            }
            expr = expr->queryChild(0);
        }
    }

//NewHqlTransformer
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr)
    {
        return new ActivityInvariantInfo(expr, &options);
    }
    ActivityInvariantInfo * queryBodyInfo(IHqlExpression * expr)
    {
        return static_cast<ActivityInvariantInfo *>(queryTransformExtra(expr->queryBody()));
    }
    static ActivityInvariantInfo * queryOptBodyInfo(IHqlExpression * expr)
    {
        return static_cast<ActivityInvariantInfo *>(expr->queryBody()->queryTransformExtra());
    }

private:
    HqlExprArray result;
    CResourceOptions & options;
    HqlExprCopyArray activeSelectors;
    ChildDependentArray allChildDependents;
};

void ActivityInvariantHoister::doTransformRoot(const HqlExprArray & in)
{
    findSplitPoints(in);

    //If no child dependents have been found then copy instead of transforming
    if (allChildDependents.ordinality() != 0)
    {
        ForEachItemIn(i, in)
            result.append(*doTransformRootExpr(&in.item(i)));
    }
    else
    {
        ForEachItemIn(i, in)
            result.append(*LINK(&in.item(i)));
    }
}

void ActivityInvariantHoister::tagActiveCursors(HqlExprCopyArray * activeRows)
{
    if (!activeRows)
        return;

    ForEachItemIn(i, *activeRows)
    {
        IHqlExpression & cur = activeRows->item(i);
        activeSelectors.append(cur);
        queryBodyInfo(&cur)->isAlreadyInScope = true;
    }
}


void ActivityInvariantHoister::transformRoot(const HqlExprArray & in, HqlExprArray & out)
{
    doTransformRoot(in);
    return out.swapWith(result);
}


IHqlExpression * ActivityInvariantHoister::transformRoot(IHqlExpression * expr)
{
    HqlExprArray exprs;
    node_operator expandOp = options.isChildQuery ? no_any : no_parallel;
    node_operator createOp = expandLists(expandOp, exprs, expr);

    doTransformRoot(exprs);

    switch (createOp)
    {
    case no_sequential:
    case no_orderedactionlist:
        return createCompound(no_orderedactionlist, result);
    default:
        return createCompound(no_actionlist, result);
    }
}


void ActivityInvariantHoister::gatherChildSplitPoints(IHqlExpression * expr, ActivityInvariantInfo * info, unsigned first, unsigned last)
{
    //NB: Don't call member functions to ensure correct nesting of transform mutexes.
    EclChildSplitPointLocator locator(expr, activeSelectors, info->childDependents, options.groupedChildIterators);
    unsigned max = expr->numChildren();

    //If child queries are supported then don't hoist the expressions if they might only be evaluated once
    //because they may be conditional
    bool alwaysOnce = false;
    switch (expr->getOperator())
    {
    case no_setresult:
    case no_selectnth:
        //set results, only done once=>don't hoist conditionals
        locator.findSplitPoints(expr, last, max, true, true);
        return;
    case no_createrow:
    case no_datasetfromrow:
    case no_projectrow:
        alwaysOnce = true;
        break;
    case no_loop:
        if ((options.targetClusterType == ThorLCRCluster) && !options.isChildQuery)
        {
            //This is ugly!  The body is executed in parallel, so don't force that as a work unit result
            //It means some child query expressions within loops don't get forced into work unit writes
            //but that just means that the generated code will be not as good as it could be.
            const unsigned bodyArg = 4;
            locator.findSplitPoints(expr, 1, bodyArg, true, false);
            locator.findSplitPoints(expr, bodyArg, bodyArg+1, false, false);
            locator.findSplitPoints(expr, bodyArg+1, max, true, false);
            return;
        }
        break;
    }
    locator.findSplitPoints(expr, 0, first, true, true);          // IF() conditions only evaluated once... => don't force
    locator.findSplitPoints(expr, last, max, true, alwaysOnce);
}



bool ActivityInvariantHoister::findSplitPoints(IHqlExpression * expr, bool isProjected)
{
    ActivityInvariantInfo * info = queryBodyInfo(expr);
    if (info && info->visited)
    {
        if (!isProjected)
            info->clearProjected();

        if (info->isAlreadyInScope || info->isActivity || !info->containsActivity)
            return info->containsActivity;
    }
    else
    {
        info->visited = true;
        if (!isProjected)
            info->clearProjected();

        bool isActivity = true;
        switch (expr->getOperator())
        {
        case no_select:
            //either a select from a setresult or use of a child-dataset
            if (isNewSelector(expr))
            {
                info->containsActivity = findSplitPoints(expr->queryChild(0), false);
                assertex(queryBodyInfo(expr->queryChild(0))->isActivity);
            }
            if (expr->isDataset() || expr->isDatarow())
            {
                info->isActivity = true;
                info->containsActivity = true;
            }
            return info->containsActivity;
        case no_setgraphresult:
            {
                IHqlExpression * dataset = expr->queryChild(0);
                ActivityInvariantInfo * childInfo = queryBodyInfo(dataset);
                childInfo->setPotentialSpillFile(expr);
                break;
            }
        case no_mapto:
            throwUnexpected();
        case no_activerow:
            info->isActivity = true;
            info->containsActivity = false;
            return false;
        case no_rowset:                         // don't resource this as an activity
            isActivity = false;
            break;
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
        case no_getgraphloopresultset:
            info->isActivity = false;
            info->containsActivity = false;
            return false;
        case no_datasetlist:
            isActivity = false;
            break;
        case no_rowsetrange:
            {
                //Don't resource this as an activity if it is a function of the input graph rows,
                //however we do want to if it is coming from a dataset list.
                IHqlExpression * ds = expr->queryChild(0);
                //MORE: Should walk further down the tree to allow for nested rowsetranges etc.
                if (ds->getOperator() == no_rowset || ds->getOperator() == no_getgraphloopresultset)
                {
                    info->isActivity = false;
                    info->containsActivity = false;
                    return false;
                }
                isActivity = false;
                break;
            }
        }

        ITypeInfo * type = expr->queryType();
        if (!type || type->isScalar())
            return false;

        info->isActivity = isActivity;
        info->containsActivity = true;
    }

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);

    if (options.hoistResourced)
    {
        switch (expr->getOperator())
        {
        case no_allnodes:
            {
                //MORE: This needs to recursively walk and lift any contained no_selfnode, but don't go past another nested no_allnodes;
                EclThisNodeLocator locator(info->childDependents);
                locator.analyseChild(expr->queryChild(0), true);
                break;
            }
        case no_childquery:
            throwUnexpected();
        default:
            {
                for (unsigned idx=first; idx < last; idx++)
                {
                    IHqlExpression * cur = expr->queryChild(idx);
                    findSplitPoints(cur, false);
                }
                gatherChildSplitPoints(expr, info, first, last);
                break;
            }
        }

        ForEachItemIn(i2, info->childDependents)
        {
            CChildDependent & cur = info->childDependents.item(i2);
            ActivityInvariantInfo * hoistedInfo = queryBodyInfo(cur.hoisted);
            if (cur.projected)
                hoistedInfo->addProjected(cur.projected);
            else
                hoistedInfo->clearProjected();

            if (cur.alwaysHoist)
                findSplitPoints(cur.hoisted, (cur.projected != NULL));

            allChildDependents.append(OLINK(cur));
        }
    }
    else
    {
        for (unsigned idx=first; idx < last; idx++)
            findSplitPoints(expr->queryChild(idx), false);
    }

    return info->containsActivity;
}


void ActivityInvariantHoister::extendSplitPoints()
{
    //NB: findSplitPoints might call this array to be extended
    for (unsigned i1=0; i1 < allChildDependents.ordinality(); i1++)
    {
        CChildDependent & cur = allChildDependents.item(i1);
        if (!cur.alwaysHoist && isWorthForcingHoist(cur.hoisted))
            findSplitPoints(cur.hoisted, (cur.projected != NULL));
    }
}

IHqlExpression * ActivityInvariantHoister::projectChildDependent(IHqlExpression * expr)
{
    ActivityInvariantInfo * info = queryBodyInfo(expr);
    if (!info || !info->projectResult || info->projected.empty())
        return expr;

    if (info->projectedExpr)
        return info->projectedExpr;

    assertex(expr->getOperator() == no_selectnth);
    IHqlExpression * row = expr;
    assertex(projectSelectorDatasetToField(row));

    unsigned totalFields = getFieldCount(row->queryRecord());
    if (totalFields == info->projected.ordinality())
    {
        info->clearProjected();
        return expr;
    }

    //Create a projection containing each of the fields that are used from the child queries.
    IHqlExpression * ds = row->queryChild(0);
    HqlExprArray fields;
    HqlExprArray values;
    ForEachItemIn(i, info->projected)
    {
        IHqlExpression * value = &info->projected.item(i);
        IHqlExpression * field = value->queryChild(1);
        LinkedHqlExpr projectedField = field;
        //Check for a very unusual situation where the same field is projected from two different sub records
        while (fields.contains(*projectedField))
            projectedField.setown(cloneFieldMangleName(field));  // Generates a new mangled name each time

        OwnedHqlExpr activeDs = createRow(no_activetable, LINK(ds->queryNormalizedSelector()));
        fields.append(*LINK(projectedField));
        values.append(*replaceSelector(value, row, activeDs));
    }

    OwnedHqlExpr projectedRecord = createRecord(fields);
    OwnedHqlExpr self = getSelf(projectedRecord);
    HqlExprArray assigns;
    ForEachItemIn(i2, fields)
    {
        IHqlExpression * field = &fields.item(i2);
        IHqlExpression * value = &values.item(i2);
        assigns.append(*createAssign(createSelectExpr(LINK(self), LINK(field)), LINK(value)));
    }
    OwnedHqlExpr transform = createValue(no_newtransform, makeTransformType(projectedRecord->getType()), assigns);
    OwnedHqlExpr projectedDs = createDataset(no_newusertable, LINK(ds), createComma(LINK(projectedRecord), LINK(transform)));
    info->projectedExpr.setown(replaceChild(row, 0, projectedDs));

    findSplitPoints(info->projectedExpr, false);

    //Ensure child dependents that are no longer used are not processed - otherwise the usage counts will be wrong.
    ForEachItemIn(ic, info->childDependents)
    {
        CChildDependent & cur = info->childDependents.item(ic);
        //Clearing these ensures that isResourcedActivity returns false;
        cur.hoisted.clear();
        cur.projectedHoisted.clear();
    }

    return info->projectedExpr;
}

void ActivityInvariantHoister::projectChildDependents()
{
    ForEachItemIn(i2, allChildDependents)
    {
        CChildDependent & cur = allChildDependents.item(i2);
        if (isResourcedActivity(cur.hoisted))
            cur.projectedHoisted.set(projectChildDependent(cur.hoisted));
    }
}

void ActivityInvariantHoister::findSplitPoints(const HqlExprArray & exprs)
{
    //Finding items that should be hoisted from child queries, and evaluated at this level is tricky:
    //* After hoisting a child expression, that may in turn contain other child expressions.
    //* Some child expressions are only worth hoisting if they are a simple function of something
    //  that will be evaluated here.
    //* If we're creating a single project for each x[1] then that needs to be done after all the
    //  expressions to hoist have been found.
    //* The usage counts have to correct - including
    //* The select child dependents for a ds[n] need to be inherited by the project(ds)[n]
    //=>
    //First walk the expression tree gathering all the expressions that will be used.
    //Project the expressions that need projecting.
    ForEachItemIn(idx, exprs)
        findSplitPoints(&exprs.item(idx), false);

    extendSplitPoints();
    projectChildDependents();
}



static IHqlExpression * getScalarReplacement(CChildDependent & cur, ActivityInvariantInfo * hoistedInfo, IHqlExpression * replacement)
{
    //First skip any wrappers which are there to cause things to be hoisted.
    IHqlExpression * value = skipScalarWrappers(cur.original);

    //Now modify the spilled result depending on how the spilled result was created (see EclHoistLocator::noteScalar() above)
    if (value->getOperator() == no_select)
    {
        bool isNew;
        IHqlExpression * ds = querySelectorDataset(value, isNew);
        if(isNew || ds->isDatarow())
        {
            if (cur.hoisted != cur.projectedHoisted)
            {
                assertex(cur.projected);
                unsigned match = hoistedInfo->projected.find(*cur.projected);
                assertex(match != NotFound);
                IHqlExpression * projectedRecord = cur.projectedHoisted->queryRecord();
                IHqlExpression * projectedField = projectedRecord->queryChild(match);
                return createNewSelectExpr(LINK(replacement), LINK(projectedField));
            }
            return replaceSelectorDataset(value, replacement);
        }
        //Very unusual - can occur when a thisnode(somefield) is extracted from an allnodes.
        //It will have gone through the default case in the noteScalar() code
    }
    else if (value->getOperator() == no_createset)
    {
        IHqlExpression * record = replacement->queryRecord();
        IHqlExpression * field = record->queryChild(0);
        return createValue(no_createset, cur.original->getType(), LINK(replacement), createSelectExpr(LINK(replacement->queryNormalizedSelector()), LINK(field)));
    }

    IHqlExpression * record = replacement->queryRecord();
    return createNewSelectExpr(LINK(replacement), LINK(record->queryChild(0)));
}


IHqlExpression * ActivityInvariantHoister::replaceResourcedReferences(ActivityInvariantInfo * info, IHqlExpression * expr)
{
    if (!isAffectedByResourcing(expr))
        return LINK(expr);

    LinkedHqlExpr mapped = expr;
    if (info && (info->childDependents.ordinality()))
    {
        HqlExprCopyArray originals;
        HqlExprArray replacements;
        ForEachItemIn(i, info->childDependents)
        {
            CChildDependent & cur = info->childDependents.item(i);
            if (!isResourcedActivity(cur.projectedHoisted))
                continue;

            ActivityInvariantInfo * info = queryBodyInfo(cur.projectedHoisted);
            LinkedHqlExpr replacement = info->hoistedRead;
            assertex(replacement);

            IHqlExpression * original = cur.original;
            if (!original->isDataset() && !original->isDatarow() && !original->isDictionary())
                replacement.setown(getScalarReplacement(cur, queryBodyInfo(cur.hoisted), replacement));

            originals.append(*original);
            replacements.append(*replacement.getClear());
        }

        if (originals.ordinality())
        {
            ChildDependentReplacer replacer(originals, replacements);
            mapped.setown(replacer.transformRoot(mapped));
        }
    }
    return mapped.getClear();
}

IHqlExpression * ActivityInvariantHoister::createTransformed(IHqlExpression * expr)
{
    ActivityInvariantInfo * info = queryBodyInfo(expr);
    if (!info || !info->containsActivity)
        return replaceResourcedReferences(info, expr);

    ForEachItemIn(i2, info->childDependents)
    {
        CChildDependent & cur = info->childDependents.item(i2);
        ensureHoisted(cur.projectedHoisted, cur.isSingleNode);
    }

    node_operator op = expr->getOperator();
    HqlExprArray args;
    OwnedHqlExpr transformed;
    bool same = true;
    if (!info->isActivity)
    {
        ForEachChild(idx, expr)
        {
            IHqlExpression * child = expr->queryChild(idx);
            IHqlExpression * resourced = transform(child);
            if (child != resourced)
                same = false;
            args.append(*resourced);
        }
    }
    else
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        switch (op)
        {
        case no_if:
        case no_choose:
        case no_chooseds:
            {
                ForEachChild(idx, expr)
                {
                    IHqlExpression * child = expr->queryChild(idx);
                    IHqlExpression * resourced;
                    if ((idx < first) || (idx >= last))
                        resourced = replaceResourcedReferences(info, child);
                    else
                        resourced = transform(child);
                    if (child != resourced)
                        same = false;
                    args.append(*resourced);
                }
                break;
            }
        case no_case:
        case no_map:
            UNIMPLEMENTED;
        case no_keyed:
            return LINK(expr);
        case no_compound:
        case no_executewhen:
            same = transformChildren(expr, args);
            break;
        case no_select:
            {
                //If this isn't a new selector, then it must be <LEFT|RIGHT>.child-dataset, which will not be mapped
                //and the dataset will not have been resourced
                if (isNewSelector(expr))
                {
                    IHqlExpression * ds = expr->queryChild(0);
                    OwnedHqlExpr newDs = transform(ds);

                    if (ds != newDs)
                    {
                        args.append(*LINK(newDs));
                        unwindChildren(args, expr, 1);
                        if (!expr->hasAttribute(newAtom) && (newDs->getOperator() != no_select))
                            args.append(*LINK(queryNewSelectAttrExpr()));
                        same = false;
                    }
                }
                break;
            }
        case no_join:
        case no_denormalize:
        case no_denormalizegroup:
            if (false)//if (isKeyedJoin(expr))
            {
                args.append(*transform(expr->queryChild(0)));
                args.append(*LINK(expr->queryChild(1)));
                unsigned max = expr->numChildren();
                for (unsigned idx=2; idx < max; idx++)
                    args.append(*replaceResourcedReferences(info, expr->queryChild(idx)));
                same = false;
                break;
            }
            //fall through...
        default:
            {
                IHqlExpression * activeTable = NULL;
                // Check to see if the activity has a dataset which is in scope for the rest of its arguments.
                // If so we'll need to remap references from the children.
                if (hasActiveTopDataset(expr) && (first != last))
                    activeTable = expr->queryChild(0);

                ForEachChild(idx, expr)
                {
                    IHqlExpression * child = expr->queryChild(idx);
                    IHqlExpression * resourced;
                    if ((idx < first) || (idx >= last))
                    {
                        LinkedHqlExpr mapped = child;
                        if (activeTable && isAffectedByResourcing(child))
                        {
                            IHqlExpression * activeTableTransformed = &args.item(0);
                            if (activeTable != activeTableTransformed)
                                mapped.setown(scopedReplaceSelector(child, activeTable, activeTableTransformed));
                        }
                        resourced = replaceResourcedReferences(info, mapped);
                    }
                    else
                        resourced = transform(child);
                    if (child != resourced)
                        same = false;
                    args.append(*resourced);
                }
            }
            break;
        }
    }

    if (!transformed)
        transformed.setown(same ? LINK(expr) : expr->clone(args));

    return transformed.getClear();
}

void ActivityInvariantHoister::ensureHoisted(IHqlExpression * expr, bool isSingleNode)
{
    ActivityInvariantInfo * info = queryBodyInfo(expr);
    if (!info->hoistedRead)
    {
        if (isSingleNode)
            info->noteUsedFromChild();

        OwnedHqlExpr hoisted = transform(expr);
        if (!info->queryOutputSpillFile())
        {
            //NOTE: ActivityInvariantHoister does not create no_readspill etc., because the transformation in
            result.append(*info->createSpilledWrite(hoisted, false));
        }

        //Create the read activity which will be substituted where it was used.
        OwnedHqlExpr hoistedRead = info->createSpilledRead(hoisted);
        info->hoistedRead.setown(expr->cloneAllAnnotations(hoistedRead));
    }
}

//---------------------------------------------------------------------------------------------------------------------

//=== The following provides information about how each kind of activity is resourced ====

static void setHashResources(IHqlExpression * expr, CResources & resources, const CResourceOptions & options)
{
    unsigned memneeded = MEM_Const_Minimal+DISTRIBUTE_RESMEM(resources.clusterSize);
    resources.set(RESslavememory, memneeded).set(REShashdist, 1);
}


//MORE: Use a single function to map an IHqlExpression to an activity kind.
void getResources(IHqlExpression * expr, CResources & resources, const CResourceOptions & options)
{
    //MORE: What effect should a child query have?  Definitely the following, but what about other resourcing?
    if (options.isChildQuery || queryHint(expr, lightweightAtom))
    {
        resources.setLightweight();
        return;
    }

    bool isLocal = isLocalActivity(expr);
    bool isGrouped = isGroupedActivity(expr);
    switch (expr->getOperator())
    {
    case no_join:
    case no_selfjoin:
    case no_denormalize:
    case no_denormalizegroup:
        if (isKeyedJoin(expr) || expr->hasAttribute(_lightweight_Atom))
            resources.setLightweight();
        else if (expr->hasAttribute(lookupAtom))
        {
            if (expr->hasAttribute(fewAtom))
            {
                resources.setLightweight().set(RESslavememory, MEM_Const_Minimal+LOOKUPJOINL_SMART_BUFFER_SIZE);
                resources.setManyToMasterSockets(1);
            }
            else
            {
                resources.setHeavyweight().set(RESslavememory, MEM_Const_Minimal+LOOKUPJOINL_SMART_BUFFER_SIZE);
                resources.setManyToMasterSockets(1);
            }
        }
        else if (expr->hasAttribute(smartAtom))
        {
            resources.setHeavyweight();
            setHashResources(expr, resources, options);
        }
        else if (expr->hasAttribute(hashAtom))
        {
            resources.setHeavyweight();
            setHashResources(expr, resources, options);
        }
        else
        {
            resources.setHeavyweight().set(RESslavememory, MEM_Const_Minimal+SORT_BUFFER_TOTAL+JOIN_SMART_BUFFER_SIZE);
            if (!isLocal)
            {
#ifndef SORT_USING_MP
                resources.setManyToManySockets(2);
#endif
            }
        }
        break;
    case no_dedup:
        if (isGrouped || (!expr->hasAttribute(allAtom) && !expr->hasAttribute(hashAtom)))
        {
            resources.setLightweight();
            if (!isGrouped && !isLocal)
            {
                resources.set(RESslavememory, MEM_Const_Minimal+DEDUP_SMART_BUFFER_SIZE);
                resources.setManyToMasterSockets(1);
            }
        }
        else if (isLocal)
        {
            resources.setHeavyweight().set(RESslavememory, MEM_Const_Minimal+DEDUP_SMART_BUFFER_SIZE);
            //This can't be right....
            //resources.setManyToMasterSockets(1);
        }
        else
        {
            //hash dedup.
            resources.setHeavyweight();
            setHashResources(expr, resources, options);
        }
        break;
    case no_rollup:
        resources.setLightweight();
        if (!isGrouped && !isLocal)
        {
            resources.set(RESslavememory, MEM_Const_Minimal+DEDUP_SMART_BUFFER_SIZE);
            //MORE: Is this still correct?
            resources.setManyToMasterSockets(1);
        }
        break;
    case no_distribute:
    case no_keyeddistribute:
        resources.setLightweight();
        setHashResources(expr, resources, options);
        break;
    case no_subsort:
        if (expr->hasAttribute(manyAtom))
            resources.setHeavyweight();
        else
            resources.setLightweight();
        break;
    case no_sort:
        if (isGrouped)
        {
            if (expr->hasAttribute(manyAtom))
                resources.setHeavyweight();
            else
                resources.setLightweight();
        }
        else if (expr->hasAttribute(fewAtom) && isLocal)
            resources.setLightweight();
        else if (isLocal)
            resources.setHeavyweight();
        else
        {
            resources.setHeavyweight();
#ifndef SORT_USING_MP
            resources.setManyToManySockets(2);
#endif
        }
        break;
    case no_topn:
        resources.setLightweight();
        break;
    case no_pipe:
        //surely it should be something like this.
        resources.setLightweight().set(RESslavesocket, 1);
        break;
    case no_table:
        {
            IHqlExpression * mode = expr->queryChild(2);
            if (mode && mode->getOperator() == no_pipe)
            {
                resources.setLightweight().set(RESslavesocket, 1);
            }
            else
            {
                resources.setLightweight();
                if (expr->hasAttribute(_workflowPersist_Atom) && expr->hasAttribute(distributedAtom))
                    setHashResources(expr, resources, options);         // may require a hash distribute
            }
            break;
        }
    case no_output:
        {
            IHqlExpression * filename = expr->queryChild(1);
            if (expr->hasAttribute(_spill_Atom))
            {
                //resources.setLightweight();   // assume no resources(!)
            }
            else if (filename && filename->getOperator() == no_pipe)
            {
                resources.setLightweight().set(RESslavesocket, 1);
            }
            else if (filename && !filename->isAttribute())
            {
                resources.setLightweight();
            }
            else
            {
                resources.setLightweight().set(RESslavememory, WORKUNITWRITE_SMART_BUFFER_SIZE);
            }
            break;
        }
    case no_distribution:
        resources.setLightweight().set(RESmastersocket, 16).set(RESslavesocket, 1);
        break;
    case no_aggregate:
    case no_newaggregate:
        {
            IHqlExpression * grouping = queryRealChild(expr, 3);
            if (grouping)
            {
                //Is this really correct???
                resources.setLightweight();
                setHashResources(expr, resources, options);
            }
            else
            {
                resources.setLightweight();
                //if (!isGrouped)
                //  resources.set(RESmastersocket, 16).set(RESslavesocket, 1);
            }
        }
        break;
    case no_hqlproject:
        resources.setLightweight();
        //Add a flag onto count project to indicate it is a different variety.
        if (expr->hasAttribute(_countProject_Atom) && !isLocal)
            resources.set(RESslavememory, COUNTPROJECT_SMART_BUFFER_SIZE);
        break;
    case no_enth:
        resources.setLightweight();
        if (!isLocal)
            resources.set(RESslavememory, CHOOSESETS_SMART_BUFFER_SIZE);
        break;
    case no_metaactivity:
        if (expr->hasAttribute(pullAtom))
            resources.setLightweight().set(RESslavememory, PULL_SMART_BUFFER_SIZE);
        break;
    case no_setresult:
    case no_extractresult:
    case no_outputscalar:
        resources.setLightweight();//.set(RESmastersocket, 1).set(RESslavesocket, 1);
        break;
    case no_choosesets:
        resources.setLightweight();
        if (!isLocal || expr->hasAttribute(enthAtom) || expr->hasAttribute(lastAtom))
            resources.set(RESslavememory, CHOOSESETS_SMART_BUFFER_SIZE);
        break;
    case no_iterate:
        resources.setLightweight();
        if (!isGrouped && !isLocal)
            resources.setManyToMasterSockets(1).set(RESslavememory, ITERATE_SMART_BUFFER_SIZE);
        break;
    case no_choosen:
        resources.setLightweight().set(RESslavememory, FIRSTN_SMART_BUFFER_SIZE);
        break;
    case no_spill:
    case no_spillgraphresult:
        //assumed to take no resources;
        break;
    case no_addfiles:
    case no_merge:
        {
            resources.setLightweight();
            unsigned bufSize = FUNNEL_PERINPUT_BUFF_SIZE*expr->numChildren();
            if (bufSize < FUNNEL_MIN_BUFF_SIZE) bufSize = FUNNEL_MIN_BUFF_SIZE;
            resources.set(RESslavememory, MEM_Const_Minimal+bufSize);
            break;
        }
    case no_compound:
        //MORE: Should really be the total resources for the lhs...  Really needs more thought.
        break;
    case no_libraryselect:
        //Do not allocate any resources for this, we don't want it to cause a spill under any circumstances
        break;
    case no_split:
        //Should really be included in the cost....
    default:
        resources.setLightweight();
        break;
    }
}


CResources & CResources::setLightweight()
{
    return set(RESslavememory, 0x10000).set(RESactivities, 1);
}


CResources & CResources::setHeavyweight()
{
    return set(RESslavememory, 0x100000).set(RESheavy, 1).set(RESactivities, 1);
}


//-------------------------------------------------------------------------------------------


const char * queryResourceName(ResourceType kind)
{
    switch (kind)
    {
    case RESslavememory:    return "Slave Memory";
    case RESslavesocket:    return "Slave Sockets";
    case RESmastermemory:   return "Master Memory";
    case RESmastersocket:   return "Master Sockets";
    case REShashdist:       return "Hash Distributes";
    case RESheavy:          return "Heavyweight";
    case RESactivities:     return "Activities";
    }
    return "Unknown";
}

inline ResourcerInfo * queryResourceInfo(IHqlExpression * expr) { return (ResourcerInfo *)expr->queryBody()->queryTransformExtra(); }

inline bool isResourcedActivity(IHqlExpression * expr)
{
    if (!expr)
        return false;
    ResourcerInfo * extra = queryResourceInfo(expr);
    return extra && extra->isResourcedActivity();
}

bool isWorthForcingHoist(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_selectnth:
        case no_filter:
        case no_newaggregate:
            if (isResourcedActivity(expr->queryChild(0)))
                return true;
            break;
        default:
            return false;
        }
        expr = expr->queryChild(0);
    }
}


void CResources::add(const CResources & other)
{
    for (unsigned i = 0; i < RESmax; i++)
        resource[i] += other.resource[i];
}

bool CResources::addExceeds(const CResources & other, const CResources & limit) const
{
    for (unsigned i = 0; i < RESmax; i++)
    {
        if (resource[i] + other.resource[i] > limit.resource[i])
        {
            //DBGLOG("Cannot merge because limit for %s exceeded (%d cf %d)", queryResourceName((ResourceType)i), resource[i] + other.resource[i], limit.resource[i]);
            return true;
        }
    }
    return false;
}

StringBuffer & CResources::getExceedsReason(StringBuffer & reasonText, const CResources & other, const CResources & limit) const
{
    bool first = true;
    for (unsigned i = 0; i < RESmax; i++)
    {
        if (resource[i] + other.resource[i] > limit.resource[i])
        {
            if (!first) reasonText.append(", ");
            first = false;
            reasonText.appendf("%s (%d>%d)", queryResourceName((ResourceType)i), resource[i] + other.resource[i], limit.resource[i]);
        }
    }
    return reasonText;
}

bool CResources::exceeds(const CResources & limit) const
{
    for (unsigned i = 0; i < RESmax; i++)
    {
        if (resource[i] > limit.resource[i])
            return true;
    }
    return false;
}

void CResources::maximize(const CResources & other)
{
    for (unsigned i = 0; i < RESmax; i++)
    {
        if (resource[i] < other.resource[i])
           resource[i] = other.resource[i];
    }
}

CResources & CResources::setManyToMasterSockets(unsigned numSockets)
{
    set(RESslavesocket, numSockets);
    set(RESmastersocket, numSockets * clusterSize);
    return *this;
}

CResources & CResources::setManyToManySockets(unsigned numSockets)
{
    return set(RESslavesocket, numSockets * clusterSize);
}

void CResources::sub(const CResources & other)
{
    for (unsigned i = 0; i < RESmax; i++)
        resource[i] -= other.resource[i];
}

//===========================================================================

bool isSimpleAggregateResult(IHqlExpression * expr)
{
    //MORE: no_extractresult is really what is meant
    if (expr->getOperator() != no_extractresult)
        return false;
    IHqlExpression * value = expr->queryChild(0);
    if (value->getOperator() != no_datasetfromrow)
        return false;
    //MORE: This currently doesn't hoist selects from nested records, but not sure there is a syntax to do that either.
    IHqlExpression * ds = value->queryChild(0);
    if (!isSelectFirstRow(ds))
        return false;
    ds = ds->queryChild(0);
    if (ds->getOperator() != no_newaggregate)
        return false;
    return true;
}

bool lightweightAndReducesDatasetSize(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_hqlproject:
    case no_newusertable:
        return reducesRowSize(expr);
    case no_dedup:
        if (isGroupedActivity(expr) || (!expr->hasAttribute(allAtom) && !expr->hasAttribute(hashAtom)))
            return true;
        break;
    case no_aggregate:
    case no_newaggregate:
        {
            IHqlExpression * grouping = queryRealChild(expr, 3);
            if (grouping)
                return false;
            return true;
        }
    case no_rollupgroup:
    case no_rollup:
    case no_choosen:
    case no_choosesets:
    case no_enth:
    case no_sample:
    case no_filter:
    case no_limit:
    case no_filtergroup:
        return true;
    case no_group:
        //removing grouping will reduce size of the spill file.
        if (!isGrouped(expr))
            return true;
        break;
    }
    return false;
}


bool heavyweightAndReducesSizeOrSkew(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_aggregate:
    case no_newaggregate:
        {
            //more; hash aggregate?
            break;
        }
    case no_distribute:
        return true;
    }
    return false;
}

//---------------------------------------------------------------------------

IHqlExpression * CResourceOptions::createResultSpillName()
{
    return getSizetConstant(nextResult++);
}

IHqlExpression * CResourceOptions::createDiskSpillName()
{
    StringBuffer s;
    s.append("~spill::");
    appendUniqueId(s, spillSequence.next());
    return createConstant(s.str());
}

IHqlExpression * CResourceOptions::createGlobalSpillName()
{
    StringBuffer valueText;
    appendUniqueId(valueText.append("spill"), spillSequence.next());
    return createConstant(valueText.str());
}

//---------------------------------------------------------------------------

IHqlExpression * appendUniqueAttr(IHqlExpression * expr)
{
    return replaceOwnedAttribute(expr, createUniqueId());
}


bool queryAddUniqueToActivity(IHqlExpression * expr)
{
    if (!isSourceActivity(expr))
        return false;
    switch (expr->getOperator())
    {
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_xmlproject:
    case no_datasetfromrow:
    case no_datasetfromdictionary:
    case no_rows:
    case no_allnodes:
    case no_thisnode:
    case no_select:         // it can get lost, and that then causes inconsistent trees.
        return false;
    }
    return true;
}

//---------------------------------------------------------------------------

ResourceGraphLink::ResourceGraphLink(ResourceGraphInfo * _sourceGraph, IHqlExpression * _sourceNode, ResourceGraphInfo * _sinkGraph, IHqlExpression * _sinkNode, LinkKind _linkKind)
{
    assertex(_sourceGraph);
    sourceGraph.set(_sourceGraph);
    sourceNode.set(_sourceNode);
    sinkGraph.set(_sinkGraph);
    sinkNode.set(_sinkNode);

    assertex(!sinkGraph || sinkNode);
    linkKind = _linkKind;
    trace("create");
}


ResourceGraphLink::~ResourceGraphLink()
{
    trace("delete");
}

void ResourceGraphLink::changeSourceGraph(ResourceGraphInfo * newGraph)
{
    sourceGraph->sinks.zap(*this);
    sourceGraph.set(newGraph);
    newGraph->sinks.append(*this);
    trace("change source");
}

void ResourceGraphLink::changeSinkGraph(ResourceGraphInfo * newGraph)
{
    sinkGraph->sources.zap(*this);
    sinkGraph.set(newGraph);
    newGraph->sources.append(*this);
    trace("change sink");
}


bool ResourceGraphLink::isRedundantLink()
{
    ResourcerInfo * info = queryResourceInfo(sourceNode);
    return info->expandRatherThanSpill(false);
}

void ResourceGraphLink::trace(const char * name)
{
#ifdef TRACE_RESOURCING
    PrintLog("%s: %lx source(%lx,%lx) sink(%lx,%lx) %s", name, this, sourceGraph.get(), sourceNode->queryBody(), sinkGraph.get(), sinkNode ? sinkNode->queryBody() : NULL,
             linkKind == SequenceLink ? "sequence" : "");
#endif
}



ResourceGraphDependencyLink::ResourceGraphDependencyLink(ResourceGraphInfo * _sourceGraph, IHqlExpression * _sourceNode, ResourceGraphInfo * _sinkGraph, IHqlExpression * _sinkNode, IHqlExpression * _dependency)
: ResourceGraphLink(_sourceGraph, _sourceNode, _sinkGraph, _sinkNode, UnconditionalLink), dependency(_dependency)
{
}


void ResourceGraphDependencyLink::changeSourceGraph(ResourceGraphInfo * newGraph)
{
    sourceGraph.set(newGraph);
    trace("change source");
}

void ResourceGraphDependencyLink::changeSinkGraph(ResourceGraphInfo * newGraph)
{
    sinkGraph.set(newGraph);
    newGraph->dependsOn.append(*this);
    trace("change sink");
}


//---------------------------------------------------------------------------

ResourceGraphInfo::ResourceGraphInfo(CResourceOptions * _options) : resources(_options->clusterSize)
{
    options = _options;
    depth = 0;
    depthSequence = -1;
    beenResourced = false;
    isUnconditional = false;
    mergedConditionSource = false;
    hasConditionSource = false;
    hasSequentialSource = false;
    hasRootActivity = false;
    isDead = false;
    startedGeneratingResourced = false;
    inheritedExpandedDependencies = false;
    cachedDependent.other = NULL;
    cachedDependent.ignoreSources = false;
    cachedDependent.updateSequence = 0;
    cachedDependent.value = false;
}

ResourceGraphInfo::~ResourceGraphInfo()
{
}

bool ResourceGraphInfo::addCondition(IHqlExpression * condition)
{
    if (conditions.find(*condition) == NotFound)
    {
        conditions.append(*LINK(condition));

#ifdef SPOT_UNCONDITIONAL_CONDITIONS
        IAtom * name = condition->queryName();
        IAtom * invName = NULL;
        if (name == trueAtom)
            invName = falseAtom;
        else if (name == falseAtom)
            invName = trueAtom;

        if (invName)
        {
            IHqlExpression * parent = condition->queryChild(1);
            OwnedHqlExpr invTag = createAttribute(invName, LINK(condition->queryChild(0)), LINK(parent));
            if (conditions.find(*invTag) != NotFound)
            {
                if (!parent)
                    return true;
                return addCondition(parent);
            }
        }
#endif

    }
    return false;
}

bool ResourceGraphInfo::isSharedInput(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    if (unbalancedExternalSources.contains(*body))
        return false;
    if (queryResourceInfo(expr)->expandRatherThanSplit())
        return false;
    unsigned numUses = 0;
    ForEachItemIn(i, balancedExternalSources)
    {
        if (body == &balancedExternalSources.item(i))
            numUses++;
    }
    //NumUses could be zero if an input should be expanded, and that input is shared by another graph which also expands
    //the input.  E.g. project(meta(diskread).
    return numUses > 1;
}

void ResourceGraphInfo::addSharedInput(IHqlExpression * expr, IHqlExpression * mapped)
{
    sharedInputs.append(*LINK(expr));
    sharedInputs.append(*LINK(mapped));
}

IHqlExpression * ResourceGraphInfo::queryMappedSharedInput(IHqlExpression * expr)
{
    unsigned max = sharedInputs.ordinality();
    for (unsigned i=0; i < max; i+= 2)
    {
        if (expr == &sharedInputs.item(i))
            return &sharedInputs.item(i+1);
    }
    return NULL;
}

bool ResourceGraphInfo::allocateResources(const CResources & value, const CResources & limit)
{
    if (resources.addExceeds(value, limit))
        return false;
    resources.add(value);
    return true;
}


bool ResourceGraphInfo::canMergeActionAsSibling(bool sequential) const
{
    //sequential requires root actions in separate graphs
    if (sequential && hasRootActivity)
        return false;
    //Dependent actions need to be in a separate graph if this flag is set.
    if (options->actionLinkInNewGraph && !hasRootActivity)
        return false;
    return true;
}

bool ResourceGraphInfo::containsActiveSinks() const
{
    ForEachItemIn(idx, sinks)
    {
        ResourcerInfo * info = queryResourceInfo(sinks.item(idx).sourceNode);
        if (!info->expandRatherThanSpill(false))
            return true;
    }
    return false;
}

bool ResourceGraphInfo::containsActionSink() const
{
    ForEachItemIn(idx, sinks)
    {
        IHqlExpression * sink = sinks.item(idx).sourceNode;
        if (sink->isAction())
            return true;
    }
    return false;
}

void ResourceGraphInfo::display()
{
    StringBuffer s;
    s.appendf("graph %p src(", this);
    ForEachItemIn(idxs, sources)
        s.appendf("%p ", sources.item(idxs).sourceGraph.get());
    s.append(") dep(");
    ForEachItemIn(idxd, dependsOn)
        s.appendf("%p ", dependsOn.item(idxd).sourceGraph.get());
    s.append(")");
    if (isUnconditional)
        s.append(" <unconditional>");
    DBGLOG("%s", s.str());
}

void ResourceGraphInfo::getMergeFailReason(StringBuffer & reasonText, ResourceGraphInfo * otherGraph, const CResources & limit)
{
    resources.getExceedsReason(reasonText, otherGraph->resources, limit);
}

unsigned ResourceGraphInfo::getDepth()
{
    //If no graphs have been merged since this was last called it is still valid.
    if (depthSequence == options->state.updateSequence)
        return depth;

    depthSequence = options->state.updateSequence;
    depth = 0;
    ForEachItemIn(idx, dependsOn)
    {
        ResourceGraphInfo * source = dependsOn.item(idx).sourceGraph;
        if (source->getDepth() >= depth)
            depth = source->getDepth() + 1;
    }
    ForEachItemIn(idx2, sources)
    {
        ResourceGraphInfo * source = sources.item(idx2).sourceGraph;
        if (source->getDepth() >= depth)
            depth = source->getDepth() + 1;
    }
    return depth;
}

bool ResourceGraphInfo::hasSameConditions(ResourceGraphInfo & other)
{
    if (conditions.ordinality() != other.conditions.ordinality())
        return false;
    ForEachItemIn(i, conditions)
        if (other.conditions.find(conditions.item(i)) == NotFound)
            return false;
    return true;
}

bool ResourceGraphInfo::evalDependentOn(ResourceGraphInfo & other, bool ignoreSources)
{
    ForEachItemIn(idx, dependsOn)
    {
        ResourceGraphInfo * cur = dependsOn.item(idx).sourceGraph;
        if (cur == &other)
            return true;
        if (cur->isDependentOn(other, false))
            return true;
    }
    ForEachItemIn(idx2, sources)
    {
        ResourceGraphInfo * cur = sources.item(idx2).sourceGraph;
        if ((cur == &other) && !ignoreSources)
            return true;
        if (cur->isDependentOn(other, false))
            return true;
    }
    return false;
}


bool ResourceGraphInfo::isDependentOn(ResourceGraphInfo & other, bool ignoreSources)
{
    //Cache the last query so that traversal doesn't convert a graph into a tree walk
    if ((cachedDependent.other == &other) && (cachedDependent.ignoreSources == ignoreSources) &&
        (cachedDependent.updateSequence == options->state.updateSequence))
        return cachedDependent.value;

    if (getDepth() <= other.getDepth())
        return false;

    cachedDependent.other = &other;
    cachedDependent.ignoreSources = ignoreSources;
    cachedDependent.updateSequence = options->state.updateSequence;
    cachedDependent.value = evalDependentOn(other, ignoreSources);
    return cachedDependent.value;
}


bool ResourceGraphInfo::isVeryCheap()
{
    if (sinks.ordinality() != 1)
        return false;

    IHqlExpression * sourceNode = sinks.item(0).sourceNode;
    if (isSimpleAggregateResult(sourceNode))
        return true;

//  Not sure about the following....
//  if (sourceNode->getOperator() == no_setresult)
//      return true;

    //Could be other examples...
    return false;
}


bool ResourceGraphInfo::mergeInSource(ResourceGraphInfo & other, const CResources & limit, bool mergeInSpillOutput)
{
    bool mergeConditions = mergeInSpillOutput;
    if (!isUnconditional && !mergeInSpillOutput)
    {
        //if it is conditional and it is very cheap then it is still more efficient to merge
        //rather than read from a spill file.
        if (other.isUnconditional || !hasSameConditions(other))
        {
            if ((hasConditionSource || !isVeryCheap()) && (other.sinks.ordinality() != 1))
                return false;
            mergeConditions = true;
        }
    }

    if (isDependentOn(other, true))
        return false;

    if (!options->canSplit())
    {
        //Don't merge two graphs that will cause a splitter to be created
        //Either already used internally, or an output will be merged twice
        HqlExprArray mergeNodes;
        ForEachItemIn(idx, sources)
        {
            ResourceGraphLink & cur = sources.item(idx);
            if (cur.sourceGraph == &other)
            {
                IHqlExpression * sourceNode = cur.sourceNode->queryBody();
                ResourcerInfo * info = queryResourceInfo(sourceNode);
                if ((info->numInternalUses() != 0) || (mergeNodes.find(*sourceNode) != NotFound) || info->preventMerge())
                    return false;
                mergeNodes.append(*LINK(sourceNode));
            }
        }
    }

    if (options->checkResources() && !allocateResources(other.resources, limit))
        return false;

    if (hasSequentialSource && other.hasSequentialSource)
        return false;

    if (mergeConditions)
    {
        //Merging a conditional spill output into a child graph should not make that graph conditional
        isUnconditional = other.isUnconditional;
    }
    mergeGraph(other, mergeConditions);
    return true;
}

void ResourceGraphInfo::mergeGraph(ResourceGraphInfo & other, bool mergeConditions)
{
#ifdef TRACE_RESOURCING
    DBGLOG("Merging%s source into%s sink", other.isUnconditional ? "" : " conditional", isUnconditional ? "" : " conditional");
    other.display();
    display();
    PrintLog("Merge %p into %p", &other, this);
#endif

    if (other.hasConditionSource)
        hasConditionSource = true;

    if (other.hasSequentialSource)
    {
        assertex(!hasSequentialSource);
        hasSequentialSource = true;
    }

    //Recalculate the dependents, because sources of the source merged in may no longer be indirect
    //although they may be via another path.
    options->noteGraphsChanged();

    //If was very cheap and merged into an unconditional graph, make sure this is now tagged as
    //unconditional...
    if (other.isUnconditional)
        isUnconditional = true;

    //We need to stop spills being an arm of a conditional branch - otherwise they won't get executed.
    //so see if we have merged any conditional branches in
    if (other.mergedConditionSource)
        mergedConditionSource = true;

    if (mergeConditions)
    {
        //Replace conditions with those of parent.  Only called when a very simple graph is
        //merged in, so replace conditions instead of merging
        conditions.kill();
        ForEachItemIn(i, other.conditions)
            conditions.append(OLINK(other.conditions.item(i)));
    }

    //sources and sinks are updated elsewhere...
}


bool ResourceGraphInfo::mergeInSibling(ResourceGraphInfo & other, const CResources & limit)
{
    if ((!isUnconditional || !other.isUnconditional) && !hasSameConditions(other))
        return false;

    if (hasSequentialSource && other.hasSequentialSource)
        return false;

    if (isDependentOn(other, false) || other.isDependentOn(*this, false))
        return false;

    if (options->checkResources() && !allocateResources(other.resources, limit))
        return false;

    mergeGraph(other, false);
    return true;
}


void ResourceGraphInfo::removeResources(const CResources & value)
{
    resources.sub(value);
}


//---------------------------------------------------------------------------

unsigned ChildDependentArray::findOriginal(IHqlExpression * expr)
{
    ForEachItem(i)
    {
        if (item(i).original == expr)
            return i;
    }
    return NotFound;
}
//---------------------------------------------------------------------------

static void appendCloneAttribute(HqlExprArray & args, IHqlExpression * expr, IAtom * name)
{
    IHqlExpression * prop = expr->queryAttribute(name);
    if (prop)
        args.append(*LINK(prop));
}

SpillerInfo::SpillerInfo(IHqlExpression * _original, CResourceOptions * _options) : NewTransformInfo(_original), options(_options)
{
    outputToUseForSpill = NULL;
    linkedFromChild = false;
}

void SpillerInfo::addSpillFlags(HqlExprArray & args, bool isRead)
{
    if (isGrouped(original))
        args.append(*createAttribute(groupedAtom));

    if (outputToUseForSpill)
    {
        assertex(isRead);
        appendCloneAttribute(args, outputToUseForSpill, __compressed__Atom);
        appendCloneAttribute(args, outputToUseForSpill, jobTempAtom);
        appendCloneAttribute(args, outputToUseForSpill, _spill_Atom);
    }
    else
    {
        args.append(*createAttribute(__compressed__Atom));
        args.append(*createAttribute(_spill_Atom));
        args.append(*createAttribute(_noReplicate_Atom));
        if (options->targetRoxie())
            args.append(*createAttribute(_noAccess_Atom));
    }

    if (isRead)
    {
        args.append(*createAttribute(_noVirtual_Atom));         // Don't interpret virtual fields as virtual...
        if (!original->isDataset() || hasSingleRow(original))
            args.append(*createAttribute(rowAtom));                 // add a flag so the selectnth[1] can be removed later...
    }
}

IHqlExpression * SpillerInfo::createSpillName()
{
    if (outputToUseForSpill)
    {
        switch (outputToUseForSpill->getOperator())
        {
        case no_setgraphresult:
            return LINK(outputToUseForSpill->queryChild(2));
        case no_output:
        {
            if (isFileOutput(outputToUseForSpill))
                return LINK(outputToUseForSpill->queryChild(1));
            IHqlExpression * nameAttr = outputToUseForSpill->queryAttribute(namedAtom);
            assertex(nameAttr);
            return LINK(nameAttr->queryChild(0));
        }
        default:
            throwUnexpected();
        }
    }
    if (!spillName)
    {
        if (useGraphResult())
            spillName.setown(options->createResultSpillName());
        else if (useGlobalResult())
            spillName.setown(options->createGlobalSpillName());
        else
            spillName.setown(options->createDiskSpillName());
    }
    return LINK(spillName);
}


IHqlExpression * SpillerInfo::createSpilledRead(IHqlExpression * spillReason)
{
    OwnedHqlExpr dataset;
    HqlExprArray args;

    IHqlExpression * record = original->queryRecord();
    bool loseDistribution = true;
    if (useGraphResult())
    {
        if (spilledDataset)
            args.append(*LINK(spilledDataset));
        else
            args.append(*LINK(record));
        args.append(*LINK(options->graphIdExpr));
        args.append(*createSpillName());
        if (isGrouped(original))
            args.append(*createAttribute(groupedAtom));
        if (!original->isDataset())
            args.append(*createAttribute(rowAtom));
        args.append(*createAttribute(_spill_Atom));
        IHqlExpression * recordCountAttr = queryRecordCountInfo(original);
        if (recordCountAttr)
            args.append(*LINK(recordCountAttr));
        if (options->targetThor() && original->isDataset() && !options->isChildQuery)
            args.append(*createAttribute(_distributed_Atom));

        node_operator readOp = spilledDataset ? no_readspill : no_getgraphresult;
        if (original->isDatarow())
            dataset.setown(createRow(readOp, args));
        else if (original->isDictionary())
            dataset.setown(createDictionary(readOp, args));
        else
            dataset.setown(createDataset(readOp, args));
        loseDistribution = false;
    }
    else if (useGlobalResult())
    {
        args.append(*LINK(record));
        args.append(*createAttribute(nameAtom, createSpillName()));
        args.append(*createAttribute(sequenceAtom, getLocalSequenceNumber()));
        addSpillFlags(args, true);
        IHqlExpression * recordCountAttr = queryRecordCountInfo(original);
        if (recordCountAttr)
            args.append(*LINK(recordCountAttr));
        if (original->isDictionary())
            dataset.setown(createDictionary(no_workunit_dataset, args));
        else
            dataset.setown(createDataset(no_workunit_dataset, args));
    }
    else
    {
        if (spilledDataset)
        {
            args.append(*LINK(spilledDataset));
            args.append(*createSpillName());
        }
        else
        {
            args.append(*createSpillName());
            args.append(*LINK(record));
        }
        args.append(*createValue(no_thor));
        addSpillFlags(args, true);
        args.append(*createUniqueId());
        if (options->isChildQuery && options->targetRoxie())
        {
            args.append(*createAttribute(_colocal_Atom));
            args.append(*createLocalAttribute());
        }
        if (spillReason)
            args.append(*LINK(spillReason));

        if (spilledDataset)
            dataset.setown(createDataset(no_readspill, args));
        else
        {
            IHqlExpression * recordCountAttr = queryRecordCountInfo(original);
            if (recordCountAttr)
                args.append(*LINK(recordCountAttr));
            dataset.setown(createDataset(no_table, args));
        }

        loseDistribution = false;
    }

    dataset.setown(preserveTableInfo(dataset, original, loseDistribution, NULL));
    return wrapRowOwn(dataset.getClear());
}

IHqlExpression * SpillerInfo::createSpilledWrite(IHqlExpression * transformed, bool allowCommonDataset)
{
    assertex(!outputToUseForSpill);
    HqlExprArray args;

    if (useGraphResult())
    {
        assertex(options->graphIdExpr);

        if (options->createSpillAsDataset && !linkedFromChild && allowCommonDataset)
        {
            IHqlExpression * value = LINK(transformed);
            if (value->isDatarow())
                value = createDatasetFromRow(value);
            spilledDataset.setown(createDataset(no_commonspill, value));
            args.append(*LINK(spilledDataset));
        }
        else
            args.append(*LINK(transformed));

        args.append(*LINK(options->graphIdExpr));
        args.append(*createSpillName());
        args.append(*createAttribute(_spill_Atom));
        if (linkedFromChild)
            args.append(*createAttribute(_accessedFromChild_Atom));
        args.append(*createAttribute(_graphLocal_Atom));
        if (spilledDataset)
            return createValue(no_writespill, makeVoidType(), args);
        return createValue(no_setgraphresult, makeVoidType(), args);
    }
    else if (useGlobalResult())
    {
        args.append(*LINK(transformed));
        args.append(*createAttribute(sequenceAtom, getLocalSequenceNumber()));
        args.append(*createAttribute(namedAtom, createSpillName()));
        addSpillFlags(args, false);
        args.append(*createAttribute(_graphLocal_Atom));
        return createValue(no_output, makeVoidType(), args);
    }
    else
    {
        if (options->createSpillAsDataset && allowCommonDataset)
        {
            IHqlExpression * value = LINK(transformed);
            if (value->isDatarow())
                value = createDatasetFromRow(value);
            spilledDataset.setown(createDataset(no_commonspill, value));
            args.append(*LINK(spilledDataset));
        }
        else
            args.append(*LINK(transformed));

        args.append(*createSpillName());
        addSpillFlags(args, false);
        args.append(*createAttribute(_graphLocal_Atom));
        if (spilledDataset)
            return createValue(no_writespill, makeVoidType(), args);

        return createValue(no_output, makeVoidType(), args);
    }
}


void SpillerInfo::setPotentialSpillFile(IHqlExpression * expr)
{
    if (!expr ||
        ((canUseResultInChildQuery(expr) || !isUsedFromChild()) && (expr->getOperator() != no_setgraphresult)))
        outputToUseForSpill = expr;
}

bool SpillerInfo::useGraphResult()
{
    if (outputToUseForSpill)
        return (outputToUseForSpill->getOperator() == no_setgraphresult);
    return options->useGraphResult(linkedFromChild);
}

bool SpillerInfo::useGlobalResult()
{
    if (outputToUseForSpill)
        return isWorkunitOutput(outputToUseForSpill);
    return options->useGlobalResult(linkedFromChild);
}


IHqlExpression * SpillerInfo::wrapRowOwn(IHqlExpression * expr)
{
    if (!original->isDataset() && !original->isDictionary() && !expr->isDatarow())
        expr = createRow(no_selectnth, expr, getSizetConstant(1));
    return expr;
}



ResourcerInfo::ResourcerInfo(IHqlExpression * _original, CResourceOptions * _options) : SpillerInfo(_original, _options)
{
    numUses = 0;
    numExternalUses = 0;
    gatheredDependencies = false;
    containsActivity = false;
    isActivity = false;
    isRootActivity = false;
    conditionSourceCount = 0;
    pathToExpr = PathUnknown;
    isAlreadyInScope = false;
    isSpillPoint = false;
    balanced = true;
    currentSource = 0;
    linkedFromChild = false;
    forceHoist = false;
    neverSplit = false;
    isConditionalFilter = false;
    projectResult = true; // projected must also be non empty to actually project this dataset
    visited = false;
    lastPass = 0;
    balancedExternalUses = 0;
    balancedInternalUses = 0;
    balancedVisiting = false;
    removedParallelPullers = false;
#ifdef TRACE_BALANCED
    balanceId = 0;
#endif
}

void ResourcerInfo::setConditionSource(IHqlExpression * condition, bool isFirst)
{
    if (isFirst)
    {
        conditionSourceCount++;
        graph->hasConditionSource = true;
    }
}

bool ResourcerInfo::addCondition(IHqlExpression * condition)
{
    conditions.append(*LINK(condition));
    return graph->addCondition(condition);
}

void ResourcerInfo::addProjected(IHqlExpression * next)
{
    if (projectResult && !projected.contains(*next))
        projected.append(*LINK(next));
}

void ResourcerInfo::clearProjected()
{
    projectResult = false;
    projected.kill();
}


bool ResourcerInfo::alwaysExpand()
{
    return (original->getOperator() == no_mapto);
}

static IHqlExpression * stripTrueFalse(IHqlExpression * expr)
{
    if (expr->queryName() == instanceAtom)
    {
        IHqlExpression * parent = expr->queryChild(2);
        if (parent)
            parent = stripTrueFalse(parent);
        return createAttribute(instanceAtom, LINK(expr->queryChild(0)), LINK(expr->queryChild(1)), parent);
    }
    else
    {
        IHqlExpression * parent = expr->queryChild(1);
        if (parent && parent->isAttribute())
            parent = stripTrueFalse(parent);
        return createAttribute(tempAtom, LINK(expr->queryChild(0)), parent);
    }
}

unsigned ResourcerInfo::calcNumConditionalUses()
{
    //for thor and hthor, where the conditional graph is cloned, the maximum number of times it can be read
    //is 1 for a shared conditional graph, or once for each combination of conditions it is used as the
    //input for.  However if it is used more than once in a single branch of a condition it needs to be counted
    //several times.
    //It is always better to overestimate than underestimate. (But even better to get it right.)
    HqlExprArray uniqueConditions;
    UnsignedArray uniqueCounts;
    ForEachItemIn(idx, conditions)
    {
        IHqlExpression & cur = conditions.item(idx);
        OwnedHqlExpr unique = stripTrueFalse(&cur);
        unsigned numOccurences = 0;
        ForEachItemIn(j, conditions)
            if (&conditions.item(j) == &cur)
                numOccurences++;

        unsigned match = uniqueConditions.find(*unique);
        if (match == NotFound)
        {
            uniqueConditions.append(*unique.getClear());
            uniqueCounts.append(numOccurences);
        }
        else
        {
            if (uniqueCounts.item(match) < numOccurences)
                uniqueCounts.replace(numOccurences, match);
        }
    }

    unsigned condUses = 0;
    ForEachItemIn(k, uniqueCounts)
        condUses += uniqueCounts.item(k);
    return condUses;
}


// Each aggregate has form.  Setresult(select(selectnth(aggregate...,1),field),name,seq)
IHqlExpression * ResourcerInfo::createAggregation(IHqlExpression * expr)
{
    LinkedHqlExpr transformed = expr;
    ForEachItemIn(idx, aggregates)
    {
        IHqlExpression & cur = aggregates.item(idx);
        IHqlExpression * row2ds = cur.queryChild(0);
        IHqlExpression * selectnth = row2ds->queryChild(0);
        IHqlExpression * aggregate = selectnth->queryChild(0);
        IHqlExpression * aggregateRecord = aggregate->queryChild(1);
        assertex(aggregate->getOperator() == no_newaggregate);
        HqlExprArray aggargs, setargs;
        //Through aggregate has form (dataset, record, transform, list of set results);
        aggargs.append(*LINK(transformed));
        aggargs.append(*LINK(aggregateRecord));
        IHqlExpression * mapped = replaceSelector(aggregate->queryChild(2), original, expr->queryNormalizedSelector());
        aggargs.append(*mapped);

        OwnedHqlExpr active = createDataset(no_anon, LINK(aggregateRecord), NULL);
        OwnedHqlExpr mappedSelect = replaceSelector(cur.queryChild(1), row2ds, active);
        setargs.append(*LINK(active));
        setargs.append(*LINK(mappedSelect));
        unwindChildren(setargs, &cur, 1);
        aggargs.append(*createValue(no_extractresult, makeVoidType(), setargs));
        transformed.setown(createDataset(no_throughaggregate, aggargs));
    }

    return transformed.getClear();
}

bool ResourcerInfo::okToSpillThrough()
{
    bool isGraphResult = useGraphResult();
    if (isGraphResult)
        return options->allowThroughResult;

    if (useGlobalResult())
        return false;

    return (options->allowThroughSpill && !options->createSpillAsDataset);
}

void ResourcerInfo::noteUsedFromChild(bool _forceHoist)
{
    linkedFromChild = true;
    outputToUseForSpill = NULL;
    if (_forceHoist)
        forceHoist = true;
}

unsigned ResourcerInfo::numInternalUses()
{
    return numUses - numExternalUses - aggregates.ordinality();
}

void ResourcerInfo::resetBalanced()
{
    // don't reset balanced since it may be set for an external result
    balancedLinks.kill();
    curBalanceLink = 0;
    balancedVisiting = false;
    balancedExternalUses = 0;
    balancedInternalUses = 0;
    removedParallelPullers = false;
}

bool ResourcerInfo::spillSharesSplitter()
{
    if (outputToUseForSpill || useGraphResult() || useGlobalResult())
        return false;
    if (okToSpillThrough())
        return false;
    if (!isSplit() || !balanced)
        return false;
    return true;
}

void ResourcerInfo::setRootActivity()
{
    isRootActivity = true;
}

IHqlExpression * ResourcerInfo::createSpiller(IHqlExpression * transformed, bool reuseSplitter)
{
    if (outputToUseForSpill)
        return LINK(transformed);

    if (!okToSpillThrough())
    {
        OwnedHqlExpr split;
        if (reuseSplitter)
        {
            assertex(transformed->getOperator() == no_split);
            split.set(transformed);
        }
        else
        {
            if (transformed->isDataset())
                split.setown(createDataset(no_split, LINK(transformed), createComma(createAttribute(balancedAtom), createUniqueId())));
            else
                split.setown(createRow(no_split, LINK(transformed), createComma(createAttribute(balancedAtom), createUniqueId())));
            split.setown(cloneInheritedAnnotations(original, split));
        }

        splitterOutput.setown(createSpilledWrite(split, true));
        return split.getClear();
    }

    HqlExprArray args;
    node_operator op;
    args.append(*LINK(transformed));
    if (useGraphResult())
    {
        op = no_spillgraphresult;
        args.append(*LINK(options->graphIdExpr));
        args.append(*createSpillName());
        args.append(*createAttribute(_spill_Atom));
    }
    else
    {
        op = no_spill;
        args.append(*createSpillName());
        addSpillFlags(args, false);
    }


    OwnedHqlExpr spill;
    if (original->isDatarow())
        spill.setown(createRow(op, args));
    else
        spill.setown(createDataset(op, args));
    return cloneInheritedAnnotations(original, spill);
}

IHqlExpression * ResourcerInfo::createSplitter(IHqlExpression * transformed)
{
    if (transformed->getOperator() == no_libraryscopeinstance)
        return LINK(transformed);

    IHqlExpression * attr = createUniqueId();
    if (balanced)
        attr = createComma(attr, createAttribute(balancedAtom));
    OwnedHqlExpr split;
    if (transformed->isDataset())
        split.setown(createDataset(no_split, LINK(transformed), attr));
    else
        split.setown(createRow(no_split, LINK(transformed), attr));
    return cloneInheritedAnnotations(original, split);
}

IHqlExpression * ResourcerInfo::createTransformedExpr(IHqlExpression * expr)
{
    LinkedHqlExpr transformed = expr;
    if (aggregates.ordinality())
        transformed.setown(createAggregation(transformed));

    if (spillSharesSplitter())
    {
        transformed.setown(createSplitter(transformed));

        if (isExternalSpill())
            transformed.setown(createSpiller(transformed, true));
    }
    else
    {
        if (isExternalSpill())
            transformed.setown(createSpiller(transformed, false));
        if (isSplit())
            transformed.setown(createSplitter(transformed));
        else if (isSpilledWrite())
            transformed.setown(createSpilledWrite(transformed, true));
    }

    return transformed.getClear();
}

bool ResourcerInfo::expandRatherThanSpill(bool noteOtherSpills)
{
    if (options->shareDontExpand)
        return expandRatherThanSplit();

    IHqlExpression * expr = original;
    if (!options->minimiseSpills || linkedFromChild)
        noteOtherSpills = false;

    if (noteOtherSpills)
    {
        ResourcerInfo * info = queryResourceInfo(expr);
        if (info && info->isSpilledWrite())
            return (info->queryTransformed() == NULL);
    }
    bool isFiltered = false;
    bool isProcessed = false;
    loop
    {
        ResourcerInfo * info = queryResourceInfo(expr);
        if (info && info->neverSplit)
            return true;
        if (info && info->forceHoist)
            return false;

        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_table:
            {
                //This is only executed for hthor/thor.  Roxie has used expandRatherThanSplit().
                //We need to balance the saving from reading reduced data in the other branches with the cost of
                //writing the spill file to disk.
                if (isFiltered && (numExternalUses >= options->filteredSpillThreshold))
                    return false;
                IHqlExpression * mode = expr->queryChild(2);
                switch (mode->getOperator())
                {
                case no_thor: case no_flat:
                    //MORE: The following is possibly better - but roxie should be able to read from non spill data files in child queries fine
                    //if ((options->targetClusterType == RoxieCluster) && linkedFromChild)) return false;
                    return true;
                default:
                    return false;
                }
            }
        case no_stepped:
            return true;
        case no_inlinetable:
            {
                IHqlExpression * transforms = expr->queryChild(0);
                //The inline table code means this should generate smaller code, and more efficient
                if (!isFiltered && !isProcessed && transforms->isConstant())
                    return true;

                if (transforms->numChildren() > MAX_INLINE_COMMON_COUNT)
                    return false;
                return true;
            }
        case no_getresult:
        case no_temptable:
        case no_rows:
        case no_xmlproject:
        case no_workunit_dataset:
            return !isProcessed && !isFiltered;
        case no_getgraphresult:
            return !expr->hasAttribute(_streaming_Atom);         // we must not duplicate streaming inputs!
        case no_keyindex:
        case no_newkeyindex:
            if (!isFiltered)
                return true;
            return options->cloneFilteredIndex;
        case no_datasetfromrow:
            if (getNumActivityArguments(expr) == 0)
                return true;
            return false;
        case no_fail:
        case no_null:
            return !expr->isAction();
        case no_assertsorted:
        case no_sorted:
        case no_grouped:
        case no_distributed:
        case no_preservemeta:
        case no_nofold:
        case no_nohoist:
        case no_nocombine:
        case no_section:
        case no_sectioninput:
        case no_dataset_alias:
            expr = expr->queryChild(0);
            break;
        case no_newusertable:
        case no_limit:
        case no_keyedlimit:
            expr = expr->queryChild(0);
            isProcessed = true;
            break;
        case no_hqlproject:
            if (expr->hasAttribute(_countProject_Atom) || expr->hasAttribute(prefetchAtom))
                return false;
            expr = expr->queryChild(0);
            isProcessed = true;
            break;
            //MORE: Not so sure about all the following, include them so behaviour doesn't change
        case no_compound_diskread:
        case no_compound_disknormalize:
        case no_compound_diskaggregate:
        case no_compound_diskcount:
        case no_compound_diskgroupaggregate:
        case no_compound_indexread:
        case no_compound_indexnormalize:
        case no_compound_indexaggregate:
        case no_compound_indexcount:
        case no_compound_indexgroupaggregate:
        case no_compound_childread:
        case no_compound_childnormalize:
        case no_compound_childaggregate:
        case no_compound_childcount:
        case no_compound_childgroupaggregate:
        case no_compound_selectnew:
        case no_compound_inline:
        case no_datasetfromdictionary:
            expr = expr->queryChild(0);
            break;
        case no_filter:
            isFiltered = true;
            expr = expr->queryChild(0);
            break;
        case no_select:
            {
                if (options->targetClusterType == RoxieCluster)
                    return false;

                if (!isNewSelector(expr))
                    return true;
                expr = expr->queryChild(0);
                break;
            }
        default:
            return false;
        }

        //The following reduces the number of spills by taking into account other spills.
        if (noteOtherSpills)
        {
            ResourcerInfo * info = queryResourceInfo(expr);
            if (info)
            {
                if (info->isSpilledWrite())
                    return (info->queryTransformed() == NULL);
                if (info->numExternalUses)
                {
                    if (isFiltered && (numExternalUses >= options->filteredSpillThreshold))
                        return false;
                    return true;
                }
            }
        }
    }
}

bool ResourcerInfo::expandRatherThanSplit()
{
    //MORE: This doesn't really work - should do indexMatching first.
    //should only expand if one side that uses this is also filtered
    IHqlExpression * expr = original;
    loop
    {
        ResourcerInfo * info = queryResourceInfo(expr);
        if (info && info->neverSplit)
            return true;
        switch (expr->getOperator())
        {
        case no_keyindex:
        case no_newkeyindex:
        case no_rowset:
        case no_getgraphloopresultset:
            return true;
        case no_null:
            return !expr->isAction();
        case no_inlinetable:
            if (options->expandSingleConstRow && hasSingleRow(expr))
            {
                IHqlExpression * values = expr->queryChild(0);
                if (values->queryChild(0)->isConstant())
                    return true;
            }
            return false;
        case no_stepped:
        case no_rowsetrange:
        case no_rowsetindex:
            return true;
        case no_sorted:
        case no_grouped:
        case no_distributed:
        case no_preservemeta:
        case no_compound_diskread:
        case no_compound_disknormalize:
        case no_compound_diskaggregate:
        case no_compound_diskcount:
        case no_compound_diskgroupaggregate:
        case no_compound_indexread:
        case no_compound_indexnormalize:
        case no_compound_indexaggregate:
        case no_compound_indexcount:
        case no_compound_indexgroupaggregate:
        case no_compound_childread:
        case no_compound_childnormalize:
        case no_compound_childaggregate:
        case no_compound_childcount:
        case no_compound_childgroupaggregate:
        case no_compound_selectnew:
        case no_compound_inline:
        case no_section:
        case no_sectioninput:
        case no_dataset_alias:
            break;
        case no_select:
            if (options->targetClusterType == RoxieCluster)
                return false;
            if (!isNewSelector(expr))
            {
                if (!hasLinkCountedModifier(expr))
                    return false;
                return true;
            }
            break;
        case no_rows:
            //If executing in a child query then you'll have less thread contention if the iterator is duplicated
            //So should probably uncomment the following.
            //return true;
            return false;
        default:
            return false;
        }
        expr = expr->queryChild(0);
    }
}

bool ResourcerInfo::hasDependency() const
{
    if (graph)
    {
        GraphLinkArray & graphLinks = graph->dependsOn;
        ForEachItemIn(i, graphLinks)
        {
            ResourceGraphLink & link = graphLinks.item(i);
            if (link.sinkNode == original)
                return true;
        }
    }
    else
    {
        //Really should save some information away...  Err on side of caution
        return true;
    }
    return false;
}

bool neverCommonUp(IHqlExpression * expr)
{
    loop
    {
        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_keyindex:
        case no_newkeyindex:
            return true;
        case no_filter:
            expr = expr->queryChild(0);
            break;
        default:
            return false;
        }
    }
}


bool ResourcerInfo::neverCommonUp()
{
    return ::neverCommonUp(original);
}

bool ResourcerInfo::isExternalSpill()
{
    if (expandRatherThanSpill(true) || (numInternalUses() == 0))
        return false;
    return (numExternalUses != 0);
}

bool ResourcerInfo::isSplit()
{
    return numSplitPaths() > 1;
}

unsigned ResourcerInfo::numSplitPaths()
{
    unsigned internal = numInternalUses();
    if ((internal == 0) || !options->allowSplitBetweenSubGraphs)
        return internal;
    //MORE
    return internal;
}


bool ResourcerInfo::isSpilledWrite()
{
    if (numInternalUses() == 0)
        return true;
    return false;
}


//---------------------------------------------------------------------------

EclResourcer::EclResourcer(IErrorReceiver & _errors, IConstWorkUnit * _wu, const HqlCppOptions & _translatorOptions, CResourceOptions & _options)
: options(_options)
{
    wu.set(_wu);
    errors = &_errors;
    lockTransformMutex();
    targetClusterType = options.targetClusterType;
    insideNeverSplit = false;
    insideSteppedNeverSplit = false;
    sequential = false;
    thisPass = 1;

    unsigned totalMemory = _translatorOptions.resourceMaxMemory ? _translatorOptions.resourceMaxMemory : DEFAULT_TOTAL_MEMORY;
    unsigned maxSockets = _translatorOptions.resourceMaxSockets ? _translatorOptions.resourceMaxSockets : DEFAULT_MAX_SOCKETS;
    unsigned maxActivities = _translatorOptions.resourceMaxActivities ? _translatorOptions.resourceMaxActivities : DEFAULT_MAX_ACTIVITIES;
    unsigned maxHeavy = _translatorOptions.resourceMaxHeavy;
    unsigned maxDistribute = _translatorOptions.resourceMaxDistribute;

    resourceLimit = new CResources(0);
    resourceLimit->set(RESactivities, maxActivities);
    switch (targetClusterType)
    {
    case ThorLCRCluster:
        resourceLimit->set(RESheavy, maxHeavy).set(REShashdist, maxDistribute);
        resourceLimit->set(RESmastersocket, maxSockets).set(RESslavememory,totalMemory);
        resourceLimit->set(RESslavesocket, maxSockets).set(RESmastermemory, totalMemory);
        break;
    default:
        resourceLimit->set(RESheavy, 0xffffffff).set(REShashdist, 0xffffffff);
        resourceLimit->set(RESmastersocket, 0xffffffff).set(RESslavememory, 0xffffffff);
        resourceLimit->set(RESslavesocket, 0xffffffff).set(RESmastermemory, 0xffffffff);
        break;
    }

    if (_translatorOptions.unlimitedResources)
    {
        resourceLimit->set(RESheavy, 0xffffffff).set(REShashdist, 0xffffffff);
        resourceLimit->set(RESmastersocket, 0xffffffff).set(RESslavememory,0xffffffff);
        resourceLimit->set(RESslavesocket, 0xffffffff).set(RESmastermemory,0xffffffff);
    }

    spilled = false;
}

EclResourcer::~EclResourcer()
{
    delete resourceLimit;
    unlockTransformMutex();
}

void EclResourcer::changeGraph(IHqlExpression * expr, ResourceGraphInfo * newGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    info->graph.set(newGraph);
    ForEachItemInRev(idx, links)
    {
        ResourceGraphLink & cur = links.item(idx);
        if (cur.sourceNode == expr)
            cur.changeSourceGraph(newGraph);
        else if (cur.sinkNode == expr)
            cur.changeSinkGraph(newGraph);
        assertex(cur.sinkGraph != cur.sourceGraph);
    }
}


ResourceGraphInfo * EclResourcer::createGraph()
{
    ResourceGraphInfo * graph = new ResourceGraphInfo(&options);
    graphs.append(*LINK(graph));
    //PrintLog("Create graph %p", graph);
    return graph;
}

void EclResourcer::connectGraphs(ResourceGraphInfo * sourceGraph, IHqlExpression * sourceNode, ResourceGraphInfo * sinkGraph, IHqlExpression * sinkNode, LinkKind kind)
{
    ResourceGraphLink * link = new ResourceGraphLink(sourceGraph, sourceNode, sinkGraph, sinkNode, kind);
    links.append(*link);
    if (sourceGraph)
        sourceGraph->sinks.append(*link);
    if (sinkGraph)
        sinkGraph->sources.append(*link);
}


ResourcerInfo * EclResourcer::queryCreateResourceInfo(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    ResourcerInfo * info = (ResourcerInfo *)body->queryTransformExtra();
    if (!info)
    {
        info = new ResourcerInfo(expr, &options);
        body->setTransformExtraOwned(info);
    }
    return info;
}


void EclResourcer::replaceGraphReferences(IHqlExpression * expr, ResourceGraphInfo * oldGraph, ResourceGraphInfo * newGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return;

    if (info->isActivity && info->graph != oldGraph)
        return;

    info->graph.set(newGraph);
    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    for (unsigned idx=first; idx < last; idx++)
        replaceGraphReferences(expr->queryChild(idx), oldGraph, newGraph);
}


void EclResourcer::removeLink(ResourceGraphLink & link, bool keepExternalUses)
{
    ResourcerInfo * info = (ResourcerInfo *)queryResourceInfo(link.sourceNode);
    assertex(info && info->numExternalUses > 0);
    if (!keepExternalUses)
        info->numExternalUses--;
    if (link.sinkGraph)
        link.sinkGraph->sources.zap(link);
    link.sourceGraph->sinks.zap(link);
    links.zap(link);
}

void EclResourcer::replaceGraphReferences(ResourceGraphInfo * oldGraph, ResourceGraphInfo * newGraph)
{
    ForEachItemIn(idx1, oldGraph->sinks)
    {
        ResourceGraphLink & sink = oldGraph->sinks.item(idx1);
        replaceGraphReferences(sink.sourceNode, oldGraph, newGraph);
    }

    ForEachItemInRev(idx2, links)
    {
        ResourceGraphLink & cur = links.item(idx2);
        if (cur.sourceGraph == oldGraph)
        {
            if (cur.sinkGraph == newGraph)
                removeLink(cur, false);
            else
                cur.changeSourceGraph(newGraph);
        }
        else if (cur.sinkGraph == oldGraph)
        {
            if (cur.sourceGraph == newGraph)
                removeLink(cur, false);
            else
                cur.changeSinkGraph(newGraph);
        }
    }
}

//------------------------------------------------------------------------------------------
// PASS1: Gather information about splitter locations..
void EclResourcer::tagActiveCursors(HqlExprCopyArray * activeRows)
{
    if (!activeRows)
        return;

    ForEachItemIn(i, *activeRows)
    {
        IHqlExpression & cur = activeRows->item(i);
        activeSelectors.append(cur);
        queryCreateResourceInfo(&cur)->isAlreadyInScope = true;
    }
}

static bool isPotentialCompoundSteppedIndexRead(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_compound_diskread:
        case no_compound_disknormalize:
        case no_compound_diskaggregate:
        case no_compound_diskcount:
        case no_compound_diskgroupaggregate:
        case no_compound_childread:
        case no_compound_childnormalize:
        case no_compound_childaggregate:
        case no_compound_childcount:
        case no_compound_childgroupaggregate:
        case no_compound_selectnew:
        case no_compound_inline:
            return false;
        case no_compound_indexread:
        case no_newkeyindex:
            return true;
        case no_getgraphloopresult:
            return true; // Could be an index read in another graph iteration, so don't combine
        case no_keyedlimit:
        case no_preload:
        case no_filter:
        case no_hqlproject:
        case no_newusertable:
        case no_limit:
        case no_sorted:
        case no_preservemeta:
        case no_distributed:
        case no_grouped:
        case no_stepped:
        case no_section:
        case no_sectioninput:
        case no_dataset_alias:
            break;
        case no_choosen:
            {
                IHqlExpression * arg2 = expr->queryChild(2);
                if (arg2 && !arg2->isPure())
                    return false;
                break;
            }
        default:
            return false;
        }
        expr = expr->queryChild(0);
    }
}

bool EclResourcer::findSplitPoints(IHqlExpression * expr, bool isProjected)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (info && info->visited)
    {
        if (!isProjected)
            info->clearProjected();

        if (info->isAlreadyInScope || info->isActivity || !info->containsActivity)
            return info->containsActivity;
    }
    else
    {
        info = queryCreateResourceInfo(expr);
        info->visited = true;
        if (!isProjected)
            info->clearProjected();

        bool isActivity = true;
        switch (expr->getOperator())
        {
        case no_select:
            //either a select from a setresult or use of a child-dataset
            if (isNewSelector(expr))
            {
                info->containsActivity = findSplitPoints(expr->queryChild(0), false);
                assertex(queryResourceInfo(expr->queryChild(0))->isActivity);
            }
            if (expr->isDataset() || expr->isDatarow())
            {
                info->isActivity = true;
                info->containsActivity = true;
            }
            return info->containsActivity;
        case no_mapto:
            throwUnexpected();
        case no_activerow:
            info->isActivity = true;
            info->containsActivity = false;
            return false;
        case no_rowset:                         // don't resource this as an activity
            isActivity = false;
            break;
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
        case no_getgraphloopresultset:
            info->isActivity = false;
            info->containsActivity = false;
            return false;
        case no_datasetlist:
            isActivity = false;
            break;
        case no_rowsetrange:
            {
                //Don't resource this as an activity if it is a function of the input graph rows, 
                //however we do want to if it is coming from a dataset list.
                IHqlExpression * ds = expr->queryChild(0);
                //MORE: Should walk further down the tree to allow for nested rowsetranges etc.
                if (ds->getOperator() == no_rowset || ds->getOperator() == no_getgraphloopresultset)
                {
                    info->isActivity = false;
                    info->containsActivity = false;
                    return false;
                }
                isActivity = false;
                break;
            }
        }

        ITypeInfo * type = expr->queryType();
        if (!type || type->isScalar())
            return false;

        info->isActivity = isActivity;
        info->containsActivity = true;
    }

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    for (unsigned idx=first; idx < last; idx++)
        findSplitPoints(expr->queryChild(idx), false);

    return info->containsActivity;
}


void EclResourcer::findSplitPoints(HqlExprArray & exprs)
{
    //Finding items that should be hoisted from child queries, and evaluated at this level is tricky:
    //* After hoisting a child expression, that may in turn contain other child expressions.
    //* Some child expressions are only worth hoisting if they are a simple function of something
    //  that will be evaluated here.
    //* If we're creating a single project for each x[1] then that needs to be done after all the
    //  expressions to hoist have been found.
    //* The usage counts have to correct - including
    //* The select child dependents for a ds[n] need to be inherited by the project(ds)[n]
    //=>
    //First walk the expression tree gathering all the expressions that will be used.
    //Project the expressions that need projecting.
    //Finally walk the tree again, this time calculating the correct usage counts.
    //
    //On reflection the hoisting and projecting could be implemented as a completely separate transformation.
    //However, it shares a reasonable amount of the logic with the rest of the resourcing, so keep together
    //for the moment.
    ForEachItemIn(idx, exprs)
        findSplitPoints(&exprs.item(idx), false);

    deriveUsageCounts(exprs);
}

void EclResourcer::deriveUsageCounts(IHqlExpression * expr)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !(info->containsActivity || info->isActivity))
        return;

    bool savedInsideNeverSplit = insideNeverSplit;
    bool savedInsideSteppedNeverSplit = insideSteppedNeverSplit;
    if (insideSteppedNeverSplit && info)
    {
        if (!isPotentialCompoundSteppedIndexRead(expr) && (expr->getOperator() != no_datasetlist) && expr->getOperator() != no_inlinetable)
            insideSteppedNeverSplit = false;
    }

    if (info->numUses)
    {
        if (insideNeverSplit || insideSteppedNeverSplit)
            info->neverSplit = true;

        if (info->isAlreadyInScope || info->isActivity || !info->containsActivity)
        {
            info->numUses++;
            return;
        }
    }
    else
    {
        info->numUses++;
        if (insideNeverSplit || insideSteppedNeverSplit)
            info->neverSplit = true;

        switch (expr->getOperator())
        {
        case no_select:
            //either a select from a setresult or use of a child-dataset
            if (isNewSelector(expr))
                deriveUsageCounts(expr->queryChild(0));
            return;
        case no_activerow:
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
        case no_rowset:                         // don't resource this as an activity
        case no_getgraphloopresultset:
            return;
        case no_keyedlimit:
            if (options.preventKeyedSplit)
                insideNeverSplit = true;
            break;
        case no_filter:
            if (options.preventKeyedSplit && filterIsKeyed(expr))
                insideNeverSplit = true;
            else
            {
                LinkedHqlExpr invariant;
                OwnedHqlExpr cond = extractFilterConditions(invariant, expr, expr->queryNormalizedSelector(), false, false);
                if (invariant)
                    info->isConditionalFilter = true;
            }
            break;
        case no_hqlproject:
        case no_newusertable:
        case no_aggregate:
        case no_newaggregate:
            if (options.preventKeyedSplit && expr->hasAttribute(keyedAtom))
                insideNeverSplit = true;
            break;
        case no_stepped:
        case no_mergejoin:
        case no_nwayjoin:
            if (options.preventSteppedSplit)
                insideSteppedNeverSplit = true;
            break;
        case no_compound_diskread:
        case no_compound_disknormalize:
        case no_compound_diskaggregate:
        case no_compound_diskcount:
        case no_compound_diskgroupaggregate:
        case no_compound_indexread:
        case no_compound_indexnormalize:
        case no_compound_indexaggregate:
        case no_compound_indexcount:
        case no_compound_indexgroupaggregate:
        case no_compound_childread:
        case no_compound_childnormalize:
        case no_compound_childaggregate:
        case no_compound_childcount:
        case no_compound_childgroupaggregate:
        case no_compound_selectnew:
        case no_compound_inline:
            insideNeverSplit = true;
            break;
        }

        ITypeInfo * type = expr->queryType();
        if (!type || type->isScalar())
        {
            insideNeverSplit = savedInsideNeverSplit;
            insideSteppedNeverSplit = savedInsideSteppedNeverSplit;
            return;
        }
    }

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);

    for (unsigned idx=first; idx < last; idx++)
        deriveUsageCounts(expr->queryChild(idx));

    insideNeverSplit = savedInsideNeverSplit;
    insideSteppedNeverSplit = savedInsideSteppedNeverSplit;
}


void EclResourcer::deriveUsageCounts(const HqlExprArray & exprs)
{
    ForEachItemIn(idx2, exprs)
        deriveUsageCounts(&exprs.item(idx2));
}

//------------------------------------------------------------------------------------------
// PASS2: Actually create the subgraphs based on splitters.

void EclResourcer::createInitialGraph(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerGraph, LinkKind linkKind, bool forceNewGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return;

    LinkKind childLinkKind = UnconditionalLink;
    Linked<ResourceGraphInfo> thisGraph = ownerGraph;
    bool forceNewChildGraph = false; 
    if (info->isActivity)
    {
        //Need to ensure no_libraryselects are not separated from the no_libraryscopeinstance
        //so ensure they are placed in the same graph.
        node_operator op = expr->getOperator();
        if (op == no_libraryselect)
        {
            ResourcerInfo * moduleInfo = queryResourceInfo(expr->queryChild(1));
            if (!info->graph && moduleInfo->graph)
                info->graph.set(moduleInfo->graph);
        }

        if (info->graph)
        {
            connectGraphs(info->graph, expr, ownerGraph, owner, linkKind);
            info->numExternalUses++;
            return;
        }

        unsigned numUses = info->numUses;
        switch (op)
        {
        case no_libraryscopeinstance:
            numUses = 1;
            break;
        case no_libraryselect:
            forceNewGraph = true;
            break;
        }

        if (!ownerGraph || numUses > 1 || (linkKind != UnconditionalLink) || forceNewGraph)
        {
            thisGraph.setown(createGraph());
            connectGraphs(thisGraph, expr, ownerGraph, owner, linkKind);
            info->numExternalUses++;
            if (!ownerGraph && sequential)
            {
                if (!expr->hasAttribute(_graphLocal_Atom))
                    thisGraph->hasSequentialSource = true;
            }
        }
        info->graph.set(thisGraph);
        if (info->isRootActivity)
            thisGraph->hasRootActivity = true;

        switch (expr->getOperator())
        {
        case no_compound:
            //NB: First argument is forced into a separate graph
            if (options.convertCompoundToExecuteWhen)
            {
                createInitialGraph(expr->queryChild(0), expr, thisGraph, UnconditionalLink, true);
            }
            else
            {
                HqlExprArray actions;
                expr->queryChild(0)->unwindList(actions, no_actionlist);
                ForEachItemIn(i, actions)
                    createInitialGraph(&actions.item(i), expr, NULL, UnconditionalLink, true);
            }
            createInitialGraph(expr->queryChild(1), expr, thisGraph, UnconditionalLink, false);
            return;
        case no_executewhen:
            {
                bool newGraph = expr->isAction() && (options.targetClusterType == HThorCluster);
                createInitialGraph(expr->queryChild(0), expr, thisGraph, UnconditionalLink, newGraph);
                createInitialGraph(expr->queryChild(1), expr, thisGraph, UnconditionalLink, true);
                return;
            }
        case no_keyindex:
        case no_newkeyindex:
            return;
        case no_parallel:
        case no_actionlist:
            {
                ForEachChild(i, expr)
                    createInitialGraph(expr->queryChild(i), expr, thisGraph, UnconditionalLink, options.actionLinkInNewGraph);
                return;
            }
        case no_if:
        case no_choose:
        case no_chooseds:
            //conditional nodes, the child branches are marked as conditional
            childLinkKind = UnconditionalLink;
            thisGraph->mergedConditionSource = true;
            if (!options.noConditionalLinks || (expr->isAction() && options.actionLinkInNewGraph))
                forceNewChildGraph = true;
            break;
        case no_filter:
            if (info->isConditionalFilter)
            {
                thisGraph->mergedConditionSource = true;
                if (!options.noConditionalLinks)
                    forceNewChildGraph = true;
            }
            break;
//      case no_nonempty:
        case no_sequential:
        case no_orderedactionlist:
            {
                unsigned first = getFirstActivityArgument(expr);
                unsigned last = first + getNumActivityArguments(expr);
                createInitialGraph(expr->queryChild(first), expr, thisGraph, SequenceLink, options.actionLinkInNewGraph);
                for (unsigned idx = first+1; idx < last; idx++)
                    createInitialGraph(expr->queryChild(idx), expr, thisGraph, SequenceLink, options.actionLinkInNewGraph);
                return;
            }
        case no_case:
        case no_map:
            {
                throwUnexpected();
            }
        case no_setgraphresult:
            {
                IHqlExpression * dataset = expr->queryChild(0);
                ResourcerInfo * childInfo = queryResourceInfo(dataset);
                childInfo->setPotentialSpillFile(expr);
                break;
            }
        case no_output:
            {
                //Tag the inputs to an output statement, so that if a spill was going to occur we read
                //from the output file instead of spilling.
                //Needs the grouping to be saved in the same way.  Could cope with compressed matching, but not
                //much point - since fairly unlikely.
                IHqlExpression * filename = queryRealChild(expr, 1);
                IHqlExpression * dataset = expr->queryChild(0);
                if (filename)
                {
                    if ((filename->getOperator() == no_constant) && !expr->hasAttribute(xmlAtom) && !expr->hasAttribute(jsonAtom) && !expr->hasAttribute(csvAtom))
                    {
                        if (expr->hasAttribute(groupedAtom) == isGrouped(dataset))
                        {
                            ResourcerInfo * childInfo = queryResourceInfo(dataset);
                            if (!isUpdatedConditionally(expr))
                                childInfo->setPotentialSpillFile(expr);
                        }
                    }
                }
                else
                {
                    if (matchesConstantValue(queryAttributeChild(expr, sequenceAtom, 0), ResultSequenceInternal))
                    {
                        //Do not read from a spilled workunit result in thor unless it is a child query
                        //Otherwise the distribution is lost causing inefficient code.
                        if (options.alwaysReuseGlobalSpills || (options.targetClusterType != ThorLCRCluster) || options.isChildQuery)
                        {
                            IHqlExpression * dataset = expr->queryChild(0);
                            ResourcerInfo * childInfo = queryResourceInfo(dataset);
                            childInfo->setPotentialSpillFile(expr);
                        }
                    }
                }

                if (isUpdatedConditionally(expr))
                    thisGraph->mergedConditionSource = true;
                break;
            }
        case no_buildindex:
            if (isUpdatedConditionally(expr))
                thisGraph->mergedConditionSource = true;
            break;
        }
    }

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    for (unsigned idx = first; idx < last; idx++)
        createInitialGraph(expr->queryChild(idx), expr, thisGraph, childLinkKind, forceNewChildGraph);
}


void EclResourcer::createInitialGraphs(HqlExprArray & exprs)
{
    ForEachItemIn(idx, exprs)
    {
        IHqlExpression * rootExpr = &exprs.item(idx);
        ResourcerInfo * info = queryResourceInfo(rootExpr);
        assertex(info->isActivity);
        if (!rootExpr->hasAttribute(_lazy_Atom) && !rootExpr->hasAttribute(_graphLocal_Atom))
            info->setRootActivity();

        createInitialGraph(rootExpr, NULL, NULL, UnconditionalLink, false);
    }
}

void EclResourcer::createInitialRemoteGraph(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerGraph, bool forceNewGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return;

    Linked<ResourceGraphInfo> thisGraph = ownerGraph;
    if (info->isActivity)
    {
        if (info->graph)
        {
            connectGraphs(info->graph, expr, ownerGraph, owner, UnconditionalLink);
            info->numExternalUses++;
            return;
        }

        if (!ownerGraph || forceNewGraph)
        {
            thisGraph.setown(createGraph());
            connectGraphs(thisGraph, expr, ownerGraph, owner, UnconditionalLink);
            info->numExternalUses++;
        }
        info->graph.set(thisGraph);

        switch (expr->getOperator())
        {
        case no_compound:
            if (options.convertCompoundToExecuteWhen)
                createInitialRemoteGraph(expr->queryChild(0), expr, thisGraph, true);
            else
                createInitialRemoteGraph(expr->queryChild(0), expr, NULL, true);
            createInitialRemoteGraph(expr->queryChild(1), expr, thisGraph, false);
            return;
        case no_executewhen:
            createInitialRemoteGraph(expr->queryChild(0), expr, thisGraph, false);
            createInitialRemoteGraph(expr->queryChild(1), expr, thisGraph, true);
            return;
        }
    }

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    for (unsigned idx = first; idx < last; idx++)
        createInitialRemoteGraph(expr->queryChild(idx), expr, thisGraph, false);
}


void EclResourcer::createInitialRemoteGraphs(HqlExprArray & exprs)
{
    ForEachItemIn(idx, exprs)
        createInitialRemoteGraph(&exprs.item(idx), NULL, NULL, false);
}

//------------------------------------------------------------------------------------------
// PASS3: Tag graphs/links that are conditional or unconditional

void EclResourcer::markChildDependentsAsUnconditional(ResourcerInfo * info, IHqlExpression * condition)
{
    ForEachItemIn(iDepend, info->dependsOn)
        markAsUnconditional(info->dependsOn.item(iDepend).sourceNode, info->graph, condition);
}

void EclResourcer::markAsUnconditional(IHqlExpression * expr, ResourceGraphInfo * ownerGraph, IHqlExpression * condition)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return;

    if (!info->isActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        for (unsigned idx=first; idx < last; idx++)
            markAsUnconditional(expr->queryChild(idx), ownerGraph, condition);
        return;
    }

    if (condition)
        if (info->addCondition(condition))
            condition = NULL;

    if (info->pathToExpr == ResourcerInfo::PathUnconditional)
        return;

    if ((info->pathToExpr == ResourcerInfo::PathConditional) && condition)
    {
        if (targetClusterType == RoxieCluster)
        {
            if (options.spillMultiCondition)
            {
                if (info->graph != ownerGraph)
                    info->graph->isUnconditional = true;
            }
            return;
        }
        else
        {
            if (info->graph != ownerGraph)
                return;
        }
    }

    bool wasConditional = (info->pathToExpr == ResourcerInfo::PathConditional);
    if (!condition)
    {
        info->pathToExpr = ResourcerInfo::PathUnconditional;
        if (info->graph != ownerGraph)
            info->graph->isUnconditional = true;
    }
    else
        info->pathToExpr = ResourcerInfo::PathConditional;

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_if:
    case no_choose:
    case no_chooseds:
        if (options.noConditionalLinks)
            break;
        if (condition)
            markCondition(expr, condition, wasConditional);
        else
        {
            //This list is processed in a second phase.
            if (rootConditions.find(*expr) == NotFound)
                rootConditions.append(*LINK(expr));
        }
        markChildDependentsAsUnconditional(info, condition);
        return;
    case no_filter:
        if (!info->isConditionalFilter || options.noConditionalLinks)
            break;

        if (condition)
            markCondition(expr, condition, wasConditional);
        else
        {
            //This list is processed in a second phase.
            if (rootConditions.find(*expr) == NotFound)
                rootConditions.append(*LINK(expr));
        }
        markChildDependentsAsUnconditional(info, condition);
        return;
    case no_compound:
        if (!options.convertCompoundToExecuteWhen)
        {
            HqlExprArray actions;
            expr->queryChild(0)->unwindList(actions, no_actionlist);
            ForEachItemIn(i, actions)
                markAsUnconditional(&actions.item(i), NULL, NULL);
            markAsUnconditional(expr->queryChild(1), ownerGraph, condition);
            return;
        }
        break;
    case no_sequential:
    case no_orderedactionlist:
//  case no_nonempty:
        if (!options.isChildQuery)
        {
            unsigned first = getFirstActivityArgument(expr);
            unsigned last = first + getNumActivityArguments(expr);
            IHqlExpression * child0 = expr->queryChild(0);
            markAsUnconditional(child0, info->graph, condition);
            queryResourceInfo(child0)->graph->hasConditionSource = true;        // force it to generate even if contains something very simple e.g., null action
            for (unsigned idx = first+1; idx < last; idx++)
            {
                OwnedHqlExpr tag = createAttribute(instanceAtom, LINK(expr), getSizetConstant(idx), LINK(condition));
                IHqlExpression * child = expr->queryChild(idx);
                markAsUnconditional(child, queryResourceInfo(child)->graph, tag);

                queryResourceInfo(child)->setConditionSource(tag, !wasConditional);
            }
            markChildDependentsAsUnconditional(info, condition);
            return;
        }
        break;

    case no_case:
    case no_map:
        UNIMPLEMENTED;
    }

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    for (unsigned idx=first; idx < last; idx++)
        markAsUnconditional(expr->queryChild(idx), info->graph, condition);

    markChildDependentsAsUnconditional(info, condition);
}


void EclResourcer::markConditionBranch(unsigned childIndex, IHqlExpression * expr, IHqlExpression * condition, bool wasConditional)
{
    IHqlExpression * child = queryRealChild(expr, childIndex);
    if (child)
    {
        OwnedHqlExpr tag;
        if (expr->getOperator() == no_if)
            tag.setown(createAttribute(((childIndex==1) ? trueAtom : falseAtom), LINK(expr), LINK(condition)));
        else
            tag.setown(createAttribute(trueAtom, LINK(expr), LINK(condition), getSizetConstant(childIndex)));
        markAsUnconditional(child, queryResourceInfo(child)->graph, tag);

        queryResourceInfo(child)->setConditionSource(tag, !wasConditional);
    }
}


void EclResourcer::markCondition(IHqlExpression * expr, IHqlExpression * condition, bool wasConditional)
{
    if (expr->getOperator() == no_filter)
    {
        markConditionBranch(0, expr, condition, wasConditional);
    }
    else
    {
        ForEachChildFrom(i, expr, 1)
            markConditionBranch(i, expr, condition, wasConditional);
    }
}

void EclResourcer::markConditions(HqlExprArray & exprs)
{
    ForEachItemIn(idx, exprs)
    {
        IHqlExpression & cur = exprs.item(idx);
        if (!cur.hasAttribute(_graphLocal_Atom))
        {
            //MORE: What should lazy do when implemented?
            markAsUnconditional(&cur, NULL, NULL);
        }
    }

    //More efficient to process the conditional branches once all unconditional branches have been walked.
    ForEachItemIn(idx2, rootConditions)
        markCondition(&rootConditions.item(idx2), NULL, false);
}



//------------------------------------------------------------------------------------------
// PASS4: Split subgraphs based on resource requirements for activities
//This will need to be improved if we allow backtracking to get the best combination of activities to fit in the subgraph

void EclResourcer::createResourceSplit(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerNewGraph, ResourceGraphInfo * originalGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);

    info->graph.setown(createGraph());
    info->graph->isUnconditional = originalGraph->isUnconditional;
    changeGraph(expr, info->graph);
    connectGraphs(info->graph, expr, ownerNewGraph, owner, UnconditionalLink);
    info->numExternalUses++;
}



void EclResourcer::getResources(IHqlExpression * expr, CResources & exprResources)
{
    ::getResources(expr, exprResources, options);
}


bool EclResourcer::calculateResourceSpillPoints(IHqlExpression * expr, ResourceGraphInfo * graph, CResources & resourcesSoFar, bool hasGoodSpillPoint, bool canSpill)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return true;

    if (!info->isActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        if (last - first == 1)
            return calculateResourceSpillPoints(expr->queryChild(first), graph, resourcesSoFar, hasGoodSpillPoint, canSpill);
        for (unsigned idx = first; idx < last; idx++)
            calculateResourceSpillPoints(expr->queryChild(idx), graph, resourcesSoFar, false, canSpill);
        return true;
    }

    if (info->graph != graph)
        return true;

    CResources exprResources(options.clusterSize);
    getResources(expr, exprResources);

    info->isSpillPoint = false;
    Owned<CResources> curResources = LINK(&resourcesSoFar);
    if (resourcesSoFar.addExceeds(exprResources, *resourceLimit))
    {
        if (hasGoodSpillPoint)
            return false;
        info->isSpillPoint = true;
        spilled = true;
        curResources.setown(new CResources(options.clusterSize));
        if (exprResources.exceeds(*resourceLimit))
            throwError2(HQLERR_CannotResourceActivity, getOpString(expr->getOperator()), options.clusterSize);
    }

    if (options.minimizeSkewBeforeSpill)
    {
        if (canSpill && heavyweightAndReducesSizeOrSkew(expr))
        {
            //if the input activity is going to cause us to run out of resources, then it is going to be better to split here than anywhere else
            //this code may conceivably cause extra spills far away because the hash distributes are moved.
            IHqlExpression * childExpr = expr->queryChild(0);
            ResourcerInfo * childInfo = queryResourceInfo(childExpr);

            if (childInfo->graph == graph)
            {
                CResources childResources(options.clusterSize);
                getResources(childExpr, childResources);
                childResources.add(exprResources);
                if (curResources->addExceeds(childResources, *resourceLimit))
                {
                    info->isSpillPoint = true;
                    spilled = true;
                    calculateResourceSpillPoints(childExpr, graph, exprResources, false, true);
                    return true;
                }
            }
            //otherwise continue as normal.
        }
    }

    curResources->add(exprResources);

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    if (hasGoodSpillPoint)
    {
        if (exprResources.resource[RESheavy] || exprResources.resource[REShashdist] || last-first != 1)
            hasGoodSpillPoint = false;
    }
    else if (!info->isSpillPoint && canSpill)
    {
        if (lightweightAndReducesDatasetSize(expr) || queryHint(expr, spillAtom))
        {
            CResources savedResources(*curResources);
            if (!calculateResourceSpillPoints(expr->queryChild(0), graph, *curResources, true, true))
            {
                curResources->set(savedResources);
                info->isSpillPoint = true;
                spilled = true;
                calculateResourceSpillPoints(expr->queryChild(0), graph, exprResources, false, true);
            }
            return true;
        }
    }

    node_operator op = expr->getOperator();
    if ((op == no_if) || (op == no_choose) || (op == no_chooseds))
    {
        //For conditions, spill on intersection of resources used, not union.
        CResources savedResources(*curResources);
        if (!calculateResourceSpillPoints(expr->queryChild(1), graph, *curResources, hasGoodSpillPoint, true))
            return false;
        ForEachChildFrom(i, expr, 2)
        {
            if (expr->queryChild(i)->isAttribute())
                continue;
            if (!calculateResourceSpillPoints(expr->queryChild(i), graph, savedResources, hasGoodSpillPoint, true))
                return false;
            curResources->maximize(savedResources);
        }
    }
    else
    {
        for (unsigned idx = first; idx < last; idx++)
            if (!calculateResourceSpillPoints(expr->queryChild(idx), graph, *curResources, hasGoodSpillPoint, true))
                return false;
    }
    return true;
}

void EclResourcer::insertResourceSpillPoints(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerOriginalGraph, ResourceGraphInfo * ownerNewGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return;

    if (!info->isActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        for (unsigned idx = first; idx < last; idx++)
            insertResourceSpillPoints(expr->queryChild(idx), expr, ownerOriginalGraph, ownerNewGraph);
        return;
    }

    ResourceGraphInfo * originalGraph = info->graph;    //NB: Graph will never cease to exist, so don't need to link.
    if (originalGraph != ownerOriginalGraph)
        return;

    if (info->isSpillPoint)
        createResourceSplit(expr, owner, ownerNewGraph, originalGraph);
    else if (info->graph != ownerNewGraph)
        changeGraph(expr, ownerNewGraph);

    CResources exprResources(options.clusterSize);
    getResources(expr, exprResources);

    bool ok = info->graph->allocateResources(exprResources, *resourceLimit);
    assertex(ok);

    node_operator op = expr->getOperator();
    if ((op == no_if) || (op == no_choose) || (op == no_chooseds))
    {
        CResources savedResources(info->graph->resources);
        insertResourceSpillPoints(expr->queryChild(1), expr, originalGraph, info->graph);

        ForEachChildFrom(i, expr, 2)
        {
            if (expr->queryChild(i)->isAttribute())
                continue;
            CResources branchResources(info->graph->resources);
            info->graph->resources.set(savedResources);
            insertResourceSpillPoints(expr->queryChild(i), expr, originalGraph, info->graph);
            info->graph->resources.maximize(branchResources);
        }
    }
    else
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        for (unsigned idx = first; idx < last; idx++)
            insertResourceSpillPoints(expr->queryChild(idx), expr, originalGraph, info->graph);
    }
}


void EclResourcer::resourceSubGraph(ResourceGraphInfo * graph)
{
    if (graph->beenResourced)
        return;
    graph->beenResourced = true;
    ForEachItemIn(idx, graph->sources)
        resourceSubGraph(graph->sources.item(idx).sourceGraph);

    IHqlExpression * sourceNode = graph->sinks.item(0).sourceNode->queryBody();
#ifdef _DEBUG
    //Sanity check, ensure there is only a single sink for this graph.  
    //However because libraryselects are tightly bound to their library instance there may be multiple library selects.  
    //They won't affect the resourcing though, since they'll plug into the same library instance, and the selects use no resources.
    ForEachItemIn(idx2, graph->sinks)
    {
        IHqlExpression * thisSink = graph->sinks.item(idx2).sourceNode->queryBody();
        if (thisSink->getOperator() != no_libraryselect)
            assertex(thisSink == sourceNode);
    }
#endif
    spilled = false;
    CResources resources(options.clusterSize);
    calculateResourceSpillPoints(sourceNode, graph, resources, false, false);
    if (spilled)
        insertResourceSpillPoints(sourceNode, NULL, graph, graph);
    else
        graph->resources.set(resources);
}

void EclResourcer::resourceSubGraphs(HqlExprArray & exprs)
{
    ForEachItemIn(idx, graphs)
        resourceSubGraph(&graphs.item(idx));
}


//------------------------------------------------------------------------------------------
// PASS5: Link subrgaphs with dependency information so they don't get merged by accident.

struct DependencySourceInfo
{
    HqlExprArray                    search;
    CIArrayOf<ResourceGraphInfo>    graphs;
    HqlExprArray                    exprs;
};

class EclResourceDependencyGatherer
{
public:
    EclResourceDependencyGatherer(IErrorReceiver * _errors)
    : errors(_errors)
    {
    }

    void addChildDependencies(IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr);
    bool addExprDependency(IHqlExpression * expr, ResourceGraphInfo * curGraph, IHqlExpression * activityExpr);
    void appendLinkOwn(ResourceGraphDependencyLink & link) { links.append(link); }

    const CIArrayOf<ResourceGraphLink> & queryLinks() const { return links; }

protected:
    void addDependencySource(IHqlExpression * search, ResourceGraphInfo * curGraph, IHqlExpression * expr);
    void addDependencyUse(IHqlExpression * search, ResourceGraphInfo * curGraph, IHqlExpression * expr);
    void addRefExprDependency(IHqlExpression * expr, ResourceGraphInfo * curGraph, IHqlExpression * activityExpr);
    void doAddChildDependencies(IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr);

protected:
    DependencySourceInfo dependencySource;
    IErrorReceiver * errors;
    CIArrayOf<ResourceGraphLink> links;
};

void EclResourceDependencyGatherer::addDependencySource(IHqlExpression * search, ResourceGraphInfo * curGraph, IHqlExpression * expr)
{
    //MORE: Should we check this doesn't already exist?
    dependencySource.search.append(*LINK(search));
    dependencySource.graphs.append(*LINK(curGraph));
    dependencySource.exprs.append(*LINK(expr));
}

void EclResourceDependencyGatherer::addDependencyUse(IHqlExpression * search, ResourceGraphInfo * curGraph, IHqlExpression * expr)
{
    unsigned index = dependencySource.search.find(*search);
    if (index != NotFound)
    {
        if (&dependencySource.graphs.item(index) == curGraph)
        {
            //Don't give a warning if get/set is within the same activity (e.g., within a local())
            if (&dependencySource.exprs.item(index) != expr)
                //errors->reportWarning(HQLWRN_RecursiveDependendencies, HQLWRN_RecursiveDependendencies_Text, *codeGeneratorAtom, 0, 0, 0);
                errors->reportError(HQLWRN_RecursiveDependendencies, HQLWRN_RecursiveDependendencies_Text, str(codeGeneratorId), 0, 0, 0);
        }
        else
        {
            IHqlExpression * sourceExpr = &dependencySource.exprs.item(index);
            ResourceGraphLink * link = new ResourceGraphDependencyLink(&dependencySource.graphs.item(index), sourceExpr, curGraph, expr, search);
            links.append(*link);
        }
    }
}


void EclResourceDependencyGatherer::addRefExprDependency(IHqlExpression * expr, ResourceGraphInfo * curGraph, IHqlExpression * activityExpr)
{
    IHqlExpression * filename = queryTableFilename(expr);
    if (filename)
    {
        OwnedHqlExpr value = createAttribute(fileAtom, getNormalizedFilename(filename));
        addDependencySource(value, curGraph, activityExpr);
    }
}

bool EclResourceDependencyGatherer::addExprDependency(IHqlExpression * expr, ResourceGraphInfo * curGraph, IHqlExpression * activityExpr)
{
    switch (expr->getOperator())
    {
    case no_buildindex:
    case no_output:
        {
            IHqlExpression * filename = queryRealChild(expr, 1);
            if (filename)
            {
                switch (filename->getOperator())
                {
                case no_pipe:
//                  allWritten = true;
                    break;
                default:
                    OwnedHqlExpr value = createAttribute(fileAtom, getNormalizedFilename(filename));
                    addDependencySource(value, curGraph, activityExpr);
                    break;
                }
            }
            else
            {
                IHqlExpression * seq = querySequence(expr);
                assertex(seq && seq->queryValue());
                IHqlExpression * name = queryResultName(expr);
                OwnedHqlExpr value = createAttribute(resultAtom, LINK(seq), LINK(name));
                addDependencySource(value, curGraph, activityExpr);
            }
            return true;
        }
    case no_keydiff:
        {
            addRefExprDependency(expr->queryChild(0), curGraph, activityExpr);
            addRefExprDependency(expr->queryChild(1), curGraph, activityExpr);
            OwnedHqlExpr value = createAttribute(fileAtom, getNormalizedFilename(expr->queryChild(2)));
            addDependencySource(value, curGraph, activityExpr);
            return true;
        }
    case no_keypatch:
        {
            addRefExprDependency(expr->queryChild(0), curGraph, activityExpr);
            OwnedHqlExpr patchName = createAttribute(fileAtom, getNormalizedFilename(expr->queryChild(1)));
            addDependencyUse(patchName, curGraph, activityExpr);
            OwnedHqlExpr value = createAttribute(fileAtom, getNormalizedFilename(expr->queryChild(2)));
            addDependencySource(value, curGraph, activityExpr);
            return true;
        }
    case no_table:
        {
            IHqlExpression * filename = expr->queryChild(0);
            OwnedHqlExpr value = createAttribute(fileAtom, getNormalizedFilename(filename));
            addDependencyUse(value, curGraph, activityExpr);
            return !filename->isConstant();
        }
    case no_select:
        return isNewSelector(expr);
    case no_workunit_dataset:
        {
            IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryAttributeChild(expr, nameAtom, 0);
            OwnedHqlExpr value = createAttribute(resultAtom, LINK(sequence), LINK(name));
            addDependencyUse(value, curGraph, activityExpr);
            return false;
        }
    case no_getresult:
        {
            IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
            OwnedHqlExpr value = createAttribute(resultAtom, LINK(sequence), LINK(name));
            addDependencyUse(value, curGraph, activityExpr);
            return false;
        }
    case no_getgraphresult:
        {
            OwnedHqlExpr value = createAttribute(resultAtom, LINK(expr->queryChild(1)), LINK(expr->queryChild(2)));
            addDependencyUse(value, curGraph, activityExpr);
            return false;
        }
    case no_setgraphresult:
        {
            OwnedHqlExpr value = createAttribute(resultAtom, LINK(expr->queryChild(1)), LINK(expr->queryChild(2)));
            addDependencySource(value, curGraph, activityExpr);
            return true;
        }
    case no_ensureresult:
    case no_setresult:
    case no_extractresult:
        {
            IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
            OwnedHqlExpr value = createAttribute(resultAtom, LINK(sequence), LINK(name));
            addDependencySource(value, curGraph, activityExpr);
            return true;
        }
    case no_attr:
    case no_attr_link:
    case no_record:
    case no_field:
        return false; //no need to look any further
    default:
        return true;
    }
}


void EclResourceDependencyGatherer::doAddChildDependencies(IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr)
{
    if (expr->queryTransformExtra())
        return;
    expr->setTransformExtraUnlinked(expr);

    if (addExprDependency(expr, graph, activityExpr))
    {
        ForEachChild(idx, expr)
            doAddChildDependencies(expr->queryChild(idx), graph, activityExpr);
    }
}

void EclResourceDependencyGatherer::addChildDependencies(IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr)
{
    if (graph)
    {
        TransformMutexBlock block;
        doAddChildDependencies(expr, graph, activityExpr);
    }
}

//-----------------------------------------

void EclResourcer::addDependencies(EclResourceDependencyGatherer & gatherer, IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info)
        return;

    if (info->containsActivity)
    {
        if (info->isActivity)
        {
            if (info->gatheredDependencies)
                return;
            info->gatheredDependencies = true;
            graph = info->graph;
            activityExpr = expr;
        }
        if (gatherer.addExprDependency(expr, graph, activityExpr))
        {
            unsigned first = getFirstActivityArgument(expr);
            unsigned last = first + getNumActivityArguments(expr);
            ForEachChild(idx, expr)
            {
                if ((idx >= first) && (idx < last))
                    addDependencies(gatherer, expr->queryChild(idx), graph, activityExpr);
                else
                    gatherer.addChildDependencies(expr->queryChild(idx), graph, activityExpr);
            }
        }
    }
    else
        gatherer.addChildDependencies(expr, graph, activityExpr);
}

void EclResourcer::addDependencies(HqlExprArray & exprs)
{
    EclResourceDependencyGatherer gatherer(errors);
    ForEachItemIn(idx, exprs)
        addDependencies(gatherer, &exprs.item(idx), NULL, NULL);

    const CIArrayOf<ResourceGraphLink> & dependLinks = gatherer.queryLinks();
    ForEachItemIn(i, dependLinks)
    {
        ResourceGraphLink & curLink = dependLinks.item(i);

        //NOTE: queryResourceInfo() cannot be called inside gatherer because that uses the transform mutex
        //for something different
        ResourcerInfo * sinkInfo = queryResourceInfo(curLink.sinkNode);
        sinkInfo->dependsOn.append(curLink);
        curLink.sinkGraph->dependsOn.append(curLink);
        links.append(OLINK(curLink));
    }
}

//--------------------------------------------------------

void EclResourcer::oldSpotUnbalancedSplitters(IHqlExpression * expr, unsigned whichSource, IHqlExpression * path, ResourceGraphInfo * graph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info)
        return;

    if (graph && info->graph && info->graph != graph)
    {
        if ((info->currentSource == whichSource) && (info->pathToSplitter != path))
            graph->unbalancedExternalSources.append(*LINK(expr->queryBody()));
        info->currentSource = whichSource;
        info->pathToSplitter.set(path);
        return;
    }
    else
    {
        if (info->currentSource == whichSource)
        {
            if (info->pathToSplitter != path)
                info->balanced = false;
            return;
        }

        info->currentSource = whichSource;
        info->pathToSplitter.set(path);
    }

    if (info->containsActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned num = getNumActivityArguments(expr);
        bool modify = false;
        if (num > 1)
        {
            switch (expr->getOperator())
            {
            case no_addfiles:
                if (expr->hasAttribute(_ordered_Atom) || expr->hasAttribute(_orderedPull_Atom) || isGrouped(expr))
                    modify = true;
                break;
            default:
                modify = true;
                break;
            }
        }

        unsigned last = first + num;
        for (unsigned idx = first; idx < last; idx++)
        {
            OwnedHqlExpr childPath = modify ? createAttribute(pathAtom, getSizetConstant(idx), LINK(path)) : LINK(path);

            oldSpotUnbalancedSplitters(expr->queryChild(idx), whichSource, childPath, graph);
        }
    }


    //Now check dependencies between graphs (for roxie)
    if (!graph)
    {
        if (info->graph)
        {
            GraphLinkArray & graphLinks = info->graph->dependsOn;
            ForEachItemIn(i, graphLinks)
            {
                ResourceGraphLink & link = graphLinks.item(i);
                if (link.sinkNode == expr)
                {
                    OwnedHqlExpr childPath = createAttribute(dependencyAtom, LINK(link.sourceNode), LINK(path));
                    oldSpotUnbalancedSplitters(link.sourceNode, whichSource, childPath, graph);
                }
            }
        }
        else
        {
            ForEachItemIn(i, links)
            {
                ResourceGraphLink & link = links.item(i);
                if (link.sinkNode == expr)
                {
                    OwnedHqlExpr childPath = createAttribute(dependencyAtom, LINK(link.sourceNode), LINK(path));
                    oldSpotUnbalancedSplitters(link.sourceNode, whichSource, childPath, graph);
                }
            }
        }
    }
}

void EclResourcer::oldSpotUnbalancedSplitters(HqlExprArray & exprs)
{
    unsigned curSource = 1;
    switch (targetClusterType)
    {
    case HThorCluster:
        break;
    case ThorLCRCluster:
        {
            //Thor only handles one graph at a time, so only walk expressions within a single graph.
            ForEachItemIn(i1, graphs)
            {
                ResourceGraphInfo & curGraph = graphs.item(i1);
                ForEachItemIn(i2, curGraph.sinks)
                {
                    ResourceGraphLink & cur = curGraph.sinks.item(i2);
                    oldSpotUnbalancedSplitters(cur.sourceNode, curSource++, 0, &curGraph);
                }
            }
        }
        break;
    case RoxieCluster:
        {
            //Roxie pulls all at once, so need to analyse globally.
            ForEachItemIn(idx, exprs)
                oldSpotUnbalancedSplitters(&exprs.item(idx), curSource++, 0, NULL);
            break;
        }
    }
}

void EclResourcer::spotSharedInputs(IHqlExpression * expr, ResourceGraphInfo * graph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info)
        return;

    if (info->graph && info->graph != graph)
    {
        IHqlExpression * body = expr->queryBody();
        if (!graph->unbalancedExternalSources.contains(*body))
            graph->balancedExternalSources.append(*LINK(body));

        return;
    }

    if (info->isSplit())
    {
        //overload currentSource to track if we have visited this splitter before.  It cannot have value value NotFound up to now
        if (info->currentSource == NotFound)
            return;
        info->currentSource = NotFound;
    }

    if (info->containsActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned num = getNumActivityArguments(expr);
        unsigned last = first + num;
        for (unsigned idx = first; idx < last; idx++)
        {
            spotSharedInputs(expr->queryChild(idx), graph);
        }
    }
}

void EclResourcer::spotSharedInputs()
{
    //Thor only handles one graph at a time, so only walk expressions within a single graph.
    ForEachItemIn(i1, graphs)
    {
        ResourceGraphInfo & curGraph = graphs.item(i1);
        HqlExprCopyArray visited;
        ForEachItemIn(i2, curGraph.sinks)
        {
            ResourceGraphLink & cur = curGraph.sinks.item(i2);
            IHqlExpression * curExpr = cur.sourceNode->queryBody();
            if (!visited.contains(*curExpr))
            {
                ResourcerInfo * info = queryResourceInfo(curExpr);
                if (!info->isExternalSpill() && !info->expandRatherThanSpill(true))
                {
                    spotSharedInputs(curExpr, &curGraph);
                    visited.append(*curExpr);
                }
            }
        }
    }
}

//--------------------------------------------------------------------------------------------------------------------

/*
 * Splitters can either have a limited or unlimited read-ahead.  A splitter with unlimited read-ahead is likely to use
 * more memory, and needs to be able to spill its read ahead buffer to disk.  A splitter with limited read-ahead
 * ("balanced") is likely to be more efficient - but it can also potentially cause deadlock.
 *
 * A balanced splitter can deadlock because each of its output activities is effectively dependent on the others, which
 * can create dependency cycles in the graph.
 *
 * Say you have f(x,y) and g(x,y), if x and y are unbalanced splitters you have directed edges (f->x),(f->y),(g->x),(g->y)
 * which contains no cycles.  If x is balanced then you also have (x->f),(x->g) - still without any cycles, but if y is
 * also balanced then you gain the edges (y->f,y->g) which creates a cycle.  So either x or y need to become unbalanced.
 *
 * The previous implementation worked by tagging each splitter with the paths that were used to get there, and
 * marking a splitter as unbalanced if there was more than one path from the same output.  However that fails to catch the
 * case where you have f(a,b) and g(a,b) which create more complicated cycles.
 *
 * This is essentially a graph traversal problem, where a balanced splitter makes each output dependent on each other
 * (they effectively become bi-directional dependencies).  Converting balanced splitters to unbalanced is equivalent to
 * calculating the feedback arc set that will remove all the cycles.  (Unfortunately calculating the minimal set is NP
 * hard, but because we are dealing with a specialised graph we can avoid that.)
 *
 * The approach is as follows
 *   Loop through each output
 *     If not already visited (from another output), then perform a depth-first traversal of the graph for that output
 *
 * When traversing a node we mark it as being visited, and then walk through the other links.
 *    If node at the end of the current link is being visited then we have found a cycle - so we need to introduce
 *       a unbalanced splitter to remove some of the paths.  That splitter must be on the path between the two
 *       visits to the nodes.
 *       - It could be the node we have just reached
 *       - It could be any other node on the route.
 *
 *    We can tell which one it is because
 *    - if is is the node we have just reached, the visiting link must be from an output, and previous link from node to output
 *    - if on the path then exit path will be to an output, and entry path from an output.
 *
 *    If not then there must be by definition another node on that path that satisfies that condition.
 *
 * Note: This may over-estimate the number of splitters that need to be marked as unbalanced, but in general it is fairly good.
 */


CSplitterInfo::CSplitterInfo(EclResourcer & _resourcer, bool _preserveBalanced, bool _ignoreExternalDependencies)
: resourcer(_resourcer), preserveBalanced(_preserveBalanced), ignoreExternalDependencies(_ignoreExternalDependencies)
{
    nextBalanceId = 0;
#ifdef TRACE_BALANCED
    printf("digraph {\n");
#endif
}

CSplitterInfo::~CSplitterInfo()
{
    if (preserveBalanced)
        restoreBalanced();
#ifdef TRACE_BALANCED
    printf("}\n");
#endif
}


void CSplitterInfo::addLink(IHqlExpression * source, IHqlExpression * sink, bool isExternal)
{
    ResourcerInfo * sourceInfo = queryResourceInfo(source);
#ifdef TRACE_BALANCED
    if (sourceInfo->balanceId == 0)
    {
        sourceInfo->balanceId = ++nextBalanceId;
        printf("\tn%u [label=\"%u\"]; // %s\n", sourceInfo->balanceId, sourceInfo->balanceId, getOpString(source->getOperator()));
    }
#endif
    if (isExternal)
    {
        if (!externalSources.contains(*source))
        {
            externalSources.append(*source);
            if (preserveBalanced)
                wasBalanced.append(sourceInfo->balanced);
        }
    }

    sourceInfo->balanced = true;

    if (sink)
    {
        CSplitterLink * link = new CSplitterLink(source, sink);
        ResourcerInfo * sinkInfo = queryResourceInfo(sink);

        sourceInfo->balancedLinks.append(*link);
        sinkInfo->balancedLinks.append(*LINK(link));
        sourceInfo->balancedInternalUses++;

#ifdef TRACE_BALANCED
        printf("\tn%u -> n%u;\n", sourceInfo->balanceId, sinkInfo->balanceId);
#endif
    }
    else
    {
        sinks.append(*source);
        sourceInfo->balancedExternalUses++;
    }
}

void CSplitterLink::mergeSinkLink(CSplitterLink & sinkLink)
{
    IHqlExpression * newSink = sinkLink.querySink();
    assertex(newSink);
    assertex(sinkLink.hasSource(querySink()));
    sink.set(newSink);
    ResourcerInfo * sinkInfo = queryResourceInfo(newSink);
    unsigned sinkPos = sinkInfo->balancedLinks.find(sinkLink);
    sinkInfo->balancedLinks.replace(OLINK(*this), sinkPos);
}

bool CSplitterInfo::allInputsPulledIndependently(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_addfiles:
        if (expr->hasAttribute(_ordered_Atom) || expr->hasAttribute(_orderedPull_Atom) || isGrouped(expr))
            return false;
        return true;
    case no_parallel:
        //MORE; This can probably return true - and generate fewer unbalanced splitters.
        break;
    }
    return false;
}

bool CSplitterInfo::isSplitOrBranch(IHqlExpression * expr) const
{
    unsigned num = getNumActivityArguments(expr);
    if (num > 1)
        return true;

    //Is this potentially splitter?  Better to have false positives...
    ResourcerInfo * info = queryResourceInfo(expr);
    assertex(info);
    if (info->numUses > 1)
        return true;

    if (info->hasDependency())
        return true;

    return false;
}

bool CSplitterInfo::isBalancedSplitter(IHqlExpression * expr) const
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info->balanced)
        return false;
    unsigned numOutputs = info->balancedExternalUses + info->balancedInternalUses;
    return (numOutputs > 1);
}


void CSplitterInfo::gatherPotentialSplitters(IHqlExpression * expr, IHqlExpression * sink, ResourceGraphInfo * graph, bool isDependency)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info)
        return;

    switch (expr->getOperator())
    {
    case no_null:
    case no_fail:
        //Any sources that never generate any rows are always fine as a splitter
        return;
    //MORE: A source that generates a single row, and subsequent rows are never read, is always fine.
    //      but if read as a dataset it could deadlock unless there was a 1 row read-ahead.

    }
    bool alreadyVisited = resourcer.checkAlreadyVisited(info);
    if (!alreadyVisited)
    {
        info->resetBalanced();
#ifdef TRACE_BALANCED
        info->balanceId = 0;
#endif
    }

    if (graph && info->graph && info->graph != graph)
    {
        if (!ignoreExternalDependencies || !isDependency)
            addLink(expr, sink, true);
        return;
    }

    if (isSplitOrBranch(expr) || !sink)
    {
        addLink(expr, sink, false);
        if (alreadyVisited)
            return;

        sink = expr;
    }

    if (info->containsActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned num = getNumActivityArguments(expr);
        unsigned last = first + num;
        for (unsigned idx = first; idx < last; idx++)
            gatherPotentialSplitters(expr->queryChild(idx), sink, graph, false);
    }

    //MORE: dependencies should have onStart as their sink, and there should be a link between onStart and allSinks.
    //Now check dependencies between graphs (for roxie) and possibly within a graph for thor
    if (info->graph)
    {
        GraphLinkArray & graphLinks = info->graph->dependsOn;
        ForEachItemIn(i, graphLinks)
        {
            ResourceGraphLink & link = graphLinks.item(i);
            if (link.sinkNode == expr)
                gatherPotentialDependencySplitters(link.sourceNode, sink, graph);
        }
    }
    else
    {
        ForEachItemIn(i, resourcer.links)
        {
            ResourceGraphLink & link = resourcer.links.item(i);
            if (link.isDependency() && (link.sinkNode == expr))
            {
                gatherPotentialDependencySplitters(link.sourceNode, sink, graph);
            }
        }
    }
}

void CSplitterInfo::gatherPotentialDependencySplitters(IHqlExpression * expr, IHqlExpression * sink, ResourceGraphInfo * graph)
{
    //Strictly speaking dependencies are executed *before* the activity.
    //I experimented with making them dependent on a onStart pseudo expression, but that doesn't work because
    //there are multiple onStart items (as the dependencies nest).
    //There is still the outside possibility of deadlock between expressions and dependencies of items inside
    //non-ordered concats, but they would only occur in roxie, and this flag is currently ignored by roxie.
    gatherPotentialSplitters(expr, sink, graph, true);
}

void CSplitterInfo::restoreBalanced()
{
    ForEachItemIn(i, externalSources)
    {
        IHqlExpression & cur = externalSources.item(i);
        ResourcerInfo * info = queryResourceInfo(&cur);
        info->balanced = wasBalanced.item(i);
    }
}

IHqlExpression * EclResourcer::walkPotentialSplitters(CSplitterInfo & connections, IHqlExpression * expr, const CSplitterLink & link)
{
    ResourcerInfo * info = queryResourceInfo(expr);

    //If already visited all the links, then no need to do any more.
    if (info->finishedWalkingSplitters())
        return NULL;

    //Are we currently in the process of visiting this node?
    if (info->balancedVisiting)
    {
#ifdef TRACE_BALANCED
        printf("//Follow %u->%u has problems....\n", queryResourceInfo(link.queryOther(expr))->balanceId, info->balanceId);
#endif
        if (link.hasSource(expr))
        {
            //Walking up the tree, and the current node is being visited => it is a node that needs to become unbalanced - if it is a balanced splitter
            if (connections.isBalancedSplitter(expr))
            {
                const CSplitterLink * originalLink = info->queryCurrentLink();
                if (originalLink->hasSource(expr))
                    return expr->queryBody();
            }

            //Must be a node with multiple inputs, walked from one input, and visited another.
            return backtrackPseudoExpr;
        }
        else
        {
            //found a loop-> need to do something about it.
            return backtrackPseudoExpr;
        }
    }

#ifdef TRACE_BALANCED
    printf("//Follow %u->%u\n", queryResourceInfo(link.queryOther(expr))->balanceId, info->balanceId);
#endif
    return walkPotentialSplitterLinks(connections, expr, &link);
}

IHqlExpression * EclResourcer::walkPotentialSplitterLinks(CSplitterInfo & connections, IHqlExpression * expr, const CSplitterLink * link)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    info->balancedVisiting = true;

    //This may iterate through all links again - but will return quickly if already visited
    for (unsigned i=0; i < info->balancedLinks.ordinality(); i++)
    {
        const CSplitterLink & cur = info->balancedLinks.item(i);
        if (&cur != link) // don't walk the link we reached here by
        {
            if (info->balanced || cur.hasSink(expr))
            {
                info->curBalanceLink = i;
                IHqlExpression * problem = walkPotentialSplitters(connections, cur.queryOther(expr), cur);
                if (problem)
                {
                    bool forceUnbalanced = false;
                    if (problem == backtrackPseudoExpr)
                    {
                        //both links are to outputs
                        if (link && link->hasSource(expr) && cur.hasSource(expr))
                        {
                            assertex(connections.isBalancedSplitter(expr));
                            forceUnbalanced = true;
                        }
                    }
                    else
                        forceUnbalanced = (expr->queryBody() == problem);

                    if (!forceUnbalanced)
                    {
                        //No longer visiting - we'll come back here again later
                        info->balancedVisiting = false;
                        return problem;
                    }
#ifdef TRACE_BALANCED
                    printf("\tn%u [color=red];\n", info->balanceId);
                    printf("//%u marked as unbalanced\n", info->balanceId);
#endif
                    info->balanced = false;
                }
            }
        }
    }
    info->curBalanceLink = info->balancedLinks.ordinality();
    return NULL;
}

bool EclResourcer::removePassThrough(CSplitterInfo & connections, ResourcerInfo & info)
{
    if (info.balancedLinks.ordinality() != 2)
        return false;

    CSplitterLink & link0 = info.balancedLinks.item(0);
    CSplitterLink & link1 = info.balancedLinks.item(1);

    CSplitterLink * sourceLink;
    CSplitterLink * sinkLink;
    if (link0.hasSource(info.original) && link1.hasSink(info.original))
    {
        sourceLink = &link1;
        sinkLink = &link0;
    }
    else if (link0.hasSink(info.original) && link1.hasSource(info.original))
    {
        sourceLink = &link0;
        sinkLink = &link1;
    }
    else
        return false;

    if (!sinkLink->querySink())
        return false;

#ifdef TRACE_BALANCED
    printf("//remove node %u since now pass-through\n", info.balanceId);
#endif

    sourceLink->mergeSinkLink(*sinkLink);
    return true;
}

void EclResourcer::removeDuplicateIndependentLinks(CSplitterInfo & connections, ResourcerInfo & info)
{
    IHqlExpression * expr = info.original;
    loop
    {
        bool again = false;
        for (unsigned i=0; i < info.balancedLinks.ordinality(); i++)
        {
            CSplitterLink & cur = info.balancedLinks.item(i);
            if (cur.hasSource(expr))
            {
                IHqlExpression * sink = cur.queryOther(expr);
                assertex(sink);
                ResourcerInfo & sinkInfo = *queryResourceInfo(sink);
                if (CSplitterInfo::allInputsPulledIndependently(sink))
                {
                    unsigned numRemoved = 0;
                    for (unsigned j=info.balancedLinks.ordinality()-1; j > i; j--)
                    {
                        CSplitterLink & next = info.balancedLinks.item(j);
                        if (next.hasSource(expr) && next.hasSink(sink))
                        {
                            info.balancedLinks.remove(j);
                            sinkInfo.balancedLinks.zap(next);
                            numRemoved++;
                        }
                    }

#ifdef TRACE_BALANCED
                    if (numRemoved)
                        printf("//removed %u duplicate links from %u to %u\n", numRemoved, info.balanceId, sinkInfo.balanceId);
#endif

                }

                //Removing duplicate links has turned the source item into a pass-through.
                //Replace references to the sink activity with references to its sink
                //to possibly allow more to be removed.
                if (removePassThrough(connections, sinkInfo))
                {
#ifdef TRACE_BALANCED
                    printf("//remove %u now pass-through\n", sinkInfo.balanceId);
#endif
                    again = true;
                }
            }
        }
        if (!again)
            break;
    }
}


void EclResourcer::optimizeIndependentLinks(CSplitterInfo & connections, ResourcerInfo & info)
{
    if (info.removedParallelPullers)
        return;
    info.removedParallelPullers = true;

    removeDuplicateIndependentLinks(connections, info);

    //Recurse over inputs to this activity (each call may remove links)
    for (unsigned i=0; i < info.balancedLinks.ordinality(); i++)
    {
        CSplitterLink & cur = info.balancedLinks.item(i);
        if (cur.hasSink(info.original))
            optimizeIndependentLinks(connections, *queryResourceInfo(cur.querySource()));
    }
}


void EclResourcer::optimizeConditionalLinks(CSplitterInfo & connections)
{
    //MORE: IF() can be special cased.  If it creates two identical links then one of them can be removed
    //Implement by post processing the links and removing duplicates
    ForEachItemIn(i, connections.sinks)
    {
        IHqlExpression & cur = connections.sinks.item(i);
        ResourcerInfo * info = queryResourceInfo(&cur);
        optimizeIndependentLinks(connections, *info);
    }
}

void EclResourcer::walkPotentialSplitters(CSplitterInfo & connections)
{
    ForEachItemIn(i, connections.sinks)
    {
        IHqlExpression & cur = connections.sinks.item(i);
        ResourcerInfo * info = queryResourceInfo(&cur);
        if (!info->finishedWalkingSplitters())
        {
            IHqlExpression * problem = walkPotentialSplitterLinks(connections, &cur, NULL);
            assertex(!problem);
        }
    }
}

void EclResourcer::extractSharedInputs(CSplitterInfo & connections, ResourceGraphInfo & graph)
{
    ForEachItemIn(i, connections.externalSources)
    {
        IHqlExpression & cur = connections.externalSources.item(i);
        ResourcerInfo * info = queryResourceInfo(&cur);
        if (connections.isBalancedSplitter(&cur))
        {
            //Add two entries for compatibility with old code.
            graph.balancedExternalSources.append(*LINK(cur.queryBody()));
            graph.balancedExternalSources.append(*LINK(cur.queryBody()));
        }
    }
}

void EclResourcer::spotUnbalancedSplitters(const HqlExprArray & exprs)
{
    switch (targetClusterType)
    {
    case HThorCluster:
        break;
    case ThorLCRCluster:
        {
            //Thor only handles one graph at a time, so only walk expressions within a single graph.
            ForEachItemIn(i1, graphs)
            {
                ResourceGraphInfo & curGraph = graphs.item(i1);
                CSplitterInfo info(*this, true, true);
                nextPass();
                ForEachItemIn(i2, curGraph.sinks)
                {
                    ResourceGraphLink & cur = curGraph.sinks.item(i2);
                    info.gatherPotentialSplitters(cur.sourceNode, NULL, &curGraph, false);
                }

                optimizeConditionalLinks(info);
                walkPotentialSplitters(info);
                extractSharedInputs(info, curGraph);
            }
        }
        break;
    case RoxieCluster:
        {
            //Roxie pulls all at once, so need to analyse globally.
            CSplitterInfo info(*this, false, false);
            nextPass();
            ForEachItemIn(i2, exprs)
                info.gatherPotentialSplitters(&exprs.item(i2), NULL, NULL, false);

            optimizeConditionalLinks(info);
            walkPotentialSplitters(info);
            //no splitters from reading at start of a subgraph
            break;
        }
    }
}
//------------------------------------------------------------------------------------------
// PASS6: Merge sub graphs that can share resources and don't have dependencies
// MORE: Once sources are merged, should try merging between trees.

static bool conditionsMatch(const HqlExprArray & left, const HqlExprArray & right)
{
    if (left.ordinality() != right.ordinality())
        return false;

    ForEachItemIn(i, left)
    {
        if (!left.contains(right.item(i)) || !right.contains(left.item(i)))
            return false;
    }
    return true;
}


bool EclResourcer::queryMergeGraphLink(ResourceGraphLink & link)
{
    if (link.linkKind == UnconditionalLink)
    {
        //Don't combine any dependencies 
        const GraphLinkArray & sinks = link.sourceGraph->sinks;
        ForEachItemIn(i1, sinks)
        {
            ResourceGraphLink & cur = sinks.item(i1);
            if (cur.sinkGraph && cur.sourceNode->isAction())
                return false;
        }

        //Roxie pulls all subgraphs at same time, so no problem with conditional links since handled at run time.
        if (options.noConditionalLinks)
            return true;

        //No conditionals in the sink graph=>will be executed just as frequently
        if (!link.sinkGraph->mergedConditionSource)
            return true;

        //Is this the only place this source graph is used?  If so, always fine to merge
        if (sinks.ordinality() == 1)
            return true;

        //1) if context the source graph is being merged into is unconditional, then it is ok [ could have conditional and unconditional paths to same graph]
        //2) if context is conditional, then we don't really want to do it unless the conditions on all sinks are identical, and the only links occur between these two graphs.
        //   (situation occurs with spill fed into two branches of a join).
        bool isConditionalInSinkGraph = false;
        bool accessedFromManyGraphs = false;
        ForEachItemIn(i, sinks)
        {
            ResourceGraphLink & cur = sinks.item(i);
            if (cur.sinkNode)
            {
                if (cur.sinkGraph != link.sinkGraph)
                    accessedFromManyGraphs = true;
                else
                {
                    if (!isConditionalInSinkGraph)
                    {
                        ResourcerInfo * sinkInfo = queryResourceInfo(cur.sinkNode);

                        //If this is conditional, don't merge if there is a link to another graph
                        if ((!cur.isDependency() && sinkInfo->isConditionExpr()) ||
                        //if (sinkInfo->isConditionExpr() ||
                            (!sinkInfo->isUnconditional() && sinkInfo->conditions.ordinality()))
                            isConditionalInSinkGraph = true;
                    }
                }
            }
        }

        if (isConditionalInSinkGraph && accessedFromManyGraphs)
            return false;

        return true;
    }

    return false;
}


bool EclResourcer::checkAlreadyVisited(ResourcerInfo * info)
{
    if (info->lastPass == thisPass)
        return true;
    info->lastPass = thisPass;
    return false;
}

unsigned EclResourcer::getMaxDepth() const
{
    unsigned maxDepth = 0;
    for (unsigned idx = 0; idx < graphs.ordinality(); idx++)
    {
        unsigned depth = graphs.item(idx).getDepth();
        if (depth > maxDepth)
            maxDepth = depth;
    }
    return maxDepth;
}


void EclResourcer::mergeSubGraphs(unsigned pass)
{
    unsigned maxDepth = getMaxDepth();
    for (unsigned curDepth = maxDepth+1; curDepth-- != 0;)
    {
mergeAgain:
        for (unsigned idx = 0; idx < graphs.ordinality(); idx++)
        {
            ResourceGraphInfo & cur = graphs.item(idx);
            if ((cur.getDepth() == curDepth) && !cur.isDead)
            {
                bool tryAgain;
                do
                {
                    tryAgain = false;
                    for (unsigned idxSource = 0; idxSource < cur.sources.ordinality(); /*incremented in loop*/)
                    {
                        ResourceGraphLink & curLink = cur.sources.item(idxSource);
                        ResourceGraphInfo * source = curLink.sourceGraph;
                        IHqlExpression * sourceNode = curLink.sourceNode;
                        bool tryToMerge;
                        bool expandSourceInPlace = queryResourceInfo(sourceNode)->expandRatherThanSpill(false);
                        if (pass == 0)
                            tryToMerge = !expandSourceInPlace;
                        else
                            tryToMerge = expandSourceInPlace;

                        if (tryToMerge)
                        {
                            bool ok = true;
                            bool mergeInSpillOutput = false;
                            ResourcerInfo * sourceResourceInfo = queryResourceInfo(sourceNode);
                            IHqlExpression * sourceSpillOutput = sourceResourceInfo->queryOutputSpillFile();
                            if (sourceSpillOutput)
                            {
                                if (targetClusterType == HThorCluster)
                                {
                                    if (curLink.sinkNode != sourceSpillOutput)
                                        ok = false;
                                }
                                //If a dataset is being spilled to be read from a child expression etc. it is going to
                                //be more efficient to use that spilled expression => try and force it to merge
                                if (curLink.sinkNode == sourceSpillOutput)
                                    mergeInSpillOutput = true;
                            }
                            if (sequential && source->containsActionSink())
                                ok = false;

                            unsigned curSourceDepth = source->getDepth();
                            //MORE: Merging identical conditionals?
                            if (ok && queryMergeGraphLink(curLink) &&
                                !sourceResourceInfo->expandRatherThanSplit() &&
                                cur.mergeInSource(*source, *resourceLimit, mergeInSpillOutput))
                            {
                                //NB: Following cannot remove sources below the current index.
                                replaceGraphReferences(source, &cur);
                                source->isDead = true;
#ifdef VERIFY_RESOURCING
                                checkRecursion(&cur);
#endif
                                unsigned newDepth = cur.getDepth();
                                //Unusual: The source we are merging with has just increased in depth, so any
                                //dependents have also increased in depth.  Need to try again at different depth
                                //to see if one of those will merge in.
                                if (newDepth > curSourceDepth)
                                {
                                    curDepth += (newDepth - curSourceDepth);
                                    goto mergeAgain;
                                }
                                //depth of this element has changed, so don't check to see if it merges with any other
                                //sources on this iteration.
                                if (newDepth != curDepth)
                                {
                                    tryAgain = false;
                                    break;
                                }
                                tryAgain = true;
                            }
                            else
                                idxSource++;
                        }
                        else
                            idxSource++;
                    }
                } while (tryAgain);
            }
        }
    }
}

void EclResourcer::mergeSiblings()
{
    unsigned maxDepth = getMaxDepth();
    for (unsigned curDepth = maxDepth+1; curDepth-- != 0;)
    {
        for (unsigned idx = 0; idx < graphs.ordinality(); idx++)
        {
            ResourceGraphInfo & cur = graphs.item(idx);
            if ((cur.getDepth() == curDepth) && !cur.isDead)
            {
                if (cur.containsActionSink())
                {
                    if (!cur.canMergeActionAsSibling(sequential))
                        continue;
                }

                ForEachItemIn(idxSource, cur.sources)
                {
                    ResourceGraphLink & curLink = cur.sources.item(idxSource);
                    ResourceGraphInfo * source = curLink.sourceGraph;
                    IHqlExpression * sourceNode = curLink.sourceNode;
                    ResourcerInfo * sourceInfo = queryResourceInfo(sourceNode);
                    if (sourceInfo->neverSplit || sourceInfo->expandRatherThanSplit())
                        continue;

                    for (unsigned iSink = 0; iSink < source->sinks.ordinality(); )
                    {
                        ResourceGraphLink & secondLink = source->sinks.item(iSink);
                        ResourceGraphInfo * sink = secondLink.sinkGraph;
                        bool ok = false;
                        if (sink && (sink != &cur) && !sink->isDead && sourceNode->queryBody() == secondLink.sourceNode->queryBody())
                        {
                            ok = true;
                            if (sequential && !sink->canMergeActionAsSibling(sequential))
                                ok = false;

                            if (ok && cur.mergeInSibling(*sink, *resourceLimit))
                            {
                                //NB: Following cannot remove sources below the current index.
                                replaceGraphReferences(sink, &cur);
                                sink->isDead = true;
                            }
                            else
                                iSink++;
                        }
                        else
                           iSink++;
                    }
                }
            }
        }
    }
}

void EclResourcer::mergeSubGraphs()
{
    for (unsigned pass=0; pass < 2; pass++)
        mergeSubGraphs(pass);

    if (options.combineSiblings)
        mergeSiblings();

    ForEachItemInRev(idx2, graphs)
    {
        if (graphs.item(idx2).isDead)
            graphs.remove(idx2);
    }

    //If there is a link setting info->queryOutputSpillFile() which hasn't been merged, then need to clear
    //otherwise you will get an internal error
    ForEachItemIn(iLink, links)
    {
        ResourceGraphLink & curLink = links.item(iLink);
        ResourcerInfo * sourceInfo = queryResourceInfo(curLink.sourceNode);
        IHqlExpression * outputFile = sourceInfo->queryOutputSpillFile();
        if (outputFile && outputFile == curLink.sinkNode)
            sourceInfo->setPotentialSpillFile(NULL);
    }
}

//------------------------------------------------------------------------------------------
// PASS7: Optimize aggregates off of splitters into through aggregates.

bool EclResourcer::optimizeAggregate(IHqlExpression * expr)
{
    if (!isSimpleAggregateResult(expr))
        return false;

    IHqlExpression * row2ds = expr->queryChild(0);          // no_datasetfromrow
    IHqlExpression * selectNth = row2ds->queryChild(0);

    ResourcerInfo * row2dsInfo = queryResourceInfo(row2ds);

    //If more than one set result for the same aggregate don't merge the aggregation because
    //it messes up the internal count.  Should really fix it and do multiple stores in the
    //through aggregate.
    if (row2dsInfo->numInternalUses() > 1)
        return false;

    //Be careful not to lose any spills...
    if (row2dsInfo->numExternalUses)
        return false;

    ResourcerInfo * selectNthInfo = queryResourceInfo(selectNth);
    if (!selectNthInfo || selectNthInfo->numExternalUses)
        return false;

    IHqlExpression * aggregate = selectNth->queryChild(0);      // no_newaggregate
    IHqlExpression * parent = aggregate->queryChild(0);
    ResourcerInfo * info = queryResourceInfo(parent);
    if (info->numInternalUses() <= 1)
        return false;

    //ok, we can go ahead and merge.
    info->aggregates.append(*LINK(expr));
//  info->numExternalUses--;
//  info->numUses--;
    return true;
}


void EclResourcer::optimizeAggregates()
{
    for (unsigned idx = 0; idx < graphs.ordinality(); idx++)
    {
        ResourceGraphInfo & cur = graphs.item(idx);
        for (unsigned idxSink = 0; idxSink < cur.sinks.ordinality(); /*incremented in loop*/)
        {
            ResourceGraphLink & link = cur.sinks.item(idxSink);
            if ((link.sinkGraph == NULL) && optimizeAggregate(link.sourceNode))
                cur.sinks.remove(idxSink);
            else
                idxSink++;
        }
    }
}

//------------------------------------------------------------------------------------------
// PASS8: Improve efficiency by merging the split points slightly

IHqlExpression * EclResourcer::findPredecessor(IHqlExpression * expr, IHqlExpression * search, IHqlExpression * prev)
{
    if (expr == search)
        return prev;

    ResourcerInfo * info = queryResourceInfo(expr);
    if (info && info->containsActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        for (unsigned idx=first; idx < last; idx++)
        {
            IHqlExpression * match = findPredecessor(expr->queryChild(idx), search, expr);
            if (match)
                return match;
        }
    }
    return NULL;
}

IHqlExpression * EclResourcer::findPredecessor(ResourcerInfo * search)
{
    ForEachItemIn(idx, links)
    {
        ResourceGraphLink & cur = links.item(idx);

        if (cur.sourceGraph == search->graph)
        {
            IHqlExpression * match = findPredecessor(cur.sourceNode, search->original, NULL);
            if (match)
                return match;
        }
    }
    return NULL;
}

void EclResourcer::moveExternalSpillPoints()
{
    if (options.minimizeSpillSize == 0)
        return;

    //if we have a external spill point where all the external outputs reduce their data significantly
    //either via a project or a filter, then it might be worth including those activities in the main
    //graph and ,if all external children reduce data, then may be best to filter 
    ForEachItemIn(idx, links)
    {
        ResourceGraphLink & cur = links.item(idx);
        if ((cur.linkKind == UnconditionalLink) && cur.sinkGraph)
        {
            while (lightweightAndReducesDatasetSize(cur.sinkNode))
            {
                ResourcerInfo * sourceInfo = queryResourceInfo(cur.sourceNode);
                if (!sourceInfo->isExternalSpill() || (sourceInfo->numExternalUses > options.minimizeSpillSize))
                    break;

                ResourcerInfo * sinkInfo = queryResourceInfo(cur.sinkNode);
                if (sinkInfo->numInternalUses() != 1)
                    break;

                IHqlExpression * sinkPred = findPredecessor(sinkInfo);
                sinkInfo->graph.set(cur.sourceGraph);
                sourceInfo->numExternalUses--;
                sinkInfo->numExternalUses++;
                cur.sourceNode.set(cur.sinkNode);
                cur.sinkNode.set(sinkPred);
            }
        }
    }
}


//------------------------------------------------------------------------------------------
// PASS9: Create a new expression tree representing the information

static IHqlExpression * getScalarReplacement(CChildDependent & cur, ResourcerInfo * hoistedInfo, IHqlExpression * replacement)
{
    //First skip any wrappers which are there to cause things to be hoisted.
    IHqlExpression * value = skipScalarWrappers(cur.original);

    //Now modify the spilled result depending on how the spilled result was created (see EclHoistLocator::noteScalar() above)
    if (value->getOperator() == no_select)
    {
        bool isNew;
        IHqlExpression * ds = querySelectorDataset(value, isNew);
        if(isNew || ds->isDatarow())
        {
            if (cur.hoisted != cur.projectedHoisted)
            {
                assertex(cur.projected);
                unsigned match = hoistedInfo->projected.find(*cur.projected);
                assertex(match != NotFound);
                IHqlExpression * projectedRecord = cur.projectedHoisted->queryRecord();
                IHqlExpression * projectedField = projectedRecord->queryChild(match);
                return createNewSelectExpr(LINK(replacement), LINK(projectedField));
            }
            return replaceSelectorDataset(value, replacement);
        }
        //Very unusual - can occur when a thisnode(somefield) is extracted from an allnodes.
        //It will have gone through the default case in the noteScalar() code
    }
    else if (value->getOperator() == no_createset)
    {
        IHqlExpression * record = replacement->queryRecord();
        IHqlExpression * field = record->queryChild(0);
        return createValue(no_createset, cur.original->getType(), LINK(replacement), createSelectExpr(LINK(replacement->queryNormalizedSelector()), LINK(field)));
    }

    IHqlExpression * record = replacement->queryRecord();
    return createNewSelectExpr(LINK(replacement), LINK(record->queryChild(0)));
}


IHqlExpression * EclResourcer::doCreateResourced(IHqlExpression * expr, ResourceGraphInfo * ownerGraph, bool expandInParent, bool defineSideEffect)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    node_operator op = expr->getOperator();
    HqlExprArray args;
    bool same = true;

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    OwnedHqlExpr transformed;
    switch (op)
    {
    case no_if:
    case no_choose:
    case no_chooseds:
        {
            ForEachChild(idx, expr)
            {
                IHqlExpression * child = expr->queryChild(idx);
                IHqlExpression * resourced;
                if ((idx < first) || (idx >= last))
                    resourced = LINK(child);
                else
                    resourced = createResourced(child, ownerGraph, expandInParent, false);
                if (child != resourced) 
                    same = false;
                args.append(*resourced);
            }
            break;
        }
    case no_case:
    case no_map:
        UNIMPLEMENTED;
    case no_keyed:
        return LINK(expr);
    case no_compound:
        if (options.convertCompoundToExecuteWhen)
        {
            //NB: Arguments to no_executewhen are the reverse of no_compound.
            args.append(*createResourced(expr->queryChild(1), ownerGraph, expandInParent, false));
            args.append(*createResourced(expr->queryChild(0), ownerGraph, expandInParent, false));
            transformed.setown(createDataset(no_executewhen, args));
        }
        else
            transformed.setown(createResourced(expr->queryChild(1), ownerGraph, expandInParent, false));
        break;
    case no_executewhen:
        {
            args.append(*createResourced(expr->queryChild(0), ownerGraph, expandInParent, false));
            args.append(*createResourced(expr->queryChild(1), ownerGraph, expandInParent, false));
            assertex(args.item(1).getOperator() == no_callsideeffect);
            unwindChildren(args, expr, 2);
            same = false;
            break;
        }
    case no_select:
        {
            //If this isn't a new selector, then it must be <LEFT|RIGHT>.child-dataset, which will not be mapped
            //and the dataset will not have been resourced
            if (isNewSelector(expr))
            {
                IHqlExpression * ds = expr->queryChild(0);
                OwnedHqlExpr newDs = createResourced(ds, ownerGraph, expandInParent, false);

                if (ds != newDs)
                {
                    args.append(*LINK(newDs));
                    unwindChildren(args, expr, 1);
                    if (!expr->hasAttribute(newAtom) && (newDs->getOperator() != no_select))
                        args.append(*LINK(queryNewSelectAttrExpr()));
                    same = false;
                }
            }
            break;
        }
    default:
        {
            IHqlExpression * activeTable = NULL;
            // Check to see if the activity has a dataset which is in scope for the rest of its arguments.
            // If so we'll need to remap references from the children.
            if (hasActiveTopDataset(expr) && (first != last))
                activeTable = expr->queryChild(0);

            ForEachChild(idx, expr)
            {
                IHqlExpression * child = expr->queryChild(idx);
                IHqlExpression * resourced;
                if ((idx < first) || (idx >= last))
                {
                    LinkedHqlExpr mapped = child;
                    if (activeTable && isAffectedByResourcing(child))
                    {
                        IHqlExpression * activeTableTransformed = &args.item(0);
                        if (activeTable != activeTableTransformed)
                            mapped.setown(scopedReplaceSelector(child, activeTable, activeTableTransformed));
                    }
                    resourced = mapped.getClear();
                }
                else
                    resourced = createResourced(child, ownerGraph, expandInParent, false);
                if (child != resourced) 
                    same = false;
                args.append(*resourced);
            }
        }
        break;
    }

    if (!transformed)
        transformed.setown(same ? LINK(expr) : expr->clone(args));

    if (!expandInParent)
    {
        if (!transformed->isAction())
            transformed.setown(info->createTransformedExpr(transformed));
        else if (defineSideEffect)
            transformed.setown(createValue(no_definesideeffect, LINK(transformed), createUniqueId()));
    }

    return transformed.getClear();
}

/*
 Need to be careful because result should not reuse the same expression tree unless that element is a splitter.

createResourced()
{
    if (!isActivity)
        if (!containsActivity)
            replace any refs to the activeTable with whatever it has been mapped to
        else
            recurse
    else if (!isDefinedInSameGraph)
        expand/create reader
        set active table
    else isSplitter and alreadyGeneratedForThisGraph
        return previous result
    else
        create transformed  
}
  */

void EclResourcer::doCheckRecursion(ResourceGraphInfo * graph, PointerArray & visited)
{
    visited.append(graph);
    ForEachItemIn(idxD, graph->dependsOn)
        checkRecursion(graph->dependsOn.item(idxD).sourceGraph, visited);
    ForEachItemIn(idxS, graph->sources)
        checkRecursion(graph->sources.item(idxS).sourceGraph, visited);
    visited.pop();
}

void EclResourcer::checkRecursion(ResourceGraphInfo * graph, PointerArray & visited)
{
    if (visited.find(graph) != NotFound)
        throwUnexpected();
    doCheckRecursion(graph, visited);
}

void EclResourcer::checkRecursion(ResourceGraphInfo * graph)
{
    PointerArray visited;
    doCheckRecursion(graph, visited);
}

IHqlExpression * EclResourcer::createResourced(IHqlExpression * expr, ResourceGraphInfo * ownerGraph, bool expandInParent, bool defineSideEffect)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return LINK(expr);

    if (!info->isActivity)
    {
        assertex(!defineSideEffect);
        HqlExprArray args;
        bool same = true;
        ForEachChild(idx, expr)
        {
            IHqlExpression * cur = expr->queryChild(idx);
            IHqlExpression * curResourced = createResourced(cur, ownerGraph, expandInParent, false);
            args.append(*curResourced);
            if (cur != curResourced)
                same = false;
        }
        if (same)
            return LINK(expr);
        return expr->clone(args);
    }

    if (info->graph != ownerGraph)
    {
        assertex(!defineSideEffect);
        bool isShared = options.optimizeSharedInputs && ownerGraph && ownerGraph->isSharedInput(expr);
        if (isShared)
        {
            IHqlExpression * mapped = ownerGraph->queryMappedSharedInput(expr->queryBody());
            if (mapped)
                return LINK(mapped);
        }

        IHqlExpression * source;
        if (info->expandRatherThanSpill(true))
        {
            bool expandChildInParent = options.minimiseSpills ? expandInParent : true;
            OwnedHqlExpr resourced = doCreateResourced(expr, ownerGraph, expandChildInParent, false);
            if (queryAddUniqueToActivity(resourced))
                source = appendUniqueAttr(resourced);
            else
                source = LINK(resourced);
        }
        else
        {
            if (!expr->isAction())
            {
                OwnedHqlExpr reason;
                if (ownerGraph && options.checkResources())
                {
                    StringBuffer reasonText;
                    ownerGraph->getMergeFailReason(reasonText, info->graph, *resourceLimit);
                    if (reasonText.length())
                    {
                        reasonText.insert(0, "Resource limit spill: ");
                        reason.setown(createAttribute(_spillReason_Atom, createConstant(reasonText.str())));
                    }
                }

                source = info->createSpilledRead(reason);
            }
            else
            {
                IHqlExpression * transformed = info->queryTransformed();
                if (transformed->getOperator() == no_definesideeffect)
                {
                    IHqlExpression * uid = transformed->queryAttribute(_uid_Atom);
                    assertex(uid);
                    source = createValue(no_callsideeffect, makeVoidType(), LINK(uid));
                }
                else
                    source = LINK(transformed);
            }
        }

        if (isShared)
        {
            if (!source->isAction())
            {
                if (source->isDataset())
                    source = createDatasetF(no_split, source, createAttribute(balancedAtom), createUniqueId(), NULL);
                else
                    source = createRowF(no_split, source, createAttribute(balancedAtom), createUniqueId(), NULL);

                ownerGraph->addSharedInput(expr->queryBody(), source);
            }
        }

        return source;
    }

    if (!expandInParent && info->queryTransformed() && info->isSplit())
    {
        return LINK(info->queryTransformed());
    }

    OwnedHqlExpr resourced = doCreateResourced(expr, ownerGraph, expandInParent, defineSideEffect);
    if (queryAddUniqueToActivity(resourced))// && !resourced->hasAttribute(_internal_Atom))
        resourced.setown(appendUniqueAttr(resourced));

    if (!expandInParent)
    {
        info->setTransformed(resourced);
    }
    return resourced.getClear();
}

void EclResourcer::createResourced(ResourceGraphInfo * graph, HqlExprArray & transformed)
{
    if (graph->createdGraph || graph->isDead)
        return;
    if (!graph->containsActiveSinks() && (!graph->hasConditionSource))
        return;

#ifdef VERIFY_RESOURCING
    checkRecursion(graph);
#endif
//  DBGLOG("Prepare to CreateResourced(%p)", graph);
    if (graph->startedGeneratingResourced)
        throwError(HQLWRN_RecursiveDependendencies);

    graph->startedGeneratingResourced = true;
    ForEachItemIn(idxD, graph->dependsOn)
        createResourced(graph->dependsOn.item(idxD).sourceGraph, transformed);
    ForEachItemIn(idxS, graph->sources)
        createResourced(graph->sources.item(idxS).sourceGraph, transformed);

//  DBGLOG("Create resourced %p", graph);

    //First generate the graphs for all the unconditional sinks
    HqlExprArray args;
    ForEachItemIn(idx, graph->sinks)
    {
        ResourceGraphLink & sink = graph->sinks.item(idx);
        IHqlExpression * sinkNode = sink.sourceNode;
        ResourcerInfo * info = queryResourceInfo(sinkNode);
        //If graph is unconditional, any condition sinks are forced to be generated (and spilt)
        if (!info->queryTransformed())
        {
            // if it is a spiller, then it will be generated from another sink
            if (!info->isExternalSpill())
            {
                IHqlExpression * resourced = createResourced(sinkNode, graph, false, sinkNode->isAction() && sink.sinkGraph);
                assertex(info->queryTransformed());
                args.append(*resourced);
            }
        }
    }

    ForEachItemIn(i2, graph->sinks)
    {
        ResourceGraphLink & sink = graph->sinks.item(i2);
        IHqlExpression * sinkNode = sink.sourceNode;
        ResourcerInfo * info = queryResourceInfo(sinkNode);
        IHqlExpression * splitter = info->splitterOutput;
        if (splitter && !args.contains(*splitter))
            args.append(*LINK(splitter));
    }

    if (args.ordinality() == 0)
        graph->isDead = true;
    else
    {
        if (options.useGraphResults || options.alwaysUseGraphResults)
            args.append(*createAttribute(childAtom));
        graph->createdGraph.setown(createValue(no_subgraph, makeVoidType(), args));
        transformed.append(*LINK(graph->createdGraph));
    }
}


void EclResourcer::inheritRedundantDependencies(ResourceGraphInfo * thisGraph)
{
    if (thisGraph->inheritedExpandedDependencies)
        return;
    thisGraph->inheritedExpandedDependencies = true;
    ForEachItemIn(idx, thisGraph->sources)
    {
        ResourceGraphLink & cur = thisGraph->sources.item(idx);
        if (cur.isRedundantLink())
        {
            inheritRedundantDependencies(cur.sourceGraph);
            ForEachItemIn(i, cur.sourceGraph->dependsOn)
            {
                ResourceGraphLink & curDepend = cur.sourceGraph->dependsOn.item(i);
                ResourceGraphLink * link = new ResourceGraphDependencyLink(curDepend.sourceGraph, curDepend.sourceNode, thisGraph, cur.sinkNode, cur.queryDependency());
                thisGraph->dependsOn.append(*link);
                links.append(*link);
            }
        }
    }
}


void EclResourcer::createResourced(HqlExprArray & transformed)
{
    //Before removing null links (e.g., where the source graph is expanded inline), need to make sure
    //dependencies are cloned, otherwise graphs can be generated in the wrong order
    ForEachItemIn(idx1, graphs)
        inheritRedundantDependencies(&graphs.item(idx1));

    ForEachItemInRev(idx2, links)
    {
        ResourceGraphLink & cur = links.item(idx2);
        if (cur.isRedundantLink())
            removeLink(cur, true);
    }

    ForEachItemIn(idx3, graphs)
        createResourced(&graphs.item(idx3), transformed);
}

static int compareGraphDepth(CInterface * const * _l, CInterface * const * _r)
{
    ResourceGraphInfo * l = (ResourceGraphInfo *)*_l;
    ResourceGraphInfo * r = (ResourceGraphInfo *)*_r;
    return l->getDepth() - r->getDepth();
}

static int compareLinkDepth(CInterface * const * _l, CInterface * const * _r)
{
    ResourceGraphLink * l = (ResourceGraphLink *)*_l;
    ResourceGraphLink * r = (ResourceGraphLink *)*_r;
    int diff = l->sourceGraph->getDepth() - r->sourceGraph->getDepth();
    if (diff) return diff;
    if (l->sinkGraph)
        if (r->sinkGraph)
            return l->sinkGraph->getDepth() - r->sinkGraph->getDepth();
        else
            return -1;
    else
        if (r->sinkGraph)
            return +1;
        else
            return 0;
}

void EclResourcer::display(StringBuffer & out)
{
    CIArrayOf<ResourceGraphInfo> sortedGraphs;
    ForEachItemIn(j1, graphs)
        sortedGraphs.append(OLINK(graphs.item(j1)));
    sortedGraphs.sort(compareGraphDepth);

    out.append("Graphs:\n");
    ForEachItemIn(i, sortedGraphs)
    {
        ResourceGraphInfo & cur = sortedGraphs.item(i);
        out.appendf("%d: depth(%d) uncond(%d) cond(%d) %s {%p}\n", i, cur.getDepth(), cur.isUnconditional, cur.hasConditionSource, cur.isDead ? "dead" : "", &cur);
        ForEachItemIn(j, cur.sources)
        {
            ResourceGraphLink & link = cur.sources.item(j);
            out.appendf("  Source: %p %s\n", link.sinkNode.get(), getOpString(link.sinkNode->getOperator()));
        }
        ForEachItemIn(k, cur.sinks)
        {
            ResourceGraphLink & link = cur.sinks.item(k);
            IHqlExpression * sourceNode = link.sourceNode;
            ResourcerInfo * sourceInfo = queryResourceInfo(sourceNode);
            out.appendf("  Sink: %p %s cond(%d,%d) int(%d) ext(%d)\n", sourceNode, getOpString(sourceNode->getOperator()), sourceInfo->conditions.ordinality(), sourceInfo->conditionSourceCount, sourceInfo->numInternalUses(), sourceInfo->numExternalUses);
        }
        ForEachItemIn(i3, links)
        {
            ResourceGraphLink & curLink = links.item(i3);
            if ((curLink.sourceGraph == &cur) && curLink.queryDependency())
            {
                StringBuffer s;
                toECL(curLink.queryDependency(), s);
                out.appendf("  Creates: %s\n", s.str());
            }
        }
    }
    out.append("Links:\n");

    CIArrayOf<ResourceGraphLink> sortedLinks;
    ForEachItemIn(j2, links)
        sortedLinks.append(OLINK(links.item(j2)));
    sortedLinks.sort(compareLinkDepth);
    ForEachItemIn(i2, sortedLinks)
    {
        ResourceGraphLink & link = sortedLinks.item(i2);
        unsigned len = out.length();
        out.appendf("  Source: %d %s", sortedGraphs.find(*link.sourceGraph), getOpString(link.sourceNode->getOperator()));
        if (link.sinkNode)
        {
            out.padTo(len+30);
            out.appendf("  Sink: %d %s", sortedGraphs.find(*link.sinkGraph), getOpString(link.sinkNode->getOperator()));
        }
        if (link.linkKind == SequenceLink)
            out.append(" <sequence>");
        if (link.isDependency())
            out.append(" <dependency>");
        out.newline();
    }
}

void EclResourcer::trace()
{
    StringBuffer s;
    display(s);
    DBGLOG("%s", s.str());
}

//---------------------------------------------------------------------------

void EclResourcer::resourceGraph(IHqlExpression * expr, HqlExprArray & transformed)
{
    if (isSequentialActionList(expr))
        setSequential(true);

    HqlExprArray exprs;
    node_operator expandOp = options.isChildQuery ? no_any : no_parallel;
    expandLists(expandOp, exprs, expr);

    //NB: This only resources a single level of queries.  SubQueries should be resourced in a separate
    //pass so that commonality between different activities/subgraphs isn't introduced/messed up.
    findSplitPoints(exprs);
    createInitialGraphs(exprs);
    addDependencies(exprs);
    markConditions(exprs);
    if (options.checkResources())
        resourceSubGraphs(exprs);
#ifdef TRACE_RESOURCING
    trace();
#endif
    mergeSubGraphs();
#ifdef TRACE_RESOURCING
    trace();
#endif

    if (!options.newBalancedSpotter)
    {
        oldSpotUnbalancedSplitters(exprs);
        if (options.optimizeSharedInputs)
            spotSharedInputs();
    }
    else
        spotUnbalancedSplitters(exprs);

    if (options.spotThroughAggregate)
        optimizeAggregates();
    moveExternalSpillPoints();
    createResourced(transformed);
}


void EclResourcer::resourceRemoteGraph(IHqlExpression * expr, HqlExprArray & transformed)
{
    HqlExprArray exprs;
    expandLists(no_any, exprs, expr);

    //NB: This only resources a single level of queries.  SubQueries should be resourced in a separate
    //pass so that commonality between different activities/subgraphs isn't introduced/messed up.
    findSplitPoints(exprs);
    createInitialRemoteGraphs(exprs);
    markConditions(exprs);
    addDependencies(exprs);
#ifdef TRACE_RESOURCING
    trace();
#endif
    mergeSubGraphs();
#ifdef TRACE_RESOURCING
    trace();
#endif
    createResourced(transformed);
}


//---------------------------------------------------------------------------

IHqlExpression * resourceThorGraph(HqlCppTranslator & translator, IHqlExpression * _expr, ClusterType targetClusterType, unsigned clusterSize, IHqlExpression * graphIdExpr)
{
    CResourceOptions options(targetClusterType, clusterSize, translator.queryOptions(), translator.querySpillSequence());
    if (graphIdExpr)
        options.setNewChildQuery(graphIdExpr, 0);

    LinkedHqlExpr expr = _expr;
    {
        ActivityInvariantHoister hoister(options);
        HqlExprArray hoisted;
        expr.setown(hoister.transformRoot(expr));
        translator.traceExpression("AfterInvariant Child", expr);
    }

    HqlExprArray transformed;
    {
        EclResourcer resourcer(translator.queryErrorProcessor(), translator.wu(), translator.queryOptions(), options);
        resourcer.resourceGraph(expr, transformed);
    }
    hoistNestedCompound(translator, transformed);
    return createActionList(transformed);
}


static IHqlExpression * doResourceGraph(HqlCppTranslator & translator, HqlExprCopyArray * activeRows, IHqlExpression * _expr,
                                        ClusterType targetClusterType, unsigned clusterSize,
                                        IHqlExpression * graphIdExpr, unsigned numResults, bool isChild, bool useGraphResults)
{
    LinkedHqlExpr expr = _expr;
    HqlExprArray transformed;
    unsigned totalResults;
    CResourceOptions options(targetClusterType, clusterSize, translator.queryOptions(), translator.querySpillSequence());
    if (isChild)
        options.setChildQuery(true);
    options.setNewChildQuery(graphIdExpr, numResults);
    options.setUseGraphResults(useGraphResults);

    {
        ActivityInvariantHoister hoister(options);
        hoister.tagActiveCursors(activeRows);
        HqlExprArray hoisted;
        expr.setown(hoister.transformRoot(expr));
        translator.traceExpression("AfterInvariant Child", expr);
    }

    {
        EclResourcer resourcer(translator.queryErrorProcessor(), translator.wu(), translator.queryOptions(), options);
        resourcer.tagActiveCursors(activeRows);
        resourcer.resourceGraph(expr, transformed);
        totalResults = resourcer.numGraphResults();
    }

    hoistNestedCompound(translator, transformed);

    if (totalResults == 0)
        totalResults = 1;
    transformed.append(*createAttribute(numResultsAtom, getSizetConstant(totalResults)));
    transformed.append(*LINK(graphIdExpr));
    if (isSequentialActionList(expr))
        transformed.append(*createAttribute(sequentialAtom));
    return createValue(no_subgraph, makeVoidType(), transformed);
}


IHqlExpression * resourceLibraryGraph(HqlCppTranslator & translator, IHqlExpression * expr, ClusterType targetClusterType, unsigned clusterSize, IHqlExpression * graphIdExpr, unsigned numResults)
{
    return doResourceGraph(translator, NULL, expr, targetClusterType, clusterSize, graphIdExpr, numResults, false, true);       //?? what value for isChild (e.g., thor library call).  Need to gen twice?
}


IHqlExpression * resourceNewChildGraph(HqlCppTranslator & translator, HqlExprCopyArray & activeRows, IHqlExpression * expr, ClusterType targetClusterType, IHqlExpression * graphIdExpr, unsigned numResults)
{
    return doResourceGraph(translator, &activeRows, expr, targetClusterType, 0, graphIdExpr, numResults, true, true);
}

IHqlExpression * resourceLoopGraph(HqlCppTranslator & translator, HqlExprCopyArray & activeRows, IHqlExpression * expr, ClusterType targetClusterType, IHqlExpression * graphIdExpr, unsigned numResults, bool insideChildQuery)
{
    return doResourceGraph(translator, &activeRows, expr, targetClusterType, 0, graphIdExpr, numResults, insideChildQuery, true);
}

IHqlExpression * resourceRemoteGraph(HqlCppTranslator & translator, IHqlExpression * _expr, ClusterType targetClusterType, unsigned clusterSize)
{
    CResourceOptions options(targetClusterType, clusterSize, translator.queryOptions(), translator.querySpillSequence());

    LinkedHqlExpr expr = _expr;
    {
        ActivityInvariantHoister hoister(options);
        HqlExprArray hoisted;
        expr.setown(hoister.transformRoot(expr));
        translator.traceExpression("AfterInvariant Child", expr);
    }

    HqlExprArray transformed;
    {
        EclResourcer resourcer(translator.queryErrorProcessor(), translator.wu(), translator.queryOptions(), options);

        resourcer.resourceRemoteGraph(expr, transformed);
    }
    hoistNestedCompound(translator, transformed);
    return createActionList(transformed);
}


/*
Conditions:

They are nasty.  We process the tree in two passes.  First we tag anything which must be evaluated, and
save a list of condition statements to process later.
Second pass we tag conditionals.
a) all left and right branches of a condition are tagged. [conditionSourceCount]
b) all conditional expressions are tagged with the conditions they are evaluated for.
   [if the condition lists match then it should be possible to merge the graphs]
c) The spill count for an node should ignore the number of links from conditional graphs,
   but should add the number of conditions.
d) if (a, b(f1) +b(f2), c) needs to link b twice though!

*/

/*
This transformer converts spill activities to no_dataset/no_output, and also converts splitters of splitters into
a single splitter.
*/

class SpillActivityTransformer : public NewHqlTransformer
{
public:
    SpillActivityTransformer(bool _createGraphResults);

protected:
    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    bool isUnbalanced(IHqlExpression * body)
    {
        ANewTransformInfo * info = queryTransformExtra(body);
        return info->spareByte1 != 0;
    }
    void setUnbalanced(IHqlExpression * body)
    {
        ANewTransformInfo * info = queryTransformExtra(body);
        info->spareByte1 = true;
    }
protected:
    bool createGraphResults;
};

static HqlTransformerInfo spillActivityTransformerInfo("SpillActivityTransformer");
SpillActivityTransformer::SpillActivityTransformer(bool _createGraphResults)
: NewHqlTransformer(spillActivityTransformerInfo), createGraphResults(_createGraphResults)
{ 
}

void SpillActivityTransformer::analyseExpr(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    if (alreadyVisited(body))
        return;

    //If splitters are commoned up ensure unbalanced splitters stay unbalanced.
    if ((body->getOperator() == no_split) && !body->hasAttribute(balancedAtom))
    {
        IHqlExpression * splitter = NULL;
        IHqlExpression * cur = body->queryChild(0);
        loop
        {
            node_operator op = cur->getOperator();
            if (op == no_split)
                splitter = cur;
            else if (op != no_commonspill)
                break;
            cur = cur->queryChild(0);
        }
        if (splitter)
            setUnbalanced(splitter->queryBody());
    }
    NewHqlTransformer::analyseExpr(expr);
}

IHqlExpression * SpillActivityTransformer::createTransformed(IHqlExpression * expr)
{
    IHqlExpression * annotation = queryTransformAnnotation(expr);
    if (annotation)
        return annotation;

    switch (expr->getOperator())
    {
    case no_split:
        {
            OwnedHqlExpr input = transform(expr->queryChild(0));
            if (input->getOperator() == no_split)
                return input.getClear();
            OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
            if (transformed->hasAttribute(balancedAtom) && isUnbalanced(expr))
                return removeAttribute(transformed, balancedAtom);
            return transformed.getClear();
        }
    case no_writespill:
        {
            HqlExprArray args;
            transformChildren(expr, args);
            if (createGraphResults)
                return createValue(no_setgraphresult, makeVoidType(), args);
            return createValue(no_output, makeVoidType(), args);
        }
    case no_commonspill:
        return transform(expr->queryChild(0));
    case no_readspill:
        {
            OwnedHqlExpr ds = transform(expr->queryChild(0));
            node_operator readOp = createGraphResults ? no_getgraphresult : no_table;

            HqlExprArray args;
            if (!createGraphResults)
                args.append(*transform(expr->queryChild(1)));
            args.append(*LINK(ds->queryRecord()));
            if (createGraphResults)
                args.append(*transform(expr->queryChild(1)));
            ForEachChildFrom(i, expr, 2)
            {
                IHqlExpression * cur = expr->queryChild(i);
                args.append(*transform(cur));
            }
            IHqlExpression * recordCountAttr = queryRecordCountInfo(expr);
            if (recordCountAttr)
                args.append(*LINK(recordCountAttr));

            OwnedHqlExpr ret;
            if (expr->isDatarow())
                ret.setown(createRow(readOp, args));
            else if (expr->isDictionary())
                ret.setown(createDictionary(readOp, args));
            else
                ret.setown(createDataset(readOp, args));
            const bool loseDistribution = false;
            return preserveTableInfo(ret, ds, loseDistribution, NULL);
        }
    }
    return NewHqlTransformer::createTransformed(expr);
}

IHqlExpression * convertSpillsToActivities(IHqlExpression * expr, bool createGraphResults)
{
    SpillActivityTransformer transformer(createGraphResults);
    transformer.analyse(expr, 0);
    return transformer.transformRoot(expr);
}



/*

The classes in this file are responsible for converting a logical declarative graph into an execution graph.
This process requires multiple stages:

a) Spot which dataset expression should be evaluated once outside the activity they are contained in, and create a shared result.

b) Spot scalar expressions that should be evaluated once outside the activity they are contained in, and create a shared result.

c) Combine setresult activities that are
   a) always unconditionally executed. or
   b) linked by dependencies and the extra cost of evaluation outweighs the cost of saving the temporary
   c) Only used as dependents from the same set of activities.
   d) Only used within the same set of conditions (a superset of (c) but possibly harder to acheive)

d) Spot global expressions that will be evaluated in more than one setresult/(or other activity that is only
   executed once - e.g., IF) and create a shared result.

e) Repeat (c) again.

f) Split the logical graph into subgraphs.  For Thor the subgraphs have the following requirements:
   1) Dependencies are in separate subgraphs from the activities that need them
   2) Lazy results are in separate subgraphs from non-lazy results.
      **When lazy results are implemented, check if this really is still a requirement, or can Thor child graphs
        be executed more like roxie?
   3) Conditional subgraphs are not combined
   4) Each subgraph is within the appropriate resource limits.

g) Optimize the spills to reduce the number of fields spilled to disk and read from disk.

h) Create splitters within subgraphs, and for Thor mark if they are balanced or not.  (i.e., Can they deadlock if they don't read ahead?)

i) Convert spills into diskread/diskwrite (or result read/write) activities


Previously these were all done in a single transformation, but that has now been split up into separate stages for simplicity.
Because they are implemented by different stages it is necessary to keep track of which outputs and results
are required, and which are primarily dependencies within the graph.  (Note external results that are lazily executed
may also be marked as lazy.)  The following attributes are used:

attr(_lazy_Atom, optCondition) - this external result is evaluated lazily.  The condition is optional.
attr(_graphLocal_Atom) - this result is only evaluated within the graph.  (May want to not save via the workunit.)
attr(_update_Atom) - a dependency of something with a ,UPDATE flag so may not be evaluated.  (Not currently used.)

 */
