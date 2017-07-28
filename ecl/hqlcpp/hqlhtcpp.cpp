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
#include "jliball.hpp"
#include <exception>

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "jdebug.hpp"
#include "rtldynfield.hpp"

#include "build-config.h"
#include "hql.hpp"
#include "hqlthql.hpp"
#include "hqlmeta.hpp"
#include "hqlutil.hpp"
#include "hqlpmap.hpp"
#include "hqlattr.hpp"
#include "hqlerrors.hpp"
#include "hqlvalid.hpp"
#include "hqlerror.hpp"

#include "hqlhtcpp.ipp"
#include "hqlttcpp.ipp"
#include "hqlwcpp.hpp"
#include "hqlcpputil.hpp"
#include "hqltcppc.ipp"
#include "hqlopt.hpp"
#include "hqlfold.hpp"
#include "hqlcerrors.hpp"
#include "hqlcatom.hpp"
#include "hqllib.ipp"
#include "hqlresource.hpp"
#include "hqlregex.ipp"
#include "hqlsource.ipp"
#include "hqlcse.ipp"
#include "hqlgraph.ipp"
#include "hqlscope.hpp"
#include "hqlccommon.hpp"
#include "deffield.hpp"
#include "hqlinline.hpp"
#include "hqlusage.hpp"
#include "hqlhoist.hpp"
#include "hqlcppds.hpp"

//The following are include to ensure they call compile...
#include "eclhelper.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield.hpp"
#include "rtlds_imp.hpp"
namespace hqlhtcpp
{  // Make sure we can't clash with generated versions or version check mechanism fails (if this dll was ever in the future linked in runtime)
#include "eclhelper_base.hpp"
}

#include "ctfile.hpp"   // for KEYBUILD_MAXLENGTH

#define MAX_ROWS_OUTPUT_TO_SDS              1000
#define MAX_SAFE_RECORD_SIZE                10000000
#define MAX_GRAPH_ECL_LENGTH                1000
#define MAX_ROW_VALUE_TEXT_LEN              10

//#define TRACE_META_TO_GRAPH
//#define FLATTEN_DATASETS

//#define TraceTableFields
//#define TRACE_ASSIGN_MATCH
//#define TRACE_DUMPTREE
//#define _GATHER_USAGE_STATS

//#define _SR6_

#define MAX_CSV_RECORD_SIZE     4096

#define ECLRTL_LIB          "eclrtl"

//===========================================================================


#ifdef _GATHER_USAGE_STATS
unsigned activityCounts[TAKlast][TAKlast];
#endif

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    dumpActivityCounts();
}

//===========================================================================

static const char * TF[2] = {"false","true"};
const char * boolToText(bool value) { return TF[value]; }           // is this strictly legal?

//---------------------------------------------------------------------------

/*

When should an activity be executed?
1. If it is used unconditionally.
and
2. if
  a) It isn't a result of some kind.
  or
  b) It is an external result
  or
  b) It is an internal result that is used by something outside this graph.

Any subgraph that contains unconditional activities should be marked with the <attr name="RootGraph" value="1"/>
And activity that should not be executed unconditionally should have _internal set.
*/

inline bool isInternalSeq(IHqlExpression * seq)
{
    return !seq || matchesConstantValue(seq, ResultSequenceInternal);
}

static void markSubGraphAsRoot(IPropertyTree * tree)
{
    if (!tree->hasProp("att[@name=\"rootGraph\"]"))
        addGraphAttributeBool(tree, "rootGraph", true);
}

SubGraphInfo * matchActiveGraph(BuildCtx & ctx, IHqlExpression * graphTag)
{
    FilteredAssociationIterator iter(ctx, AssocSubGraph);
    ForEach(iter)
    {
        SubGraphInfo & cur = static_cast<SubGraphInfo &>(iter.get());
        if (graphTag == cur.graphTag)
            return &cur;
    }
    return NULL;
}


bool isActiveGraph(BuildCtx & ctx, IHqlExpression * graphTag)
{
    return matchActiveGraph(ctx, graphTag) != NULL;
}


//---------------------------------------------------------------------------

class InternalResultTracker : public CInterface
{
public:
    InternalResultTracker(IHqlExpression * _name, IPropertyTree * _subGraphTree, unsigned _graphSeq, ActivityInstance * _definingActivity)
        : name(_name), subGraphTree(_subGraphTree), graphSeq(_graphSeq), definingActivity(_definingActivity)
    {
    }

    bool noteUse(IHqlExpression * searchName, unsigned curGraphSeq);

public:
    LinkedHqlExpr name;
    Linked<IPropertyTree> subGraphTree;
    unsigned graphSeq;
    Linked<ActivityInstance> definingActivity;
};

bool InternalResultTracker::noteUse(IHqlExpression * searchName, unsigned curGraphSeq)
{
    if (searchName == name)
    {
        if ((graphSeq != curGraphSeq) && subGraphTree)
        {
            markSubGraphAsRoot(subGraphTree);
            definingActivity->setInternalSink(false);
            subGraphTree.clear();
        }
        return true;
    }
    return false;
}

//---------------------------------------------------------------------------
IHqlExpression * getMetaUniqueKey(IHqlExpression * record, bool grouped)
{
    if (record) record = record->queryBody();
    LinkedHqlExpr search = record;
    if (grouped)
        search.setown(createAttribute(groupedAtom, search.getClear()));
    if (!search)
        search.setown(createValue(no_null));
    return search.getClear();
}

IHqlExpression * getNullStringPointer(bool translated)
{
    IHqlExpression * null = createValue(no_nullptr, LINK(constUnknownVarStringType));
    if (translated)
        return createTranslatedOwned(null);
    return null;
}

//---------------------------------------------------------------------------

bool canIterateTableInline(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_filter:
        return canIterateTableInline(expr->queryChild(0));
    case no_field:
    case no_select:
        return true;
    case no_newaggregate:
        {
            IHqlExpression * child = expr->queryChild(0);
            if (!isGrouped(child))
                return canIterateTableInline(child);
            return false;
        }
    default:
        return false;
    }
}

static IHqlExpression * createResultName(IHqlExpression * name)
{
    if (!name)
        return createQuoted("0", makeReferenceModifier(makeVarStringType(UNKNOWN_LENGTH)));
    return LINK(name);
}

//---------------------------------------------------------------------------

ColumnToOffsetMap * RecordOffsetMap::queryMapping(IHqlExpression * record, unsigned maxRecordSize, bool useAccessorClass)
{
    OwnedHqlExpr key = useAccessorClass ? createAttribute(classAtom, LINK(record)) : LINK(record);
    ColumnToOffsetMap * match = find(key);
    if (!match)
    {
        match = new ColumnToOffsetMap(key, record, ordinality(), 1, maxRecordSize, false, useAccessorClass);
        match->init(*this);
        addOwn(*match);
    }
    return match;
}

//---------------------------------------------------------------------------

MemberFunction::MemberFunction(HqlCppTranslator & _translator, BuildCtx & classctx) : translator(_translator), ctx(classctx)
{
}

MemberFunction::MemberFunction(HqlCppTranslator & _translator, BuildCtx & classctx, const char * text, unsigned _flags) : translator(_translator), ctx(classctx), flags(_flags)
{
    stmt = ctx.addQuotedFunction(text, (flags & MFdynamicproto) != 0);
}

MemberFunction::~MemberFunction() noexcept(false)
{
    //Do not process the aliases if we are aborting from an error
    if (!std::uncaught_exception())
        finish();
}

void MemberFunction::finish()
{
    if (!stmt)
        return;

    if ((flags & MFopt) && (stmt->numChildren() == 0))
        stmt->setIncluded(false);

    stmt = nullptr;
}

unsigned MemberFunction::numStmts() const
{
    if (!stmt)
        return 0;
    return calcTotalChildren(stmt);
}

void MemberFunction::setIncluded(bool value)
{
    stmt->setIncluded(value);
}

void MemberFunction::setIncomplete(bool value)
{
    stmt->setIncomplete(value);
}

void MemberFunction::start(const char * text, unsigned _flags)
{
    flags = _flags;
    stmt = ctx.addQuotedFunction(text, (flags & MFdynamicproto) != 0);
}

//---------------------------------------------------------------------------

static HqlTransformerInfo childDatasetSpotterInfo("ChildDatasetSpotter");
class NewChildDatasetSpotter : public ConditionalContextTransformer
{
public:
    NewChildDatasetSpotter(HqlCppTranslator & _translator, BuildCtx & _ctx, bool _forceRoot)
        : ConditionalContextTransformer(childDatasetSpotterInfo, true), translator(_translator), ctx(_ctx), forceRoot(_forceRoot)
    {
        //The following line forces the conditionalContextTransformer code to generate a single root subgraph.
        //An alternative would be to generate one (or more) graphs at the first unconditional place they are
        //used.  e.g., adding a no_compound(no_childquery, f(no_getgraphresult)) into the tree.
        //This was the initial approach, but it causes problems for subsequent optimizations -
        //if an optimzation causes an expression containing the no_getgraphresult to be hoisted so it is
        //evaluated before the no_childquery it creates an out-of-order dependency.
        createRootGraph = true;
    }

    virtual void analyseExpr(IHqlExpression * expr)
    {
        switch (pass)
        {
        case PassFindCandidates:
            if (!alreadyVisited(expr->queryBody()))
                markHoistPoints(expr);
            break;
        default:
            ConditionalContextTransformer::analyseExpr(expr);
            break;
        }
    }

    //MORE: This is a bit of a hack, and should be improved (share code with resource child hoist?)
    inline bool walkFurtherDownTree(IHqlExpression * expr)
    {
        //There are operators which can occur down the tree which may contain datasets
        //This should match the analyse code above
        switch (expr->getOperator())
        {
        case no_createrow:
        case no_inlinetable:
            //The expressions in the transform may contain datasets
        case no_addfiles:
        case no_datasetfromrow:
        case no_datasetfromdictionary:
        case no_alias_scope:
            //child datasets may have something worth creating a graph for
        case no_if:
            //The condition may be worth hoisting (and some of the inputs)
            return true;
        }
        return false;
    }

    void markHoistPoints(IHqlExpression * expr)
    {
        node_operator op = expr->getOperator();
        if (op == no_sizeof)
            return;

        if (expr->isDataset() || (expr->isDatarow() && (op != no_select)))
        {
            if (!translator.canAssignInline(&ctx, expr))
            {
                noteCandidate(expr);
                return;
            }
            if (!walkFurtherDownTree(expr))
                return;
        }

        doAnalyseExpr(expr);
    }

    IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        IHqlExpression * body = expr->queryBody(true);
        if (expr != body)
            return createTransformedAnnotation(expr);

        ConditionalContextInfo * extra = queryBodyExtra(expr);

        //The following must preceed transforming the children
        OwnedHqlExpr subgraph = createDefinitions(extra);

        OwnedHqlExpr transformed = ConditionalContextTransformer::createTransformed(expr);
        updateOrphanedSelectors(transformed, expr);

        assertex(!extra->moveTo);

        if (subgraph)
            return createCompound(subgraph.getClear(), transformed.getClear());
        return transformed.getClear();
    }

    virtual void transformCandidate(ConditionalContextInfo * candidate)
    {
        while (builders.ordinality() < insertLocations.ordinality())
            builders.append(* new ChildGraphExprBuilder(0));

        IHqlExpression * expr = candidate->original;
        ConditionalContextInfo * moveTo = candidate->moveTo;
        OwnedHqlExpr guard;
        if (moveTo)
        {
            guard.setown(getGuardCondition(moveTo, expr));

            //Expressions which are very simple functions of unconditional expressions are treated as if they
            //are unconditional
            if (moveTo->isUnconditional() && isUsedUnconditionallyEnough(expr))
                guard.set(queryBoolExpr(true));

            bool invalid = !canDuplicateExpr(guard);
            //version 1: don't guard any child queries.
            if (!matchesBoolean(guard, true))
            {
                assertex(moveTo->guards);
                //MORE: For the moment disable any expressions that are only used conditionally.
                //Often including conditions improves the code, but sometimes the duplicate evaluation of the
                //guard conditions in the parent and the child causes excessive code generation.
                //And forcing it into an alias doesn't help because that isn't currently executed in the parent.
                //Uncomment: if (moveTo->guards->guardContainsCandidate(expr))
                {
                    invalid = true;
                }
            }

            if (invalid)
            {
                removeDefinition(moveTo, candidate);
                moveTo = NULL;
            }
        }

        //A candidate inside a condition that prevents it being moved just creates a definition where it is.
        if (!moveTo)
            return;

        ChildGraphExprBuilder & builder = queryBuilder(moveTo);
        IHqlExpression * annotated = candidate->firstAnnotatedExpr ? candidate->firstAnnotatedExpr : expr;
        OwnedHqlExpr guarded = createGuardedDefinition(moveTo, annotated, guard);
        OwnedHqlExpr transformed = builder.addDataset(guarded);

        if (moveTo == candidate)
        {
            OwnedHqlExpr subgraph = createDefinitions(moveTo);
            transformed.setown(createCompound(subgraph.getClear(), transformed.getClear()));
        }

        setTransformed(expr, transformed);
    }

    virtual IHqlExpression * createDefinitions(ConditionalContextInfo * extra)
    {
        if (!extra->hasDefinitions())
            return NULL;

        ChildGraphExprBuilder & builder = queryBuilder(extra);
        OwnedHqlExpr graph = builder.getGraph();
        OwnedHqlExpr cleanedGraph = mapExternalToInternalResults(graph, builder.queryRepresents());
        return cleanedGraph.getClear();
    }

    ChildGraphExprBuilder & queryBuilder(ConditionalContextInfo * extra)
    {
        unsigned match = insertLocations.find(*extra);
        assertex(match != NotFound);
        return builders.item(match);
    }

    inline bool isUsedUnconditionallyEnough(IHqlExpression * expr)
    {
        IHqlExpression * search = expr;
        for (;;)
        {
            if (isUsedUnconditionally(search))
                return true;
            switch (search->getOperator())
            {
            case no_newaggregate:
                //Hash aggregate is NOT a trivial operation.
                if (queryRealChild(search, 3))
                    return false;
                break;
            case no_selectnth:
            case no_filter:
                break;
            case no_select:
                if (isNewSelector(search))
                    break;
                return false;
            default:
                return false;
            }
            search = search->queryChild(0);
        }
    }

protected:
    CIArrayOf<ChildGraphExprBuilder> builders;
    HqlCppTranslator & translator;
    BuildCtx & ctx;
    bool forceRoot;
};



class StatementCollection : public HqlExprArray
{
public:
    //Combine multiple conditional assigns, where the guard condition is the same.
    //Combine either IF() or CHOOSE()
    void combineConditions()
    {
        unsigned max = ordinality();
        for (unsigned first=0; first+1 < max; first++)
        {
            IHqlExpression & cur = item(first);
            if (cur.getOperator() == no_assign)
            {
                IHqlExpression * rhs = cur.queryChild(1);
                if (isCast(rhs))
                    rhs = rhs->queryChild(0);
                node_operator firstOp = rhs->getOperator();
                unsigned numFirstChildren = rhs->numChildren();
                //Don't combine choose() operators with large numbers of constant values since an array lookup is probably more efficient.
                if ((firstOp == no_if) ||
                    ((firstOp == no_choose) && (numFirstChildren <= 3 || !allBranchesAreConstant(rhs))))
                {
                    IHqlExpression * cond = rhs->queryChild(0);
                    unsigned next = first+1;
                    while (next != max)
                    {
                        IHqlExpression & nextAssign = item(next);
                        if (nextAssign.getOperator() != no_assign)
                            break;

                        IHqlExpression * nextRhs = nextAssign.queryChild(1);
                        if (isCast(nextRhs))
                            nextRhs = nextRhs->queryChild(0);
                        if (nextRhs->getOperator() != firstOp)
                            break;
                        if (nextRhs->queryChild(0) != cond)
                            break;
                        if (nextRhs->numChildren() != numFirstChildren)
                            break;
                        next++;
                    }
                    if (next != first+1)
                    {
                        OwnedHqlExpr combined = combineConditionRange(first, next);
                        replace(*combined.getClear(), first);
                        unsigned num = (next - first - 1);
                        removen(first+1, num);
                        max -= num;
                    }
                }
                //MORE: Combine no_case and no_map - but need to be careful, 
                //because they don't currently have an implementation for actions, so either need to implement, or convert
                //to ifs, but also need to be careful we don't make the implementation worse!
            }
        }
    }

    void replaceAssignment(IHqlExpression & search, IHqlExpression & newAssign)
    {
        unsigned match = find(search);
        replace(OLINK(newAssign), match);
    }

    bool onlyOccursOnce(IHqlExpression * expr)
    {
        return (getNumOccurences(*this, expr, 2) == 1);
    }

protected:
    bool allBranchesAreConstant(IHqlExpression * expr)
    {
        ForEachChildFrom(i, expr, 1)
        {
            IHqlExpression * cur = expr->queryChild(i);
            if (!cur->queryValue())
                return false;
        }
        return true;
    }

    IHqlExpression * extractBranches(unsigned from, unsigned to, unsigned child)
    {
        StatementCollection assigns;
        for (unsigned i=from; i < to; i++)
        {
            IHqlExpression & cur = item(i);
            IHqlExpression * lhs = cur.queryChild(0);
            IHqlExpression * rhs = cur.queryChild(1);
            OwnedHqlExpr newRhs;
            if (isCast(rhs))
            {
                IHqlExpression * branch = rhs->queryChild(0)->queryChild(child);
                newRhs.setown(ensureExprType(branch, rhs->queryType()));
            }
            else
                newRhs.set(rhs->queryChild(child));
            assigns.append(*createAssign(LINK(lhs), LINK(newRhs)));
        }
        assigns.combineConditions();
        return createActionList(assigns);
    }

    IHqlExpression * combineConditionRange(unsigned from, unsigned to)
    {
        IHqlExpression * firstRhs = item(from).queryChild(1);
        IHqlExpression * conditionExpr = isCast(firstRhs) ? firstRhs->queryChild(0) : firstRhs;
        HqlExprArray args;
        args.append(*LINK(conditionExpr->queryChild(0)));
        ForEachChildFrom(i, conditionExpr, 1)
            args.append(*extractBranches(from, to, i));
        return createValue(conditionExpr->getOperator(), makeVoidType(), args);
    }
};

class DelayedStatementExecutor
{
public:
    DelayedStatementExecutor(HqlCppTranslator & _translator, BuildCtx & _ctx)
        : translator(_translator), buildctx(_ctx)
    {
        processed = false;
    }

    void processAssign(BuildCtx & ctx, IHqlExpression * stmt)
    {
        pending.append(*LINK(stmt));
    }

    void processAlias(BuildCtx & ctx, IHqlExpression * stmt)
    {
        pending.append(*LINK(stmt));
    }

    void processStmts(IHqlExpression * expr)
    {
        expr->unwindList(pending, no_actionlist);
    }

    void clear()
    {
        pending.kill();
        processed = false;
    }

    IHqlExpression * getPrefetchGraph()
    {
        spotChildDatasets(true);
        if (pending.ordinality() == 0)
            return NULL;
        IHqlExpression & subquery = pending.item(0);
        if (subquery.getOperator() == no_childquery)
        {
            pending.remove(0, true);
            return &subquery;
        }

        return NULL;
    }

    void flush(BuildCtx & ctx)
    {
        spotChildDatasets(false);
        combineConditions();
        optimizeAssigns();
        ForEachItemIn(i, pending)
            translator.buildStmt(ctx, &pending.item(i));
        pending.kill();
    }

    void optimize()
    {
        spotChildDatasets(false);
        combineConditions();
        optimizeAssigns();
    }

    IHqlExpression * getActionList()
    {
        OwnedHqlExpr ret = createActionList(pending);
        pending.kill();
        return ret.getClear();
    }

protected:
    //Combine multiple conditional assigns, where the guard condition is the same.
    void combineConditions()
    {
        pending.combineConditions();
    }

    void spotChildDatasets(bool forceRoot)
    {
        if (!processed && translator.queryCommonUpChildGraphs())
        {
            HqlExprArray analyseExprs;
            ForEachItemIn(i, pending)
            {
                IHqlExpression & cur = pending.item(i);
                IHqlExpression * value = &cur;
                switch (value->getOperator())
                {
                case no_assign:
                    value = value->queryChild(1);
                    break;
                case no_alias:
                case no_skip:
                    value = value->queryChild(0);
                    break;
                }
                if (value)
                    analyseExprs.append(*LINK(value));
            }

            NewChildDatasetSpotter spotter(translator, buildctx, forceRoot);
            if (spotter.analyseNeedsTransform(analyseExprs))
            {
                //This could be conditional on whether or not there is an unconditional candidate, but that would stop
                //the same expression being commoned up between two conditional branches.
                //So, only avoid if 1 conditional candidate used in a single location.
                bool worthHoisting = true;
                if (!forceRoot)
                {
                    if (spotter.hasSingleConditionalCandidate())
                        worthHoisting = false;
                }

                if (worthHoisting)
                {
                    bool createSubQueryBeforeAll = forceRoot;
                    spotter.transformAll(pending, createSubQueryBeforeAll);
                    translator.traceExpressions("spotted child", pending);
                }
            }
            processed = true;
        }
    }

    virtual void optimizeAssigns() {}


protected:
    HqlCppTranslator & translator;
    BuildCtx buildctx;
    StatementCollection pending;
    bool processed;
};

void HqlCppTranslator::optimizeBuildActionList(BuildCtx & ctx, IHqlExpression * exprs)
{
    if ((exprs->getOperator() != no_actionlist) || !activeGraph)
    {
        buildStmt(ctx, exprs);
        return;
    }

    DelayedStatementExecutor delayed(*this, ctx);

    delayed.processStmts(exprs);
    delayed.flush(ctx);
}


//---------------------------------------------------------------------------

static IHqlExpression * getExtractMatchingAssign(HqlExprArray & assigns, IHqlExpression * search, unsigned & expectedIndex, IHqlExpression * selfSelector)
{
    if (assigns.isItem(expectedIndex))
    {
        IHqlExpression & candidate = assigns.item(expectedIndex);
        IHqlExpression * lhs = candidate.queryChild(0);
        IHqlExpression * candidateField = lhs->queryChild(1);
        if (candidateField == search)
        {
            OwnedHqlExpr ret;
            if (lhs->queryChild(0) == selfSelector)
                ret.set(&candidate);
            else
            {
                IHqlExpression * rhs = candidate.queryChild(1);
                ret.setown(createAssign(createSelectExpr(LINK(selfSelector), LINK(candidateField)), LINK(rhs)));
            }
            expectedIndex++;
            return ret.getClear();
        }
    }

    ForEachItemIn(idx, assigns)
    {
        IHqlExpression & assign = assigns.item(idx);
#ifdef TRACE_ASSIGN_MATCH
        PrintLog("Next comparison:");
        x.clear().append("target(").append((unsigned)assign.queryChild(0)->queryChild(0)).append(":");
        x.appendf("%p", assign.queryChild(0)->queryChild(1)).append(")   ");
        assign.queryChild(0)->toString(x);
        PrintLog(x.str());
        x.clear().append("search(").appendf("%p", search).append(")   ");
        search->toString(x);
        PrintLog(x.str());
#endif
        IHqlExpression * lhs = assign.queryChild(0);
        IHqlExpression * candidateField = lhs->queryChild(1);
        if (candidateField == search)
        {
            OwnedHqlExpr ret;
            if (lhs->queryChild(0) == selfSelector)
                ret.set(&assign);
            else
            {
                IHqlExpression * rhs = assign.queryChild(1);
                ret.setown(createAssign(createSelectExpr(LINK(selfSelector), LINK(candidateField)), LINK(rhs)));
            }

            expectedIndex = idx+1;
            return ret.getClear();
        }
    }
    return NULL;
}



class TransformBuilder : public DelayedStatementExecutor
{
public:
    TransformBuilder(HqlCppTranslator & _translator, BuildCtx & _ctx, IHqlExpression * _record, BoundRow * _self, HqlExprArray & _assigns) :
            DelayedStatementExecutor(_translator, _ctx), assigns(_assigns), record(_record), self(_self)
    {
        expectedIndex = 0;
        if (translator.recordContainsIfBlock(record))
            mapper.setown(new NestedHqlMapTransformer);
    }

    TransformBuilder(const TransformBuilder & other, BuildCtx & _ctx) :
            DelayedStatementExecutor(other.translator, _ctx), mapper(other.mapper), assigns(other.assigns), self(other.self)
    {
        expectedIndex = 0;
    }

    void doTransform(BuildCtx & ctx, IHqlExpression * transform, BoundRow * self);
    void buildTransformChildren(BuildCtx & ctx, IHqlExpression * record, IHqlExpression * parentSelector);

protected:
    virtual void checkAssigned() {  }

    virtual void onIfBlock(IHqlExpression * expr) { }

    virtual void onMissingAssignment(IHqlExpression * expr)
    {
            StringBuffer s;
            expr->toString(s);
            throwError2(HQLERR_MissingTransformAssignXX, s.str(), expr);
    }

    void pushCondition(IHqlExpression * cond)
    {
        mapper->beginNestedScope();
    }

    void popCondition()
    {
        mapper->endNestedScope();
    }

    void buildTransform(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * parentSelector);
    void doBuildTransformChildren(BuildCtx & ctx, IHqlExpression * record, IHqlExpression * parentSelector);

public:
    Linked<NestedHqlMapTransformer> mapper;
    HqlExprArray & assigns;
    LinkedHqlExpr record;
    BoundRow * self;
    unsigned expectedIndex;
};

void TransformBuilder::buildTransform(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * parentSelector)
{
    switch (expr->getOperator())
    {
    case no_ifblock:
        {
            onIfBlock(expr);

            flush(ctx);

            assertex(mapper != NULL);
            OwnedHqlExpr test = replaceSelector(expr->queryChild(0), querySelfReference(), parentSelector);
            OwnedHqlExpr foldedTest = mapper->transformRoot(test);
            foldedTest.setown(foldHqlExpression(foldedTest));           // can only contain references to self, so don't need to worry about other datasets in scope being messed up.

            OwnedHqlExpr sizeOfIfBlock = createValue(no_sizeof, makeIntType(4,false), createSelectExpr(LINK(parentSelector), LINK(expr)));

            IValue * mappedValue = foldedTest->queryValue();


            BuildCtx subctx(ctx);
            bool include = true;
            if (mappedValue)
            {
                //Associate the ifblock condition to avoid it being evaluated later when calculating the field offsets
                ctx.associateExpr(test, foldedTest);

                if (!mappedValue->getBoolValue())
                    include = false;
            }
            else
                pushCondition(foldedTest);

            if (include)
            {
                translator.buildFilter(subctx, foldedTest);
                TransformBuilder childBuilder(*this, subctx);
                childBuilder.buildTransformChildren(subctx, expr->queryChild(1), parentSelector);
                childBuilder.flush(subctx);

                //This calculates the size of the previous block.  It means that subsequent uses of the 
                //offsets are cached - even if they are inside another ifblock().
                CHqlBoundExpr bound;
                translator.buildCachedExpr(ctx, sizeOfIfBlock, bound);
            }
            else
            {
                //This calculates the size of the previous block.  It means that subsequent uses of the 
                //offsets are cached - even if they are inside another ifblock().
                OwnedHqlExpr zero = getSizetConstant(0);
                ctx.associateExpr(sizeOfIfBlock, zero);
            }

            if (!mappedValue)
                popCondition();
        }
        break;
    case no_record:
        doBuildTransformChildren(ctx, expr, parentSelector);
        break;
    case no_field:
        {
            OwnedHqlExpr match = getExtractMatchingAssign(assigns, expr, expectedIndex, parentSelector);
            if (match)
            {
                processAssign(ctx, match);
                if (mapper && (match->getOperator() == no_assign))
                {
                    IHqlExpression * rhs = match->queryChild(1);
                    if (rhs->queryValue())
                    {
                        IHqlExpression * lhs = match->queryChild(0);
                        OwnedHqlExpr cast = ensureExprType(rhs, lhs->queryType());
                        mapper->setMapping(lhs, cast);
                    }
                }
                return;
            }

            onMissingAssignment(expr);
        }
        break;
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        break;
    default:
        UNIMPLEMENTED;
    }
}

void TransformBuilder::doBuildTransformChildren(BuildCtx & ctx, IHqlExpression * record, IHqlExpression * parentSelector)
{
    ForEachChild(idx, record)
        buildTransform(ctx, record->queryChild(idx), parentSelector);
}


void TransformBuilder::buildTransformChildren(BuildCtx & ctx, IHqlExpression * record, IHqlExpression * parentSelector)
{
    assertex(parentSelector);
    expectedIndex = 0;
    doBuildTransformChildren(ctx, record, parentSelector);
}


void TransformBuilder::doTransform(BuildCtx & ctx, IHqlExpression * transform, BoundRow * self)
{
    IHqlExpression * body = transform->queryBody(true);
    if (transform != body)
    {
        ErrorSeverityMapper::Scope saved(translator.queryLocalOnWarningMapper());
        switch (transform->getAnnotationKind())
        {
        case annotate_meta:
            translator.queryLocalOnWarningMapper().processMetaAnnotation(transform);
            break;
        case annotate_symbol:
            translator.queryLocalOnWarningMapper().setSymbol(transform);
            break;
        }
        doTransform(ctx, body, self);
        return;
    }

    if (!isKnownTransform(transform))
    {
        translator.doUserTransform(ctx, transform, self);
        return;
    }

    translator.filterExpandAssignments(ctx, this, assigns, transform);
    IHqlExpression * selfRecord = self->queryRecord();
    buildTransformChildren(ctx, selfRecord, self->querySelector());
    flush(ctx);
    checkAssigned();

    //If this is a blank record with the size "fixed" to 1, clear the byte so consistent and disk writes compress well
    if (isEmptyRecord(selfRecord) && selfRecord->hasAttribute(_nonEmpty_Atom))
        translator.buildClearRecord(ctx, self->querySelector(), selfRecord, 0);
}


class UpdateTransformBuilder : public TransformBuilder
{
    friend class UnsafeSelectorReplacer;
public:
    UpdateTransformBuilder(HqlCppTranslator & _translator, BuildCtx & _ctx, IHqlExpression * record, BoundRow * _self, IHqlExpression * _prevSelector, HqlExprArray & _assigns, bool _canRemoveLeadingAssigns) :
        TransformBuilder(_translator, _ctx, record, _self, _assigns), prevSelector(_prevSelector)
    {
        aliasInsertPos = 0;
        needToReassignAll = false;
        canRemoveLeadingAssigns = _canRemoveLeadingAssigns;
    }

    void ensureAlias(IHqlExpression * expr);
    inline bool isUnsafeSelector(IHqlExpression * expr) const
    {
        return needToReassignAll || unsafeSelectors.contains(*expr);
    }

protected:
    virtual void checkAssigned() { }
    virtual void onIfBlock(IHqlExpression * expr) { throwUnexpected(); }
    virtual void onMissingAssignment(IHqlExpression * expr) {}
    virtual void optimizeAssigns();

    bool isSpecialAssignment(IHqlExpression * assign, node_operator op, IHqlExpression * previous) const;
    void optimizeAssigns(IHqlExpression * expr, IHqlExpression * parentSelector);
    void optimizeRecordAssigns(IHqlExpression * record, IHqlExpression * parentSelector);
    IHqlExpression * replaceUnsafeSelectors(IHqlExpression * rhs);
    void optimizeSpecialAssignments(IHqlExpression * expr, IHqlExpression * parentSelector);
    void optimizeRecordSpecialAssignments(IHqlExpression * expr, IHqlExpression * parentSelector);

    void protectAgainstLeaks(IHqlExpression * expr);
    void protectRecordAgainstLeaks(IHqlExpression * expr);

protected:
    LinkedHqlExpr prevSelector;
    HqlExprArray unsafeSelectors;
    HqlExprArray preservedSelectors;
    unsigned aliasInsertPos;
    bool needToReassignAll;
    bool canRemoveLeadingAssigns;
};

void UpdateTransformBuilder::ensureAlias(IHqlExpression * expr)
{
    if (!pending.contains(*expr))
        pending.add(*LINK(expr), aliasInsertPos++);
}

bool UpdateTransformBuilder::isSpecialAssignment(IHqlExpression * assign, node_operator op, IHqlExpression * previous) const
{
    IHqlExpression * rhs = assign->queryChild(1);
    if (rhs->getOperator() != op)
        return false;
    if (rhs->queryChild(0) != previous)
        return false;
    return true;
}

void UpdateTransformBuilder::optimizeSpecialAssignments(IHqlExpression * expr, IHqlExpression * parentSelector)
{
    switch (expr->getOperator())
    {
    case no_record:
        optimizeRecordSpecialAssignments(expr, parentSelector);
        break;
    case no_field:
        {
            //check for SELF.x := RIGHT.x <add-file> f(LEFT)
            node_operator newOp = no_none;
            OwnedHqlExpr previous = createSelectExpr(LINK(prevSelector), LINK(expr));
            OwnedHqlExpr match;
            if (expr->isDataset())
            {
                match.setown(getExtractMatchingAssign(pending, expr, expectedIndex, parentSelector));

                if (match && isSpecialAssignment(match, no_addfiles, previous))
                    newOp = no_assign_addfiles;
            }

            if (newOp && pending.onlyOccursOnce(previous))
            {
                IHqlExpression * rhs = match->queryChild(1);
                OwnedHqlExpr newAssign = createValue(newOp, makeVoidType(), LINK(match->queryChild(0)), LINK(rhs->queryChild(1)));
                pending.replaceAssignment(*match, *newAssign);
                preservedSelectors.append(*LINK(previous));
            }
        }
        break;
    }
}


void UpdateTransformBuilder::optimizeRecordSpecialAssignments(IHqlExpression * record, IHqlExpression * parentSelector)
{
    ForEachChild(idx, record)
        optimizeSpecialAssignments(record->queryChild(idx), parentSelector);
}


//Ensure all fields from SELF are cloned before they are overwritten.
//If only fixed width fields have been updated so far then any fields not yet assigned can be used
//If a variable width field has been updated all fields updated so far are invalid, and all field that
//follow may have been overwritten => it will need an alias.  For simplicity all fields are assumed tainted.
void UpdateTransformBuilder::optimizeAssigns(IHqlExpression * expr, IHqlExpression * parentSelector)
{
    switch (expr->getOperator())
    {
    case no_ifblock:
        throwUnexpected();
    case no_record:
        optimizeRecordAssigns(expr, parentSelector);
        break;
    case no_field:
        {
            OwnedHqlExpr match = getExtractMatchingAssign(pending, expr, expectedIndex, parentSelector);
            if (match)
            {
                IHqlExpression * source = match->queryChild(1);
                OwnedHqlExpr previous = createSelectExpr(LINK(prevSelector), LINK(expr));
                bool usesPrevious = (source != previous) && exprReferencesDataset(source, prevSelector);
                bool retainAssign = !canRemoveLeadingAssigns || needToReassignAll || usesPrevious || (match->getOperator() != no_assign) || isGraphDependent(source);
                if (retainAssign)
                {
                    IHqlExpression * target = match->queryChild(0);

                    //Variable offset => all subsequent fields need to be reassigned.
                    ITypeInfo * type = expr->queryType();
                    bool hasVariableSize = (type->getSize() == UNKNOWN_LENGTH);

                    //Potential problems with fixed length strings.  Otherwise it should be safe, or go via a temporary
                    bool safeToAccessSelf = hasVariableSize || !isTypePassedByAddress(type);        // not true for some fixed length strings, what else?

                    //Any access to this field now needs to go via a temporary
                    if (!safeToAccessSelf)
                        unsafeSelectors.append(*LINK(previous));
                    OwnedHqlExpr safeRhs = replaceUnsafeSelectors(source);
                    if (safeToAccessSelf)
                        unsafeSelectors.append(*LINK(previous));

                    if (hasVariableSize)
                        needToReassignAll = true;
                    if (safeRhs != source)
                    {
                        //MORE: Create no_plusequals, no_concat_equals
                        OwnedHqlExpr newAssign = createAssign(LINK(target), LINK(safeRhs));
                        unsigned pos = pending.find(*match);
                        assertex(pos != NotFound);
                        pending.replace(*newAssign.getClear(), pos);
                    }
                }
                else
                {
                    preservedSelectors.append(*LINK(previous));
                    pending.zap(*match);
                }
                return;
            }
        }
        break;
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        break;
    default:
        UNIMPLEMENTED;
    }
}


void UpdateTransformBuilder::optimizeRecordAssigns(IHqlExpression * record, IHqlExpression * parentSelector)
{
    ForEachChild(idx, record)
        optimizeAssigns(record->queryChild(idx), parentSelector);
}

void UpdateTransformBuilder::protectAgainstLeaks(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_record:
        protectRecordAgainstLeaks(expr);
        break;
    case no_field:
        {
            if (hasLinkCountedModifier(expr))
            {
                OwnedHqlExpr previous = createSelectExpr(LINK(prevSelector), LINK(expr));
                if (!preservedSelectors.contains(*previous))
                {
                    OwnedHqlExpr owned = createAliasOwn(ensureOwned(previous), NULL);
                    ensureAlias(owned);
                }
            }
        }
        break;
    }
}


void UpdateTransformBuilder::protectRecordAgainstLeaks(IHqlExpression * record)
{
    ForEachChild(idx, record)
        protectAgainstLeaks(record->queryChild(idx));
}

void UpdateTransformBuilder::optimizeAssigns()
{
    expectedIndex = 0;
    optimizeRecordSpecialAssignments(record, self->querySelector());
    expectedIndex = 0;
    optimizeRecordAssigns(record, self->querySelector());
    protectRecordAgainstLeaks(record);
}

static HqlTransformerInfo unsafeSelectorReplacerInfo("UnsafeSelectorReplacer");
class UnsafeSelectorReplacer : public NewHqlTransformer
{
public:
    UnsafeSelectorReplacer(UpdateTransformBuilder & _builder, IHqlExpression * _searchSelector)
        : NewHqlTransformer(unsafeSelectorReplacerInfo), builder(_builder), searchSelector(_searchSelector)
    {
    }

    inline IHqlExpression * getReplacement(IHqlExpression * expr)
    {
        //Yuk if the whole row is referenced the code is not going to be good!
        if (expr == searchSelector)
            return ensureAliased(expr);
        if ((expr->getOperator() == no_select) && (expr->queryChild(0) == searchSelector))
        {
            if (builder.isUnsafeSelector(expr))
                return ensureAliased(expr);
            return LINK(expr);
        }
        return NULL;
    }

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        IHqlExpression * body = expr->queryBody(true);
        if (expr == body)
        {
            IHqlExpression * replacement = getReplacement(expr);
            if (replacement)
                return replacement;
            return NewHqlTransformer::createTransformed(expr);
        }
        OwnedHqlExpr transformed = transform(body);
        return expr->cloneAnnotation(transformed);
    }

    virtual IHqlExpression * createTransformedSelector(IHqlExpression * expr)
    {
        IHqlExpression * replacement = getReplacement(expr);
        if (replacement)
            return replacement;
        return NewHqlTransformer::createTransformedSelector(expr);
    }

    IHqlExpression * ensureAliased(IHqlExpression * expr)
    {
        OwnedHqlExpr alias = createAlias(expr, NULL);
        builder.ensureAlias(alias);
        return alias.getClear();
    }

protected:
    UpdateTransformBuilder & builder;
    IHqlExpression * searchSelector;
};

IHqlExpression * UpdateTransformBuilder::replaceUnsafeSelectors(IHqlExpression * rhs)
{
    UnsafeSelectorReplacer replacer(*this, prevSelector);
    return replacer.transformRoot(rhs);
}

//---------------------------------------------------------------------------

void HqlCppTranslator::doFilterAssignment(BuildCtx & ctx, TransformBuilder * builder, HqlExprArray & assigns, IHqlExpression * cur)
{
    node_operator op = cur->getOperator();
    switch (op)
    {
    case no_assignall:
    case no_newtransform:
    case no_transform:
    case no_alias_scope:
        doFilterAssignments(ctx, builder, assigns, cur);
        break;
    case no_assign:
        assigns.append(*LINK(cur));
        break;
    case no_assert:
    case no_skip:
    case no_alias:
        if (builder)
            builder->processAlias(ctx, cur);
        else
            buildStmt(ctx, cur);
        break;
    case no_attr:
    case no_attr_link:
    case no_attr_expr:
        break;
    default:
        UNIMPLEMENTED;
    }
}

void HqlCppTranslator::doFilterAssignments(BuildCtx & ctx, TransformBuilder * builder, HqlExprArray & assigns, IHqlExpression * expr)
{
    if (expr->getOperator() == no_alias_scope)
    {
        ForEachChildFrom(i, expr, 1)
            doFilterAssignment(ctx, builder, assigns, expr->queryChild(i));
        doFilterAssignment(ctx, builder, assigns, expr->queryChild(0));
    }
    else
    {
        ForEachChild(i, expr)
            doFilterAssignment(ctx, builder, assigns, expr->queryChild(i));
    }
}

void HqlCppTranslator::filterExpandAssignments(BuildCtx & ctx, TransformBuilder * builder, HqlExprArray & assigns, IHqlExpression * rawExpr)
{
    LinkedHqlExpr expr = rawExpr;

    if (options.spotCSE)
        expr.setown(spotScalarCSE(expr, NULL, queryOptions().spotCseInIfDatasetConditions));
    traceExpression("transform cse", expr);

//  expandAliases(ctx, expr);
    doFilterAssignments(ctx, builder, assigns, expr);
}


void HqlCppTranslator::associateCounter(BuildCtx & ctx, IHqlExpression * counterExpr, const char * name)
{
    if (counterExpr)
    {
        OwnedHqlExpr counterVar = createVariable(name, LINK(counterType));
        ctx.associateExpr(counterExpr, counterVar);
    }
}


unsigned HqlCppTranslator::getConsistentUID(IHqlExpression * ptr)
{
    if (!ptr)
        return 0;
    // Allocate consistent numbers helps to regression check the generated code
    if (recordIndexCache.find(ptr) == NotFound)
        recordIndexCache.append(ptr);
    return recordIndexCache.find(ptr)+1;
}


unsigned HqlCppTranslator::getNextGlobalCompareId()
{
    return nextGlobalCompareId++;
}


unsigned HqlCppTranslator::beginFunctionGetCppIndex(unsigned activityId, bool isChildActivity)
{
    activitiesThisCpp++;
    if (activitiesThisCpp > options.activitiesPerCpp)
    {
        //Allow 25% over the default number of activities per child in order to reduce the number of activities moved into
        //the header file.
        if (!isChildActivity || activitiesThisCpp > (options.activitiesPerCpp * 5 / 4))
        {
            curCppFile++;
            activitiesThisCpp = 1;
            code->cppInfo.append(* new CppFileInfo(activityId));
        }
    }
    CppFileInfo & curCpp = code->cppInfo.tos();
    if (activityId)
    {
        if (curCpp.minActivityId == 0)
            curCpp.minActivityId = activityId;
        curCpp.maxActivityId = activityId;
    }
    return curCppFile;
}

//---------------------------------------------------------------------------

static IHqlExpression * createResultAttribute(IHqlExpression * seq, IHqlExpression * name)
{
    //if a named user output then set seq to the name so that workunit reads from the named symbol get commoned up correctly
    if (name && !name->queryType()->isInteger() && (getIntValue(seq, -1) >= 0))
        seq = name;
    return createAttribute(resultAtom, LINK(seq), LINK(name));
}

static void associateRemoteResult(BuildCtx & ctx, ABoundActivity * table, IHqlExpression * seq, IHqlExpression * name)
{
    OwnedHqlExpr attr = createResultAttribute(seq, name);
    OwnedHqlExpr unknown = createUnknown(no_attr, NULL, NULL, LINK(table));
    ctx.associateExpr(attr, unknown);
}

void HqlCppTranslator::associateRemoteResult(ActivityInstance & instance, IHqlExpression * seq, IHqlExpression * name)
{
    ::associateRemoteResult(*activeGraphCtx, instance.table, seq, name);

    if (name && targetRoxie())
    {
        OwnedHqlExpr attr = createResultAttribute(seq, name);
        globalFiles.append(* new GlobalFileTracker(attr, instance.graphNode));
    }
}

void HqlCppTranslator::queryAddResultDependancy(ABoundActivity & whoAmIActivity, IHqlExpression * seq, IHqlExpression * name)
{
    if (activeGraphCtx)
    {
        //Because of extend, we need to find all the possible matches, not just the last one.
        AssociationIterator iter(*activeGraphCtx);
        OwnedHqlExpr attr = createResultAttribute(seq, name);
        ForEach(iter)
        {
            HqlExprAssociation & cur = iter.get();
            if (cur.represents == attr)
            {
                ABoundActivity * match = (ABoundActivity *)cur.queryExpr()->queryUnknownExtra();
                IHqlExpression * whoAmI = whoAmIActivity.queryBound();
                OwnedHqlExpr dependency = createAttribute(dependencyAtom, LINK(whoAmI), LINK(match->queryBound()));
                if (!activeGraphCtx->queryMatchExpr(dependency))
                {
                    activeGraphCtx->associateExpr(dependency, dependency);
                    addDependency(*activeGraphCtx, match, &whoAmIActivity, dependencyAtom);
                }
            }
        }
        if (name && targetRoxie())
            registerGlobalUsage(attr);
    }
}

bool HqlCppTranslator::tempRowRequiresFinalize(IHqlExpression * record) const
{
    if (recordRequiresDestructor(record) || options.finalizeAllRows)
        return true;
    if (isVariableSizeRecord(record))
        return true;
    return false;
}

BoundRow * HqlCppTranslator::createRowBuilder(BuildCtx & ctx, BoundRow * targetRow)
{
    IHqlExpression * record = targetRow->queryRecord();
    IHqlExpression * boundTarget = targetRow->queryBound();
    bool targetIsOwnedRow = hasWrapperModifier(boundTarget->queryType());

    StringBuffer builderName, rowName;
    getUniqueId(builderName.append("b"));
    rowName.append(builderName).append(".row()");

    if (!targetIsOwnedRow && isFixedWidthDataset(record) && !options.alwaysCreateRowBuilder)
    {
        BoundRow * self = bindSelf(ctx, targetRow->queryDataset(), boundTarget, NULL);
        return LINK(self);
    }

    if (targetIsOwnedRow)
    {
        OwnedHqlExpr allocator = createRowAllocator(ctx, record);

        StringBuffer s;
        s.clear().append("RtlDynamicRowBuilder ").append(builderName).append("(");
        generateExprCpp(s, allocator).append(");");
        ctx.addQuoted(s);

        BoundRow * self = bindSelf(ctx, targetRow->queryDataset(), builderName);
        return LINK(self);
    }
    else
    {
        StringBuffer s;
        s.clear().append("RtlStaticRowBuilder ").append(builderName).append("(");
        generateExprCpp(s, boundTarget);
        s.append(",").append(getMaxRecordSize(record)).append(");");
        ctx.addQuoted(s);

        OwnedHqlExpr builder = createVariable(builderName, makeBoolType());
        BoundRow * self = bindSelf(ctx, targetRow->queryDataset(), builderName);
        return LINK(self);
    }
}

IHqlExpression * HqlCppTranslator::declareLinkedRowExpr(BuildCtx & ctx, IHqlExpression * record, bool isMember)
{
    StringBuffer rowName;
    getUniqueId(rowName.append('r'));

    Owned<ITypeInfo> rowType = makeRowType(record->getType());
    rowType.setown(makeAttributeModifier(makeWrapperModifier(rowType.getClear()), getLinkCountedAttr()));
    if (isMember)
        rowType.setown(makeModifier(rowType.getClear(), typemod_member, NULL));

    OwnedHqlExpr boundRow = createVariable(rowName, LINK(rowType));

    //Ugly, but necessary.  Conditional temporary rows will be accessed in a lifetime outside of the scope they are
    //evaluated in - so the declaration needs to be in a scope where they will not be freed.  For the moment make
    //this the outer most scope (within a function)
    ctx.setNextPriority(BuildCtx::OutermostScopePrio);
    ctx.addDeclare(boundRow);
    return boundRow.getClear();
}

BoundRow * HqlCppTranslator::declareLinkedRow(BuildCtx & ctx, IHqlExpression * expr, bool isMember)
{
    assertex(expr->isDatarow());
    OwnedHqlExpr boundRow = declareLinkedRowExpr(ctx, expr->queryRecord(), isMember);
    return createBoundRow(expr, boundRow);
}

BoundRow * HqlCppTranslator::declareStaticRow(BuildCtx & ctx, IHqlExpression * expr)
{
    assertex(expr->isDatarow());

    IHqlExpression * record = expr->queryRecord();
    unsigned maxRecordSize = getMaxRecordSize(record);

    StringBuffer rowName;
    getUniqueId(rowName.append('r'));
    Owned<ITypeInfo> rowType = makeRowType(record->getType());

    BuildCtx * declarectx = &ctx;
    if (maxRecordSize > options.maxLocalRowSize)
        getInvariantMemberContext(ctx, &declarectx, NULL, false, false);

    if (!declarectx->isSameLocation(ctx))
        rowType.setown(makeModifier(rowType.getClear(), typemod_member, NULL));
    else
        declarectx->setNextPriority(BuildCtx::OutermostScopePrio);

    OwnedHqlExpr boundRow = createVariable(rowName, LINK(rowType));
    StringBuffer s;
    declarectx->addQuoted(s.append("byte ").append(rowName).append("[").append(maxRecordSize).append("];"));

    return createBoundRow(expr, boundRow);
}


BoundRow * HqlCppTranslator::declareTempRow(BuildCtx & ctx, BuildCtx & codectx, IHqlExpression * expr)
{
    assertex(expr->isDatarow());
    IHqlExpression * record = expr->queryRecord();

    //if maxRecordSize is too large, and cannot store it in a class, then allocate a pointer to it dynamically.
    unsigned maxRecordSize = getMaxRecordSize(record);
    bool createRowDynamically = tempRowRequiresFinalize(record) || (maxRecordSize > options.maxLocalRowSize);
    if (createRowDynamically)
    {
        return declareLinkedRow(ctx, expr, !ctx.isSameLocation(codectx));
    }
    else
    {
        return declareStaticRow(ctx, expr);
    }
}

BoundRow * HqlCppTranslator::declareTempAnonRow(BuildCtx & ctx, BuildCtx & codectx, IHqlExpression * record)
{
    OwnedHqlExpr anon = createRow(no_self, LINK(record->queryRecord()), createUniqueId());
    return declareTempRow(ctx, codectx, anon);
}



void HqlCppTranslator::finalizeTempRow(BuildCtx & ctx, BoundRow * row, BoundRow * builder)
{
    IHqlExpression * targetRow = row->queryBound();
    bool targetIsOwnedRow = hasWrapperModifier(targetRow->queryType());

    if (builder->queryBuilder() && targetIsOwnedRow)
    {
        OwnedHqlExpr createdRowSize = getRecordSize(builder->querySelector());
        HqlExprArray args;
        args.append(*LINK(builder->queryBuilder()));
        args.append(*LINK(createdRowSize));
        OwnedHqlExpr call = bindFunctionCall(finalizeRowClearId, args, targetRow->queryType());
        CHqlBoundTarget target;
        target.expr.set(targetRow);
        buildExprAssign(ctx, target, call);

        CHqlBoundExpr bound;
        buildExpr(ctx, createdRowSize, bound);
        OwnedHqlExpr sizeofTarget = createSizeof(row->querySelector());
        ctx.associateExpr(sizeofTarget, bound);

    }
    ctx.removeAssociation(builder);
}


//---------------------------------------------------------------------------

bool GlobalFileTracker::checkMatch(IHqlExpression * searchFilename)
{
    if (searchFilename->queryBody() == filename.get())
    {
        usageCount++;
        return true;
    }
    return false;
}

void GlobalFileTracker::writeToGraph()
{
    if (usageCount && graphNode)
        addGraphAttributeInt(graphNode, "_globalUsageCount", usageCount);
}

//---------------------------------------------------------------------------

MetaInstance::MetaInstance(HqlCppTranslator & translator, IHqlExpression * _record, bool _isGrouped)
{
    setMeta(translator, _record, _isGrouped);
}

void MetaInstance::setMeta(HqlCppTranslator & translator, IHqlExpression * _record, bool _isGrouped)
{
    record = _record;
    grouped = _isGrouped;
    assertex(!record || record->getOperator() == no_record);

    searchKey.setown(::getMetaUniqueKey(record, grouped));

    StringBuffer s,recordBase;
    appendUniqueId(recordBase, translator.getConsistentUID(searchKey));

    metaName.set(s.clear().append("mi").append(recordBase).str());
    instanceName.set(s.clear().append("mx").append(recordBase).str());

    //MORE: This function is only used by the fvsource code - getResultRecordSizeEntry
    //It seems a bit of a waste generating it for something used infrequently.
    metaFactoryName.set(s.clear().append("mf").append(recordBase).str());
}


//---------------------------------------------------------------------------

unsigned LocationArray::findLocation(IHqlExpression * location)
{
    ISourcePath * sourcePath = location->querySourcePath();
    unsigned line = location->getStartLine();
    ForEachItem(i)
    {
        IHqlExpression & cur = item(i);
        if ((cur.querySourcePath() == sourcePath) && (cur.getStartLine() == line))
            return i;
    }
    return NotFound;
}

bool LocationArray::queryNewLocation(IHqlExpression * location)
{
    if (findLocation(location) != NotFound)
        return false;
    append(*LINK(location));
    return true;
}


IHqlDelayedCodeGenerator * ABoundActivity::createOutputCountCallback()
{
    return new DelayedUnsignedGenerator(outputCount);
}

enum { createPrio = 1000, inputPrio = 3000, readyPrio = 4000, goPrio = 5000, donePrio = 7000, destroyPrio = 9000 };

ActivityInstance::ActivityInstance(HqlCppTranslator & _translator, BuildCtx & ctx, ThorActivityKind _kind, IHqlExpression * _dataset, const char * _activityArgName) :
    HqlExprAssociation(activeActivityMarkerExpr),
    translator(_translator), classctx(ctx), startctx(ctx), createctx(ctx), nestedctx(ctx), onstartctx(ctx), numChildQueries(0)
{
    dataset.set(_dataset);
    kind = _kind;

    node_operator op = dataset->getOperator();
    isGrouped = isGroupedActivity(dataset);
    isLocal = !isGrouped && isLocalActivity(dataset) && localChangesActivity(dataset) && !translator.targetHThor();
    implementationClassName = NULL;

    activityArgName.set(_activityArgName);

    IHqlExpression * outputDataset = dataset;
    if (outputDataset->isAction() && (getNumChildTables(outputDataset) == 1))
        outputDataset = dataset->queryChild(0);

    if (translator.targetRoxie())
    {
        if ((op == no_output) && dataset->hasAttribute(_spill_Atom) && queryRealChild(dataset, 1))
            outputDataset = dataset->queryChild(0);
    }
    if ((op == no_setgraphresult) && translator.queryOptions().minimizeActivityClasses)
        outputDataset = dataset->queryChild(0);

    bool removeXpath = dataset->hasAttribute(noXpathAtom) || (op == no_output && translator.queryOptions().removeXpathFromOutput);

    LinkedHqlExpr record = queryRecord(outputDataset);
    if (removeXpath)
        record.setown(removeAttributeFromFields(record, xpathAtom));
    meta.setMeta(translator, record, ::isGrouped(outputDataset));

    activityId = translator.nextActivityId();

    StringBuffer s;
    className.set(s.clear().append("cAc").append(activityId).str());
    factoryName.set(s.clear().append("fAc").append(activityId).str());
    instanceName.set(s.clear().append("iAc").append(activityId).str());
    argsName.set(s.clear().append("oAc").append(activityId).str());

    OwnedHqlExpr boundName = createVariable(instanceName, dataset->getType());
    isMember = false;
    instanceIsLocal = false;
    classStmt = NULL;
    classGroup = NULL;
    classGroupStmt = NULL;
    hasChildActivity = false;
    initialGroupMarker = 0;

    includedInHeader = false;
    isCoLocal = false;
    isNoAccess = false;
    executedRemotely = translator.targetThor();// && !translator.isNeverDistributed(dataset);
    containerActivity = NULL;
    subgraph = queryActiveSubGraph(ctx);
    onCreateStmt = NULL;

    //count index and count disk need to be swapped to the new (much simpler) mechanism
    //until then, they need to be special cased.
    activityLocalisation = GraphNoAccess;
    containerActivity = translator.queryCurrentActivity(ctx);
    parentEvalContext.set(translator.queryEvalContext(ctx));
    parentExtract.set(static_cast<ParentExtract*>(ctx.queryFirstAssociation(AssocExtract)));

    bool optimizeParentAccess = translator.queryOptions().optimizeParentAccess;
    if (parentExtract)
    {
        GraphLocalisation localisation = parentExtract->queryLocalisation();
        activityLocalisation = translator.isAlwaysCoLocal() ? GraphCoLocal : queryActivityLocalisation(dataset, optimizeParentAccess);
        if (activityLocalisation == GraphNoAccess)
            isNoAccess = true;
        else if (activityLocalisation == GraphNeverAccess)
            activityLocalisation = GraphNoAccess;

        if (translator.targetThor() && !translator.insideChildQuery(ctx))
            executedRemotely = true;
        else
            executedRemotely = ((activityLocalisation == GraphNonLocal) || (localisation == GraphRemote));

        isCoLocal = false;
        if (containerActivity && !executedRemotely && (localisation != GraphNonLocal))
        {
            // if we supported GraphNonCoLocal this test would not be needed
            if (activityLocalisation != GraphNoAccess)
                isCoLocal = true;
        }

        //if top level activity within a query library then need to force access to the parent extract
        if (!containerActivity && translator.insideLibrary())
        {
            //there should be no colocal activity (container = null)
            if (activityLocalisation != GraphNoAccess)
                activityLocalisation = GraphNonLocal;
        }
        if (activityLocalisation == GraphNoAccess)
            parentExtract.clear();

        if (isCoLocal)
            colocalMember.setown(createVariable("colocal", makeVoidType()));
    }
    else
    {
        if (executedRemotely)
        {
            GraphLocalisation localisation = queryActivityLocalisation(dataset, optimizeParentAccess);
            if ((kind == TAKsimpleaction) || (localisation == GraphNeverAccess) || (localisation == GraphNoAccess))
                executedRemotely = false;
        }
    }

    if (!parentExtract && (translator.getTargetClusterType() == RoxieCluster))
        executedRemotely = isNonLocal(dataset, false);

    unsigned containerId = 0;
    if (containerActivity)
    {
        containerActivity->hasChildActivity = true;
        containerId = containerActivity->activityId;
    }

    table = new ThorBoundActivity(dataset, boundName, activityId, containerId, translator.curSubGraphId(ctx), kind);
    table->setActive(this);
}

ActivityInstance::~ActivityInstance()
{
    table->setActive(nullptr);
    table->Release();
}


void ActivityInstance::addBaseClass(const char * name, bool needLinkOverride)
{
    baseClassExtra.append(", public ").append(name);
}

ABoundActivity * ActivityInstance::getBoundActivity()
{
    return LINK(table);
}

BuildCtx & ActivityInstance::onlyEvalOnceContext()
{
    return evalContext->onCreate.childctx;
}

bool ActivityInstance::isExternal()
{
    return !isMember && !instanceIsLocal;
}

void ActivityInstance::addAttribute(const char * name, const char * value)
{
    addGraphAttribute(graphNode, name, value);
}

void ActivityInstance::addAttributeInt(const char * name, __int64 value)
{
    addGraphAttributeInt(graphNode, name, value);
}

void ActivityInstance::addAttributeBool(const char * name, bool value, bool alwaysAdd)
{
    addGraphAttributeBool(graphNode, name, value, alwaysAdd);
}

void ActivityInstance::addAttribute(const char * name, IHqlExpression * expr)
{
    StringBuffer temp;
    IValue * value = expr->queryValue();
    assertex(value);
    value->getStringValue(temp);
    addGraphAttribute(graphNode, name, temp);
}

void ActivityInstance::addSignedAttribute(IHqlExpression * signedAttr)
{
    if (signedAttr)
    {
        StringBuffer buf;
        getStringValue(buf, signedAttr->queryChild(0));
        addAttribute("signedBy", buf.str());
    }
}

void ActivityInstance::addLocationAttribute(IHqlExpression * location)
{
    if (!translator.queryOptions().reportLocations || translator.queryOptions().obfuscateOutput)
        return;

    unsigned line = location->getStartLine();
    if (line == 0)
        return;

    if (!locations.queryNewLocation(location))
        return;

    ISourcePath * sourcePath = location->querySourcePath();
    unsigned column = location->getStartColumn();
    StringBuffer s;
    s.append(str(sourcePath)).append("(").append(line);
    if (column)
        s.append(",").append(column);
    s.append(")");
    addAttribute("definition", s.str());
}


void ActivityInstance::addNameAttribute(IHqlExpression * symbol)
{
    if (translator.queryOptions().obfuscateOutput)
        return;

    //Not so sure about adding a location for a named symbol if there are other locations already present....
    //We should probably perform some deduping instead.
    addLocationAttribute(symbol);

    IAtom * name = symbol->queryName();
    if (!name)
        return;

    ForEachItemIn(i, names)
    {
        if (names.item(i).queryName() == name)
            return;
    }
    names.append(*symbol);
    addAttribute("name", str(name));
}

void ActivityInstance::removeAttribute(const char * name)
{
    removeGraphAttribute(graphNode, name);
}

void ActivityInstance::processAnnotation(IHqlExpression * annotation)
{
    switch (annotation->getAnnotationKind())
    {
    case annotate_meta:
        {
            unsigned i=0;
            IHqlExpression * cur;
            while ((cur = annotation->queryAnnotationParameter(i++)) != NULL)
            {
                IAtom * name = cur->queryName();
                if (name == sectionAtom)
                    processSection(cur);
                else if (name == hintAtom)
                    processHints(cur);
            }
        }
    }
}

void ActivityInstance::processAnnotations(IHqlExpression * expr)
{
    ForEachChild(iHint, expr)
    {
        IHqlExpression * cur = expr->queryChild(iHint);
        if ((cur->queryName() == hintAtom) && cur->isAttribute())
            processHints(cur);
    }

    IHqlExpression * cur = expr;
    for (;;)
    {
        IHqlExpression * body = cur->queryBody(true);
        if (cur == body)
            break;
        processAnnotation(cur);
        cur = body;
    }
}


void ActivityInstance::processHint(IHqlExpression * attr)
{
    StringBuffer name;
    StringBuffer value;
    getHintNameValue(attr, name, value);
    IPropertyTree * att = createPTree();
    att->setProp("@name", name);
    att->setProp("@value", value);
    graphNode->addPropTree("hint", att);
}

void ActivityInstance::processSection(IHqlExpression * section)
{
    if (!translator.queryOptions().obfuscateOutput)
    {
        StringBuffer sectionName;
        getStringValue(sectionName, section->queryChild(0));
        addAttribute("section", sectionName);
    }
}

void ActivityInstance::processHints(IHqlExpression * hintAttr)
{
    ForEachChild(i, hintAttr)
        processHint(hintAttr->queryChild(i));
}



void ActivityInstance::changeActivityKind(ThorActivityKind newKind)
{
    kind = newKind;
    if (graphNode)
    {
        removeGraphAttribute(graphNode, "_kind");
        addAttributeInt("_kind", kind);
    }
    if (table)
        table->updateActivityKind(kind);
}


void ActivityInstance::setInternalSink(bool value)
{
    if (value)
        addAttributeBool("_internal", true);
    else
        removeAttribute("_internal");
}


static void getRecordSizeText(StringBuffer & out, IHqlExpression * record)
{
    size32_t minSize = getMinRecordSize(record);
    if (isVariableSizeRecord(record))
    {
        size32_t expectedSize = getExpectedRecordSize(record);
        out.append(minSize).append("..");
        if (maxRecordSizeUsesDefault(record))
            out.append("?");
        else
            out.append(getMaxRecordSize(record, 0));
        out.append("(").append(expectedSize).append(")");
    }
    else
        out.append(minSize);
}

void ActivityInstance::createGraphNode(IPropertyTree * defaultSubGraph, bool alwaysExecuted)
{
    IPropertyTree * parentGraphNode = subgraph ? subgraph->tree.get() : defaultSubGraph;
    if (!parentGraphNode)
        return;

    HqlCppOptions const & options = translator.queryOptions();
    assertex(kind < TAKlast);
    graphNode.set(parentGraphNode->addPropTree("node", createPTree()));

    graphNode->setPropInt64("@id", activityId);

    if (!options.obfuscateOutput)
    {
        StringBuffer label;
        if (isGrouped)
            label.append("Grouped ");
        else if (isLocal)
            label.append("Local ");
        label.append(getActivityText(kind));

        graphNode->setProp("@label", graphLabel ? graphLabel.get() : label.str());
    }

    IHqlExpression * cur = dataset;
    for (;;)
    {
        IHqlExpression * body = cur->queryBody(true);
        if (cur == body)
            break;
        switch (cur->getAnnotationKind())
        {
        case annotate_symbol:
            addNameAttribute(cur);
            break;
        case annotate_location:
            addLocationAttribute(cur);
            break;
        }
        cur = body;
    }

    addAttributeInt("_kind", kind);
    addAttributeBool("grouped", isGrouped);
    addAttributeBool("local", isLocal);

#ifdef _DEBUG
//    assertex(dataset->isAction() == isActivitySink(kind));
#endif
    if (dataset->isAction())
    {
        if (alwaysExecuted)
            markSubGraphAsRoot(parentGraphNode);
        else
            addAttributeBool("_internal", true);
    }

    if (containerActivity)
        addAttributeInt("_parentActivity", containerActivity->activityId);
    if (parentExtract && isCoLocal)
        addAttributeBool("coLocal", true);
    if (isNoAccess)
        addAttributeBool("noAccess", true);
    if (dataset->hasAttribute(parallelAtom))
        addAttributeInt("parallel", getIntValue(queryAttributeChild(dataset, parallelAtom, 0), -1));
    if (hasOrderedAttribute(dataset))
        addAttributeBool("ordered", isOrdered(dataset), true);
    if (dataset->hasAttribute(algorithmAtom))
        addAttribute("algorithm", queryAttributeChild(dataset, algorithmAtom, 0));

    if (!options.obfuscateOutput)
    {
        if (graphEclText.length() == 0)
            toECL(dataset->queryBody(), graphEclText, false, true);

        elideString(graphEclText, MAX_GRAPH_ECL_LENGTH);
        if (options.showEclInGraph)
        {
            if (strcmp(graphEclText.str(), "<>") != 0)
                addAttribute("ecl", graphEclText.str());
        }

        if (options.showSeqInGraph)
        {
            IHqlExpression * selSeq = querySelSeq(dataset);
            if (selSeq)
                addAttributeInt("selSeq", selSeq->querySequenceExtra());
        }


        if (options.showMetaInGraph)
        {
            StringBuffer s;
            if (translator.targetThor())
            {
                IHqlExpression * distribution = queryDistribution(dataset);
                if (distribution && distribution->queryName() != localAtom)
                    addAttribute("metaDistribution", getExprECL(distribution, s.clear(), true).str());
            }

            IHqlExpression * grouping = queryGrouping(dataset);
            if (grouping)
                addAttribute("metaGrouping", getExprECL(grouping, s.clear(), true).str());

            if (translator.targetThor())
            {
                IHqlExpression * globalSortOrder = queryGlobalSortOrder(dataset);
                if (globalSortOrder)
                    addAttribute("metaGlobalSortOrder", getExprECL(globalSortOrder, s.clear(), true).str());
            }

            IHqlExpression * localSortOrder = queryLocalUngroupedSortOrder(dataset);
            if (localSortOrder)
                addAttribute("metaLocalSortOrder", getExprECL(localSortOrder, s.clear(), true).str());

            IHqlExpression * groupSortOrder = queryGroupSortOrder(dataset);
            if (groupSortOrder)
                addAttribute("metaGroupSortOrder", getExprECL(groupSortOrder, s.clear(), true).str());
        }

        if (options.noteRecordSizeInGraph)
        {
            LinkedHqlExpr record = dataset->queryRecord();
            if (!record && (getNumChildTables(dataset) == 1))
                record.set(dataset->queryChild(0)->queryRecord());
            if (record)
            {
                //In Thor the serialized record is the interesting value, so include that in the graph
                if (translator.targetThor())
                    record.setown(getSerializedForm(record, diskAtom));
                StringBuffer temp;
                getRecordSizeText(temp, record);
                addAttribute("recordSize", temp.str());
            }
        }

        if (options.showRecordCountInGraph && !dataset->isAction())
        {
            StringBuffer text;
            getRecordCountText(text, dataset);
            addAttribute("predictedCount", text);
        }
    }

    processAnnotations(dataset);
}

void ActivityInstance::moveDefinitionToHeader()
{
    if (!includedInHeader)
    {
        //remove this class from the c++ file and include it in the header file instead
        includedInHeader = true;
        classGroupStmt->setIncluded(false);
        addAttributeBool("helperinheader", true);

        BuildCtx headerctx(*translator.code, parentHelpersAtom);
        headerctx.addAlias(classStmt);
    }
}

void ActivityInstance::noteChildActivityLocation(IHqlExpression * pass)
{
    if (containerActivity && colocalMember)
        containerActivity->noteChildActivityLocation(pass);

    //A child helper has been generated in a different module => 
    //remove this class from the c++ file and include it in the header file instead
    if (translator.queryOptions().spanMultipleCpp && !includedInHeader && (pass != sourceFileSequence))
        moveDefinitionToHeader();
}

void ActivityInstance::buildPrefix()
{
    const HqlCppOptions & options = translator.queryOptions();
    if (options.generateActivityThresholdCycles != 0)
        startTime = get_cycles_now();

    startDistance = querySearchDistance();
    StringBuffer s;

    sourceFileSequence.setown(getSizetConstant(translator.beginFunctionGetCppIndex(activityId, isChildActivity())));
    if (containerActivity && colocalMember)
        containerActivity->noteChildActivityLocation(sourceFileSequence);

    classctx.set(helperAtom);
    classGroupStmt = classctx.addGroupPass(sourceFileSequence);

    classctx.associate(*this);
    classGroup = classctx.addGroup();

    if (!implementationClassName)
    {
        s.clear().append("struct ").append(className).append(" : public CThor").append(activityArgName).append("Arg").append(baseClassExtra);
        classStmt = classctx.addQuotedCompound(s, ";");

        if (subgraph)
            classctx.associate(*subgraph);

        //Generate functions in the order i) always callable ii) after create iii) after start
        nestedctx.set(classctx);
        nestedctx.addGroup();
        createctx.set(classctx);
        createctx.setNextDestructor();
        createctx.addGroup();
        startctx.set(createctx);
        startctx.setNextDestructor();
        startctx.addGroup();

        createctx.associateExpr(codeContextMarkerExpr, codeContextMarkerExpr);
        evalContext.setown(new ActivityEvalContext(translator, this, parentExtract, parentEvalContext, colocalMember, createctx, startctx));
        classctx.associate(*evalContext);

        //virtual void onCreate(ICodeContext * ctx, IHThorArg * colocalParent, MemoryBuffer * serializedCreate)
        BuildCtx oncreatectx(createctx);
        if (parentExtract && isCoLocal && containerActivity)
        {
            oncreatectx.addQuotedCompoundLiteral("virtual void onCreate(ICodeContext * _ctx, IHThorArg * _colocal, MemoryBuffer * in)");
            oncreatectx.addQuoted(s.clear().append("colocal = (").append(containerActivity->className).append("*)_colocal;"));
        }
        else
        {
            onCreateStmt = oncreatectx.addQuotedCompoundLiteral("virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in)");
        }

        oncreatectx.associateExpr(insideOnCreateMarker, NULL);
        oncreatectx.addQuotedLiteral("ctx = _ctx;");

        evalContext->onCreate.createFunctionStructure(translator, oncreatectx, true, executedRemotely ? "serializeCreateContext" : NULL);
        if (onCreateStmt)
            onCreateStmt->finishedFramework();

        onstartctx.set(startctx);

        if (parentExtract)
        {
            onstartctx.addQuotedCompoundOpt("virtual void onStart(const byte * pe, MemoryBuffer * in)");
            parentExtract->beginChildActivity(startctx, onstartctx, activityLocalisation, colocalMember, false, false, containerActivity);
        }
        else
        {
            onstartctx.addQuotedCompoundOpt("virtual void onStart(const byte *, MemoryBuffer * in)");
        }
        onstartctx.associateExpr(insideOnStartMarker, NULL);

        evalContext->onStart.createFunctionStructure(translator, onstartctx, true, executedRemotely ? "serializeStartContext" : NULL);

        if (colocalMember)
        {
            s.clear().append(containerActivity->className).append(" * colocal;");
            classctx.addQuoted(s);
        }

        if (baseClassExtra.length())
        {
            nestedctx.addQuoted(s.clear().append("virtual void Link() const { CThor").append(activityArgName).append("Arg::Link(); }"));
            nestedctx.addQuoted(s.clear().append("virtual bool Release() const { return CThor").append(activityArgName).append("Arg::Release(); }"));
        }

    //  if (!isMember)
    //      classGroupStmt->setIncomplete(true);

    }
    else
    {
        s.clear().append("// use library for ").append(className);
        classctx.addQuoted(s);
        assertex(isExternal());

        nestedctx.set(classctx);
        createctx.set(classctx);
        startctx.set(createctx);
        initialGroupMarker = classGroup->numChildren();
    }
}


void ActivityInstance::buildSuffix()
{
    if (numChildQueries)
        addAttributeInt("childQueries", numChildQueries);

    //Paranoid check to ensure that library classes aren't used when member functions were required
    if (implementationClassName && (initialGroupMarker != classGroup->numChildren()))
        throwUnexpectedX("Implementation class created, but member functions generated");

    const HqlCppOptions & options = translator.queryOptions();
    if (classStmt && (options.spotComplexClasses || options.showActivitySizeInGraph))
    {
        //NOTE: The peephole optimizer means this is often vastly larger than the actual number of lines generated
        unsigned approxSize = calcTotalChildren(classStmt);
        if (options.spotComplexClasses && (approxSize >= options.complexClassesThreshold))
        {
            if ((options.complexClassesActivityFilter == 0) || (kind == options.complexClassesActivityFilter))
                translator.WARNING2(CategoryEfficiency, HQLWRN_ComplexHelperClass, activityId, approxSize);
        }
        if (!options.obfuscateOutput && options.showActivitySizeInGraph)
            addAttributeInt("approxClassSize", approxSize);
    }

    if (options.generateActivityThresholdCycles != 0)
    {
        cycle_t totalTime = get_cycles_now() - startTime;
        cycle_t localTime = totalTime - nestedTime;
        if (localTime > options.generateActivityThresholdCycles)
        {
            if (containerActivity)
                containerActivity->nestedTime += totalTime;

            unsigned __int64 generateTime = cycle_to_nanosec(localTime);
            //Record as a statistic rather than a graph attribute to avoid stats iterators needing to walk the graph
            //We could also record local and totalTime if they differ - but that would then need another stats kind
            StringBuffer scope;
            getScope(scope);
            translator.wu()->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTactivity, scope, StTimeGenerate, nullptr, generateTime, 1, 0, StatsMergeReplace);
        }
    }

    unsigned __int64 searchDistance = querySearchDistance() - startDistance;
    if (searchDistance > options.searchDistanceThreshold)
        addAttributeInt("searchDistance", searchDistance);

//  if (!isMember)
//      classGroupStmt->setIncomplete(false);

    if (parentExtract)
        parentExtract->endChildActivity();

    classctx.removeAssociation(this);
    locations.kill();
    names.kill();

    if (isExternal())
    {
        BuildCtx globalctx(*translator.code, helperAtom);
        globalctx.addGroupPass(sourceFileSequence);

        if (implementationClassName)
        {
            //Meta is always the last parameter...
            addConstructorMetaParameter();

            StringBuffer s;
            s.append("extern \"C\" ECL_API IHThorArg * ").append(factoryName).append("()");
            globalctx.addQuotedFunction(s, true);
            OwnedHqlExpr call = translator.bindFunctionCall(implementationClassName, constructorArgs);

            //Don't call buildReturn because we're lying about the return type, and we don't want a boolean temporary created.
            CHqlBoundExpr bound;
            translator.buildExpr(globalctx, call, bound);
            globalctx.addReturn(bound.expr);
            //translator.buildReturn(globalctx, call);
        }
        else
        {
            StringBuffer s;
            s.append("extern \"C\" ECL_API IHThorArg * ").append(factoryName).append("() { return new ").append(className).append("; }");
            globalctx.addQuoted(s);
        }
    }
}

void ActivityInstance::buildMetaMember()
{
    if (implementationClassName)
        return;

    translator.buildMetaInfo(meta);

    if (meta.queryRecord())
    {
        StringBuffer s;
        s.append("virtual IOutputMetaData * queryOutputMeta() { return &").append(meta.queryInstanceObject()).append("; }");
        classctx.addQuoted(s);
    }
}


void ActivityInstance::getScope(StringBuffer & scope) const
{
    if (containerActivity)
    {
        containerActivity->getScope(scope);
        scope.append(":");
    }
    else
    {
        scope.append(WorkflowScopePrefix).append(translator.curWfid).append(":");
        if (translator.activeGraph)
            scope.append(translator.activeGraph->name).append(":");
    }

    if (subgraph)
    {
        if (subgraph->graphId)
            scope.append(ChildGraphScopePrefix).append(subgraph->graphId).append(":");
        scope.append(SubGraphScopePrefix).append(subgraph->id).append(":");
    }
    scope.append(ActivityScopePrefix).append(activityId);
}

void ActivityInstance::addConstructorMetaParameter()
{
    translator.buildMetaInfo(meta);

    if (meta.queryRecord())
    {
        StringBuffer s;
        s.append("&").append(meta.queryInstanceObject());

        OwnedHqlExpr metaExpr = createQuoted(s.str(), makeBoolType());
        constructorArgs.append(*metaExpr.getClear());
    }
    else if ((kind == TAKwhen_action) || (kind == TAKemptyaction))
    {
        constructorArgs.append(*createTranslatedOwned(createValue(no_nullptr, makeBoolType())));
    }
}

ParentExtract * ActivityInstance::createNestedExtract()
{
    if (!nestedExtract)
    {
        nestedExtract.setown(new ParentExtract(translator, PETnested, NULL, GraphCoLocal, evalContext));
        nestedExtract->beginNestedExtract(startctx);
    }
    return LINK(nestedExtract);
}


//----------------------------------------------------------------------------------------------------

StringBuffer &expandLiteral(StringBuffer &s, const char *f)
{
    const char * startLine = f;
    char c;
    s.append('"');
    while ((c = *f++) != 0)
    {
        switch (c)
        {
        case '\t':
            s.append("\\t");
            break;
        case '\r':
            s.append("\\r");
            break;
        case '\n':
            s.append("\\n\"\n\t\t\"");
            startLine = f;
            break;
        case ',':
            s.append(c);
            if (f - startLine > 60)
            {
                s.append("\"\n\t\t\"");
                startLine = f;
            }
            break;
        case '\\':
        case '\"':
        case '\'':
            s.append('\\');
            // fall into...
        default:
            s.append(c);
            break;
        }
        if (f - startLine > 120)
        {
            s.append("\"\n\t\t\"");
            startLine = f;
        }
    }
    return s.append("\"");
}

//---------------------------------------------------------------------------

ReferenceSelector::ReferenceSelector(HqlCppTranslator & _translator) : translator(_translator)
{
}

//---------------------------------------------------------------------------

/* In parms: _dataset, _path are NOT linked */
DatasetSelector::DatasetSelector(HqlCppTranslator & _translator, BoundRow * _row, IHqlExpression * _path)
: ReferenceSelector(_translator)
{
    row = LINK(_row);
    matchedDataset = false;
    column = row->queryRootColumn();
    column->Link();
    path.set(_path);
    if (!_path)
        path.set(row->querySelector());
    parent = NULL;
}

/* All in parms: NOT linked */
DatasetSelector::DatasetSelector(DatasetSelector * _parent, BoundRow * _row, AColumnInfo * _column, IHqlExpression * _path)
: ReferenceSelector(_parent->translator)
{
    parent = LINK(_parent);
    matchedDataset = _parent->matchedDataset;
    column = LINK(_column);
    path.set(_path);
    row = LINK(_row);
}

DatasetSelector::~DatasetSelector()
{
    ::Release(column);
    ::Release(parent);
    ::Release(row);
}

void DatasetSelector::assignTo(BuildCtx & ctx, const CHqlBoundTarget & target)
{
    OwnedHqlExpr mapped = row->getMappedSelector(ctx, this);
    if (mapped)
        translator.buildExprAssign(ctx, target, mapped);
    else
        column->buildAssign(translator, ctx, this, target);
}

void DatasetSelector::buildAddress(BuildCtx & ctx, CHqlBoundExpr & target)
{
    OwnedHqlExpr mapped = row->getMappedSelector(ctx, this);
    if (mapped)
        translator.buildAddress(ctx, mapped, target);
    else
        column->buildAddress(translator, ctx, this, target);
}

void DatasetSelector::buildClear(BuildCtx & ctx, int direction)
{
    assertex(row->isModifyable());
    column->buildClear(translator, ctx, this, direction);
}

bool DatasetSelector::isBinary()
{
    return row->isBinary();
}

bool DatasetSelector::isRoot()
{
    return parent == NULL;
}

/* _newCursor, _newColumn: not linked. newPathOwn: linked */
DatasetSelector * DatasetSelector::createChild(BoundRow * _newCursor, AColumnInfo * newColumn, IHqlExpression * newPath)
{
    return new DatasetSelector(this, _newCursor, newColumn, newPath);
}

void DatasetSelector::get(BuildCtx & ctx, CHqlBoundExpr & bound)
{
    OwnedHqlExpr mapped = row->getMappedSelector(ctx, this);
    if (mapped)
        translator.buildAnyExpr(ctx, mapped, bound);
    else
        column->buildExpr(translator, ctx, this, bound);
}

void DatasetSelector::getOffset(BuildCtx & ctx, CHqlBoundExpr & bound)
{
    //OwnedHqlExpr mapped = row->getMappedSelector(ctx, this);
    assertex(!row->isNonLocal());
    column->buildOffset(translator, ctx, this, bound);
}

size32_t DatasetSelector::getContainerTrailingFixed()
{
    assertex(!row->isNonLocal());
    return column->getContainerTrailingFixed();
}

void DatasetSelector::getSize(BuildCtx & ctx, CHqlBoundExpr & bound)
{
    //OwnedHqlExpr mapped = row->getMappedSelector(ctx, this);
    assertex(!row->isNonLocal());
    column->buildSizeOf(translator, ctx, this, bound);
}

bool DatasetSelector::isDataset()
{
    switch (column->queryType()->getTypeCode())
    {
    case type_table:
    case type_groupedtable:
        return true;
    }
    return false;
}

bool DatasetSelector::isConditional()
{
    return row->isConditional() || column->isConditional();
}

AColumnInfo * DatasetSelector::queryColumn()
{
    return column;
}

BoundRow * DatasetSelector::queryRootRow()
{
    return row;
}

IHqlExpression * DatasetSelector::queryExpr()
{
    return path;
}

ITypeInfo * DatasetSelector::queryType()
{
    if (parent)
        return path->queryType();
    return row->queryDataset()->queryType();
}

IHqlExpression * DatasetSelector::resolveChildDataset(IHqlExpression * searchDataset) const
{
    searchDataset = searchDataset->queryNormalizedSelector();
    IHqlExpression * compare = row->queryDataset()->queryNormalizedSelector();
    if (searchDataset == compare)
        return compare;
    return NULL;
}

AColumnInfo * DatasetSelector::resolveField(IHqlExpression * search) const
{
    AColumnInfo * nextColumn = column->lookupColumn(search);
    if (nextColumn)
        return nextColumn;

    return NULL;
}


void DatasetSelector::set(BuildCtx & ctx, IHqlExpression * expr)
{
    while (expr->getOperator() == no_compound)
    {
        translator.buildStmt(ctx, expr->queryChild(0));
        expr = expr->queryChild(1);
    }

    assertex(row->isModifyable());
    ITypeInfo * type = column->queryType();
    if (!hasReferenceModifier(type))
    {
        switch (type->getTypeCode())
        {
        case type_row:
            {
                if (queryRecordType(queryType()) == expr->queryRecordType())
                {
                    translator.buildRowAssign(ctx, this, expr);
                }
                else
                {
                    Owned<IReferenceSelector> sourceRef = translator.buildNewRow(ctx, expr);
                    setRow(ctx, sourceRef);
                }
                return;
            }
        }
    }

    column->setColumn(translator, ctx, this, expr);
}


void DatasetSelector::setRow(BuildCtx & ctx, IReferenceSelector * rhs)
{
    assertex(row->isModifyable());
    column->setRow(translator, ctx, this, rhs);
}

void DatasetSelector::modifyOp(BuildCtx & ctx, IHqlExpression * expr, node_operator op)
{
    assertex(row->isModifyable());
//  IReferenceSelector * aliasedSelector = row->queryAlias();
//  assertex(aliasedSelector);
    if (column->modifyColumn(translator, ctx, this, expr, op))
        return;

//  OwnedHqlExpr sourceValue = aliasedSelector->queryRootRow()->bindToRow(path, row->querySelector());
    OwnedHqlExpr sourceValue = LINK(path);
    OwnedHqlExpr result;
    switch (op)
    {
    case no_assign_addfiles:
        result.setown(createDataset(no_addfiles, ensureOwned(sourceValue), LINK(expr)));
        break;
#ifdef _THE_FOLLOWING_ARENT_YET_IMPLEMENTED_BUT_WOULD_BE_USEFUL
    case no_assign_concat:
        result.setown(createValue(no_concat, path->getType(), LINK(sourceValue), LINK(expr)));
        break;
    case no_assign_add:
        result.setown(createValue(no_add, path->getType(), LINK(sourceValue), LINK(expr)));
        break;
#endif
    default:
        throwError1(HQLERR_UnknownCompoundAssign, getOpString(op));
    }

    set(ctx, result);
}


void DatasetSelector::buildDeserialize(BuildCtx & ctx, IHqlExpression * helper, IAtom * serializeForm)
{
    column->buildDeserialize(translator, ctx, this, helper, serializeForm);
}


void DatasetSelector::buildSerialize(BuildCtx & ctx, IHqlExpression * helper, IAtom * serializeForm)
{
    column->buildSerialize(translator, ctx, this, helper, serializeForm);
}

/* In selector: not linked. Return: linked */
IReferenceSelector * DatasetSelector::select(BuildCtx & ctx, IHqlExpression * selectExpr)
{
//  assertex(!isDataset());

    OwnedHqlExpr selected;
    //Optimize so don't create so many select expressions that already exist.
    if (selectExpr->getOperator() != no_select)
        selected.setown(createSelectExpr(LINK(path), LINK(selectExpr)));
    else if (selectExpr->queryChild(0) == path)
        selected.set(selectExpr);
    else
        selected.setown(createSelectExpr(LINK(path), LINK(selectExpr->queryChild(1))));

    IHqlExpression * selectedField = selected->queryChild(1);
    AColumnInfo * newColumn = resolveField(selectedField);
    if (!newColumn)
    {
        //could be a dataset selector
        IHqlExpression * selected = NULL;
        if (!matchedDataset)
            selected = resolveChildDataset(selectedField);

        if (!selected)
        {
#ifdef TraceTableFields
            IHqlExpression * searchDataset = row->queryDataset();       // MORE what when children selected?
            IHqlExpression * record = searchDataset->queryRecord()->queryBody();
            unsigned numFields = record->numChildren();
            unsigned idxc;
            StringBuffer fields;
            PrintLog("Fields:");
            for (idxc = 0; idxc < numFields; idxc++)
            {
                IHqlExpression * field = record->queryChild(idxc);
                IAtom * name = field->queryName();
                fields.clear();
                fields.appendf("        %20s [@%lx := %lx] ", name->str(), field, field->queryChild(0));
                PrintLog(fields.str());
            }
            PrintLog("Search: %20s [@%lx])", selectedField->queryName()->str(),selectedField);
#endif

            StringBuffer searchName, datasetName;

            selectedField->toString(searchName);
            if (path->queryName())
                datasetName.append(path->queryName());
            else
                path->toString(datasetName);

            throwError2(HQLERR_XDoesNotContainExpressionY, datasetName.str(), searchName.str());
        }

        DatasetSelector * next = createChild(row, column, selected);
        next->matchedDataset = true;
        return next;
    }

    return createChild(row, newColumn, selected);
}

BoundRow * DatasetSelector::getRow(BuildCtx & ctx)
{
    if (isRoot())
        return LINK(row);
    CHqlBoundExpr bound;
    buildAddress(ctx, bound);
    Owned<ITypeInfo> type = makeReferenceModifier(LINK(queryType()));
    OwnedHqlExpr address = createValue(no_implicitcast, type.getClear(), LINK(bound.expr));
    return translator.createBoundRow(path, address);
}

IReferenceSelector * HqlCppTranslator::createSelfSelect(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr, IHqlExpression * rootSelector)
{
    if (expr == rootSelector)
        return LINK(target);
    assertex(expr->getOperator() == no_select);
    Owned<IReferenceSelector> parent = createSelfSelect(ctx, target, expr->queryChild(0), rootSelector);
    return parent->select(ctx, expr);
}


void initBoundStringTarget(CHqlBoundTarget & target, ITypeInfo * type, const char * lenName, const char * dataName)
{
    if (type->getSize() == UNKNOWN_LENGTH)
        target.length.setown(createVariable(lenName, LINK(sizetType)));
    target.expr.setown(createVariable(dataName, makeReferenceModifier(LINK(type))));
}

//---------------------------------------------------------------------------

GlobalClassBuilder::GlobalClassBuilder(HqlCppTranslator & _translator, BuildCtx & _ctx, const char * _className, const char * _baseName, const char * _accessorInterface)
: translator(_translator), classctx(_ctx), nestedctx(_ctx), startctx(_ctx), createctx(_ctx)
{
    className.set(_className);
    baseName.set(_baseName);
    accessorInterface.set(_accessorInterface);
    if (accessorInterface)
    {
        StringBuffer s;
        accessorName.set(s.clear().append("cr").append(className).str());
    }
    onCreateStmt = NULL;
    classStmt = NULL;
}

void GlobalClassBuilder::buildClass(unsigned priority)
{
    StringBuffer s;
    s.append("struct ").append(className);
    if (baseName)
        s.append(" : public ").append(baseName);

    classctx.set(declareAtom);
    if (priority)
        classctx.setNextPriority(priority);
    classStmt = classctx.addQuotedCompound(s, ";");
    if (!baseName)
        classctx.addQuotedLiteral("ICodeContext * ctx;");
    classctx.associateExpr(codeContextMarkerExpr, codeContextMarkerExpr);

    //Generate functions in the order i) always callable ii) after create iii) after start
    nestedctx.set(classctx);
    nestedctx.addGroup();
    createctx.set(classctx);
    createctx.setNextDestructor();
    createctx.addGroup();
    startctx.set(createctx);

    evalContext.setown(new GlobalClassEvalContext(translator, parentExtract, parentEvalContext, createctx, startctx));
    classctx.associate(*evalContext);

    //virtual void onCreate(ICodeContext * ctx, IHThorArg * colocalParent, MemoryBuffer * serializedCreate)
    BuildCtx oncreatectx(createctx);
    onCreateStmt = oncreatectx.addQuotedCompoundLiteral("void onCreate(ICodeContext * _ctx)");
    oncreatectx.associateExpr(insideOnCreateMarker, NULL);
    oncreatectx.addQuotedLiteral("ctx = _ctx;");

    evalContext->onCreate.createFunctionStructure(translator, oncreatectx, true, NULL);
    onCreateStmt->finishedFramework();
}

void GlobalClassBuilder::completeClass(unsigned priority)
{
    //MORE: This should be generated from a system function prototype somehow - so we can extend it to user functions later.
    //arguments and parameters should also be configured similarly.
    if (accessorInterface)
    {
        StringBuffer s, prototype;
        prototype.append("extern ECL_API ").append(accessorInterface).append(" * ").append(accessorName).append("(ICodeContext * ctx, unsigned activityId)");

        BuildCtx accessctx(classctx);
        accessctx.set(declareAtom);
        if (priority)
            accessctx.setNextPriority(priority);
        accessctx.addQuotedFunction(prototype, true);
        accessctx.addQuoted(s.clear().append(className).append("* p = new ").append(className).append("(activityId); "));
        accessctx.addQuotedLiteral("p->onCreate(ctx);");
        accessctx.addQuotedLiteral("return p;");

        if (translator.queryOptions().spanMultipleCpp)
        {
            BuildCtx protoctx(*translator.queryCode(), mainprototypesAtom);
            protoctx.addQuoted(s.clear().append(prototype).append(";"));
        }
    }
}

//---------------------------------------------------------------------------

static bool expandThisPass(IHqlExpression * name, unsigned pass)
{
    if (!name)
        return pass == 1;
    StringBuffer temp;
    name->queryValue()->getStringValue(temp);
    if (temp.length() && temp.charAt(0) =='@')
        return pass == 0;
    return pass == 1;
}

static bool anyXmlGeneratedForPass(IHqlExpression * expr, unsigned pass)
{
    switch (expr->getOperator())
    {
    case no_field:
        {
            OwnedHqlExpr name;
            extractXmlName(name, NULL, NULL, expr, NULL, false);

            ITypeInfo * type = expr->queryType()->queryPromotedType();
            switch (type->getTypeCode())
            {
            case type_row:
                if (name)
                    return (pass == 1);
                return anyXmlGeneratedForPass(queryRecord(type), pass);
            case type_set:
                return (pass==1);
            case type_dictionary:
            case type_table:
            case type_groupedtable:
                return (pass == 1);
            default:
                return expandThisPass(name, pass);
            }
            break;
        }
    case no_ifblock:
        return anyXmlGeneratedForPass(expr->queryChild(1), pass);
    case no_record:
        {
            ForEachChild(idx, expr)
                if (anyXmlGeneratedForPass(expr->queryChild(idx), pass))
                    return true;
            return false;
        }
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        return false;
    default:
        UNIMPLEMENTED;
    }
    throwUnexpected(); // unreachable, but some compilers will complain about missing return
}


//---------------------------------------------------------------------------

bool HqlCppTranslator::insideOnCreate(BuildCtx & ctx)
{
    return ctx.queryMatchExpr(insideOnCreateMarker) != NULL;
}

bool HqlCppTranslator::insideOnStart(BuildCtx & ctx)
{
    return ctx.queryMatchExpr(insideOnStartMarker) != NULL;
}

bool HqlCppTranslator::getInvariantMemberContext(BuildCtx & ctx, BuildCtx * * declarectx, BuildCtx * * initctx, bool isIndependentMaybeShared, bool invariantEachStart)
{
    EvalContext * instance = queryEvalContext(ctx);
    if (instance)
        return instance->getInvariantMemberContext(&ctx, declarectx, initctx, isIndependentMaybeShared, invariantEachStart);
    return false;
}

void getMemberClassName(StringBuffer & className, const char * member)
{
    className.append((char)toupper(member[0])).append(member+1).append("Class");
}

IHqlStmt * HqlCppTranslator::beginNestedClass(BuildCtx & ctx, const char * member, const char * bases, const char * memberExtra, ParentExtract * extract)
{
//  ActivityInstance * activity = queryCurrentActivity(ctx);
//  Owned<ParentExtract> nestedUse;
//  if (activity)
//      nestedUse.setown(extract ? LINK(extract) : activity->createNestedExtract());

    StringBuffer begin,end;
    StringBuffer className;
    getMemberClassName(className, member);

    begin.append("struct ").append((char)toupper(member[0])).append(member+1).append("Class");
    if (bases)
        begin.append(" : public ").append(bases);
    end.append(" ").append(member).append(memberExtra).append(";");

    IHqlStmt * stmt = ctx.addQuotedCompound(begin.str(), end.str());
    stmt->setIncomplete(true);

    OwnedHqlExpr colocalName = createVariable("activity", makeVoidType());
    ActivityInstance * activity = queryCurrentActivity(ctx);
    if (activity)
    {
        Owned<ParentExtract> nestedUse = extract ? LINK(extract) : activity->createNestedExtract();
        NestedEvalContext * nested = new NestedEvalContext(*this, member, nestedUse, queryEvalContext(ctx), colocalName, ctx, ctx);
        ctx.associateOwn(*nested);
        nested->initContext();
    }
    return stmt;
}

void HqlCppTranslator::endNestedClass(IHqlStmt * stmt)
{
    stmt->setIncomplete(false);
}

void HqlCppTranslator::doBuildFunctionReturn(BuildCtx & ctx, ITypeInfo * type, IHqlExpression * value)
{
    bool returnByReference = false;
    CHqlBoundTarget target;
    OwnedHqlExpr returnValue;

    switch (type->getTypeCode())
    {
    case type_varstring:
    case type_varunicode:
        if (type->getSize() == UNKNOWN_LENGTH)
            break;
        //fall through
    case type_qstring:
    case type_string:
    case type_data:
    case type_unicode:
    case type_utf8:
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        if (!hasStreamedModifier(type))
        {
            initBoundStringTarget(target, type, "__lenResult", "__result");
            returnByReference = true;
        }
        break;
    case type_row:
        if (!hasLinkCountedModifier(type))
        {
            initBoundStringTarget(target, type, "__lenResult", "__result");
            returnByReference = true;
        }
        break;
    case type_transform:
        {
            OwnedHqlExpr dataset = createDataset(no_anon, LINK(::queryRecord(type)));
            BoundRow * row = bindSelf(ctx, dataset, "__self");
            target.expr.set(row->querySelector());
            returnByReference = true;
            //A transform also returns the size that was generated (which will be bound to a local variable)
            returnValue.setown(getRecordSize(row->querySelector()));
            break;
        }
    case type_set:
        target.isAll.setown(createVariable("__isAllResult", makeBoolType()));
        target.length.setown(createVariable("__lenResult", LINK(sizetType)));
        target.expr.setown(createVariable("__result", makeReferenceModifier(LINK(type))));
        returnByReference = true;
        break;
    }

    if (returnByReference)
    {
        buildExprAssign(ctx, target, value);
        if (returnValue)
            buildReturn(ctx, returnValue);
    }
    else
        buildReturn(ctx, value, type);
}


//MORE: Should have a generalized doBuildFunctionHeader(ctx, name, retType, HqlExprArray & parameters)
//      for when we generate functions for aliases

void HqlCppTranslator::doBuildBoolFunction(BuildCtx & ctx, const char * name, bool value)
{
    StringBuffer s;
    s.append("virtual bool ").append(name).append("() { return ").append(TF[value]).append("; }");
    ctx.addQuoted(s);
}


void HqlCppTranslator::doBuildDataFunction(BuildCtx & ctx, const char * name, IHqlExpression * value)
{
    doBuildFunction(ctx, unknownDataType, name, value);
}

void HqlCppTranslator::doBuildStringFunction(BuildCtx & ctx, const char * name, IHqlExpression * value)
{
    doBuildFunction(ctx, unknownStringType, name, value);
}

void HqlCppTranslator::doBuildVarStringFunction(BuildCtx & ctx, const char * name, IHqlExpression * value)
{
    StringBuffer proto;
    proto.append("virtual const char * ").append(name).append("()");

    MemberFunction func(*this, ctx, proto, MFdynamicproto);
    if (value)
        buildReturn(func.ctx, value, constUnknownVarStringType);
    else
        func.ctx.addReturn(queryQuotedNullExpr());
}

void HqlCppTranslator::doBuildBoolFunction(BuildCtx & ctx, const char * name, IHqlExpression * value)
{
    doBuildFunction(ctx, queryBoolType(), name, value);
}

void HqlCppTranslator::doBuildUnsignedFunction(BuildCtx & ctx, const char * name, unsigned value)
{
    StringBuffer s;
    s.append("virtual unsigned ").append(name).append("() { return ").append(value).append("; }");
    ctx.addQuoted(s);
}

void HqlCppTranslator::doBuildSizetFunction(BuildCtx & ctx, const char * name, size32_t value)
{
    StringBuffer s;
    s.append("virtual size32_t ").append(name).append("() { return ").append(value).append("; }");
    ctx.addQuoted(s);
}

void HqlCppTranslator::doBuildUnsignedFunction(BuildCtx & ctx, const char * name, IHqlExpression * value)
{
    doBuildFunction(ctx, unsignedType, name, value);
}

void HqlCppTranslator::doBuildDoubleFunction(BuildCtx & ctx, const char * name, IHqlExpression * value)
{
    Owned<ITypeInfo> type = makeRealType(8);
    doBuildFunction(ctx, doubleType, name, value);
}

void HqlCppTranslator::doBuildUnsigned64Function(BuildCtx & ctx, const char * name, IHqlExpression * value)
{
    Owned<ITypeInfo> type = makeIntType(8, false);
    doBuildFunction(ctx, type, name, value);
}

void HqlCppTranslator::doBuildUnsignedFunction(BuildCtx & ctx, const char * name, const char * value)
{
    if (value)
    {
        StringBuffer s;
        s.append("virtual unsigned ").append(name).append("() { return ").append(value).append("; }");
        ctx.addQuoted(s);
    }
}

void HqlCppTranslator::doBuildSignedFunction(BuildCtx & ctx, const char * name, IHqlExpression * value)
{
    doBuildFunction(ctx, signedType, name, value);
}

void HqlCppTranslator::doBuildFunction(BuildCtx & ctx, ITypeInfo * type, const char * name, IHqlExpression * value)
{
    if (value)
    {
        LinkedHqlExpr cseValue = value;
        if (options.spotCSE)
            cseValue.setown(spotScalarCSE(cseValue, NULL, queryOptions().spotCseInIfDatasetConditions));

        if (false)
        {
            HqlExprArray parameters;
            OwnedHqlExpr entrypoint = createAttribute(entrypointAtom, createConstant(name));
            OwnedHqlExpr body = createValue(no_null, LINK(type), LINK(entrypoint));
            OwnedHqlExpr formals = createSortList(parameters);
            OwnedHqlExpr attrs = createAttribute(virtualAtom);
            OwnedHqlExpr function = createFunctionDefinition(NULL, LINK(body), LINK(formals), NULL, LINK(attrs));

            BuildCtx funcctx(ctx);
            funcctx.addFunction(function);
            doBuildFunctionReturn(funcctx, type, cseValue);
        }
        else
        {
            StringBuffer s, returnParameters;
            s.append("virtual ");
            generateFunctionReturnType(s, returnParameters, type, NULL, options.targetCompiler);
            s.append(" ").append(name).append("(").append(returnParameters).append(")");

            MemberFunction func(*this, ctx, s, MFdynamicproto);
            doBuildFunctionReturn(func.ctx, type, cseValue);
        }
    }
}

void HqlCppTranslator::addFilenameConstructorParameter(ActivityInstance & instance, const char * name, IHqlExpression * expr)
{
    OwnedHqlExpr folded = foldHqlExpression(expr);
    instance.addConstructorParameter(folded);
    noteFilename(instance, name, folded, false);
}

void HqlCppTranslator::buildFilenameFunction(ActivityInstance & instance, BuildCtx & classctx, const char * name, IHqlExpression * expr, bool isDynamic)
{
    OwnedHqlExpr folded = foldHqlExpression(expr);
    doBuildVarStringFunction(classctx, name, folded);
    noteFilename(instance, name, folded, isDynamic);
}

void HqlCppTranslator::noteFilename(ActivityInstance & instance, const char * name, IHqlExpression * expr, bool isDynamic)
{
    if (options.addFilesnamesToGraph)
    {
        StringBuffer propName;
        const char * propNameBase = name;
        if (memicmp(propNameBase, "get", 3) == 0)
            propNameBase += 3;
        else if (memicmp(propNameBase, "query", 3) == 0)
            propNameBase += 5;
        propName.append("_").append((char)tolower(*propNameBase)).append(propNameBase+1);

        OwnedHqlExpr folded = foldHqlExpression(expr);
        if (folded)
        {
            if (!folded->queryValue())
            {
                if (!isDynamic && !options.allowVariableRoxieFilenames && targetRoxie())
                {
                    StringBuffer x;
                    folded->toString(x);
                    throwError1(HQLERR_RoxieExpectedConstantFilename, x.str());
                }
            }
            else
            {
                StringBuffer propValue;
                folded->queryValue()->getStringValue(propValue);
                instance.addAttribute(propName, propValue);
            }
        }
        if (isDynamic)
        {
            unsigned len = propName.length();
            if ((len > 4) && (memicmp(propName.str() + len-4, "name", 4) == 0))
                propName.setLength(len-4);
            propName.append("_dynamic");
            instance.addAttributeBool(propName.str(), true);
        }
    }
}

void HqlCppTranslator::buildRefFilenameFunction(ActivityInstance & instance, BuildCtx & classctx, const char * name, IHqlExpression * expr)
{
    IHqlExpression * table = queryPhysicalRootTable(expr);
    assertex(table);

    IHqlExpression * filename = NULL;
    switch (table->getOperator())
    {
    case no_keyindex:
        filename = table->queryChild(2);
        break;
    case no_newkeyindex:
        filename = table->queryChild(3);
        break;
    case no_table:
        filename = table->queryChild(0);
        break;
    }

    buildFilenameFunction(instance, classctx, name, filename, hasDynamicFilename(table));
}

void HqlCppTranslator::buildConnectInputOutput(BuildCtx & ctx, ActivityInstance * instance, ABoundActivity * table, unsigned outputIndex, unsigned inputIndex, const char * label, bool nWay)
{
#ifdef _GATHER_USAGE_STATS
    activityCounts[table->queryActivityKind()][instance->kind]++;
#endif
    outputIndex = table->nextOutputCount();
    logGraphEdge(instance->querySubgraphNode(), table->queryActivityId(), instance->activityId, outputIndex, inputIndex, label, nWay);
}

void HqlCppTranslator::buildInstancePrefix(ActivityInstance * instance)
{
    instance->buildPrefix();

    activeActivities.append(*instance->getBoundActivity());
}


void HqlCppTranslator::buildInstanceSuffix(ActivityInstance * instance)
{
    instance->buildMetaMember();
    instance->buildSuffix();

    activeActivities.pop();
}


IHqlExpression * HqlCppTranslator::getRecordSize(IHqlExpression * dataset)
{
    IHqlExpression * ds = dataset->queryNormalizedSelector();
    return createValue(no_sizeof, makeIntType(4, false), ensureActiveRow(ds));
}

/* In parms: not linked */
void HqlCppTranslator::getRecordSize(BuildCtx & ctx, IHqlExpression * dataset, CHqlBoundExpr & bound)
{
    OwnedHqlExpr size = getRecordSize(dataset);
    buildExpr(ctx, size, bound);
}

unsigned HqlCppTranslator::getMaxRecordSize(IHqlExpression * record)
{
    if (!record)
        return 0;
    return ::getMaxRecordSize(record, options.maxRecordSize);
}

unsigned HqlCppTranslator::getCsvMaxLength(IHqlExpression * csvAttr)
{
    if (options.testIgnoreMaxLength)
        return 1;
    HqlExprArray attrs;
    if (csvAttr)
    {
        ForEachChild(idx, csvAttr)
            csvAttr->queryChild(idx)->unwindList(attrs, no_comma);
    }
    IHqlExpression * maxLength = queryAttribute(maxLengthAtom, attrs);
    if (maxLength)
        return (unsigned)getIntValue(maxLength->queryChild(0), 0);
    return MAX_CSV_RECORD_SIZE;
}


bool HqlCppTranslator::isFixedWidthDataset(IHqlExpression * dataset)
{
    return isFixedSizeRecord(dataset->queryRecord());
}


void HqlCppTranslator::createAccessFunctions(StringBuffer & helperFunc, BuildCtx & ctx, unsigned prio, const char * interfaceName, const char * object)
{
    helperFunc.append("q").append(object);

    StringBuffer s;
    s.clear().append(interfaceName).append(" & ").append(helperFunc).append("() { ");
    s.append("return ").append(object).append("; ");
    s.append("}");
    if (prio)
        ctx.setNextPriority(prio);
    ctx.addQuoted(s);

    s.clear().append("extern ").append(interfaceName).append(" & ").append(helperFunc).append("();");
    BuildCtx protoctx(*code, mainprototypesAtom);
    protoctx.addQuoted(s);
}

void HqlCppTranslator::ensureRowAllocator(StringBuffer & allocatorName, BuildCtx & ctx, IHqlExpression * record, IHqlExpression * activityId)
{
    OwnedHqlExpr marker = createAttribute(rowAllocatorMarkerAtom, LINK(record->queryBody()), LINK(activityId));
    HqlExprAssociation * match = ctx.queryMatchExpr(marker);
    if (match)
    {
        generateExprCpp(allocatorName, match->queryExpr());
        return;
    }

    StringBuffer uid;
    getUniqueId(uid.append("alloc"));

    BuildCtx subctx(ctx);
    BuildCtx * declarectx = &subctx;
    BuildCtx * callctx = &subctx;
    if (!getInvariantMemberContext(ctx, &declarectx, &callctx, true, false))
    {
        //The following will not currently work because not all compound statements are correctly marked as
        //complete/incomplete
        //subctx.selectOutermostScope();
    }

    StringBuffer s;
    s.append("Owned<IEngineRowAllocator> ").append(uid).append(";");
    declarectx->addQuoted(s);

    StringBuffer decl;
    s.clear().append(uid).append(".setown(ctx->getRowAllocator(&");
    buildMetaForRecord(s, record);
    s.append(",");
    generateExprCpp(s, activityId).append(")");
    s.append(");");

    callctx->addQuoted(s);

    OwnedHqlExpr value = createVariable(uid.str(), makeBoolType());
    declarectx->associateExpr(marker, value);
    allocatorName.append(uid);
}


IHqlExpression * HqlCppTranslator::createRowAllocator(BuildCtx & ctx, IHqlExpression * record)
{
    StringBuffer allocatorName;
    OwnedHqlExpr curActivityId = getCurrentActivityId(ctx);
    ensureRowAllocator(allocatorName, ctx, record, curActivityId);
    return createQuoted(allocatorName, makeBoolType());
}


void HqlCppTranslator::buildMetaSerializerClass(BuildCtx & ctx, IHqlExpression * record, const char * serializerName, IAtom * serializeForm)
{
    StringBuffer s;
    GlobalClassBuilder serializer(*this, ctx, serializerName, "COutputRowSerializer", "IOutputRowSerializer");

    serializer.buildClass(RowMetaPrio);
    serializer.setIncomplete(true);

    BuildCtx & classctx = serializer.classctx;
    s.clear().append("inline ").append(serializerName).append("(unsigned _activityId) : COutputRowSerializer(_activityId) {}");
    classctx.addQuoted(s);

    OwnedHqlExpr id = createVariable("activityId", LINK(sizetType));
    serializer.classctx.associateExpr(queryActivityIdMarker(), id);

    OwnedHqlExpr dataset = createDataset(no_null, LINK(record));
    {
        MemberFunction func(*this, serializer.startctx, "virtual void serialize(IRowSerializerTarget & out, const byte * self)");
        OwnedHqlExpr helper = createVariable("out", makeBoolType());
        BoundRow * row = bindTableCursor(func.ctx, dataset, "self");
        OwnedHqlExpr size = getRecordSize(row->querySelector());
        CHqlBoundExpr boundSize;
        buildExpr(func.ctx, size, boundSize);
        if (recordRequiresSerialization(record, serializeForm))
        {
            Owned<IReferenceSelector> selector = buildActiveRow(func.ctx, row->querySelector());
            selector->buildSerialize(func.ctx, helper, serializeForm);
        }
        else
        {
            HqlExprArray args;
            args.append(*LINK(helper));
            args.append(*LINK(boundSize.expr));
            args.append(*LINK(row->queryBound()));
            OwnedHqlExpr call = bindTranslatedFunctionCall(serializerPutId, args);
            func.ctx.addExpr(call);
        }
    }

    serializer.setIncomplete(false);
    serializer.completeClass(RowMetaPrio);
}

void HqlCppTranslator::buildMetaDeserializerClass(BuildCtx & ctx, IHqlExpression * record, const char * deserializerName, IAtom * serializeForm)
{
    StringBuffer s;
    GlobalClassBuilder deserializer(*this, ctx, deserializerName, "COutputRowDeserializer", "IOutputRowDeserializer");

    deserializer.buildClass(RowMetaPrio);
    deserializer.setIncomplete(true);

    BuildCtx & classctx = deserializer.classctx;
    s.clear().append("inline ").append(deserializerName).append("(unsigned _activityId) : COutputRowDeserializer(_activityId) {}");
    classctx.addQuoted(s);

    OwnedHqlExpr id = createVariable("activityId", LINK(sizetType));
    deserializer.classctx.associateExpr(queryActivityIdMarker(), id);

    OwnedHqlExpr dataset = createDataset(no_null, LINK(record));
    {
        MemberFunction func(*this, deserializer.startctx, "virtual size32_t deserialize(ARowBuilder & crSelf, IRowDeserializerSource & in)");
        BoundRow * row = bindSelf(func.ctx, dataset, "crSelf");
        ensureRowAllocated(func.ctx, "crSelf");

        OwnedHqlExpr helper = createVariable("in", makeBoolType());
        Owned<IReferenceSelector> selector = buildActiveRow(func.ctx, row->querySelector());
        selector->buildDeserialize(func.ctx, helper, serializeForm);
        buildReturnRecordSize(func.ctx, row);
    }

    deserializer.setIncomplete(false);
    deserializer.completeClass(RowMetaPrio);
}

bool HqlCppTranslator::buildMetaPrefetcherClass(BuildCtx & ctx, IHqlExpression * record, const char * prefetcherName)
{
    StringBuffer s;
    GlobalClassBuilder prefetcher(*this, ctx, prefetcherName, "CSourceRowPrefetcher", NULL);

    prefetcher.buildClass(RowMetaPrio);
    prefetcher.setIncomplete(true);

    BuildCtx & classctx = prefetcher.classctx;
    s.clear().append("inline ").append(prefetcherName).append("(unsigned _activityId) : CSourceRowPrefetcher(_activityId) {}");
    classctx.addQuoted(s);

    OwnedHqlExpr id = createVariable("activityId", LINK(sizetType));
    prefetcher.classctx.associateExpr(queryActivityIdMarker(), id);

    OwnedHqlExpr dataset = createDataset(no_null, LINK(record));
    bool ok;
    {
        MemberFunction func(*this, prefetcher.startctx, "virtual void readAhead(IRowDeserializerSource & in)");
        OwnedHqlExpr helper = createVariable("in", makeBoolType());

        ok = queryRecordOffsetMap(record, false)->buildReadAhead(*this, func.ctx, helper);
    }

    if (ok)
    {
        prefetcher.setIncomplete(false);
        prefetcher.completeClass(RowMetaPrio);
    }
    else
        prefetcher.setIncluded(false);
    return ok;
}

IHqlExpression * HqlCppTranslator::getRtlFieldKey(IHqlExpression * expr, IHqlExpression * rowRecord)
{
    /*
    Most field information is context independent - which make life much easier, there are a few exceptions though:
    type_bitfield.   The offset within the bitfield, and whether the bitfield is the last in the block depend on the other adjacent bitfields.
    type_alien.  Because it can refer to self in the parameters to the type definition it is dependent on the containing record
    no_ifblock:  Again because it references no_self, it is context dependent.
                 Theoretically with an inline record definition for a field it might be possible to make an ifblock dependent on something other than the most
                 immediate parent record, but it would be extremely pathological, and probably wouldn't work in lots of other ways.
    */

    bool contextDependent = false;
    LinkedHqlExpr extra = rowRecord;
    switch  (expr->getOperator())
    {
    case no_field:
        switch (expr->queryType()->getTypeCode())
        {
        case type_bitfield:
            {
                ColumnToOffsetMap * map = queryRecordOffsetMap(rowRecord, false);
                AColumnInfo * root = map->queryRootColumn();
                CBitfieldInfo * resolved = static_cast<CBitfieldInfo *>(root->lookupColumn(expr));
                assertex(resolved);
                unsigned offset = resolved->queryBitfieldOffset();
                bool isLastBitfield = resolved->queryIsLastBitfield();

                Linked<ITypeInfo> fieldType = expr->queryType();
                fieldType.setown(makeAttributeModifier(LINK(fieldType), createAttribute(bitfieldOffsetAtom, getSizetConstant(offset))));
                if (isLastBitfield)
                    fieldType.setown(makeAttributeModifier(LINK(fieldType), createAttribute(isLastBitfieldAtom)));

                HqlExprArray args;
                unwindChildren(args, expr);
                return createField(expr->queryId(), LINK(fieldType), args);
            }
            break;
        case type_alien:
            //actually too strict - some alien data types are not context dependent.
            contextDependent = true;
            break;
        }
        break;
    case no_ifblock:
        contextDependent = true;
        break;
    }

    if (contextDependent)
        return createAttribute(rtlFieldKeyMarkerAtom, LINK(expr), extra.getClear());
    return LINK(expr);
}

unsigned HqlCppTranslator::buildRtlField(StringBuffer & instanceName, IHqlExpression * field, IHqlExpression * rowRecord)
{
    OwnedHqlExpr fieldKey = getRtlFieldKey(field, rowRecord);

    BuildCtx declarectx(*code, declareAtom);
    HqlExprAssociation * match = declarectx.queryMatchExpr(fieldKey);
    if (match)
    {
        IHqlExpression * mapped = match->queryExpr();
        mapped->queryChild(0)->toString(instanceName);
        return (unsigned)getIntValue(mapped->queryChild(1));
    }

    StringBuffer name;
    unsigned typeFlags = 0;
    unsigned fieldFlags = 0;
    if (field->getOperator() == no_ifblock)
    {
        typeFlags = buildRtlIfBlockField(name, field, rowRecord);
    }
    else
    {
        Linked<ITypeInfo> fieldType = field->queryType();
        switch (field->queryType()->getTypeCode())
        {
        case type_alien:
            //MORE:::
            break;
        case type_row:
            //Backward compatibility - should revisit
            fieldType.set(fieldType->queryChildType());
            break;
        case type_bitfield:
            //fieldKey contains a field with a type annotated with offsets/isLastBitfield
            fieldType.set(fieldKey->queryType());
            break;
        }

        StringBuffer xpathName, xpathItem;
        switch (fieldType->getTypeCode())
        {
        case type_set:
            extractXmlName(xpathName, &xpathItem, NULL, field, "Item", false);
            break;
        case type_dictionary:
        case type_table:
        case type_groupedtable:
            extractXmlName(xpathName, &xpathItem, NULL, field, "Row", false);
            //Following should be in the type processing, and the type should include the information
            if (field->hasAttribute(sizeAtom) || field->hasAttribute(countAtom))
                fieldFlags |= RFTMinvalidxml;
            break;
        default:
            extractXmlName(xpathName, NULL, NULL, field, NULL, false);
            break;
        }

        if (xpathName.length())
        {
            if (xpathName.charAt(0) == '@')
                fieldFlags |= RFTMhasxmlattr;
            if (checkXpathIsNonScalar(xpathName))
                fieldFlags |= RFTMhasnonscalarxpath;
        }

        StringBuffer lowerName;
        lowerName.append(field->queryName()).toLowerCase();

        if (options.debugGeneratedCpp)
        {
            name.append("rf_");
            convertToValidLabel(name, lowerName.str(), lowerName.length());
            name.append("_").append(++nextFieldId);
        }
        else
            name.append("rf").append(++nextFieldId);

        //Format of the xpath field is (nested-item 0x01 repeated-item)
        StringBuffer xpathFull, xpathCppText;
        xpathFull.append(xpathName);
        if (xpathItem.length())
            xpathFull.append(xpathCompoundSeparatorChar).append(xpathItem);

        if (strcmp(lowerName, xpathFull) != 0)
            appendStringAsQuotedCPP(xpathCppText, xpathFull.length(), xpathFull.str(), false);
        else
            xpathCppText.append("NULL");

        StringBuffer defaultInitializer;
        IHqlExpression *defaultValue = queryAttributeChild(field, defaultAtom, 0);
        if (defaultValue)
        {
            LinkedHqlExpr targetField = field;
            if (fieldType->getTypeCode() == type_bitfield)
                targetField.setown(createField(field->queryId(), LINK(fieldType->queryChildType()), NULL));

            MemoryBuffer target;
            if (createConstantField(target, targetField, defaultValue))
                appendStringAsQuotedCPP(defaultInitializer, target.length(), target.toByteArray(), false);
            else
                throwError1(HQLERR_CouldNotGenerateDefault, str(field->queryName()));
        }

        StringBuffer definition;
        StringBuffer typeName;
        typeFlags |= buildRtlType(typeName, fieldType);
        typeFlags |= fieldFlags;

        definition.append("const RtlFieldStrInfo ").append(name).append("(\"").append(lowerName).append("\",").append(xpathCppText).append(",&").append(typeName);
        if (fieldFlags || defaultInitializer.length())
            definition.append(',').appendf("0x%x", fieldFlags);
        if (defaultInitializer.length())
            definition.append(',').append(defaultInitializer);
        definition.append(");");

        BuildCtx fieldctx(declarectx);
        fieldctx.setNextPriority(TypeInfoPrio);
        fieldctx.addQuoted(definition);

        name.insert(0, "&");
    }
    OwnedHqlExpr nameExpr = createVariable(name.str(), makeBoolType());
    OwnedHqlExpr mapped = createAttribute(fieldAtom, LINK(nameExpr), getSizetConstant(typeFlags));
    declarectx.associateExpr(fieldKey, mapped);
    instanceName.append(name);
    return typeFlags;
}


unsigned HqlCppTranslator::buildRtlIfBlockField(StringBuffer & instanceName, IHqlExpression * ifblock, IHqlExpression * rowRecord)
{
    StringBuffer typeName, s;
    BuildCtx declarectx(*code, declareAtom);

    //First generate a pseudo type entry for an ifblock.
    unsigned fieldType = type_ifblock|RFTMcontainsifblock|RFTMnoserialize|RFTMunknownsize;
    {
        unsigned length = 0;
        StringBuffer childTypeName;
        unsigned childType = buildRtlRecordFields(childTypeName, ifblock->queryChild(1), rowRecord);
        fieldType |= (childType & RFTMinherited);

        StringBuffer className;
        typeName.append("ty").append(++nextTypeId);
        className.append("tyc").append(nextFieldId);

        //The ifblock needs a unique instance of the class to evaluate the test
        BuildCtx fieldclassctx(declarectx);
        fieldclassctx.setNextPriority(TypeInfoPrio);
        fieldclassctx.addQuotedCompound(s.clear().append("struct ").append(className).append(" : public RtlIfBlockTypeInfo"), ";");
        fieldclassctx.addQuoted(s.clear().append(className).append("() : RtlIfBlockTypeInfo(0x").appendf("%x", fieldType).append(",").append(0).append(",").append(childTypeName).append(") {}"));

        OwnedHqlExpr anon = createDataset(no_anon, LINK(rowRecord));

        {
            MemberFunction condfunc(*this, fieldclassctx, "virtual bool getCondition(const byte * self) const");
            BoundRow * self = bindTableCursor(condfunc.ctx, anon, "self");
            OwnedHqlExpr cond = self->bindToRow(ifblock->queryChild(0), querySelfReference());
            buildReturn(condfunc.ctx, cond);
        }

        s.clear().append("const ").append(className).append(" ").append(typeName).append(";");

        BuildCtx typectx(declarectx);
        typectx.setNextPriority(TypeInfoPrio);
        typectx.addQuoted(s);
    }

    StringBuffer name;
    name.append("rf").append(++nextFieldId);

    //Now generate a pseudo field for the ifblock
    s.clear().append("const RtlFieldStrInfo ").append(name).append("(NULL, NULL,&").append(typeName).append(");");

    BuildCtx fieldctx(declarectx);
    fieldctx.setNextPriority(TypeInfoPrio);
    fieldctx.addQuoted(s);

    instanceName.append("&").append(name);
    return fieldType;
}


unsigned HqlCppTranslator::expandRtlRecordFields(StringBuffer & fieldListText, IHqlExpression * record, IHqlExpression * rowRecord)
{
    unsigned fieldType = 0;
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        unsigned childType = 0;
        switch (cur->getOperator())
        {
        case no_field:
        case no_ifblock:
            childType = buildRtlField(fieldListText, cur, rowRecord);
            fieldListText.append(",");
            break;
        case no_record:
            childType = expandRtlRecordFields(fieldListText, cur, rowRecord);
            break;
        }
        fieldType |= (childType & RFTMinherited);
    }
    return fieldType;
}


unsigned HqlCppTranslator::buildRtlRecordFields(StringBuffer & instanceName, IHqlExpression * record, IHqlExpression * rowRecord)
{
    StringBuffer fieldListText;
    unsigned fieldFlags = expandRtlRecordFields(fieldListText, record, rowRecord);

    StringBuffer name;
    name.append("tl").append(++nextTypeId);

    StringBuffer s;
    s.append("const RtlFieldInfo * const ").append(name).append("[] = { ").append(fieldListText).append(" 0 };");

    BuildCtx listctx(*code, declareAtom);
    listctx.setNextPriority(TypeInfoPrio);
    listctx.addQuoted(s);

    instanceName.append(name);
    return fieldFlags;
}

unsigned HqlCppTranslator::buildRtlType(StringBuffer & instanceName, ITypeInfo * type)
{
    assertex(type);
    if (type->getTypeCode() == type_record)
        type = queryUnqualifiedType(type);
    OwnedHqlExpr search = createVariable("t", LINK(type));
    BuildCtx declarectx(*code, declareAtom);
    HqlExprAssociation * match = declarectx.queryMatchExpr(search);
    if (match)
    {
        IHqlExpression * value = match->queryExpr();
        value->queryChild(0)->toString(instanceName);
        return (unsigned)getIntValue(value->queryChild(1));
    }

    StringBuffer name, arguments;
    if (options.debugGeneratedCpp)
    {
        StringBuffer ecl;
        type->getECLType(ecl);
        name.append("ty_");
        convertToValidLabel(name, ecl.str(), ecl.length());
        name.append("_").append(++nextTypeId);
    }
    else
        name.append("ty").append(++nextTypeId);

    FieldTypeInfoStruct info;
    getFieldTypeInfo(info, type);
    unsigned childType = 0;

    switch (info.fieldType & RFTMkind)
    {
    case type_record:
        {
            IHqlExpression * record = ::queryRecord(type);
            arguments.append(",");
            StringBuffer fieldsInstance;
            childType = buildRtlRecordFields(fieldsInstance, record, record);
            arguments.append(fieldsInstance);

            //The following code could be used to generate an extra list of fields with nested records expanded out,
            //but it causes some queries to grow significantly, so not currently used.
#if 0
            if (!recordContainsNestedRow(record))
            {
                arguments.append(fieldsInstance).append(",");
                arguments.append(fieldsInstance);
            }
            else
            {
                arguments.append(fieldsInstance).append(",");
                childType |= buildRtlRecordFields(arguments, record, record, true);
            }
#endif
            break;
        }
    case type_row:
        {
            arguments.append(",&");
            childType = buildRtlType(arguments, ::queryRecordType(type));
            break;
        }
    case type_table:
    case type_groupedtable:
        {
            arguments.append(",&");
            childType = buildRtlType(arguments, ::queryRecordType(type));
            break;
        }
    case type_dictionary:
        {
            arguments.append(",&");
            childType = buildRtlType(arguments, ::queryRecordType(type));
            StringBuffer lookupHelperName;
            buildDictionaryHashClass(::queryRecord(type), lookupHelperName);
            arguments.append(",&").append(lookupHelperName.str());
            break;
        }
    case type_set:
        arguments.append(",&");
        childType = buildRtlType(arguments, type->queryChildType());
        break;
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        arguments.append(", \"").append(info.locale).append("\"").toLowerCase();
        break;
    }
    info.fieldType |= (childType & RFTMinherited);

    StringBuffer definition;
    definition.append("const ").append(info.className).append(" ").append(name).append("(0x").appendf("%x", info.fieldType).append(",").append(info.length).append(arguments).append(");");

    BuildCtx typectx(declarectx);
    typectx.setNextPriority(TypeInfoPrio);
    typectx.addQuoted(definition);

    OwnedHqlExpr nameExpr = createVariable(name.str(), makeVoidType());
    OwnedHqlExpr mapped = createAttribute(fieldAtom, LINK(nameExpr), getSizetConstant(info.fieldType));
    declarectx.associateExpr(search, mapped);
    instanceName.append(name);
    return info.fieldType;
}


void HqlCppTranslator::buildMetaInfo(MetaInstance & instance)
{
    if (options.spanMultipleCpp)
    {
        StringBuffer queryFunctionName;
        queryFunctionName.append("q").append(instance.instanceName).append("()");
        instance.instanceObject.set(queryFunctionName);
    }

    BuildCtx declarectx(*code, declareAtom);

    OwnedHqlExpr search = instance.getMetaUniqueKey();

    // stop duplicate classes being generated.
    // MORE: If this ever includes sorting/grouping, the dependence on a record will need to be revised
    HqlExprAssociation * match = declarectx.queryMatchExpr(search);
    if (match)
        return;

    bool savedContextAvailable = contextAvailable;
    contextAvailable = false;
    metas.append(*search.getLink());
    StringBuffer s;
    StringBuffer serializerName, deserializerName, prefetcherName, internalSerializerName, internalDeserializerName;

    StringBuffer endText;

    endText.append(" ").append(instance.instanceName).append(";");
    BuildCtx metactx(declarectx);

    IHqlExpression * record = instance.queryRecord();

    unsigned flags = MDFhasserialize;       // we always generate a serialize since 
    bool useTypeForXML = false;
    if (instance.isGrouped())
        flags |= MDFgrouped;
    if (record)
        flags |= MDFhasxml;
    if (record)
    {
        if (recordRequiresDestructor(record))
            flags |= MDFneeddestruct;
        if (recordRequiresSerialization(record, diskAtom))
            flags |= MDFneedserializedisk;
        if (recordRequiresSerialization(record, internalAtom))
            flags |= MDFneedserializeinternal;

        const unsigned serializeFlags = (flags & MDFneedserializemask);
        if ((serializeFlags == MDFneedserializemask) && !recordSerializationDiffers(record, diskAtom, internalAtom))
            flags |= MDFdiskmatchesinternal;

        if (maxRecordSizeUsesDefault(record))
            flags |= MDFunknownmaxlength;
        useTypeForXML = true;
    }

    if (instance.isGrouped())
    {
        MetaInstance ungroupedMeta(*this, record, false);
        buildMetaInfo(ungroupedMeta);

        s.append("struct ").append(instance.metaName).append(" : public ").append(ungroupedMeta.metaName);
        metactx.setNextPriority(RowMetaPrio);
        metactx.addQuotedCompound(s, endText.str());
        doBuildUnsignedFunction(metactx, "getMetaFlags", flags);
    }
    else
    {
        //Serialization classes need to be generated for all meta information - because they may be called by parent row classes
        //however, the CFixedOutputMetaData base class contains a default implementation - reducing the required code.
        if (record && (isVariableSizeRecord(record) || (flags & MDFneedserializemask)))
        {
            //Base class provides a default variable width implementation
            if (flags & MDFneedserializedisk)
            {
                serializerName.append("s").append(instance.metaName);
                buildMetaSerializerClass(declarectx, record, serializerName.str(), diskAtom);
            }
            bool needInternalSerializer = ((flags & MDFneedserializeinternal) && recordSerializationDiffers(record, diskAtom, internalAtom));

            if (needInternalSerializer)
            {
                internalSerializerName.append("si").append(instance.metaName);
                buildMetaSerializerClass(declarectx, record, internalSerializerName.str(), internalAtom);
            }

            //MORE:
            //still generate a deserialize for the variable width case because it offers protection
            //against accessing out of bounds data
            deserializerName.append("d").append(instance.metaName);
            buildMetaDeserializerClass(declarectx, record, deserializerName.str(), diskAtom);

            if (needInternalSerializer)
            {
                internalDeserializerName.append("di").append(instance.metaName);
                buildMetaDeserializerClass(declarectx, record, internalDeserializerName.str(), internalAtom);
            }

            //The base class implements prefetch using the serialized meta so no need to generate...
            if (!(flags & MDFneedserializemask))
            {
                prefetcherName.append("p").append(instance.metaName);
                if (!buildMetaPrefetcherClass(declarectx, record, prefetcherName))
                    prefetcherName.clear();
            }
        }

        s.append("struct ").append(instance.metaName).append(" : public ");
        if (!record)
            s.append("CActionOutputMetaData");
        else if (isFixedSizeRecord(record))
            s.append("CFixedOutputMetaData");
        else
            s.append("CVariableOutputMetaData");

        metactx.setNextPriority(RowMetaPrio);
        IHqlStmt * metaclass = metactx.addQuotedCompound(s, endText.str());
        metaclass->setIncomplete(true);

        if (record)
        {
            if (isFixedSizeRecord(record))
            {
                unsigned fixedSize = getMinRecordSize(record);
                s.clear().append("inline ").append(instance.metaName).append("() : CFixedOutputMetaData(").append(fixedSize).append(") {}");
                metactx.addQuoted(s);
            }
            else
            {
                unsigned minSize = getMinRecordSize(record);
                unsigned maxLength = getMaxRecordSize(record);
                if (maxLength < minSize)
                    reportError(queryLocation(record), ECODETEXT(HQLERR_MaximumSizeLessThanMinimum_XY), maxLength, minSize);
                    
                //These use a CVariableOutputMetaData base class instead, and trade storage for number of virtuals
                s.clear().append("inline ").append(instance.metaName).append("() : CVariableOutputMetaData(").append(minSize).append(") {}");
                metactx.addQuoted(s);

                if (options.testIgnoreMaxLength)
                    maxLength = minSize;

                MemberFunction getFunc(*this, metactx, "virtual size32_t getRecordSize(const void * data)");
                s.clear().append("if (!data) return ").append(maxLength).append(";");
                getFunc.ctx.addQuoted(s.str());
                getFunc.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *)data;");

                OwnedHqlExpr selfDs = createDataset(no_null, LINK(instance.queryRecord()));
                BoundRow * selfRow = bindTableCursorOrRow(getFunc.ctx, selfDs, "left");
                OwnedHqlExpr size = getRecordSize(selfRow->querySelector());
                buildReturn(getFunc.ctx, size);
            }
            assertex(!instance.isGrouped());

            StringBuffer typeName;
            unsigned recordTypeFlags = buildRtlType(typeName, record->queryType());
            s.clear().append("virtual const RtlTypeInfo * queryTypeInfo() const { return &").append(typeName).append("; }");
            metactx.addQuoted(s);

            if (record->numChildren() != 0)
            {
                if (!useTypeForXML || (recordTypeFlags & (RFTMinvalidxml|RFTMhasxmlattr)))
                {
                    OwnedHqlExpr anon = createDataset(no_anon, LINK(instance.queryRecord()));
                    buildXmlSerialize(metactx, anon, "toXML", true);
                }
            }

            generateMetaRecordSerialize(metactx, record, serializerName.str(), deserializerName.str(), internalSerializerName.str(), internalDeserializerName.str(), prefetcherName.str());

            if (flags != (MDFhasserialize|MDFhasxml))
                doBuildUnsignedFunction(metactx, "getMetaFlags", flags);

            if (flags & MDFneedserializedisk)
            {
                OwnedHqlExpr serializedRecord = getSerializedForm(record, diskAtom);

                MetaInstance serializedMeta(*this, serializedRecord, false);
                buildMetaInfo(serializedMeta);
                StringBuffer s;
                s.append("virtual IOutputMetaData * querySerializedDiskMeta() { return &").append(serializedMeta.queryInstanceObject()).append("; }");
                metactx.addQuoted(s);
            }
        }

        metaclass->setIncomplete(false);
    }

    s.clear().append("extern \"C\" ECL_API IOutputMetaData * ").append(instance.metaFactoryName).append("() { ");
    s.append(instance.instanceName).append(".Link(); ");
    s.append("return &").append(instance.instanceName).append("; ");
    s.append("}");
    declarectx.setNextPriority(RowMetaPrio);
    declarectx.addQuoted(s);
    if (options.spanMultipleCpp)
    {
        StringBuffer temp;
        createAccessFunctions(temp, declarectx, RowMetaPrio, "IOutputMetaData", instance.instanceName);
    }

    OwnedHqlExpr temp = createVariable(instance.metaName, makeVoidType());
    declarectx.associateExpr(search, temp);
    contextAvailable = savedContextAvailable;
}


class MetaMemberCallback
{
public:
    MetaMemberCallback(HqlCppTranslator & _translator) : translator(_translator) {}

    void callChildFunction(BuildCtx & ctx, IHqlExpression * selected)
    {
        MetaInstance childMeta(translator, selected->queryRecord(), false);
        translator.buildMetaInfo(childMeta);
        callChildFunction(ctx, selected, childMeta);
    }

    void walkRecord(BuildCtx & ctx, IHqlExpression * selector, IHqlExpression * record)
    {
        ForEachChild(i, record)
        {
            IHqlExpression * cur = record->queryChild(i);
            switch (cur->getOperator())
            {
            case no_record:
                walkRecord(ctx, selector, cur);
                break;
            case no_ifblock:
                {
                    OwnedHqlExpr cond = replaceSelector(cur->queryChild(0), querySelfReference(), selector);
                    BuildCtx condctx(ctx);
                    translator.buildFilter(condctx, cond);
                    walkRecord(condctx, selector, cur->queryChild(1));
                    break;
                }
            case no_field:
                {
                    ITypeInfo * type = cur->queryType();
                    switch (type->getTypeCode())
                    {
                    case type_alien:
                        //MORE: Allow for alien data types to have destructors.
                        break;
                    case type_row:
                        {
                            IHqlExpression * record = cur->queryRecord();
                            if (recordRequiresDestructor(record))
                            {
                                OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(cur));
                                callChildFunction(ctx, selected);
                            }
                            break;
                        }
                    case type_dictionary:
                    case type_table:
                    case type_groupedtable:
                        {
                            OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(cur));
                            IHqlExpression * record = cur->queryRecord();
                            if (cur->hasAttribute(_linkCounted_Atom))
                            {
                                //releaseRowset(ctx, count, rowset)
                                MetaInstance childMeta(translator, selected->queryRecord(), false);
                                translator.buildMetaInfo(childMeta);
                                processRowset(ctx, selected, childMeta);
                            }
                            else if (recordRequiresDestructor(record))
                            {
                                BuildCtx iterctx(ctx);
                                translator.buildDatasetIterate(iterctx, selected, false);
                                OwnedHqlExpr active = createRow(no_activerow, LINK(selected));
                                callChildFunction(iterctx, active);
                            }
                            break;
                        }
                    }
                    break;
                }
            }
        }
    }

protected:
    virtual void callChildFunction(BuildCtx & ctx, IHqlExpression * selected, MetaInstance & childMeta) = 0;
    virtual void processRowset(BuildCtx & ctx, IHqlExpression * selected, MetaInstance & childMeta) = 0;

protected:
    HqlCppTranslator & translator;
};

class MetaDestructCallback : public MetaMemberCallback
{
public:
    MetaDestructCallback(HqlCppTranslator & _translator) : MetaMemberCallback(_translator) {}

protected:
    virtual void callChildFunction(BuildCtx & ctx, IHqlExpression * selected, MetaInstance & childMeta)
    {
        HqlExprArray args;
        args.append(*createQuoted(childMeta.queryInstanceObject(), makeBoolType()));
        args.append(*LINK(selected));
        translator.buildFunctionCall(ctx, destructMetaMemberId, args);
    }

    virtual void processRowset(BuildCtx & ctx, IHqlExpression * selected, MetaInstance & childMeta)
    {
        HqlExprArray args;
        args.append(*LINK(selected));
        translator.buildFunctionCall(ctx, releaseRowsetId, args);
    }
};

void HqlCppTranslator::doGenerateMetaDestruct(BuildCtx & ctx, IHqlExpression * selector, IHqlExpression * record)
{
    MetaDestructCallback builder(*this);
    builder.walkRecord(ctx, selector, record);
}


class MetaWalkIndirectCallback : public MetaMemberCallback
{
public:
    MetaWalkIndirectCallback(HqlCppTranslator & _translator, IHqlExpression * _visitor)
        : MetaMemberCallback(_translator), visitor(_visitor) {}

protected:
    virtual void callChildFunction(BuildCtx & ctx, IHqlExpression * selected, MetaInstance & childMeta)
    {
        HqlExprArray args;
        args.append(*createQuoted(childMeta.queryInstanceObject(), makeBoolType()));
        args.append(*LINK(selected));
        args.append(*LINK(visitor));
        translator.buildFunctionCall(ctx, walkIndirectMetaMemberId, args);
    }

    virtual void processRowset(BuildCtx & ctx, IHqlExpression * selected, MetaInstance & childMeta)
    {
        HqlExprArray args;
        args.append(*LINK(visitor));
        args.append(*LINK(selected));
        translator.buildFunctionCall(ctx, IIndirectMemberVisitor_visitRowsetId, args);
    }

protected:
    LinkedHqlExpr visitor;
};



class MetaChildMetaCallback
{
public:
    MetaChildMetaCallback(HqlCppTranslator & _translator, IHqlStmt * _switchStmt)
        : translator(_translator), switchStmt(_switchStmt)
    {
        nextIndex = 0;
    }

    void walkRecord(BuildCtx & ctx, IHqlExpression * record)
    {
        ForEachChild(i, record)
        {
            IHqlExpression * cur = record->queryChild(i);
            switch (cur->getOperator())
            {
            case no_record:
                walkRecord(ctx, cur);
                break;
            case no_ifblock:
                walkRecord(ctx, cur->queryChild(1));
                break;
            case no_field:
                {
                    ITypeInfo * type = cur->queryType();
                    switch (type->getTypeCode())
                    {
                    case type_row:
                        walkRecord(ctx, queryRecord(cur));
                        break;
                    case type_dictionary:
                    case type_table:
                    case type_groupedtable:
                        {
                            IHqlExpression * record = cur->queryRecord()->queryBody();
                            if (!visited.contains(*record))
                            {
                                BuildCtx condctx(ctx);
                                OwnedHqlExpr branch = getSizetConstant(nextIndex++);
                                OwnedHqlExpr childMeta = translator.buildMetaParameter(record);
                                OwnedHqlExpr ret = createValue(no_address, makeBoolType(), LINK(childMeta));

                                condctx.addCase(switchStmt, branch);
                                condctx.addReturn(ret);
                                visited.append(*record);
                            }
                            break;
                        }
                    }
                    break;
                }
            }
        }
    }

protected:
    HqlCppTranslator & translator;
    IHqlStmt * switchStmt;
    HqlExprCopyArray visited;
    unsigned nextIndex;
};



void HqlCppTranslator::generateMetaRecordSerialize(BuildCtx & ctx, IHqlExpression * record, const char * diskSerializerName, const char * diskDeserializerName, const char * internalSerializerName, const char * internalDeserializerName, const char * prefetcherName)
{
    OwnedHqlExpr dataset = createDataset(no_null, LINK(record));

    if (recordRequiresDestructor(record))
    {
        MemberFunction func(*this, ctx, "virtual void destruct(byte * self)");
        bindTableCursor(func.ctx, dataset, "self");
        MetaDestructCallback builder(*this);
        builder.walkRecord(func.ctx, dataset, record);
    }

    if (recordRequiresDestructor(record))
    {
        OwnedHqlExpr visitor = createVariable("visitor", makeBoolType());       // makeClassType("IIndirectMemberVisitor");
        MemberFunction func(*this, ctx, "virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor)");
        bindTableCursor(func.ctx, dataset, "self");
        MetaWalkIndirectCallback builder(*this, visitor);
        builder.walkRecord(func.ctx, dataset, record);
    }

    {
        OwnedHqlExpr switchVar = createVariable("i", makeIntType(4, false));

        MemberFunction func(*this, ctx, "virtual IOutputMetaData * queryChildMeta(unsigned i)");

        BuildCtx switchctx(func.ctx);
        IHqlStmt * switchStmt = switchctx.addSwitch(switchVar);
        unsigned prevChildren = func.numStmts();
        MetaChildMetaCallback builder(*this, switchStmt);
        builder.walkRecord(func.ctx, record);
        if (prevChildren != func.numStmts())
            func.ctx.addReturn(queryQuotedNullExpr());
        else
            func.setIncluded(false);
    }

    if (diskSerializerName && *diskSerializerName)
    {
        BuildCtx serializectx(ctx);
        serializectx.addQuotedFunction("virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId)");

        StringBuffer s;
        s.append("return cr").append(diskSerializerName).append("(ctx, activityId);");
        serializectx.addQuoted(s);
    }

    if (diskDeserializerName && *diskDeserializerName)
    {
        BuildCtx deserializectx(ctx);
        deserializectx.addQuotedFunction("virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId)");

        StringBuffer s;
        s.append("return cr").append(diskDeserializerName).append("(ctx, activityId);");
        deserializectx.addQuoted(s);
    }

    if (internalSerializerName && *internalSerializerName)
    {
        BuildCtx serializectx(ctx);
        serializectx.addQuotedFunction("virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId)");

        StringBuffer s;
        s.append("return cr").append(internalSerializerName).append("(ctx, activityId);");
        serializectx.addQuoted(s);
    }

    if (internalDeserializerName && *internalDeserializerName)
    {
        BuildCtx deserializectx(ctx);
        deserializectx.addQuotedFunction("virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId)");

        StringBuffer s;
        s.append("return cr").append(internalDeserializerName).append("(ctx, activityId);");
        deserializectx.addQuoted(s);
    }

    if (prefetcherName && *prefetcherName)
    {
        BuildCtx deserializectx(ctx);
        deserializectx.addQuotedFunction("virtual CSourceRowPrefetcher * doCreateDiskPrefetcher(unsigned activityId)");

        StringBuffer s;
        s.append("return new ").append(prefetcherName).append("(activityId);");
        deserializectx.addQuoted(s);
    }
}

IHqlExpression * HqlCppTranslator::buildMetaParameter(IHqlExpression * arg)
{
    MetaInstance meta(*this, arg->queryRecord(), false);
    buildMetaInfo(meta);
    return createQuoted(meta.queryInstanceObject(), makeBoolType());
}

void HqlCppTranslator::buildMetaMember(BuildCtx & ctx, IHqlExpression * datasetOrRecord, bool grouped, const char * name)
{
    MetaInstance meta(*this, ::queryRecord(datasetOrRecord), grouped);
    StringBuffer s;

    buildMetaInfo(meta);
    s.append("virtual IOutputMetaData * ").append(name).append("() { return &").append(meta.queryInstanceObject()).append("; }");
    ctx.addQuoted(s);
}

void HqlCppTranslator::buildMetaForRecord(StringBuffer & name, IHqlExpression * record)
{
    MetaInstance meta(*this, record, false);
    buildMetaInfo(meta);
    name.append(meta.queryInstanceObject());
}

void HqlCppTranslator::buildMetaForSerializedRecord(StringBuffer & name, IHqlExpression * record, bool isGrouped)
{
    if (isGrouped)
    {
        HqlExprArray args;
        unwindChildren(args, record);
        args.append(*createField(__eogId, makeBoolType(), NULL, NULL));
        OwnedHqlExpr groupedRecord = record->clone(args);
        buildMetaForRecord(name, groupedRecord);
    }
    else
        buildMetaForRecord(name, record);
}

void HqlCppTranslator::ensureRowSerializer(StringBuffer & serializerName, BuildCtx & ctx, IHqlExpression * record, IAtom * format, IAtom * kind)
{
    OwnedHqlExpr marker = createAttribute(serializerInstanceMarkerAtom, LINK(record->queryBody()), createAttribute(kind));
    HqlExprAssociation * match = ctx.queryMatchExpr(marker);
    if (match)
    {
        generateExprCpp(serializerName, match->queryExpr());
        return;
    }

    StringBuffer uid;
    getUniqueId(uid.append("ser"));

    BuildCtx * declarectx = &ctx;
    BuildCtx * callctx = &ctx;
    getInvariantMemberContext(ctx, &declarectx, &callctx, true, false);

    StringBuffer s;
    const char * kindText = (kind == serializerAtom) ? "Serializer" : "Deserializer";

    s.append("Owned<IOutputRow").append(kindText).append("> ").append(uid).append(";");
    declarectx->addQuoted(s);

    MetaInstance meta(*this, record, false);
    buildMetaInfo(meta);

    s.clear().append(uid).append(".setown(").append(meta.queryInstanceObject());

    if (format == diskAtom)
        s.append(".createDisk").append(kindText);
    else if (format == internalAtom)
        s.append(".createInternal").append(kindText);
    else
        throwUnexpected();

    s.append("(ctx, ");
    OwnedHqlExpr activityId = getCurrentActivityId(ctx);
    generateExprCpp(s, activityId);
    s.append("));");
    callctx->addQuoted(s);

    OwnedHqlExpr value = createVariable(uid.str(), makeBoolType());
    declarectx->associateExpr(marker, value);
    serializerName.append(uid);
}


void HqlCppTranslator::ensureRowPrefetcher(StringBuffer & prefetcherName, BuildCtx & ctx, IHqlExpression * record)
{
    OwnedHqlExpr marker = createAttribute(prefetcherInstanceMarkerAtom, LINK(record->queryBody()));
    HqlExprAssociation * match = ctx.queryMatchExpr(marker);
    if (match)
    {
        generateExprCpp(prefetcherName, match->queryExpr());
        return;
    }

    StringBuffer uid;
    getUniqueId(uid.append("pf"));

    BuildCtx * declarectx = &ctx;
    BuildCtx * callctx = &ctx;
    getInvariantMemberContext(ctx, &declarectx, &callctx, true, false);

    StringBuffer s;
    s.append("Owned<ISourceRowPrefetcher> ").append(uid).append(";");
    declarectx->addQuoted(s);

    MetaInstance meta(*this, record, false);
    buildMetaInfo(meta);

    s.clear().append(uid).append(".setown(").append(meta.queryInstanceObject());
    s.append(".createDiskPrefetcher(ctx, ");
    OwnedHqlExpr activityId = getCurrentActivityId(ctx);
    generateExprCpp(s, activityId);
    s.append("));");
    callctx->addQuoted(s);

    OwnedHqlExpr value = createVariable(uid.str(), makeBoolType());
    declarectx->associateExpr(marker, value);
    prefetcherName.append(uid);
}


IHqlExpression * HqlCppTranslator::createSerializer(BuildCtx & ctx, IHqlExpression * record, IAtom * form, IAtom * kind)
{
    StringBuffer serializerName;
    ensureRowSerializer(serializerName, ctx, record, form, kind);
    return createQuoted(serializerName.str(), makeBoolType());
}

IHqlExpression * HqlCppTranslator::createResultName(IHqlExpression * name, bool expandLogicalName)
{
    IHqlExpression * resultName = ::createResultName(name);
    if (!expandLogicalName)
        return resultName;

    HqlExprArray args;
    args.append(*resultName);
    return bindFunctionCall(getExpandLogicalNameId, args);
}

bool HqlCppTranslator::registerGlobalUsage(IHqlExpression * filename)
{
    bool matched = false;
    ForEachItemIn(i, globalFiles)
    {
        if (globalFiles.item(i).checkMatch(filename))
            matched = true;
    }
    return matched;
}


//---------------------------------------------------------------------------

IHqlExpression * HqlCppTranslator::convertBetweenCountAndSize(const CHqlBoundExpr & bound, bool getCount)
{
    ITypeInfo * type = bound.expr->queryType();
    if (getCount)
    {
        if (getIntValue(bound.length, 1) == 0)
            return getSizetConstant(0);
    }
    else
    {
        if (getIntValue(bound.count, 1) == 0)
            return getSizetConstant(0);
    }


    OwnedHqlExpr record;
    switch (type->getTypeCode())
    {
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        record.set(bound.expr->queryRecord());
        break;
    case type_set:
    case type_array:
        {
            ITypeInfo * elementType = type->queryChildType();
            HqlExprArray fields;
            fields.append(*createField(valueId, LINK(elementType), NULL));
            record.setown(createRecord(fields));
            break;
        }
    default:
        UNIMPLEMENTED;
    }

    if (isFixedSizeRecord(record))
    {
        unsigned fixedSize = getMinRecordSize(record);
        if (fixedSize == 0)
            throwError(HQLERR_ZeroLengthIllegal);
        if (type->getTypeCode() == type_groupedtable)
            fixedSize++;

        if (getCount)
        {
            if (fixedSize == 1)
                return LINK(bound.length);
            IValue * value = bound.length->queryValue();
            if (value)
                return getSizetConstant((unsigned)value->getIntValue()/fixedSize);
            return createValue(no_div, LINK(sizetType), LINK(bound.length), getSizetConstant(fixedSize));
        }
        else
        {
            if (fixedSize == 1)
                return LINK(bound.count);
            IValue * value = bound.count->queryValue();
            if (value)
                return getSizetConstant((unsigned)value->getIntValue() * fixedSize);
            return createValue(no_mul, LINK(sizetType), LINK(bound.count), getSizetConstant(fixedSize));
        }
    }

    StringBuffer metaInstanceName, s;
    buildMetaForSerializedRecord(metaInstanceName, record, (type->getTypeCode() == type_groupedtable));
    HqlExprArray args;

    IIdAtom * func;
    if (getCount)
    {
        args.append(*getBoundSize(bound));
        args.append(*LINK(bound.expr));
        func = countRowsId;
    }
    else
    {
        args.append(*LINK(bound.count));
        args.append(*LINK(bound.expr));
        func = countToSizeId;
    }
    args.append(*createQuoted(s.clear().append("&").append(metaInstanceName), makeBoolType()));

    return bindTranslatedFunctionCall(func, args);
}

//---------------------------------------------------------------------------

void HqlCppTranslator::noteResultDefined(BuildCtx & ctx, ActivityInstance * activityInstance, IHqlExpression * seq, IHqlExpression * name, bool alwaysExecuted)
{
    unsigned graph = curGraphSequence();
    assertex(graph);

    SubGraphInfo * activeSubgraph = queryActiveSubGraph(ctx);
    assertex(activeSubgraph);
    if (isInternalSeq(seq))
    {
        internalResults.append(* new InternalResultTracker(name, activeSubgraph->tree, graph, activityInstance));
    }
    else if (alwaysExecuted)
    {
        assertex(activeSubgraph->tree->hasProp("att[@name=\"rootGraph\"]"));
    }
}

void HqlCppTranslator::noteResultAccessed(BuildCtx & ctx, IHqlExpression * seq, IHqlExpression * name)
{
    if (isInternalSeq(seq))
    {
        unsigned graph = curGraphSequence();
        ForEachItemIn(i, internalResults)
        {
            if (internalResults.item(i).noteUse(name, graph))
            {
                //Can't currently break because the same result might be generated more than once
                //if an expression ends up in two different graphs.
                //break;
            }
        }
    }
}

void HqlCppTranslator::buildGetResultInfo(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr * boundTarget, const CHqlBoundTarget * targetAssign)
{
    IHqlExpression * seq = queryAttributeChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
    if (!name)
        name = queryAttributeChild(expr, nameAtom, 0);

    noteResultAccessed(ctx, seq, name);

    if (insideLibrary())
    {
        SCMStringBuffer libraryName;
        getOutputLibraryName(libraryName, wu());
        StringBuffer storedName;
        getStoredDescription(storedName, seq, name, true);
        throwError2(HQLERR_LibraryNoWorkunitRead, libraryName.str(), storedName.str());
    }

    __int64 seqValue = seq->queryValue()->getIntValue();
    assertex(!expr->hasAttribute(internalAtom) && !expr->hasAttribute(_internal_Atom));
    bool expandLogical = (seqValue == ResultSequencePersist) && !expr->hasAttribute(_internal_Atom);

    HqlExprArray args;
    args.append(*createResultName(name, expandLogical));
    args.append(*LINK(seq));

    IIdAtom * func;
    ITypeInfo * type = expr->queryType();
    type_t ttc = type->getTypeCode();
    OwnedITypeInfo overrideType;
    switch(ttc)
    {
    case type_int:      func = getResultIntId; break;
    case type_swapint:  func = getResultIntId; break;
    case type_boolean:  func = getResultBoolId; break;
    case type_data:     func = getResultDataId; break;
    case type_dictionary:
    case type_table:
    case type_groupedtable:
    case type_set:
        //MORE: type_row...
        {
            OwnedHqlExpr record;
            bool ensureSerialized = true;
            if (ttc == type_dictionary)
            {
                record.set(::queryRecord(type));

                //NB: The result type will be overridden when this function is bound
                ensureSerialized = false;
                overrideType.setown(setLinkCountedAttr(type, true));
                func = getResultDictionaryId;
            }
            else if (ttc != type_set)
            {
                overrideType.set(type);
                record.set(::queryRecord(type));
                //NB: The result type (including grouping) will be overridden when this function is bound
                func = getResultDatasetId;
                bool defaultLCR = targetAssign ? hasLinkedRow(targetAssign->queryType()) : true;
                if (hasLinkCountedModifier(type) || defaultLCR)
                {
                    ensureSerialized = false;
                    args.append(*createRowAllocator(ctx, record));
                    args.append(*createConstant(isGrouped(expr)));
                    overrideType.setown(setLinkCountedAttr(overrideType, true));
                    func = getResultRowsetId;
                }
            }
            else
            {
                overrideType.set(type);
                ITypeInfo * elementType = type->queryChildType();
                OwnedHqlExpr field = createField(valueId, LINK(elementType), NULL);
                record.setown(createRecord(field));
                func = getResultSetId;
            }

            if (ensureSerialized && record)
                record.setown(getSerializedForm(record, diskAtom));

            if (record && (seqValue == ResultSequenceStored))
            {
                StringBuffer s;
                OwnedHqlExpr ds = createDataset(no_anon, LINK(record));

                StringBuffer xmlInstanceName, xmlFactoryName;
                bool usesContents = false;
                getUniqueId(xmlInstanceName.append("xml"));
                buildXmlReadTransform(ds, xmlFactoryName, usesContents);
                OwnedHqlExpr curActivityId = getCurrentActivityId(ctx);
                s.append("Owned<IXmlToRowTransformer> ").append(xmlInstanceName).append(" = ").append(xmlFactoryName).append("(ctx,");
                generateExprCpp(s, curActivityId).append(");");
                ctx.addQuoted(s);
                args.append(*createQuoted(xmlInstanceName, makeBoolType()));

                StringBuffer csvInstanceName;
                if (canReadFromCsv(record))
                {
                    buildCsvReadTransformer(ds, csvInstanceName, NULL);
                    csvInstanceName.insert(0, "&");
                }
                else
                {
                    csvInstanceName.clear().append("0");
                }
                args.append(*createQuoted(csvInstanceName, makeBoolType()));
            }
            else
            {
                args.append(*createQuoted("0", makeBoolType()));
                args.append(*createQuoted("0", makeBoolType()));
            }
            if (ttc == type_dictionary)
            {
                StringBuffer lookupHelperName;
                buildDictionaryHashClass(expr->queryRecord(), lookupHelperName);

                lookupHelperName.insert(0, "&");    // MORE: Should this be passed by reference instead - it isn't optional
                args.append(*createQuoted(lookupHelperName.str(), makeBoolType()));
            }
            break;
        }
    case type_string:
        {
            func = getResultStringId;
            if ((type->queryCharset()->queryName() != asciiAtom) || !targetAssign)
                break;
            ITypeInfo * targetType = targetAssign->queryType();
            if ((targetType->getTypeCode() != type_string) || (targetType->getSize() == UNKNOWN_LENGTH) ||
                (targetType->queryCharset() != type->queryCharset()))
                break;
            //more: if (options.checkOverflow && queryUnqualifiedType(targetType) != queryUnqualifiedType(type)
            args.add(*targetAssign->getTranslatedExpr(), 0);
            buildFunctionCall(ctx, getResultStringFId, args);
            return;
        }
    case type_qstring:  func = getResultStringId; break;
    case type_varstring:func = getResultVarStringId; break;
    case type_unicode:  func = getResultUnicodeId; break;
    case type_varunicode:func = getResultVarUnicodeId; break;
    case type_utf8:     func = getResultUnicodeId; break;
    case type_real:     func = getResultRealId; break;
    case type_decimal:
        {
            //Special case - need to bind the first parameter..., since not calling buildExpr on the call.
            //sequence is always a constant, so no need to bind.
            CHqlBoundExpr boundName;
            buildExpr(ctx, &args.item(0), boundName);
            args.replace(*LINK(boundName.expr), 0);

            const CHqlBoundTarget * getTarget = targetAssign;
            CHqlBoundTarget tempTarget;
            if (!getTarget)
            {
                getTarget = &tempTarget;
                createTempFor(ctx, expr, tempTarget);
            }
            args.add(*createConstant((int)type->getSize()), 0);
            args.add(*getSizetConstant(type->getPrecision()),1);
            args.add(*createConstant(type->isSigned()),2);
            args.add(*getPointer(getTarget->expr), 3);
            callProcedure(ctx, getResultDecimalId, args);
            if (boundTarget)
                boundTarget->setFromTarget(*getTarget);
            return;
        }
    case type_row:      UNIMPLEMENTED; break; //should be translated to rawData.
    default:
        PrintLog("%d", ttc);
        throwUnexpectedX("No getResult defined for this type");
        break;
    }

    OwnedHqlExpr function = bindFunctionCall(func, args, overrideType);

    switch (ttc)
    {
    case type_qstring:
        {
            Owned<ITypeInfo> qstrType = makeQStringType(UNKNOWN_LENGTH);
            function.setown(ensureExprType(function, type));
            break;
        }
    case type_string:
    case type_varstring:
        {
            if (type->queryCollation()->queryName() != asciiAtom)
                function.setown(ensureExprType(function, type));
            break;
        }
    }

    if (boundTarget)
        buildExpr(ctx, function, *boundTarget);
    else
        buildExprAssign(ctx, *targetAssign, function);
}

void HqlCppTranslator::buildSetXmlSerializer(StringBuffer & helper, ITypeInfo * valueType)
{
    BuildCtx declarectx(*code, declareAtom);

    OwnedHqlExpr search = createQuoted("setXmlHelper", LINK(valueType));

    // stop duplicate classes being generated.
    // MORE: If this ever includes sorting/grouping, the dependence on a record will need to be revised
    HqlExprAssociation * match = declarectx.queryMatchExpr(search);
    if (match)
    {
        match->queryExpr()->toString(helper);
        return;
    }

    StringBuffer helperclass;
    unique_id_t id = getUniqueId();
    appendUniqueId(helper.append("r2x"), id);
    appendUniqueId(helperclass.append("cr2x"), id);

    BuildCtx r2xctx(declarectx);
    r2xctx.setNextPriority(XmlTransformerPrio);

    StringBuffer s, endText;
    s.append("struct ").append(helperclass).append(" : public ISetToXmlTransformer");
    endText.append(" ").append(helper).append(";");
    r2xctx.addQuotedCompound(s, endText.str());

    CHqlBoundExpr boundValue;
    boundValue.isAll.setown(createVariable("isAll", makeBoolType()));
    boundValue.length.setown(createVariable("length", LINK(sizetType)));
    boundValue.expr.setown(createVariable("self", makeReferenceModifier(LINK(valueType))));

    {
        MemberFunction func(*this, r2xctx, "virtual void toXML(bool isAll, size32_t length, const byte * self, IXmlWriter & out)");
        OwnedHqlExpr itemName = createConstant("Item");
        OwnedHqlExpr value = boundValue.getTranslatedExpr();
        buildXmlSerializeSetValues(func.ctx, value, itemName, true);
    }

    if (options.spanMultipleCpp)
    {
        StringBuffer helperFunc;
        createAccessFunctions(helperFunc, declarectx, XmlTransformerPrio, "ISetToXmlTransformer", helper);
        helper.clear().append(helperFunc).append("()");
    }

    OwnedHqlExpr name = createVariable(helper, makeVoidType());
    declarectx.associateExpr(search, name);
}

IWUResult * HqlCppTranslator::createWorkunitResult(int sequence, IHqlExpression * nameExpr)
{
    switch(sequence)
    {
    case ResultSequenceStored:
        {
            assertex(nameExpr);
            StringBuffer storedName;
            getStringValue(storedName, nameExpr);
            return wu()->updateVariableByName(storedName.str());
        }
    case ResultSequencePersist:
    case ResultSequenceInternal:
    case ResultSequenceOnce:
        return NULL;
    }
    assertex(sequence >= 0);

    StringBuffer resultName;
    getStringValue(resultName, nameExpr);
    if (resultName.length() == 0)
        resultName.append("Result ").append(sequence+1);

    Owned<IWUResult> result = wu()->updateResultBySequence(sequence);
    result->setResultName(resultName);
    return result.getClear();
}

void checkAppendXpathNamePrefix(StringArray &prefixes, const char *xpathName)
{
    if (!xpathName || !*xpathName)
        return;
    if (*xpathName=='@')
        xpathName++;
    if (*xpathName==':')
        return;
    const char *colon = strchr(xpathName, ':');
    if (!colon)
        return;
    StringAttr prefix;
    prefix.set(xpathName, colon-xpathName);
    if (prefixes.find(prefix.get())==NotFound)
        prefixes.append(prefix);
}

void gatherXpathPrefixes(StringArray &prefixes, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
        {
            //don't need to be too picky about xpath field types, worst case if an xpath is too long, we end up with an extra prefix
            StringBuffer xpathName, xpathItem;
            extractXmlName(xpathName, &xpathItem, NULL, cur, NULL, false);
            checkAppendXpathNamePrefix(prefixes, xpathName);
            checkAppendXpathNamePrefix(prefixes, xpathItem);

            ITypeInfo * type = cur->queryType();
            switch (type->getTypeCode())
            {
            case type_row:
            case type_dictionary:
            case type_table:
            case type_groupedtable:
                gatherXpathPrefixes(prefixes, cur->queryRecord());
                break;
            }
            break;
        }
        case no_ifblock:
            gatherXpathPrefixes(prefixes, cur->queryChild(1));
            break;
        case no_record:
            gatherXpathPrefixes(prefixes, cur);
            break;
        }
    }
}

void addDatasetResultXmlNamespaces(IWUResult &result, HqlExprArray &xmlnsAttrs, IHqlExpression *record)
{
    StringArray declaredPrefixes;
    ForEachItemIn(idx, xmlnsAttrs)
    {
        IHqlExpression & xmlns = xmlnsAttrs.item(idx);
        StringBuffer xmlnsPrefix;
        StringBuffer xmlnsURI;
        getUTF8Value(xmlnsPrefix, xmlns.queryChild(0));
        getUTF8Value(xmlnsURI, xmlns.queryChild(1));

        if (xmlnsURI.length())
        {
            if (xmlnsPrefix.length() && declaredPrefixes.find(xmlnsPrefix)==NotFound)
            {
                if (!validateXMLTag(xmlnsPrefix))
                    throwError1(HQLERR_InvalidXmlnsPrefix, xmlnsPrefix.str());
                declaredPrefixes.append(xmlnsPrefix);
            }
            result.setResultXmlns(xmlnsPrefix, xmlnsURI);
        }
    }
    StringArray usedPrefixes;
    if (record)
        gatherXpathPrefixes(usedPrefixes, record);
    ForEachItemIn(p, usedPrefixes)
    {
        const char *prefix = usedPrefixes.item(p);
        if (declaredPrefixes.find(prefix)==NotFound)
        {
            StringBuffer uri("urn:hpccsystems:ecl:unknown:");
            uri.append(prefix);
            result.setResultXmlns(prefix, uri);
        }
    }
}

void HqlCppTranslator::buildSetResultInfo(BuildCtx & ctx, IHqlExpression * originalExpr, IHqlExpression * value, ITypeInfo * type, bool isPersist, bool associateResult)
{
    IHqlExpression * seq = queryAttributeChild(originalExpr, sequenceAtom, 0);
    IHqlExpression * name = queryAttributeChild(originalExpr, namedAtom, 0);

    if (insideLibrary())
    {
        SCMStringBuffer libraryName;
        getOutputLibraryName(libraryName, wu());
        StringBuffer storedName;
        getStoredDescription(storedName, seq, name, true);
        throwError2(HQLERR_LibraryNoWorkunitWrite, libraryName.str(), storedName.str());
    }

    ITypeInfo * resultType = type ? type->queryPromotedType() : value->queryType()->queryPromotedType();
    Linked<ITypeInfo> schemaType = resultType;
    type_t retType = schemaType->getTypeCode();
    IIdAtom * func = NULL;
    CHqlBoundExpr valueToSave;
    LinkedHqlExpr castValue = value;
    LinkedHqlExpr size;
    switch(retType)
    {
    case type_int:
    case type_swapint:
        {
            bool isSigned = schemaType->isSigned();
            func = isSigned ? setResultIntId : setResultUIntId;
            schemaType.setown(makeIntType(8, isSigned));
            size.setown(getSizetConstant(schemaType->getSize()));
            break;
        }
    case type_boolean:  func = setResultBoolId; break;
    case type_string:   func = setResultStringId; schemaType.setown(makeStringType(UNKNOWN_LENGTH, NULL, NULL)); break;
    case type_unicode:  func = setResultUnicodeId; schemaType.setown(makeUnicodeType(UNKNOWN_LENGTH, 0)); break;
    case type_utf8:     func = setResultUnicodeId; schemaType.setown(makeUnicodeType(UNKNOWN_LENGTH, 0)); castValue.setown(ensureExprType(value, schemaType)); associateResult = false; break;
    case type_qstring:  func = setResultStringId; schemaType.setown(makeStringType(UNKNOWN_LENGTH, NULL, NULL)); break;
    case type_data:     func = setResultDataId; schemaType.setown(makeDataType(UNKNOWN_LENGTH)); break;
    case type_varstring:func = setResultVarStringId; schemaType.setown(makeStringType(UNKNOWN_LENGTH, NULL, NULL)); break;
    case type_varunicode:func = setResultVarUnicodeId; schemaType.setown(makeUnicodeType(UNKNOWN_LENGTH, 0)); break;
    case type_real:     func = setResultRealId; schemaType.setown(makeRealType(8)); break;
    case type_decimal:  func = setResultDecimalId; break;
    case type_row:
        {
            CHqlBoundExpr boundLength;
            OwnedHqlExpr serialized = ::ensureSerialized(value, diskAtom);
            func = setResultRawId;
            Owned<IReferenceSelector> ds = buildNewRow(ctx, serialized);
            OwnedHqlExpr size = createSizeof(ds->queryExpr());
            buildExpr(ctx, size, boundLength);
            ds->buildAddress(ctx, valueToSave);
            valueToSave.length.set(boundLength.expr);
            valueToSave.expr.setown(createValue(no_typetransfer, makeDataType(UNKNOWN_LENGTH), LINK(valueToSave.expr)));
            schemaType.set(schemaType->queryChildType());
            break;
        }
    case type_set:
        {
            func = setResultSetId;
            ITypeInfo * elementType = LINK(schemaType->queryChildType());
            if (!elementType)
                elementType = makeStringType(UNKNOWN_LENGTH, NULL, NULL);
            schemaType.setown(makeSetType(elementType));
        }
        break;
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        {
            throwUnexpected();
#if 0
            HqlExprArray args;
            args.append(*LINK(value));
            args.append(*createAttribute(sequenceAtom, LINK(seq)));
            if (name)
                args.append(*createAttribute(nameAtom, LINK(name)));
            OwnedHqlExpr createFile = createValue(no_output, makeVoidType(), args);
            buildStmt(ctx, createFile);
            // MORE - the file name should be a unique temporary...
            // MORE - associate a logical name with it
            // MORE - save the logical name in the workunit
#endif
            return;
        }
    case type_any:
        //Someone has used error instead of fail.  Don't do anything....
        if (value->getOperator() == no_fail)
        {
            buildStmt(ctx, value);
            return;
        }
        //fall through
    default:
        PrintLog("%d", retType);
        throwError(HQLERR_InvalidSetResultType);
    }

    HqlExprArray args;

    if (!valueToSave.expr)
    {
        LinkedHqlExpr cseValue = castValue;
        if (options.spotCSE)
            cseValue.setown(spotScalarCSE(cseValue, NULL, queryOptions().spotCseInIfDatasetConditions));

        if ((retType == type_set) && isComplexSet(resultType, castValue->isConstant()) && castValue->getOperator() == no_list && !isNullList(castValue))
        {
            CHqlBoundTarget tempTarget;
            createTempFor(ctx, resultType, tempTarget, typemod_none, FormatBlockedDataset);
            Owned<IHqlCppSetBuilder> builder = createTempSetBuilder(tempTarget.queryType()->queryChildType(), tempTarget.isAll);
            builder->buildDeclare(ctx);
            buildSetAssign(ctx, builder, castValue);
            builder->buildFinish(ctx, tempTarget);
            valueToSave.setFromTarget(tempTarget);
        }
        else
            buildSimpleExpr(ctx, cseValue, valueToSave);

        if (associateResult)
        {
            OwnedHqlExpr getResult = createGetResultFromSetResult(originalExpr);
            ctx.associateExpr(getResult, valueToSave);
        }
    }

    assertex(func);
    OwnedHqlExpr nameText = createResultName(name, isPersist);
    if (retType == type_decimal)
    {
        assertex(schemaType->getSize() != UNKNOWN_LENGTH);
        //An ugly exception because it takes an arbitrary length decimal.
        //This should be handled by having a decimal(unknown length) parameter to a function which passes size and precision
        CHqlBoundExpr boundName;
        buildExpr(ctx, nameText, boundName);
        args.append(*LINK(boundName.expr));
        args.append(*LINK(seq));
        args.append(*getBoundSize(valueToSave));
        args.append(*getSizetConstant(schemaType->getPrecision()));
        args.append(*createConstant(schemaType->isSigned()));
        args.append(*getPointer(valueToSave.expr));
        buildTranslatedFunctionCall(ctx, func, args);
    }
    else
    {
        args.append(*nameText.getLink());
        args.append(*LINK(seq));
        args.append(*valueToSave.getTranslatedExpr());
        if (func == setResultSetId)
        {
            StringBuffer helper, s;
            buildSetXmlSerializer(helper, resultType);
            s.clear().append("&").append(helper);
            args.append(*createQuoted(s, makeBoolType()));
        }
        else if (func == setResultIntId || func == setResultUIntId)
            args.append(*getSizetConstant(schemaType->getSize()));

        buildFunctionCall(ctx, func, args);
    }

    if(wu())
    {
        HqlExprArray xmlnsAttrs;
        gatherAttributes(xmlnsAttrs, xmlnsAtom, originalExpr);
        Owned<IWUResult> result;
        if (retType == type_row)
        {
            OwnedHqlExpr record = LINK(::queryRecord(schemaType));
            if (originalExpr->hasAttribute(noXpathAtom))
                record.setown(removeAttributeFromFields(record, xpathAtom));
            result.setown(createDatasetResultSchema(seq, name, record, xmlnsAttrs, false, false, 0));
            if (result)
                result->setResultTotalRowCount(1);
        }
        else
        {
            // Bit of a mess - should split into two procedures
            int sequence = (int) seq->queryValue()->getIntValue();
            result.setown(createWorkunitResult(sequence, name));
            if(result)
            {
                StringBuffer fieldName;
                SCMStringBuffer resultName;
                result->getResultName(resultName);
                const char * cur = resultName.str();
                while (*cur)
                {
                    unsigned char c = *cur++;
                    if (isalnum(c) || (c == '_'))
                        fieldName.append(c);
                    else if (isspace(c))
                        fieldName.append('_');
                }

                addDatasetResultXmlNamespaces(*result, xmlnsAttrs, NULL);

                MemoryBuffer schema;
                schema.append(fieldName.str());
                schemaType->serialize(schema);
                schema.append("").append((unsigned char) type_void);
                schema.append((unsigned)0);
                result->setResultSchemaRaw(schema.length(), schema.toByteArray());

                StringBuffer xml;
                {
                    XmlSchemaBuilder xmlbuilder(false);
                    xmlbuilder.addField(fieldName, *schemaType, false);
                    xmlbuilder.getXml(xml);
                }
                addSchemaResource(sequence, resultName.str(), xml.length()+1, xml.str());
            }
        }

        if (result)
        {
            ActivityInstance * activity = queryCurrentActivity(ctx);
            if (activity)
            {
                const char * graphName = activeGraph->name;
                result->setResultWriteLocation(graphName, activity->activityId);
            }

            IHqlExpression * format = originalExpr->queryAttribute(storedFieldFormatAtom);
            if (format)
            {
                ForEachChild(i, format)
                {
                    StringBuffer name;
                    StringBuffer value;
                    OwnedHqlExpr folded = foldHqlExpression(format->queryChild(i));
                    getHintNameValue(folded, name, value);
                    result->setResultFieldOpt(name, value);
                }
            }
        }
    }
}

void HqlCppTranslator::buildCompareClass(BuildCtx & ctx, const char * name, IHqlExpression * orderExpr, IHqlExpression * datasetLeft, IHqlExpression * datasetRight, IHqlExpression * selSeq)
{
    BuildCtx classctx(ctx);
    IHqlStmt * classStmt = beginNestedClass(classctx, name, "ICompare");

    {
        MemberFunction func(*this, classctx, "virtual int docompare(const void * _left, const void * _right) const" OPTIMIZE_FUNCTION_ATTRIBUTE);
        func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
        func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");
        func.ctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);

        bindTableCursor(func.ctx, datasetLeft, "left", no_left, selSeq);
        bindTableCursor(func.ctx, datasetRight, "right", no_right, selSeq);
        if (orderExpr->getOperator() == no_order)
            doBuildReturnCompare(func.ctx, orderExpr, no_order, false, false);
        else
            buildReturn(func.ctx, orderExpr);
    }

    endNestedClass(classStmt);
}


void HqlCppTranslator::buildCompareMemberLR(BuildCtx & ctx, const char * name, IHqlExpression * orderExpr, IHqlExpression * datasetLeft, IHqlExpression * datasetRight, IHqlExpression * selSeq)
{
    StringBuffer s;

    s.clear().append("virtual ICompare * query").append(name).append("() { return &").append(name).append("; }");
    ctx.addQuoted(s);

    buildCompareClass(ctx, name, orderExpr, datasetLeft, datasetRight, selSeq);
}


void HqlCppTranslator::buildCompareMember(BuildCtx & ctx, const char * name, IHqlExpression * cond, const DatasetReference & dataset)
{
    //MORE:Support multiple comparison fields.
    IHqlExpression * datasetExpr = dataset.queryDataset();
    OwnedHqlExpr seq = createDummySelectorSequence();
    OwnedHqlExpr leftSelect = createSelector(no_left, datasetExpr, seq);
    OwnedHqlExpr rightSelect = createSelector(no_right, datasetExpr, seq);
    IHqlExpression * left = dataset.mapCompound(cond, leftSelect);
    IHqlExpression * right = dataset.mapCompound(cond, rightSelect);
    OwnedHqlExpr compare = createValue(no_order, LINK(signedType), left, right);

    buildCompareMemberLR(ctx, name, compare, datasetExpr, datasetExpr, seq);
}

void HqlCppTranslator::buildCompareEqClass(BuildCtx & ctx, const char * name, IHqlExpression * orderExpr, IHqlExpression * datasetLeft, IHqlExpression * datasetRight, IHqlExpression * selSeq)
{
    BuildCtx classctx(ctx);
    IHqlStmt * classStmt = beginNestedClass(classctx, name, "ICompareEq");

    {
        MemberFunction func(*this, classctx, "virtual bool match(const void * _left, const void * _right) const");
        func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
        func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");
        func.ctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);

        bindTableCursor(func.ctx, datasetLeft, "left", no_left, selSeq);
        bindTableCursor(func.ctx, datasetRight, "right", no_right, selSeq);
        if (orderExpr->getOperator() == no_order)
            doBuildReturnCompare(func.ctx, orderExpr, no_eq, true, false);
        else
            buildReturn(func.ctx, orderExpr);
    }

    endNestedClass(classStmt);
}


void HqlCppTranslator::buildCompareEqMemberLR(BuildCtx & ctx, const char * name, IHqlExpression * orderExpr, IHqlExpression * datasetLeft, IHqlExpression * datasetRight, IHqlExpression * selSeq)
{
    StringBuffer s;

    s.clear().append("virtual ICompareEq * query").append(name).append("() { return &").append(name).append("; }");
    ctx.addQuoted(s);

    buildCompareEqClass(ctx, name, orderExpr, datasetLeft, datasetRight, selSeq);
}


void HqlCppTranslator::buildNaryCompareClass(BuildCtx & ctx, const char * name, IHqlExpression * expr, IHqlExpression * dataset, IHqlExpression * selSeq, IHqlExpression * rowsid)
{
    BuildCtx classctx(ctx);
    IHqlStmt * classStmt = beginNestedClass(classctx, name, "INaryCompareEq");

    {
        MemberFunction func(*this, classctx, "virtual bool match(unsigned numRows, const void * * _rows) const");
        func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _rows[0];");
        func.ctx.addQuotedLiteral("unsigned char * * rows = (unsigned char * *) _rows;");
        func.ctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);

        bindTableCursor(func.ctx, dataset, "left", no_left, selSeq);
        bindRows(func.ctx, no_left, selSeq, rowsid, dataset, "numRows", "rows", false);

        buildReturn(func.ctx, expr);
    }

    endNestedClass(classStmt);
}


void HqlCppTranslator::buildNaryCompareMember(BuildCtx & ctx, const char * name, IHqlExpression * expr, IHqlExpression * datasetLeft, IHqlExpression * selSeq, IHqlExpression * rowsid)
{
    StringBuffer s;

    s.clear().append("virtual INaryCompareEq * query").append(name).append("() { return &").append(name).append("; }");
    ctx.addQuoted(s);

    buildNaryCompareClass(ctx, name, expr, datasetLeft, selSeq, rowsid);
}


void HqlCppTranslator::buildCompareEqMember(BuildCtx & ctx, const char * name, IHqlExpression * cond, const DatasetReference & dataset)
{
    //MORE:Support multiple comparison fields.
    IHqlExpression * datasetExpr = dataset.queryDataset();
    OwnedHqlExpr seq = createDummySelectorSequence();
    OwnedHqlExpr leftSelect = createSelector(no_left, datasetExpr, seq);
    OwnedHqlExpr rightSelect = createSelector(no_right, datasetExpr, seq);
    IHqlExpression * left = dataset.mapCompound(cond, leftSelect);
    IHqlExpression * right = dataset.mapCompound(cond, rightSelect);
    OwnedHqlExpr compare = createValue(no_order, LINK(signedType), left, right);

    buildCompareEqMemberLR(ctx, name, compare, datasetExpr, datasetExpr, seq);
}

void HqlCppTranslator::buildOrderedCompare(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * sorts, CHqlBoundExpr & bound, IHqlExpression * leftDataset, IHqlExpression * rightDataset)
{
    //MORE: This needs to be more intelligent to deal with related datasets (child datasets will work ok).

    //MORE: Should create a member function if a member of a class.
    //Otherwise a function + generate an error if it uses fields outside of the context?  
    //Would also need to pass in parent cursors for a global function since parent will not be available.

    //MORE:Support multiple comparison fields.
    OwnedHqlExpr compare = createOrderFromSortList(DatasetReference(dataset), sorts, leftDataset, rightDataset);

    buildTempExpr(ctx, compare, bound);
}

void HqlCppTranslator::buildHashClass(BuildCtx & ctx, const char * name, IHqlExpression * orderExpr, const DatasetReference & dataset)
{
    StringBuffer s;

    s.clear().append("virtual IHash * query").append(name).append("() { return &").append(name).append("; }");
    ctx.addQuoted(s);

    BuildCtx classctx(ctx);
    IHqlStmt * classStmt = beginNestedClass(classctx, name, "IHash");

    {
        MemberFunction hashFunc(*this, classctx, "virtual unsigned hash(const void * _self)");
        hashFunc.ctx.addQuotedLiteral("const unsigned char * self = (const unsigned char *) _self;");

        bindTableCursor(hashFunc.ctx, dataset.queryDataset(), "self", dataset.querySide(), dataset.querySelSeq());
        OwnedITypeInfo returnType = makeIntType(4, false);
        buildReturn(hashFunc.ctx, orderExpr, returnType);
    }

    endNestedClass(classStmt);
}

static void buildCompareMemberFunction(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * sortList, const DatasetReference & dataset)
{
    MemberFunction func(translator, ctx, "virtual int docompare(const void * _left, const void * _right) const" OPTIMIZE_FUNCTION_ATTRIBUTE);
    func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
    func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");
    func.ctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);

    translator.buildReturnOrder(func.ctx, sortList, dataset);
}

void HqlCppTranslator::buildCompareClass(BuildCtx & ctx, const char * name, IHqlExpression * sortList, const DatasetReference & dataset, StringBuffer & compareFuncName)
{
    if (options.useGlobalCompareClass)
    {
        BuildCtx buildctx(*code, declareAtom);

        // stop duplicate classes being generated.
        OwnedHqlExpr searchKey = createAttribute(noSortAtom, LINK(sortList), LINK(dataset.queryDataset()->queryRecord()));
        HqlExprAssociation * match = buildctx.queryMatchExpr(searchKey);
        if (match)
        {
            match->queryExpr()->toString(compareFuncName);
            return;
        }
        BuildCtx classctx(buildctx);
        StringBuffer instanceName, startText, endText;

        // Create global compare class
        unsigned id = getNextGlobalCompareId();
        instanceName.append("compare").append(id);
        startText.append("struct Compare").append(id).append(" : public ICompare");
        endText.append(" ").append(instanceName).append(";");
        classctx.addQuotedCompound(startText,endText);

        if (options.spanMultipleCpp)
        {
            BuildCtx declarectx(*code, declareAtom);
            StringBuffer helperFunc;
            createAccessFunctions(helperFunc, declarectx,  BuildCtx::NormalPrio, "ICompare", instanceName);
            compareFuncName.set(helperFunc).append("()");
        }
        else
            compareFuncName.set(instanceName);

        buildCompareMemberFunction(*this, classctx, sortList, dataset);
        OwnedHqlExpr temp = createVariable(compareFuncName, makeVoidType());
        buildctx.associateExpr(searchKey, temp);
    }
    else
    {
        BuildCtx comparectx(ctx);
        IHqlStmt * classStmt = beginNestedClass(comparectx, name, "ICompare");
        buildCompareMemberFunction(*this, comparectx, sortList, dataset);
        endNestedClass(classStmt);
        compareFuncName.set(name);
    }
}


void HqlCppTranslator::buildHashOfExprsClass(BuildCtx & ctx, const char * name, IHqlExpression * cond, const DatasetReference & dataset, bool compareToSelf)
{
    IHqlExpression * attr = compareToSelf ? createAttribute(internalAtom) : NULL;
    OwnedHqlExpr hash = createValue(no_hash32, LINK(unsignedType), LINK(cond), attr);

    buildHashClass(ctx, name, hash, dataset);
}


void HqlCppTranslator::buildDictionaryHashClass(IHqlExpression *record, StringBuffer &funcName)
{
    BuildCtx declarectx(*code, declareAtom);
    OwnedHqlExpr attr = createAttribute(lookupAtom, LINK(record));
    HqlExprAssociation * match = declarectx.queryMatchExpr(attr);
    if (match)
        match->queryExpr()->toString(funcName);
    else
    {
        StringBuffer lookupHelperName;
        appendUniqueId(lookupHelperName.append("lu"), getConsistentUID(record));

        BuildCtx classctx(declarectx);
        classctx.setNextPriority(TypeInfoPrio);

        IHqlStmt * classStmt = beginNestedClass(classctx, lookupHelperName, "IHThorHashLookupInfo");
        OwnedHqlExpr searchRecord = getDictionarySearchRecord(record);
        OwnedHqlExpr keyRecord = getDictionaryKeyRecord(record);

        HqlExprArray keyedSourceFields;
        HqlExprArray keyedDictFields;
        OwnedHqlExpr source = createDataset(no_null, LINK(searchRecord));
        DatasetReference sourceRef(source, no_none, NULL);
        OwnedHqlExpr dict = createDataset(no_null, LINK(record));
        DatasetReference dictRef(dict, no_none, NULL);

        ForEachChild(idx, searchRecord)
        {
            IHqlExpression *child = searchRecord->queryChild(idx);
            if (!child->isAttribute())
                keyedSourceFields.append(*createSelectExpr(LINK(source->queryNormalizedSelector()), LINK(child)));
        }
        ForEachChild(idx2, keyRecord)
        {
            IHqlExpression *child = keyRecord->queryChild(idx2);
            if (!child->isAttribute())
                keyedDictFields.append(*createSelectExpr(LINK(dict->queryNormalizedSelector()), LINK(child)));
        }
        OwnedHqlExpr keyedSourceList = createValueSafe(no_sortlist, makeSortListType(NULL), keyedSourceFields);
        OwnedHqlExpr keyedDictList = createValueSafe(no_sortlist, makeSortListType(NULL), keyedDictFields);

        buildHashOfExprsClass(classctx, "HashLookup", keyedSourceList, sourceRef, false);
        buildHashOfExprsClass(classctx, "Hash", keyedDictList, dictRef, false);

        OwnedHqlExpr seq = createDummySelectorSequence();
        OwnedHqlExpr leftSelect = createSelector(no_left, source, seq);
        OwnedHqlExpr rightSelect = createSelector(no_right, dict, seq);
        IHqlExpression * left = sourceRef.mapCompound(keyedSourceList, leftSelect);
        IHqlExpression * right = dictRef.mapCompound(keyedDictList, rightSelect);
        OwnedHqlExpr compare = createValue(no_order, LINK(signedType), left, right);

        buildCompareMemberLR(classctx, "CompareLookup", compare, source, dict, seq);
        buildCompareMember(classctx, "Compare", keyedDictList, dictRef);
        endNestedClass(classStmt);

        if (queryOptions().spanMultipleCpp)
        {
            createAccessFunctions(funcName, declarectx, BuildCtx::NormalPrio, "IHThorHashLookupInfo", lookupHelperName);
            funcName.append("()");
        }
        else
            funcName.append(lookupHelperName);
        OwnedHqlExpr func = createVariable(funcName, makeVoidType());
        declarectx.associateExpr(attr, func);
    }
}

void HqlCppTranslator::buildDictionaryHashMember(BuildCtx & ctx, IHqlExpression *dictionary, const char * memberName)
{
    StringBuffer lookupHelperName;
    buildDictionaryHashClass(dictionary->queryRecord(), lookupHelperName);

    BuildCtx funcctx(ctx);
    StringBuffer s;
    s.append("virtual IHThorHashLookupInfo * ").append(memberName).append("() { return &").append(lookupHelperName).append("; }");
    funcctx.addQuoted(s);
}


//---------------------------------------------------------------------------

IHqlExpression * queryImplementationInterface(IHqlExpression * moduleFunc)
{
    IHqlExpression * module = moduleFunc->queryChild(0);
    IHqlExpression * library = module->queryAttribute(libraryAtom);
    if (library)
        return library->queryChild(0);
    return moduleFunc;
}

bool isLibraryScope(IHqlExpression * expr)
{
    while (expr->getOperator() == no_comma)
        expr = expr->queryChild(1);
    return expr->isScope();
}

bool HqlCppTranslator::prepareToGenerate(HqlQueryContext & query, WorkflowArray & actions, bool isEmbeddedLibrary)
{
    bool createLibrary = isLibraryScope(query.expr);

    if (createLibrary)
    {
        if (query.expr->getOperator() != no_funcdef)
            throwError(HQLERR_LibraryMustBeFunctional);

        ::Release(outputLibrary);
        outputLibrary = NULL;
        outputLibraryId.setown(createAttribute(graphAtom, getSizetConstant(nextActivityId())));
        outputLibrary = new HqlCppLibraryImplementation(*this, queryImplementationInterface(query.expr), outputLibraryId, targetClusterType);

        if (!isEmbeddedLibrary)
        {
            wu()->setLibraryInformation(wu()->queryJobName(), outputLibrary->getInterfaceHash(), getLibraryCRC(query.expr));
        }
    }
    else
    {
        if (options.applyInstantEclTransformations)
            query.expr.setown(doInstantEclTransformations(query.expr, options.applyInstantEclTransformationsLimit));
    }

    if (!transformGraphForGeneration(query, actions))
        return false;

    return true;
}


void HqlCppTranslator::updateClusterType()
{
    const char * clusterTypeText="?";
    switch (targetClusterType)
    {
    case ThorLCRCluster:
         clusterTypeText = "thorlcr";
         break;
     case HThorCluster:
         clusterTypeText = "hthor";
         break;
     case RoxieCluster:
         clusterTypeText = "roxie";
         break;
    }
    //ensure targetClusterType is consistently set in the work unit - so library code can use it.
    wu()->setDebugValue("targetClusterType", clusterTypeText, true);
}


struct CountEntry : public CInterface
{
    ThorActivityKind from;
    ThorActivityKind to;
    unsigned cnt;
};

int compareItems(CInterface * * _l, CInterface * * _r)
{
    CountEntry * l = (CountEntry *)*_l;
    CountEntry * r = (CountEntry *)*_r;
    return (int)(r->cnt - l->cnt);
}


void dumpActivityCounts()
{
#ifdef _GATHER_USAGE_STATS
    CIArray items;
    for (unsigned i = TAKnone; i < TAKlast; i++)
    {
        for (unsigned j = TAKnone; j < TAKlast; j++)
        {
            if (activityCounts[i][j])
            {
                CountEntry & next = * new CountEntry;
                next.from = (ThorActivityKind)i;
                next.to = (ThorActivityKind)j;
                next.cnt = activityCounts[i][j];
                items.append(next);
            }
        }
    }

    items.sort(compareItems);

    FILE * out = fopen("stats.txt", "w");
    if (!out)
        return;
    fprintf(out, "-Count-  Source  -> Sink\n");
    ForEachItemIn(k, items)
    {
        CountEntry & cur = (CountEntry &)items.item(k);
        fprintf(out, "%8d %s -> %s\n", cur.cnt, getActivityText(cur.from), getActivityText(cur.to));
    }
    fclose(out);
#endif
}



bool HqlCppTranslator::buildCode(HqlQueryContext & query, const char * embeddedLibraryName, const char * embeddedGraphName)
{
    WorkflowArray workflow;
    bool ok = prepareToGenerate(query, workflow, (embeddedLibraryName != NULL));
    if (ok)
    {
        //This is done late so that pickBestEngine has decided which engine we are definitely targeting.
        if (!embeddedLibraryName)
            updateClusterType();

        if (insideLibrary())
        {
            //always do these checks for consistency
            OwnedHqlExpr graph;
            ForEachItemIn(i, workflow)
            {
                WorkflowItem & cur = workflow.item(i);
                if (!cur.isFunction())
                {
                    assertex(!graph);
                    HqlExprArray & exprs = cur.queryExprs();
                    assertex(exprs.ordinality() == 1);
                    graph.set(&exprs.item(0));
                    assertex(graph->getOperator() == no_thor);
                }
            }

            //More: this should be cleaned up - with a flag in the workflow items to indicate a library graph instead...
            if (embeddedLibraryName)
            {
                ForEachItemIn(i, workflow)
                {
                    WorkflowItem & cur = workflow.item(i);
                    if (cur.isFunction())
                    {
                        OwnedHqlExpr function = cur.getFunction();
                        buildFunctionDefinition(function);
                    }
                }

                BuildCtx ctx(*code, goAtom);
                buildLibraryGraph(ctx, graph, embeddedGraphName);
            }
            else
                buildWorkflow(workflow);
        }
        else
            buildWorkflow(workflow);

        if (options.calculateComplexity)
        {
            cycle_t startCycles = get_cycles_now();
            StringBuffer complexityText;
            complexityText.append(getComplexity(workflow));
            wu()->setDebugValue("__Calculated__Complexity__", complexityText, true);
            if (options.timeTransforms)
                noteFinishedTiming("compile:complexity", startCycles);
        }

        buildRowAccessors();
    }

    ::Release(outputLibrary);
    outputLibrary = NULL;
    outputLibraryId.clear();
    return ok;
}

bool HqlCppTranslator::buildCpp(IHqlCppInstance & _code, HqlQueryContext & query)
{
    if (!internalScope)
        return false;

    try
    {
        wu()->setCodeVersion(ACTIVITY_INTERFACE_VERSION,BUILD_TAG,LANGUAGE_VERSION);
        cacheOptions();

        if (options.obfuscateOutput)
        {
            Owned<IWUQuery> query = wu()->updateQuery();
            query->setQueryText(NULL);
        }

        useLibrary(ECLRTL_LIB);
        useInclude("eclrtl.hpp");

        HqlExprArray internalLibraries;
        query.expr.setown(separateLibraries(query.expr, internalLibraries));

        //General internal libraries first, in dependency order
        ForEachItemIn(i, internalLibraries)
        {
            IHqlExpression & cur = internalLibraries.item(i);
            assertex(cur.getOperator() == no_funcdef);
            IHqlExpression * moduleExpr = cur.queryChild(0);
            IHqlExpression * definition = queryAttributeChild(moduleExpr, internalAtom, 0);
            IHqlExpression * name = queryAttributeChild(moduleExpr, nameAtom, 0);

            StringBuffer internalLibraryName;
            name->queryValue()->getStringValue(internalLibraryName);
            //Use a graph number that couldn't possibly clash with any graphs generated by the main query
            StringBuffer embeddedGraphName;
            embeddedGraphName.append("graph").append(EMBEDDED_GRAPH_DELTA+i+1);

            overrideOptionsForLibrary();
            HqlQueryContext libraryQuery;
            libraryQuery.expr.set(definition);

            if (!buildCode(libraryQuery, internalLibraryName.str(), embeddedGraphName.str()))
                return false;
        }

        if (isLibraryScope(query.expr))
            overrideOptionsForLibrary();
        else
            overrideOptionsForQuery();
        if (!buildCode(query, NULL, NULL))
            return false;

        //Return early if iteratively generating the field usage statistics
        if (getDebugFlag("generateFullFieldUsage", false))
            return false;

    #ifdef _GATHER_USAGE_STATS
        if (getDebugFlag("dumpActivityCounts", false))
            dumpActivityCounts();
    #endif

        ForEachItemIn(i1, globalFiles)
            globalFiles.item(i1).writeToGraph();

        //Have to submit graphs to work unit right at the end so that globalUsage counts etc. can be updated in them.
        ForEachItemIn(i2, graphs)
        {
            GeneratedGraphInfo & cur = graphs.item(i2);
            wu()->createGraph(cur.name, cur.label, GraphTypeActivities, cur.xgmml.getClear());
        }

        code->processIncludes();
        if (options.peephole)
        {
            cycle_t startCycles = get_cycles_now();
            peepholeOptimize(*code, *this);
            if (options.timeTransforms)
                noteFinishedTiming("compile:transform:peephole", startCycles);
        }
    }
    catch (IException * e)
    {
        ensureWorkUnitUpdated();
        if (e->errorCode() != HQLERR_ErrorAlreadyReported)
            throw;
        e->Release();
        return false;
    }
    catch (...)
    {
        ensureWorkUnitUpdated();
        throw;
    }
    ensureWorkUnitUpdated();


    return true;
}

class WuTimingUpdater : implements ITimeReportInfo
{
public:
    WuTimingUpdater(IWorkUnit * _wu) { wu = _wu; }

    virtual void report(const char * scope, const char * description, const __int64 totaltime, const __int64 maxtime, const unsigned count)
    {
        StatisticScopeType scopeType = SSTcompilestage;
        StatisticKind kind = StTimeTotalExecute;
        wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scope, kind, description, totaltime, count, maxtime, StatsMergeReplace);
    }

protected:
    IWorkUnit * wu;
};


void HqlCppTranslator::ensureWorkUnitUpdated()
{
    if (timeReporter)
    {
        WuTimingUpdater updater(wu());
        timeReporter->report(updater);
    }
}

double HqlCppTranslator::getComplexity(IHqlExpression * expr, ClusterType cluster)
{
    double complexity = 0;
    switch (expr->getOperator())
    {
    case no_sequential:
    case no_comma:
    case no_compound:
    case no_parallel:
    case no_actionlist:
    case no_orderedactionlist:
        break;
    case no_thor:
        {
            OwnedHqlExpr resourced = getResourcedGraph(expr->queryChild(0), NULL);
            lockTransformMutex();
            complexity = getComplexity(resourced, targetClusterType);
            unlockTransformMutex();
            return complexity;
        }
    case no_subgraph:
        return getComplexity(expr->queryChild(0), cluster);
    case no_selfjoin:
    case no_join:
        if (isLocalActivity(expr) || !isThorCluster(cluster))
            complexity = 1;
        else
            complexity = 5;
        break;
    case no_distribute:
        if (isThorCluster(cluster))
            complexity = 5;
        break;
    case no_subsort:
        complexity = 1;
        break;
    case no_sort:
        if (isLocalActivity(expr) || !isThorCluster(cluster))
            complexity = 2;
        else
            complexity = 5;
        break;
    case no_ensureresult:
        if (options.freezePersists)
            return 0;
        if (expr->queryChild(0)->queryType()->getTypeCode() == type_void)
            return getComplexity(expr->queryChild(0), cluster);
        //fall through..
    case no_setresult:
        {
            if (cluster == NoCluster)
                return 0.05;
            return 1;
        }
    case no_split:
        //Only count the parents of a splitter once
        if (expr->queryTransformExtra())
            return 0;
        expr->setTransformExtraUnlinked(expr);
        complexity = 1;
        break;
    case no_forcelocal:
        return getComplexity(expr->queryChild(0), cluster);
    default:
        complexity = 1;
        break;
    }

    ForEachChildActivity(i, expr)
        complexity += getComplexity(queryChildActivity(expr, i), cluster);
    return complexity;
}


double HqlCppTranslator::getComplexity(IHqlCppInstance & _code, IHqlExpression * exprlist)
{
    WorkflowArray workflow;

    HqlQueryContext query;
    query.expr.set(exprlist);
    if (!prepareToGenerate(query, workflow, false))
        return 0;

    return getComplexity(workflow);
}

double HqlCppTranslator::getComplexity(WorkflowArray & workflow)
{
    double complexity = 0;
    ForEachItemIn(i, workflow)
        complexity += getComplexity(workflow.item(i).queryExprs());
    return complexity;
}

double HqlCppTranslator::getComplexity(HqlExprArray & exprs)
{
    double complexity = 0;
    ForEachItemIn(idx, exprs)
        complexity += getComplexity(&exprs.item(idx), NoCluster);
    return complexity;
}

//---------------------------------------------------------------------------------------------------------------------

static int compareTrackedSourceByName(CInterface * const * _left, CInterface * const * _right)
{
    SourceFieldUsage & left = static_cast<SourceFieldUsage &>(**_left);
    SourceFieldUsage & right = static_cast<SourceFieldUsage &>(**_right);

    const char * leftName = left.queryFilenameText();
    const char * rightName = right.queryFilenameText();
    return stricmp(leftName, rightName);
}

IPropertyTree * HqlCppTranslator::gatherFieldUsage(const char * variant, const IPropertyTree * exclude)
{
    Owned<IPropertyTree> sources = createPTree("usedsources");
    sources->setProp("@varient", variant);
    trackedSources.sort(compareTrackedSourceByName);
    ForEachItemIn(i, trackedSources)
    {
        IPropertyTree * next = trackedSources.item(i).createReport(options.reportFieldUsage || options.recordFieldUsage, exclude);
        if (next)
            sources->addPropTree(next->queryName(), next);
    }

    return sources.getClear();
}

SourceFieldUsage * HqlCppTranslator::querySourceFieldUsage(IHqlExpression * expr)
{
    if (!(options.reportFieldUsage || options.recordFieldUsage || options.reportFileUsage) || !expr)
        return NULL;

    if (expr->hasAttribute(_spill_Atom) || expr->hasAttribute(jobTempAtom))
        return NULL;

    OwnedHqlExpr normalized = removeAttribute(expr, _uid_Atom);
    IHqlExpression * original = normalized->queryAttribute(_original_Atom);
    if (original)
    {
        OwnedHqlExpr normalTable = removeAttribute(original->queryChild(0), _uid_Atom);
        OwnedHqlExpr normalOriginal = replaceChild(original, 0, normalTable);
        normalized.setown(replaceOwnedAttribute(normalized, normalOriginal.getClear()));
    }

    ForEachItemIn(i, trackedSources)
    {
        SourceFieldUsage & cur = trackedSources.item(i);
        if (cur.matches(normalized))
            return &cur;
    }
    SourceFieldUsage * next = new SourceFieldUsage(normalized);
    trackedSources.append(*next);
    return next;
}

void HqlCppTranslator::noteAllFieldsUsed(IHqlExpression * expr)
{
    SourceFieldUsage * match = querySourceFieldUsage(expr);
    if (match)
        match->noteAll();
}

void HqlCppTranslator::writeFieldUsage(const char * targetDir, IPropertyTree * source, const char * variant)
{
    if (source)
    {
        StringBuffer fullname;
        addDirectoryPrefix(fullname, targetDir).append(soName).append("_fieldusage");
        if (variant)
            fullname.append("_").append(variant);
        fullname.append(".xml");

        saveXML(fullname, source);

        Owned<IWUQuery> query = wu()->updateQuery();
        associateLocalFile(query, FileTypeXml, fullname, "FieldUsage", 0);
    }
}


void HqlCppTranslator::generateStatistics(const char * targetDir, const char * variant)
{
    if (options.reportFieldUsage || options.reportFileUsage)
    {
        Owned<IPropertyTree> sources = gatherFieldUsage(variant, NULL);
        writeFieldUsage(targetDir, sources, NULL);
    }
    
    if (options.recordFieldUsage)
    {
        Owned<IPropertyTree> sources = gatherFieldUsage(variant, NULL);
        wu()->noteFieldUsage(LINK(sources));
    }
}

//---------------------------------------------------------------------------------------------------------------------

BoundRow * HqlCppTranslator::resolveDatasetRequired(BuildCtx & ctx, IHqlExpression * expr)
{
    BoundRow * cursor = resolveSelectorDataset(ctx, expr);
    if (!cursor)
    {
        StringBuffer tablename;
        tablename.append(expr->queryName());
        if (tablename.length() == 0)
            getExprECL(expr, tablename);
        if (ctx.queryFirstAssociation(AssocCursor))
            throwError1(HQLERR_CouldNotFindDataset, tablename.str());
        throwError1(HQLERR_CouldNotAnyDatasetX, tablename.str());
    }
    return cursor;
}

IReferenceSelector * HqlCppTranslator::buildActiveReference(BuildCtx & ctx, IHqlExpression * expr)
{
    ITypeInfo * type = expr->queryType();
    assertex(type);
    switch (type->getTypeCode())
    {
    case type_row:
        return buildActiveRow(ctx, expr);
    }
    return buildReference(ctx, expr);
}


IReferenceSelector * HqlCppTranslator::createReferenceSelector(BoundRow * cursor, IHqlExpression * path)
{
    return new DatasetSelector(*this, cursor, path);
}

IReferenceSelector * HqlCppTranslator::createReferenceSelector(BoundRow * cursor)
{
    return new DatasetSelector(*this, cursor, cursor->querySelector());
}

IReferenceSelector * HqlCppTranslator::buildReference(BuildCtx & ctx, IHqlExpression * expr)
{
    ITypeInfo * type = expr->queryType();
    assertex(type);
    switch (type->getTypeCode())
    {
    case type_row:
        return buildNewRow(ctx, expr);
    }

    const node_operator op = expr->getOperator();
    switch (op)
    {
    case no_variable:
    case no_field:
    case no_ifblock:
        throwUnexpectedOp(op);
    case no_select:
        {
            IHqlExpression * ds = expr->queryChild(0);
#ifdef _DEBUG
            //Here to make tracing easier in a debugger
            IHqlExpression * field = expr->queryChild(1);
#endif
            Owned<IReferenceSelector> selector;
            if (isNewSelector(expr))
            {
                //could optimize selection from a project of an in-scope dataset.
                if (((ds->getOperator() == no_newusertable) || (ds->getOperator() == no_hqlproject)) &&
                    isAlwaysActiveRow(ds->queryChild(0)))
                {
                    //MORE: could optimize selection from a project of an in-scope dataset.
                }
                selector.setown(buildNewRow(ctx, ds));
            }
            else
                selector.setown(buildActiveRow(ctx, ds));
            return selector->select(ctx, expr);
        }
    }

    BoundRow * cursor = resolveDatasetRequired(ctx, expr);
    IReferenceSelector * alias = cursor->queryAlias();
    if (alias)
        return LINK(alias);

    IHqlExpression * dataset = cursor->queryDataset();
    return createReferenceSelector(cursor, dataset);
}

ABoundActivity * HqlCppTranslator::buildActivity(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    checkAbort();
    ErrorSeverityMapper::Scope saved(*localOnWarnings);

    //Process any annotations first - but still pass the original expr to the doBuildActivtyXXX functions.
    IHqlExpression * cur = expr;
    for (;;)
    {
        IHqlExpression * body = cur->queryBody(true);
        if (cur == body)
            break;

        switch (cur->getAnnotationKind())
        {
        case annotate_meta:
            localOnWarnings->processMetaAnnotation(cur);  // state restored by OnWarningStateBlock
            break;
        case annotate_symbol:
            {
#ifdef SPOT_POTENTIAL_COMMON_ACTIVITIES
                if (cur->getStartLine() && cur->queryName()->str()[0] != '_')
                {
                    unsigned match = savedActivityLocations.findLocation(cur);
                    if (match == NotFound)
                    {
                        savedActivityLocations.append(*LINK(cur));
                        savedActivities.append(*LINK(expr));
                    }
                    else
                    {
                        debugFindFirstDifference(cur, &savedActivities.item(match));
                        //MORE: Could add some kind of comment.
                    }
                }
#endif
                localOnWarnings->setSymbol(cur);
                break;
            }
        }
        cur = body;
    }

    if (isCompoundSource(expr))
    {
        OwnedHqlExpr mapped = normalizeAnyDatasetAliases(expr);
        if (mapped != expr)
            return buildActivity(ctx, mapped, isRoot);
    }

    ABoundActivity * result;
    try
    {
        node_operator op = expr->getOperator();
        switch (op)
        {
            case no_merge:
                result = doBuildActivityMerge(ctx, expr);
                break;
            case no_nwaymerge:
                result = doBuildActivityNWayMerge(ctx, expr);
                break;
            case no_mergejoin:
            case no_nwayjoin:
                result = doBuildActivityNWayMergeJoin(ctx, expr);
                break;
            case no_rowsetrange:
                result = doBuildActivityRowsetRange(ctx, expr);
                break;
            case no_rowsetindex:
                result = doBuildActivityRowsetIndex(ctx, expr);
                break;
            case no_datasetlist:
                {
                    OwnedHqlExpr allExpr = createValue(no_all, makeSetType(LINK(sizetType)));
                    result = doBuildActivityRowsetRange(ctx, expr, expr, allExpr);
                    break;
                }
            case no_addfiles:
    //          result = doBuildActivityChildDataset(ctx, expr);
    //          break;
                result = doBuildActivityConcat(ctx, expr);
                break;
            case no_regroup:
                result = doBuildActivityRegroup(ctx, expr);
                break;
            case no_nonempty:
                result = doBuildActivityNonEmpty(ctx, expr);
                break;
            case no_combine:
                result = doBuildActivityCombine(ctx, expr);
                break;
            case no_combinegroup:
                result = doBuildActivityCombineGroup(ctx, expr);
                break;
            case no_rollupgroup:
                result = doBuildActivityRollupGroup(ctx, expr);
                break;
            case no_filtergroup:
                result = doBuildActivityFilterGroup(ctx, expr);
                break;
            case no_cachealias:
                result = doBuildActivityCacheAlias(ctx, expr);
                break;
            case no_cloned:
                result = doBuildActivityCloned(ctx, expr);
                break;
            case no_distribute:
            case no_assertdistributed:
                result = doBuildActivityDistribute(ctx, expr);
                break;
            case no_keyeddistribute:
                result = doBuildActivityKeyedDistribute(ctx, expr);
                break;
            case no_select:
                if (isNewSelector(expr))
                    result = doBuildActivitySelectNew(ctx, expr);
                else
                    result = doBuildActivityChildDataset(ctx, expr);
                break;
                //Items in this list need to also be in the list inside doBuildActivityChildDataset
            case no_call:
            case no_externalcall:
                if (expr->isAction())
                    result = doBuildActivityAction(ctx, expr, isRoot);
                else if (expr->isDatarow())
                    result = doBuildActivityCreateRow(ctx, expr, false);
                else if (hasStreamedModifier(expr->queryType()))
                {
                    result = doBuildActivityStreamedCall(ctx, expr);
                }
                else if (hasLinkCountedModifier(expr->queryType()))
                {
                    result = doBuildActivityLinkedRawChildDataset(ctx, expr);
                }
                else
                    result = doBuildActivityChildDataset(ctx, expr);
                break;
            case no_left:
            case no_right:
            case no_top:
            case no_activetable:
            case no_id2blob:
            case no_typetransfer:
            case no_rows:
            case no_xmlproject:
            case no_libraryinput:
            case no_translated:
                if (expr->isDatarow())
                    result = doBuildActivityCreateRow(ctx, expr, false);
                else
                    result = doBuildActivityChildDataset(ctx, expr);
                break;
            case no_compound_inline:
                result = doBuildActivityChildDataset(ctx, expr->queryChild(0));
                break;
            case no_dataset_from_transform:
                result = doBuildActivityCountTransform(ctx, expr);
                break;
            case no_table:
                result = doBuildActivityTable(ctx, expr);
                break;
            case no_compound_diskread:
                result = doBuildActivityDiskRead(ctx, expr);
                break;
            case no_compound_diskaggregate:
            case no_compound_diskcount:
                result = doBuildActivityDiskAggregate(ctx, expr);
                break;
            case no_compound_diskgroupaggregate:
                result = doBuildActivityDiskGroupAggregate(ctx, expr);
                break;
            case no_compound_disknormalize:
                result = doBuildActivityDiskNormalize(ctx, expr);
                break;
            case no_compound_childread:
            case no_compound_childnormalize:
                result = doBuildActivityChildNormalize(ctx, expr);
                break;
            case no_compound_childaggregate:
            case no_compound_childcount:
                result = doBuildActivityChildAggregate(ctx, expr);
                break;
            case no_compound_childgroupaggregate:
                result = doBuildActivityChildGroupAggregate(ctx, expr);
                break;
            case no_compound_selectnew:
                result = doBuildActivityCompoundSelectNew(ctx, expr);
                break;
            case no_denormalize:
            case no_denormalizegroup:
                result = doBuildActivityDenormalize(ctx, expr);
                break;
            case no_datasetfromrow:
                {
                    OwnedHqlExpr row = expr->cloneAllAnnotations(expr->queryChild(0));  // preserve any position information....
                    if (worthGeneratingRowAsSingleActivity(row))
                        result = doBuildActivityCreateRow(ctx, row, false);
                    else
                        result = buildCachedActivity(ctx, row);
                    break;
                }
            case no_datasetfromdictionary:
                result = doBuildActivityChildDataset(ctx, expr);
                break;
            case no_temptable:
                result = doBuildActivityTempTable(ctx, expr);
                break;
            case no_inlinetable:
                result = doBuildActivityInlineTable(ctx, expr);
                break;
            case no_temprow:
                throwUnexpected();
                break;
            case no_workunit_dataset:
                if (targetRoxie() && queryCurrentActivity(ctx))
                    result = doBuildActivityChildDataset(ctx, expr);
                else
                    result = doBuildActivityWorkunitRead(ctx, expr);
                break;
            case no_fail:
                if (expr->isAction())
                    result = doBuildActivityAction(ctx, expr, isRoot);
                else
                    result = doBuildActivitySideEffect(ctx, expr, isRoot, true);
                break;
            case no_null:
                if (expr->isDatarow())
                    result = doBuildActivityCreateRow(ctx, expr, false);
                else
                    result = doBuildActivityNull(ctx, expr, isRoot);
                break;
            case no_split:
                result = doBuildActivitySplit(ctx, expr);
                break;
            case no_apply:
                result = doBuildActivityApply(ctx, expr, isRoot);
                break;
            case no_output:
                result = doBuildActivityOutput(ctx, expr, isRoot);
                break;
            case no_extractresult:
            case no_setresult:
                result = doBuildActivitySetResult(ctx, expr, isRoot);
                break;
            case no_returnresult:
                result = doBuildActivityReturnResult(ctx, expr, isRoot);
                break;
            case no_getgraphresult:
                //Use the get graph result activity if we are generating the correct level graph.
                //otherwise it needs to be serialized from the parent activity
                {
                    IHqlExpression * graphId = expr->queryChild(1);
                    bool canAccessResultDirectly = isCurrentActiveGraph(ctx, graphId);
                    if (!canAccessResultDirectly)
                    {
                        //Sometimes results for the parent graph can be accessed from a child graph (e.g., loops).
                        //The test for Thor is temporary - roxie and hthor should both allow access to outer results
                        //from inside a loop.
                        //In fact roxie/hthor could access parent results directly from a child query if the parent
                        //activity is always on the master.  (Thor could if it knew to access the entire result.)
                        if (getTargetClusterType() == ThorLCRCluster)
                        {
                            ParentExtract * extract = static_cast<ParentExtract*>(ctx.queryFirstAssociation(AssocExtract));
                            if (extract)
                                canAccessResultDirectly = extract->areGraphResultsAccessible(graphId);
                        }
                        else if (getTargetClusterType() == HThorCluster)
                        {
                            //Only create the activity for results from parent graphs, not from siblings
                            if (matchActiveGraph(ctx, graphId))
                                canAccessResultDirectly = true;
                        }
                    }
                    if (canAccessResultDirectly)
                        result = doBuildActivityGetGraphResult(ctx, expr);
                    else
                        result = doBuildActivityChildDataset(ctx, expr);
                    break;
                }
            case no_getgraphloopresult:
                //Use the get graph result activity if we are generating the correct level graph.
                //otherwise it needs to be serialized from the parent activity
                {
                    if (isCurrentActiveGraph(ctx, expr->queryChild(1)))
                        result = doBuildActivityGetGraphLoopResult(ctx, expr);
                    else
                        throwError(HQLERR_GraphInputAccessedChild);
                    break;
                }
            case no_setgraphresult:
            case no_spillgraphresult:
                result = doBuildActivitySetGraphResult(ctx, expr, isRoot);
                break;
            case no_setgraphloopresult:
                result = doBuildActivitySetGraphLoopResult(ctx, expr);
                break;
            case no_spill:
                result = doBuildActivitySpill(ctx, expr);
                break;
            case no_buildindex:
                result = doBuildActivityOutputIndex(ctx, expr, isRoot);
                break;
            case no_distribution:
                result = doBuildActivityDistribution(ctx, expr, isRoot);
                break;
            case no_keydiff:
                result = doBuildActivityKeyDiff(ctx, expr, isRoot);
                break;
            case no_keypatch:
                result = doBuildActivityKeyPatch(ctx, expr, isRoot);
                break;
            case no_if:
                result = doBuildActivityIf(ctx, expr, isRoot);
                break;
            case no_case:
            case no_map:
                result = doBuildActivityCase(ctx, expr, isRoot);
                break;
            case no_quantile:
                result = doBuildActivityQuantile(ctx, expr);
                break;
            case no_chooseds:
            case no_choose:
                result = doBuildActivityChoose(ctx, expr, isRoot);
                break;
            case no_iterate:
                result = doBuildActivityIterate(ctx, expr);
                break;
            case no_process:
                result = doBuildActivityProcess(ctx, expr);
                break;
            case no_group:
                //Special case group(subsort) which will be mapped to group(group(sort(group))) to remove
                //the redundant group
                if (!options.supportsSubSortActivity && (expr->queryChild(0)->getOperator() == no_subsort))
                {
                    IHqlExpression * subsort = expr->queryChild(0);
                    OwnedHqlExpr groupedSort = convertSubSortToGroupedSort(subsort);
                    assertex(groupedSort->getOperator() == no_group);
                    OwnedHqlExpr newGroup = replaceChild(expr, 0, groupedSort->queryChild(0));
                    result = doBuildActivityGroup(ctx, newGroup);
                }
                else
                    result = doBuildActivityGroup(ctx, expr);
                break;
            case no_cogroup:
            case no_assertgrouped:
                result = doBuildActivityGroup(ctx, expr);
                break;
            case no_normalize:
                result = doBuildActivityNormalize(ctx, expr);
                break;
            case no_normalizegroup:
                result = doBuildActivityNormalizeGroup(ctx, expr);
                break;
            case no_distributed:
            {
                IHqlExpression * in = expr->queryChild(0);
                if (isGrouped(in))
                {
                    Owned<ABoundActivity> boundInput = buildCachedActivity(ctx, in);
                    result = doBuildActivityUngroup(ctx, expr, boundInput);
                }
                else
                    result = buildCachedActivity(ctx, in);
                break;
            }
            case no_sorted:
            case no_preservemeta:
            case no_unordered:
            case no_grouped:
            case no_nofold:
            case no_nohoist:
            case no_nocombine:
            case no_globalscope:
            case no_thisnode:
            case no_forcegraph:
            case no_keyed:
                result = buildCachedActivity(ctx, expr->queryChild(0));
                break;
            case no_dataset_alias:
                if (!expr->hasAttribute(_normalized_Atom))
                {
                    OwnedHqlExpr uniqueChild = normalizeDatasetAlias(expr);
                    result = buildCachedActivity(ctx, uniqueChild);
                }
                else
                    result = buildCachedActivity(ctx, expr->queryChild(0));
                break;
            case no_alias_scope:
            case no_alias:
                result = buildCachedActivity(ctx, expr->queryChild(0));
                break;
            case no_section:
                result = doBuildActivitySection(ctx, expr);
                break;
            case no_sectioninput:
                result = doBuildActivitySectionInput(ctx, expr);
                break;
            case no_forcelocal:
                result = doBuildActivityForceLocal(ctx, expr);
                break;
            case no_preload:
                {
                    StringBuffer s;
                    DBGLOG("%s", getExprECL(expr, s).str());
                    throwError(HQLERR_TooComplicatedToPreload);
                }
            case no_sub:
                result = doBuildActivitySub(ctx, expr);
                break;
            case no_subsort:
                if (!options.supportsSubSortActivity)
                {
                    OwnedHqlExpr groupedSort = convertSubSortToGroupedSort(expr);
                    result = buildCachedActivity(ctx, groupedSort);
                }
                else
                    result = doBuildActivitySort(ctx, expr);
                break;
            case no_sort:
            case no_cosort:
            case no_topn:
            case no_assertsorted:
                result = doBuildActivitySort(ctx, expr);
                break;
            case no_dedup:
                result = doBuildActivityDedup(ctx, expr);
                break;
            case no_enth:
                result = doBuildActivityEnth(ctx, expr);
                break;
            case no_pipe:
                result = doBuildActivityPipeThrough(ctx, expr);
                break;
            case no_sample:
                result = doBuildActivitySample(ctx, expr);
                break;
            case no_filter:
                result = doBuildActivityFilter(ctx, expr);
                break;
            case no_limit:
                result = doBuildActivityLimit(ctx, expr);
                break;
            case no_catchds:
                result = doBuildActivityCatch(ctx, expr);
                break;
            case no_keyedlimit:
                {
                    traceExpression("keyed fail", expr);
                    StringBuffer s;
                    getExprECL(expr->queryChild(1), s);
                    throwError1(HQLERR_KeyedLimitNotKeyed, s.str());
                }
            case no_index:
            case no_selectnth:
                result = doBuildActivitySelectNth(ctx, expr);
                break;
            case no_selectmap:
                result = doBuildActivityCreateRow(ctx, expr, false);
                break;
            case no_join:
            case no_selfjoin:
                result = doBuildActivityJoin(ctx, expr);
                break;
            case no_fetch:
            case no_compound_fetch:
                result = doBuildActivityFetch(ctx, expr);
                break;
            case no_rollup:
                result = doBuildActivityRollup(ctx, expr);
                break;
            case no_hqlproject:
            case no_transformascii:
            case no_transformebcdic:
            case no_projectrow:
                result = doBuildActivityProject(ctx, expr);
                break;
            case no_createrow:
                result = doBuildActivityCreateRow(ctx, expr, false);
                break;
            case no_newusertable:
                result = doBuildActivityProject(ctx, expr);
                break;
            case no_aggregate:
            case no_newaggregate:
            case no_throughaggregate:
                result = doBuildActivityAggregate(ctx, expr);
                break;
            case no_metaactivity:
                if (expr->hasAttribute(pullAtom))
                    result = doBuildActivityPullActivity(ctx, expr);
                else if (expr->hasAttribute(traceAtom))
                    result = doBuildActivityTraceActivity(ctx, expr);
                else
                    throwUnexpected();
                break;
            case no_choosen:
                result = doBuildActivityFirstN(ctx, expr);
                break;
            case no_choosesets:
                result = doBuildActivityChooseSets(ctx, expr);
                break;
            case no_newkeyindex:
            case no_compound_indexread:
                result = doBuildActivityIndexRead(ctx, expr);
                break;
            case no_compound_indexcount:
            case no_compound_indexaggregate:
                result = doBuildActivityIndexAggregate(ctx, expr);
                break;
            case no_compound_indexgroupaggregate:
                result = doBuildActivityIndexGroupAggregate(ctx, expr);
                break;
            case no_compound_indexnormalize:
                result = doBuildActivityIndexNormalize(ctx, expr);
                break;
            case no_compound:
                buildStmt(ctx, expr->queryChild(0));
                result = buildCachedActivity(ctx, expr->queryChild(1));
                break;
            case no_newparse:
                result = doBuildActivityParse(ctx, expr);
                break;
            case no_newxmlparse:
                result = doBuildActivityXmlParse(ctx, expr);
                break;
            case no_httpcall:
                result = doBuildActivityHTTP(ctx, expr, (expr->isAction()), isRoot);
                break;
            case no_newsoapcall:
            case no_newsoapcall_ds:
            case no_newsoapaction_ds:
                result = doBuildActivitySOAP(ctx, expr, (expr->isAction()), isRoot);
                break;
            case no_parallel:
            case no_sequential:
            case no_actionlist:
            case no_orderedactionlist:
                result = doBuildActivitySequentialParallel(ctx, expr, isRoot);
                break;
            case no_activerow:
                result = doBuildActivityCreateRow(ctx, expr, false);
                break;
            case no_assert_ds:
                result = doBuildActivityAssert(ctx, expr);
                break;
            case no_loop:
                result = doBuildActivityLoop(ctx, expr);
                break;
            case no_graphloop:
                result = doBuildActivityGraphLoop(ctx, expr);
                break;
            case no_allnodes:
                result = doBuildActivityRemote(ctx, expr, isRoot);
                break;
            case no_libraryselect:
                result = doBuildActivityLibrarySelect(ctx, expr);
                break;
            case no_libraryscopeinstance:
                result = doBuildActivityLibraryInstance(ctx, expr);
                break;
            case no_serialize:
            case no_deserialize:
                result = doBuildActivitySerialize(ctx, expr);
                break;
            case no_definesideeffect:
                result = doBuildActivityDefineSideEffect(ctx, expr);
                break;
            case no_callsideeffect:
                result = doBuildActivityCallSideEffect(ctx, expr);
                break;
            case no_executewhen:
                result = doBuildActivityExecuteWhen(ctx, expr, isRoot);
                break;
            case no_param:
                throwUnexpectedX("Create Parameter as an activity");
            case no_thor:
                UNIMPLEMENTED;
                break;
            case no_stepped:
                throwError(HQLERR_SteppedNotImplemented);
            default:
                if (expr->isAction())
                    return doBuildActivityAction(ctx, expr, isRoot);
                if (expr->isDatarow())
                {
                    return doBuildActivityCreateRow(ctx, expr, false);
                }
                else
                {
                    UNIMPLEMENTED_XY("Activity", getOpString(op));
                }
        }
    }
    catch (IError *)
    {
        throw;
    }
    catch (IException * e)
    {
        IHqlExpression * location = queryActiveActivityLocation();
        if (location)
        {
            IError * error = annotateExceptionWithLocation(e, location);
            e->Release();
            throw error;
        }
        throw;
    }

    return result;
}


ABoundActivity * HqlCppTranslator::buildCachedActivity(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    switch (expr->getOperator())
    {
    case no_split:
    case no_libraryscopeinstance:
        {
            ActivityAssociation * match = static_cast<ActivityAssociation *>(ctx.queryAssociation(expr, AssocActivity, NULL));
            if (match)
                return LINK(match->activity);
            break;
        }
    case no_if:
        {
            if (options.recreateMapFromIf && !expr->isAction())
            {
                OwnedHqlExpr converted = combineIfsToMap(expr);
                if (converted)
                    return buildCachedActivity(ctx, converted);
            }
            break;
        }
    }

    //NB: ActivityExprStack is purely used for improving the error reporting
    activityExprStack.append(*LINK(expr));
    try
    {
        //Don't modify aliases in child queries - otherwise they can fail to match the aliases generated in the parent
        OwnedHqlExpr optimized = insideChildOrLoopGraph(ctx) ? LINK(expr) : optimizeActivityAliasReferences(expr);
        ThorBoundActivity * bound = (ThorBoundActivity *)buildActivity(ctx, optimized, isRoot);
        activityExprStack.pop();
        ActivityAssociation * table = new ActivityAssociation(expr->queryBody(), bound);
        ctx.associateOwn(*table);
        return bound;
    }
    catch (IException *)
    {
        activityExprStack.pop();
        throw;
    }
}


void HqlCppTranslator::buildRootActivity(BuildCtx & ctx, IHqlExpression * expr)
{
    ErrorSeverityMapper::Scope saved(*localOnWarnings);
    ::Release(buildCachedActivity(ctx, expr, true));
}


void HqlCppTranslator::buildRecordSerializeExtract(BuildCtx & ctx, IHqlExpression * memoryRecord)
{
    OwnedHqlExpr serializedRecord = getSerializedForm(memoryRecord, diskAtom);
    OwnedHqlExpr serializedDataset = createDataset(no_null, LINK(serializedRecord));
    OwnedHqlExpr memoryDataset = createDataset(no_anon, LINK(memoryRecord));

    MetaInstance meta(*this, memoryRecord, false);
    buildMetaInfo(meta);

    if (recordTypesMatch(memoryRecord, serializedRecord))
    {
        StringBuffer s;

        if (isFixedRecordSize(memoryRecord))
        {
            ctx.addQuoted(s.append("size32_t size = ").append(getFixedRecordSize(memoryRecord)).append(";"));
        }
        else
        {
            ctx.addQuoted(s.append("size32_t size = ").append(meta.queryInstanceObject()).append(".getRecordSize(_left);"));
        }

        ctx.addQuotedLiteral("byte * self = crSelf.ensureCapacity(size, NULL);");
        ctx.addQuotedLiteral("memcpy(crSelf.row(), _left, size);");
        ctx.addQuotedLiteral("return size;");
    }
    else
    {
        ctx.addQuotedLiteral("const byte * left = (const byte *)_left;");
        BoundRow * self = bindSelf(ctx, serializedDataset, "crSelf");
        BoundRow * left = bindTableCursor(ctx, memoryDataset, "left");
        OwnedHqlExpr rhs = ensureActiveRow(left->querySelector());

        OwnedHqlExpr serializedRow = ::ensureSerialized(rhs, diskAtom); // Ensure this is always accessed as disk serialized

        buildAssign(ctx, self->querySelector(), serializedRow);
        buildReturnRecordSize(ctx, self);
    }
}

//---------------------------------------------------------------------------

BoundRow * HqlCppTranslator::bindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * bound, node_operator side, IHqlExpression * selSeq)
{
    IHqlExpression * record = dataset->queryRecord();
    bool useRowAccessor = useRowAccessorClass(record, side == no_self);
    BoundRow * cursor = createTableCursor(dataset, bound, useRowAccessor, side, selSeq);
    if (useRowAccessor)
        cursor->prepareAccessor(*this, ctx);
    ctx.associateOwn(*cursor);
    return cursor;
}

BoundRow * HqlCppTranslator::bindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, const char * name, bool isLinkCounted, node_operator side, IHqlExpression * selSeq)
{
    Owned<ITypeInfo> type = makeRowReferenceType(dataset);
    if (isLinkCounted)
        type.setown(makeAttributeModifier(type.getClear(), getLinkCountedAttr()));

    Owned<IHqlExpression> bound = createVariable(name, type.getClear());
    return bindTableCursor(ctx, dataset, bound, side, selSeq);
}

BoundRow * HqlCppTranslator::rebindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, BoundRow * row, node_operator side, IHqlExpression * selSeq)
{
    BoundRow * cursor = recreateTableCursor(dataset, row, side, selSeq);
    ctx.associateOwn(*cursor);
    return cursor;
}


BoundRow * HqlCppTranslator::createTableCursor(IHqlExpression * dataset, IHqlExpression * bound, bool useAccessorClass, node_operator side, IHqlExpression * selSeq)
{
    return new BoundRow(dataset, bound, NULL, queryRecordOffsetMap(dataset->queryRecord(), useAccessorClass), side, selSeq);
}

BoundRow * HqlCppTranslator::recreateTableCursor(IHqlExpression * dataset, BoundRow * row, node_operator side, IHqlExpression * selSeq)
{
    ColumnToOffsetMap * columnMap = queryRecordOffsetMap(row->queryRecord(), false);
    return new BoundRow(dataset, row->queryBound(), nullptr, columnMap, side, selSeq);

}

BoundRow * HqlCppTranslator::bindXmlTableCursor(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * bound, node_operator side, IHqlExpression * selSeq, bool translateVirtuals)
{
    Owned<ColumnToOffsetMap> xmlMap = new XmlColumnToOffsetMap(dataset->queryRecord(), getDefaultMaxRecordSize(), translateVirtuals);
    xmlMap->init(recordMap);
    BoundRow * cursor = new BoundRow(dataset, bound, NULL, xmlMap, side, selSeq);
    ctx.associateOwn(*cursor);
    return cursor;
}

BoundRow * HqlCppTranslator::bindXmlTableCursor(BuildCtx & ctx, IHqlExpression * dataset, const char * name, node_operator side, IHqlExpression * selSeq, bool translateVirtuals)
{
    OwnedHqlExpr bound = createVariable(name, makeRowReferenceType(NULL));
//  OwnedHqlExpr bound = createVariable(name, makeRowReferenceType(dataset));
    return bindXmlTableCursor(ctx, dataset, bound, side, selSeq, translateVirtuals);
}

BoundRow * HqlCppTranslator::bindCsvTableCursor(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * bound, node_operator side, IHqlExpression * selSeq, bool translateVirtuals, IAtom * encoding)
{
    Owned<ColumnToOffsetMap> csvMap = new CsvColumnToOffsetMap(dataset->queryRecord(), getDefaultMaxRecordSize(), translateVirtuals, encoding);
    csvMap->init(recordMap);
    BoundRow * cursor = new BoundRow(dataset, bound, NULL, csvMap, side, selSeq);
    ctx.associateOwn(*cursor);
    return cursor;
}

BoundRow * HqlCppTranslator::bindCsvTableCursor(BuildCtx & ctx, IHqlExpression * dataset, const char * name, node_operator side, IHqlExpression * selSeq, bool translateVirtuals, IAtom * encoding)
{
    OwnedHqlExpr bound = createVariable(name, makeRowReferenceType(NULL));
//  OwnedHqlExpr bound = createVariable(name, makeRowReferenceType(dataset));
    return bindCsvTableCursor(ctx, dataset, bound, side, selSeq, translateVirtuals, encoding);
}

BoundRow * HqlCppTranslator::bindSelf(BuildCtx & ctx, IHqlExpression * dataset, const char * builderName)
{
    StringBuffer bound;
    bound.append(builderName).append(".row()");
    BoundRow * row = bindTableCursor(ctx, dataset, bound, false, no_self, NULL);
    OwnedHqlExpr builder = createVariable(builderName, makeBoolType());
    row->setBuilder(builder);
    return row;
}

BoundRow * HqlCppTranslator::bindSelf(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * expr, IHqlExpression * builder)
{
    BoundRow * row = bindTableCursor(ctx, dataset, expr, no_self, NULL);
    row->setBuilder(builder);
    return row;
}

BoundRow * HqlCppTranslator::bindRow(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * bound)
{
    BoundRow * row = createBoundRow(expr, bound);
    ctx.associateOwn(*row);
    return row;
}

BoundRow * HqlCppTranslator::bindRow(BuildCtx & ctx, IHqlExpression * expr, const char * name)
{
    Owned<IHqlExpression> bound = createVariable(name, makeRowReferenceType(NULL));
    return bindRow(ctx, expr, bound);
}


BoundRow * HqlCppTranslator::bindTableCursorOrRow(BuildCtx & ctx, IHqlExpression * expr, const char * name)
{
    if (expr->getOperator() == no_activerow)
        expr = expr->queryChild(0);
    if (expr->isDatarow())
        return bindRow(ctx, expr, name);
    else
        return bindTableCursor(ctx, expr, name);
}

BoundRow * HqlCppTranslator::createBoundRow(IHqlExpression * dataset, IHqlExpression * bound)
{
    bool useAccessor = false;
    IHqlExpression * accessor = NULL;
    return new BoundRow(dataset->queryBody(), bound, accessor, queryRecordOffsetMap(dataset->queryRecord(), (accessor != NULL)));
}

BoundRow * HqlCppTranslator::bindSelectorAsSelf(BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * expr)
{
    BoundRow * rootRow = selector->queryRootRow();
    if (!rootRow->queryBuilder())
    {
        if (options.alwaysCreateRowBuilder || !isFixedWidthDataset(rootRow->queryRecord()))
            UNIMPLEMENTED_X("expected a row builder");
    }
    if (selector->isRoot())
    {
        if (rootRow->querySide() == no_self)
        {
            //No need to associate since it is already present in the context
            //ctx.associate(*rootRow);
            return rootRow;
        }
        return bindSelf(ctx, expr, rootRow->queryBound(), rootRow->queryBuilder());
    }

    //Need to bind a delta address to a new variable.
    if (!rootRow->queryBuilder())
        UNIMPLEMENTED_X("expected a row builder");

    CHqlBoundExpr offset;
    selector->getOffset(ctx, offset);
    CHqlBoundExpr address;
    selector->buildAddress(ctx, address);
    OwnedHqlExpr row = createValue(no_typetransfer, makeReferenceModifier(LINK(selector->queryType())), LINK(address.expr));
    unsigned trailingFixed = selector->getContainerTrailingFixed();

    StringBuffer builderName;
    {
        getUniqueId(builderName.append("b"));

        StringBuffer s;
        s.append("RtlNestedRowBuilder ").append(builderName).append("(");
        generateExprCpp(s, rootRow->queryBuilder()).append(",");
        generateExprCpp(s, offset.expr).append(",").append(trailingFixed).append(");");
        ctx.addQuoted(s);
    }

    BoundRow * selfRow = bindSelf(ctx, expr, builderName);
    selfRow->setAlias(selector);
    return selfRow;
}

void HqlCppTranslator::finishSelf(BuildCtx & ctx, BoundRow * self, BoundRow * target)
{
    if (target)
    {
        OwnedHqlExpr sizeofSelf = createSizeof(self->querySelector());
        CHqlBoundExpr bound;
        buildExpr(ctx, sizeofSelf, bound);
        OwnedHqlExpr sizeofTarget = createSizeof(target->querySelector());
        ctx.associateExpr(sizeofTarget, bound);
    }
}


void HqlCppTranslator::ensureRowAllocated(BuildCtx & ctx, const char * builder)
{
    StringBuffer s;
    s.append(builder).append(".getSelf();");
    ctx.addQuoted(s);
}

void HqlCppTranslator::ensureRowAllocated(BuildCtx & ctx, BoundRow * row)
{
    assertex(row->queryBuilder());
    StringBuffer s;
    generateExprCpp(s, row->queryBuilder()).append(".getSelf();");
    ctx.addQuoted(s);
}

//---------------------------------------------------------------------------

BoundRow * HqlCppTranslator::bindSelectorAsRootRow(BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * expr)
{
    BoundRow * rootRow = selector->queryRootRow();
    assertex(!rootRow->queryBuilder());
    if (selector->isRoot())
    {
        ctx.associate(*rootRow);
        return rootRow;
    }

    //Need to bind a delta address to a new variable.
    CHqlBoundExpr address;
    selector->buildAddress(ctx, address);
    OwnedHqlExpr row = createValue(no_typetransfer, makeReferenceModifier(LINK(selector->queryType())), LINK(address.expr));

    BoundRow * childRow = bindTableCursor(ctx, expr, row);
    childRow->setAlias(selector);
    return childRow;
}

//---------------------------------------------------------------------------

BoundRow * HqlCppTranslator::resolveSelectorDataset(BuildCtx & ctx, IHqlExpression * dataset)
{
    return static_cast<BoundRow *>(ctx.queryAssociation(dataset->queryNormalizedSelector(), AssocCursor, NULL));
}


//---------------------------------------------------------------------------

void HqlCppTranslator::addDependency(BuildCtx & ctx, ABoundActivity * sourceActivity, IPropertyTree * sinkGraphTree, ABoundActivity * sinkActivity, IAtom * kind, const char * label, unsigned inputIndex, int whenId)
{
    IPropertyTree * graphTree = NULL;
    if (sinkActivity->queryGraphId() == sourceActivity->queryGraphId())
    {
        if (targetHThor())
            throwError1(HQLERR_DependencyWithinGraph, sinkActivity->queryGraphId());
        graphTree = sinkGraphTree;
    }

    unsigned outputIndex = 0;
    if (kind != childAtom)
        outputIndex = sourceActivity->nextOutputCount();

    StringBuffer idText;
    idText.append(sourceActivity->queryActivityId()).append('_').append(sinkActivity->queryActivityId());

    IPropertyTree *edge = createPTree();
    edge->setProp("@id", idText.str());
    if (label)
        edge->setProp("@label", label);
    if (targetRoxie())
    {
        if (outputIndex)
            addGraphAttributeInt(edge, "_sourceIndex", outputIndex);
    }
    if (inputIndex)
        addGraphAttributeInt(edge, "_targetIndex", inputIndex);

    if (kind == childAtom)
    {
        addGraphAttributeBool(edge, "_childGraph", true);
    }
    else if (kind == dependencyAtom)
    {
        addGraphAttributeBool(edge, "_dependsOn", true);
    }
    else if (sourceActivity->queryContainerId() != sinkActivity->queryContainerId())
    {
        //mark as a dependency if the source and target aren't at the same depth
        addGraphAttributeBool(edge, "_dependsOn", true);
    }

    if (whenId)
        addGraphAttributeInt(edge, "_when", whenId);


    if (graphTree)
    {
        edge->setPropInt64("@target", sinkActivity->queryActivityId());
        edge->setPropInt64("@source", sourceActivity->queryActivityId());
        graphTree->addPropTree("edge", edge);
    }
    else
    {
        edge->setPropInt64("@target", sinkActivity->queryGraphId());
        edge->setPropInt64("@source", sourceActivity->queryGraphId());
        addGraphAttributeInt(edge, "_sourceActivity", sourceActivity->queryActivityId());
        addGraphAttributeInt(edge, "_targetActivity", sinkActivity->queryActivityId());
        activeGraph->xgmml->addPropTree("edge", edge);
    }
}

void HqlCppTranslator::addDependency(BuildCtx & ctx, ABoundActivity * element, ABoundActivity * dependent, IAtom * kind, const char * label)
{
    unsigned whenId = 0;
    addDependency(ctx, element, NULL, dependent, kind, label, 0, whenId);
}

void HqlCppTranslator::addDependency(BuildCtx & ctx, ABoundActivity * element, ActivityInstance * instance, IAtom * kind, const char * label)
{
    unsigned whenId = 0;
    addDependency(ctx, element, instance->querySubgraphNode(), instance->queryBoundActivity(), kind, label, 0, whenId);
}

void HqlCppTranslator::addActionConnection(BuildCtx & ctx, ABoundActivity * element, ActivityInstance * instance, IAtom * kind, const char * label, unsigned inputIndex, int whenId)
{
    addDependency(ctx, element, instance->querySubgraphNode(), instance->queryBoundActivity(), kind, label, inputIndex, whenId);
}

void HqlCppTranslator::buildClearRecord(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * record, int direction)
{
    Owned<IReferenceSelector> selector = buildActiveRow(ctx, dataset);
    selector->buildClear(ctx, direction);
}


IHqlExpression * HqlCppTranslator::getClearRecordFunction(IHqlExpression * record, int direction)
{
    assertex(record);
    IHqlExpression * dirExpr = getSizetConstant((size32_t)direction);
    OwnedHqlExpr search = createAttribute(__clearHelperAtom, LINK(record->queryBody()), dirExpr);

    BuildCtx declarectx(*code, declareAtom);
    HqlExprAssociation * match = declarectx.queryMatchExpr(search);
    if (match)
        return LINK(match->queryExpr());

    StringBuffer functionName;
    getUniqueId(functionName.append("clearRow"));

    BuildCtx clearctx(declarectx);

    StringBuffer s;
    s.append("size32_t ").append(functionName).append("(ARowBuilder & crSelf, IResourceContext * ctx)");

    clearctx.setNextPriority(RowMetaPrio);
    {
        MemberFunction clearFunc(*this, clearctx, s, MFdynamicproto);
        clearFunc.setIncomplete(true);

        OwnedHqlExpr dataset = createDataset(no_anon, LINK(record));
        BoundRow * cursor = bindSelf(clearFunc.ctx, dataset, "crSelf");
        ensureRowAllocated(clearFunc.ctx, "crSelf");
        buildClearRecord(clearFunc.ctx, cursor->querySelector(), record, direction);
        buildReturnRecordSize(clearFunc.ctx, cursor);
        clearFunc.setIncomplete(false);
    }

    if (options.spanMultipleCpp)
    {
        s.clear().append("extern size32_t ").append(functionName).append("(ARowBuilder & crSelf, IResourceContext * ctx);");
        BuildCtx protoctx(*code, mainprototypesAtom);
        protoctx.addQuoted(s);
    }

    OwnedHqlExpr temp = createVariable(functionName, makeVoidType());
    declarectx.associateExpr(search, temp);
    return temp.getLink();

}


void HqlCppTranslator::buildClearRecordMember(BuildCtx & ctx, const char * name, IHqlExpression * dataset)
{
    BuildCtx clearCtx(ctx);

    StringBuffer s;
    s.append("virtual size32_t createDefault").append(name).append("(ARowBuilder & crSelf)");

    if (dataset && dataset->queryRecord()->numChildren())
    {
        OwnedHqlExpr func = getClearRecordFunction(dataset->queryRecord());
        generateExprCpp(s.append(" { return "), func).append("(crSelf, ctx); }");
        clearCtx.addQuoted(s);
    }
    else
        clearCtx.addQuoted(s.append(" { return 0; }"));
}


void HqlCppTranslator::doBuildExprEvaluate(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * field = expr->queryChild(1);

    BoundRow * boundRow = resolveSelectorDataset(ctx, dataset);
    if (boundRow)
    {
        //E.g. EVALUATE(LEFT, attribute).  Need to make sure field refs are unambiguous.
        BuildCtx subctx(ctx);
        subctx.addGroup();
        bindTableCursor(subctx, boundRow->queryDataset(), boundRow->queryBound());

        buildExpr(subctx, field, tgt);
    }
    else
    {
        switch (dataset->queryType()->getTypeCode())
        {
        case type_row:
            BuildCtx subctx(ctx);
            subctx.addGroup();
            Owned<IReferenceSelector> selector = buildNewRow(subctx, dataset);
            Owned<BoundRow> boundRow = selector->getRow(subctx);
            //subctx.associateOwn???
            bindTableCursor(subctx, boundRow->queryDataset(), boundRow->queryBound());
            buildExpr(subctx, field, tgt);
            return;
        }

        throwError(HQLERR_EvaluateTableNotInScope);
    }
}

void HqlCppTranslator::doBuildExprCounter(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    if (buildExprInCorrectContext(ctx, expr, tgt, false))
        return;

    throwError(HQLERR_CounterNotValid);
}

//---------------------------------------------------------------------------

void HqlCppTranslator::doBuildSerialize(BuildCtx & ctx, IIdAtom * name, IHqlExpression * length, CHqlBoundExpr & bound, const char * bufferName)
{
    HqlExprArray args;
    if (length)
        args.append(*LINK(length));
    else
    {
        args.append(*getBoundSize(bound));
    }
    args.append(*getPointer(bound.expr));
    args.append(*createVariable(bufferName, makeBoolType()));
    callProcedure(ctx, name, args);
}

void HqlCppTranslator::ensureSerialized(const CHqlBoundTarget & variable, BuildCtx & serializectx, BuildCtx & deserializectx, const char * inBufferName, const char * outBufferName, IAtom * serializeForm)
{
    CHqlBoundExpr value;
    value.setFromTarget(variable);
    while (isCast(value.expr))
        value.expr.set(value.expr->queryChild(0));
    ITypeInfo * type = value.expr->queryType();
    if ((type->getSize() == UNKNOWN_LENGTH) || hasLinkCountedModifier(type) || hasWrapperModifier(type))
    {
        HqlExprArray serializeArgs;
        serializeArgs.append(*value.getTranslatedExpr());

        HqlExprArray deserializeArgs;

        IIdAtom * serializeName;
        IIdAtom * deserializeName;
        OwnedITypeInfo serializedType;
        type_t tc = type->getTypeCode();
        switch (tc)
        {
        case type_varstring:
            serializeName = serializeCStringXId;
            deserializeName = deserializeCStringXId;
            break;
        case type_string:
            serializeName = serializeStringXId;
            deserializeName = deserializeStringXId;
            break;
        case type_data:
            serializeName = serializeDataXId;
            deserializeName = deserializeDataXId;
            break;
        case type_set:
            serializeName = serializeSetId;
            deserializeName = deserializeSetId;
            break;
        case type_qstring:
            serializeName = serializeQStrXId;
            deserializeName = deserializeQStrXId;
            break;
        case type_unicode:
            serializeName = serializeUnicodeXId;
            deserializeName = deserializeUnicodeXId;
            break;
        case type_varunicode:
            serializeName = serializeUnicodeXId;
            deserializeName = deserializeVUnicodeXId;
            break;
        case type_utf8:
            serializeName = serializeUtf8XId;
            deserializeName = deserializeUtf8XId;
            break;
        case type_dictionary:
        case type_table:
        case type_groupedtable:
            {
                IHqlExpression * record = ::queryRecord(type);
                if (hasLinkCountedModifier(type))
                {
                    deserializeArgs.append(*createSerializer(deserializectx, record, serializeForm, deserializerAtom));

                    serializeArgs.append(*createSerializer(serializectx, record, serializeForm, serializerAtom));
                    if (tc == type_dictionary)
                    {
                        serializeName = serializeDictionaryXId;
                        deserializeName = deserializeDictionaryXId;
                    }
                    else if (tc == type_groupedtable)
                    {
                        serializeName = serializeGroupedRowsetXId;
                        deserializeName = deserializeGroupedRowsetXId;
                    }
                    else
                    {
                        serializeName = serializeRowsetXId;
                        deserializeName = deserializeRowsetXId;
                    }
                }
                else
                {
                    assertex(!recordRequiresSerialization(record, serializeForm));
                    if (tc == type_dictionary)
                    {
                        throwUnexpected();
                    }
                    else if (tc == type_groupedtable)
                    {
                        serializeName = serializeGroupedDatasetXId;
                        deserializeName = deserializeGroupedDatasetXId;
                    }
                    else
                    {
                        serializeName = serializeDatasetXId;
                        deserializeName = deserializeDatasetXId;
                    }
                }
                serializedType.set(type);
                break;
            }
        case type_row:
            {
                IHqlExpression * record = ::queryRecord(type);
                assertex(hasWrapperModifier(type));

                serializeArgs.append(*createSerializer(serializectx, record, serializeForm, serializerAtom));
                serializeArgs.append(*createVariable(outBufferName, makeBoolType()));
                buildFunctionCall(serializectx, serializeRowId, serializeArgs);


                deserializeArgs.append(*createSerializer(deserializectx, record, serializeForm, deserializerAtom));
                deserializeArgs.append(*createVariable(inBufferName, makeBoolType()));
                Owned<ITypeInfo> resultType = makeReferenceModifier(makeAttributeModifier(makeRowType(record->getType()), getLinkCountedAttr()));
                OwnedHqlExpr call = bindFunctionCall(deserializeRowId, deserializeArgs, resultType);
                buildExprAssign(deserializectx, variable, call);
                return;
            }
        default:
            UNIMPLEMENTED;
        }

        serializeArgs.append(*createVariable(outBufferName, makeBoolType()));
        deserializeArgs.append(*createVariable(inBufferName, makeBoolType()));

        buildFunctionCall(serializectx, serializeName, serializeArgs);

        OwnedHqlExpr deserializeCall = bindFunctionCall(deserializeName, deserializeArgs, serializedType);
        buildExprAssign(deserializectx, variable, deserializeCall);
    }
    else
    {
        OwnedHqlExpr length;
        switch (type->getTypeCode())
        {
        case type_int:
        case type_swapint:
        case type_packedint:
            switch (type->getSize())
            {
            case 3: case 5: case 6: case 7:
                if (isLittleEndian(type))
                    buildClear(deserializectx, variable);
                else
                {
                    unsigned newSize = (type->getSize() == 3) ? 4 : 8;
                    length.setown(getSizetConstant(newSize));
                }
                break;
            }
            break;
        case type_row:
            {
                //MORE: This will cause problems if rows are dynamically allocated
                if (hasReferenceModifier(type))
                    length.setown(getSizetConstant(sizeof(void*)));
                else
                {
                    IHqlExpression * record = ::queryRecord(type);
                    length.setown(getSizetConstant(getMaxRecordSize(record)));
                }
                break;
            }
        case type_bitfield:
            UNIMPLEMENTED;
        }
        doBuildSerialize(serializectx, serializeRawId, length, value, outBufferName);
        doBuildSerialize(deserializectx, deserializeRawId, length, value, inBufferName);
    }
}

void HqlCppTranslator::ensureSerialized(BuildCtx & ctx, const CHqlBoundTarget & variable)
{
    EvalContext * instance = queryEvalContext(ctx);
    assertex(instance);
    instance->tempCompatiablityEnsureSerialized(variable);
}


bool HqlCppTranslator::checkGetResultContext(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    IHqlExpression * seq = queryAttributeChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
    if (!name)
        name = queryAttributeChild(expr, nameAtom, 0);
    if (!contextAvailable)
    {
        StringBuffer s;
        getStoredDescription(s, seq, name, true);
        ::throwError1(HQLERR_InvalidAcessStoredVariable, s.str());
    }

    if (!insideOnCreate(ctx) && !ctx.queryMatchExpr(globalContextMarkerExpr) && !matchesConstantValue(seq, ResultSequenceOnce))
    {
        if (queryEvalContext(ctx))
        {
            doBuildAliasValue(ctx, expr, tgt, NULL);
            return true;
        }

        StringBuffer s;
        getStoredDescription(s, seq, name, true);
        ::throwError1(HQLERR_CannotAccessStoredVariable, s.str());
    }

    if (ctx.getMatchExpr(expr, tgt))
        return true;

    //Use top activity, rather than queryCurrentActivity() - since we want the dependency to the child (for graph display)
    if (activeActivities.ordinality())
        queryAddResultDependancy(activeActivities.tos(), seq, name);
    else if (name && targetRoxie())
    {
        OwnedHqlExpr attr = createResultAttribute(seq, name);
        registerGlobalUsage(attr);
    }
    return false;
}

void HqlCppTranslator::doBuildExprGetResult(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    if (checkGetResultContext(ctx, expr, tgt))
        return;

    ITypeInfo * exprType = expr->queryType();
    CHqlBoundTarget tempTarget;
    createTempFor(ctx, exprType, tempTarget, typemod_none, FormatLinkedDataset);
    buildGetResultInfo(ctx, expr, NULL, &tempTarget);
    tgt.setFromTarget(tempTarget);
    ctx.associateExpr(expr, tgt);
}


void HqlCppTranslator::doBuildAssignGetResult(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr)
{
    CHqlBoundExpr bound;
    if (checkGetResultContext(ctx, expr, bound))
    {
        assign(ctx, target, bound);
        return;
    }

    buildGetResultInfo(ctx, expr, NULL, &target);
}


void HqlCppTranslator::pushCluster(BuildCtx & ctx, IHqlExpression * cluster)
{
    HqlExprArray args;
    args.append(*LINK(cluster));
    buildFunctionCall(ctx, selectClusterId, args);

    StringBuffer clusterText;
    getStringValue(clusterText, cluster);
    if (clusterText.length())
        ctxCallback->noteCluster(clusterText.str());
}


void HqlCppTranslator::popCluster(BuildCtx & ctx)
{
    HqlExprArray args;
    callProcedure(ctx, restoreClusterId, args);
}


void HqlCppTranslator::doBuildStmtSetResult(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * seq = queryAttributeChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
    IHqlExpression * persist = expr->queryAttribute(_workflowPersist_Atom);
    IHqlExpression * cluster = expr->queryAttribute(clusterAtom);

    BuildCtx subctx(ctx);
    LinkedHqlExpr value;
    if (expr->getOperator() == no_extractresult)
    {
        IHqlExpression * ds = expr->queryChild(0);
        OwnedHqlExpr row = removeDatasetWrapper(ds);
        OwnedHqlExpr newDs = (row->getOperator() == no_activerow) ? LINK(row) : createRow(no_newrow, LINK(row));
        value.setown(replaceSelector(expr->queryChild(1), ds, newDs));
    }
    else
        value.set(expr->queryChild(0));

    if (matchesConstantValue(seq, ResultSequenceStored) || matchesConstantValue(seq, ResultSequencePersist))
    {
        StringBuffer text;
        text.append("Create ");
        getStoredDescription(text, seq, name, true);
        graphLabel.set(text.str());
    }

    if (insideChildQuery(ctx))
    {
        StringBuffer description;
        getStoredDescription(description, seq, name, true);
        reportWarning(CategoryUnusual, SeverityError, queryLocation(expr), ECODETEXT(HQLWRN_OutputScalarInsideChildQuery), description.str());
    }

    if (cluster)
        pushCluster(subctx, cluster->queryChild(0));

    switch (value->queryType()->getTypeCode())
    {
    case type_void:
        {
            buildStmt(subctx, value);

            IHqlExpression * result = queryBoolExpr(true);
            if (expr->queryAttribute(checkpointAtom))
            {
                IHqlExpression * search = value;
                if (search->getOperator() == no_thor)
                    search = search->queryChild(0);
                if ((search->getOperator() == no_output) && search->queryChild(1))
                {
                    BuildCtx atendctx(*code, goAtom);
                    atendctx.setNextDestructor();
                    HqlExprArray args;
                    args.append(*LINK(search->queryChild(1)));
                    callProcedure(atendctx, deleteFileId, args);
                }
            }

            if (!expr->hasAttribute(noSetAtom))
                buildSetResultInfo(subctx, expr, result, NULL, (persist != NULL), false);
        }
        break;
    case type_set:
        {
            ITypeInfo * setType = NULL;
            IHqlExpression  * original = queryAttributeChild(expr, _original_Atom, 0);
            if (original)
                setType = original->queryType();

            OwnedHqlExpr normalized = normalizeListCasts(value);
            buildSetResultInfo(subctx, expr, normalized, setType, (persist != NULL), true);
            break;
        }
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        switch (value->getOperator())
        {
        case no_null:
            {
                HqlExprArray args;
                args.append(*createResultName(name, false));
                args.append(*LINK(seq));
                args.append(*createValue(no_translated, makeSetType(NULL), createValue(no_nullptr, makeSetType(NULL)), getSizetConstant(0)));
                args.append(*createTranslatedOwned(createValue(no_nullptr, makeBoolType())));
                buildFunctionCall(subctx, setResultSetId, args);
                HqlExprArray xmlnsAttrs;
                gatherAttributes(xmlnsAttrs, xmlnsAtom, expr);
                Owned<IWUResult> result = createDatasetResultSchema(seq, name, value->queryRecord(), xmlnsAttrs, true, false, 0);
                break;
            }
        default:
            assertex(!"Should never occur - should have been transformed to an OUTPUT()");
        }
        break;
    default:
        buildSetResultInfo(subctx, expr, value, NULL, (persist != NULL), true);
        break;
    }

    if (cluster)
        popCluster(subctx);

    if (matchesConstantValue(seq, ResultSequenceStored) || matchesConstantValue(seq, ResultSequencePersist))
        graphLabel.clear();
}

static bool isFilePersist(IHqlExpression * expr)
{
    for (;;)
    {
        switch (expr->getOperator())
        {
        case no_thor:
            expr = expr->queryChild(0);
            break;
        case no_compound:
            expr = expr->queryChild(1);
            break;
        case no_output:
            return isFileOutput(expr);
        case no_actionlist:
        case no_orderedactionlist:
            expr = expr->queryChild(expr->numChildren()-1);
            break;
        default:
            return false;
        }
    }
}

IHqlExpression * HqlCppTranslator::calculatePersistInputCrc(BuildCtx & ctx, IHqlExpression * expr)
{
    DependenciesUsed dependencies(true);
    gatherDependencies(expr, dependencies, GatherAll);
    dependencies.removeInternalReads();

    return calculatePersistInputCrc(ctx, dependencies);
}

IHqlExpression * HqlCppTranslator::calculatePersistInputCrc(BuildCtx & ctx, DependenciesUsed & dependencies)
{
    Owned<ITypeInfo> crcType = makeIntType(8, false);
    OwnedHqlExpr zero = createNullExpr(crcType);
    if ((dependencies.tablesRead.ordinality() == 0) && (dependencies.resultsRead.ordinality() == 0))
        return zero.getClear();

    OwnedHqlExpr crcExpr = ctx.getTempDeclare(crcType, zero);
    ForEachItemIn(idx1, dependencies.tablesRead)
    {
        IHqlExpression & cur = dependencies.tablesRead.item(idx1);
        HqlExprArray args;
        args.append(OLINK(cur));
        args.append(*LINK(crcExpr));

        OwnedHqlExpr function = bindFunctionCall(getDatasetHashId, args);
        buildAssignToTemp(ctx, crcExpr, function);
    }

    ForEachItemIn(idx2, dependencies.resultsRead)
    {
        IHqlExpression & cur = dependencies.resultsRead.item(idx2);
        IHqlExpression * seq = cur.queryChild(0);
        IHqlExpression * name = cur.queryChild(1);
        IHqlExpression * wuid = cur.queryChild(2);
        if (name->isAttribute())
        {
            assertex(name->queryName() == wuidAtom);
            wuid = name;
            name = NULL;
        }

        //Not sure if we need to do this if the result is internal.  Leave on for the moment.
        //if (seq->queryValue()->getIntValue() != ResultSequenceInternal)
        bool expandLogical = matchesConstantValue(seq, ResultSequencePersist) && !cur.hasAttribute(_internal_Atom);
        HqlExprArray args;
        if (wuid)
            args.append(*LINK(wuid->queryChild(0)));
        args.append(*createResultName(name, expandLogical));
        args.append(*LINK(seq));
        OwnedHqlExpr call = bindFunctionCall(wuid ? getExternalResultHashId : getResultHashId, args);
        OwnedHqlExpr value = createValue(no_bxor, crcExpr->getType(), LINK(crcExpr), ensureExprType(call, crcExpr->queryType()));
        buildAssignToTemp(ctx, crcExpr, value);
    }
    return crcExpr.getClear();
}

void HqlCppTranslator::doBuildStmtEnsureResult(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * value = expr->queryChild(0);
    IHqlExpression * seq = queryAttributeChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);

    OwnedHqlExpr resultName = ::createResultName(name);
    resultName.setown(ensureExprType(resultName, unknownVarStringType));

    HqlExprArray args;
    args.append(*LINK(resultName));
    args.append(*LINK(seq));
    OwnedHqlExpr checkExists = createValue(no_not, makeBoolType(), bindFunctionCall(isResultId, args));
    if ((value->getOperator() == no_thor) && (value->queryChild(0)->getOperator() == no_output))
    {
        IHqlExpression * filename = queryRealChild(value->queryChild(0), 1);
        if (filename)
        {
            args.append(*LINK(filename));
            OwnedHqlExpr fileExists = createValue(no_not, makeBoolType(), bindFunctionCall(fileExistsId, args));
            checkExists.setown(createBoolExpr(no_or, checkExists.getClear(), fileExists.getClear()));
        }
    }

    BuildCtx subctx(ctx);
    buildFilter(subctx, checkExists);

    doBuildStmtSetResult(subctx, expr);
}


//---------------------------------------------------------------------------

void HqlCppTranslator::doBuildEvalOnce(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, CHqlBoundExpr * bound)
{
    IHqlExpression * value = expr->queryChild(0);
    CHqlBoundExpr result;
    doBuildAliasValue(ctx, value, result, NULL);
    if (target)
    {
        OwnedHqlExpr translated = result.getTranslatedExpr();
        buildExprAssign(ctx, *target, translated);
    }
    else if (bound)
        bound->set(result);
}


//---------------------------------------------------------------------------

void HqlCppTranslator::doBuildExprSizeof(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    if (ctx.getMatchExpr(expr, tgt))
        return;

    IHqlExpression * child = expr->queryChild(0);
    ITypeInfo * type = child->queryType();
    if (expr->hasAttribute(maxAtom))
    {
        if (type)
        {
            unsigned size = UNKNOWN_LENGTH;
            switch (type->getTypeCode())
            {
            case type_dictionary:
            case type_table:
            case type_groupedtable:
            case type_record:
            case type_row:
                {
                    OwnedHqlExpr record = getSerializedForm(child->queryRecord(), diskAtom);
                    if (isFixedSizeRecord(record))
                        size = getMinRecordSize(record);
                    else
                        size = getMaxRecordSize(record);
                }
                break;
            case type_alien:
                {
                    IHqlAlienTypeInfo * alien = queryAlienType(type);
                    size = alien->getMaxSize();
                    break;
                }
            default:
                size = type->getSize();
                break;
            }
            if (size == UNKNOWN_LENGTH)
                throwError(HQLERR_CouldNotDetermineMaxSize);
            tgt.expr.setown(getSizetConstant(size));
            return;
        }
    }

    if (expr->hasAttribute(minAtom))
    {
        if (type)
        {
            unsigned size = UNKNOWN_LENGTH;
            switch (type->getTypeCode())
            {
            case type_dictionary:
            case type_table:
            case type_groupedtable:
            case type_record:
            case type_row:
                {
                    OwnedHqlExpr record = getSerializedForm(child->queryRecord(), diskAtom);
                    size = getMinRecordSize(record);
                }
                break;
            default:
                size = type->getSize();
                break;
            }
            if (size == UNKNOWN_LENGTH)
                throwError(HQLERR_CouldNotDetermineMinSize);
            tgt.expr.setown(getSizetConstant(size));
            return;
        }
    }

#if 0
    IHqlExpression * limitExpr = expr->queryChild(1);
    if (limitExpr)
    {
        OwnedHqlExpr other = createValue(no_sizeof, expr->getType(), LINK(child));

        HqlExprAssociation * match = ctx.getMatchExpr(other);
        if (match)
        {
            tgt.expr.set(match->expr);
            return;
        }
    }
#endif

    // Size calculation needs to only come in to play if the field/record can't be found in scope
    // otherwise sizeof(field) is wrong if it is inside an ifblock.
    Owned<IReferenceSelector> selector;
    try
    {
        selector.setown(buildReference(ctx, child));
        selector->getSize(ctx, tgt);

        //cache non-constant values in a temporary variable...
        if (!tgt.expr->queryValue())
        {
            if (!isSimpleLength(tgt.expr))
            {
                IHqlExpression * temp = ctx.getTempDeclare(expr->queryType(), tgt.expr);
                tgt.expr.setown(temp);
            }
            ctx.associateExpr(expr, tgt);
        }
    }
    catch (IException * e)
    {
        switch (child->getOperator())
        {
        case no_translated:
            {
                CHqlBoundExpr bound;
                buildExpr(ctx, child, bound);
                tgt.expr.setown(getBoundSize(bound));
                return;
            }
        }

        // Size calculation needs to only come in to play if the field/record can't be found in scope
        // otherwise sizeof(field) is wrong if it is inside an ifblock.
        if (type)
        {
            if (type->getTypeCode() == type_alien)
            {
                IHqlAlienTypeInfo * alien = queryAlienType(type);
                type = alien->queryPhysicalType();
            }

            switch (type->getTypeCode())
            {
            case type_dictionary:
            case type_table:
            case type_groupedtable:
            case type_record:
            case type_row:
                {
                    e->Release();
                    OwnedHqlExpr record = getSerializedForm(child->queryRecord(), diskAtom);
                    if (isFixedSizeRecord(record))
                    {
                        tgt.expr.setown(getSizetConstant(getMinRecordSize(record)));
                        return;
                    }
                    throwError(HQLERR_CannotDetermineSizeVar);
                }
                break;
            case type_void:
                break;
            default:
                if ((type->getSize() != UNKNOWN_LENGTH) && (!selector || !selector->isConditional()))
                {
                    tgt.expr.setown(getSizetConstant(type->getSize()));
                    e->Release();
                    return;
                }
            }
        }
        throw;  // really an internal error - the parse should not have let it through....
    }

}

void HqlCppTranslator::doBuildExprRowDiff(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, IHqlExpression * leftSelector, IHqlExpression * rightRecord, IHqlExpression * rightSelector, StringBuffer & selectorText, bool isCount)
{
    switch (expr->getOperator())
    {
    case no_field:
        {
            IIdAtom * id = expr->queryId();
            IHqlSimpleScope * rightScope = rightRecord->querySimpleScope();
            OwnedHqlExpr match = rightScope ? rightScope->lookupSymbol(id) : NULL;
            if (!match)
                return;

            OwnedHqlExpr left = createSelectExpr(LINK(leftSelector), LINK(expr));
            OwnedHqlExpr right = createSelectExpr(LINK(rightSelector), LINK(match));
            ITypeInfo * leftType = expr->queryType()->queryPromotedType();

            switch (leftType->getTypeCode())
            {
            case type_record:
            case type_row:
                {
                    StringBuffer subSelectorText;
                    subSelectorText.append(selectorText).append(expr->queryName()).append(".");
                    IHqlExpression * record = ::queryRecord(leftType);
                    doBuildExprRowDiff(ctx, target, record, left, right->queryRecord(), right, subSelectorText, isCount);
                    return;
                }
                break;
            case type_dictionary:
            case type_table:
            case type_groupedtable:
                {
                    StringBuffer typeName;
                    getFriendlyTypeStr(leftType, typeName);
                    throwError2(HQLERR_UnsupportedRowDiffType, typeName.str(), str(expr->queryId()));
                }
            }

            StringBuffer fullName;
            fullName.append(selectorText).append(str(id));

            ITypeInfo * rightType = right->queryType()->queryPromotedType();
            if (!leftType->assignableFrom(rightType))
                throwError1(HQLERR_MismatchRowDiffType, fullName.str());

            Owned<ITypeInfo> compareType = ::getPromotedECLType(leftType, rightType);
            OwnedHqlExpr test = createBoolExpr(no_ne, ensureExprType(left, compareType), ensureExprType(right, compareType));
            HqlExprArray args;

            CHqlBoundExpr bound;
            buildExpr(ctx, test, bound);

            StringBuffer specialText;
            generateExprCpp(specialText, target.length).append(",");
            generateExprCpp(specialText, target.expr).append(".refextendstr()");
            OwnedHqlExpr special = createQuoted(specialText.str(), makeBoolType());

            BuildCtx condctx(ctx);
            IHqlStmt * cond = condctx.addFilter(bound.expr);
            //if differ...
            args.append(*LINK(special));
            if (isCount)
                args.append(*createConstant(",1"));
            else
            {
                StringBuffer temp;
                temp.append(",").append(fullName);
                args.append(*createConstant(temp));
            }
            buildFunctionCall(condctx, concatExtendId, args);

            //else if same...
            if (isCount)
            {
                condctx.selectElse(cond);
                args.append(*LINK(special));
                args.append(*createConstant(",0"));
                buildFunctionCall(condctx, concatExtendId, args);
            }
            break;
        }
    case no_ifblock:
        {
            doBuildExprRowDiff(ctx, target, expr->queryChild(1), leftSelector, rightRecord, rightSelector, selectorText, isCount);
            break;
        }
    case no_record:
        {
            ForEachChild(idx, expr)
                doBuildExprRowDiff(ctx, target, expr->queryChild(idx), leftSelector, rightRecord, rightSelector, selectorText, isCount);
            break;
        }
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        break;
    default:
        UNIMPLEMENTED;
    }
}

IHqlExpression * HqlCppTranslator::queryRecord(BuildCtx & ctx, IHqlExpression * expr)
{
    return expr->queryRecord();
}

void HqlCppTranslator::doBuildExprRowDiff(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    CHqlBoundTarget tempTarget;
    createTempFor(ctx, expr, tempTarget);
    StringBuffer selectorText;
    IHqlExpression * left = expr->queryChild(0);
    IHqlExpression * leftRecord = queryRecord(ctx, left);
    IHqlExpression * right = expr->queryChild(1);
    IHqlExpression * rightRecord = queryRecord(ctx, right);

    ctx.addAssign(tempTarget.length, queryZero());
    doBuildExprRowDiff(ctx, tempTarget, leftRecord, left, rightRecord, right, selectorText, expr->hasAttribute(countAtom));

    OwnedHqlExpr result = createValue(no_substring, LINK(unknownStringType), tempTarget.getTranslatedExpr(), createValue(no_rangefrom, makeVoidType(), createConstant(2)));
    buildExpr(ctx, result, tgt);
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityCacheAlias(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(1);

    return buildCachedActivity(ctx, dataset);
}

//---------------------------------------------------------------------------
// no_cloned

ABoundActivity * HqlCppTranslator::doBuildActivityCloned(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);

    return buildCachedActivity(ctx, dataset);
}

//---------------------------------------------------------------------------
// no_addfiles

static void unwindAddFiles(HqlExprArray & args, IHqlExpression * expr, bool reqIsOrdered, bool isOrderedPull)
{
    if ((expr->getOperator() == no_addfiles) && (isOrdered(expr) == reqIsOrdered) && (expr->hasAttribute(pullAtom) == isOrderedPull))
    {
        unwindAddFiles(args, expr->queryChild(0), reqIsOrdered, isOrderedPull);
        unwindAddFiles(args, expr->queryChild(1), reqIsOrdered, isOrderedPull);
    }
    else
        args.append(*LINK(expr));
}

static IHqlExpression * queryRootConcatActivity(IHqlExpression * expr)
{
    for (;;)
    {
        node_operator curOp = expr->getOperator();
        switch (curOp)
        {
        case no_nofold:
        case no_section:
        case no_sectioninput:
        case no_preservemeta:
        case no_nocombine:
        case no_forcegraph:
            break;
        default:
            return expr;
        }
        expr = expr->queryChild(0);
    }
}

ABoundActivity * HqlCppTranslator::doBuildActivityConcat(BuildCtx & ctx, IHqlExpression * expr)
{
    HqlExprArray inExprs;
    bool ordered = isOrdered(expr);
    bool orderedPull = expr->hasAttribute(pullAtom);
    unwindAddFiles(inExprs, expr, ordered, orderedPull);

    //If all coming from disk, probably better to pull them in order.
    bool allFromDisk = options.orderDiskFunnel;
    CIArray bound;
    ForEachItemIn(idx, inExprs)
    {
        IHqlExpression * cur = &inExprs.item(idx);
        bound.append(*buildCachedActivity(ctx, cur));

        cur = queryRootConcatActivity(cur);

        switch (cur->getOperator())
        {
        case no_compound_diskread:
        case no_compound_disknormalize:
        case no_compound_diskaggregate:
        case no_compound_diskcount:
        case no_compound_diskgroupaggregate:
            break;
        case no_temptable:
        case no_inlinetable:
        case no_temprow:
        case no_datasetfromrow:
        case no_projectrow:
        case no_createrow:
        case no_typetransfer:
            break;
        case no_table:
            switch (cur->queryChild(2)->getOperator())
            {
            case no_thor: case no_flat:
                break;
            default:
                allFromDisk = false;
                break;
            }
            break;
        default:
            allFromDisk = false;
            break;
        }
    }

    if (orderedPull || (allFromDisk && !targetRoxie()))
        ordered = true;
    if (!expr->hasAttribute(orderedAtom) && insideChildQuery(ctx))
        ordered = true;

    bool useImplementationClass = options.minimizeActivityClasses && targetRoxie();
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKfunnel, expr, "Funnel");
    if (useImplementationClass)
        instance->setImplementationClass(newFunnelArgId);

    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    if (!useImplementationClass)
    {
        if (ordered)
            doBuildBoolFunction(instance->classctx, "isOrdered", true);
        if (orderedPull)
            doBuildBoolFunction(instance->classctx, "pullSequentially", orderedPull);
    }
    else
    {
        instance->addConstructorParameter(queryBoolExpr(ordered));
        instance->addConstructorParameter(queryBoolExpr(orderedPull));
    }


    buildInstanceSuffix(instance);

    ForEachItemIn(idx2, bound)
        buildConnectInputOutput(ctx, instance, (ABoundActivity *)&bound.item(idx2), 0, idx2);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityMerge(BuildCtx & ctx, IHqlExpression * expr)
{
    CIArrayOf<ABoundActivity> inputs;
    ForEachChild(idx, expr)
    {
        IHqlExpression *cur = expr->queryChild(idx);
        if (!cur->isAttribute())
            inputs.append(*buildCachedActivity(ctx, cur));
    }

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKmerge, expr, "Merge");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * sortAttr = expr->queryAttribute(sortedAtom);
    HqlExprArray sorts;
    unwindChildren(sorts, sortAttr);

    if (sorts.ordinality() != 0)
    {
        OwnedHqlExpr sortOrder = createValueSafe(no_sortlist, makeSortListType(NULL), sorts);
        DatasetReference dsRef(dataset, no_activetable, NULL);
        buildCompareFuncHelper(*this, *instance, "compare", sortOrder, dsRef);
        if (!instance->isLocal)
            generateSerializeKey(instance->nestedctx, no_none, dsRef, sorts, !instance->isChildActivity(), true);
    }
    else
        throwError(HQLERR_InputMergeNotSorted);

    if (expr->hasAttribute(dedupAtom))
        doBuildBoolFunction(instance->classctx, "dedup", true);

    buildInstanceSuffix(instance);

    ForEachItemIn(idx2, inputs)
        buildConnectInputOutput(ctx, instance, &inputs.item(idx2), 0, idx2);

    return instance->getBoundActivity();
}


ABoundActivity * HqlCppTranslator::doBuildActivityRegroup(BuildCtx & ctx, IHqlExpression * expr)
{
    HqlExprArray inExprs;
    expr->unwindList(inExprs, no_regroup);

    CIArray bound;
    ForEachItemIn(idx, inExprs)
    {
        IHqlExpression & cur = inExprs.item(idx);
        if (!cur.isAttribute())
            bound.append(*buildCachedActivity(ctx, &cur));
    }

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKregroup, expr, "Regroup");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);
    buildInstanceSuffix(instance);

    ForEachItemIn(idx2, bound)
        buildConnectInputOutput(ctx, instance, (ABoundActivity *)&bound.item(idx2), 0, idx2);

    return instance->getBoundActivity();
}

static void unwindNonEmpty(HqlExprCopyArray & args, IHqlExpression * expr, bool isLocal)
{
    if ((expr->getOperator() == no_nonempty) && (expr->hasAttribute(localAtom) == isLocal))
    {
        ForEachChild(i, expr)
            unwindNonEmpty(args, expr->queryChild(i), isLocal);
    }
    else
        args.append(*expr);
}

ABoundActivity * HqlCppTranslator::doBuildActivityNonEmpty(BuildCtx & ctx, IHqlExpression * expr)
{
    HqlExprCopyArray inExprs;
    unwindNonEmpty(inExprs, expr, expr->hasAttribute(localAtom));

    CIArray bound;
    ForEachItemIn(idx, inExprs)
    {
        IHqlExpression * cur = &inExprs.item(idx);
        if (!cur->isAttribute())
            bound.append(*buildCachedActivity(ctx, cur));
    }

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKnonempty, expr, "NonEmpty");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    buildInstanceSuffix(instance);

    ForEachItemIn(idx2, bound)
        buildConnectInputOutput(ctx, instance, (ABoundActivity *)&bound.item(idx2), 0, idx2);

    return instance->getBoundActivity();
}


ABoundActivity * HqlCppTranslator::doBuildActivitySplit(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset  = expr->queryChild(0);
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    bool useImplementationClass = options.minimizeActivityClasses;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKsplit, expr, "Split");
    if (useImplementationClass)
        instance->setImplementationClass(newSplitArgId);

    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    //IHqlExpression * numWays = expr->queryChild(1);
    OwnedHqlExpr numWaysCallback = createUnknown(no_callback, LINK(sizetType), countAtom, instance->createOutputCountCallback());
    OwnedHqlExpr numWays = createTranslated(numWaysCallback);
    bool balanced = expr->hasAttribute(balancedAtom);
    if (!useImplementationClass)
    {
        if (!matchesConstantValue(numWays, 2))
            doBuildUnsignedFunction(instance->classctx, "numBranches", numWays);
        if (balanced)
            doBuildBoolFunction(instance->classctx, "isBalanced", true);
    }
    else
    {
        instance->addConstructorParameter(numWays);
        instance->addConstructorParameter(queryBoolExpr(balanced));
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    return instance->getBoundActivity();
}


ABoundActivity * HqlCppTranslator::doBuildActivitySpill(BuildCtx & ctx, IHqlExpression * expr)
{
    return doBuildActivityOutput(ctx, expr, false);
}


bool HqlCppTranslator::isCurrentActiveGraph(BuildCtx & ctx, IHqlExpression * graphTag)
{
    SubGraphInfo * activeSubgraph = queryActiveSubGraph(ctx);
    assertex(activeSubgraph);
    return (graphTag == activeSubgraph->graphTag);
}


IHqlExpression * HqlCppTranslator::createLoopSubquery(IHqlExpression * dataset, IHqlExpression * selSeq, IHqlExpression * rowsid, IHqlExpression * body, IHqlExpression * filter, IHqlExpression * again, IHqlExpression * counter, bool multiInstance, unsigned & loopAgainResult)
{
    //Now need to generate the body of the loop.
    //output dataset is result 0
    //input dataset is fed in using result 1
    //counter (if required) is fed in using <result-2>[0].counter;
    ChildGraphExprBuilder graphBuilder(2);
    IHqlExpression * graphid = graphBuilder.queryRepresents();

    LinkedHqlExpr transformedBody = body;
    LinkedHqlExpr transformedAgain = again;

    //Result 1 is the input dataset.
    HqlExprArray args;
    args.append(*LINK(dataset->queryRecord()));
    args.append(*LINK(graphid));
    args.append(*getSizetConstant(1));
    if (isGrouped(dataset))
        args.append(*createAttribute(groupedAtom));
    args.append(*createAttribute(_loop_Atom));
    if (multiInstance)
        args.append(*createAttribute(_streaming_Atom));
    if (targetThor())    // MORE: && !isChildQuery(ctx)..
        args.append(*createAttribute(_distributed_Atom));
    OwnedHqlExpr inputResult= createDataset(no_getgraphresult, args);

    //Result 2 is the counter - if present
    OwnedHqlExpr counterResult;
    if (counter)
    {
        OwnedHqlExpr select = createCounterAsGraphResult(counter, graphid, 2);
        transformedBody.setown(replaceExpression(transformedBody, counter, select));
        if (transformedAgain)
        {
            //The COUNTER for the global termination condition is whether to execute iteration COUNTER, 1=1st iter
            //Since we're evaluating the condition in the previous iteration it needs to be increased by 1.
            OwnedHqlExpr nextCounter = adjustValue(select, 1);
            transformedAgain.setown(replaceExpression(transformedAgain, counter, nextCounter));
        }

        graphBuilder.addInput();
    }

    //first create the result...
    //Need to replace ROWS(LEFT) with the result1
    OwnedHqlExpr left = createSelector(no_left, dataset, selSeq);
    OwnedHqlExpr rowsExpr = createDataset(no_rows, LINK(left), LINK(rowsid));
    transformedBody.setown(replaceExpression(transformedBody, rowsExpr, inputResult));

    OwnedHqlExpr result = createValue(no_setgraphresult, makeVoidType(), LINK(transformedBody), LINK(graphid), getSizetConstant(0), createAttribute(_loop_Atom));
    graphBuilder.addAction(result);

    if (transformedAgain)
    {
        LinkedHqlExpr nextLoopDataset = transformedBody;
        if (filter)
        {
            //If there is a loop filter then the global condition is applied to dataset filtered by that.
            OwnedHqlExpr mappedFilter = replaceSelector(filter, left, nextLoopDataset);
            nextLoopDataset.setown(createDataset(no_filter, nextLoopDataset.getClear(), LINK(mappedFilter)));
        }
        transformedAgain.setown(replaceExpression(transformedAgain, rowsExpr, nextLoopDataset));

        loopAgainResult = graphBuilder.addInput();

        //MORE: Add loopAgainResult as an attribute on the no_childquery rather than using a reference parameter
        OwnedHqlExpr againResult = convertScalarToGraphResult(transformedAgain, queryBoolType(), graphid, loopAgainResult);
        graphBuilder.addAction(againResult);

    }

    return graphBuilder.getGraph();
}



ABoundActivity * HqlCppTranslator::doBuildActivityLoop(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * count = queryRealChild(expr, 1);
    IHqlExpression * filter = queryRealChild(expr, 2);
    IHqlExpression * loopCond = queryRealChild(expr, 3);
    IHqlExpression * body = expr->queryChild(4);
    assertex(body->getOperator() == no_loopbody);

    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
    IHqlExpression * rowsid = expr->queryAttribute(_rowsid_Atom);
    IHqlExpression * selSeq = querySelSeq(expr);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    ThorActivityKind kind = TAKnone;
    //LOOP(dataset, count[, rowFilter])
    //LOOP(dataset, <dataset-filter>, <rowfilter>)
    //LOOP(dataset, <dataset-filter|rowfilter> 
    if (count)
        kind = TAKloopcount;
    else if (loopCond)
        kind = TAKloopdataset;
    else
        kind = TAKlooprow;

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, "Loop");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    if (filter)
    {
        MemberFunction func(*this, instance->startctx, "virtual bool sendToLoop(unsigned counter, const void * _self)");
        func.ctx.addQuotedLiteral("unsigned char * self = (unsigned char *) _self;");

        associateCounter(func.ctx, counter, "counter");
        bindTableCursor(func.ctx, dataset, "self", no_left, selSeq);
        buildReturn(func.ctx, filter);
    }

    if (count)
        doBuildUnsignedFunction(instance->startctx, "numIterations", count);

    if (loopCond)
    {
        MemberFunction func(*this, instance->startctx, "virtual bool loopAgain(unsigned counter, unsigned numRows, const void * * _rows)");
        func.ctx.addQuotedLiteral("unsigned char * * rows = (unsigned char * *) _rows;");

        associateCounter(func.ctx, counter, "counter");

        bindRows(func.ctx, no_left, selSeq, rowsid, dataset, "numRows", "rows", options.mainRowsAreLinkCounted);
        buildReturn(func.ctx, loopCond);
    }

    IHqlExpression * loopFirst = queryAttributeChild(expr, _loopFirst_Atom, 0);
    if (loopFirst)
        doBuildBoolFunction(instance->startctx, "loopFirstTime", loopFirst);

    IHqlExpression * parallel = expr->queryAttribute(parallelAtom);
    if (parallel && (targetHThor() || !count || loopCond))
        parallel = NULL;

    if (parallel)
    {
        IHqlExpression * numThreads = parallel->queryChild(0);
        if (numThreads)
            doBuildUnsignedFunction(instance->startctx, "defaultParallelIterations", numThreads);
    }

    StringBuffer flags;
    if (counter) flags.append("|LFcounter");
    if (parallel) flags.append("|LFparallel");
    if (filter) flags.append("|LFfiltered");
    if (loopFirst) flags.append("|LFnewloopagain");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    {
        MemberFunction func(*this, instance->startctx, "virtual void createParentExtract(rtlRowBuilder & builder)");

        //Now need to generate the body of the loop.
        unsigned loopAgainResult = 0;
        OwnedHqlExpr childquery = createLoopSubquery(dataset, selSeq, rowsid, body->queryChild(0), filter, loopCond, counter, (parallel != NULL), loopAgainResult);

        ChildGraphBuilder builder(*this, childquery);
        unique_id_t loopId = builder.buildLoopBody(func.ctx, (parallel != NULL), getBoolAttribute(expr, fewAtom));
        instance->addAttributeInt("_loopid", loopId);

        if (loopAgainResult)
            doBuildUnsignedFunction(instance->startctx, "loopAgainResult", loopAgainResult);
    }

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    return instance->getBoundActivity();
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityGraphLoop(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * count = expr->queryChild(1);
    IHqlExpression * body = expr->queryChild(2);
    assertex(body->getOperator() == no_loopbody);
    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
    IHqlExpression * rowsid = expr->queryAttribute(_rowsid_Atom);
    IHqlExpression * selSeq = querySelSeq(expr);
    IHqlExpression * parallel = expr->queryAttribute(parallelAtom);
    if (parallel && targetHThor())
        parallel = NULL;

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    ThorActivityKind kind = parallel ? TAKparallelgraphloop : TAKgraphloop;

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, "GraphLoop");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    doBuildUnsignedFunction(instance->startctx, "numIterations", count);

    StringBuffer flags;
    if (counter) flags.append("|GLFcounter");
    if (parallel) flags.append("|GLFparallel");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    {
        MemberFunction func(*this, instance->startctx, "virtual void createParentExtract(rtlRowBuilder & builder)");

        //Now need to generate the body of the loop.
        //output dataset is result 0
        //input dataset is fed in using result 1
        //counter (if required) is fed in using result 2[0].counter;
        unique_id_t loopId = buildGraphLoopSubgraph(func.ctx, dataset, selSeq, rowsid, body->queryChild(0), counter, (parallel != NULL), getBoolAttribute(expr, fewAtom));
        instance->addAttributeInt("_loopid", loopId);
    }

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    return instance->getBoundActivity();
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityRemote(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IHqlExpression * child = expr->queryChild(0);
    if (targetHThor() || (targetThor() && !insideChildQuery(ctx)))
    {
        if (!options.alwaysAllowAllNodes)
            throwError(HQLERR_RemoteNoMeaning);
    }
    if (isGrouped(child))
        throwError(HQLERR_RemoteGrouped);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKremotegraph, expr, "Remote");
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * rowlimit = expr->queryAttribute(rowLimitAtom);

    if (rowlimit)
    {
        doBuildUnsigned64Function(instance->startctx, "getRowLimit", rowlimit->queryChild(0));
        IHqlExpression * fail = queryChildOperator(no_fail, rowlimit);
        if (fail)
        {
            MemberFunction func(*this, instance->startctx, "virtual void onLimitExceeded()");
            buildStmt(func.ctx, fail);
        }
    }

    {
        MemberFunction func(*this, instance->startctx, "virtual void createParentExtract(rtlRowBuilder & builder)");

        //output dataset is result 0
        unique_id_t remoteId = buildRemoteSubgraph(func.ctx, dataset);
        instance->addAttributeInt("_graphid", remoteId);
    }

    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}


//---------------------------------------------------------------------------
// no_update

void HqlCppTranslator::doBuildStmtUpdate(BuildCtx & ctx, IHqlExpression * expr)
{
    // MJH - CODE TO DO UPDATE GOES HERE
    PrintLog("in HqlCppTranslator::doBuildStmtUpdate()");
}


IHqlExpression * HqlCppTranslator::createClearRowCall(BuildCtx & ctx, BoundRow * self)
{
    IHqlExpression * record = self->querySelector()->queryRecord();
    OwnedHqlExpr clearFunc = getClearRecordFunction(record, 0);
    StringBuffer s;

    ensureContextAvailable(ctx);
    IHqlExpression * boundRow = self->queryBound();
    OwnedHqlExpr rowPointer = getPointer(boundRow);

    generateExprCpp(s, clearFunc).append("(");
    if (self->queryBuilder())
    {
        generateExprCpp(s, self->queryBuilder());
    }
    else
    {
        StringBuffer builderName;
        getUniqueId(builderName.append("rb"));
        StringBuffer temp;
        temp.append("RtlStaticRowBuilder ").append(builderName).append("(");
        if (ctx.queryMatchExpr(constantMemberMarkerExpr))
            temp.append("(byte *)");

        generateExprCpp(temp, rowPointer);
        temp.append(",").append(getMaxRecordSize(record)).append(");");
        ctx.addQuoted(temp);
        s.append(builderName);
    }
    s.append(", ctx)");
    return createQuoted(s.str(), makeVoidType());
}


void HqlCppTranslator::associateSkipReturnMarker(BuildCtx & ctx, IHqlExpression * value, BoundRow * self)
{
    ctx.associateExpr(skipReturnMarker, value);
}

void HqlCppTranslator::doBuildStmtSkip(BuildCtx & ctx, IHqlExpression * expr, bool * canReachFollowing)
{
    HqlExprAssociation * match = ctx.queryMatchExpr(skipActionMarker);
    IHqlExpression * cond = expr->queryChild(0);
    if (cond && cond->isRecord())
        cond = NULL;
    if (canReachFollowing)
        *canReachFollowing = true;

    BuildCtx subctx(ctx);
    if (match)
    {
        IHqlCodeCallback * callback = static_cast<IHqlCodeCallback *>(match->queryExpr()->queryUnknownExtra());
        if (cond)
            buildFilter(subctx, cond);
        callback->buildCode(*this, subctx);
    }
    else
    {
        match = ctx.queryMatchExpr(skipReturnMarker);
        if (match)
        {
            if (cond)
                buildFilteredReturn(ctx, cond, match->queryExpr());
            else
            {
                buildReturn(ctx, match->queryExpr());
                if (canReachFollowing)
                    *canReachFollowing = false;
            }
        }
        else
            throwError(HQLERR_SkipNotValidHere);
    }
}

void HqlCppTranslator::doBuildStmtAssert(BuildCtx & ctx, IHqlExpression * expr)
{
    if (!options.checkAsserts)
        return;

    IHqlExpression * cond = expr->queryChild(0);
    LinkedHqlExpr locationAttr = expr->queryAttribute(_location_Atom);
    if (!locationAttr)
    {
        IHqlExpression * activeNamedActivity = queryActiveNamedActivity();
        if (activeNamedActivity)
        {
            ISourcePath * sourcePath = activeNamedActivity->querySourcePath();
            if (sourcePath)
                locationAttr.setown(createLocationAttr(sourcePath, 0, 0, 0));
        }
    }

    LinkedHqlExpr msg = queryRealChild(expr, 1);
    if (!msg)
        msg.setown(createDefaultAssertMessage(cond));

    if (expr->hasAttribute(constAtom))
    {
        IValue * condValue = cond->queryValue();
        assertex(condValue && msg->queryValue());
        if (condValue->getBoolValue())
            return;

        StringBuffer msgText;
        getStringValue(msgText, msg);
        reportErrorDirect(locationAttr, ERR_ASSERTION_FAILS, msgText.str(), false);
        return;
    }

    BuildCtx condctx(ctx);
    OwnedHqlExpr inverse = getInverse(cond);
    buildFilter(condctx, inverse);

    OwnedHqlExpr action;
    HqlExprArray args;
    args.append(*getSizetConstant(ERR_ASSERTION_FAILS));
    args.append(*LINK(msg));

    ECLlocation location;
    if (locationAttr)
        location.extractLocationAttr(locationAttr);

    if (location.sourcePath)
    {
        const char * filename = str(location.sourcePath);
        if (options.reportAssertFilenameTail)
            filename = pathTail(filename);
        args.append(*createConstant(filename));
    }
    else
        args.append(*getNullStringPointer(true));

    args.append(*getSizetConstant(location.lineno));
    args.append(*getSizetConstant(location.column));
    args.append(*createConstant(expr->hasAttribute(failAtom)));

    action.setown(bindFunctionCall(addWorkunitAssertFailureId, args));

    buildStmt(condctx, action);
}


void HqlCppTranslator::doBuildStmtCluster(BuildCtx & ctx, IHqlExpression * expr)
{
    pushCluster(ctx, expr->queryChild(1));
    buildStmt(ctx, expr->queryChild(0));
    popCluster(ctx);
}

//---------------------------------------------------------------------------
// no_apply

void HqlCppTranslator::doBuildSequenceFunc(BuildCtx & ctx, IHqlExpression * seq, bool ignoreInternal)
{
    if (ignoreInternal && isInternalSeq(seq))
        return;

    //virtual unsigned getSequence() = 0;
    StringBuffer s;
    s.append("virtual int getSequence() { return ");
    if (seq)
        generateExprCpp(s, seq);
    else
        s.append("-1");
    s.append("; }");
    ctx.addQuoted(s);
}

class ApplyStmtBuilder
{
public:
    ApplyStmtBuilder(HqlCppTranslator & _translator) : translator(_translator) {}
//  ~ApplyStmtBuilder() { assertex(!builder); }         // don't do this because throwing exceptions inside destructors is very messy

    void flush(BuildCtx & ctx)
    {
        if (builder)
        {
            OwnedHqlExpr childquery = builder->getGraph(no_orderedactionlist);
            translator.buildStmt(ctx, childquery);
            builder.clear();
        }
    }

    bool requiresGraph(BuildCtx & ctx, IHqlExpression * expr)
    {
        if (!expr) return false;
        switch (expr->getOperator())
        {
        case NO_ACTION_REQUIRES_GRAPH:
            return true;
        case no_parallel:
        case no_sequential:
        case no_actionlist:
        case no_orderedactionlist:
        case no_compound:
            {
                ForEachChild(idx, expr)
                {
                    if (requiresGraph(ctx, expr->queryChild(idx)))
                        return true;
                }
                break;
            }
        case no_if:
            return requiresGraph(ctx, expr->queryChild(1)) || requiresGraph(ctx, expr->queryChild(2));
        }
        return false;
    }

    void addGraphStmt(BuildCtx & ctx, IHqlExpression * expr)
    {
        if (!builder)
            builder.setown(new ChildGraphExprBuilder(0));
        builder->addAction(expr);
    }

    void buildStmt(BuildCtx & ctx, IHqlExpression * expr)
    {
        node_operator op = expr->getOperator();
        switch (expr->getOperator())
        {
        case NO_ACTION_REQUIRES_GRAPH:
            addGraphStmt(ctx, expr);
            break;
        case no_parallel:
        case no_sequential:
        case no_actionlist:
        case no_orderedactionlist:
        case no_compound:
            {
                ForEachChild(idx, expr)
                {
                    buildStmt(ctx, expr->queryChild(idx));
                    if (op == no_sequential)
                        flush(ctx);
                }
                break;
            }
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            break;
        case no_if:
            if (requiresGraph(ctx, expr))
                addGraphStmt(ctx, expr);
            else
                translator.buildStmt(ctx, expr);
            break;
        default:
            translator.buildStmt(ctx, expr);
            break;
        }
    }

protected:
    HqlCppTranslator & translator;
    Owned<ChildGraphExprBuilder> builder;
};

ABoundActivity * HqlCppTranslator::doBuildActivityApply(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    StringBuffer s;
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * start = expr->queryAttribute(beforeAtom);
    IHqlExpression * end = expr->queryAttribute(afterAtom);
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKapply, expr, "Apply");
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    doBuildSequenceFunc(instance->classctx, querySequence(expr), false);

    ApplyStmtBuilder builder(*this);
    {
        MemberFunction func(*this, instance->startctx, "virtual void apply(const void * _self)");
        s.clear().append("unsigned char * self = (unsigned char *) _self;");
        func.ctx.addQuoted(s);

        bindTableCursor(func.ctx, dataset, "self");
        unsigned max = expr->numChildren();

        for (unsigned i=1; i < max; i++)
            builder.buildStmt(func.ctx, expr->queryChild(i));
        builder.flush(func.ctx);
    }

    if (start)
    {
        MemberFunction func(*this, instance->startctx, "virtual void start()");
        builder.buildStmt(func.ctx, start->queryChild(0));
        builder.flush(func.ctx);
    }

    if (end)
    {
        MemberFunction func(*this, instance->startctx, "virtual void end()");
        builder.buildStmt(func.ctx, end->queryChild(0));
        builder.flush(func.ctx);
    }

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
//-- no_thor --

ActivityInstance * HqlCppTranslator::queryCurrentActivity(BuildCtx & ctx)
{
    return static_cast<ActivityInstance *>(ctx.queryFirstAssociation(AssocActivityInstance));
}

bool HqlCppTranslator::insideActivityRemoteSerialize(BuildCtx & ctx)
{
    ActivityInstance * activeActivity = queryCurrentActivity(ctx);
    return activeActivity && activeActivity->requiresRemoteSerialize();
}

unique_id_t HqlCppTranslator::queryCurrentActivityId(BuildCtx & ctx)
{
    ActivityInstance * activeActivity = queryCurrentActivity(ctx);
    if (activeActivity)
        return activeActivity->activityId;
    HqlExprAssociation * match = ctx.queryMatchExpr(queryActivityIdMarker());
    if (match)
        return getIntValue(match->queryExpr());
    return 0;
}

IHqlExpression * HqlCppTranslator::getCurrentActivityId(BuildCtx & ctx)
{
    ActivityInstance * activeActivity = queryCurrentActivity(ctx);
    if (activeActivity)
        return getSizetConstant(activeActivity->activityId);
    HqlExprAssociation * match = ctx.queryMatchExpr(queryActivityIdMarker());
    if (match)
        return LINK(match->queryExpr());
    return getSizetConstant(0);
}

bool HqlCppTranslator::insideRemoteGraph(BuildCtx & ctx)
{
    //Dubious... how about a remote child query?
    SubGraphInfo * activeSubgraph = queryActiveSubGraph(ctx);
    if (!activeSubgraph)
        return false;
    return (activeSubgraph->type == SubGraphRemote);
}

bool HqlCppTranslator::insideChildOrLoopGraph(BuildCtx & ctx)
{
    FilteredAssociationIterator iter(ctx, AssocSubGraph);
    ForEach(iter)
    {
        SubGraphInfo & cur = static_cast<SubGraphInfo &>(iter.get());
        if ((cur.type == SubGraphChild) || (cur.type == SubGraphLoop))
            return true;
    }
    return false;
}

bool HqlCppTranslator::insideChildQuery(BuildCtx & ctx)
{
    ParentExtract * extract = static_cast<ParentExtract*>(ctx.queryFirstAssociation(AssocExtract));
    if (extract)
        return extract->insideChildQuery();
    EvalContext * instance = queryEvalContext(ctx);
    if (instance)
        return instance->insideChildQuery();
    return false;
}


unsigned HqlCppTranslator::curSubGraphId(BuildCtx & ctx)
{
    SubGraphInfo * activeSubgraph = queryActiveSubGraph(ctx);
    return activeSubgraph ? activeSubgraph->id : 0;
}

unsigned HqlCppTranslator::doBuildThorChildSubGraph(BuildCtx & ctx, IHqlExpression * expr, SubGraphType kind, unsigned reservedId, IHqlExpression * graphTag)
{
//NB: Need to create the graph in the correct order so the references to property trees we retain
// remain live..
    unsigned thisId = reservedId ? reservedId : nextActivityId();
    unsigned graphId = thisId;
    SubGraphInfo * activeSubgraph = queryActiveSubGraph(ctx);
    IPropertyTree * node = createPTree("node");
    if (activeSubgraph)
    {
        if (!graphTag)
            graphTag = activeSubgraph->graphTag;
        node = activeSubgraph->tree->addPropTree("node", node);
        if (activeSubgraph->graphTag == graphTag)
            graphId = activeSubgraph->graphId;
    }
    else
        node = activeGraph->xgmml->addPropTree("node", node);

    node->setPropInt("@id", thisId);

    IPropertyTree * graphAttr = node->addPropTree("att", createPTree("att"));
    IPropertyTree * subGraph = graphAttr->addPropTree("graph", createPTree("graph"));

    Owned<SubGraphInfo> graphInfo = new SubGraphInfo(subGraph, thisId, graphId, graphTag, kind);
    ctx.associate(*graphInfo);

    IHqlExpression * numResultsAttr = expr->queryAttribute(numResultsAtom);
    if (numResultsAttr)
        addGraphAttributeInt(subGraph, "_numResults", getIntValue(numResultsAttr->queryChild(0), 0));
    if (expr->hasAttribute(multiInstanceAtom))
        subGraph->setPropBool("@multiInstance", true);
    if (expr->hasAttribute(delayedAtom))
        subGraph->setPropBool("@delayed", true);
    if (expr->queryAttribute(childAtom))
        subGraph->setPropBool("@child", true);
    if (expr->hasAttribute(sequentialAtom))
        subGraph->setPropBool("@sequential", true);
    if (kind == SubGraphLoop)
        subGraph->setPropBool("@loopBody", true);

    if (insideChildOrLoopGraph(ctx))
    {
        graphAttr->setProp("@name", "_kind");
        graphAttr->setPropInt("@value", TAKsubgraph);

        ActivityInstance * curActivityInstance = queryCurrentActivity(ctx);
        if (curActivityInstance)
            addGraphAttributeInt(node, "_parentActivity", curActivityInstance->activityId);
    }

    OwnedHqlExpr idExpr = createConstant((__int64)thisId);
    ctx.associateExpr(expr, idExpr);
    if (thisId == options.subgraphToRegenerate)
    {
        StringBuffer ecl;
        regenerateECL(expr, ecl);
        ecl.replaceString("\r","");
        fputs(ecl.str(), stdout);

        fflush(stdout);
    }

    BuildCtx subctx(ctx);
    ForEachChild(idx, expr)
    {
        IHqlExpression * cur = expr->queryChild(idx);
        switch (cur->getOperator())
        {
        case no_subgraph:
             doBuildThorChildSubGraph(ctx, cur, SubGraphChild, 0, graphTag);
             break;
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            break;
        default:
            buildRootActivity(subctx, cur);
            break;
        }
    }

    ctx.removeAssociation(graphInfo);
    return thisId;
}

unsigned HqlCppTranslator::doBuildThorSubGraph(BuildCtx & ctx, IHqlExpression * expr, SubGraphType kind, unsigned reservedId, IHqlExpression * graphTag)
{
    BuildCtx graphctx(ctx);
    graphctx.addGroup();

    bool needToCreateGraph = !activeGraph;
    if (!graphTag && outputLibraryId)
        graphTag = outputLibraryId;
    if (needToCreateGraph)
        beginGraph();

    unsigned thisId = doBuildThorChildSubGraph(graphctx, expr, kind, reservedId, graphTag);

    if (needToCreateGraph)
        endGraph();

    return thisId;
}


void HqlCppTranslator::beginGraph(const char * _graphName)
{
    if (activeGraph)
        throwError(HQLERR_NestedThorNodes);

    graphSeqNumber++;
    StringBuffer graphName;
    if (!_graphName)
        graphName.append("graph").append(graphSeqNumber);
    else
        graphName.append(_graphName);

    activeGraph.setown(new GeneratedGraphInfo(graphName, graphLabel));
    graphLabel.clear();

    if (insideLibrary())
        activeGraph->xgmml->setPropBool("@library", true);
    if (curWfid)
        activeGraph->xgmml->setPropInt("@wfid", curWfid);
}


void HqlCppTranslator::endGraph()
{
    graphs.append(*activeGraph.getClear());
}

void HqlCppTranslator::clearGraph()
{
    activeGraph.clear();
}


/*
Tricky getting this in the correct order, problems are:
1. generally best to move limits over projects and everything else
2. Don't move a limit over a project if that will be part of a compound disk/index read.
3. Need to make sure preloaded items aren't messed up by the resourcing.
4. Resourcing ensures that shared items are explicitly shared if needed, it also ensures that
   index-reads are not shared if it would be inefficient.
5. Worth optimizing the graph before resourcing because it may move split points - e.g.,
   moving a filter/choosen over a sort/spill greatly reducing the time taken.
6. Optimizing may move items over conditions which can improve the executed code.  E.g.,
   where it allows a limit to be added to something.
*/

IHqlExpression * HqlCppTranslator::optimizeCompoundSource(IHqlExpression * expr, unsigned flags)
{
    CompoundSourceTransformer transformer(*this, flags);
    return transformer.process(expr);
}

IHqlExpression * HqlCppTranslator::optimizeGraphPostResource(IHqlExpression * expr, unsigned csfFlags, bool projectBeforeSpill, bool insideChildQuery)
{
    LinkedHqlExpr resourced = expr;
    // Second attempt to spot compound disk reads - this time of spill files for thor.
    resourced.setown(optimizeCompoundSource(resourced, csfFlags));

    //MORE: This call (enabled by -fparanoid) isn't correct when this is processing a child query
    checkNormalized(resourced);

    //insert projects after compound created...
    if (options.optimizeResourcedProjects)
    {
        OwnedHqlExpr optimized = insertImplicitProjects(*this, resourced.get(), projectBeforeSpill);
        traceExpression("AfterResourcedImplicit", resourced);
        checkNormalized(optimized);

        if (optimized != resourced)
            resourced.setown(optimizeCompoundSource(optimized, csfFlags));
    }

    //Now call the optimizer again - the main purpose is to move projects over limits and into compound index/disk reads
    if (options.optimizeGraph)
    {
        traceExpression("BeforeOptimize2", resourced);
        resourced.setown(optimizeHqlExpression(queryErrorProcessor(), resourced, getOptimizeFlags(insideChildQuery)|HOOcompoundproject));
        traceExpression("AfterOptimize2", resourced);
    }
    resourced.setown(optimizeCompoundSource(resourced, csfFlags));
    return resourced.getClear();
}

IHqlExpression * HqlCppTranslator::getResourcedGraph(IHqlExpression * expr, IHqlExpression * graphIdExpr)
{
    LinkedHqlExpr resourced = expr;

    unsigned csfFlags = CSFindex|options.optimizeDiskFlag;
    if (!targetRoxie())
        csfFlags |= CSFcompoundSpill;

    //Convert queries on preloaded into compound activities - before resourcing so keyed gets done correctly
    checkNormalized(expr);
    traceExpression("BeforeCompound", resourced);
    if (true)
        resourced.setown(optimizeCompoundSource(resourced, CSFpreload|csfFlags));

    //Check to see if fields can be removed - this helps LOOP bodies, but also seems to help situations where hoisting
    //expressions prevents child expressions from preventing fields from being removed.
    //Perform before the optimizeHqlExpression() so decisions about reducing row sizes are accurate
    traceExpression("BeforeImplicitProjectGraph", resourced);
    resourced.setown(insertImplicitProjects(*this, resourced, false));

    // Call optimizer before resourcing so items get moved over conditions, and remove other items
    // which would otherwise cause extra spills.
    traceExpression("BeforeOptimize", resourced);
    unsigned optFlags = getOptimizeFlags(false);

    checkNormalized(resourced);
    if (options.optimizeGraph)
    {
        resourced.setown(optimizeHqlExpression(queryErrorProcessor(), resourced, optFlags|HOOfiltersharedproject));
        //have the following on an "aggressive fold" option?  If no_selects extract constants it can be quite impressive (jholt22.hql)
        //resourced.setown(foldHqlExpression(resourced));
    }
    traceExpression("AfterOptimize", resourced);
    checkNormalized(resourced);

    resourced.setown(convertSetResultToExtract(resourced));
    traceExpression("After ConvertSetResultToExtract", resourced);
    checkNormalized(resourced);

    if (true)
        resourced.setown(optimizeCompoundSource(resourced, CSFpreload|csfFlags));

    //Now resource the graph....
    unsigned numNodes = 0;
    if (options.specifiedClusterSize != 0)
        numNodes = options.specifiedClusterSize;

    traceExpression("BeforeResourcing", resourced);

    cycle_t startCycles = get_cycles_now();
    if (outputLibraryId)
    {
        unsigned numResults = outputLibrary->numResultsUsed();
        resourced.setown(resourceLibraryGraph(*this, resourced, targetClusterType, numNodes, outputLibraryId, numResults));
        resourced.setown(appendAttribute(resourced, multiInstanceAtom));  // since can be called from multiple places.
    }
    else
        resourced.setown(resourceThorGraph(*this, resourced, targetClusterType, numNodes, graphIdExpr));

    if (!resourced)
        return NULL;

    if (options.timeTransforms)
        noteFinishedTiming("compile:resource graph", startCycles);
    traceExpression("AfterResourcing", resourced);

    if (options.regressionTest)
        checkDependencyConsistency(resourced);

    checkNormalized(resourced);

    bool createGraphResults = (outputLibraryId != 0) || options.alwaysUseGraphResults;
    resourced.setown(optimizeGraphPostResource(resourced, csfFlags, options.optimizeSpillProject && !createGraphResults, false));
    if (options.optimizeSpillProject)
    {
        resourced.setown(convertSpillsToActivities(resourced, createGraphResults));
        resourced.setown(optimizeGraphPostResource(resourced, csfFlags, false, false));
    }

    checkNormalized(resourced);
    //Finally create a couple of special compound activities.
    //e.g., filtered fetch, limited keyed join
    {
        CompoundActivityTransformer transformer(targetClusterType);
        resourced.setown(transformer.transformRoot(resourced));
        traceExpression("AfterCompoundActivity", resourced);
    }

    resourced.setown(spotTableInvariant(resourced));
    traceExpression("TableInvariant", resourced);
    checkNormalized(resourced);

    return resourced.getClear();
}


void HqlCppTranslator::doBuildThorGraph(BuildCtx & ctx, IHqlExpression * expr)
{
    assertex(expr->queryType()->getTypeCode() == type_void);
    if (outputLibrary)
        buildLibraryGraph(ctx, expr, NULL);
    else
    {
        beginGraph();

        unsigned id = 0;
        OwnedHqlExpr graphTag = createAttribute(graphAtom, createUniqueId());
        OwnedHqlExpr resourced = getResourcedGraph(expr->queryChild(0), graphTag);
        if (resourced)
        {
            traceExpression("beforeGenerate", resourced);
            BuildCtx graphctx(ctx);
            graphctx.addGroup();

            Owned<SubGraphInfo> graphInfo = new SubGraphInfo(activeGraph->xgmml, 0, 0, graphTag, SubGraphRoot);
            graphctx.associate(*graphInfo);

            activeGraphCtx = &graphctx;
            buildStmt(graphctx, resourced);
            activeGraphCtx = NULL;

            graphctx.removeAssociation(graphInfo);

            HqlExprArray args;
            args.append(*createConstant(activeGraph->name));
            args.append(*createConstant(targetThor()));
            args.append(*createConstant(0));
            args.append(*createValue(no_nullptr, makeReferenceModifier(makeRowType(queryNullRecord()->getType()))));
            callProcedure(ctx, executeGraphId, args);
        }

        endGraph();
    }
}

void HqlCppTranslator::buildReturnCsvValue(BuildCtx & ctx, IHqlExpression * _expr)
{
    LinkedHqlExpr expr = _expr;
    IValue * value = expr->queryValue();
    if (value && isUnicodeType(value->queryType()))
    {
        StringBuffer temp;
        value->getUTF8Value(temp);
        expr.setown(createConstant(createStringValue(temp.str(), temp.length())));
    }
    buildReturn(ctx, expr, constUnknownVarStringType);
}

void HqlCppTranslator::buildCsvListFunc(BuildCtx & classctx, const char * func, IHqlExpression * value, const char * defaultValue)
{
    StringBuffer s;
    s.clear().append("virtual const char * ").append(func).append("(unsigned idx)");

    MemberFunction csvFunc(*this, classctx, s, MFdynamicproto);
    if (value || defaultValue)
    {
        OwnedHqlExpr idxVar = createVariable("idx", LINK(unsignedType));
        if (!value || !isEmptyList(value))
        {
            IHqlStmt * caseStmt = csvFunc.ctx.addSwitch(idxVar);
            if (value)
            {
                if (!value->isList())
                {
                    OwnedHqlExpr label = createConstant((__int64)0);
                    csvFunc.ctx.addCase(caseStmt, label);
                    buildReturnCsvValue(csvFunc.ctx, value);
                }
                else
                {
                    ForEachChild(idx, value)
                    {
                        OwnedHqlExpr label = createConstant((__int64)idx);
                        csvFunc.ctx.addCase(caseStmt, label);
                        buildReturnCsvValue(csvFunc.ctx, value->queryChild(idx));
                    }
                }
            }
            else
            {
                unsigned entry = 0;
                const char * start  = defaultValue;
                for (;;)
                {
                    const char * end = strchr(start, '|');
                    if (!end) end = start+strlen(start);
                    s.clear().append("case ").append(entry++).append(": return ");
                    appendStringAsQuotedCPP(s, end-start, start, false);
                    s.append(";");
                    csvFunc.ctx.addQuoted(s);
                    if (!*end)
                        break;
                    start = end+1;
                }
            }
            csvFunc.ctx.addDefault(caseStmt);
        }
    }
    csvFunc.ctx.addReturn(queryQuotedNullExpr());
}

static void expandDefaultString(StringBuffer & out, IHqlExpression * value, const char * defaultValue, IAtom * encoding)
{
    //If there are multiple alternatives use the first in the list as the default
    if (value && value->getOperator() == no_list)
        value = value->queryChild(0);
    if (value && value->queryValue())
    {
        if (encoding == unicodeAtom)
            getUTF8Value(out, value);
        else
            value->queryValue()->getStringValue(out);
    }
    else
        out.append(defaultValue);
}

static IHqlExpression * forceToCorrectEncoding(IHqlExpression * expr, IAtom * encoding)
{
    //This is ugly.  Really it should cast to a varutf8 type - but that isn't implemented.  So instead it
    //casts it to a utf8, type transfers it to a string, and then casts that to a varstring!
    //Reimplement if varutf8 is ever implemented.
    if (expr && (encoding == unicodeAtom))
    {
        if (expr->isList())
        {
            assertex(expr->getOperator() == no_list);
            HqlExprArray args;
            ForEachChild(i, expr)
            {
                IHqlExpression * value = expr->queryChild(i);
                args.append(*forceToCorrectEncoding(value, encoding));
            }
            return expr->clone(args);
        }
        else if (!isNumericType(expr->queryType()))
        {
            OwnedHqlExpr cast = ensureExprType(expr, unknownUtf8Type);
            OwnedHqlExpr transfer = createValue(no_typetransfer, LINK(unknownStringType), LINK(cast));
            OwnedHqlExpr recast = ensureExprType(transfer, unknownVarStringType);
            return foldHqlExpression(recast);
        }
    }
    return LINK(expr);
}

void HqlCppTranslator::buildCsvParameters(BuildCtx & subctx, IHqlExpression * csvAttr, IHqlExpression * record, bool isReading)
{
    HqlExprArray attrs;
    if (csvAttr)
        unwindChildren(attrs, csvAttr);

    BuildCtx classctx(subctx);
    StringBuffer s;
    IHqlStmt * classStmt = beginNestedClass(classctx, "csv", "ICsvParameters");

    doBuildBoolFunction(classctx, "queryEBCDIC", queryAttribute(ebcdicAtom, attrs)!=NULL);

    bool singleHeader = false;
    bool manyHeader = false;
    IHqlExpression * headerAttr = queryAttribute(headingAtom, attrs);
    IHqlExpression * terminatorAttr = queryAttribute(terminatorAtom, attrs);
    IHqlExpression * separatorAttr = queryAttribute(separatorAtom, attrs);
    IHqlExpression * escapeAttr = queryAttribute(escapeAtom, attrs);
    IHqlExpression * quoteAttr = queryAttribute(quoteAtom, attrs);
    LinkedHqlExpr terminator = terminatorAttr ? terminatorAttr->queryChild(0) : nullptr;
    LinkedHqlExpr separator = separatorAttr ? separatorAttr->queryChild(0) : nullptr;
    LinkedHqlExpr escape = escapeAttr ? escapeAttr->queryChild(0) : nullptr;
    LinkedHqlExpr quote = quoteAttr ? quoteAttr->queryChild(0) : nullptr;

    IAtom * encoding = queryCsvEncoding(csvAttr);
    if (headerAttr)
    {
        LinkedHqlExpr header = queryRealChild(headerAttr, 0);
        if (header)
        {
            header.setown(forceToCorrectEncoding(header, encoding));
            if (header->queryType()->isInteger())
            {
                classctx.addQuotedLiteral("virtual const char * getHeader() { return NULL; }");
                doBuildUnsignedFunction(classctx, "queryHeaderLen", header);
            }
            else
            {
                doBuildVarStringFunction(classctx, "getHeader", header);
                classctx.addQuotedLiteral("virtual unsigned queryHeaderLen() { return 1; }");
            }
        }
        else
        {
            StringBuffer names;
            if (!isReading)
            {
                StringBuffer comma;
                expandDefaultString(comma, separator, ",", encoding);
                expandFieldNames(queryErrorProcessor(), names, record, comma.str(), queryAttributeChild(headerAttr, formatAtom, 0));
                expandDefaultString(names, terminator, "\n", encoding);
            }
            OwnedHqlExpr namesExpr = createConstant(names.str());
            doBuildVarStringFunction(classctx, "getHeader", namesExpr);
            classctx.addQuotedLiteral("virtual unsigned queryHeaderLen() { return 1; }");
        }

        if (isReading)
        {
            manyHeader = headerAttr->hasAttribute(manyAtom) && !headerAttr->hasAttribute(singleAtom);
            singleHeader = !manyHeader;
        }
        else
        {
            if (queryRealChild(headerAttr, 1))
                doBuildVarStringFunction(classctx, "getFooter", headerAttr->queryChild(1));
            if (headerAttr->hasAttribute(singleAtom))
                singleHeader = true;
            else
                manyHeader = true;
        }
    }
    else
    {
        classctx.addQuotedLiteral("virtual const char * getHeader() { return NULL; }");
        classctx.addQuotedLiteral("virtual unsigned queryHeaderLen() { return 0; }");
    }


    doBuildSizetFunction(classctx, "queryMaxSize", getCsvMaxLength(csvAttr));

    quote.setown(forceToCorrectEncoding(quote, encoding));
    separator.setown(forceToCorrectEncoding(separator, encoding));
    terminator.setown(forceToCorrectEncoding(terminator, encoding));
    escape.setown(forceToCorrectEncoding(escape, encoding));

    buildCsvListFunc(classctx, "getQuote", quote, isReading ? "\"" : NULL);
    buildCsvListFunc(classctx, "getSeparator", separator, ",");
    buildCsvListFunc(classctx, "getTerminator", terminator, isReading ? "\r\n|\n" : "\n");
    buildCsvListFunc(classctx, "getEscape", escape, NULL);

    StringBuffer flags;
    if (!quoteAttr)                             flags.append("|defaultQuote");
    if (!separatorAttr)                         flags.append("|defaultSeparate");
    if (!terminatorAttr)                        flags.append("|defaultTerminate");
    if (!escapeAttr)                            flags.append("|defaultEscape");
    if (singleHeader)                           flags.append("|singleHeaderFooter");
    if (manyHeader)                             flags.append("|manyHeaderFooter");
    if (queryAttribute(noTrimAtom, attrs))      flags.append("|preserveWhitespace");
    if (flags.length() == 0)                    flags.append("|0");

    doBuildUnsignedFunction(classctx, "getFlags", flags.str()+1);

    endNestedClass(classStmt);

    subctx.addQuotedLiteral("virtual ICsvParameters * queryCsvParameters() { return &csv; }");
}

void HqlCppTranslator::buildCsvWriteScalar(BuildCtx & ctx, IHqlExpression * expr, IAtom * encoding)
{
    ITypeInfo * type = expr->queryType()->queryPromotedType();
    type_t tc = type->getTypeCode();
    LinkedHqlExpr value = expr;
    IIdAtom * func;
    if (type->isInteger() || tc == type_boolean)
    {
        if (type->isSigned())
            func = writeSignedId;
        else
            func = writeUnsignedId;
    }
    else if (tc == type_real)
        func = writeRealId;
    else if (tc == type_utf8)
    {
        func = writeUtf8Id;
        value.setown(createValue(no_trim, makeUtf8Type(UNKNOWN_LENGTH, NULL), LINK(value)));
    }
    else if (isUnicodeType(type))
    {
        func = writeUnicodeId;
        value.setown(createValue(no_trim, makeUnicodeType(UNKNOWN_LENGTH, NULL), LINK(value)));
    }
    else
    {
        func = writeStringId;
        value.setown(createValue(no_trim, LINK(unknownStringType), ensureExprType(value, unknownStringType)));
    }

    if (encoding == asciiAtom)
    {
        func = writeStringId;
        Owned<ITypeInfo> type = makeStringType(UNKNOWN_LENGTH, getCharset(asciiAtom), NULL);
        value.setown(ensureExprType(value, type));
    }
    else if (encoding == ebcdicAtom)
    {
        func = writeEbcdicId;
        Owned<ITypeInfo> type = makeStringType(UNKNOWN_LENGTH, getCharset(ebcdicAtom), NULL);
        value.setown(ensureExprType(value, type));
    }
    else if (encoding == unicodeAtom)
    {
        func = writeUnicodeId;
        Owned<ITypeInfo> type = makeUnicodeType(UNKNOWN_LENGTH, NULL);
        value.setown(ensureExprType(value, type));
    }

    HqlExprArray args;
    args.append(*createVariable("out", makeBoolType()));
    args.append(*LINK(value));

    buildFunctionCall(ctx, func, args);
}

void HqlCppTranslator::buildCsvWriteTransform(BuildCtx & subctx, IHqlExpression * expr, IHqlExpression * selector, IAtom * encoding)
{
    switch (expr->getOperator())
    {
    case no_field:
        {
            ITypeInfo * type = expr->queryType()->queryPromotedType();
            OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(expr));
            if (type->getTypeCode() == type_row)
            {
                buildCsvWriteTransform(subctx, expr->queryRecord(), selected, encoding);
                break;
            }

            if (expr->isDataset())
            {
                //May as well output datasets in csv in some way - a count followed by the expanded fields...
                Owned<IHqlCppDatasetCursor> cursor = createDatasetSelector(subctx, selected);

                CHqlBoundExpr boundCount;
                cursor->buildCount(subctx, boundCount);
                OwnedHqlExpr translatedCount = boundCount.getTranslatedExpr();
                buildCsvWriteScalar(subctx, translatedCount, encoding);

                BuildCtx loopctx(subctx);
                cursor->buildIterateLoop(loopctx, false);
                buildCsvWriteTransform(loopctx, expr->queryRecord(), selected, encoding);
                return;
            }
            else if (type->getTypeCode() == type_set)
            {
                BuildCtx condctx(subctx);
                Owned<IHqlCppSetCursor> cursor = createSetSelector(condctx, selected);

                CHqlBoundExpr isAll;
                cursor->buildIsAll(condctx, isAll);
                IHqlStmt * stmt = condctx.addFilter(isAll.expr);
                OwnedHqlExpr allText = createConstant("ALL");
                buildCsvWriteScalar(condctx, allText, encoding);
                condctx.selectElse(stmt);
                CHqlBoundExpr boundCurElement;
                cursor->buildIterateLoop(condctx, boundCurElement, false);
                OwnedHqlExpr curElement = boundCurElement.getTranslatedExpr();

                buildCsvWriteScalar(condctx, curElement, encoding);
            }
            else
                buildCsvWriteScalar(subctx, selected, encoding);
            break;
        }
    case no_ifblock:
        {
            OwnedHqlExpr cond = replaceSelector(expr->queryChild(0), querySelfReference(), selector);
            BuildCtx condctx(subctx);
            buildFilter(condctx, cond);
            buildCsvWriteTransform(condctx, expr->queryChild(1), selector, encoding);
            break;
        }
    case no_record:
        {
            ForEachChild(idx, expr)
                buildCsvWriteTransform(subctx, expr->queryChild(idx), selector, encoding);
            break;
        }
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        break;
    }
}

void HqlCppTranslator::buildCsvWriteTransform(BuildCtx & subctx, IHqlExpression * dataset, IAtom * encoding)
{
    MemberFunction func(*this, subctx, "void writeRow(const byte * self, ITypedOutputStream * out)");
    BoundRow * cursor = bindTableCursor(func.ctx, dataset, "self");
    buildCsvWriteTransform(func.ctx, dataset->queryRecord(), cursor->querySelector(), encoding);
}

void HqlCppTranslator::buildExpiryHelper(BuildCtx & ctx, IHqlExpression * expireAttr)
{
    if (expireAttr)
    {
        LinkedHqlExpr num = expireAttr->queryChild(0);
        if (!num)
            num.setown(getSizetConstant(options.defaultExpiry));
        doBuildUnsignedFunction(ctx, "getExpiryDays", num);
    }
}

void HqlCppTranslator::buildEncryptHelper(BuildCtx & ctx, IHqlExpression * encryptAttr, const char * funcname)
{
    if (encryptAttr)
    {
        if (!funcname) funcname = "getEncryptKey";
        doBuildDataFunction(ctx, funcname, encryptAttr->queryChild(0));
    }
}

void HqlCppTranslator::buildUpdateHelper(BuildCtx & ctx, ActivityInstance & instance, IHqlExpression * input, IHqlExpression * updateAttr)
{
    if (updateAttr)
    {
        MemberFunction func(*this, ctx, "virtual void getUpdateCRCs(unsigned & eclCrc, unsigned __int64 & totalCRC)");
        OwnedHqlExpr eclCrcVar = createVariable("eclCrc", LINK(unsignedType));
        OwnedHqlExpr totalCrcVar = createVariable("totalCRC", makeIntType(8, false));

        IHqlExpression * originalCrc = updateAttr->queryChild(0);
        DependenciesUsed dependencies(true);
        IHqlExpression * filesRead = updateAttr->queryAttribute(_files_Atom);
        if (filesRead)
        {
            ForEachChild(i, filesRead)
                dependencies.tablesRead.append(*getNormalizedFilename(filesRead->queryChild(i)));
        }
        IHqlExpression * resultsRead = updateAttr->queryAttribute(_results_Atom);
        if (resultsRead)
            unwindChildren(dependencies.resultsRead, resultsRead);

        OwnedHqlExpr crcExpr = calculatePersistInputCrc(func.ctx, dependencies);
        buildAssignToTemp(func.ctx, eclCrcVar, originalCrc);
        buildAssignToTemp(func.ctx, totalCrcVar, crcExpr);

        if (!updateAttr->hasAttribute(alwaysAtom))
            instance.addAttributeBool("_updateIfChanged", true);
    }
}

void HqlCppTranslator::buildClusterHelper(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * cluster = expr->queryAttribute(clusterAtom);
    if (!cluster)
        return;

    MemberFunction func(*this, ctx, "virtual const char * getCluster(unsigned idx)");

    BuildCtx switchctx(func.ctx);
    OwnedHqlExpr var = createVariable("idx", LINK(unsignedType));
    IHqlStmt * switchStmt = switchctx.addSwitch(var);
    unsigned count = 0;
    ForEachChild(i, cluster)
    {
        IHqlExpression * cur = queryRealChild(cluster, i);
        if (cur)
        {
            BuildCtx casectx(switchctx);
            OwnedHqlExpr label = getSizetConstant(count++);
            casectx.addCase(switchStmt, label);
            buildReturn(casectx, cur, constUnknownVarStringType);
            // I think this is wrong: it forces the arguments of the CLUSTER on OUTPUT/BUILDINDEX to by topology clusters (i.e. query destinations) instead of thor clusters (i.e. file destinations)
            //if (logger)
            //{
            //  OwnedHqlExpr folded = foldHqlExpression(cur);
            //  if (folded->queryValue())
            //  {
            //      StringBuffer clusterText;
            //      folded->queryValue()->getStringValue(clusterText);
            //      logger->noteCluster(clusterText.str());
            //  }
            //}
        }
    }
    func.ctx.addReturn(queryQuotedNullExpr());
}


void HqlCppTranslator::buildRecordEcl(BuildCtx & subctx, IHqlExpression * record, const char * methodName)
{
    StringBuffer eclFuncName;
    StringBuffer s;

    appendUniqueId(eclFuncName.append("ecl"), getConsistentUID(record));

    BuildCtx declarectx(*code, declareAtom);
    OwnedHqlExpr attr = createAttribute(eclAtom, LINK(record));
    HqlExprAssociation * match = declarectx.queryMatchExpr(attr);
    if (!match)
    {
        StringBuffer eclText;
        getRecordECL(record, eclText);

        BuildCtx funcctx(declarectx);
        funcctx.setNextPriority(EclTextPrio);
        s.append("const char * ").append(eclFuncName).append("(ICodeContext * ctx)");
        funcctx.addQuotedFunction(s, true);

        OwnedHqlExpr v = addStringLiteral(eclText);
        funcctx.addReturn(v);
        OwnedHqlExpr temp = createVariable(eclFuncName.str(), makeVoidType());
        declarectx.associateExpr(attr, temp);

        if (options.spanMultipleCpp)
        {
            s.clear().append("extern const char * ").append(eclFuncName).append("(ICodeContext * ctx);");
            BuildCtx protoctx(*code, mainprototypesAtom);
            protoctx.addQuoted(s);
        }
    }
    s.clear().append("virtual const char * ").append(methodName).append("() { return ").append(eclFuncName).append("(ctx); }");
    subctx.addQuoted(s);
}


void HqlCppTranslator::buildFormatCrcFunction(BuildCtx & ctx, const char * name, IHqlExpression * dataset, IHqlExpression * expr, unsigned payloadDelta)
{
    IHqlExpression * payload = expr ? expr->queryAttribute(_payload_Atom) : NULL;
    OwnedHqlExpr exprToCrc = getSerializedForm(dataset->queryRecord(), diskAtom);

    unsigned payloadSize = 1;
    if (payload)
        payloadSize = (unsigned)getIntValue(payload->queryChild(0)) + payloadDelta;

    //FILEPOSITION(FALSE) means we have counted 1 too many in the payload
    if (!getBoolAttribute(expr, filepositionAtom, true))
        payloadSize--;

    exprToCrc.setown(createComma(exprToCrc.getClear(), getSizetConstant(payloadSize)));

    traceExpression("crc:", exprToCrc);
    OwnedHqlExpr crc = getSizetConstant(getExpressionCRC(exprToCrc));
    doBuildUnsignedFunction(ctx, name, crc);
}

static void createOutputIndexRecord(HqlMapTransformer & mapper, HqlExprArray & fields, IHqlExpression * record, bool hasFileposition, bool allowTranslate)
{
    unsigned numFields = record->numChildren();
    unsigned max = hasFileposition ? numFields-1 : numFields;
    for (unsigned idx=0; idx < max; idx++)
    {
        IHqlExpression * cur = record->queryChild(idx);
        IHqlExpression * newField = NULL;
        switch (cur->getOperator())
        {
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            newField = LINK(cur);
            break;
        case no_ifblock:
            {
                HqlExprArray newFields;
                createOutputIndexRecord(mapper, newFields, cur->queryChild(1), false, false);
                newField = createValue(no_ifblock, makeNullType(), mapper.transformRoot(cur->queryChild(0)), createRecord(newFields));
                break;
            }
        case no_record:
            {
                HqlExprArray newFields;
                createOutputIndexRecord(mapper, newFields, cur, false, allowTranslate);
                newField = createRecord(newFields);
                break;
            }
        case no_field:
            if (cur->hasAttribute(blobAtom))
            {
                newField = createField(cur->queryId(), makeIntType(8, false), NULL, NULL);
            }
            else if (allowTranslate)
            {
                if (cur->isDatarow() && !isInPayload())
                {
                    HqlMapTransformer childMapper;
                    HqlExprArray newFields;
                    createOutputIndexRecord(childMapper, newFields, cur->queryRecord(), false, allowTranslate);
                    OwnedHqlExpr newRecord = createRecord(newFields);
                    HqlExprArray args;
                    unwindChildren(args, cur);
                    newField = createField(cur->queryId(), newRecord->getType(), args);
                }
                else
                {
                    OwnedHqlExpr select = createSelectExpr(getActiveTableSelector(), LINK(cur));
                    OwnedHqlExpr value = getHozedKeyValue(select);
                    ITypeInfo * newType = value->getType();
                    newField = createField(cur->queryId(), newType, NULL, extractFieldAttrs(cur));

                    //Now set up the mappings for ifblocks
                    OwnedHqlExpr selfSelect = createSelectExpr(LINK(querySelfReference()), LINK(cur));
                    OwnedHqlExpr physicalSelect = createSelectExpr(LINK(querySelfReference()), LINK(newField));
                    OwnedHqlExpr keyValue = convertIndexPhysical2LogicalValue(cur, physicalSelect, true);
                    mapper.setMapping(selfSelect, keyValue);
                }
            }
            else
                newField = LINK(cur);
            break;
        }

        fields.append(*newField);
    }
}


static void createOutputIndexTransform(HqlExprArray & assigns, IHqlExpression * self, IHqlExpression * tgtRecord, IHqlExpression * srcRecord, IHqlExpression* srcDataset, bool hasFileposition, bool allowTranslate)
{
    unsigned numFields = srcRecord->numChildren();
    unsigned max = hasFileposition ? numFields-1 : numFields;
    for (unsigned idx=0; idx < max; idx++)
    {
        IHqlExpression * cur = srcRecord->queryChild(idx);
        IHqlExpression * curNew = tgtRecord->queryChild(idx);

        switch (cur->getOperator())
        {
        case no_ifblock:
            createOutputIndexTransform(assigns, self, curNew->queryChild(1), cur->queryChild(1), srcDataset, false, false);
            break;
        case no_record:
            createOutputIndexTransform(assigns, self, curNew, cur, srcDataset, false, allowTranslate);
            break;
        case no_field:
            {
                OwnedHqlExpr select = createSelectExpr(LINK(srcDataset), LINK(cur));
                OwnedHqlExpr value;

                if (cur->hasAttribute(blobAtom))
                {
                    value.setown(createValue(no_blob2id, curNew->getType(), LINK(select)));
                }
                else if (cur->isDatarow() && !isInPayload())
                {
                    OwnedHqlExpr childSelf = createSelectExpr(LINK(self), LINK(curNew));
                    createOutputIndexTransform(assigns, childSelf, curNew->queryRecord(), cur->queryRecord(), select, false, allowTranslate);
                }
                else
                {
                    if (allowTranslate)
                        value.setown(getHozedKeyValue(select));
                    else
                        value.set(select);
                }

                if (value)
                    assigns.append(*createAssign(createSelectExpr(LINK(self), LINK(curNew)), LINK(value)));
                break;
            }
        }
    }
}


void HqlCppTranslator::doBuildIndexOutputTransform(BuildCtx & ctx, IHqlExpression * record, SharedHqlExpr & rawRecord, bool hasFileposition, IHqlExpression * maxlength)
{
    OwnedHqlExpr srcDataset = createDataset(no_anon, LINK(record));

    HqlExprArray fields;
    HqlExprArray assigns;
    HqlMapTransformer mapper;
    createOutputIndexRecord(mapper, fields, record, hasFileposition, true);

    OwnedHqlExpr newRecord = createRecord(fields);
    rawRecord.set(newRecord);
    OwnedHqlExpr self = getSelf(newRecord);
    createOutputIndexTransform(assigns, self, newRecord, record, srcDataset, hasFileposition, true);

    OwnedHqlExpr tgtDataset = createDataset(no_anon, newRecord.getLink());

    {
        OwnedHqlExpr transform = createValue(no_newtransform, makeTransformType(newRecord->getType()), assigns);
        MemberFunction func(*this, ctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left, IBlobCreator * blobs, unsigned __int64 & filepos)");
        ensureRowAllocated(func.ctx, "crSelf");
        func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
        associateBlobHelper(func.ctx, srcDataset, "blobs");

        BoundRow * selfCursor = bindSelf(func.ctx, tgtDataset, "crSelf");
        bindTableCursor(func.ctx, srcDataset, "left");

        associateSkipReturnMarker(func.ctx, queryZero(), selfCursor);
        doTransform(func.ctx, transform, selfCursor);

        OwnedHqlExpr fposVar = createVariable("filepos", makeIntType(8, false));
        OwnedHqlExpr fposValue;
        if (hasFileposition)
            fposValue.setown(createSelectExpr(LINK(srcDataset), LINK(queryLastField(record))));
        else
            fposValue.setown(getSizetConstant(0));
        buildAssignToTemp(func.ctx, fposVar, fposValue);

        buildReturnRecordSize(func.ctx, selfCursor);
    }

    buildMetaMember(ctx, tgtDataset, false, "queryDiskRecordSize");

    size32_t maxRecordSize = 32767;
    if (isVariableSizeRecord(newRecord))
    {
        if (maxlength)
        {
            maxRecordSize = (size32_t)getIntValue(maxlength->queryChild(0), 0);
            if (maxRecordSize == 0)
                maxRecordSize = getMaxRecordSize(newRecord);
        }
    }
    else
        maxRecordSize = getMinRecordSize(newRecord);

    doBuildUnsignedFunction(ctx, "getMaxKeySize", maxRecordSize);
    if (maxRecordSize > KEYBUILD_MAXLENGTH)
        throwError2(HQLERR_MaxlengthExceedsLimit, maxRecordSize, KEYBUILD_MAXLENGTH);
}


class TranslatorMaxSizeCallback : public IMaxSizeCallback
{
public:
    enum { sizeofFposField = 8 };

    TranslatorMaxSizeCallback(HqlCppTranslator & _translator) : translator(_translator) {}

    virtual size32_t getMaxSize(IHqlExpression * record)
    {
        bool isKnownSize, usedDefaultMaxSize;
        size32_t maxSize = getMaxRecordSize(record, translator.getDefaultMaxRecordSize(), isKnownSize, usedDefaultMaxSize);
        if (usedDefaultMaxSize)
            maxSize += sizeofFposField;                 //adjust maximum size for the fileposition, so consistent with raw record size
        return maxSize;
    }

protected:
    HqlCppTranslator & translator;
};


IDefRecordElement * HqlCppTranslator::createMetaRecord(IHqlExpression * record)
{
    TranslatorMaxSizeCallback callback(*this);
    return ::createMetaRecord(record, &callback);
}


IHqlExpression * HqlCppTranslator::getSerializedLayoutFunction(IHqlExpression * record, unsigned numKeyedFields)
{
    OwnedHqlExpr serializedRecord = getSerializedForm(record, diskAtom);
    OwnedHqlExpr search = createAttribute(indexLayoutMarkerAtom, LINK(serializedRecord), getSizetConstant(numKeyedFields));

    BuildCtx declarectx(*code, declareAtom);
    HqlExprAssociation * match = declarectx.queryMatchExpr(search);
    if (match)
        return LINK(match->queryExpr());

    //Modify the record, so that blob fields are tagged as represented as they are in the index
    OwnedHqlExpr indexRecord = annotateIndexBlobs(serializedRecord);
    Owned<IDefRecordElement> re = createMetaRecord(indexRecord);
    if (!re)
        return NULL;

    StringBuffer functionName;
    getUniqueId(functionName.append("getLayout"));

    BuildCtx layoutctx(declarectx);

    StringBuffer s;
    s.append("void ").append(functionName).append("(size32_t & __lenResult, void * & __result, IResourceContext * ctx)");
    layoutctx.setNextPriority(RowMetaPrio);
    layoutctx.addQuotedFunction(s, true);

    Owned<IDefRecordMeta> meta = createDefRecordMeta(re, numKeyedFields);
    MemoryBuffer serialized;
    serializeRecordMeta(serialized, meta, true);
    OwnedHqlExpr metaExpr = createConstant(createDataValue(serialized.toByteArray(), serialized.length()));

    Owned<ITypeInfo> type = makeDataType(UNKNOWN_LENGTH);
    doBuildFunctionReturn(layoutctx, type, metaExpr);

    if (options.showMetaText)
    {
        s.clear().append("/*").newline();
        getRecordMetaAsString(s, meta);
        s.append("*/");
        layoutctx.addQuoted(s.str());
    }

    if (options.spanMultipleCpp)
    {
        s.clear().append("extern void ").append(functionName).append("(size32_t & __lenResult, void * & __result, IResourceContext * ctx);");
        BuildCtx protoctx(*code, mainprototypesAtom);
        protoctx.addQuoted(s);
    }

    OwnedHqlExpr temp = createVariable(functionName, makeVoidType());
    declarectx.associateExpr(search, temp);
    return temp.getLink();

}


void HqlCppTranslator::buildSerializedLayoutMember(BuildCtx & ctx, IHqlExpression * record, const char * name, unsigned numKeyedFields)
{
    OwnedHqlExpr func = getSerializedLayoutFunction(record, numKeyedFields);
    if (func)
    {
        StringBuffer s;
        s.append("virtual bool ").append(name).append("(size32_t & __lenResult, void * & __result) { ");
        generateExprCpp(s, func).append("(__lenResult, __result, ctx); return true; }");

        ctx.addQuoted(s);
    }
}



ABoundActivity * HqlCppTranslator::doBuildActivityOutputIndex(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    ensureDiskAccessAllowed(expr);

    IHqlExpression * dataset  = expr->queryChild(0);
    IHqlExpression * filename = queryRealChild(expr, 1);
    IHqlExpression * record = dataset->queryRecord();

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKindexwrite, expr, "IndexWrite");
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    //virtual const char * getFileName() { return "x.d00"; }
    buildFilenameFunction(*instance, instance->startctx, "getFileName", filename, hasDynamicFilename(expr));

    //virtual unsigned getFlags() = 0;
    IHqlExpression * updateAttr = expr->queryAttribute(updateAtom);
    IHqlExpression * compressAttr = expr->queryAttribute(compressedAtom);
    IHqlExpression * widthExpr = queryAttributeChild(expr, widthAtom, 0);
    bool hasTLK = !expr->hasAttribute(noRootAtom);
    bool hasFileposition = getBoolAttribute(expr, filepositionAtom, true);
    bool singlePart = expr->hasAttribute(fewAtom);
    if (matchesConstantValue(widthExpr, 1))
    {
        singlePart = true;
        widthExpr = NULL;
    }

    StringBuffer s;
    StringBuffer flags;
    if (expr->hasAttribute(overwriteAtom)) flags.append("|TIWoverwrite");
    if (expr->hasAttribute(noOverwriteAtom)) flags.append("|TIWnooverwrite");
    if (expr->hasAttribute(backupAtom))    flags.append("|TIWbackup");
    if (!filename->isConstant())          flags.append("|TIWvarfilename");
    if (singlePart)                       flags.append("|TIWsmall");
    if (updateAttr)                       flags.append("|TIWupdatecrc");
    if (updateAttr && !updateAttr->queryAttribute(alwaysAtom)) flags.append("|TIWupdate");
    if (!hasTLK && !singlePart)           flags.append("|TIWlocal");
    if (expr->hasAttribute(expireAtom))   flags.append("|TIWexpires");
    if (expr->hasAttribute(maxLengthAtom))   flags.append("|TIWmaxlength");

    if (compressAttr)
    {
        if (compressAttr->hasAttribute(rowAtom))   flags.append("|TIWrowcompress");
        if (!compressAttr->hasAttribute(lzwAtom))  flags.append("|TIWnolzwcompress");
    }
    if (widthExpr) flags.append("|TIWhaswidth");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    IHqlExpression * indexNameAttr = expr->queryAttribute(indexAtom);
    if (indexNameAttr)
        buildFilenameFunction(*instance, instance->startctx, "getDistributeIndexName", indexNameAttr->queryChild(0), hasDynamicFilename(expr));

    buildExpiryHelper(instance->createctx, expr->queryAttribute(expireAtom));
    buildUpdateHelper(instance->createctx, *instance, dataset, updateAttr);
    buildClusterHelper(instance->classctx, expr);

    LinkedHqlExpr serializedRecord = record;

    // virtual unsigned getKeyedSize()
    HqlExprArray fields;
    unwindChildren(fields, record);
    removeAttributes(fields);
    fields.popn(numPayloadFields(expr));
    OwnedHqlExpr keyedRecord = createRecord(fields); // must be fixed length => no maxlength
    if (expr->hasAttribute(_payload_Atom))
    {
        instance->classctx.addQuoted(s.clear().append("virtual unsigned getKeyedSize() { return ").append(getFixedRecordSize(keyedRecord)).append("; }"));
        serializedRecord.setown(notePayloadFields(serializedRecord, numPayloadFields(expr)));
    }
    else
        instance->classctx.addQuoted(s.clear().append("virtual unsigned getKeyedSize() { return (unsigned) -1; }"));

    //virtual const char * queryRecordECL() = 0;
    serializedRecord.setown(getSerializedForm(serializedRecord, diskAtom));
    buildRecordEcl(instance->createctx, serializedRecord, "queryRecordECL");

    doBuildSequenceFunc(instance->classctx, querySequence(expr), false);
    HqlExprArray xmlnsAttrs;
    gatherAttributes(xmlnsAttrs, xmlnsAtom, expr);
    Owned<IWUResult> result = createDatasetResultSchema(querySequence(expr), queryResultName(expr), dataset->queryRecord(), xmlnsAttrs, false, true, fields.ordinality());

    if (expr->hasAttribute(setAtom))
    {
        MemberFunction func(*this, instance->startctx, "virtual bool getIndexMeta(size32_t & lenName, char * & name, size32_t & lenValue, char * & value, unsigned idx)");

        CHqlBoundTarget nameTarget, valueTarget;
        initBoundStringTarget(nameTarget, unknownStringType, "lenName", "name");
        //more should probably be utf-8 rather than string
        initBoundStringTarget(valueTarget, unknownStringType, "lenValue", "value");

        OwnedHqlExpr idxVar = createVariable("idx", LINK(sizetType));
        BuildCtx casectx(func.ctx);
        IHqlStmt * switchStmt = casectx.addSwitch(idxVar);
        unsigned count = 0;
        ForEachChild(i, expr)
        {
            IHqlExpression * cur = expr->queryChild(i);
            if (cur->isAttribute() && cur->queryName() == setAtom)
            {
                OwnedHqlExpr label = getSizetConstant(count++);
                casectx.addCase(switchStmt, label);
                buildExprAssign(casectx, nameTarget, cur->queryChild(0));
                buildExprAssign(casectx, valueTarget, cur->queryChild(1));
                buildReturn(casectx, queryBoolExpr(true));
            }
        }
        buildReturn(func.ctx, queryBoolExpr(false));
    }

    OwnedHqlExpr rawRecord;
    doBuildIndexOutputTransform(instance->startctx, record, rawRecord, hasFileposition, expr->queryAttribute(maxLengthAtom));
    buildFormatCrcFunction(instance->classctx, "getFormatCrc", rawRecord, expr, 0);

    if (compressAttr && compressAttr->hasAttribute(rowAtom))
    {
        if (!isFixedWidthDataset(rawRecord))
            throwError(HQLERR_RowCompressRequireFixedSize);
    }
    if (!expr->hasAttribute(fixedAtom))
        buildSerializedLayoutMember(instance->classctx, record, "getIndexLayout", fields.ordinality());

    if (widthExpr)
    {
        doBuildUnsignedFunction(instance->startctx, "getWidth", widthExpr);

        if (!hasTLK)
        {
            HqlExprArray sorts;
            gatherIndexBuildSortOrder(sorts, expr, options.sortIndexPayload);
            OwnedHqlExpr sortOrder = createValueSafe(no_sortlist, makeSortListType(NULL), sorts);
            buildCompareFuncHelper(*this, *instance, "compare", sortOrder, DatasetReference(dataset));
        }
    }
    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    instance->addSignedAttribute(expr->queryAttribute(_signed_Atom));

    OwnedHqlExpr dependency = createAttribute(fileAtom, getNormalizedFilename(filename));
    Owned<ABoundActivity> bound = instance->getBoundActivity();
    OwnedHqlExpr boundUnknown = createUnknown(no_attr, NULL, NULL, LINK(bound));
    ctx.associateExpr(dependency, boundUnknown);
    return instance->getBoundActivity();
}

void HqlCppTranslator::buildCsvWriteMembers(ActivityInstance * instance, IHqlExpression * dataset, IHqlExpression * csvAttr)
{
    buildCsvParameters(instance->nestedctx, csvAttr, dataset->queryRecord(), false);
    buildCsvWriteTransform(instance->startctx, dataset, queryCsvEncoding(csvAttr));
}


void HqlCppTranslator::buildXmlWriteMembers(ActivityInstance * instance, IHqlExpression * dataset, IHqlExpression * xmlAttr)
{
    buildXmlSerialize(instance->startctx, dataset, "toXML", false);

    IHqlExpression * rowAttr = xmlAttr->queryAttribute(rowAtom);
    if (rowAttr)
        doBuildVarStringFunction(instance->startctx, "getXmlIteratorPath", rowAttr->queryChild(0));
    IHqlExpression * headerAttr = xmlAttr->queryAttribute(headingAtom);
    if (headerAttr)
    {
        doBuildVarStringFunction(instance->startctx, "getHeader", headerAttr->queryChild(0));
        doBuildVarStringFunction(instance->startctx, "getFooter", headerAttr->queryChild(1));
    }
    StringBuffer xmlFlags;
    if (xmlAttr->hasAttribute(trimAtom))
        xmlFlags.append("|XWFtrim");
    if (xmlAttr->hasAttribute(optAtom))
        xmlFlags.append("|XWFopt");
    if (xmlFlags.length())
    {
        StringBuffer s;
        s.append("virtual unsigned getXmlFlags() { return ").append(xmlFlags.str()+1).append("; }");
        instance->classctx.addQuoted(s);
    }
}

ABoundActivity * HqlCppTranslator::doBuildActivityOutput(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IHqlExpression * dataset  = expr->queryChild(0);
    IHqlExpression * rawFilename = queryRealChild(expr, 1);

    if (dataset->isDictionary())
    {
        //OUTPUT(dictionary,,'filename') should never be generated - it should go via a dataset
        assertex(!rawFilename);
        return doBuildActivityDictionaryWorkunitWrite(ctx, expr, isRoot);
    }
    if (!rawFilename)
        return doBuildActivityOutputWorkunit(ctx, expr, isRoot);

    OwnedHqlExpr filename = foldHqlExpression(rawFilename);
    IHqlExpression * program  = queryRealChild(expr, 2);
    IHqlExpression * csvAttr = expr->queryAttribute(csvAtom);
    bool isJson = false;
    IHqlExpression * xmlAttr = expr->queryAttribute(xmlAtom);
    if (!xmlAttr)
    {
        xmlAttr = expr->queryAttribute(jsonAtom);
        if (xmlAttr)
            isJson=true;
    }
    LinkedHqlExpr expireAttr = expr->queryAttribute(expireAtom);
    IHqlExpression * seq = querySequence(expr);

    IHqlExpression *pipe = NULL;
    if (program)
    {
        if (program->getOperator()==no_pipe)
            pipe = program->queryChild(0);
    }
    else if (filename->getOperator()==no_pipe)
        pipe = filename->queryChild(0);

    if (pipe && expr->hasAttribute(_disallowed_Atom))
        throwError(HQLERR_PipeNotAllowed);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    ThorActivityKind kind = TAKdiskwrite;
    const char * activityArgName = "DiskWrite";
    if (expr->getOperator() == no_spill)
    {
        kind = TAKspill;
        activityArgName = "Spill";
    }
    else if (pipe)
    {
        kind = TAKpipewrite;
        activityArgName = "PipeWrite";
    }
    else if (csvAttr)
    {
        kind = TAKcsvwrite;
        activityArgName = "CsvWrite";
    }
    else if (xmlAttr)
    {
        kind = (isJson) ? TAKjsonwrite : TAKxmlwrite;
        activityArgName = "XmlWrite";
    }

    bool useImplementationClass = options.minimizeActivityClasses && targetRoxie() && expr->hasAttribute(_spill_Atom);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, activityArgName);
    //Output to a variable filename is either a user result, or a computed workflow spill, both need evaluating.

    if (useImplementationClass)
        instance->setImplementationClass(newMemorySpillSplitArgId);

    if ((kind == TAKdiskwrite) && filename->queryValue())
    {
        StringBuffer s;
        s.append(getActivityText(kind));
        if (expr->hasAttribute(_spill_Atom))
            s.append("\nSpill File");
        else
            filename->toString(s.append("\n"));
        instance->graphLabel.set(s.str());
    }

    if (pipe)
    {
        if (csvAttr)
            instance->addBaseClass("IHThorCsvWriteExtra", true);
        else if (xmlAttr)
            instance->addBaseClass("IHThorXmlWriteExtra", true);
    }

    buildActivityFramework(instance, isRoot && !isInternalSeq(seq));

    buildInstancePrefix(instance);

    noteResultDefined(ctx, instance, seq, filename, isRoot);

    //virtual const char * getFileName() { return "x.d00"; }

    OwnedHqlExpr tempCount;
    if (expr->hasAttribute(_spill_Atom) || expr->hasAttribute(jobTempAtom))
    {
        IPropertyTree * graphNode = NULL;
        if (targetRoxie() && expr->hasAttribute(jobTempAtom))
            graphNode = instance->graphNode;

        GlobalFileTracker * tracker = new GlobalFileTracker(filename, graphNode);
        globalFiles.append(*tracker);
        OwnedHqlExpr callback = createUnknown(no_callback, LINK(unsignedType), globalAtom, LINK(tracker));
        tempCount.setown(createTranslated(callback));
    }

    if (!useImplementationClass)
    {
        if (pipe)
        {
            //MORE or pipe name is dependent on the input dataset - !constant is not sufficient
            if (expr->hasAttribute(repeatAtom))
            {
                //virtual const char * getPipeProgram() { return "grep"; }
                instance->startctx.addQuotedLiteral("virtual const char * getPipeProgram() { return NULL; }");

                MemberFunction func(*this, instance->startctx, "virtual char * getNameFromRow(const void * _self)");
                func.ctx.addQuotedLiteral("const unsigned char * self = (const unsigned char *) _self;");
                bindTableCursor(func.ctx, dataset, "self");
                buildReturn(func.ctx, pipe, unknownVarStringType);
            }
            else
            {
                //virtual const char * getPipeProgram() { return "grep"; }
                MemberFunction func(*this, instance->startctx, "virtual const char * getPipeProgram()");
                buildReturn(func.ctx, pipe, unknownVarStringType);
            }

            if (csvAttr)
                instance->classctx.addQuotedLiteral("virtual IHThorCsvWriteExtra * queryCsvOutput() { return this; }");
            if (xmlAttr)
                instance->classctx.addQuotedLiteral("virtual IHThorXmlWriteExtra * queryXmlOutput() { return this; }");

            StringBuffer flags;
            if (expr->hasAttribute(repeatAtom))
                flags.append("|TPFrecreateeachrow");
            if (expr->hasAttribute(optAtom))
                flags.append("|TPFnofail");
            if (csvAttr)
                flags.append("|TPFwritecsvtopipe");
            if (xmlAttr)
                flags.append("|TPFwritexmltopipe");

            if (flags.length())
                doBuildUnsignedFunction(instance->classctx, "getPipeFlags", flags.str()+1);
        }
        else
        {
            bool constFilename = true;
            //virtual const char * getFileName() = 0;
            if (filename && filename->getOperator() != no_pipe)
            {
                bool isDynamic = expr->hasAttribute(resultAtom) || hasDynamicFilename(expr);
                buildFilenameFunction(*instance, instance->startctx, "getFileName", filename, isDynamic);
                if (!filename->isConstant())
                    constFilename = false;
            }
            else
            {
                MemberFunction func(*this, instance->startctx, "virtual const char * getFileName()");
                func.ctx.addReturn(queryQuotedNullExpr());
            }

            //Expire if explicit, or if a persisted output, and persists default to expiring
            bool expires = expireAttr || (expr->hasAttribute(_workflowPersist_Atom) && options.expirePersists);
            if (expires && !expireAttr && options.defaultPersistExpiry)
                expireAttr.setown(createExprAttribute(expireAtom, getSizetConstant(options.defaultPersistExpiry)));

            //virtual unsigned getFlags() = 0;
            IHqlExpression * updateAttr = expr->queryAttribute(updateAtom);
            StringBuffer s;
            StringBuffer flags;
            if (expr->hasAttribute(_spill_Atom)) flags.append("|TDXtemporary");
            if (expr->hasAttribute(groupedAtom)) flags.append("|TDXgrouped");
            if (expr->hasAttribute(compressedAtom)) flags.append("|TDWnewcompress");
            if (expr->hasAttribute(__compressed__Atom)) flags.append("|TDXcompress");
            if (expr->hasAttribute(extendAtom)) flags.append("|TDWextend");
            if (expr->hasAttribute(overwriteAtom)) flags.append("|TDWoverwrite");
            if (expr->hasAttribute(noOverwriteAtom)) flags.append("|TDWnooverwrite");
            if (expr->hasAttribute(_workflowPersist_Atom)) flags.append("|TDWpersist");
            if (expr->hasAttribute(_noReplicate_Atom)) flags.append("|TDWnoreplicate");
            if (expr->hasAttribute(backupAtom)) flags.append("|TDWbackup");
            if (expr->hasAttribute(resultAtom)) flags.append("|TDWowned|TDWresult");
            if (expr->hasAttribute(ownedAtom)) flags.append("|TDWowned");
            if (!constFilename) flags.append("|TDXvarfilename");
            if (hasDynamicFilename(expr)) flags.append("|TDXdynamicfilename");
            if (expr->hasAttribute(jobTempAtom)) flags.append("|TDXjobtemp");
            if (updateAttr) flags.append("|TDWupdatecrc");
            if (updateAttr && !updateAttr->queryAttribute(alwaysAtom)) flags.append("|TDWupdate");
            if (expires) flags.append("|TDWexpires");

            if (flags.length())
                doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

            //virtual const char * queryRecordECL() = 0;
            //Ensure the ECL for the record reflects its serialized form, not the internal form
            OwnedHqlExpr record = getSerializedForm(dataset->queryRecord(), diskAtom);
            if (expr->hasAttribute(noXpathAtom))
                record.setown(removeAttributeFromFields(record, xpathAtom));
            buildRecordEcl(instance->createctx, record, "queryRecordECL");

            buildExpiryHelper(instance->createctx, expireAttr);
            buildUpdateHelper(instance->createctx, *instance, dataset, updateAttr);
        }

        doBuildSequenceFunc(instance->classctx, seq, true);
        if (tempCount)
        {
            if ((kind != TAKspill) || !matchesConstantValue(tempCount, 1))
            {
                MemberFunction func(*this, instance->classctx, "virtual unsigned getTempUsageCount()");
                buildReturn(func.ctx, tempCount, unsignedType);
            }
        }

        IHqlExpression * outputRecord = instance->meta.queryRecord();
        OwnedHqlExpr outputDs = createDataset(no_null, LINK(outputRecord));
        HqlExprArray xmlnsAttrs;
        gatherAttributes(xmlnsAttrs, xmlnsAtom, expr);
        bool createTransformer = (kind != TAKcsvwrite) && (kind != TAKxmlwrite) && (kind != TAKjsonwrite);
        Owned<IWUResult> result = createDatasetResultSchema(seq, queryResultName(expr), outputRecord, xmlnsAttrs, createTransformer, true, 0);
        if (expr->hasAttribute(resultAtom))
            result->setResultRowLimit(-1);

        //Remove virtual attributes from the record, so the crc will be compatible with the disk read record
        OwnedHqlExpr noVirtualRecord = removeVirtualAttributes(dataset->queryRecord());
        buildFormatCrcFunction(instance->classctx, "getFormatCrc", noVirtualRecord, NULL, 0);

        bool grouped = isGrouped(dataset);
        bool ignoreGrouped = !expr->hasAttribute(groupedAtom);
        if ((kind != TAKspill) || (dataset->queryType() != expr->queryType()) || (grouped && ignoreGrouped))
            buildMetaMember(instance->classctx, dataset, grouped && !ignoreGrouped, "queryDiskRecordSize");
        buildClusterHelper(instance->classctx, expr);

        //Both csv write and pipe with csv/xml format
        if (csvAttr)
            buildCsvWriteMembers(instance, outputDs, csvAttr);
        if (xmlAttr)
            buildXmlWriteMembers(instance, outputDs, xmlAttr);

        buildEncryptHelper(instance->startctx, expr->queryAttribute(encryptAtom));
    }
    else
    {
        assertex(tempCount.get() && !hasDynamic(expr));
        instance->addConstructorParameter(tempCount);
        addFilenameConstructorParameter(*instance, "getFileName", filename);
    }

    instance->addSignedAttribute(expr->queryAttribute(_signed_Atom));

    instance->addAttributeBool("_isSpill", expr->hasAttribute(_spill_Atom));
    if (targetRoxie())
        instance->addAttributeBool("_isSpillGlobal", expr->hasAttribute(jobTempAtom));

    buildInstanceSuffix(instance);
    if (boundDataset)
    {
        buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    }

    OwnedHqlExpr dependency = createAttribute(fileAtom, getNormalizedFilename(filename));
    Owned<ABoundActivity> bound = instance->getBoundActivity();
    OwnedHqlExpr boundUnknown = createUnknown(no_attr, NULL, NULL, LINK(bound));
    activeGraphCtx->associateExpr(dependency, boundUnknown);

    IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
    if (name)
        associateRemoteResult(*instance, seq, name);

    return instance->getBoundActivity();
}

void HqlCppTranslator::addSchemaField(IHqlExpression *field, MemoryBuffer &schema, IHqlExpression *selector)
{
    IAtom * name = field->queryName();
    StringBuffer schemaName;
    if (name)
    {
        schemaName.append(str(name));
    }
    else
    {
        schemaName.append("unknown_name");
        getUniqueId(schemaName);
    }
    schemaName.toLowerCase();

    StringBuffer funcname;
    Linked<ITypeInfo> schemaType = queryUnqualifiedType(field->queryType());

    switch(schemaType->getTypeCode())
    {
    case type_alien:
    case type_enumerated:
        schemaType.set(schemaType->queryChildType());
        break;
    case type_bitfield:
        schemaType.set(schemaType->queryPromotedType());
        //fall through;
    case type_dictionary:
    case type_table:
    case type_groupedtable:
    case type_row:
        schemaType.setown(makeStringType(UNKNOWN_LENGTH, NULL, NULL));
        break;
    }
    schema.append(schemaName.str());
    schemaType->serialize(schema);
}

void HqlCppTranslator::doAddSchemaFields(IHqlExpression * record, MemoryBuffer &schema, IHqlExpression *selector)
{
    assertex(record->getOperator() == no_record);
    ForEachChild(idx, record)
    {
        IHqlExpression *field = record->queryChild(idx);
        switch (field->getOperator())
        {
        case no_ifblock:
            doAddSchemaFields(field->queryChild(1), schema, selector);
            break;
        case no_record:
            doAddSchemaFields(field, schema, selector);
            break;
        case no_field:
            if (field->isDatarow())
            {
                // MORE - should I record this nesting in the schema?
                // MORE - this does not yet work...
                OwnedHqlExpr subselector = createSelectExpr(LINK(selector), LINK(field));
                doAddSchemaFields(field->queryRecord(), schema, subselector);
            }
            else
                addSchemaField(field, schema, selector);
            break;
        }
    }
}


void HqlCppTranslator::getRecordECL(IHqlExpression * deserializedRecord, StringBuffer & eclText)
{
    OwnedHqlExpr record = getSerializedForm(deserializedRecord, diskAtom);
    if ((options.maxRecordSize != MAX_RECORD_SIZE) && maxRecordSizeUsesDefault(record))
    {
        //Add an explicit record size if default max record size
        size32_t maxSize = getMaxRecordSize(record);
        HqlExprArray args;
        unwindChildren(args, record);
        args.append(*createExprAttribute(maxLengthAtom, getSizetConstant(maxSize)));
        OwnedHqlExpr annotatedRecord = record->clone(args);
        ::getRecordECL(annotatedRecord, eclText);
    }
    else
        ::getRecordECL(record, eclText);
}

void HqlCppTranslator::addSchemaFields(IHqlExpression * record, MemoryBuffer &schema, IHqlExpression *selector)
{
    doAddSchemaFields(record, schema, selector);
    schema.append("").append((unsigned char) type_void);

    StringBuffer eclText;
    getRecordECL(record, eclText);

    schema.append((unsigned)eclText.length());
    schema.append(eclText.length(), eclText.str()); // could compress at this point...
}


void HqlCppTranslator::addSchemaResource(int seq, const char * name, IHqlExpression * record, unsigned keyedCount)
{
    StringBuffer xml;
    getRecordXmlSchema(xml, record, true, keyedCount);
    addSchemaResource(seq, name, xml.length()+1, xml.str());
}

void HqlCppTranslator::addSchemaResource(int seq, const char * name, unsigned len, const char * schemaXml)
{
    Owned<IPropertyTree> manifestEntry = createPTree("Resource");
    manifestEntry->setProp("@name", name);
    manifestEntry->setPropInt("@seq", seq);
    code->addCompressResource("RESULT_XSD", len, schemaXml, manifestEntry);
}


void HqlCppTranslator::finalizeResources()
{
}

IWUResult * HqlCppTranslator::createDatasetResultSchema(IHqlExpression * sequenceExpr, IHqlExpression * name, IHqlExpression * record, HqlExprArray &xmlnsAttrs, bool createTransformer, bool isFile, unsigned keyedCount)
{
    //Some spills have no sequence attached
    if (!sequenceExpr)
        return NULL;

    int sequence = (int)getIntValue(sequenceExpr);
    Owned<IWUResult> result = createWorkunitResult(sequence, name);
    if (!result)
        return NULL;

    addDatasetResultXmlNamespaces(*result, xmlnsAttrs, record);

    MemoryBuffer schema;
    OwnedHqlExpr self = getSelf(record);
    addSchemaFields(record, schema, self);

    SCMStringBuffer resultName;
    result->getResultName(resultName);
    addSchemaResource(sequence, resultName.str(), record, keyedCount);

    result->setResultSchemaRaw(schema.length(), schema.toByteArray());
    result->setResultScalar(false);

    OwnedHqlExpr serialRecord = getSerializedForm(record, diskAtom);
    OwnedHqlExpr ds = createDataset(no_anon, LINK(serialRecord));
    MetaInstance meta(*this, serialRecord, false);
    buildMetaInfo(meta);
    result->setResultRecordSizeEntry(meta.metaFactoryName);

    if (createTransformer)
    {
        OwnedHqlExpr noVirtualRecord = removeVirtualAttributes(serialRecord);
        Owned<IHqlExpression> transformedRecord = getFileViewerRecord(noVirtualRecord, false);
        if (transformedRecord)
        {
            OwnedHqlExpr ds = createDataset(no_anon, LINK(noVirtualRecord));
            OwnedHqlExpr seq = createDummySelectorSequence();
            OwnedHqlExpr leftSelect = createSelector(no_left, ds, seq);
            OwnedHqlExpr transform = getSimplifiedTransform(transformedRecord, noVirtualRecord, leftSelect);
            OwnedHqlExpr tds = createDataset(no_anon, LINK(transformedRecord));
            StringBuffer s, name;
            getUniqueId(name.append("tf"));

            BuildCtx transformctx(*code, declareAtom);
            transformctx.setNextPriority(RecordTranslatorPrio);
            s.clear().append("extern \"C\" ECL_API size32_t ").append(name).append("(ARowBuilder & crSelf, const byte * src)");

            MemberFunction tranformFunc(*this, transformctx, s, MFdynamicproto);

            BoundRow * selfCursor = bindSelf(tranformFunc.ctx, tds, "crSelf");
            bindTableCursor(tranformFunc.ctx, ds, "src", no_left, seq);
            associateSkipReturnMarker(tranformFunc.ctx, queryZero(), selfCursor);
            ensureRowAllocated(tranformFunc.ctx, "crSelf");

            doTransform(tranformFunc.ctx, transform, selfCursor);
            buildReturnRecordSize(tranformFunc.ctx, selfCursor);

            result->setResultTransformerEntry(name.str());
        }
    }

    return result.getClear();
}

//-------------------------------------------------------------------------------------------------------------------

void HqlCppTranslator::buildXmlSerializeSetValues(BuildCtx & ctx, IHqlExpression * value, IHqlExpression * itemName, bool includeAll)
{
    OwnedHqlExpr simpleValue = simplifyFixedLengthList(value);
    BuildCtx subctx(ctx);
    Owned<IHqlCppSetCursor> cursor = createSetSelector(ctx, simpleValue);

    if (includeAll)
    {
        CHqlBoundExpr isAll;
        cursor->buildIsAll(ctx, isAll);
        IHqlStmt * stmt = subctx.addFilter(isAll.expr);
        HqlExprArray args;
        args.append(*createVariable("out", makeBoolType()));
        callProcedure(subctx, outputXmlSetAllId, args);
        subctx.selectElse(stmt);
    }
    buildXmlSerializeBeginArray(subctx, itemName);
    {
        BuildCtx loopctx(subctx);
        CHqlBoundExpr boundCurElement;
        cursor->buildIterateLoop(loopctx, boundCurElement, false);
        OwnedHqlExpr curElement = boundCurElement.getTranslatedExpr();
        buildXmlSerializeScalar(loopctx, curElement, itemName);
    }
    buildXmlSerializeEndArray(subctx, itemName);
}

void HqlCppTranslator::buildXmlSerializeBeginNested(BuildCtx & ctx, IHqlExpression * name, bool doIndent)
{
    if (name)
    {
        HqlExprArray args;
        args.append(*createVariable("out", makeBoolType()));
        args.append(*LINK(name));
        args.append(*createConstant(false));
        callProcedure(ctx, outputXmlBeginNestedId, args);
    }
}

void HqlCppTranslator::buildXmlSerializeEndNested(BuildCtx & ctx, IHqlExpression * name)
{
    if (name)
    {
        HqlExprArray args;
        args.append(*createVariable("out", makeBoolType()));
        args.append(*LINK(name));
        callProcedure(ctx, outputXmlEndNestedId, args);
    }
}

void HqlCppTranslator::buildXmlSerializeBeginArray(BuildCtx & ctx, IHqlExpression * name)
{
    if (name)
    {
        HqlExprArray args;
        args.append(*createVariable("out", makeBoolType()));
        args.append(*LINK(name));
        callProcedure(ctx, outputXmlBeginArrayId, args);
    }
}

void HqlCppTranslator::buildXmlSerializeEndArray(BuildCtx & ctx, IHqlExpression * name)
{
    if (name)
    {
        HqlExprArray args;
        args.append(*createVariable("out", makeBoolType()));
        args.append(*LINK(name));
        callProcedure(ctx, outputXmlEndArrayId, args);
    }
}

void HqlCppTranslator::buildXmlSerializeSet(BuildCtx & ctx, IHqlExpression * field, IHqlExpression * value)
{
    OwnedHqlExpr name, itemName;
    extractXmlName(name, &itemName, NULL, field, "Item", false);

    HqlExprArray args;
    buildXmlSerializeBeginNested(ctx, name, false);
    buildXmlSerializeSetValues(ctx, value, itemName, (name != NULL));
    buildXmlSerializeEndNested(ctx, name);
}

void HqlCppTranslator::buildXmlSerializeDataset(BuildCtx & ctx, IHqlExpression * field, IHqlExpression * value, HqlExprArray * assigns)
{
    OwnedHqlExpr name, rowName;
    extractXmlName(name, &rowName, NULL, field, "Row", false);

    HqlExprArray args;
    buildXmlSerializeBeginNested(ctx, name, false);
    buildXmlSerializeBeginArray(ctx, rowName);

    Owned<IHqlCppDatasetCursor> cursor = createDatasetSelector(ctx, value);
    BuildCtx subctx(ctx);
    BoundRow * sourceRow = cursor->buildIterateLoop(subctx, false);
    buildXmlSerializeBeginNested(subctx, rowName, true);

    StringBuffer boundRowText;
    generateExprCpp(boundRowText, sourceRow->queryBound());
    buildXmlSerializeUsingMeta(subctx, field, boundRowText.str());

    buildXmlSerializeEndNested(subctx, rowName);

    buildXmlSerializeEndArray(ctx, rowName);
    buildXmlSerializeEndNested(ctx, name);
}

void HqlCppTranslator::buildXmlSerializeScalar(BuildCtx & ctx, IHqlExpression * selected, IHqlExpression * name)
{
    ITypeInfo * type = selected->queryType()->queryPromotedType();
    LinkedHqlExpr value = selected;
    LinkedHqlExpr size;
    IIdAtom * func;
    switch (type->getTypeCode())
    {
    case type_boolean:
        func = outputXmlBoolId;
        break;
    case type_string:
    case type_varstring:
        func = outputXmlStringId;
        break;
    case type_qstring:
        func = outputXmlQStringId;
        break;
    case type_data:
        func = outputXmlDataId;
        break;
    case type_unicode:
    case type_varunicode:
        func = outputXmlUnicodeId;
        break;
    case type_utf8:
        func = outputXmlUtf8Id;
        break;
    case type_real:
        func = outputXmlRealId;
        break;
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_bitfield:
        size.setown(getSizetConstant(type->getSize()));
        if (type->isSigned())
            func = outputXmlIntId;
        else
            func = outputXmlUIntId;
        break;
    case type_decimal:
        value.setown(ensureExprType(value, unknownStringType));
        func = outputXmlStringId;
        break;
    default:
        UNIMPLEMENTED;
    }

    HqlExprArray args;
    args.append(*createVariable("out", makeBoolType()));
    args.append(*value.getLink());
    if (size)
        args.append(*LINK(size));
    if (name)
        args.append(*LINK(name));
    else
        args.append(*getNullStringPointer(true));

    buildFunctionCall(ctx, func, args);
}

void HqlCppTranslator::buildXmlSerialize(BuildCtx & subctx, IHqlExpression * expr, IHqlExpression * selector, HqlExprArray * assigns, unsigned pass, unsigned & expectedIndex)
{
    if (anyXmlGeneratedForPass(expr, pass))
    {
        switch (expr->getOperator())
        {
        case no_field:
            {
                OwnedHqlExpr name;
                extractXmlName(name, NULL, NULL, expr, NULL, false);

                LinkedHqlExpr value;
                OwnedHqlExpr selected;
                ITypeInfo * type = expr->queryType()->queryPromotedType();
                if (assigns)
                {
                    OwnedHqlExpr match = getExtractMatchingAssign(*assigns, expr, expectedIndex, selector);
                    if (!match)
                    {
                        StringBuffer s;
                        expr->toString(s);
                        throwError2(HQLERR_MissingTransformAssignXX, s.str(), expr);
                    }

                    selected.set(match->queryChild(0));
                    value.setown(ensureExprType(match->queryChild(1), type));
                }
                else
                {
                    selected.setown(createSelectExpr(LINK(selector), LINK(expr)));
                    value.set(selected);
                }

                switch (type->getTypeCode())
                {
                case type_row:
                    {
                        IHqlExpression * record = queryOriginalRecord(type);
                        buildXmlSerializeBeginNested(subctx, name, false);
                        if (assigns)
                        {
                            if (value->getOperator() == no_createrow)
                            {
                                HqlExprArray childAssigns;
                                filterExpandAssignments(subctx, NULL, childAssigns, value->queryChild(0));
                                OwnedHqlExpr childSelf = createSelector(no_self, value, NULL);
                                if (name)
                                    buildXmlSerialize(subctx, record, childSelf, &childAssigns);
                                else
                                    buildXmlSerialize(subctx, record, childSelf, &childAssigns, pass, expectedIndex);
                            }
                            else
                            {
                                CHqlBoundExpr bound;
                                Owned<IReferenceSelector> ref = buildNewRow(subctx, value);
                                if (name)
                                    buildXmlSerialize(subctx, record, value, NULL);
                                else
                                    buildXmlSerialize(subctx, record, value, NULL, pass, expectedIndex);
                            }
                        }
                        else
                        {
                            if (name)
                                buildXmlSerialize(subctx, record, selected, assigns);
                            else
                                buildXmlSerialize(subctx, record, selected, assigns, pass, expectedIndex);
                        }
                        buildXmlSerializeEndNested(subctx, name);
                        return;
                    }
                    break;
                case type_set:
                    buildXmlSerializeSet(subctx, expr, value);
                    break;
                case type_dictionary:
                case type_table:
                case type_groupedtable:
                    buildXmlSerializeDataset(subctx, expr, value, assigns);
                    break;
                default:
                    buildXmlSerializeScalar(subctx, value, name);
                    break;
                }
            }
            break;
        case no_ifblock:
            {
                OwnedHqlExpr cond = replaceSelector(expr->queryChild(0), querySelfReference(), selector);
                BuildCtx condctx(subctx);
                buildFilter(condctx, cond);
                buildXmlSerialize(condctx, expr->queryChild(1), selector, assigns, pass, expectedIndex);
            }
            break;
        case no_record:
            {
                ForEachChild(idx, expr)
                    buildXmlSerialize(subctx, expr->queryChild(idx), selector, assigns, pass, expectedIndex);
            }
            break;
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            break;
        default:
            UNIMPLEMENTED;
        }
    }
}

void HqlCppTranslator::buildXmlSerialize(BuildCtx & subctx, IHqlExpression * expr, IHqlExpression * selector, HqlExprArray * assigns)
{
    unsigned expectedIndex = 0;
    buildXmlSerialize(subctx, expr, selector, assigns, 0, expectedIndex);
    expectedIndex = 0;
    buildXmlSerialize(subctx, expr, selector, assigns, 1, expectedIndex);
}

void HqlCppTranslator::buildXmlSerialize(BuildCtx & ctx, IHqlExpression * dataset, const char * funcName, bool isMeta)
{
    StringBuffer proto;
    proto.append("virtual void ").append(funcName).append("(const byte * self, IXmlWriter & out)");

    MemberFunction xmlFunc(*this, ctx, proto, MFdynamicproto);
    if (!isMeta)
    {
        buildXmlSerializeUsingMeta(xmlFunc.ctx, dataset, "self");
    }
    else
    {
        BoundRow * selfCursor = bindTableCursor(xmlFunc.ctx, dataset, "self");
        buildXmlSerialize(xmlFunc.ctx, dataset->queryRecord(), selfCursor->querySelector(), NULL);
    }
}

void HqlCppTranslator::buildXmlSerializeUsingMeta(BuildCtx & ctx, IHqlExpression * dataset, const char * self)
{
    MetaInstance meta(*this, dataset->queryRecord(), false);
    buildMetaInfo(meta);

    StringBuffer s;
    ctx.addQuoted(s.append(meta.queryInstanceObject()).append(".toXML(").append(self).append(", out);"));
}

//-------------------------------------------------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityOutputWorkunit(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * seq = querySequence(expr);
    IHqlExpression * name = queryResultName(expr);
    IHqlExpression * maxsize = expr->queryAttribute(maxSizeAtom);
    int sequence = (int)getIntValue(seq, ResultSequenceInternal);

    if (expr->hasAttribute(diskAtom))
    {
        StringBuffer suffix;
        suffix.append("_").append(sequence);
        IHqlExpression * newName = createConstant("~result::");
        newName = createValue(no_concat, LINK(unknownStringType), newName, createValue(no_wuid, LINK(unknownStringType)));
        newName = createValue(no_concat, LINK(unknownStringType), newName, createConstant(suffix));

        HqlExprArray args;
        unwindChildren(args, expr);
        args.add(*newName, 1);
        args.append(*createAttribute(overwriteAtom));
        args.append(*createAttribute(resultAtom));

        OwnedHqlExpr outputToTemp = expr->clone(args);
        return buildActivity(ctx, outputToTemp, isRoot);
    }

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    StringBuffer graphLabel;
    bool useImplementationClass = options.minimizeActivityClasses && (sequence == ResultSequenceInternal) && !maxsize;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKworkunitwrite, expr, "WorkUnitWrite");
    if (useImplementationClass)
        instance->setImplementationClass(newWorkUnitWriteArgId);

    graphLabel.append(getActivityText(instance->kind)).append("\n");
    getStoredDescription(graphLabel, seq, name, true);
    instance->graphLabel.set(graphLabel.str());
    buildActivityFramework(instance, isRoot && !isInternalSeq(seq));

    buildInstancePrefix(instance);

    noteResultDefined(ctx, instance, seq, name, isRoot);

    //virtual unsigned getFlags()
    StringBuffer flags;
    if (expr->hasAttribute(extendAtom))
        flags.append("|POFextend");
    if (expr->hasAttribute(groupedAtom))
        flags.append("|POFgrouped");
    if (maxsize)
        flags.append("|POFmaxsize");

    if (!useImplementationClass)
    {
        doBuildSequenceFunc(instance->classctx, seq, true);
        if (name)
        {
            MemberFunction func(*this, instance->startctx, "virtual const char * queryName()");
            buildReturn(func.ctx, name, constUnknownVarStringType);
        }
        if (maxsize)
            doBuildUnsignedFunction(instance->createctx, "getMaxSize", maxsize->queryChild(0));

        HqlExprArray xmlnsAttrs;
        gatherAttributes(xmlnsAttrs, xmlnsAtom, expr);
        IHqlExpression * outputRecord = instance->meta.queryRecord();
        Owned<IWUResult> result = createDatasetResultSchema(seq, name, outputRecord, xmlnsAttrs, true, false, 0);
        if (result)
        {
            result->setResultRowLimit(-1);
            if (sequence >= 0)
            {
                OwnedHqlExpr outputDs = createDataset(no_null, LINK(outputRecord));
                buildXmlSerialize(instance->startctx, outputDs, "serializeXml", false);
            }
        }

        if (flags.length())
            doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    }
    else
    {
        if (flags.length() == 0)
            flags.append("|0");
        OwnedHqlExpr flagsExpr = createQuoted(flags.str()+1, LINK(unsignedType));
        instance->addConstructorParameter(name);
        instance->addConstructorParameter(flagsExpr);
    }

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    associateRemoteResult(*instance, seq, name);
    return instance->getBoundActivity();
}


void HqlCppTranslator::doBuildStmtOutput(BuildCtx & ctx, IHqlExpression * expr)
{
    if (queryCurrentActivity(ctx))
    {
        ApplyStmtBuilder builder(*this);
        builder.buildStmt(ctx, expr);
        builder.flush(ctx);
        return;
    }

    IHqlExpression * dataset = expr->queryChild(0);
    if (expr->hasAttribute(groupedAtom) && (dataset->getOperator() != no_null))
        throwError1(HQLERR_NotSupportedInsideNoThor, "Grouped OUTPUT");

    LinkedHqlExpr seq = querySequence(expr);
    LinkedHqlExpr name = queryResultName(expr);
    assertex(seq != NULL);
    int sequence = (int)getIntValue(seq, (int)ResultSequenceInternal);
    if (!seq)
        seq.setown(getSizetConstant(sequence));
    if (!name)
        name.setown(createQuoted("NULL", LINK(constUnknownVarStringType)));

    HqlExprArray xmlnsAttrs;
    gatherAttributes(xmlnsAttrs, xmlnsAtom, expr);
    Owned<IWUResult> result = createDatasetResultSchema(seq, name, dataset->queryRecord(), xmlnsAttrs, true, false, 0);

    CHqlBoundExpr bound;
    buildDataset(ctx, dataset, bound, FormatNatural);
    OwnedHqlExpr count = getBoundCount(bound);

    HqlExprArray args;
    args.append(*LINK(name));
    args.append(*LINK(seq));
    args.append(*bound.getTranslatedExpr());
    args.append(*createTranslated(count));
    args.append(*LINK(queryBoolExpr(expr->hasAttribute(extendAtom))));
    buildFunctionCall(ctx, setResultDatasetId, args);
}


//-------------------------------------------------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityDictionaryWorkunitWrite(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IHqlExpression * dictionary = expr->queryChild(0);
    IHqlExpression * seq = querySequence(expr);
    IHqlExpression * name = queryResultName(expr);
    int sequence = (int)getIntValue(seq, ResultSequenceInternal);

    OwnedHqlExpr dataset;
    switch (dictionary->getOperator())
    {
    case no_null:
        dataset.setown(createNullDataset(dictionary));
        break;
    case no_createdictionary:
        dataset.set(dictionary->queryChild(0));
        break;
    default:
        throwUnexpectedOp(dictionary->getOperator());
    }

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    StringBuffer graphLabel;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKdictionaryworkunitwrite, expr, "DictionaryWorkUnitWrite");

    graphLabel.append(getActivityText(instance->kind)).append("\n");
    getStoredDescription(graphLabel, seq, name, true);
    instance->graphLabel.set(graphLabel.str());
    buildActivityFramework(instance, isRoot && !isInternalSeq(seq));

    buildInstancePrefix(instance);

    noteResultDefined(ctx, instance, seq, name, isRoot);

    //virtual unsigned getFlags()
    StringBuffer flags;

    doBuildSequenceFunc(instance->classctx, seq, true);
    if (name)
    {
        MemberFunction func(*this, instance->startctx, "virtual const char * queryName()");
        buildReturn(func.ctx, name, constUnknownVarStringType);
    }

    //Owned<IWUResult> result = createDatasetResultSchema(seq, name, record, true, false);
    buildDictionaryHashMember(instance->createctx, dictionary, "queryHashLookupInfo");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);


    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    associateRemoteResult(*instance, seq, name);
    return instance->getBoundActivity();
}


//---------------------------------------------------------------------------


ABoundActivity * HqlCppTranslator::doBuildActivityPipeThrough(BuildCtx & ctx, IHqlExpression * expr)
{
    if (expr->hasAttribute(_disallowed_Atom))
        throwError(HQLERR_PipeNotAllowed);
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * pipe = expr->queryChild(1);
    IHqlExpression * output = expr->queryAttribute(outputAtom);
    IHqlExpression * csvToPipe = output ? output->queryAttribute(csvAtom) : NULL;
    IHqlExpression * xmlToPipe = output ? output->queryAttribute(xmlAtom) : NULL;
    IHqlExpression * csvFromPipe = expr->queryAttribute(csvAtom);
    IHqlExpression * xmlFromPipe = expr->queryAttribute(xmlAtom);

    //MORE: Could optimize dataset to not use LCR rows - if it is coming from a disk file
    //Some other activities could similarly benefit (e.g., SORT), but they might need two
    //metas and more intelligence in the activities...
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKpipethrough, expr, "PipeThrough");
    if (csvToPipe)
        instance->addBaseClass("IHThorCsvWriteExtra", true);
    else if (xmlToPipe)
        instance->addBaseClass("IHThorXmlWriteExtra", true);
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    if (expr->hasAttribute(repeatAtom))
    {
        //virtual const char * getPipeProgram() { return "grep"; }
        instance->startctx.addQuotedLiteral("virtual const char * getPipeProgram() { return NULL; }");

        MemberFunction func(*this, instance->startctx, "virtual char * getNameFromRow(const void * _self)");
        func.ctx.addQuotedLiteral("const unsigned char * self = (const unsigned char *) _self;");
        bindTableCursor(func.ctx, dataset, "self");
        buildReturn(func.ctx, pipe, unknownVarStringType);
    }
    else
    {
        //virtual const char * getPipeProgram() { return "grep"; }
        MemberFunction func(*this, instance->startctx, "virtual const char * getPipeProgram()");
        buildReturn(func.ctx, pipe, unknownVarStringType);
    }

    if (csvToPipe)
    {
        buildCsvWriteMembers(instance, dataset, csvToPipe);
        instance->classctx.addQuotedLiteral("virtual IHThorCsvWriteExtra * queryCsvOutput() { return this; }");
    }

    if (xmlToPipe)
    {
        buildXmlWriteMembers(instance, dataset, xmlToPipe);
        instance->classctx.addQuotedLiteral("virtual IHThorXmlWriteExtra * queryXmlOutput() { return this; }");
    }

    bool usesContents = false;
    if (csvFromPipe)
    {
        if (isValidCsvRecord(expr->queryRecord()))
        {
            StringBuffer csvInstanceName;
            buildCsvReadTransformer(expr, csvInstanceName, csvFromPipe);

            StringBuffer s;
            s.append("virtual ICsvToRowTransformer * queryCsvTransformer() { return &").append(csvInstanceName).append("; }");
            instance->classctx.addQuoted(s);
        }
        else
        {
            throwUnexpected();  // should be caught earlier
        }
    }
    else if (xmlFromPipe)
    {
        doBuildXmlReadMember(*instance, expr, "queryXmlTransformer", usesContents);
        doBuildVarStringFunction(instance->classctx, "getXmlIteratorPath", queryAttributeChild(xmlFromPipe, rowAtom, 0));
    }

    StringBuffer flags;
    if (expr->hasAttribute(repeatAtom))
        flags.append("|TPFrecreateeachrow");
    if (expr->hasAttribute(groupAtom))
        flags.append("|TPFgroupeachrow");
    if (expr->hasAttribute(optAtom))
        flags.append("|TPFnofail");

    if (csvToPipe)
        flags.append("|TPFwritecsvtopipe");
    if (xmlToPipe)
        flags.append("|TPFwritexmltopipe");
    if (csvFromPipe)
        flags.append("|TPFreadcsvfrompipe");
    if (xmlFromPipe)
        flags.append("|TPFreadxmlfrompipe");
    if (usesContents)
        flags.append("|TPFreadusexmlcontents");
    if (xmlToPipe && xmlToPipe->hasAttribute(noRootAtom))
        flags.append("|TPFwritenoroot");
    if (xmlFromPipe && xmlFromPipe->hasAttribute(noRootAtom))
        flags.append("|TPFreadnoroot");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getPipeFlags", flags.str()+1);

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
//-- no_join [JOIN] --

/* in parms: NOT linked */
void HqlCppTranslator::doCompareLeftRight(BuildCtx & ctx, const char * funcname, const DatasetReference & datasetLeft, const DatasetReference & datasetRight, const HqlExprArray & left, const HqlExprArray & right)
{
    OwnedHqlExpr selSeq = createDummySelectorSequence();
    OwnedHqlExpr leftList = createValueSafe(no_sortlist, makeSortListType(NULL), left);
    OwnedHqlExpr leftSelect = datasetLeft.getSelector(no_left, selSeq);
    OwnedHqlExpr leftResolved = datasetLeft.mapCompound(leftList, leftSelect);
    OwnedHqlExpr rightList = createValueSafe(no_sortlist, makeSortListType(NULL), right);
    OwnedHqlExpr rightSelect = datasetRight.getSelector(no_right, selSeq);
    OwnedHqlExpr rightResolved = datasetRight.mapCompound(rightList, rightSelect);
    OwnedHqlExpr order = createValue(no_order, LINK(signedType), LINK(leftResolved), LINK(rightResolved));

    buildCompareMemberLR(ctx, funcname, order, datasetLeft.queryDataset(), datasetRight.queryDataset(), selSeq);
}

void HqlCppTranslator::buildSlidingMatchFunction(BuildCtx & ctx, const HqlExprArray & leftEq, const HqlExprArray & rightEq, const HqlExprArray & slidingMatches, const char * funcname, unsigned childIndex, const DatasetReference & datasetL, const DatasetReference & datasetR)
{
    HqlExprArray left, right;
    unsigned numSimple = leftEq.ordinality() - slidingMatches.ordinality();
    for (unsigned j=0; j<numSimple; j++)
    {
        left.append(OLINK(leftEq.item(j)));
        right.append(OLINK(rightEq.item(j)));
    }
    ForEachItemIn(i, slidingMatches)
    {
        IHqlExpression & cur = slidingMatches.item(i);
        left.append(*LINK(cur.queryChild(0)));
        right.append(*LINK(cur.queryChild(childIndex)));
    }

    doCompareLeftRight(ctx, funcname, datasetL, datasetR, left, right);
}

void HqlCppTranslator::generateSortCompare(BuildCtx & nestedctx, BuildCtx & ctx, node_operator side, const DatasetReference & dataset, const HqlExprArray & sorts, IHqlExpression * noSortAttr, bool canReuseLeft, bool isLightweight, bool isLocal)
{
    StringBuffer s, compareName;

    const char * sideText = (side == no_left) ? "Left" : "Right";
    compareName.append("compare").append(sideText);

    assertex(dataset.querySide() == no_activetable);
    bool noNeedToSort = isAlreadySorted(dataset.queryDataset(), sorts, isLocal, true, true);
    if (userPreventsSort(noSortAttr, side))
        noNeedToSort = true;

    if (noNeedToSort || isLightweight)
    {
        if (!noNeedToSort)
        {
            DBGLOG("Lightweight true, but code generator didn't think sort was required");
            ctx.addQuotedLiteral("//Forced by lightweight");
        }
        s.clear().append("virtual bool is").append(sideText).append("AlreadySorted() { return true; }");
        ctx.addQuoted(s);
    }

    if (canReuseLeft)
    {
        s.clear().append("virtual ICompare * queryCompare").append(sideText).append("() { return &compareLeft; }");
        ctx.addQuoted(s);
    }
    else
    {
        s.clear().append("virtual ICompare * queryCompare").append(sideText).append("() { return &").append(compareName).append("; }");
        ctx.addQuoted(s);

        BuildCtx classctx(nestedctx);
        IHqlStmt * classStmt = beginNestedClass(classctx, compareName.str(), "ICompare");

        {
            MemberFunction func(*this, classctx, "virtual int docompare(const void * _left, const void * _right) const" OPTIMIZE_FUNCTION_ATTRIBUTE);
            func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
            func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");
            func.ctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);

            OwnedHqlExpr groupOrder = createValueSafe(no_sortlist, makeSortListType(NULL), sorts);
            buildReturnOrder(func.ctx, groupOrder, dataset);
        }

        endNestedClass(classStmt);
    }
}

void HqlCppTranslator::generateSerializeAssigns(BuildCtx & ctx, IHqlExpression * record, IHqlExpression * selector, IHqlExpression * selfSelect, IHqlExpression * leftSelect, const DatasetReference & srcDataset, const DatasetReference & tgtDataset, HqlExprArray & srcSelects, HqlExprArray & tgtSelects, bool needToClear, node_operator serializeOp, IAtom * serialForm)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            {
                OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(cur));
                unsigned matchIndex = tgtSelects.find(*selected);
                if (matchIndex != NotFound)
                {
                    Owned<IHqlExpression> self = tgtDataset.mapScalar(&tgtSelects.item(matchIndex), selfSelect);
                    Owned<IHqlExpression> left = srcDataset.mapScalar(&srcSelects.item(matchIndex), leftSelect);
                    if (self->queryType() != left->queryType())
                    {
                        HqlExprArray args;
                        args.append(*LINK(left));
                        if (serializeOp == no_deserialize)
                            args.append(*LINK(self->queryRecord()));
                        args.append(*createAttribute(serialForm));
                        left.setown(createWrapper(serializeOp, self->queryType(), args));
                    }
                    buildAssign(ctx, self, left);
                    //Note, we could stop here if needToClear and all fields have been assigned, and all the following fields are fixed width.
                    // but not really sure it is worth it.
                }
                else if (cur->isDatarow())
                {
                    generateSerializeAssigns(ctx, cur->queryRecord(), selected, selfSelect, leftSelect, srcDataset, tgtDataset, srcSelects, tgtSelects, needToClear, serializeOp, serialForm);
                }
                else if (needToClear || mustInitializeField(cur))
                {
                    //MORE: Might want to recurse if a record
                    Owned<IHqlExpression> self = tgtDataset.mapScalar(selected, selfSelect);
                    buildClear(ctx, self);
                }
                break;
            }
        case no_record:
            generateSerializeAssigns(ctx, cur, selector, selfSelect, leftSelect, srcDataset, tgtDataset, srcSelects, tgtSelects, needToClear, serializeOp, serialForm);
            break;
        case no_ifblock:
            //Filter on target...
            UNIMPLEMENTED;
            //generateSerializeAssigns(ctx, cur->queryChild(1), selector, selfSelect, leftSelect, srcDataset, tgtDataset, srcSelects, tgtSelects, needToClear, serializeOp, serialForm);
            break;
        }
    }
}


void HqlCppTranslator::generateSerializeFunction(BuildCtx & ctx, const char * funcName, const DatasetReference & srcDataset, const DatasetReference & tgtDataset, HqlExprArray & srcSelects, HqlExprArray & tgtSelects, node_operator serializeOp, IAtom * serialForm)
{
    StringBuffer s;
    s.append("virtual unsigned ").append(funcName).append("(ARowBuilder & crSelf, const void * _src, unsigned & thisRecordSize)");

    MemberFunction func(*this, ctx, s, MFdynamicproto);
    ensureRowAllocated(func.ctx, "crSelf");
    func.ctx.addQuotedLiteral("const unsigned char * src = (const unsigned char *) _src;");

    OwnedHqlExpr selSeq = createDummySelectorSequence();
    BoundRow * tgtCursor = bindSelf(func.ctx, tgtDataset.queryDataset(), "crSelf");
    BoundRow * srcCursor = bindTableCursor(func.ctx, srcDataset.queryDataset(), "src", no_left, selSeq);

    IHqlExpression * leftSelect = srcCursor->querySelector();
    IHqlExpression * selfSelect = tgtCursor->querySelector();
    IHqlExpression * record = tgtDataset.queryDataset()->queryRecord();

    generateSerializeAssigns(func.ctx, record, tgtDataset.querySelector(), selfSelect, leftSelect, srcDataset, tgtDataset, srcSelects, tgtSelects, !isFixedRecordSize(record), serializeOp, serialForm);

    const bool serialize = (serializeOp == no_serialize);
    BoundRow * recordCursor = serialize ? srcCursor : tgtCursor;
    OwnedHqlExpr recordSize = getRecordSize(recordCursor->querySelector());
    OwnedHqlExpr recordSizeVar = createVariable("thisRecordSize", LINK(unsignedType));
    buildAssignToTemp(func.ctx, recordSizeVar, recordSize);

    buildReturnRecordSize(func.ctx, serialize ? tgtCursor : srcCursor);
}

class SerializeKeyInfo
{
public:
    HqlExprArray keyFields;
    HqlExprArray keySelects;
    HqlExprArray keyCompares;
    HqlExprArray allKeyCompares;
    HqlExprArray datasetSelects;
    HqlExprArray filteredSorts;
    OwnedHqlExpr keyRecord;
    OwnedHqlExpr keyDataset;
};

bool HqlCppTranslator::extractSerializeKey(SerializeKeyInfo & info, const DatasetReference & dataset, const HqlExprArray & sorts, bool isGlobal)
{
    if (!targetThor() || !isGlobal)
        return false;

    //check if there are any ifblocks, and if so don't allow it.  Even more accurate would be no join fields used in ifblocks
    //This test could be removed if keyToRecord() wasn't generated anymore
    IHqlExpression * record = dataset.queryDataset()->queryRecord();
    if (recordContainsIfBlock(record))
        return false;

    ForEachItemIn(idx, sorts)
    {
        //MORE: Nested - this won't serialize the key if sorting by a field in a nested record
        //      If this is a problem we will need to create new fields for each value.
        IHqlExpression & cur = sorts.item(idx);
        IHqlExpression * value = &cur;
        if (value->getOperator() == no_negate)
            value=value->queryChild(0);
        //It isn't possible to sensibly compare serialized dictionaries at the moment.
        if (value->isDictionary())
            return false;

        if ((value->getOperator() == no_select) && (value->queryChild(0)->queryNormalizedSelector() == dataset.querySelector()))
        {
            if (value->queryType()->getTypeCode() == type_alien)
            {
                //MORE: Really should check if a self contained alien data type.
                return false;
            }

            OwnedHqlExpr serializedField = getSerializedForm(value->queryChild(1), diskAtom); // Could be internal, but may require serialized compare
            OwnedHqlExpr mappedSelect = dataset.mapScalar(value,queryActiveTableSelector());
            OwnedHqlExpr keyedCompare = dataset.mapScalar(&cur,queryActiveTableSelector());
            info.keyFields.append(*LINK(serializedField));
            info.keySelects.append(*createSelectExpr(LINK(mappedSelect->queryChild(0)), LINK(serializedField)));
            info.datasetSelects.append(*LINK(value));
            info.keyCompares.append(*LINK(keyedCompare));
            info.filteredSorts.append(OLINK(cur));
            info.allKeyCompares.append(*LINK(keyedCompare));
        }
        else if (value->isConstant())
            info.allKeyCompares.append(OLINK(cur));
        else
            return false;
    }

    const bool aggressive = false;
    // When projecting is done by the serialize() function this will be worth changing to true
    // otherwise the extra cost of the project probably isn't likely to outweigh the extra copy
    unsigned numToSerialize = aggressive ? info.filteredSorts.ordinality() : sorts.ordinality();

    //The following test will need to change if we serialize when nested fields are used (see above)
    if (numToSerialize >= getFlatFieldCount(record))
        return false;

    info.keyRecord.setown(createRecord(info.keyFields));
    info.keyDataset.setown(createDataset(no_anon, LINK(info.keyRecord)));
    return true;
}

void HqlCppTranslator::generateSerializeKey(BuildCtx & nestedctx, node_operator side, SerializeKeyInfo & keyInfo, const DatasetReference & dataset, bool generateCompares)
{
    //check if there are any ifblocks, and if so don't allow it.  Even more accurate would be no join fields used in ifblocks
    //IHqlExpression * record = dataset.queryDataset()->queryRecord();
    const char * sideText = (side == no_none) ? "" : (side == no_left) ? "Left" : "Right";
    StringBuffer s;

    StringBuffer memberName;
    memberName.append("serializer").append(sideText);

    BuildCtx classctx(nestedctx);
    IHqlStmt * classStmt = beginNestedClass(classctx, memberName, "ISortKeySerializer");

    DatasetReference keyActiveRef(keyInfo.keyDataset, no_activetable, NULL);
    OwnedHqlExpr keyOrder = createValueSafe(no_sortlist, makeSortListType(NULL), keyInfo.keyCompares);

    generateSerializeFunction(classctx, "recordToKey", dataset, keyActiveRef, keyInfo.datasetSelects, keyInfo.keySelects, no_serialize, diskAtom);
    generateSerializeFunction(classctx, "keyToRecord", keyActiveRef, dataset, keyInfo.keySelects, keyInfo.datasetSelects, no_deserialize, diskAtom);
    buildMetaMember(classctx, keyInfo.keyRecord, false, "queryRecordSize");

    buildCompareMember(classctx, "CompareKey", keyOrder, keyActiveRef);
    doCompareLeftRight(classctx, "CompareKeyRow", keyActiveRef, dataset, keyInfo.keyCompares, keyInfo.filteredSorts);

    endNestedClass(classStmt);

    s.clear().append("virtual ISortKeySerializer * querySerialize").append(sideText).append("() { return &").append(memberName).append("; }");
    nestedctx.addQuoted(s);

    if (generateCompares)
    {
        buildCompareMember(nestedctx, "CompareKey", keyOrder, keyActiveRef);
        doCompareLeftRight(nestedctx, "CompareRowKey", dataset, keyActiveRef, keyInfo.filteredSorts, keyInfo.keyCompares);
    }
}

void HqlCppTranslator::generateSerializeKey(BuildCtx & nestedctx, node_operator side, const DatasetReference & dataset, const HqlExprArray & sorts, bool isGlobal, bool generateCompares)
{
    SerializeKeyInfo keyInfo;
    if (!extractSerializeKey(keyInfo, dataset, sorts, isGlobal))
        return;

    generateSerializeKey(nestedctx, side, keyInfo, dataset, generateCompares);
}

IHqlExpression * HqlCppTranslator::createFailMessage(const char * prefix, IHqlExpression * limit, IHqlExpression * filename, unique_id_t id)
{
    StringBuffer s;

    HqlExprArray values;
    values.append(*createConstant(s.clear().append(prefix)));
    if (limit)
    {
        values.append(*createConstant("("));
        values.append(*ensureExprType(limit, unknownStringType));
        values.append(*createConstant(")"));
    }

    if (filename)
    {
        values.append(*createConstant(" file '"));
        values.append(*ensureExprType(filename, unknownStringType));
        values.append(*createConstant("'"));
    }

    if (id)
        values.append(*createConstant(s.clear().append(" [id=").append(id).append("]")));

    OwnedHqlExpr errorText = createBalanced(no_concat, unknownStringType, values);
    return foldHqlExpression(errorText);
}


IHqlExpression * HqlCppTranslator::createFailAction(const char * prefix, IHqlExpression * limit, IHqlExpression * filename, unique_id_t id)
{
    IHqlExpression * msg = createFailMessage(prefix, limit, filename, id);
    return createValue(no_fail, makeVoidType(), msg, getDefaultAttr());
}

void HqlCppTranslator::doBuildJoinRowLimitHelper(ActivityInstance & instance, IHqlExpression * rowlimit, IHqlExpression * filename, bool generateImplicitLimit)
{
    if (rowlimit)
    {
        doBuildUnsignedFunction(instance.startctx, "getMatchAbortLimit", rowlimit->queryChild(0));
        if (!rowlimit->hasAttribute(skipAtom))
        {
            LinkedHqlExpr fail = queryChildOperator(no_fail, rowlimit);
            if (!fail)
                fail.setown(createFailAction("JOIN limit exceeded", rowlimit->queryChild(0), filename, instance.activityId));

            MemberFunction func(*this, instance.startctx, "virtual void onMatchAbortLimitExceeded()");
            buildStmt(func.ctx, fail);
        }
    }
    else if (generateImplicitLimit)
    {
        OwnedHqlExpr implicitLimit = getSizetConstant(options.defaultImplicitKeyedJoinLimit);
        doBuildUnsignedFunction(instance.startctx, "getMatchAbortLimit", implicitLimit);
        if (options.warnOnImplicitJoinLimit)
        {
            StringBuffer fname;
            if (filename)
                getExprECL(filename, fname.append(" "));
            WARNING2(CategoryLimit, HQLWRN_ImplicitJoinLimit, options.defaultImplicitKeyedJoinLimit, fname.str());
        }
    }
}


static size32_t getMaxSubstringLength(IHqlExpression * expr)
{
    IHqlExpression * rawSelect = expr->queryChild(0);
    IHqlExpression * range = expr->queryChild(1);
    IHqlExpression * rangeLow = range->queryChild(0);
    unsigned rawLength = rawSelect->queryType()->getStringLen();
    if (matchesConstantValue(rangeLow, 1))
        return rawLength;

    __int64 lowValue = getIntValue(rangeLow, UNKNOWN_LENGTH);
    size32_t resultLength = UNKNOWN_LENGTH;
    if ((rawLength != UNKNOWN_LENGTH) && (lowValue >= 1) && (lowValue <= rawLength))
        resultLength = rawLength - (size32_t)(lowValue - 1);
    return resultLength;
}

static IHqlExpression * getSimplifiedCommonSubstringRange(IHqlExpression * expr)
{
    IHqlExpression * rawSelect = expr->queryChild(0);
    IHqlExpression * range = expr->queryChild(1);
    IHqlExpression * rangeLow = range->queryChild(0);
    if (matchesConstantValue(rangeLow, 1))
        return LINK(rawSelect);

    HqlExprArray args;
    args.append(*LINK(rawSelect));
    args.append(*createValue(no_rangefrom, makeNullType(), LINK(rangeLow)));
    return expr->clone(args);
}


ABoundActivity * HqlCppTranslator::doBuildActivityJoinOrDenormalize(BuildCtx & ctx, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    assertex(op==no_join || op==no_selfjoin || op==no_denormalize || op==no_denormalizegroup);

    LinkedHqlExpr dataset1 = expr->queryChild(0);
    LinkedHqlExpr dataset2 = queryJoinRhs(expr);
    IHqlExpression * transform = expr->queryChild(3);
    IHqlExpression * noSortAttr = expr->queryAttribute(noSortAtom);
    IHqlExpression * rowlimit = expr->queryAttribute(rowLimitAtom);
    IHqlExpression * selSeq = querySelSeq(expr);
    bool isLeftOuter = false;
    bool isRightOuter = false;
    bool excludeMatches = false;
    bool isAllJoin = false;
    bool isLightweight = expr->hasAttribute(_lightweight_Atom);
    bool isManyLookup = expr->hasAttribute(manyAtom);

    if (expr->hasAttribute(leftouterAtom))
        isLeftOuter = true;
    if (expr->hasAttribute(rightouterAtom))
        isRightOuter = true;
    if (expr->hasAttribute(fullouterAtom))
    {
        isLeftOuter = true;
        isRightOuter = true;
    }
    if (expr->hasAttribute(leftonlyAtom))
    {
        isLeftOuter = true;
        excludeMatches = true;
    }
    if (expr->hasAttribute(rightonlyAtom))
    {
        isRightOuter = true;
        excludeMatches = true;
    }
    if (expr->hasAttribute(fullonlyAtom))
    {
        isLeftOuter = true;
        isRightOuter = true;
        excludeMatches = true;
    }
    if (expr->hasAttribute(allAtom))
        isAllJoin = true;

    bool isSmartJoin = expr->hasAttribute(smartAtom);
    bool isLookupJoin = expr->hasAttribute(lookupAtom) && !isSmartJoin;
    bool isHashJoin = targetThor() && expr->hasAttribute(hashAtom) && !isSmartJoin;
    bool isLocalJoin = !isHashJoin && expr->hasAttribute(localAtom);
    bool joinToSelf = (op == no_selfjoin);
    bool allowAllToLookupConvert = !options.noAllToLookupConversion;
    IHqlExpression * atmostAttr = expr->queryAttribute(atmostAtom);
    LinkedHqlExpr keepLimit = queryAttributeChild(expr, keepAtom, 0);
    //Delay removing ungroups until this point because they can be useful for reducing the size of spill files.
    if (isUngroup(dataset1) && !isLookupJoin)
        dataset1.set(dataset1->queryChild(0));
    if (isUngroup(dataset2))
        dataset2.set(dataset2->queryChild(0));

    if (expr->hasAttribute(groupedAtom) && targetThor())
        WARNING(CategoryEfficiency, HQLWRN_GroupedJoinIsLookupJoin);

    //Hash and smart joins are not valid inside child queries - convert to a normal join.
    //The flags should already have been stripped if targetting hthor/roxie
    if (insideChildQuery(ctx) && (isHashJoin || isSmartJoin))
    {
        assertex(targetThor());
        isHashJoin = false;
        isSmartJoin = false;
    }

    if ((op == no_denormalize || op == no_denormalizegroup) && targetThor() && options.checkThorRestrictions)
    {
        if (isHashJoin)
            throwError1(HQLERR_ThorDenormNoFeatureX, "HASH");
        if (expr->hasAttribute(firstAtom))
            throwError1(HQLERR_ThorDenormNoFeatureX, "FIRST");
        if (expr->hasAttribute(firstLeftAtom))
            throwError1(HQLERR_ThorDenormNoFeatureX, "FIRST LEFT");
        if (expr->hasAttribute(firstRightAtom))
            throwError1(HQLERR_ThorDenormNoFeatureX, "FIRST RIGHT");
        if (expr->hasAttribute(partitionRightAtom))
            throwError1(HQLERR_ThorDenormNoFeatureX, "PARTITION RIGHT");
    }


    bool slidingAllowed = options.slidingJoins && canBeSlidingJoin(expr);
    JoinSortInfo joinInfo(expr);
    joinInfo.findJoinSortOrders(slidingAllowed);

    if (atmostAttr && joinInfo.hasHardRightNonEquality())
    {
        if (isAllJoin)
            allowAllToLookupConvert = false;
        else
        {
            StringBuffer s;
            throwError1(HQLERR_BadJoinConditionAtMost,getExprECL(joinInfo.extraMatch, s.append(" (")).append(")").str());
        }
    }

    LinkedHqlExpr rhs = dataset2;
    if (isAllJoin)
    {
        if (joinInfo.hasRequiredEqualities() && allowAllToLookupConvert)
        {
            //Convert an all join to a many lookup if it can be done that way - more efficient, and same resourcing/semantics ...
            isManyLookup = true;
            isAllJoin = false;
            isLookupJoin = true;
        }
    }
    else if (!joinInfo.hasRequiredEqualities() && !joinInfo.hasOptionalEqualities())
    {
        if (expr->hasAttribute(_conditionFolded_Atom))
        {
            //LIMIT on an ALL join is equivalent to applying a limit to the rhs of the join (since all will hard match).
            //This could be transformed early, but uncommon enough to not be too concerned.
            if (rowlimit)
            {
                HqlExprArray args;
                args.append(*LINK(rhs));

                //A LIMIT on a join means no limit, whilst a LIMIT(ds, 0) limits to no records.
                //So avoid adding a zero limit (if constant), or ensure 0 is mapped to a maximal value.
                LinkedHqlExpr count = rowlimit->queryChild(0);
                if (count->queryValue())
                {
                    if (isZero(count))
                        count.clear();
                }
                else
                {
                    OwnedHqlExpr zero = createConstant(count->queryType()->castFrom(false, I64C(0)));
                    OwnedHqlExpr all = createConstant(count->queryType()->castFrom(false, I64C(-1)));
                    OwnedHqlExpr ne = createBoolExpr(no_ne, LINK(count), zero.getClear());
                    count.setown(createValue(no_if, count->getType(), LINK(ne), LINK(count), LINK(all)));
                }
                if (count)
                {
                    args.append(*LINK(count));
                    unwindChildren(args, rowlimit, 1);
                    rhs.setown(createDataset(no_limit, args));
                }
            }
            isAllJoin = true;
            //A non-many LOOKUP join can't really be converted to an ALL join.
            //Possibly if KEEP(1) was added, no limits, no skipping in transform etc.
            if ((isLookupJoin && !isManyLookup) || (op == no_selfjoin))
                isAllJoin = false;
            WARNING(CategoryUnusual, HQLWRN_JoinConditionFoldedNowAll);
        }
        else
        {
            StringBuffer name;
            if (expr->queryName())
                name.append(" ").append(expr->queryName());
            throwError1(HQLERR_JoinXTooComplex, name.str());
        }
    }
    if (isAllJoin)
        isLightweight = false;

    Owned<ABoundActivity> boundDataset1 = buildCachedActivity(ctx, dataset1);
    Owned<ABoundActivity> boundDataset2;
    if (!joinToSelf)
        boundDataset2.setown(buildCachedActivity(ctx, rhs));

    const char * argName;
    ThorActivityKind kind;
    if (op == no_selfjoin)
    {
        if (isLightweight)
            kind = TAKselfjoinlight;
        else
            kind = TAKselfjoin;
        argName = "Join";
    }
    else if (op == no_join)
    {
        if (isAllJoin)
        {
            kind = TAKalljoin;
            argName = "AllJoin";
        }
        else if (isLookupJoin)
        {
            kind = TAKlookupjoin;
            argName = "HashJoin";
        }
        else if (isSmartJoin)
        {
            kind = TAKsmartjoin;
            argName = "HashJoin";
        }
        else if (isHashJoin)
        {
            kind = TAKhashjoin;
            argName = "HashJoin";
        }
        else
        {
            kind = TAKjoin;
            argName = "Join";
        }
    }
    else if (op == no_denormalize)
    {
        if (isAllJoin)
        {
            kind = TAKalldenormalize;
            argName = "AllDenormalize";
        }
        else if (isLookupJoin)
        {
            kind = TAKlookupdenormalize;
            argName = "HashDenormalize";
        }
        else if (isSmartJoin)
        {
            kind = TAKsmartdenormalize;
            argName = "HashDenormalize";
        }
        else if (isHashJoin)
        {
            kind = TAKhashdenormalize;
            argName = "HashDenormalize";
        }
        else
        {
            kind = TAKdenormalize;
            argName = "Denormalize";
        }
    }
    else
    {
        if (isAllJoin)
        {
            kind = TAKalldenormalizegroup;
            argName = "AllDenormalizeGroup";
        }
        else if (isLookupJoin)
        {
            kind = TAKlookupdenormalizegroup;
            argName = "HashDenormalizeGroup";
        }
        else if (isSmartJoin)
        {
            kind = TAKsmartdenormalizegroup;
            argName = "HashDenormalizeGroup";
        }
        else if (isHashJoin)
        {
            kind = TAKhashdenormalizegroup;
            argName = "HashDenormalizeGroup";
        }
        else
        {
            kind = TAKdenormalizegroup;
            argName = "DenormalizeGroup";
        }
    }

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, argName);
    if (isLightweight)
    {
        StringBuffer graphLabel;
        if (kind != TAKselfjoinlight)
            graphLabel.append("Lightweight ");
        graphLabel.append(getActivityText(kind));
        instance->graphLabel.set(graphLabel.str());
    }

    instance->setLocal(isLocalJoin && !insideChildQuery(ctx));
    buildActivityFramework(instance);

    buildInstancePrefix(instance);
    bool partitionRight = expr->hasAttribute(partitionRightAtom) && (kind != TAKselfjoin) && !joinInfo.isSlidingJoin();
    DatasetReference lhsDsRef(dataset1, no_activetable, NULL);
    DatasetReference rhsDsRef(dataset2, no_activetable, NULL);

    bool couldBeKeepOne = keepLimit && (!keepLimit->queryValue() || (keepLimit->queryValue()->getIntValue() <= 1));
    if (dataset1->queryRecord() == dataset2->queryRecord())
    {
        //more could use the compareLeftRight function instead of generating the same code 
        //several time....
    }
    bool canReuseLeftCompare = recordTypesMatch(dataset1, dataset2) && arraysMatch(joinInfo.queryLeftSort(), joinInfo.queryRightSort());
    if (!isAllJoin)
    {
        bool isLocalSort = isLocalJoin || !targetThor();
        //Lookup join doesn't need the left sort (unless it is reused elsewhere), or the right sort unless it is deduping.
        if (canReuseLeftCompare || !isLookupJoin)
            generateSortCompare(instance->nestedctx, instance->classctx, no_left, lhsDsRef, joinInfo.queryLeftSort(), noSortAttr, false, isLightweight, isLocalSort);
        generateSortCompare(instance->nestedctx, instance->classctx, no_right, rhsDsRef, joinInfo.queryRightSort(), noSortAttr, canReuseLeftCompare, isLightweight, isLocalSort);

        //Only joins that partition need the serialization functions
        if (!isHashJoin && !isLookupJoin && !joinInfo.isSlidingJoin())
        {
            bool isGlobal = !isLocalJoin && !instance->isChildActivity();
            BuildCtx nestedctx(instance->nestedctx);
            SerializeKeyInfo keyInfo;
            if (!partitionRight)
            {
                if (extractSerializeKey(keyInfo, lhsDsRef, joinInfo.queryLeftSort(), isGlobal))
                {
                    generateSerializeKey(nestedctx, no_left, keyInfo, lhsDsRef, false);
                    DatasetReference keyActiveRef(keyInfo.keyDataset, no_activetable, NULL);
                    HqlExprArray keyRequired;
                    appendArray(keyRequired, keyInfo.allKeyCompares);
                    keyRequired.trunc(joinInfo.numRequiredEqualities());
                    doCompareLeftRight(nestedctx, "CompareLeftKeyRightRow", keyActiveRef, rhsDsRef, keyRequired, joinInfo.queryRightReq());
                }
            }
            else
            {
                if (extractSerializeKey(keyInfo, rhsDsRef, joinInfo.queryRightSort(), isGlobal))
                {
                    generateSerializeKey(nestedctx, no_right, keyInfo, rhsDsRef, false);
                    DatasetReference keyActiveRef(keyInfo.keyDataset, no_activetable, NULL);
                    HqlExprArray keyRequired;
                    appendArray(keyRequired, keyInfo.allKeyCompares);
                    keyRequired.trunc(joinInfo.numRequiredEqualities());
                    doCompareLeftRight(nestedctx, "CompareRightKeyLeftRow", keyActiveRef, lhsDsRef, keyRequired, joinInfo.queryLeftReq());
                }
            }
        }
    }

    StringBuffer flags;
    if (excludeMatches) flags.append("|JFexclude");
    if (isLeftOuter)    flags.append("|JFleftouter");
    if (isRightOuter)   flags.append("|JFrightouter");
    if (expr->hasAttribute(firstAtom)) flags.append("|JFfirst");
    if (expr->hasAttribute(firstLeftAtom)) flags.append("|JFfirstleft");
    if (expr->hasAttribute(firstRightAtom)) flags.append("|JFfirstright");
    if (partitionRight) flags.append("|JFpartitionright");
    if (expr->hasAttribute(parallelAtom)) flags.append("|JFparallel");
    if (expr->hasAttribute(sequentialAtom)) flags.append("|JFsequential");
    if (transformContainsSkip(transform))
        flags.append("|JFtransformMaySkip");
    if (rowlimit && rowlimit->hasAttribute(skipAtom))
        flags.append("|JFmatchAbortLimitSkips");
    if (rowlimit && rowlimit->hasAttribute(countAtom))
        flags.append("|JFcountmatchabortlimit");
    if (joinInfo.isSlidingJoin()) flags.append("|JFslidingmatch");
    if (joinInfo.extraMatch) flags.append("|JFmatchrequired");
    if (isLookupJoin && isManyLookup) flags.append("|JFmanylookup");
    if (expr->hasAttribute(onFailAtom))
        flags.append("|JFonfail");
    if (!isOrdered(expr))
        flags.append("|JFreorderable");
    if (transformReturnsSide(expr, no_left, 0))
        flags.append("|JFtransformmatchesleft");
    if (joinInfo.hasOptionalEqualities())
        flags.append("|JFlimitedprefixjoin");

    if (isAlreadySorted(dataset1, joinInfo.queryLeftSort(), true, true, false) || userPreventsSort(noSortAttr, no_left))
        flags.append("|JFleftSortedLocally");
    if (isAlreadySorted(dataset2, joinInfo.queryRightSort(), true, true, false) || userPreventsSort(noSortAttr, no_right))
        flags.append("|JFrightSortedLocally");
    if (isSmartJoin) flags.append("|JFsmart|JFmanylookup");
    if (isSmartJoin || expr->hasAttribute(unstableAtom))
        flags.append("|JFunstable");
    if (joinInfo.neverMatchSelf())
        flags.append("|JFnevermatchself");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getJoinFlags", flags.str()+1);

    if (!isAllJoin)
    {
        buildSkewThresholdMembers(instance->classctx, expr);

        if (!isZero(joinInfo.atmost.limit))
            doBuildUnsignedFunction(instance->startctx, "getJoinLimit", joinInfo.atmost.limit);
    }

    if (keepLimit)
        doBuildUnsignedFunction(instance->startctx, "getKeepLimit", keepLimit);

    // The transform function is pretty standard - no need for copies here
    switch (op)
    {
    case no_join:
    case no_selfjoin:
    case no_denormalize:
        {
            MemberFunction func(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned counter, unsigned flags)");
            ensureRowAllocated(func.ctx, "crSelf");

            IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
            associateCounter(func.ctx, counter, "counter");
            associateLocalJoinTransformFlags(func.ctx, "flags", dataset1, no_left, selSeq);
            associateLocalJoinTransformFlags(func.ctx, "flags", dataset2, no_right, selSeq);

            buildTransformBody(func.ctx, transform, dataset1, dataset2, instance->dataset, selSeq);
            break;
        }
    case no_denormalizegroup:
        {
            MemberFunction func(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned numRows, const void * * _rows, unsigned flags)");
            ensureRowAllocated(func.ctx, "crSelf");
            associateLocalJoinTransformFlags(func.ctx, "flags", dataset1, no_left, selSeq);
            associateLocalJoinTransformFlags(func.ctx, "flags", dataset2, no_right, selSeq);

            func.ctx.addQuotedLiteral("unsigned char * * rows = (unsigned char * *) _rows;");


            BoundRow * selfCursor = buildTransformCursors(func.ctx, transform, dataset1, dataset2, instance->dataset, selSeq);
            bindRows(func.ctx, no_right, selSeq, expr->queryAttribute(_rowsid_Atom), dataset2, "numRows", "rows", options.mainRowsAreLinkCounted);
            doBuildTransformBody(func.ctx, transform, selfCursor);
            break;
        }
    }

    IHqlExpression * onFail = expr->queryAttribute(onFailAtom);
    if (onFail)
    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t onFailTransform(ARowBuilder & crSelf, const void * _left, const void * _right, IException * except, unsigned flags)");
        ensureRowAllocated(func.ctx, "crSelf");
        associateLocalFailure(func.ctx, "except");
        associateLocalJoinTransformFlags(func.ctx, "flags", dataset1, no_left, selSeq);
        associateLocalJoinTransformFlags(func.ctx, "flags", dataset2, no_right, selSeq);

        buildTransformBody(func.ctx, onFail->queryChild(0), dataset1, dataset2, instance->dataset, selSeq);
    }

    // The collate function is used to work out which side to read from or if we have a potentially matching record
    if (!isAllJoin)
    {
        //if left and right match, then leftright compare function is also the same
        if (canReuseLeftCompare && !joinInfo.hasOptionalEqualities())
            instance->nestedctx.addQuotedLiteral("virtual ICompare * queryCompareLeftRight() { return &compareLeft; }");
        else
            doCompareLeftRight(instance->nestedctx, "CompareLeftRight", lhsDsRef, rhsDsRef, joinInfo.queryLeftReq(), joinInfo.queryRightReq());
    }

    doBuildJoinRowLimitHelper(*instance, rowlimit, NULL, false);

    //--function to clear left, used for right outer join and vice-versa
    bool createDefaultRight = onFail || isLeftOuter;
    if (isRightOuter)
        buildClearRecordMember(instance->createctx, "Left", dataset1);
    if (createDefaultRight)
        buildClearRecordMember(instance->createctx, "Right", dataset2);
    buildJoinMatchFunction(instance->startctx, "match", dataset1, dataset2, joinInfo.extraMatch, selSeq);

    if (joinInfo.isSlidingJoin())
    {
        buildSlidingMatchFunction(instance->nestedctx, joinInfo.queryLeftSort(), joinInfo.queryRightSort(), joinInfo.slidingMatches, "CompareLeftRightLower", 1, lhsDsRef, rhsDsRef);
        buildSlidingMatchFunction(instance->nestedctx, joinInfo.queryLeftSort(), joinInfo.queryRightSort(), joinInfo.slidingMatches, "CompareLeftRightUpper", 2, lhsDsRef, rhsDsRef);
    }

    if (isHashJoin||isLookupJoin|isSmartJoin)
    {
        OwnedHqlExpr leftList = createValueSafe(no_sortlist, makeSortListType(NULL), joinInfo.queryLeftReq());
        buildHashOfExprsClass(instance->nestedctx, "HashLeft", leftList, lhsDsRef, false);

        bool canReuseLeftHash = recordTypesMatch(dataset1, dataset2) && arraysMatch(joinInfo.queryLeftReq(), joinInfo.queryRightReq());
        if (!canReuseLeftHash)
        {
            OwnedHqlExpr rightList = createValueSafe(no_sortlist, makeSortListType(NULL), joinInfo.queryRightReq());
            buildHashOfExprsClass(instance->nestedctx, "HashRight", rightList, rhsDsRef, false);
        }
        else
            instance->nestedctx.addQuotedLiteral("virtual IHash * queryHashRight() { return &HashLeft; }");
    }

    if (joinInfo.hasOptionalEqualities())
    {
        OwnedHqlExpr leftSelect = createSelector(no_left, dataset1, selSeq);
        OwnedHqlExpr rightSelect = createSelector(no_right, dataset2, selSeq);

        UnsignedArray origins;
        unsigned origin = 0;
        ForEachItemIn(i, joinInfo.queryLeftOpt())
        {
            IHqlExpression & left = joinInfo.queryLeftOpt().item(i);
            IHqlExpression & right = joinInfo.queryRightOpt().item(i);
            unsigned delta;
            if (origin == UNKNOWN_LENGTH)
                throwError(HQLERR_AtmostFollowUnknownSubstr);

            if (isCommonSubstringRange(&left))
            {
                size32_t leftLen = getMaxSubstringLength(&left);
                size32_t rightLen = getMaxSubstringLength(&right);
                if (leftLen == rightLen)
                    delta = leftLen;
                else
                    delta = UNKNOWN_LENGTH;
            }
            else
                delta = 1;
            origins.append(origin);
            if (delta != UNKNOWN_LENGTH)
                origin += delta;
            else
                origin = UNKNOWN_LENGTH;
        }

        OwnedHqlExpr compare;
        OwnedITypeInfo retType = makeIntType(4, true);
        OwnedHqlExpr zero = createConstant(retType->castFrom(true, 0));
        ForEachItemInRev(i1, joinInfo.queryLeftOpt())
        {
            IHqlExpression & left = joinInfo.queryLeftOpt().item(i1);
            IHqlExpression & right = joinInfo.queryRightOpt().item(i1);

            unsigned origin = origins.item(i1);
            if (isCommonSubstringRange(&left))
            {
                OwnedHqlExpr simpleLeft = getSimplifiedCommonSubstringRange(&left);
                OwnedHqlExpr simpleRight = getSimplifiedCommonSubstringRange(&right);
                HqlExprArray args;
                args.append(*lhsDsRef.mapCompound(simpleLeft, leftSelect));
                args.append(*rhsDsRef.mapCompound(simpleRight, rightSelect));

                IIdAtom * func = prefixDiffStrId;
                ITypeInfo * lhsType = args.item(0).queryType();
                if (isUnicodeType(lhsType))
                {
                    func = prefixDiffUnicodeId;
                    args.append(*createConstant(str(lhsType->queryLocale())));
                }
                args.append(*getSizetConstant(origin));
                OwnedHqlExpr diff = bindFunctionCall(func, args);
                if (compare)
                {
                    OwnedHqlExpr alias = createAlias(diff, NULL);
                    OwnedHqlExpr compareNe = createValue(no_ne, makeBoolType(), LINK(alias), LINK(zero));
                    compare.setown(createValue(no_if, LINK(retType), compareNe.getClear(), LINK(alias), compare.getClear()));
                }
                else
                    compare.set(diff);
            }
            else
            {
                OwnedHqlExpr leftExpr = lhsDsRef.mapCompound(&left, leftSelect);
                OwnedHqlExpr rightExpr = lhsDsRef.mapCompound(&right, rightSelect);
                OwnedHqlExpr compareGt = createValue(no_gt, makeBoolType(), LINK(leftExpr), LINK(rightExpr));
                OwnedHqlExpr gtValue = createConstant(retType->castFrom(true, origin+1));
                OwnedHqlExpr ltValue = createConstant(retType->castFrom(true, -(int)(origin+1)));
                OwnedHqlExpr mismatch = createValue(no_if, LINK(retType), compareGt.getClear(), gtValue.getClear(), ltValue.getClear());
                OwnedHqlExpr compareNe = createValue(no_ne, makeBoolType(), LINK(leftExpr), LINK(rightExpr));
                OwnedHqlExpr eqValue = compare ? LINK(compare) : LINK(zero);
                compare.setown(createValue(no_if, LINK(retType), compareNe.getClear(), mismatch.getClear(), eqValue.getClear()));
                origin += 1;
            }
        }

        buildCompareMemberLR(instance->nestedctx, "PrefixCompare", compare, dataset1, dataset2, selSeq);
    }

//    buildCompareMemberLR(instance->nestedctx, "CompareLeftKeyRightRow", compare, dataset1, dataset2, selSeq);

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset1, 0, 0, boundDataset2 ? "LEFT" : NULL);
    if (boundDataset2)
        buildConnectInputOutput(ctx, instance, boundDataset2, 0, 1, "RIGHT");

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityJoin(BuildCtx & ctx, IHqlExpression * expr)
{
    if (isKeyedJoin(expr))
        return doBuildActivityKeyedJoinOrDenormalize(ctx, expr);
    else
        return doBuildActivityJoinOrDenormalize(ctx, expr);
}

//---------------------------------------------------------------------------

void HqlCppTranslator::doUserTransform(BuildCtx & ctx, IHqlExpression * transform, BoundRow * self)
{
    node_operator transformOp = transform->getOperator();
    assertex(transformOp == no_call || transformOp == no_externalcall);

    //Ugly, but target selector is passed in as the target.expr.  Should possibly have an extra parameter.
    CHqlBoundTarget target;
    target.expr.set(self->querySelector());
    doBuildCall(ctx, &target, transform, NULL);
}

void HqlCppTranslator::doTransform(BuildCtx & ctx, IHqlExpression * transform, BoundRow * self)
{
    HqlExprArray assigns;
    IHqlExpression * record = self->queryRecord();
    TransformBuilder builder(*this, ctx, record, self, assigns);
    builder.doTransform(ctx, transform, self);
}

void HqlCppTranslator::doUpdateTransform(BuildCtx & ctx, IHqlExpression * transform, BoundRow * self, BoundRow * previous, bool alwaysNextRow)
{
    HqlExprArray assigns;
    IHqlExpression * record = self->queryRecord();
    UpdateTransformBuilder builder(*this, ctx, record, self, previous->querySelector(), assigns, alwaysNextRow);
    builder.doTransform(ctx, transform, self);
}

void HqlCppTranslator::doInlineTransform(BuildCtx & ctx, IHqlExpression * transform, BoundRow * targetRow)
{
    Owned<BoundRow> rowBuilder = createRowBuilder(ctx, targetRow);
    doTransform(ctx, transform, rowBuilder);
    finalizeTempRow(ctx, targetRow, rowBuilder);
}

void HqlCppTranslator::precalculateFieldOffsets(BuildCtx & ctx, IHqlExpression * expr, BoundRow * cursor)
{
    if (!cursor)
        return;
    if (isFixedRecordSize(cursor->queryRecord()))
        return;
    IHqlExpression * lastField;
    IHqlExpression * selector = cursor->querySelector();
    {
        FieldAccessAnalyser analyser(selector);
        analyser.analyse(expr, 0);
        lastField = analyser.queryLastFieldAccessed();
    }
    if (!lastField)
        return;

    OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(lastField));
    Owned<IReferenceSelector> ref = buildReference(ctx, selected);
    CHqlBoundExpr boundOffset;
    ref->buildAddress(ctx, boundOffset);
}

BoundRow * HqlCppTranslator::buildTransformCursors(BuildCtx & ctx, IHqlExpression * transform, IHqlExpression * left, IHqlExpression * right, IHqlExpression * self, IHqlExpression * selSeq)
{
    if (transform->getOperator() == no_skip)
        return NULL;

    assertRecordTypesMatch(self->queryRecord(), transform->queryRecord());

    if (left)
        ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
    if (right)
        ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");

    // Bind left to "left" and right to RIGHT
    BoundRow * leftRow = NULL;
    BoundRow * rightRow = NULL;
    if (left)
        leftRow = bindTableCursor(ctx, left, "left", no_left, selSeq);
    if (right)
        rightRow = bindTableCursor(ctx, right, "right", no_right, selSeq);

    if (options.precalculateFieldOffsets)
    {
        precalculateFieldOffsets(ctx, transform, leftRow);
        precalculateFieldOffsets(ctx, transform, rightRow);
    }
    return bindSelf(ctx, self, "crSelf");
}

void HqlCppTranslator::doBuildTransformBody(BuildCtx & ctx, IHqlExpression * transform, BoundRow * selfCursor)
{
    if (transform->getOperator() == no_skip)
    {
        ctx.addReturn(queryZero());
        return;
    }

    associateSkipReturnMarker(ctx, queryZero(), selfCursor);
    doTransform(ctx, transform, selfCursor);
    buildReturnRecordSize(ctx, selfCursor);
}

void HqlCppTranslator::buildTransformBody(BuildCtx & ctx, IHqlExpression * transform, IHqlExpression * left, IHqlExpression * right, IHqlExpression * self, IHqlExpression * selSeq)
{
    BoundRow * selfCursor = buildTransformCursors(ctx, transform, left, right, self, selSeq);
    doBuildTransformBody(ctx, transform, selfCursor);
}


void HqlCppTranslator::buildIterateTransformFunction(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * transform, IHqlExpression * counter, IHqlExpression * selSeq)
{
    MemberFunction func(*this, ctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned __int64 counter)");
    ensureRowAllocated(func.ctx, "crSelf");
    associateCounter(func.ctx, counter, "counter");
    buildTransformBody(func.ctx, transform, dataset, dataset, dataset, selSeq);
}


void HqlCppTranslator::buildRollupTransformFunction(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * transform, IHqlExpression * selSeq)
{
    MemberFunction func(*this, ctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right)");
    ensureRowAllocated(func.ctx, "crSelf");
    buildTransformBody(func.ctx, transform, dataset, dataset, dataset, selSeq);
}


ABoundActivity * HqlCppTranslator::doBuildActivityIterate(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * transform = expr->queryChild(1);
    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
    IHqlExpression * selSeq = querySelSeq(expr);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKiterate, expr, "Iterate");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);
    buildIterateTransformFunction(instance->startctx, dataset, transform, counter, selSeq);
    buildClearRecordMember(instance->createctx, "", dataset);
    if (transformContainsSkip(transform))
        doBuildBoolFunction(instance->classctx, "canFilter", true);

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

IHqlExpression * HqlCppTranslator::queryExpandAliasScope(BuildCtx & ctx, IHqlExpression * expr)
{
    for (;;)
    {
        switch (expr->getOperator())
        {
        case no_alias_scope:
            expandAliasScope(ctx, expr);
            expr = expr->queryChild(0);
            break;
        case no_compound:
            buildStmt(ctx, expr->queryChild(0));
            expr = expr->queryChild(1);
            break;
        default:
            return expr;
        }
    }
}


void HqlCppTranslator::buildProcessTransformFunction(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * right = expr->queryChild(1);
    IHqlExpression * transformRow = expr->queryChild(2);
    IHqlExpression * transformRight = expr->queryChild(3);
    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
    IHqlExpression * selSeq = querySelSeq(expr);

    MemberFunction func(*this, ctx, "virtual size32_t transform(ARowBuilder & crSelf, ARowBuilder & crSelfRight, const void * _left, const void * _right, unsigned __int64 counter)");
    associateCounter(func.ctx, counter, "counter");

    if ((transformRow->getOperator() == no_skip) || (transformRight->getOperator() == no_skip))
    {
        func.ctx.addReturn(queryZero());
        return;
    }

    ensureRowAllocated(func.ctx, "crSelf");
    ensureRowAllocated(func.ctx, "crSelfRight");
    func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
    func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");
    bindTableCursor(func.ctx, dataset, "left", no_left, selSeq);
    bindTableCursor(func.ctx, right, "right", no_right, selSeq);

    LinkedHqlExpr skipReturnValue = queryZero();
    associateSkipReturnMarker(func.ctx, skipReturnValue, NULL);

    //Perform cse on both transforms
    OwnedHqlExpr comma = createComma(LINK(transformRow), LINK(transformRight));
    comma.setown(spotScalarCSE(comma, NULL, queryOptions().spotCseInIfDatasetConditions));
    if (comma->getOperator() == no_alias_scope)
        comma.set(comma->queryChild(0));

    HqlExprArray unwound;
    comma->unwindList(unwound, no_comma);
    unsigned max = unwound.ordinality();

    BoundRow * selfCursor = bindSelf(func.ctx, dataset, "crSelf");
    BoundRow * selfRowCursor = bindSelf(func.ctx, right, "crSelfRight");

    for (unsigned i=0; i<max-2; i++)
        buildStmt(func.ctx, &unwound.item(i));

    IHqlExpression * newTransformRow = queryExpandAliasScope(func.ctx, &unwound.item(max-2));
    IHqlExpression * newTransformRight = queryExpandAliasScope(func.ctx, &unwound.item(max-1));
    assertex(newTransformRow->getOperator() == no_transform && newTransformRight->getOperator() == no_transform);

    doTransform(func.ctx, newTransformRow, selfCursor);
    doTransform(func.ctx, newTransformRight, selfRowCursor);
    buildReturnRecordSize(func.ctx, selfCursor);
}


ABoundActivity * HqlCppTranslator::doBuildActivityProcess(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * right = expr->queryChild(1);
    IHqlExpression * transformRow = expr->queryChild(2);
    IHqlExpression * transformRight = expr->queryChild(3);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKprocess, expr, "Process");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    buildMetaMember(instance->classctx, right->queryRecord(), false, "queryRightRecordSize");

    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t createInitialRight(ARowBuilder & crSelf)");

        ensureRowAllocated(func.ctx, "crSelf");
        BoundRow * cursor = bindSelf(func.ctx, right, "crSelf");
        Owned<IReferenceSelector> createdRef = createReferenceSelector(cursor);
        buildRowAssign(func.ctx, createdRef, right);
        buildReturnRecordSize(func.ctx, cursor);
    }

    buildProcessTransformFunction(instance->startctx, expr);
    if (transformContainsSkip(transformRow) || transformContainsSkip(transformRight))
        doBuildBoolFunction(instance->classctx, "canFilter", true);

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------


ABoundActivity * HqlCppTranslator::doBuildActivitySelectNth(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * index = expr->queryChild(1);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    //If selecting 1st element of a non-grouped aggregate (which can only produce one row) then don't need to 
    //add the selectNth operator.
    IHqlExpression * search = dataset;
    if (hasSingleRow(dataset))
    {
        IValue * indexValue = index->queryValue();
        if (indexValue && (indexValue->getIntValue() == 1))
        {
            //index first element - don't need to do anything...
            //if x[n] is ever used as a dataset this assumption is invalid....
            return LINK(boundDataset);
        }
    }

#if 0
    //MORE: Should optimize left.child[1] and probably others - e.g., localresult[n]
    switch (dataset->getOperator())
    {
    case no_select:
        if (!isNewSelector(dataset))
            return doBuildActivityChildDataset(ctx, expr);
        break;
        //MORE: What other selects are worth special casing?
    }
#endif

    bool useImplementationClass = options.minimizeActivityClasses && (index->getOperator() == no_constant);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKselectn, expr, "SelectN");
    if (useImplementationClass)
        instance->setImplementationClass(newSelectNArgId);
    buildActivityFramework(instance);

    if (matchesConstantValue(index, 1))
        instance->graphLabel.set("Select 1st");

    buildInstancePrefix(instance);

    if (!useImplementationClass)
    {
        MemberFunction func(*this, instance->startctx, "virtual unsigned __int64 getRowToSelect()");
        buildReturn(func.ctx, index);

        buildClearRecordMember(instance->createctx, "", dataset);
    }
    else
    {
        instance->addConstructorParameter(index);
        OwnedHqlExpr func = getClearRecordFunction(dataset->queryRecord());
        //This is a mess - pretend that the pointer to function parameter is a boolean
        //The code generator really should have better support for classes etc..  One day...
        OwnedHqlExpr fakeFunc = createValue(no_typetransfer, makeBoolType(), LINK(func));
        OwnedHqlExpr translatedFakeFunc = createValue(no_translated, makeBoolType(), LINK(fakeFunc));
        instance->addConstructorParameter(translatedFakeFunc);
    }

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

void HqlCppTranslator::doBuildClearAggregateRecord(BuildCtx & ctx, IHqlExpression * record, IHqlExpression * self, IHqlExpression * transform)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            doBuildClearAggregateRecord(ctx, cur, self, transform);
            break;
        case no_field:
            {
                OwnedHqlExpr target = createSelectExpr(LINK(self), LINK(cur));
                IHqlExpression * value = queryTransformAssignValue(transform, cur);
                assertex(value);
                if (value->isConstant())
                    buildAssign(ctx, target, value);
                else
                    buildClear(ctx, target);
                break;
            }
        case no_ifblock:
            throwUnexpected();
        }
    }
}

void HqlCppTranslator::doBuildAggregateClearFunc(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * tgtRecord = expr->queryChild(1);
    IHqlExpression * transform = expr->queryChild(2);

    MemberFunction func(*this, ctx, "virtual size32_t clearAggregate(ARowBuilder & crSelf)");

    OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(tgtRecord));
    BoundRow * selfRow = bindSelf(func.ctx, resultDataset, "crSelf");

    if (!isKnownTransform(transform))
    {
        OwnedHqlExpr clearCall = createClearRowCall(func.ctx, selfRow);
        func.ctx.addReturn(clearCall);
        return;
    }

    ensureRowAllocated(func.ctx, "crSelf");

    doBuildClearAggregateRecord(func.ctx, transform->queryRecord(), selfRow->querySelector(), transform);
    buildReturnRecordSize(func.ctx, selfRow);
}


void HqlCppTranslator::doBuildAggregateFirstFunc(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * tgtRecord = expr->queryChild(1);

    MemberFunction func(*this, ctx, "virtual size32_t processFirst(ARowBuilder & crSelf, const void * _src)");
    ensureRowAllocated(func.ctx, "crSelf");
    func.ctx.addQuotedLiteral("unsigned char * src = (unsigned char *) _src;");

    //NOTE: no_throughaggregate recordof(expr) != tgtRecord => we need to create a temporary dataset
    OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(tgtRecord));
    BoundRow * selfRow = bindSelf(func.ctx, resultDataset, "crSelf");
    bindTableCursor(func.ctx, dataset, "src");

    doBuildAggregateProcessTransform(func.ctx, selfRow, expr, queryBoolExpr(false));
    buildReturnRecordSize(func.ctx, selfRow);
}

void HqlCppTranslator::doBuildAggregateNextFunc(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * tgtRecord = expr->queryChild(1);

    MemberFunction func(*this, ctx, "virtual size32_t processNext(ARowBuilder & crSelf, const void * _src)");
    //no need ensureRowAllocated(func.ctx, "crSelf");
    func.ctx.addQuotedLiteral("unsigned char * src = (unsigned char *) _src;");

    OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(tgtRecord));
    BoundRow * selfRow = bindSelf(func.ctx, resultDataset, "crSelf");
    bindTableCursor(func.ctx, dataset, "src");

    doBuildAggregateProcessTransform(func.ctx, selfRow, expr, queryBoolExpr(true));
    buildReturnRecordSize(func.ctx, selfRow);
}


void HqlCppTranslator::doBuildAggregateProcessTransform(BuildCtx & ctx, BoundRow * selfRow, IHqlExpression * expr, IHqlExpression * alreadyDoneExpr)
{
    bool alwaysFirstRow = matchesBoolean(alreadyDoneExpr, false);
    bool alwaysNextRow = matchesBoolean(alreadyDoneExpr, true);
    OwnedHqlExpr notAlreadyDone = getInverse(alreadyDoneExpr);

    IHqlExpression * transform = expr->queryChild(2);
    unsigned numAggregates = transform->numChildren();
    unsigned idx;
    bool isVariableOffset = false;
    bool isDynamicOffset = false;
    OwnedHqlExpr self = getSelf(expr->queryChild(1));
    for (idx = 0; idx < numAggregates; idx++)
    {
        IHqlExpression * cur = transform->queryChild(idx);
        if (cur->isAttribute())
            continue;

        OwnedHqlExpr target = selfRow->bindToRow(cur->queryChild(0), self);

        IHqlExpression * src = cur->queryChild(1);
        IHqlExpression * arg = src->queryChild(0);
        IHqlExpression * cond = queryRealChild(src, 1);

        BuildCtx condctx(ctx);
        node_operator srcOp = src->getOperator();
        switch (srcOp)
        {
        case no_countgroup:
            {
                //This could be supported in more situations - e.g. if always/neverFirstRow.
                if (arg && isVariableOffset)
                    throwError1(HQLERR_ConditionalAggregateVarOffset, str(target->queryChild(1)->queryId()));
                if (arg)
                    buildFilter(condctx, arg);
                OwnedHqlExpr one = createConstant(createIntValue(1,8,true));
                if (alwaysFirstRow)
                {
                    buildAssign(condctx, target, one);
                }
                else
                {
                    if (!alwaysNextRow && isVariableOffset)
                    {
                        IHqlStmt * ifStmt = condctx.addFilter(notAlreadyDone);
                        buildAssign(condctx, target, one);
                        condctx.selectElse(ifStmt);
                    }
                    buildIncrementAssign(condctx, target, one);
                }
            }
            break;
        case no_sumgroup:
            {
                if (cond && isVariableOffset)
                    throwError1(HQLERR_ConditionalAggregateVarOffset, str(target->queryChild(1)->queryId()));
                if (cond)
                    buildFilter(condctx, cond);
                if (alwaysFirstRow)
                {
                    buildAssign(condctx, target, arg);
                }
                else
                {
                    if (!alwaysNextRow && isVariableOffset)
                    {
                        IHqlStmt * ifStmt = condctx.addFilter(notAlreadyDone);
                        buildAssign(condctx, target, arg);
                        condctx.selectElse(ifStmt);
                    }
                    OwnedITypeInfo type = getPromotedECLType(target->queryType(), arg->queryType());
                    buildIncrementAssign(condctx, target, arg);
                }
            }
            break;
        case no_maxgroup:
        case no_mingroup:
            {
                node_operator compareOp = (srcOp == no_maxgroup) ? no_gt : no_lt;
                assertex(!cond);
                OwnedHqlExpr castArg = ensureExprType(arg, target->queryType());        // cast to correct type, assume it can fit in the target type.
                if (!alwaysFirstRow)
                {
                    castArg.setown(buildSimplifyExpr(condctx, castArg));
                    OwnedHqlExpr compare = createBoolExpr (compareOp, LINK(castArg), LINK(target));
                    if (!alwaysNextRow)
                        compare.setown(createBoolExpr(no_or, LINK(notAlreadyDone), LINK(compare)));
                    buildFilter(condctx, compare);
                }
                buildAssign(condctx, target, castArg);
            }
            break;
        case no_existsgroup:
            if (arg && isVariableOffset)
                throwError1(HQLERR_ConditionalAggregateVarOffset, str(target->queryChild(1)->queryId()));
            cond = arg;
            if (cond || !alwaysNextRow)
            {
                //The assign is conditional because unconditionally it is done in the AggregateFirst
                if (cond)
                    buildFilter(condctx, cond);
                buildAssign(condctx, target, queryBoolExpr(true));
            }
            break;
        default:
            if (!src->isConstant() || isVariableOffset)
            {
                if (!alwaysNextRow)
                {
                    if (!alwaysFirstRow)
                        buildFilter(condctx, notAlreadyDone);
                    buildAssign(condctx, target, src);
                }
            }
            break;
        }

        if (isDynamicOffset)
            throwError1(HQLERR_AggregateDynamicOffset, str(target->queryChild(1)->queryId()));

        if (target->queryType()->getSize() == UNKNOWN_LENGTH)
        {
           isVariableOffset = true;
           if (src->isGroupAggregateFunction())
               isDynamicOffset = true;
        }
    }
}

void HqlCppTranslator::doBuildAggregateMergeFunc(BuildCtx & ctx, IHqlExpression * expr, bool & requiresOrderedMerge)
{
    if (expr->getOperator() == no_aggregate)
    {
        OwnedHqlExpr mergeTransform = getUserAggregateMergeTransform(expr, requiresOrderedMerge);
        doBuildUserMergeAggregateFunc(ctx, expr, mergeTransform);
        return;
    }

    IHqlExpression * tgtRecord = expr->queryChild(1);
    IHqlExpression * transform = expr->queryChild(2);

    requiresOrderedMerge = false;
    OwnedHqlExpr selSeq = createDummySelectorSequence();
    MemberFunction func(*this, ctx, "virtual size32_t mergeAggregate(ARowBuilder & crSelf, const void * _right)");
    //ensureRowAllocated(func.ctx, "crSelf"); must be non null
    func.ctx.addQuotedLiteral("unsigned char * right = (unsigned char *) _right;");

    OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(tgtRecord));
    BoundRow * selfRow = bindSelf(func.ctx, resultDataset, "crSelf");
    BoundRow * rightCursor = bindTableCursor(func.ctx, resultDataset, "right", no_right, selSeq);

    unsigned numAggregates = transform->numChildren();
    unsigned idx;
    IHqlExpression * right = rightCursor->querySelector();
    OwnedHqlExpr self = getSelf(tgtRecord);
    for (idx = 0; idx < numAggregates; idx++)
    {
        IHqlExpression * cur = transform->queryChild(idx);
        if (cur->isAttribute())
            continue;

        OwnedHqlExpr target = selfRow->bindToRow(cur->queryChild(0), self);
        OwnedHqlExpr src = replaceSelector(cur->queryChild(0), self, right);
        IHqlExpression * op = cur->queryChild(1);

        //MORE: How bind cursors...
        switch (op->getOperator())
        {
        case no_countgroup:
        case no_sumgroup:
            {
                buildIncrementAssign(func.ctx, target, src);
            }
            break;
        case no_maxgroup:
            {
                OwnedHqlExpr compare = createBoolExpr (no_gt, LINK(src), LINK(target));
                BuildCtx filteredctx(func.ctx);
                buildFilter(filteredctx, compare);
                buildAssign(filteredctx, target, src);
            }
            break;
        case no_mingroup:
            {
                OwnedHqlExpr compare = createBoolExpr (no_lt, LINK(src), LINK(target));
                BuildCtx filteredctx(func.ctx);
                buildFilter(filteredctx, compare);
                buildAssign(filteredctx, target, src);
            }
            break;
        case no_existsgroup:
            {
                BuildCtx condctx(func.ctx);
                buildFilter(condctx, src);
                buildAssign(condctx, target, queryBoolExpr(true));
                break;
            }
        default:
            //already filled in and wouldn't be legal to have an expression in this case anyway...
            break;
        }
    }
    buildReturnRecordSize(func.ctx, selfRow);
}

//--------------------------------------------------------------------------------------
// User aggregate helpers
//--------------------------------------------------------------------------------------

void HqlCppTranslator::doBuildUserAggregateProcessTransform(BuildCtx & ctx, BoundRow * selfRow, IHqlExpression * expr, IHqlExpression * transform, IHqlExpression * alreadyDoneExpr)
{
    bool alwaysFirstRow = matchesBoolean(alreadyDoneExpr, false);
    bool alwaysNextRow = matchesBoolean(alreadyDoneExpr, true);
    OwnedHqlExpr right = createSelector(no_right, expr, querySelSeq(expr));
    bool usesRight = exprReferencesDataset(transform, right);

    BuildCtx condctx(ctx);
    if (!isKnownTransform(transform))
    {
        if (!alwaysNextRow)
        {
            IHqlStmt * ifStmt = NULL;
            if (!alwaysFirstRow)
                ifStmt = condctx.addFilter(alreadyDoneExpr);

            if (usesRight)
            {
                CHqlBoundExpr boundNullRow;
                buildDefaultRow(condctx, transform, boundNullRow);
                bindTableCursor(condctx, transform->queryRecord(), boundNullRow.expr, no_right, querySelSeq(expr));
            }

            doUserTransform(condctx, transform, selfRow);

            if (ifStmt)
                condctx.selectElse(ifStmt);
        }

        if (!alwaysFirstRow)
        {
            if (usesRight)
                bindTableCursor(condctx, transform->queryRecord(), selfRow->queryBound(), no_right, querySelSeq(expr));

            doUserTransform(condctx, transform, selfRow);
        }
    }
    else
    {
        if (usesRight)
        {
            BoundRow * rightCursor;
            if (alwaysNextRow)
                rightCursor = bindTableCursor(condctx, transform->queryRecord(), selfRow->queryBound(), no_right, querySelSeq(expr));
            else
            {
                CHqlBoundExpr boundNullRow;
                buildDefaultRow(condctx, transform, boundNullRow);

                if (alwaysFirstRow)
                {
                    //MORE: Only do this (and create default row) if transform refers to RIGHT...
                    rightCursor = bindTableCursor(condctx, transform->queryRecord(), boundNullRow.expr, no_right, querySelSeq(expr));
                }
                else
                {
                    //create a temporary
                    Owned<ITypeInfo> rowType = makeReferenceModifier(expr->getType());
                    OwnedHqlExpr rowExpr = ctx.getTempDeclare(rowType, NULL);
                    OwnedHqlExpr defaultRowPtr = getPointer(boundNullRow.expr);
                    OwnedHqlExpr condRow = createValue(no_if, LINK(rowType), LINK(alreadyDoneExpr), LINK(selfRow->queryBound()), LINK(defaultRowPtr));
                    condctx.addAssign(rowExpr, condRow);
                    rightCursor = bindTableCursor(condctx, transform->queryRecord(), rowExpr, no_right, querySelSeq(expr));
                }
            }

            if (alwaysFirstRow)
                doTransform(condctx, transform, selfRow);
            else
                doUpdateTransform(condctx, transform, selfRow, rightCursor, alwaysNextRow);
        }
        else
            doTransform(condctx, transform, selfRow);
    }
}

//------------------------------------------------------------------------------------------------

static bool matchesSelect(IHqlExpression * expr, IHqlExpression * selector, IHqlExpression * field)
{
    if (isCast(expr))
    {
        ITypeInfo * afterType = expr->queryType();
        ITypeInfo * beforeType = expr->queryChild(0)->queryType();
        if (preservesValue(afterType, beforeType))
            expr = expr->queryChild(0);
        else if (beforeType->isInteger() && afterType->isInteger() && beforeType->getSize() == afterType->getSize())
            expr = expr->queryChild(0);
    }
    if (expr->getOperator() != no_select)
        return false;
    if (expr->queryChild(0) != selector)
        return false;
    if (expr->queryChild(1) != field)
        return false;
    return true;
}


//MORE: Derive a merge transform by walking and spotting self.x := self.x := right.x op a or a op right.x
class MergeTransformCreator
{
public:
    MergeTransformCreator(IHqlExpression * expr)
    {
        IHqlExpression * dataset = expr->queryChild(0);
        IHqlExpression * selSeq = querySelSeq(expr);
        IHqlExpression * rowsid = expr->queryAttribute(_rowsid_Atom);
        self.setown(getSelf(expr));
        left.setown(createSelector(no_left, dataset, selSeq));
        right.setown(createSelector(no_right, expr, selSeq));
        mergeLeft.set(right);
        OwnedHqlExpr rows = createDataset(no_rows, LINK(right), LINK(rowsid));
        mergeRight.setown(createRow(no_selectnth, LINK(rows), createConstant(2)));
        requiresOrderedMerge = false;
    }

    IHqlExpression * transform(IHqlExpression * expr);
    inline bool isOrdered() { return requiresOrderedMerge; }

protected:
    IHqlExpression * transformAssign(IHqlExpression * expr);

protected:
    OwnedHqlExpr self;
    OwnedHqlExpr left;
    OwnedHqlExpr right;
    OwnedHqlExpr mergeLeft;
    OwnedHqlExpr mergeRight;
    bool requiresOrderedMerge;
};

//self.x := right.x append|concat f() -> self.x := right.x <op> rows(right)[2].x
//self.x := f() append|concat right.x -> self.x := rows(right)[2].x <op> right.x
//self.x := right.x [+|band|bor|max|min|xor] f() -> self.x := right.x <op> rows(right)[2].x
//self.x := f() [+|band|bor|max|min|xor] right.x -> self.x := right.x <op> rows(right)[2].x

IHqlExpression * MergeTransformCreator::transformAssign(IHqlExpression * expr)
{
    IHqlExpression * lhs = expr->queryChild(0);
    IHqlExpression * rhs = expr->queryChild(1);
    IHqlExpression * field = lhs->queryChild(1);

    bool commutative = true;
    node_operator op = rhs->getOperator();
    HqlExprArray args;
    switch (op)
    {
    case no_concat:
    case no_addfiles:
        commutative = false;
        rhs->unwindList(args, op);
        break;
    case no_add:
    case no_band:
    case no_bor:
    case no_bxor:
    case no_mul:
        rhs->unwindList(args, op);
        break;
    case no_sumlist:
    case no_maxlist:
    case no_minlist:
        {
            IHqlExpression * list = rhs->queryChild(0);
            if (list->getOperator() != no_list)
                return NULL;
            unwindChildren(args, list);
            break;
        }
    default:
        //ok, if it is only assigned once.
        if (exprReferencesDataset(rhs, right))
            return NULL;
        //need to preserve the value if at a variable offset...  Should be stripped if unnecessary
        OwnedHqlExpr newRhs = createSelectExpr(LINK(mergeLeft), LINK(field));
        return createAssign(LINK(lhs), newRhs.getClear());
    }

    unsigned matchPos = NotFound;
    ForEachItemIn(i, args)
    {
        IHqlExpression & cur = args.item(i);
        if (exprReferencesDataset(&cur, right))
        {
            if ((matchPos == NotFound) && matchesSelect(&cur, right, field))
                matchPos = i;
            else
                return NULL;
        }
    }
    if (matchPos == NotFound)
        return NULL;

    if (!commutative)
        requiresOrderedMerge = true;

    HqlExprArray newArgs;
    if (commutative || (matchPos == 0))
    {
        newArgs.append(*createSelectExpr(LINK(mergeLeft), LINK(field)));
        newArgs.append(*createSelectExpr(LINK(mergeRight), LINK(field)));
    }
    else if (matchPos == args.ordinality() - 1)
    {
        newArgs.append(*createSelectExpr(LINK(mergeRight), LINK(field)));
        newArgs.append(*createSelectExpr(LINK(mergeLeft), LINK(field)));
    }

    if (newArgs.ordinality())
    {
        switch (op)
        {
        case no_sumlist:
        case no_minlist:
        case no_maxlist:
            {
                OwnedHqlExpr list = rhs->queryChild(0)->clone(newArgs);
                newArgs.kill();
                newArgs.append(*list.getClear());
                break;
            }
        }
        return createAssign(LINK(lhs), rhs->clone(newArgs));
    }
    return NULL;
}

IHqlExpression * MergeTransformCreator::transform(IHqlExpression * expr)
{
    HqlExprArray children;
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        OwnedHqlExpr mapped;
        switch (cur->getOperator())
        {
        case no_assign:
            mapped.setown(transformAssign(cur));
            if (!mapped)
                return NULL;
            break;
        case no_assignall:
            mapped.setown(transform(cur));
            if (!mapped)
                return NULL;
            break;
        case no_alias_scope:
            //Not so sure - there shouldn't be any at this point...  Maybe we should allow...
            return NULL;
        case no_assert:
        case no_skip:
            return NULL;
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            break;
        default:
            UNIMPLEMENTED;
        }
        if (mapped)
            children.append(*mapped.getClear());
    }

    return expr->clone(children);
}

//------------------------------------------------------------------------------------------------

// Replace SELF.x := F(LEFT) with SELF.x := RIGHT.x.
//i) To ensure the first value is always used and 
//ii) to allow subsequent optimizations to remove the assignment
class NextTransformCreator
{
public:
    NextTransformCreator(IHqlExpression * expr)
    {
        IHqlExpression * dataset = expr->queryChild(0);
        IHqlExpression * selSeq = querySelSeq(expr);
        self.setown(getSelf(expr));
        left.setown(createSelector(no_left, dataset, selSeq));
        right.setown(createSelector(no_right, expr, selSeq));
    }

    IHqlExpression * transform(IHqlExpression * expr);

protected:
    IHqlExpression * transformAssign(IHqlExpression * expr);

protected:
    OwnedHqlExpr self;
    OwnedHqlExpr left;
    OwnedHqlExpr right;
};

IHqlExpression * NextTransformCreator::transformAssign(IHqlExpression * expr)
{
    IHqlExpression * lhs = expr->queryChild(0);
    IHqlExpression * rhs = expr->queryChild(1);
    IHqlExpression * field = lhs->queryChild(1);

    if (!rhs->isConstant() && !exprReferencesDataset(rhs, right))
    {
        OwnedHqlExpr newRhs = createSelectExpr(LINK(right), LINK(field));
        return createAssign(LINK(lhs), newRhs.getClear());
    }
    return LINK(expr);
}

IHqlExpression * NextTransformCreator::transform(IHqlExpression * expr)
{
    HqlExprArray children;
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        OwnedHqlExpr mapped = LINK(cur);
        switch (cur->getOperator())
        {
        case no_assign:
            mapped.setown(transformAssign(cur));
            break;
        case no_assignall:
            mapped.setown(transform(cur));
            break;
        }
        children.append(*mapped.getClear());
    }

    return expr->clone(children);
}

//------------------------------------------------------------------------------------------------

void HqlCppTranslator::processUserAggregateTransform(IHqlExpression * expr, IHqlExpression * transform, SharedHqlExpr & firstTransform, SharedHqlExpr & nextTransform)
{
    if (isKnownTransform(transform))
    {
        OwnedHqlExpr right = createSelector(no_right, expr, querySelSeq(expr));
        OwnedHqlExpr nullRow = createRow(no_newrow, createRow(no_null, LINK(expr->queryRecord())));
        firstTransform.setown(replaceSelector(transform, right, nullRow));
        firstTransform.setown(foldHqlExpression(firstTransform));
    }
    else
        firstTransform.set(transform);

    if (isKnownTransform(transform))
    {
        {
            NextTransformCreator builder(expr);
            nextTransform.setown(builder.transform(transform));
        }
    }
    else
        nextTransform.set(transform);

}

IHqlExpression * HqlCppTranslator::getUserAggregateMergeTransform(IHqlExpression * expr, bool & requiresOrderedMerge)
{
    IHqlExpression * mergeTransform = queryAttributeChild(expr, mergeTransformAtom, 0);
    if (mergeTransform)
    {
        requiresOrderedMerge = true;
        return LINK(mergeTransform);
    }

    IHqlExpression * transform = expr->queryChild(2);
    if (!isKnownTransform(transform))
        return NULL;

    MergeTransformCreator builder(expr);
    OwnedHqlExpr ret = builder.transform(transform);
    requiresOrderedMerge = builder.isOrdered();
    return ret.getClear();
}


void HqlCppTranslator::doBuildUserMergeAggregateFunc(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * mergeTransform)
{
    IHqlExpression * selSeq = querySelSeq(expr);
    if (!mergeTransform)
        throwError(HQLERR_AggregateNeedMergeTransform);

    IHqlExpression * rowsid = expr->queryAttribute(_rowsid_Atom);

    MemberFunction func(*this, ctx, "virtual size32_t mergeAggregate(ARowBuilder & upRight1, const void * _right2)");
    func.ctx.addQuotedLiteral("unsigned char * right2 = (unsigned char *) _right2;");
    BoundRow * rightCursor = bindTableCursor(func.ctx, expr, "upRight1.row()", no_right, selSeq);
    BoundRow * leftCursor = bindTableCursor(func.ctx, expr, "right2", no_left, selSeq);
    BoundRow * selfCursor = bindSelf(func.ctx, expr, "upRight1");

    //Ugly!!  RIGHT:2 is represented as ROWS(RIGHT)[2].  Change it to no_left since it will make like cleaner
    //If we fail to replace all references then fall back to the worse case generation
    OwnedHqlExpr rows = createDataset(no_rows, LINK(rightCursor->querySelector()), LINK(rowsid));
    OwnedHqlExpr complexRight1 = createRow(no_selectnth, LINK(rows), createConstant(1));
    OwnedHqlExpr complexRight2 = createRow(no_selectnth, LINK(rows), createConstant(2));
    OwnedHqlExpr mappedTransform1 = removeAnnotations(mergeTransform, rows);
    OwnedHqlExpr mappedTransform2 = replaceExpression(mappedTransform1, complexRight1, rightCursor->querySelector());
    OwnedHqlExpr mappedTransform3 = replaceExpression(mappedTransform2, complexRight2, leftCursor->querySelector());
    if (containsExpression(mappedTransform3, rows))
    {
        //Worse case fallback behaviour.  Shouldn't occur in practice.
        //If ROWS() really is needed due to some weird expression then we need to clone the target/RIGHT1 row because
        //any modification may cause it to relocate and so invalidate the pointer passed in.
        IHqlExpression * leftBound = leftCursor->queryBound();
        ITypeInfo * rowType = leftBound->queryType();
        CHqlBoundExpr boundRow1;
        buildTempExpr(func.ctx, rightCursor->querySelector(), boundRow1);
        OwnedHqlExpr rows = createVariable("rows", makeArrayType(LINK(rowType), 2));
        OwnedHqlExpr initializer = createValue(no_list, makeSetType(LINK(rowType)), LINK(boundRow1.expr), LINK(leftBound));
        func.ctx.addDeclare(rows, initializer);
        bindRows(func.ctx, no_right, selSeq, rowsid, expr, "2", "rows", options.mainRowsAreLinkCounted);
    }

    doUpdateTransform(func.ctx, mappedTransform3, selfCursor, rightCursor, true);
    buildReturnRecordSize(func.ctx, selfCursor);
}

void HqlCppTranslator::doBuildUserAggregateFuncs(BuildCtx & ctx, IHqlExpression * expr, bool & requiresOrderedMerge)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * transform = expr->queryChild(2);
    IHqlExpression * selSeq = querySelSeq(expr);
    LinkedHqlExpr firstTransform;
    LinkedHqlExpr nextTransform;

    processUserAggregateTransform(expr, transform, firstTransform, nextTransform);

    {
        MemberFunction func(*this, ctx, "virtual size32_t processFirst(ARowBuilder & crSelf, const void * _src)");
        ensureRowAllocated(func.ctx, "crSelf");
        func.ctx.addQuotedLiteral("unsigned char * src = (unsigned char *) _src;");

        BoundRow * selfRow = bindSelf(func.ctx, expr, "crSelf");
        bindTableCursor(func.ctx, dataset, "src", options.mainRowsAreLinkCounted, no_left, selSeq);

        doBuildUserAggregateProcessTransform(func.ctx, selfRow, expr, firstTransform, queryBoolExpr(false));
        buildReturnRecordSize(func.ctx, selfRow);
    }

    {
        MemberFunction func(*this, ctx, "virtual size32_t processNext(ARowBuilder & crSelf, const void * _src)");
        ensureRowAllocated(func.ctx, "crSelf");
        func.ctx.addQuotedLiteral("unsigned char * src = (unsigned char *) _src;");

        BoundRow * selfCursor = bindSelf(func.ctx, expr, "crSelf");
        bindTableCursor(func.ctx, dataset, "src", options.mainRowsAreLinkCounted, no_left, selSeq);
        bindTableCursor(func.ctx, expr, "crSelf.row()", no_right, selSeq);

        doBuildUserAggregateProcessTransform(func.ctx, selfCursor, expr, nextTransform, queryBoolExpr(true));
        buildReturnRecordSize(func.ctx, selfCursor);
    }

    if (targetThor() && !isGrouped(dataset) && !expr->hasAttribute(localAtom))
    {
        OwnedHqlExpr mergeTransform = getUserAggregateMergeTransform(expr, requiresOrderedMerge);
        doBuildUserMergeAggregateFunc(ctx, expr, mergeTransform);
    }
}

//--------------------------------------------------------------------------------------


void getMappedFields(HqlExprArray & aggregateFields, IHqlExpression * transform, HqlExprArray & recordFields, IHqlExpression * newSelector)
{
    unsigned numFields = transform->numChildren();

    OwnedHqlExpr self = getSelf(transform);
    ForEachItemIn(idx, recordFields)
    {
        IHqlExpression & cur = recordFields.item(idx);

        unsigned fieldIdx;
        for (fieldIdx = 0; fieldIdx < numFields; fieldIdx++)
        {
            IHqlExpression * curAssign = transform->queryChild(fieldIdx);
            IHqlExpression * rhs = curAssign->queryChild(1);
            if (rhs->getOperator() == no_activerow)
                rhs = rhs->queryChild(0);
            if (rhs == &cur)
            {
                aggregateFields.append(*replaceSelector(curAssign->queryChild(0), self, newSelector));
                break;
            }
        }
        if (fieldIdx == numFields)
        {
            StringBuffer s;
            getExprECL(&cur, s);
            throwError1(HQLERR_AggregateMissingGroupingFields, s.str());
        }
    }
}


ABoundActivity * HqlCppTranslator::doBuildActivityAggregate(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    node_operator op = expr->getOperator();
    IHqlExpression * transform = expr->queryChild(2);
    LinkedHqlExpr grouping = queryRealChild(expr, 3);
    bool passThrough = (op == no_throughaggregate);

    if (passThrough)
        grouping.clear();

    unsigned tableType = dataset->queryType()->getTypeCode();
    const char *activity;
    ThorActivityKind kind = TAKaggregate;
    node_operator specialOp = no_none;
    IIdAtom * implementationClassId = NULL;
    if (passThrough)
    {
        activity = "ThroughAggregate";
        kind = TAKthroughaggregate;
    }
    else if (grouping)
    {
        activity = "HashAggregate";
        kind = TAKhashaggregate;
    }
    else if (targetThor() && (tableType == type_groupedtable))
        activity = "GroupAggregate";
    else
    {
        activity = "Aggregate";
        specialOp = querySimpleAggregate(expr, false, false);
        if (specialOp == no_existsgroup)
        {
            kind = TAKexistsaggregate;
            activity = "ExistsAggregate";
            if (options.minimizeActivityClasses)
                implementationClassId = newExistsAggregateArgId;
        }
        else if (specialOp == no_countgroup)
        {
            kind = TAKcountaggregate;
            activity = "CountAggregate";
            if (options.minimizeActivityClasses)
                implementationClassId = newCountAggregateArgId;
        }
        else
            specialOp = no_none;
    }


    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, activity);
    if (implementationClassId)
        instance->setImplementationClass(implementationClassId);
    if (passThrough)
    {
        StringBuffer graphLabel;
        graphLabel.append("Through Aggregate");
        ForEachChild(idx, expr)
        {
            IHqlExpression * cur = expr->queryChild(idx);
            if (cur->getOperator() == no_setresult || cur->getOperator() == no_extractresult)
            {
                IHqlExpression * sequence = queryAttributeChild(cur, sequenceAtom, 0);
                IHqlExpression * name = queryAttributeChild(cur, namedAtom, 0);
                getStoredDescription(graphLabel.append("\n"), sequence, name, false);
            }
        }
        instance->graphLabel.set(graphLabel.str());
    }

    if (isKeyedCountAggregate(expr))
        throwError(HQLERR_KeyedCountNonKeyable);

    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    StringBuffer flags;
    bool requiresOrderedMerge = false;
    if (specialOp == no_none)
    {
        doBuildAggregateClearFunc(instance->startctx, expr);
        if (op == no_aggregate)
        {
            doBuildUserAggregateFuncs(instance->startctx, expr, requiresOrderedMerge);
        }
        else
        {
            doBuildAggregateFirstFunc(instance->startctx, expr);
            doBuildAggregateNextFunc(instance->startctx, expr);
            doBuildAggregateMergeFunc(instance->startctx, expr, requiresOrderedMerge);
        }
    }

    if (requiresOrderedMerge)
        flags.append("|TAForderedmerge");
    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getAggregateFlags", flags.str()+1);

    if (grouping)
    {
        HqlExprArray recordFields, aggregateFields;
        grouping->unwindList(recordFields, no_sortlist);
        getMappedFields(aggregateFields, transform, recordFields, queryActiveTableSelector());

        DatasetReference inputRef(dataset, (op == no_aggregate) ? no_left : no_none, querySelSeq(expr));
        DatasetReference outRef(expr, no_activetable, NULL);

        OwnedHqlExpr allRecordFields = createValueSafe(no_sortlist, makeSortListType(NULL), recordFields);
        OwnedHqlExpr allAggregateFields = createValueSafe(no_sortlist, makeSortListType(NULL), aggregateFields);
        buildCompareMember(instance->nestedctx, "CompareElements", allAggregateFields, outRef);     // compare transformed elements
        doCompareLeftRight(instance->nestedctx, "CompareRowElement", inputRef, outRef, recordFields, aggregateFields);
        buildHashOfExprsClass(instance->nestedctx, "Hash", allRecordFields, inputRef, true);
        buildHashOfExprsClass(instance->nestedctx, "HashElement", allAggregateFields, outRef, true);
    }
    if (passThrough)
    {
        MemberFunction func(*this, instance->startctx, "virtual void sendResult(const void * _self)");
        func.ctx.addQuotedLiteral("const unsigned char * self = (const unsigned char *)_self;");

        OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(expr->queryChild(1)), NULL);
        bindTableCursor(func.ctx, resultDataset, "self");
        bindTableCursor(func.ctx, dataset, "self");
        ForEachChild(idx, expr)
        {
            IHqlExpression * cur = expr->queryChild(idx);
            if (cur->getOperator() == no_setresult)
            {
                IHqlExpression * value = cur->queryChild(0);
                IHqlExpression * sequence = queryAttributeChild(cur, sequenceAtom, 0);
                IHqlExpression * name = queryAttributeChild(cur, namedAtom, 0);
                buildSetResultInfo(func.ctx, cur, value, NULL, false, false);
                associateRemoteResult(*instance, sequence, name);
            }
            else if (cur->getOperator() == no_extractresult)
            {
                IHqlExpression * value = cur->queryChild(1);
                IHqlExpression * sequence = queryAttributeChild(cur, sequenceAtom, 0);
                IHqlExpression * name = queryAttributeChild(cur, namedAtom, 0);
                buildSetResultInfo(func.ctx, cur, value, NULL, false, false);
                associateRemoteResult(*instance, sequence, name);
            }
        }
        buildMetaMember(instance->classctx, resultDataset, isGrouped(resultDataset), "queryAggregateRecordSize");
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

static bool isDistributedFunctionCall(IHqlExpression * expr)
{
    IHqlExpression * funcdef = NULL;
    switch (expr->getOperator())
    {
    case no_externalcall:
        funcdef = expr->queryBody()->queryExternalDefinition();
        break;
    case no_call:
        funcdef = expr->queryBody()->queryFunctionDefinition();
        break;
    }
    return (funcdef && queryFunctionAttribute(funcdef, distributedAtom));
}

ABoundActivity * HqlCppTranslator::doBuildActivityChildDataset(BuildCtx & ctx, IHqlExpression * expr)
{
    if (options.mainRowsAreLinkCounted || isGrouped(expr))
        return doBuildActivityLinkedRawChildDataset(ctx, expr);

    StringBuffer s;

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKchilditerator, expr, "ChildIterator");
    if (isDistributedFunctionCall(expr))
        instance->setLocal(true);
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    //Dummy implementation that just clears the record
    OwnedHqlExpr value = LINK(expr);
    switch (value->getOperator())
    {
    case no_alias:
    case no_left:
    case no_right:
    case no_top:
    case no_id2blob:
    case no_rows:
    case no_getgraphresult:
    case no_activetable:
        break;
    case no_select:
        if (!isNewSelector(expr))
            break;
        //fall through
    default:
        {
            CHqlBoundExpr bound;
            doBuildAliasValue(instance->onstartctx, value, bound, NULL);
            value.setown(bound.getTranslatedExpr());
            break;
        }
    }

    Owned<IHqlCppDatasetCursor> iter = createDatasetSelector(instance->onstartctx, value);
    iter->buildIterateMembers(instance->startctx, instance->onstartctx);

    //virtual size32_t transform(ARowBuilder & crSelf) = 0;
    {
        MemberFunction func(*this, instance->startctx, "size32_t transform(ARowBuilder & crSelf)");
        ensureRowAllocated(func.ctx, "crSelf");
        BoundRow * selfCursor = bindSelf(func.ctx, expr, "crSelf");
        OwnedHqlExpr active = ensureActiveRow(value);
        buildAssign(func.ctx, selfCursor->querySelector(), active);
        buildReturnRecordSize(func.ctx, selfCursor);
    }

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityStreamedCall(BuildCtx & ctx, IHqlExpression * expr)
{
    StringBuffer s;

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKstreamediterator, expr, "StreamedIterator");
    if (isDistributedFunctionCall(expr))
        instance->setLocal(true);
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    {
        MemberFunction func(*this, instance->startctx, "virtual IRowStream * createInput()");
        buildReturn(func.ctx, expr);
    }

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityLinkedRawChildDataset(BuildCtx & ctx, IHqlExpression * expr)
{
    StringBuffer s;

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKlinkedrawiterator, expr, "LinkedRawIterator");
    if (isDistributedFunctionCall(expr))
        instance->setLocal(true);
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    OwnedHqlExpr nonparallel = removeAttribute(expr, parallelAtom);
    OwnedHqlExpr value = expr->isDatarow() ? createDatasetFromRow(nonparallel.getClear()) : nonparallel.getClear();
    BuildCtx * declarectx;
    BuildCtx * callctx;
    instance->evalContext->getInvariantMemberContext(NULL, &declarectx, &callctx, false, true);     // possibly should sometimes generate in onCreate(), if can evaluate in parent

    CHqlBoundTarget invariantDs;
    buildTempExpr(*callctx, *declarectx, invariantDs, value, FormatLinkedDataset, false);
    CHqlBoundExpr boundDs;
    boundDs.setFromTarget(invariantDs);

    CHqlBoundTarget boundActiveIndex;
    OwnedHqlExpr zero = getSizetConstant(0);
    buildTempExpr(*callctx, *declarectx, boundActiveIndex, zero, FormatNatural, false);

    //virtual byte * next() = 0;
    {
        MemberFunction func(*this, instance->startctx, "virtual byte * next()");
        OwnedHqlExpr count = getBoundCount(boundDs);
        OwnedHqlExpr test = createValue(no_lt, makeBoolType(), LINK(boundActiveIndex.expr), LINK(count));
        BuildCtx subctx(func.ctx);
        subctx.addFilter(test);
        OwnedHqlExpr ret = createValue(no_index, expr->getType(), LINK(boundDs.expr), createValue(no_postinc, LINK(sizetType), LINK(boundActiveIndex.expr)));
        subctx.addReturn(ret);
        func.ctx.addReturn(queryQuotedNullExpr());
    }

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

static void gatherDedupCompareExpr(HqlExprArray & equalities, HqlExprArray & comparisons, HqlExprArray & conds, IHqlExpression * left, IHqlExpression * right, IHqlExpression * dataset)
{
    ForEachItemIn(idx, conds)
    {
        IHqlExpression * cond = &conds.item(idx);
        if (cond->getOperator() == no_and)
        {
            HqlExprArray expanded;
            cond->unwindList(expanded, no_and);
            gatherDedupCompareExpr(equalities, comparisons, expanded, left, right, dataset);
        }
        else if (containsSelector(cond, left) || containsSelector(cond, right))
            comparisons.append(*LINK(cond));
        else if (!cond->isConstant())
            equalities.append(*LINK(cond));
    }
}


void optimizeGroupOrder(HqlExprArray & optimized, IHqlExpression * dataset, HqlExprArray & exprs)
{
    RecordSelectIterator iter(dataset->queryRecord(), dataset->queryNormalizedSelector());
    ForEach(iter)
    {
        IHqlExpression * select = iter.query();
        unsigned match = exprs.find(*select);
        if (match != NotFound)
        {
            optimized.append(*LINK(select));
            //Remove this item, and all other matches (unusual, but if it does occur it is wasteful.)
            do
            {
                exprs.remove(match);
                match = exprs.find(*select);
            } while (match != NotFound);
            if (exprs.empty())
                break;
        }
    }
}

IHqlExpression * createOrderFromCompareArray(HqlExprArray & exprs, IHqlExpression * dataset, IHqlExpression * left, IHqlExpression * right)
{
    OwnedHqlExpr equalExpr = createSortList(exprs);
    OwnedHqlExpr lhs = replaceSelector(equalExpr, dataset, left);
    OwnedHqlExpr rhs = replaceSelector(equalExpr, dataset, right);
    return createValue(no_order, LINK(signedType), lhs.getClear(), rhs.getClear());
}


void HqlCppTranslator::buildDedupFilterFunction(BuildCtx & ctx, HqlExprArray & equalities, HqlExprArray & conds, IHqlExpression * dataset, IHqlExpression * selSeq)
{
    HqlExprArray comparisons;
    HqlExprArray allEqualities;

    //MORE: The equalities really shouldn't be here - the activities should do them via the primaryCompare
    appendArray(allEqualities, equalities);
    OwnedHqlExpr left = createSelector(no_left, dataset, selSeq);
    OwnedHqlExpr right = createSelector(no_right, dataset, selSeq);
    gatherDedupCompareExpr(allEqualities, comparisons, conds, left, right, dataset);

    MemberFunction func(*this, ctx, "virtual bool matches(const void * _left, const void * _right)");
    func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
    func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");

    BuildCtx filterctx(func.ctx);

    BoundRow * lRow = bindTableCursor(filterctx, dataset, "left", no_left, selSeq);
    BoundRow * rRow = bindTableCursor(filterctx, dataset, "right", no_right, selSeq);

    //convert any LEFT.x = RIGHT.y to an equality
    unsigned numComparisons = comparisons.ordinality();
    for (unsigned i1=0; i1 < numComparisons; )
    {
        IHqlExpression * cur = &comparisons.item(i1);
        if (cur->getOperator() == no_eq)
        {
            IHqlExpression * lhs = cur->queryChild(0);
            OwnedHqlExpr newLhs = replaceSelector(lhs, lRow->querySelector(), rRow->querySelector());
            if (newLhs == cur->queryChild(1))
            {
                allEqualities.append(*replaceSelector(lhs, lRow->querySelector(), dataset));
                comparisons.remove(i1);
                numComparisons--;
            }
            //could check for right.x = left.x, but pretty unlikely.
            else
                i1++;
        }
        else
            i1++;
    }

    if (comparisons.ordinality() || allEqualities.ordinality())
    {
        ForEachItemIn(i, comparisons)
        {
            IHqlExpression * cur = &comparisons.item(i);
            //if no equalities to follow, generate a return for the last non-equality
            if (allEqualities.empty() && (i+1 == numComparisons))
            {
                buildReturn(filterctx, cur);
            }
            else
            {
                OwnedHqlExpr inverse = getInverse(cur);
                buildFilteredReturn(filterctx, inverse, queryBoolExpr(false));
            }
        }

        if (allEqualities.ordinality())
        {
            HqlExprArray optimized;
            //Even better... sort the equality list by the field order...
            if (options.optimizeGrouping && (allEqualities.ordinality() > 1))
                optimizeGroupOrder(optimized, dataset, allEqualities);
            appendArray(optimized, allEqualities);

            OwnedHqlExpr order = createOrderFromCompareArray(optimized, dataset, lRow->querySelector(), rRow->querySelector());
            doBuildReturnCompare(filterctx, order, no_eq, true, false);
        }
    }
    else
        func.setIncluded(false);           // Use the implementation in the base class
}

void HqlCppTranslator::buildDedupSerializeFunction(BuildCtx & ctx, const char * funcName, IHqlExpression * srcDataset, IHqlExpression * tgtDataset, HqlExprArray & srcValues, HqlExprArray & tgtValues, IHqlExpression * selSeq)
{
    StringBuffer s;

    s.append("virtual unsigned ").append(funcName).append("(ARowBuilder & crSelf, const void * _src)");
    MemberFunction func(*this, ctx, s, MFdynamicproto);
    ensureRowAllocated(func.ctx, "crSelf");
    func.ctx.addQuotedLiteral("const unsigned char * src = (const unsigned char *) _src;");

    BoundRow * tgtCursor = bindSelf(func.ctx, tgtDataset, "crSelf");
    BoundRow * srcCursor = bindTableCursor(func.ctx, srcDataset, "src", no_left, selSeq);
    ForEachItemIn(idx2, srcValues)
    {
        Owned<IHqlExpression> self = tgtCursor->bindToRow(&tgtValues.item(idx2), queryActiveTableSelector());
        Owned<IHqlExpression> left = srcCursor->bindToRow(&srcValues.item(idx2), srcDataset);
        buildAssign(func.ctx, self, left);
    }

    buildReturnRecordSize(func.ctx, tgtCursor);
}

ABoundActivity * HqlCppTranslator::doBuildActivityDedup(BuildCtx & ctx, IHqlExpression * expr)
{
    StringBuffer s;
    DedupInfoExtractor info(expr);

    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * selSeq = querySelSeq(expr);
    bool isGrouped = ::isGrouped(dataset);
    bool isLocal = isLocalActivity(expr);
    bool useHash = expr->hasAttribute(hashAtom);
    if (targetThor() && !isGrouped && !isLocal)
    {
        //Should really be done via an attribute on the dedup.
        if (info.compareAllRows && !info.conds.ordinality())
            useHash = true;
    }

    if (useHash && info.conds.ordinality())
        throwError(HQLERR_GlobalDedupFuzzy);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, useHash ? TAKhashdedup : TAKdedup, expr, useHash ? "HashDedup" : "Dedup");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    bool wholeRecord = false;
    if (!useHash)
    {
        if (!matchesConstantValue(info.numToKeep, 1))
        {
            doBuildUnsignedFunction(instance->startctx, "numToKeep", info.numToKeep);
            if (info.keepBest)
                throwError(HQLERR_DedupBestWithKeepn);
        }

        //MORE: If input is grouped (pretty likely), then no need to include fields in the filter function that are already included.
        if (instance->isGrouped)
        {
            HqlExprArray normalizedEqualities;
            ForEachItemIn(i1, info.equalities)
                normalizedEqualities.append(*replaceSelector(info.equalities.item(i1).queryBody(), dataset, queryActiveTableSelector()));

            IHqlExpression * grouping = queryGrouping(dataset);
            ForEachChild(i, grouping)
            {
                IHqlExpression * curGroup = grouping->queryChild(i);
                unsigned match = normalizedEqualities.find(*curGroup);
                if (match != NotFound)
                {
                    normalizedEqualities.remove(match);
                    info.equalities.remove(match);
                }
            }
        }

        HqlExprArray noEqualities;
        HqlExprArray * equalities = &info.equalities;
        if (info.compareAllRows)
        {
            if (info.equalities.ordinality() && !instance->isGrouped)
            {
                OwnedHqlExpr order = createValueSafe(no_sortlist, makeSortListType(NULL), info.equalities);
                buildCompareMember(instance->nestedctx, "ComparePrimary", order, DatasetReference(dataset));
                if (!targetThor())
                    equalities = &noEqualities;
            }
        }

        buildDedupFilterFunction(instance->startctx, *equalities, info.conds, dataset, selSeq);
    }
    else
    {
        if (info.equalities.ordinality() == 0)
            throwError(HQLERR_GlobalDedupNoEquality);
        if (!matchesConstantValue(info.numToKeep, 1))
            throwError1(HQLERR_HashDedupNotSupportX, "KEEP");
        if (!info.keepLeft)
            throwError1(HQLERR_HashDedupNotSupportX, "RIGHT");

        OwnedHqlExpr order = createValueSafe(no_sortlist, makeSortListType(NULL), info.equalities);
        buildCompareMember(instance->nestedctx, "Compare", order, DatasetReference(dataset));
        buildHashOfExprsClass(instance->nestedctx, "Hash", order, DatasetReference(dataset), true);

        bool reuseCompare = false;
        HqlExprArray fields, selects;
        ForEachItemIn(idx, info.equalities)
        {
            IHqlExpression & cur = info.equalities.item(idx);
            IHqlExpression * field;
            if ((cur.getOperator() == no_select) && (cur.queryChild(0) == dataset->queryNormalizedSelector()))
                field = LINK(cur.queryChild(1));
            else
            {
                StringBuffer name;
                name.append("_expression_").append(idx);
                field = createFieldFromValue(createIdAtom(name.str()), &cur);
            }
            fields.append(*field);
            selects.append(*createSelectExpr(getActiveTableSelector(), LINK(field)));
        }

        OwnedHqlExpr keyDataset = createDataset(no_anon, createRecord(fields));

        //virtual IOutputMetaData * queryKeySize()
        buildMetaMember(instance->classctx, keyDataset, false, "queryKeySize");

        //virtual unsigned recordToKey(void * _key, const void * _record)
        buildDedupSerializeFunction(instance->startctx, "recordToKey", dataset, keyDataset, info.equalities, selects, selSeq);

        // Helper function relating to selecting a record from a set of "duplicate" records
        if (info.keepBest)
        {
            // KeepBest stores entire record (not just the key field) so the KeyCompare and queryRowKeyCompare is the same as Compare.
            reuseCompare = true;
        }
        else
        {
            //virtual ICompare * queryKeyCompare()
            OwnedHqlExpr keyOrder = createValueSafe(no_sortlist, makeSortListType(NULL), selects);
            if (recordTypesMatch(dataset, keyDataset))
            {
                OwnedHqlExpr globalOrder = replaceSelector(order, dataset, queryActiveTableSelector());
                if (keyOrder == globalOrder)
                    reuseCompare = true;
            }
            if (!reuseCompare)
            {
                buildCompareMember(instance->nestedctx, "KeyCompare", keyOrder, DatasetReference(keyDataset, no_activetable, NULL));
                buildHashOfExprsClass(instance->nestedctx, "KeyHash", keyOrder, DatasetReference(keyDataset, no_activetable, NULL), true);
                //virtual ICompare * queryRowKeyCompare()=0; // lhs is a row, rhs is a key
                doCompareLeftRight(instance->nestedctx, "RowKeyCompare", DatasetReference(dataset), DatasetReference(keyDataset, no_activetable, NULL), info.equalities, selects);
            }
            wholeRecord = recordTypesMatch(dataset, keyDataset);
        }
        if (reuseCompare)
        {
            instance->nestedctx.addQuotedLiteral("virtual ICompare * queryKeyCompare() { return &Compare; }");
            instance->nestedctx.addQuotedLiteral("virtual IHash * queryKeyHash() { return &Hash; }");
            instance->nestedctx.addQuotedLiteral("virtual ICompare * queryRowKeyCompare() { return &Compare; }");
        }
    }

    StringBuffer flags;
    if (info.compareAllRows) flags.append("|HDFcompareall");
    if (info.keepLeft)       flags.append("|HDFkeepleft");
    if (info.keepBest)       flags.append("|HDFkeepbest");
    if (wholeRecord)         flags.append("|HDFwholerecord");
    if (!streq(flags.str(), "|HDFkeepleft"))
    {
        if (flags.length()==0) flags.append("|0");
        doBuildUnsignedFunction(instance->startctx, "getFlags", flags.str()+1);
    }

    if (info.keepBest)
    {
        IHqlExpression * sortOrder = expr->queryAttribute(bestAtom)->queryChild(0);
        buildCompareFuncHelper(*this, *instance, "compareBest", sortOrder, DatasetReference(dataset));
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
// no_distribute

ABoundActivity * HqlCppTranslator::doBuildActivityDistribute(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    if (!targetThor() || insideChildQuery(ctx))
    {
        if (isGrouped(dataset))
        {
            Owned<ABoundActivity> boundInput = buildCachedActivity(ctx, dataset);
            return doBuildActivityUngroup(ctx, expr, boundInput);
        }
        return buildCachedActivity(ctx, dataset);
    }

    StringBuffer s;

    if (isUngroup(dataset))
        dataset = dataset->queryChild(0);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    IHqlExpression * cond = expr->queryChild(1);
    IHqlExpression * mergeOrder = queryAttributeChild(expr, mergeAtom, 0);
    if (cond->getOperator() == no_sortpartition)
    {
        Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKpartition, expr, "Merge");
        buildActivityFramework(instance);

        buildInstancePrefix(instance);

        HqlExprArray sorts;
        unwindChildren(sorts, cond);

        OwnedHqlExpr sortOrder = createValueSafe(no_sortlist, makeSortListType(NULL), sorts);

        DatasetReference dsRef(dataset);
        buildCompareFuncHelper(*this, *instance, "compare", sortOrder, dsRef);

        if (!instance->isLocal)
            generateSerializeKey(instance->nestedctx, no_none, dsRef, sorts, true, true);

        buildInstanceSuffix(instance);

        buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

        return instance->getBoundActivity();
    }
    else
    {
        //Now generate the instance definition...
        ThorActivityKind tak = (expr->getOperator() == no_distribute) ?
                                    (mergeOrder ? TAKhashdistributemerge : TAKhashdistribute) : TAKdistributed;
        Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, tak, expr, "HashDistribute");
        if (expr->hasAttribute(skewAtom))
            instance->graphLabel.set("Skew Distribute");
        buildActivityFramework(instance);

        buildInstancePrefix(instance);

        if (!expr->hasAttribute(skewAtom))
            buildHashClass(instance->nestedctx, "Hash", cond, DatasetReference(dataset));
        doBuildBoolFunction(instance->classctx, "isPulled", expr->hasAttribute(pulledAtom));
        buildSkewThresholdMembers(instance->classctx, expr);
        if (mergeOrder)
            buildCompareMember(instance->nestedctx, "MergeCompare", mergeOrder, DatasetReference(dataset));

        buildInstanceSuffix(instance);

        buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

        return instance->getBoundActivity();
    }
}

//---------------------------------------------------------------------------
// no_rollup

void HqlCppTranslator::checkAmbiguousRollupCondition(IHqlExpression * expr)
{
    IHqlExpression * select = queryAmbiguousRollupCondition(expr, false);
    if (select)
    {
        IHqlExpression * dataset = expr->queryChild(0);
        OwnedHqlExpr newSelect = replaceSelector(select, dataset->queryNormalizedSelector(), queryActiveTableSelector());
        StringBuffer selectText;
        getExprECL(newSelect, selectText);
        reportWarning(CategoryUnexpected, SeverityUnknown, queryLocation(expr), ECODETEXT(HQLWRN_AmbiguousRollupCondition), selectText.str());
    }
}


ABoundActivity * HqlCppTranslator::doBuildActivityRollup(BuildCtx & ctx, IHqlExpression * expr)
{
    StringBuffer s;

    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * cond = expr->queryChild(1);
    IHqlExpression * transform = expr->queryChild(2);
    IHqlExpression * selSeq = querySelSeq(expr);
    HqlExprArray equalities;
    HqlExprArray conds;
    if (cond->getOperator() == no_sortlist)
        unwindChildren(conds, cond);
    else
        conds.append(*LINK(cond));

    //build child table.... 
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    //Now generate the instance definition...
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKrollup, expr, "Rollup");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    if (options.checkAmbiguousRollupCondition)
        checkAmbiguousRollupCondition(expr);

    buildDedupFilterFunction(instance->startctx, equalities, conds, dataset, selSeq);
    buildRollupTransformFunction(instance->startctx, dataset, transform, selSeq);

    OwnedHqlExpr left = createSelector(no_left, dataset, selSeq);
    OwnedHqlExpr right = createSelector(no_right, dataset, selSeq);
    StringBuffer flags;
    if (cond->usesSelector(left) || cond->usesSelector(right))
        flags.append("|RFrolledismatchleft");
    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);
    
    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
// no_denormalize

ABoundActivity * HqlCppTranslator::doBuildActivityDenormalize(BuildCtx & ctx, IHqlExpression * expr)
{
    if (isKeyedJoin(expr))
        return doBuildActivityKeyedJoinOrDenormalize(ctx, expr);
    return doBuildActivityJoinOrDenormalize(ctx, expr);
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityFirstN(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * limit = expr->queryChild(1);

    //choosen(x,ALL) does nothing, but is a way of getting round the implicit limitation.
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    if (isChooseNAllLimit(limit) && !expr->queryChild(2))
        return boundDataset.getClear();

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKfirstn, expr, "FirstN");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    {
        MemberFunction func(*this, instance->startctx, "virtual __int64 getLimit()");
        OwnedHqlExpr newLimit = ensurePositiveOrZeroInt64(limit);
        if (options.spotCSE)
            newLimit.setown(spotScalarCSE(newLimit, NULL, queryOptions().spotCseInIfDatasetConditions));
        buildReturn(func.ctx, newLimit);
    }

    if (queryRealChild(expr, 2))
    {
        MemberFunction func(*this, instance->startctx, "virtual __int64 numToSkip()");
        OwnedHqlExpr adjusted = adjustValue(expr->queryChild(2), -1);
        OwnedHqlExpr newAdjusted = ensurePositiveOrZeroInt64(adjusted);
        buildReturn(func.ctx, newAdjusted);
    }

    if (expr->hasAttribute(groupedAtom))
        doBuildBoolFunction(instance->classctx, "preserveGrouping", true);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityChooseSetsEx(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    bool isEnth = expr->hasAttribute(enthAtom);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, isEnth ? TAKchoosesetsenth : TAKchoosesetslast, expr, "ChooseSetsEx");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    unsigned numArgs = expr->numChildren();
    bool keepExtras = false;
    unsigned numConditions = 0;

    for (unsigned idx1 = 1; idx1 < numArgs; idx1++)
    {
        switch (expr->queryChild(idx1)->getOperator())
        {
        case no_mapto:
            numConditions++;
            break;
        case no_attr:
            break;
        default:
            keepExtras = true;
        }
    }
    unsigned numCategories = numConditions + (keepExtras ? 1 : 0);

    //virtual unsigned getNumSets()
    {
        MemberFunction func(*this, instance->classctx, "virtual unsigned getNumSets()");
        OwnedHqlExpr numExpr = createConstant((int)numCategories);
        buildReturn(func.ctx, numExpr, unsignedType);
    }

    //virtual unsigned getRecordCategory(const void * _self) = 0;
    {
        MemberFunction func(*this, instance->startctx, "virtual unsigned getCategory(const void * _self)");
        func.ctx.addQuotedLiteral("const unsigned char * self = (const unsigned char *)_self;");
        bindTableCursor(func.ctx, dataset, "self");
        HqlExprArray args;
        for (unsigned idx3 = 1; idx3 <= numConditions; idx3++)
        {
            IHqlExpression * cur = expr->queryChild(idx3);
            args.append(*createValue(no_mapto, makeVoidType(), LINK(cur->queryChild(0)), createConstant(unsignedType->castFrom(false, (__int64)idx3))));
        }
        if (keepExtras)
            args.append(*createConstant((__int64)numConditions+1));
        else
            args.append(*createConstant((__int64)0));
        OwnedHqlExpr map = createValue(no_map, LINK(unsignedType), args);
        buildReturn(func.ctx, map);
    }

    //virtual void getLimits(unsigned * counts) = 0;
    {
        StringBuffer s;
        MemberFunction func(*this, instance->startctx, "virtual void getLimits(__int64 * counts)");
        for (unsigned idx2 = 1; idx2 <= numCategories; idx2++)
        {
            IHqlExpression * cur = expr->queryChild(idx2);
            s.clear().append("counts[").append(idx2-1).append("]");
            OwnedHqlExpr target = createVariable(s.str(), LINK(defaultIntegralType));

            switch (cur->getOperator())
            {
            case no_mapto:
                buildAssignToTemp(func.ctx, target, cur->queryChild(1));
                break;
            default:
                buildAssignToTemp(func.ctx, target, cur);
                break;
            }
        }
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityChooseSets(BuildCtx & ctx, IHqlExpression * expr)
{
    if (expr->hasAttribute(enthAtom) || expr->hasAttribute(lastAtom))
        return doBuildActivityChooseSetsEx(ctx, expr);

    IHqlExpression * dataset = expr->queryChild(0);
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKchoosesets, expr, "ChooseSets");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    unsigned numArgs = expr->numChildren();
    bool allowSpill = !expr->hasAttribute(exclusiveAtom);
    bool keepExtras = false;
    unsigned numConditions = 0;

    for (unsigned idx1 = 1; idx1 < numArgs; idx1++)
    {
        switch (expr->queryChild(idx1)->getOperator())
        {
        case no_mapto:
            numConditions++;
            break;
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            break;
        default:
            keepExtras = true;
        }
    }
    unsigned numCategories = numConditions + (keepExtras ? 1 : 0);

    {
        MemberFunction func(*this, instance->classctx, "virtual unsigned getNumSets()");
        OwnedHqlExpr numExpr = createConstant((int)numCategories);
        buildReturn(func.ctx, numExpr, unsignedType);
    }

    instance->startctx.addQuotedLiteral("unsigned * counts;");
    instance->startctx.addQuotedLiteral("unsigned numFull;");

    StringBuffer s;
    MemberFunction limitFunc(*this, instance->classctx, "virtual bool setCounts(unsigned * data)");
    limitFunc.ctx.addQuotedLiteral("counts = data;");
    limitFunc.ctx.addQuotedLiteral("numFull = 0;");

    OwnedHqlExpr tally = createVariable("counts", makeIntType(4, false));
    MemberFunction validFunc(*this, instance->startctx, "virtual unsigned getRecordAction(const void * _self)");
    validFunc.ctx.addQuotedLiteral("const unsigned char * self = (const unsigned char *)_self;");
    bindTableCursor(validFunc.ctx, dataset, "self");

    OwnedHqlExpr one = createConstant((int)1);
    for (unsigned idx = 0; idx < numCategories; idx++)
    {
        OwnedHqlExpr indexExpr = getSizetConstant(idx);
        OwnedHqlExpr bucketExpr = createValue(no_index, LINK(unsignedType), tally.getLink(), indexExpr.getLink());
        OwnedHqlExpr transBucketExpr = createTranslated(bucketExpr);

        IHqlExpression * arg = expr->queryChild(idx+1);
        IHqlExpression * count = (arg->getOperator() == no_mapto ? arg->queryChild(1) : arg);

        OwnedHqlExpr cond2 = createBoolExpr(no_lt, transBucketExpr.getLink(), ensureExprType(count, unsignedType));
        OwnedHqlExpr condDone = createBoolExpr(no_eq, transBucketExpr.getLink(), ensureExprType(count, unsignedType));

        BuildCtx condctx(validFunc.ctx);
        if (arg->getOperator() == no_mapto)
        {
            IHqlExpression * filter = arg->queryChild(0);
            if (allowSpill)
            {
                OwnedHqlExpr cond = createBoolExpr(no_and, LINK(filter), cond2.getLink());
                CHqlBoundExpr bound;
                buildExpr(condctx, cond, bound);
                condctx.addFilter(bound.expr);
            }
            else
            {
                buildFilter(condctx, filter);
                BuildCtx failctx(condctx);
                buildFilter(condctx, cond2);
                buildReturn(failctx, queryZero());
            }
        }
        else
        {
            buildFilter(condctx, cond2);
        }

        OwnedHqlExpr inc = createValue(no_postinc, bucketExpr.getLink());
        condctx.addExpr(inc);
        BuildCtx doneCtx(condctx);
        buildFilter(doneCtx, condDone);
        doneCtx.addQuoted(s.clear().append("if (++numFull == ").append(numCategories).append(") return 2;"));
        buildReturn(condctx, one, unsignedType);

        BuildCtx limitCondCtx(limitFunc.ctx);
        buildFilter(limitCondCtx, condDone);
        limitCondCtx.addQuotedLiteral("numFull++;");
    }
    buildReturn(validFunc.ctx, queryZero());

    limitFunc.ctx.addQuoted(s.clear().append("return numFull == ").append(numCategories).append(";"));

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityNormalizeGroup(BuildCtx & ctx, IHqlExpression * expr)
{
    throwUnexpected();

    IHqlExpression * dataset = expr->queryChild(0);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKnormalize, expr,"Normalize");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityNormalize(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * numRows = expr->queryChild(1);
    if (!numRows->queryType()->isScalar())
        return doBuildActivityNormalizeChild(ctx, expr);

    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * transform = expr->queryChild(2);
    IHqlExpression * selSeq = querySelSeq(expr);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKnormalize, expr,"Normalize");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    {
        MemberFunction func(*this, instance->startctx, "virtual unsigned numExpandedRows(const void * _left)");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *) _left;");

        bindTableCursor(func.ctx, dataset, "left", no_left, selSeq);
        bindTableCursor(func.ctx, dataset, "left");
        buildReturn(func.ctx, numRows);
    }

    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left, unsigned counter)");
        ensureRowAllocated(func.ctx, "crSelf");

        IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
        associateCounter(func.ctx, counter, "counter");
        buildTransformBody(func.ctx, transform, dataset, NULL, instance->dataset, selSeq);
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityNormalizeChild(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    LinkedHqlExpr childDataset = expr->queryChild(1);
    IHqlExpression * transform = expr->queryChild(2);
    IHqlExpression * selSeq = querySelSeq(expr);

    if (transformReturnsSide(expr, no_right, 1))
        return doBuildActivityNormalizeLinkedChild(ctx, expr);

    StringBuffer s;
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKnormalizechild, expr,"NormalizeChild");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    //Generate queryChildRecordSize();
    buildMetaMember(instance->classctx, childDataset, isGrouped(childDataset), "queryChildRecordSize");

    // INormalizeChildIterator * queryIterator();
    {
        bool outOfLine = true;
        bool streamed = false;
        if (childDataset->isDatarow())
            childDataset.setown(createDatasetFromRow(childDataset.getClear()));
        if (childDataset->getOperator() == no_select)
            outOfLine = isArrayRowset(childDataset->queryType());
        if (hasLinkCountedModifier(childDataset))
            outOfLine = true;
        if (isStreamed(childDataset))
        {
            outOfLine = true;
            streamed = true;
        }

        BuildCtx iterclassctx(instance->nestedctx);
        StringBuffer memberName, className;
        getUniqueId(memberName.append("m"));
        getMemberClassName(className, memberName.str());

        ExpressionFormat format;
        IHqlStmt * classStmt = nullptr;
        if (streamed)
        {
            classStmt = beginNestedClass(iterclassctx, memberName, "CNormalizeStreamedChildIterator");
            format = FormatStreamedDataset;
        }
        else if (outOfLine)
        {
            classStmt = beginNestedClass(iterclassctx, memberName, "CNormalizeLinkedChildIterator");
            format = FormatLinkedDataset;
        }
        else
        {
            classStmt = beginNestedClass(iterclassctx, memberName, "CNormalizeChildIterator");
            format = FormatBlockedDataset;

            MetaInstance childmeta(*this, childDataset->queryRecord(), isGrouped(childDataset));
            buildMetaInfo(childmeta);
            s.clear().append(className).append("() : CNormalizeChildIterator(").append(childmeta.queryInstanceObject()).append(") {}");
            iterclassctx.addQuoted(s);
        }

        bool callFromActivity = false;
        {
            MemberFunction activityInitFunc(*this, instance->startctx);

            MemberFunction func(*this, iterclassctx, "virtual void init(const void * _left)");
            func.ctx.addQuotedLiteral("const byte * left = (const byte *)_left;");

            CHqlBoundExpr bound;
            if (childDataset->getOperator() != no_select)
            {
                //Ugly......
                //If this is a complex expression, then ensure the temporary variable is a member of the activity class, and
                //evaluate it in the function defined inside the activity (so the member variables don't need mangling)
                func.ctx.addQuotedLiteral("activity->init(left);");

                BuildCtx * declarectx = NULL;
                instance->evalContext->getInvariantMemberContext(NULL, &declarectx, NULL, false, true);
                queryEvalContext(iterclassctx)->ensureHelpersExist();
                assertex(declarectx);

                activityInitFunc.start("void init(const byte * left)");
                bindTableCursor(activityInitFunc.ctx, dataset, "left", no_left, selSeq);

                CHqlBoundTarget tempTarget;
                buildTempExpr(activityInitFunc.ctx, *declarectx, tempTarget, childDataset, format, false);
                bound.setFromTarget(tempTarget);

                callFromActivity = true;
            }
            else
            {
                bindTableCursor(func.ctx, dataset, "left", no_left, selSeq);
                buildDataset(func.ctx, childDataset, bound, format);
            }

            s.clear();
            if (callFromActivity)
                s.append(memberName).append(".");

            s.append("setDataset(");
            if (streamed)
            {
                generateExprCpp(s, bound.expr).append(");");
            }
            else
            {
                if (outOfLine)
                {
                    generateExprCpp(s, bound.count).append(",");
                    generateExprCpp(s, bound.expr).append(");");
                }
                else
                {
                    OwnedHqlExpr length = getBoundLength(bound);
                    generateExprCpp(s, length).append(",");
                    generateExprCpp(s, bound.expr).append(");");
                }
            }
            if (callFromActivity)
                activityInitFunc.ctx.addQuoted(s);
            else
                func.ctx.addQuoted(s);
        }

        endNestedClass(classStmt);

        s.clear().append("INormalizeChildIterator * queryIterator() { return &").append(memberName).append("; }");
        instance->startctx.addQuoted(s);
    }

    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned counter)");
        ensureRowAllocated(func.ctx, "crSelf");

        IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
        associateCounter(func.ctx, counter, "counter");
        buildTransformBody(func.ctx, transform, dataset, childDataset, instance->dataset, selSeq);
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityNormalizeLinkedChild(BuildCtx & ctx, IHqlExpression * expr)
{
    OwnedHqlExpr dataset;
    OwnedHqlExpr childDataset;
    node_operator selectorOp = no_none;
    IHqlExpression * selSeq = NULL;

    switch (expr->getOperator())
    {
    case no_select:
        {
            bool isNew;
            dataset.set(querySelectorDataset(expr, isNew));
            //Ensure input is a dataset so cleanly bound as a cursor later
            if (dataset->isDatarow())
                dataset.setown(createDatasetFromRow(dataset.getClear()));
            assertex(isNew);
            OwnedHqlExpr active = ensureActiveRow(dataset);
            childDataset.setown(replaceSelectorDataset(expr, active));
            break;
        }
    case no_normalize:
        {
            dataset.set(expr->queryChild(0));
            childDataset.set(expr->queryChild(1));
            selectorOp = no_left;
            selSeq = querySelSeq(expr);
            break;
        }
    default:
        throwUnexpectedOp(expr->getOperator());
    }

    StringBuffer s;
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKnormalizelinkedchild, expr,"NormalizeLinkedChild");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    OwnedHqlExpr value = childDataset->isDatarow() ? createDatasetFromRow(LINK(childDataset)) : LINK(childDataset);
    BuildCtx * declarectx = NULL;
    instance->evalContext->getInvariantMemberContext(NULL, &declarectx, NULL, false, true);
    assertex(declarectx);

    StringBuffer iterName;
    //virtual byte * first(const void * parentRecord) = 0;
    {
        MemberFunction func(*this, instance->startctx, "virtual byte * first(const void * parentRecord)");
        func.ctx.addQuotedLiteral("const byte * left = (const byte *)parentRecord;");
        bindTableCursor(func.ctx, dataset, "left", selectorOp, selSeq);

        ExpressionFormat format = !hasLinkCountedModifier(value) ? FormatLinkedDataset : FormatNatural;
        Owned<IHqlCppDatasetCursor> dsCursor = createDatasetSelector(func.ctx, value, format);
        dsCursor->buildIterateClass(instance->startctx, iterName, &func.ctx);

        StringBuffer s;
        OwnedHqlExpr callFirst = createQuoted(s.clear().append("(byte *)").append(iterName).append(".first()"), makeBoolType());
        func.ctx.addReturn(callFirst);
    }

    {
        //virtual byte * next() = 0;
        BuildCtx nextctx(instance->startctx);
        nextctx.addQuotedFunction("virtual byte * next()");

        StringBuffer s;
        OwnedHqlExpr callNext = createQuoted(s.clear().append("(byte *)").append(iterName).append(".next()"), makeBoolType());
        nextctx.addReturn(callNext);
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);


    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivitySelectNew(BuildCtx & ctx, IHqlExpression * expr)
{
    if (!expr->isDatarow())
        return doBuildActivityNormalizeLinkedChild(ctx, expr);

    bool isNew = false;
    IHqlExpression * dataset = querySelectorDataset(expr, isNew);
    assertex(isNew);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKproject, expr, "Project");

    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    {
        //Need to create a dataset to replace the parent selector - since it might be a row
        OwnedHqlExpr anon = createDataset(no_anon, LINK(dataset->queryRecord()));

        MemberFunction func(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left)");
        func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
        ensureRowAllocated(func.ctx, "crSelf");
        BoundRow * selfCursor = bindSelf(func.ctx, expr, "crSelf");
        bindTableCursor(func.ctx, anon->queryNormalizedSelector(), "left");

        OwnedHqlExpr activeAnon = ensureActiveRow(anon);
        OwnedHqlExpr value = replaceSelectorDataset(expr, activeAnon);
        buildAssign(func.ctx, selfCursor->querySelector(), value);

        buildReturnRecordSize(func.ctx, selfCursor);
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityPrefetchProject(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * transform = queryNewColumnProvider(expr);
    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
    IHqlExpression * selSeq = querySelSeq(expr);
    IHqlExpression * prefetch = expr->queryAttribute(prefetchAtom);
    IHqlExpression * lookahead = queryAttributeChild(expr, prefetchAtom, 0);
    IHqlExpression * record = expr->queryRecord();
#ifdef _DEBUG
    assertex((counter != NULL) == transformContainsCounter(transform, counter));
#endif

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, counter ? TAKprefetchcountproject : TAKprefetchproject, expr, "PrefetchProject");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    StringBuffer flags;
    if (prefetch && prefetch->hasAttribute(parallelAtom)) flags.append("|PPFparallel");
    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    if (transformContainsSkip(transform))
        doBuildBoolFunction(instance->classctx, "canFilter", true);
    if (lookahead)
        doBuildUnsignedFunction(instance->startctx, "getLookahead", lookahead);

    //Similar code to project below.  First generate the post processing function (which all aliases etc. will get generated into)
    MemberFunction transformFunc(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left, IEclGraphResults * results, unsigned __int64 _counter)");
    ensureRowAllocated(transformFunc.ctx, "crSelf");
    transformFunc.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
    if (expr->getOperator() == no_hqlproject)
        bindTableCursor(transformFunc.ctx, dataset, "left", no_left, selSeq);
    else
        bindTableCursor(transformFunc.ctx, dataset, "left");
    BoundRow * selfCursor = bindSelf(transformFunc.ctx, expr, "crSelf");
    associateSkipReturnMarker(transformFunc.ctx, queryZero(), selfCursor);

    if (counter)
        associateCounter(transformFunc.ctx, counter, "counter");

    //Now process the transform
    HqlExprArray assigns;

    //Introduce a scope to ensure that mapper and builder have the minimum lifetime.
    {
        //Possibly cleaner if this was implemented inside a class derived from TransformBuilder
        TransformBuilder builder(*this, transformFunc.ctx, record, selfCursor, assigns);
        filterExpandAssignments(transformFunc.ctx, &builder, assigns, transform);
        builder.buildTransformChildren(transformFunc.ctx, record, selfCursor->querySelector());
        OwnedHqlExpr subgraph = builder.getPrefetchGraph();
        if (subgraph)
        {
            //Generate the extract preparation function
            MemberFunction preTransformFunc(*this, instance->startctx, "virtual bool preTransform(rtlRowBuilder & builder, const void * _left, unsigned __int64 _counter)");
            associateSkipReturnMarker(preTransformFunc.ctx, queryBoolExpr(false), NULL);
            preTransformFunc.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
            if (expr->getOperator() == no_hqlproject)
                bindTableCursor(preTransformFunc.ctx, dataset, "left", no_left, selSeq);
            else
                bindTableCursor(preTransformFunc.ctx, dataset, "left");
            if (counter)
                associateCounter(preTransformFunc.ctx, counter, "counter");

            OwnedHqlExpr graphInstance;
            ChildGraphBuilder graphBuilder(*this, subgraph);
            graphBuilder.generatePrefetchGraph(preTransformFunc.ctx, &graphInstance);
            preTransformFunc.ctx.addReturn(queryBoolExpr(true));

            BuildCtx childctx(instance->startctx);
            childctx.addQuotedFunction("virtual IThorChildGraph *queryChild()");
            childctx.addReturn(graphInstance);

            //Add an association for the results into the transform function.
            IHqlExpression * graph = subgraph->queryChild(0);
            OwnedHqlExpr results = createAttribute(resultsAtom, LINK(graph));
            OwnedHqlExpr resultsInstanceExpr = createQuoted("results", makeBoolType());
            transformFunc.ctx.associateExpr(results, resultsInstanceExpr);
        }
        builder.flush(transformFunc.ctx);
    }

    buildReturnRecordSize(transformFunc.ctx, selfCursor);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityProject(BuildCtx & ctx, IHqlExpression * expr)
{
    if (expr->hasAttribute(prefetchAtom) || options.usePrefetchForAllProjects)
        return doBuildActivityPrefetchProject(ctx, expr);

    const node_operator op = expr->getOperator();
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * normalized = dataset->queryNormalizedSelector();
    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
    IHqlExpression * selSeq = querySelSeq(expr);
    LinkedHqlExpr transform = queryNewColumnProvider(expr);
    LinkedHqlExpr invariantCondition;
    HqlExprArray filterConditions;
    OwnedHqlExpr leftSelector = (op != no_newusertable) ? createSelector(no_left, normalized, selSeq) : NULL;

    //Spot any filters preceding this project/user table, and fold them into the project
    //It doesn't work with count projects though - because it would alter the counter passed to the transform.
    //MORE: It would be possible to spot a skip condition for a count project which was input invariant, but thor
    //would need a new implementation if the input wasn't grouped because the count would need to be global.
    if (!counter)
    {
        bool done = false;
        do
        {
            switch (dataset->getOperator())
            {
            case no_filter:
                {
                    LinkedHqlExpr invariant;
                    OwnedHqlExpr cond = extractFilterConditions(invariant, dataset, normalized, false, false);
                    //A dataset invariant filter is only worth combining if the engine supports a filtered project operation.
                    if (!options.supportFilterProject && invariant)
                    {
                        done = true;
                        break;
                    }
                    //Don't merge the condition if it would create an ambiguity on LEFT - highly unlikely to occur in practice
                    if (leftSelector && cond)
                    {
                        if (containsSelectorAnywhere(cond, leftSelector))
                        {
                            done = true;
                            break;
                        }
                        cond.setown(replaceSelector(cond, normalized, leftSelector));
                    }
                    extendConditionOwn(invariantCondition, no_and, invariant.getClear());
                    if (cond)
                        cond->unwindList(filterConditions, no_and);
                    dataset = dataset->queryChild(0);
                    break;
                }
            case no_sorted:
                dataset = dataset->queryChild(0);
                break;
            default:
                done = true;
                break;
            }
        } while (!done);

    }

    bool isFilterProject = invariantCondition != NULL;
    bool containsCounter = expr->hasAttribute(_countProject_Atom);
#ifdef _DEBUG
    assertex(containsCounter == transformContainsCounter(transform, counter));
#endif

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    Owned<ActivityInstance> instance = isFilterProject ? new ActivityInstance(*this, ctx, TAKfilterproject, expr, "FilterProject") :
                                       containsCounter ? new ActivityInstance(*this, ctx, TAKcountproject, expr, "CountProject")
                                                       : new ActivityInstance(*this, ctx, TAKproject, expr, "Project");

    if (filterConditions.ordinality())
    {
        if (isGroupedActivity(expr))
            instance->graphLabel.set("Grouped Filtered Project");
        else
            instance->graphLabel.set("Filtered Project");
    }

    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    {
        MemberFunction func(*this, instance->startctx);
        if (isFilterProject || containsCounter)
        {
            func.start("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, unsigned __int64 counter)");
            if (containsCounter)
                associateCounter(func.ctx, counter, "counter");
        }
        else
            func.start("virtual size32_t transform(ARowBuilder & crSelf, const void * _left)");

        ensureRowAllocated(func.ctx, "crSelf");
        if (filterConditions.ordinality())
        {
            HqlExprArray args;
            ForEachItemIn(i, filterConditions)
            {
                OwnedHqlExpr test = createValue(no_skip, makeVoidType(), getInverse(&filterConditions.item(i)));
                args.append(*LINK(test));
            }
            unwindChildren(args, transform);
            transform.setown(transform->clone(args));
        }

        if (op == no_newusertable)
        {
            func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");

            BoundRow * selfCursor = bindSelf(func.ctx, expr, "crSelf");
            associateSkipReturnMarker(func.ctx, queryZero(), selfCursor);
            bindTableCursor(func.ctx, dataset, "left");

            doTransform(func.ctx, transform, selfCursor);
            buildReturnRecordSize(func.ctx, selfCursor);
        }
        else
            buildTransformBody(func.ctx, transform, dataset, NULL, instance->dataset, selSeq);
    }

    if (filterConditions.ordinality() || transformContainsSkip(transform))
        doBuildBoolFunction(instance->classctx, "canFilter", true);

    if (invariantCondition)
        doBuildBoolFunction(instance->startctx, "canMatchAny", invariantCondition);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivitySerialize(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    bool serialize = (expr->getOperator() == no_serialize);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKproject, expr, "Project");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left)");

        // Bind left to "left" and right to RIGHT
        bindTableCursor(func.ctx, dataset, "_left");
        BoundRow * selfCursor = bindSelf(func.ctx, expr, "crSelf");

        //MORE: I don't have any examples that trigger this code as far as I know...
        IIdAtom * funcId = serialize ? rtlSerializeToBuilderId : rtlDeserializeToBuilderId;
        IAtom * kind = serialize ? serializerAtom : deserializerAtom;
        IAtom * serializeForm = serialize ? expr->queryChild(1)->queryName() : expr->queryChild(2)->queryName();

        IHqlExpression * record = expr->queryRecord();
        HqlExprArray args;
        args.append(*createSerializer(func.ctx, record, serializeForm, kind));
        args.append(*ensureActiveRow(dataset));
        Owned<ITypeInfo> type = makeTransformType(record->getType());
        OwnedHqlExpr call = bindFunctionCall(funcId, args, type);
        doTransform(func.ctx, call, selfCursor);
        buildReturnRecordSize(func.ctx, selfCursor);
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityDefineSideEffect(BuildCtx & ctx, IHqlExpression * expr)
{
    Owned<ABoundActivity> parentActivity = buildCachedActivity(ctx, expr->queryChild(0));
    OwnedHqlExpr attr = createAttribute(_sideEffect_Atom, LINK(expr->queryAttribute(_uid_Atom)));
    OwnedHqlExpr unknown = createUnknown(no_attr, NULL, NULL, LINK(parentActivity));
    activeGraphCtx->associateExpr(attr, unknown);
    return parentActivity.getClear();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityCallSideEffect(BuildCtx & ctx, IHqlExpression * expr)
{
    OwnedHqlExpr attr = createAttribute(_sideEffect_Atom, LINK(expr->queryAttribute(_uid_Atom)));
    HqlExprAssociation * match = activeGraphCtx->queryMatchExpr(attr);
    if (!match)
        throwUnexpected();
    ABoundActivity * activity = static_cast<ABoundActivity *>(match->queryExpr()->queryUnknownExtra());
    return LINK(activity);
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityExecuteWhen(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, expr->queryChild(0));
    Owned<ABoundActivity> associatedActivity = buildCachedActivity(ctx, expr->queryChild(1));
    if (!associatedActivity)
        return boundDataset.getClear();

    const char * label;
    int when;
    if (expr->hasAttribute(successAtom))
    {
        label = "Success";
        when = WhenSuccessId;
    }
    else if (expr->hasAttribute(failureAtom))
    {
        label = "Failure";
        when = WhenFailureId;
    }
    else if (expr->hasAttribute(parallelAtom))
    {
        label = "Parallel";
        when = WhenParallelId;
    }
    else if (expr->hasAttribute(beforeAtom))
    {
        label = "Before";
        when = WhenBeforeId;
    }
    else
    {
        //Should WHEN default to BEFORE or PARALLEL??
        label = "Parallel";
        when = WhenParallelId;
    }

    bool useImplementationClass = options.minimizeActivityClasses;
    ThorActivityKind kind = (expr->isAction() ? TAKwhen_action : TAKwhen_dataset);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, "WhenAction");
    if (useImplementationClass)
        instance->setImplementationClass(newWhenActionArgId);

    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);
    buildInstanceSuffix(instance);

    if (expr->isAction())
        addActionConnection(ctx, boundDataset, instance, dependencyAtom, NULL, 0, 1);
    else
        buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    addActionConnection(ctx, associatedActivity, instance, dependencyAtom, label, 0, when);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

IHqlExpression * extractFilterConditions(HqlExprAttr & invariant, IHqlExpression * expr, IHqlExpression * dataset, bool spotCSE, bool spotCseInIfDatasetConditions)
{
    unsigned num = expr->numChildren();
    assertex(num > 1);
    HqlExprAttr cond = queryRealChild(expr, 1);
    bool changed = false;
    unsigned idx;
    for (idx = 2; idx < num; idx++)
    {
        IHqlExpression * cur = expr->queryChild(idx);
        if (!cur->isAttribute())
            extendConditionOwn(cond, no_and, LINK(cur));
    }
    if (!cond)
        return NULL;

    if (spotCSE)
        cond.setown(spotScalarCSE(cond, NULL, spotCseInIfDatasetConditions));

    HqlExprArray tests;
    cond->unwindList(tests, no_and);
    ForEachItemInRev(i, tests)
    {
        IHqlExpression & cur = tests.item(i);
        if (!exprReferencesDataset(&cur, dataset))
        {
            changed = true;
            if (!matchesBoolean(&cur, true))
                invariant.setown(extendConditionOwn(no_and, LINK(&cur), invariant.getClear()));
            tests.remove(i);
        }
    }
    if (changed)
        cond.setown(createBalanced(no_and, queryBoolType(), tests));

    return cond.getClear();
}


ABoundActivity * HqlCppTranslator::doBuildActivityFilter(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKfilter, expr,"Filter");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    HqlExprAttr invariant;
    OwnedHqlExpr cond = extractFilterConditions(invariant, expr, dataset, options.spotCSE, queryOptions().spotCseInIfDatasetConditions);

    //Base class returns true, so only generate if no non-invariant conditions
    if (cond)
    {
        MemberFunction func(*this, instance->startctx, "virtual bool isValid(const void * _self)");
        func.ctx.addQuotedLiteral("unsigned char * self = (unsigned char *) _self;");

        bindTableCursor(func.ctx, dataset, "self");
        buildReturn(func.ctx, cond);

        if (options.addLikelihoodToGraph)
        {
            double likelihood = queryLikelihood(cond);
            if (isKnownLikelihood(likelihood))
            {
                StringBuffer text;
                likelihood *= 100;
                text.setf("%3.2f%%", likelihood);
                instance->addAttribute("matchLikelihood", text);
            }
        }
    }

    if (invariant)
        doBuildBoolFunction(instance->startctx, "canMatchAny", invariant);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityFilterGroup(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * selSeq = querySelSeq(expr);
    IHqlExpression * rowsid = expr->queryAttribute(_rowsid_Atom);
    if (targetThor() && !isGrouped(dataset))
        throwError(HQLERR_ThorHavingMustBeGrouped);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKfiltergroup, expr,"FilterGroup");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    HqlExprAttr invariant;
    OwnedHqlExpr left = createSelector(no_left, dataset, selSeq);
    OwnedHqlExpr cond = extractFilterConditions(invariant, expr, left, options.spotCSE, options.spotCseInIfDatasetConditions);

    //Base class returns true, so only generate if no non-invariant conditions
    if (cond)
    {
        MemberFunction func(*this, instance->startctx, "virtual bool isValid(unsigned numRows, const void * * _rows)");
        func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _rows[0];");
        func.ctx.addQuotedLiteral("unsigned char * * rows = (unsigned char * *) _rows;");

        bindTableCursor(func.ctx, dataset, "left", no_left, selSeq);
        bindRows(func.ctx, no_left, selSeq, rowsid, dataset, "numRows", "rows", options.mainRowsAreLinkCounted);

        buildReturn(func.ctx, cond);
    }

    if (invariant)
        doBuildBoolFunction(instance->startctx, "canMatchAny", invariant);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityCombine(BuildCtx & ctx, IHqlExpression * expr)
{
    //MORE: Need to expand nested combines so they have multiple inputs.
    //But will need to assign aliases to the inputs + do a reasonable amount of processing.
    IHqlExpression * left = expr->queryChild(0);
    IHqlExpression * right = expr->queryChild(1);
    IHqlExpression * transform = expr->queryChild(2);
    IHqlExpression * selSeq = querySelSeq(expr);

    if (targetThor() && !expr->hasAttribute(localAtom) && !insideChildQuery(ctx))
        ERRORAT(queryLocation(expr), HQLERR_ThorCombineOnlyLocal);

    CIArray bound;
    bound.append(*buildCachedActivity(ctx, left));
    bound.append(*buildCachedActivity(ctx, right));

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKcombine, expr, "Combine");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, unsigned _num, const void * * _rows)");
        if (transform->getOperator() != no_skip)
        {
            func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _rows[0];");
            func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _rows[1];");
            ensureRowAllocated(func.ctx, "crSelf");

            bindTableCursor(func.ctx, left, "left", no_left, selSeq);
            bindTableCursor(func.ctx, right, "right", no_right, selSeq);
            BoundRow * selfCursor = bindSelf(func.ctx, expr, "crSelf");
            associateSkipReturnMarker(func.ctx, queryZero(), selfCursor);

            doTransform(func.ctx, transform, selfCursor);
            buildReturnRecordSize(func.ctx, selfCursor);
        }
        else
            func.ctx.addReturn(queryZero());
    }

    if (transformContainsSkip(transform))
        doBuildBoolFunction(instance->classctx, "canFilter", true);

    buildInstanceSuffix(instance);
    ForEachItemIn(idx2, bound)
        buildConnectInputOutput(ctx, instance, (ABoundActivity *)&bound.item(idx2), 0, idx2);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

void HqlCppTranslator::bindRows(BuildCtx & ctx, node_operator side, IHqlExpression * selSeq, IHqlExpression * rowsid, IHqlExpression * dataset, const char * numText, const char * rowsText, bool rowsAreLinkCounted)
{
    OwnedHqlExpr selector = createSelector(side, dataset, selSeq);
    OwnedHqlExpr rowsExpr = createDataset(no_rows, LINK(selector), LINK(rowsid));

    Owned<ITypeInfo> rowType = makeReferenceModifier(LINK(rowsExpr->queryType()->queryChildType()));
    if (rowsAreLinkCounted)
        rowType.setown(setLinkCountedAttr(rowType, true));

    //Rows may be link counted, but rows() is not a linkable rowset
    OwnedITypeInfo rowsType = makeReferenceModifier(makeTableType(rowType.getClear()));
    rowsType.setown(makeOutOfLineModifier(LINK(rowsType)));

    CHqlBoundExpr boundRows;
    boundRows.count.setown(createQuoted(numText, LINK(unsignedType)));
    boundRows.expr.setown(createQuoted(rowsText, LINK(rowsType)));
    ctx.associateExpr(rowsExpr, boundRows);
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityCombineGroup(BuildCtx & ctx, IHqlExpression * expr)
{
    //MORE: Need to expand nested combines so they have multiple inputs.
    //But will need to assign aliases to the inputs + do a reasonable amount of processing.
    IHqlExpression * left = expr->queryChild(0);
    IHqlExpression * right = expr->queryChild(1);
    IHqlExpression * selSeq = querySelSeq(expr);
    IHqlExpression * transform = expr->queryChild(2);
    IHqlExpression * rowsid = expr->queryAttribute(_rowsid_Atom);

    CIArray bound;
    bound.append(*buildCachedActivity(ctx, left));
    bound.append(*buildCachedActivity(ctx, right));

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKcombinegroup, expr, "CombineGroup");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left, unsigned numRows, const void * * _rows)");
        if (transform->getOperator() != no_skip)
        {
            func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *)_left;");
            func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _rows[0];");
            func.ctx.addQuotedLiteral("unsigned char * * rows = (unsigned char * *) _rows;");
            ensureRowAllocated(func.ctx, "crSelf");

            bindTableCursor(func.ctx, left, "left", no_left, selSeq);
            bindTableCursor(func.ctx, right, "right", no_right, selSeq);
            bindRows(func.ctx, no_right, selSeq, rowsid, right, "numRows", "rows", options.mainRowsAreLinkCounted);
            BoundRow * selfCursor = bindSelf(func.ctx, expr, "crSelf");
            associateSkipReturnMarker(func.ctx, queryZero(), selfCursor);

            doTransform(func.ctx, transform, selfCursor);
            buildReturnRecordSize(func.ctx, selfCursor);
        }
        else
            func.ctx.addReturn(queryZero());
    }

    if (transformContainsSkip(transform))
        doBuildBoolFunction(instance->classctx, "canFilter", true);

    buildInstanceSuffix(instance);
    ForEachItemIn(idx2, bound)
        buildConnectInputOutput(ctx, instance, (ABoundActivity *)&bound.item(idx2), 0, idx2);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityRollupGroup(BuildCtx & ctx, IHqlExpression * expr)
{
    //MORE: Need to expand nested combines so they have multiple inputs.
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * transform = expr->queryChild(1);
    IHqlExpression * selSeq = querySelSeq(expr);
    IHqlExpression * rowsid = expr->queryAttribute(_rowsid_Atom);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKrollupgroup, expr, "RollupGroup");
    instance->graphLabel.set("Rollup Group");       // Grouped Rollup Group looks silly
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, unsigned numRows, const void * * _rows)");
        if (transform->getOperator() != no_skip)
        {
            func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _rows[0];");
            func.ctx.addQuotedLiteral("unsigned char * * rows = (unsigned char * *) _rows;");
            ensureRowAllocated(func.ctx, "crSelf");

            bindTableCursor(func.ctx, dataset, "left", no_left, selSeq);
            bindRows(func.ctx, no_left, selSeq, rowsid, dataset, "numRows", "rows", options.mainRowsAreLinkCounted);

            BoundRow * selfCursor = bindSelf(func.ctx, expr, "crSelf");
            associateSkipReturnMarker(func.ctx, queryZero(), selfCursor);
            doTransform(func.ctx, transform, selfCursor);
            buildReturnRecordSize(func.ctx, selfCursor);
        }
        else
            func.ctx.addReturn(queryZero());
    }

    if (transformContainsSkip(transform))
        doBuildBoolFunction(instance->classctx, "canFilter", true);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityAssert(BuildCtx & ctx, IHqlExpression * expr)
{
    HqlExprArray args;
    expr->unwindList(args, expr->getOperator());

    IHqlExpression * dataset = &args.item(0);
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    //MORE: Change this when ThroughApply activities are supported in engines.
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKfilter, expr,"Filter");
    instance->graphLabel.set("Assert");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    unsigned num = args.ordinality();

    {
        MemberFunction func(*this, instance->startctx, "virtual bool isValid(const void * _self)");
        func.ctx.addQuotedLiteral("unsigned char * self = (unsigned char *) _self;");
        bindTableCursor(func.ctx, dataset, "self");

        for (unsigned i=1; i < num; i++)
        {
            IHqlExpression & cur = args.item(i);
            if (!cur.isAttribute())
                buildStmt(func.ctx, &cur);
        }
        func.ctx.addReturn(queryBoolExpr(true));
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

void HqlCppTranslator::buildLimitHelpers(BuildCtx & ctx, IHqlExpression * rowLimit, IHqlExpression * failAction, bool isSkip, IHqlExpression * filename, unique_id_t id)
{
    doBuildUnsigned64Function(ctx, "getRowLimit", rowLimit);

    if (isZero(rowLimit))
        WARNING(CategoryUnusual, HQLWRN_LimitIsZero);

    if (!isSkip)
    {
        LinkedHqlExpr fail = failAction;
        if (!fail || fail->isAttribute())
        {
            if (!id)
                id = queryCurrentActivityId(ctx);
            fail.setown(createFailAction("Limit exceeded", rowLimit, filename, id));
        }

        MemberFunction func(*this, ctx, "virtual void onLimitExceeded()");
        buildStmt(func.ctx, fail);
    }
}


void HqlCppTranslator::buildLimitHelpers(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * filename, unique_id_t id)
{
    buildLimitHelpers(ctx, expr->queryChild(1), queryRealChild(expr, 2), expr->hasAttribute(skipAtom), filename, id);

    IHqlExpression * transform = queryAttributeChild(expr, onFailAtom, 0);
    if (transform)
    {
        MemberFunction func(*this, ctx, "virtual size32_t transformOnLimitExceeded(ARowBuilder & crSelf)");
        ensureRowAllocated(func.ctx, "crSelf");
        buildTransformBody(func.ctx, transform, NULL, NULL, expr, NULL);
    }
}



ABoundActivity * HqlCppTranslator::doBuildActivityLimit(BuildCtx & ctx, IHqlExpression * expr)
{
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, expr->queryChild(0));

    IHqlExpression * transform = queryAttributeChild(expr, onFailAtom, 0);
    ThorActivityKind kind = TAKlimit;
    const char * helper = "Limit";
    if (transform)
    {
        kind = TAKcreaterowlimit;
        helper = "CreateRowLimit";
    }
    else if (expr->hasAttribute(skipAtom))
        kind = TAKskiplimit;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, helper);

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    buildLimitHelpers(instance->startctx, expr, NULL, instance->activityId);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}


ABoundActivity * HqlCppTranslator::doBuildActivityCatch(BuildCtx & ctx, IHqlExpression * expr)
{
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, expr->queryChild(0));
    IHqlExpression * arg1 = queryRealChild(expr, 1);
    IHqlExpression * filter = NULL;
    IHqlExpression * action = NULL;
    if (arg1 && arg1->isBoolean())
    {
        filter = arg1;
        action = queryRealChild(expr, 2);
    }
    else
        action = arg1;

    IHqlExpression * transform = queryAttributeChild(expr, onFailAtom, 0);
    bool isSkip = expr->hasAttribute(skipAtom);

    ThorActivityKind kind = TAKcatch;
    const char * helper = "Catch";
    if (transform)
        kind = TAKcreaterowcatch;
    else if (isSkip)
        kind = TAKskipcatch;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, helper);

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    if (filter)
    {
        MemberFunction func(*this, instance->startctx, "virtual bool isMatch(IException * except)");
        associateLocalFailure(func.ctx, "except");
        OwnedHqlExpr cseFilter = spotScalarCSE(filter, NULL, queryOptions().spotCseInIfDatasetConditions);
        buildReturn(func.ctx, cseFilter, queryBoolType());
    }

    if (transform)
    {
        MemberFunction func(*this, instance->startctx, "virtual unsigned transformOnExceptionCaught(ARowBuilder & crSelf, IException * except)");
        ensureRowAllocated(func.ctx, "crSelf");
        associateLocalFailure(func.ctx, "except");
        buildTransformBody(func.ctx, transform, NULL, NULL, expr, NULL);
    }
    else if (!isSkip)
    {
        LinkedHqlExpr fail = action;
        if (!fail)
            fail.setown(createFailAction("Missing failure", NULL, NULL, instance->activityId));

        MemberFunction func(*this, instance->startctx, "virtual void onExceptionCaught()");
        buildStmt(func.ctx, fail);
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}


ABoundActivity * HqlCppTranslator::doBuildActivitySection(BuildCtx & ctx, IHqlExpression * expr)
{
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, expr->queryChild(0));

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKsection, expr, "Section");

    StringBuffer label;
    getStringValue(label, expr->queryChild(1));
    instance->graphLabel.set(label.str());

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    StringBuffer flags;
    IHqlExpression * description = NULL;
    ForEachChildFrom(i, expr, 2)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (cur->isAttribute())
        {
            IAtom * name= cur->queryName();
            if (name == privateAtom)
                flags.append("|TSFprivate");
        }
        else if (isStringType(cur->queryType()))
        {
            description = cur;
            flags.append("|TSFdynamicDescription");
        }
    }

    if (description)
        doBuildStringFunction(instance->startctx, "getDescription", description);

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivitySectionInput(BuildCtx & ctx, IHqlExpression * expr)
{
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, expr->queryChild(0));

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKsectioninput, expr, "SectionInput");

    StringBuffer label;
    expr->queryChild(1)->queryValue()->getStringValue(label);
    instance->graphLabel.set(label.str());
    instance->graphEclText.append("<>");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    StringBuffer flags;
    if (expr->hasAttribute(privateAtom))
        flags.append("|TSFprivate");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityPullActivity(BuildCtx & ctx, IHqlExpression * expr)
{
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, expr->queryChild(0));
    if (targetHThor())
        return boundDataset.getClear();

    assertex(expr->hasAttribute(pullAtom));

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKpull, expr, "Pull");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);
    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityTraceActivity(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    assertex(expr->hasAttribute(traceAtom));
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKtrace, expr, "Trace");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    IHqlExpression *keepLimit = queryAttributeChild(expr, keepAtom, 0);
    if (keepLimit)
        doBuildUnsignedFunction(instance->startctx, "getKeepLimit", keepLimit);

    IHqlExpression *skip = queryAttributeChild(expr, skipAtom, 0);
    if (skip)
        doBuildUnsignedFunction(instance->startctx, "getSkip", skip);

    IHqlExpression *sample = queryAttributeChild(expr, sampleAtom, 0);
    if (sample)
        doBuildUnsignedFunction(instance->startctx, "getSample", sample);

    IHqlExpression *named = queryAttributeChild(expr, namedAtom, 0);
    if (named)
        doBuildVarStringFunction(instance->startctx, "getName", named);

    HqlExprAttr invariant;
    OwnedHqlExpr cond = extractFilterConditions(invariant, expr, dataset, options.spotCSE, queryOptions().spotCseInIfDatasetConditions);

    //Base class returns true, so only generate if no non-invariant conditions
    if (cond)
    {
        MemberFunction func(*this, instance->startctx, "virtual bool isValid(const void * _self)");
        func.ctx.addQuotedLiteral("unsigned char * self = (unsigned char *) _self;");

        bindTableCursor(func.ctx, dataset, "self");
        buildReturn(func.ctx, cond);
    }
    if (invariant)
        doBuildBoolFunction(instance->startctx, "canMatchAny", invariant);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
//-- no_sub --

ABoundActivity * HqlCppTranslator::doBuildActivitySub(BuildCtx & ctx, IHqlExpression * expr)
{
    assertex(!"MORE!");
    return NULL;
}

//---------------------------------------------------------------------------
//-- no_sample [GROUP] --

ABoundActivity * HqlCppTranslator::doBuildActivityEnth(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * numerator = expr->queryChild(1);
    IHqlExpression * denominator = expr->queryChild(2);
    IHqlExpression * sample = expr->queryChild(3);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKenth, expr, "Enth");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    {
        MemberFunction func(*this, instance->startctx, "virtual unsigned __int64 getProportionNumerator()");
        buildReturn(func.ctx, numerator);
    }

    {
        MemberFunction func(*this, instance->startctx, "virtual unsigned __int64 getProportionDenominator()");
        if (denominator && !denominator->isAttribute())
            buildReturn(func.ctx, denominator);
        else
        {
            OwnedHqlExpr notProvided = createConstant(counterType->castFrom(true, I64C(-1)));
            buildReturn(func.ctx, notProvided);
        }
    }

    {
        MemberFunction func(*this, instance->startctx, "virtual unsigned getSampleNumber()");
        if (sample && !sample->isAttribute())
            buildReturn(func.ctx, sample);
        else
            func.ctx.addQuotedLiteral("return 1;");
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
//-- no_sample [GROUP] --

ABoundActivity * HqlCppTranslator::doBuildActivitySample(BuildCtx & ctx, IHqlExpression * expr)
{
    StringBuffer s;
    IHqlExpression * dataset = expr->queryChild(0);
    LinkedHqlExpr sampleExpr = queryRealChild(expr, 2);
    if (!sampleExpr)
        sampleExpr.setown(getSizetConstant(1));

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKsample, expr,"Sample");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    doBuildUnsignedFunction(instance->startctx, "getProportion", expr->queryChild(1));
    doBuildUnsignedFunction(instance->startctx, "getSampleNumber", sampleExpr);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
//-- no_group [GROUP] --

/* In parms: NOT linked. Return: linked */
IHqlExpression * HqlCppTranslator::createOrderFromSortList(const DatasetReference & dataset, IHqlExpression * sortList, IHqlExpression * leftSelect, IHqlExpression * rightSelect)
{
    HqlExprArray leftList, rightList;

    unsigned max = sortList->numChildren();
    unsigned idx;
    for (idx = 0; idx < max; idx++)
    {
        IHqlExpression * next = sortList->queryChild(idx);
        //optimize order on (string)qstring to order on qstring for example.  Can make quite a difference.
        if (isCast(next))
        {
            IHqlExpression * uncast = next->queryChild(0);
            ITypeInfo * castType = next->queryType();
            ITypeInfo * uncastType = uncast->queryType();
            if (preservesValue(castType, uncastType) && preservesOrder(castType, uncastType))
                next = uncast;
        }

        bool invert = false;
        if (next->getOperator() == no_negate)
        {
            invert = true;
            next = next->queryChild(0);
        }

        IHqlExpression * leftResolved = dataset.mapScalar(next, leftSelect);
        IHqlExpression * rightResolved = dataset.mapScalar(next, rightSelect);
        if (invert)
        {
            leftList.append(*rightResolved);
            rightList.append(*leftResolved);
        }
        else
        {
            leftList.append(*leftResolved);
            rightList.append(*rightResolved);
        }
    }

    return createValue(no_order, LINK(signedType), createSortList(leftList), createSortList(rightList));
}


void HqlCppTranslator::buildReturnOrder(BuildCtx & ctx, IHqlExpression *sortList, const DatasetReference & dataset)
{
    OwnedHqlExpr selSeq = createDummySelectorSequence();
    OwnedHqlExpr leftSelect = dataset.getSelector(no_left, selSeq);
    OwnedHqlExpr rightSelect = dataset.getSelector(no_right, selSeq);
    OwnedHqlExpr order = createOrderFromSortList(dataset, sortList, leftSelect, rightSelect);

    bindTableCursor(ctx, dataset.queryDataset(), "left", no_left, selSeq);
    bindTableCursor(ctx, dataset.queryDataset(), "right", no_right, selSeq);

    doBuildReturnCompare(ctx, order, no_order, false, false);
}

void HqlCppTranslator::doBuildFuncIsSameGroup(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * sortlist)
{
    MemberFunction func(*this, ctx, "virtual bool isSameGroup(const void * _left, const void * _right)");
    if (sortlist->getOperator() == no_activetable)
        buildReturn(func.ctx, queryBoolExpr(false));
    else
    {
        func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
        func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");

        OwnedHqlExpr selSeq = createSelectorSequence();
        OwnedHqlExpr leftSelect = createSelector(no_left, dataset, selSeq);
        OwnedHqlExpr rightSelect = createSelector(no_right, dataset, selSeq);
        HqlExprArray args;
        HqlExprArray leftValues, rightValues;
        HqlExprArray compares;
        unwindChildren(compares, sortlist);

        //Optimize the grouping conditions by ordering them by the fields in the record (so the
        //doBuildReturnCompare() can combine as many as possible),  and remove duplicates
        if (options.optimizeGrouping && (compares.ordinality() > 1))
        {
            HqlExprArray equalities;
            optimizeGroupOrder(equalities, dataset, compares);
            ForEachItemIn(i, equalities)
            {
                IHqlExpression * test = &equalities.item(i);
                leftValues.append(*replaceSelector(test, dataset, leftSelect));
                rightValues.append(*replaceSelector(test, dataset, rightSelect));
            }
        }

        ForEachItemIn(idx, compares)
        {
            IHqlExpression * test = &compares.item(idx);
            if (containsSelector(test, leftSelect) || containsSelector(test, rightSelect))
                args.append(*LINK(test));
            else
            {
                OwnedHqlExpr lhs = replaceSelector(test, dataset, leftSelect);
                OwnedHqlExpr rhs = replaceSelector(test, dataset, rightSelect);
                if (lhs != rhs)
                {
                    leftValues.append(*lhs.getClear());
                    rightValues.append(*rhs.getClear());
                }
            }
        }

        OwnedHqlExpr result;
        OwnedHqlExpr orderResult;
        //Use the optimized equality code for more than one element - which often combines the comparisons.
        if (leftValues.ordinality() != 0)
        {
            if (leftValues.ordinality() == 1)
                args.append(*createValue(no_eq, makeBoolType(), LINK(&leftValues.item(0)), LINK(&rightValues.item(0))));
            else
                orderResult.setown(createValue(no_order, LINK(signedType), createSortList(leftValues), createSortList(rightValues)));
        }

        if (args.ordinality() == 1)
            result.set(&args.item(0));
        else if (args.ordinality() != 0)
            result.setown(createValue(no_and, makeBoolType(), args));

        bindTableCursor(func.ctx, dataset, "left", no_left, selSeq);
        bindTableCursor(func.ctx, dataset, "right", no_right, selSeq);
        IHqlExpression * trueExpr = queryBoolExpr(true);
        if (result)
        {
            if (orderResult)
            {
                buildFilteredReturn(func.ctx, result, trueExpr);
                doBuildReturnCompare(func.ctx, orderResult, no_eq, true, false);
            }
            else
            {
                buildReturn(func.ctx, result);
            }
        }
        else
        {
            if (orderResult)
                doBuildReturnCompare(func.ctx, orderResult, no_eq, true, false);
            else
                buildReturn(func.ctx, trueExpr);
        }
    }
}

ABoundActivity * HqlCppTranslator::doBuildActivityUngroup(BuildCtx & ctx, IHqlExpression * expr, ABoundActivity * boundDataset)
{
    bool useImplementationClass = options.minimizeActivityClasses;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKdegroup, expr,"Degroup");
    if (useImplementationClass)
        instance->setImplementationClass(newDegroupArgId);
    buildActivityFramework(instance);

    buildInstancePrefix(instance);
    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityGroup(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * child = dataset;
    while (child->getOperator() == no_group)
        child = child->queryChild(0);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, child);
    if (queryGrouping(expr) == queryGrouping(child))
        return boundDataset.getClear();

    IHqlExpression * sortlist = queryRealChild(expr, 1);
    if (!sortlist || ((sortlist->numChildren() == 0) && (sortlist->getOperator() != no_activetable)))
    {
        return doBuildActivityUngroup(ctx, expr, boundDataset);
    }
    else
    {
        ThorActivityKind tak = (expr->getOperator() == no_group) ? TAKgroup: TAKgrouped;
        Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, tak, expr, "Group");
        buildActivityFramework(instance);

        buildInstancePrefix(instance);

        //virtual bool isSameGroup(const void *left, const void *right);
        doBuildFuncIsSameGroup(instance->startctx, dataset, sortlist);

        buildInstanceSuffix(instance);

        buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
        return instance->getBoundActivity();
    }
}

//---------------------------------------------------------------------------
//-- no_if (dataset) --

ABoundActivity * queryAssociatedActivity(BuildCtx & ctx, IHqlExpression * expr)
{
    ActivityAssociation * match = static_cast<ActivityAssociation *>(ctx.queryAssociation(expr, AssocActivity, NULL));
    if (match)
        return match->activity;
    return NULL;
}

ABoundActivity * HqlCppTranslator::getConditionalActivity(BuildCtx & ctx, IHqlExpression * expr, bool isChild)
{
    if (!expr)
        return NULL;

    return buildCachedActivity(ctx, expr);
}

ABoundActivity * HqlCppTranslator::doBuildActivityIf(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    LinkedHqlExpr cond = expr->queryChild(0);
    IHqlExpression * trueBranch = expr->queryChild(1);
    IHqlExpression * falseBranch = queryRealChild(expr, 2);
    const char * firstLabel = "True";
    if (!expr->isDatarow())
    {
        if (falseBranch && (falseBranch->getOperator() == no_null))
            falseBranch = NULL;
        else if (trueBranch->getOperator() == no_null)
        {
            trueBranch = falseBranch;
            falseBranch = NULL;
            cond.setown(getInverse(cond));
            firstLabel = "False";
        }
    }


    OwnedHqlExpr cseCond = options.spotCSE ? spotScalarCSE(cond, NULL, queryOptions().spotCseInIfDatasetConditions) : LINK(cond);
    bool isChild = (insideChildOrLoopGraph(ctx) || insideRemoteGraph(ctx) || insideLibrary());
    IHqlExpression * activeGraph = queryActiveSubGraph(ctx)->graphTag;

    if (isChild)
    {
        Owned<ABoundActivity> boundTrue = buildCachedActivity(ctx, trueBranch);
        Owned<ABoundActivity> boundFalse = falseBranch ? buildCachedActivity(ctx, falseBranch) : NULL;

        Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKchildif, expr, "If");

        buildActivityFramework(instance, isRoot);
        buildInstancePrefix(instance);

        {
            MemberFunction getcond(*this, instance->startctx, "virtual bool getCondition()", MFsingle);
            buildReturn(getcond.ctx, cseCond);
        }
        if (isGraphIndependent(cseCond, activeGraph) && !instance->hasChildActivity)
            instance->addAttributeBool("_graphIndependent", true);

        buildConnectInputOutput(ctx, instance, boundTrue, 0, 0, firstLabel);
        if (boundFalse)
            buildConnectInputOutput(ctx, instance, boundFalse, 0, 1, "False");

        buildInstanceSuffix(instance);
        return instance->getBoundActivity();
    }
    else
    {
        Owned<ABoundActivity> boundTrue = getConditionalActivity(ctx, trueBranch, isChild);
        Owned<ABoundActivity> boundFalse = getConditionalActivity(ctx, falseBranch, isChild);

        Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, expr->isAction() ? TAKifaction : TAKif, expr,"If");

        buildActivityFramework(instance, isRoot);
        buildInstancePrefix(instance);

        {
            MemberFunction getcond(*this, instance->startctx, "virtual bool getCondition()", MFsingle);
            buildReturn(getcond.ctx, cseCond);
        }

        if (isGraphIndependent(cseCond, activeGraph) && !instance->hasChildActivity)
            instance->addAttributeBool("_graphIndependent", true);

        if (expr->isAction())
        {
            if (boundTrue)
                addActionConnection(ctx, boundTrue, instance, dependencyAtom, firstLabel, 0, 1);
            if (boundFalse)
                addActionConnection(ctx, boundFalse, instance, dependencyAtom, "False", 1, 2);
        }
        else
        {
            if (boundTrue)
                buildConnectInputOutput(ctx, instance, boundTrue, 0, 0, firstLabel);
            if (boundFalse)
                buildConnectInputOutput(ctx, instance, boundFalse, 0, 1, "False");
        }

        buildInstanceSuffix(instance);
        return instance->getBoundActivity();
    }
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivitySequentialParallel(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IArray boundActivities;
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (!cur->isAttribute() && (cur->getOperator() != no_null))
        {
            ABoundActivity * activity = buildCachedActivity(ctx, cur);
            if (activity)
                boundActivities.append(*activity);
        }
    }

    ThorActivityKind kind = (expr->getOperator() != no_parallel) ? TAKsequential : TAKparallel;
    const char * helper = (expr->getOperator() != no_parallel) ? "Sequential" : "Parallel";
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, helper);

    buildActivityFramework(instance, isRoot);
    buildInstancePrefix(instance);

    doBuildUnsignedFunction(instance->createctx, "numBranches", boundActivities.ordinality());

    ForEachItemIn(j, boundActivities)
    {
        ABoundActivity & cur = (ABoundActivity&)boundActivities.item(j);
        StringBuffer temp;
        temp.append("Action #").append(j+1);
        addActionConnection(ctx, &cur, instance, dependencyAtom, temp.str(), j, j+1);
    }

    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}


ABoundActivity * HqlCppTranslator::doBuildActivityChoose(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * cond, CIArrayOf<ABoundActivity> & inputs, bool isRoot)
{
    Owned<ABoundActivity> boundDefault = &inputs.popGet();

    bool isChild = (insideChildOrLoopGraph(ctx) || insideRemoteGraph(ctx));
    assertex(!expr->isAction());
    ThorActivityKind tak = isChild ? TAKchildcase : TAKcase;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, tak, expr, "Case");

    buildActivityFramework(instance, isRoot);
    buildInstancePrefix(instance);

    OwnedHqlExpr fullCond(foldHqlExpression(cond));
    if (options.spotCSE)
        fullCond.setown(spotScalarCSE(fullCond, NULL, queryOptions().spotCseInIfDatasetConditions));

    {
        MemberFunction func(*this, instance->startctx, "virtual unsigned getBranch()");
        buildReturn(func.ctx, fullCond);
    }

    StringBuffer label;
    ForEachItemIn(branchIdx, inputs)
    {
        ABoundActivity * boundBranch = &inputs.item(branchIdx);
        label.clear().append("Branch ").append(branchIdx+1);
        if (expr->isAction())
            addActionConnection(ctx, boundBranch, instance, dependencyAtom, label.str(), branchIdx, branchIdx+1);
        else
            buildConnectInputOutput(ctx, instance, boundBranch, 0, branchIdx, label.str());
    }

    IHqlExpression * activeGraph = queryActiveSubGraph(ctx)->graphTag;
    bool graphIndependent = isGraphIndependent(fullCond, activeGraph);

    if (graphIndependent && !instance->hasChildActivity)
        instance->addAttributeBool("_graphIndependent", true);

    buildConnectInputOutput(ctx, instance, boundDefault, 0, inputs.ordinality(), "default");

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityChoose(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    bool isChild = (insideChildOrLoopGraph(ctx) || insideRemoteGraph(ctx));

    CIArrayOf<ABoundActivity> inputs;
    ForEachChildFrom(i, expr, 1)
    {
        IHqlExpression * cur = queryRealChild(expr, i);
        if (cur)
            inputs.append(*getConditionalActivity(ctx, cur, isChild));
    }

    OwnedHqlExpr branch = adjustValue(expr->queryChild(0), -1);
    return doBuildActivityChoose(ctx, expr, branch, inputs, isRoot);
}


ABoundActivity * HqlCppTranslator::doBuildActivityCase(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    node_operator op = expr->getOperator();
    bool isChild = (insideChildOrLoopGraph(ctx) || insideRemoteGraph(ctx));
    unsigned first = 0;
    unsigned max = expr->numChildren();
    if (op == no_case)
        first++;

    CIArrayOf<ABoundActivity> inputs;
    for (unsigned iinput = first; iinput < max-1; iinput++)
        inputs.append(*getConditionalActivity(ctx, expr->queryChild(iinput)->queryChild(1), isChild));
    Owned<ABoundActivity> boundDefault = getConditionalActivity(ctx, expr->queryChild(max-1), isChild);

    ThorActivityKind tak = isChild ? TAKchildcase : TAKcase;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, tak, expr, "Case");

    buildActivityFramework(instance, isRoot);
    buildInstancePrefix(instance);

    //MORE: If we created a map/case expression and then called buildReturn it would potentially generate better code.
    // code is below, but not enabled because it doesn't necessarily improve things at the moment (see jholt39.xhql)
    IHqlExpression * activeGraph = queryActiveSubGraph(ctx)->graphTag;
    HqlExprArray args;
    if (op == no_case)
        args.append(*LINK(expr->queryChild(0)));

    StringBuffer label;
    for (unsigned idx = first; idx < max-1; idx++)
    {
        unsigned branchIdx = idx-first;
        IHqlExpression * branch = expr->queryChild(idx);
        IHqlExpression * branchCond = branch->queryChild(0);
        OwnedHqlExpr ret = getSizetConstant(branchIdx);
        args.append(*createValue(no_mapto, ret->getType(), LINK(branchCond), LINK(ret)));

        ABoundActivity * boundBranch = &inputs.item(branchIdx);
        getExprECL(branchCond, label.clear(), false, true);
        if (label.length() > 20)
            label.clear().append("Branch ").append(branchIdx+1);

        if (expr->isAction())
            addActionConnection(ctx, boundBranch, instance, dependencyAtom, label.str(), branchIdx, branchIdx+1);
        else
            buildConnectInputOutput(ctx, instance, boundBranch, 0, branchIdx, label.str());
    }

    args.append(*createConstant(unsignedType->castFrom(false, (__int64)max-1)));
    OwnedHqlExpr fullCond = createValue(op, LINK(unsignedType), args);

    {
        MemberFunction func(*this, instance->startctx, "virtual unsigned getBranch()");
        fullCond.setown(foldHqlExpression(fullCond));
        if (options.spotCSE)
            fullCond.setown(spotScalarCSE(fullCond, NULL, queryOptions().spotCseInIfDatasetConditions));
        buildReturn(func.ctx, fullCond);
    }

    bool graphIndependent = isGraphIndependent(fullCond, activeGraph);

    if (graphIndependent && !instance->hasChildActivity)
        instance->addAttributeBool("_graphIndependent", true);

    buildConnectInputOutput(ctx, instance, boundDefault, 0, max-1-first, "default");

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
//-- no_sort [SORT] --

void HqlCppTranslator::buildSkewThresholdMembers(BuildCtx & ctx, IHqlExpression * expr)
{
    StringBuffer s, temp;
    if (getAttribute(expr, thresholdAtom, temp.clear()))
    {
        s.clear().append("virtual unsigned __int64 getThreshold() { return ").append(temp).append("; }");
        ctx.addQuoted(s);
    }

    IHqlExpression * skew = expr->queryAttribute(skewAtom);
    if (skew)
    {
        Owned<ITypeInfo> doubleType = makeRealType(8);
        IHqlExpression * skewMax = skew->queryChild(0);
        if (skewMax->getOperator() != no_null)
            doBuildFunction(ctx, doubleType, "getSkew", skewMax);

        IHqlExpression * skewTarget = queryRealChild(skew, 1);
        doBuildFunction(ctx, doubleType, "getTargetSkew", skewTarget);
    }
}


ABoundActivity * HqlCppTranslator::doBuildActivitySort(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    LinkedHqlExpr sortlist = expr->queryChild(1);
    IHqlExpression * limit = NULL;
    IHqlExpression * cosort = NULL;

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    const char *helper;
    ThorActivityKind actKind = TAKsort;
    switch (expr->getOperator())
    {
    case no_topn:
        {
            actKind = TAKtopn;
            limit = expr->queryChild(2);
            helper = "TopN";
            break;
        }
    case no_assertsorted:
        {
            actKind = TAKsorted;
            helper = "Sort";
            break;
        }
    case no_subsort:
        actKind = TAKsubsort;
        helper = "SubSort";
        break;
    default:
        {
            cosort = expr->queryChild(2);
            if (cosort && cosort->isAttribute())
                cosort = NULL;
            helper = "Sort";
            break;
        }
    }

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, actKind, expr, helper);
    buildActivityFramework(instance);

    StringBuffer s;
    buildInstancePrefix(instance);

//  sortlist.setown(spotScalarCSE(sortlist));
    buildCompareFuncHelper(*this, *instance, "compare", sortlist, DatasetReference(dataset));

    IHqlExpression * record = dataset->queryRecord();
    IAtom * serializeType = diskAtom; //MORE: Does this place a dependency on the implementation?
    OwnedHqlExpr serializedRecord = getSerializedForm(record, serializeType);
    if (!targetRoxie())
    {
        if (record != serializedRecord)
        {
            OwnedHqlExpr selSeq = createSelectorSequence();
            OwnedHqlExpr leftSelector = createSelector(no_left, dataset, selSeq);
            OwnedHqlExpr mappedSortlist = replaceSelector(sortlist, dataset, leftSelector);
            OwnedHqlExpr serializedSortlist = replaceMemorySelectorWithSerializedSelector(mappedSortlist, record, no_left, selSeq, serializeType);
            OwnedHqlExpr serializedDataset = createDataset(no_null, LINK(serializedRecord));
            DatasetReference serializedRef(serializedDataset, no_left, selSeq);
            try
            {
                buildCompareFuncHelper(*this, *instance, "compareSerializedRow", serializedSortlist, serializedRef);
            }
            catch (IException * e)
            {
                e->Release();
                ERRORAT(queryLocation(expr), HQLERR_CannotGenerateSerializedCompare);
            }
        }
    }

    HqlExprArray sorts;
    sortlist->unwindList(sorts, no_sortlist);
    bool tryToSerializeKey = (actKind == TAKsort) && !isGroupedActivity(expr) && !isLocalActivity(expr) && !instance->isChildActivity();
    generateSerializeKey(instance->nestedctx, no_none, DatasetReference(dataset), sorts, tryToSerializeKey, false);

    buildSkewThresholdMembers(instance->classctx, expr);

    if (expr->getOperator() == no_subsort)
        doBuildFuncIsSameGroup(instance->startctx, dataset, expr->queryChild(2));

    if (limit)
    {
        OwnedHqlExpr newLimit = ensurePositiveOrZeroInt64(limit);
        MemberFunction func(*this, instance->startctx, "virtual __int64 getLimit()");
        buildReturn(func.ctx, newLimit, defaultIntegralType);
    }

    IHqlExpression * best = expr->queryAttribute(bestAtom);
    if (best)
    {
        doBuildBoolFunction(instance->classctx, "hasBest", true);

        HqlExprArray sortValues, maxValues;
        sortlist->unwindList(sortValues, no_sortlist);
        unwindChildren(maxValues, best);
        ForEachItemIn(i, sortValues)
        {
            IHqlExpression & cur = sortValues.item(i);
            if (cur.getOperator() == no_negate)
            {
                sortValues.replace(OLINK(maxValues.item(i)), i);
                maxValues.replace(*LINK(cur.queryChild(0)), i);
            }
        }
        OwnedHqlExpr order = createValue(no_order, LINK(signedType), createSortList(sortValues), createSortList(maxValues));

        MemberFunction func(*this, instance->startctx, "virtual int compareBest(const void * _self)");
        func.ctx.addQuotedLiteral("unsigned char * self = (unsigned char *) _self;");
        bindTableCursor(func.ctx, dataset, "self");
        buildReturn(func.ctx, order);
    }

    if (cosort)
    {
        //cosort has form joined(dataset), 
        IHqlExpression * cosortDataset = cosort->queryChild(0);
        if (cosortDataset->getOperator() == no_compound_diskread)
            cosortDataset = cosortDataset->queryChild(0);
        if (cosortDataset->getOperator() == no_sorted)
        {
            IHqlExpression * source = cosortDataset->queryChild(0);
            if (source->getOperator() != no_table)
                throwError(HQLERR_JoinSortedMustBeDataset);
            IHqlExpression * sourceType = source->queryChild(2);
            if (!sourceType || sourceType->getOperator() != no_thor)
                throwError(HQLERR_JoinSortedMustBeThor);

            s.clear().append("virtual const char * getSortedFilename() { return ");
            generateExprCpp(s, source->queryChild(0)).append("; }");
            instance->startctx.addQuoted(s);

            buildMetaMember(instance->classctx, cosortDataset, false, "querySortedRecordSize");
        }
        else
        {
            instance->startctx.addQuotedLiteral("virtual const char * getSortedFilename() { return NULL; }");
            instance->classctx.addQuotedLiteral("virtual IOutputMetaData * querySortedRecordSize() { return NULL; }");

            ABoundActivity * masterSort = queryAssociatedActivity(ctx, cosortDataset);
            if (!masterSort)
                throwError(HQLERR_SortAndCoSortConcurrent);

            Owned<ABoundActivity> slave = instance->getBoundActivity();
            buildConnectOrders(ctx, slave, masterSort);
        }

        HqlExprArray left, right;
        cosortDataset->queryChild(1)->unwindList(left, no_sortlist);
        sortlist->unwindList(right, no_sortlist);

        doCompareLeftRight(instance->nestedctx, "CompareLeftRight", DatasetReference(cosortDataset), DatasetReference(dataset), left, right);
    }

    if (expr->hasAttribute(manyAtom))
        instance->classctx.addQuotedLiteral("virtual bool hasManyRecords() { return true; }");

    IHqlExpression * stable = expr->queryAttribute(stableAtom);
    IHqlExpression * unstable = expr->queryAttribute(unstableAtom);
    IHqlExpression * spill = expr->queryAttribute(spillAtom);
    IHqlExpression * method = NULL;
    StringBuffer flags;
    if (stable)
    {
        flags.append("|TAFstable");
        method = stable->queryChild(0);
    }
    else if (unstable)
    {
        flags.append("|TAFunstable");
        method = unstable->queryChild(0);
    }
    else
    {
        //If a dataset is sorted by all fields then it is impossible to determine if the original order
        //was preserved - so mark the sort as potentially unstable (to reduce memory usage at runtime)
        if (options.optimizeSortAllFields &&
            allFieldsAreSorted(expr->queryRecord(), sortlist, dataset->queryNormalizedSelector(), options.optimizeSortAllFieldsStrict))
        {
            flags.append("|TAFunstable");
        }
    }
    if (!method)
        method = queryAttributeChild(expr, algorithmAtom, 0);

    if (spill)
        flags.append("|TAFspill");
    if (!method || method->isConstant())
        flags.append("|TAFconstant");
    if (expr->hasAttribute(parallelAtom))
        flags.append("|TAFparallel");

    if (method)
        doBuildVarStringFunction(instance->startctx, "getAlgorithm", method);

    if (!streq(flags.str(), "|TAFconstant"))
        instance->classctx.addQuotedF("virtual unsigned getAlgorithmFlags() { return %s; }", flags.str()+1);

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityQuantile(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKquantile, expr, "Quantile");
    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    IHqlExpression * number = expr->queryChild(1);
    IHqlExpression * sortlist = expr->queryChild(2);
    IHqlExpression * transform = expr->queryChild(3);
    IHqlExpression * score = queryAttributeChild(expr, scoreAtom, 0);
    IHqlExpression * skew = queryAttributeChild(expr, skewAtom, 0);
    IHqlExpression * dedupAttr = expr->queryAttribute(dedupAtom);
    IHqlExpression * range = queryAttributeChild(expr, rangeAtom, 0);

    buildCompareFuncHelper(*this, *instance, "compare", sortlist, DatasetReference(dataset));
    doBuildUnsigned64Function(instance->startctx, "getNumDivisions", number);

    if (skew)
        doBuildFunction(instance->startctx, doubleType, "getSkew", skew);

    if (range)
    {
        Owned<ITypeInfo> setType = makeSetType(makeIntType(8, false));
        doBuildFunction(instance->startctx, setType, "getRange", range);
    }

    if (score)
    {
        MemberFunction func(*this, instance->startctx, "virtual unsigned __int64 getScore(const void * _self)");
        func.ctx.addQuotedLiteral("unsigned char * self = (unsigned char *) _self;");
        bindTableCursor(func.ctx, dataset, "self");
        buildReturn(func.ctx, score);
    }

    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
    IHqlExpression * selSeq = querySelSeq(expr);
    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left, unsigned __int64 counter)");
        if (counter)
            associateCounter(func.ctx, counter, "counter");
        buildTransformBody(func.ctx, transform, dataset, NULL, instance->dataset, selSeq);
    }

    buildClearRecordMember(instance->createctx, "", dataset);

    //If a dataset is sorted by all fields then it is impossible to determine if the original order
    //was preserved - so mark the sort as potentially unstable (to reduce memory usage at runtime)
    bool unstable = expr->hasAttribute(unstableAtom);
    if (options.optimizeSortAllFields &&
        allFieldsAreSorted(expr->queryRecord(), sortlist, dataset->queryNormalizedSelector(), options.optimizeSortAllFieldsStrict))
        unstable = true;

    StringBuffer flags;
    if (expr->hasAttribute(firstAtom))
        flags.append("|TQFfirst");
    if (expr->hasAttribute(lastAtom))
        flags.append("|TQFlast");
    if (isAlreadySorted(dataset, sortlist, false, false, false))
        flags.append("|TQFsorted|TQFlocalsorted");
    else if (isAlreadySorted(dataset, sortlist, true, false, false))
        flags.append("|TQFlocalsorted");
    if (score)
        flags.append("|TQFhasscore");
    if (range)
        flags.append("|TQFhasrange");
    if (skew)
        flags.append("|TQFhasskew");
    if (dedupAttr)
        flags.append("|TQFdedup");
    if (unstable)
        flags.append("|TQFunstable");
    if (!number->queryValue())
        flags.append("|TQFvariabledivisions");
    if (!transformReturnsSide(expr, no_left, 0))
        flags.append("|TQFneedtransform");

    if (flags.length())
        instance->classctx.addQuotedF("virtual unsigned getFlags() { return %s; }", flags.str()+1);

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

void HqlCppTranslator::doBuildXmlReadMember(ActivityInstance & instance, IHqlExpression * expr, const char * functionName, bool & usesContents)
{
    IHqlExpression * mode = expr->queryChild(2);
    node_operator modeType = mode->getOperator();

    StringBuffer s, xmlInstanceName;
    usesContents = false;
    if (isValidXmlRecord(expr->queryRecord()))
    {
        StringBuffer xmlFactoryName;
        getUniqueId(xmlInstanceName.append((modeType==no_json) ? "json" : "xml"));

        buildXmlReadTransform(expr, xmlFactoryName, usesContents);
        instance.classctx.addQuoted(s.clear().append("Owned<IXmlToRowTransformer> ").append(xmlInstanceName).append(";"));

        BuildCtx * callctx = NULL;
        instance.evalContext->getInvariantMemberContext(NULL, NULL, &callctx, true, false);
        callctx->addQuoted(s.clear().append(xmlInstanceName).append(".setown(").append(xmlFactoryName).append("(ctx,").append(instance.activityId).append("));"));
    }
    else
        xmlInstanceName.append("NULL");

    s.clear().append("virtual IXmlToRowTransformer * ").append(functionName).append("() { return ").append(xmlInstanceName).append("; }");
    instance.classctx.addQuoted(s);
}


ABoundActivity * HqlCppTranslator::doBuildActivityWorkunitRead(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * wuid = expr->queryAttribute(wuidAtom);
    IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryAttributeChild(expr, nameAtom, 0);

    __int64 sequenceValue = sequence->queryValue()->getIntValue();
    bool isStored = (sequenceValue == ResultSequenceStored);
    bool useImplementationClass = options.minimizeActivityClasses && !wuid && (sequenceValue == ResultSequenceInternal);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKworkunitread, expr,"WorkunitRead");
    if (useImplementationClass)
        instance->setImplementationClass(newWorkUnitReadArgId);

    noteResultAccessed(ctx, sequence, name);

    StringBuffer graphLabel;
    graphLabel.append(getActivityText(instance->kind)).append("\n");
    getStoredDescription(graphLabel, sequence, name, true);
    instance->graphLabel.set(graphLabel.str());

    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    if (!useImplementationClass)
    {
        doBuildVarStringFunction(instance->classctx, "queryName", name);

        if (sequenceValue != ResultSequenceInternal)
        {
            MemberFunction func(*this, instance->classctx, "virtual int querySequence()");
            buildReturn(func.ctx, sequence, signedType);
        }

        if (wuid)
            doBuildVarStringFunction(instance->startctx, "getWUID", wuid->queryChild(0));

        bool usesContents = false;
        if (isStored || (targetRoxie() && (sequenceValue >= 0)))
            doBuildXmlReadMember(*instance, expr, "queryXmlTransformer", usesContents);

        StringBuffer csvInstanceName;
        if (isValidCsvRecord(expr->queryRecord()) && isStored && options.allowCsvWorkunitRead)
        {
            buildCsvReadTransformer(expr, csvInstanceName, NULL);

            StringBuffer s;
            s.append("virtual ICsvToRowTransformer * queryCsvTransformer() { return &").append(csvInstanceName).append("; }");
            instance->classctx.addQuoted(s);
        }
    }
    else
    {
        instance->addConstructorParameter(name);
    }

    queryAddResultDependancy(*instance->queryBoundActivity(), sequence, name);
    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
//-- xmlparse

void HqlCppTranslator::noteXpathUsed(const char * xpath)
{
    if (strstr(xpath, XPATH_CONTENTS_TEXT))
        xmlUsesContents = true;
}


void HqlCppTranslator::noteXpathUsed(IHqlExpression * expr)
{
    IValue * value = expr->queryValue();
    if (value)
    {
        StringBuffer temp;
        value->getStringValue(temp);
        noteXpathUsed(temp);
    }
    else
        xmlUsesContents = true;
}


ABoundActivity * HqlCppTranslator::doBuildActivityXmlParse(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * selSeq = querySelSeq(expr);
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKxmlparse, expr, "XmlParse");

    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    IHqlExpression * xmlAttr = expr->queryAttribute(xmlAtom);
    //MORE: What encoding is the search text in???

    doBuildParseSearchText(instance->startctx, dataset, expr->queryChild(1), type_utf8, unknownStringType);
    doBuildVarStringFunction(instance->classctx, "getXmlIteratorPath", xmlAttr ? queryRealChild(xmlAttr, 0) : NULL);

    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left, IColumnProvider * parsed)");
        ensureRowAllocated(func.ctx, "crSelf");
        func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");

        // Both left and the dataset are bound to left because it might be a new transform or a transform
        IHqlExpression * transform = expr->queryChild(3);
        BoundRow * selfCursor = bindSelf(func.ctx, expr, "crSelf");
        if (transform->getOperator() == no_newtransform)
            bindTableCursor(func.ctx, dataset, "left");
        else
            bindTableCursor(func.ctx, dataset, "left", no_left, selSeq);
        associateSkipReturnMarker(func.ctx, queryZero(), selfCursor);

        OwnedHqlExpr helperName = createQuoted("parsed", makeBoolType());
        func.ctx.associateExpr(xmlColumnProviderMarkerExpr, helperName);
        bindTableCursor(func.ctx, queryXmlParsePseudoTable(), queryXmlParsePseudoTable());
        xmlUsesContents = false;
        doTransform(func.ctx, transform, selfCursor);
        buildReturnRecordSize(func.ctx, selfCursor);
    }

    if (xmlUsesContents)
        instance->classctx.addQuotedLiteral("virtual bool requiresContents() { return true; }");

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}


void HqlCppTranslator::doBuildExprXmlText(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    HqlExprAssociation * match = ctx.queryMatchExpr(xmlColumnProviderMarkerExpr);
    if (!match)
        throwError(HQLERR_XmlTextNotValid);

    IHqlExpression * xpath = expr->queryChild(0);
    noteXpathUsed(xpath);

    HqlExprArray args;
    args.append(*LINK(match->queryExpr()));
    args.append(*LINK(xpath));

    OwnedHqlExpr call = bindFunctionCall(columnGetStringXId, args);
    buildCachedExpr(ctx, call, tgt);
}


void HqlCppTranslator::doBuildExprXmlUnicode(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    HqlExprAssociation * match = ctx.queryMatchExpr(xmlColumnProviderMarkerExpr);
    if (!match)
        throwError(HQLERR_XmlUnicodeNotValid);

    IHqlExpression * xpath = expr->queryChild(0);
    noteXpathUsed(xpath);

    HqlExprArray args;
    args.append(*LINK(match->queryExpr()));
    args.append(*LINK(xpath));

    OwnedHqlExpr call = bindFunctionCall(columnGetUnicodeXId, args);
    buildCachedExpr(ctx, call, tgt);
}


void HqlCppTranslator::buildDatasetAssignXmlProject(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr)
{
    HqlExprAssociation * match = ctx.queryMatchExpr(xmlColumnProviderMarkerExpr);
    if (!match)
        throwError(HQLERR_XmlTextNotValid);

    StringBuffer iterTag;
    IHqlExpression * xpath = expr->queryChild(0);
    if (xpath->queryValue())
        xpath->queryValue()->getStringValue(iterTag);

    noteXpathUsed(xpath);

    //Generate the code to process a child iterator
    OwnedHqlExpr subRowExpr;
    BuildCtx loopctx(ctx);
    buildXmlReadChildrenIterator(loopctx, iterTag.str(), match->queryExpr(), subRowExpr);
    loopctx.associateExpr(xmlColumnProviderMarkerExpr, subRowExpr);

    BoundRow * targetRow = target->buildCreateRow(loopctx);
    Owned<IReferenceSelector> targetRef = buildActiveRow(loopctx, targetRow->querySelector());
    OwnedHqlExpr rowValue = createRow(no_createrow, LINK(expr->queryChild(1)));
    buildRowAssign(loopctx, targetRef, rowValue);
    target->finishRow(loopctx, targetRow);
}


//---------------------------------------------------------------------------
//-- no_temptable [DATASET] --

void HqlCppTranslator::doBuildTempTableFlags(BuildCtx & ctx, IHqlExpression * expr, bool isConstant, bool canFilter)
{
    StringBuffer flags;
    if (expr->hasAttribute(distributedAtom))
        flags.append("|TTFdistributed");
    if (!isConstant)
        flags.append("|TTFnoconstant");
    if (canFilter)
        flags.append("|TTFfiltered");

    if (flags.length())
        doBuildUnsignedFunction(ctx, "getFlags", flags.str()+1);
}

ABoundActivity * HqlCppTranslator::doBuildActivityTempTable(BuildCtx & ctx, IHqlExpression * expr)
{
    StringBuffer s;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKinlinetable, expr, "InlineTable");

    OwnedHqlExpr values = normalizeListCasts(expr->queryChild(0));
    IHqlExpression * record = expr->queryChild(1);

    assertex(values->getOperator() != no_recordlist);       // should have been transformed by now.

    //-----------------
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    OwnedHqlExpr rowsExpr;
    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t getRow(ARowBuilder & crSelf, __uint64 row)");
        ensureRowAllocated(func.ctx, "crSelf");
        BoundRow * selfCursor = bindSelf(func.ctx, instance->dataset, "crSelf");
        IHqlExpression * self = selfCursor->querySelector();
        OwnedHqlExpr clearAction;

        OwnedHqlExpr rowVar = createVariable("row", makeIntType(8, false));
        if (expr->getOperator() == no_datasetfromrow)
        {
            BuildCtx subctx(func.ctx);
            subctx.addFilter(rowVar);
            if (clearAction)
                subctx.addExpr(clearAction);
            subctx.addReturn(queryZero());

            buildAssign(func.ctx, self, values);
            buildReturnRecordSize(func.ctx, selfCursor);
            rowsExpr.setown(getSizetConstant(1));
        }
        else if ((values->getOperator() == no_list) || (values->getOperator() == no_null))
        {
            unsigned maxRows = values->numChildren();
            if (maxRows)
            {
                unsigned dummyIdx = 0;
                OwnedHqlExpr tgt = createSelectExpr(LINK(self), LINK(queryNextRecordField(record, dummyIdx)));

                BuildCtx switchctx(func.ctx);
                switchctx.addQuotedCompoundLiteral("switch (row)");

                unsigned row;
                for (row = 0; row < maxRows; row++)
                {
                    BuildCtx casectx(switchctx);
                    casectx.addQuotedCompound(s.clear().append("case ").append(row).append(":"), nullptr);

                    buildAssign(casectx, tgt, values->queryChild(row));
                    buildReturnRecordSize(casectx, selfCursor);
                }
            }

            if (clearAction)
                func.ctx.addExpr(clearAction);
            func.ctx.addReturn(queryZero());

            rowsExpr.setown(getSizetConstant(maxRows));
        }
        else
        {
            CHqlBoundExpr bound;
            //MORE: This shouldn't be done this way...
            OwnedHqlExpr normalized = normalizeListCasts(values);
            if (options.spotCSE)
                normalized.setown(spotScalarCSE(normalized, NULL, queryOptions().spotCseInIfDatasetConditions));

            if (normalized->getOperator() == no_alias)
            {
                buildExpr(func.ctx, normalized, bound);
                rowsExpr.setown(createValue(no_countlist, makeIntType(8, false), LINK(normalized)));
            }
            else
            {
                BuildCtx * declarectx;
                BuildCtx * callctx;
                instance->evalContext->getInvariantMemberContext(NULL, &declarectx, &callctx, false, isContextDependent(normalized, false, false) || !isIndependentOfScope(normalized));
    //          if (isContextDependent(normalized, false, false))
    //              buildTempExpr(instance->onstartctx, *declarectx, normalized, bound, FormatNatural);
    //          else
                CHqlBoundTarget tempTarget;
                buildTempExpr(*callctx, *declarectx, tempTarget, normalized, FormatNatural, !canSetBeAll(normalized));
                bound.setFromTarget(tempTarget);
                rowsExpr.setown(getBoundCount(bound));
                rowsExpr.setown(createTranslated(rowsExpr));
            }

            OwnedHqlExpr compare = createValue(no_ge, makeBoolType(), LINK(rowVar), ensureExprType(rowsExpr, rowVar->queryType()));
            BuildCtx condctx(func.ctx);
            buildFilter(condctx, compare);
            if (clearAction)
                condctx.addExpr(clearAction);
            buildReturn(condctx, queryZero());

            HqlExprArray args;
            args.append(*bound.getTranslatedExpr());
            args.append(*adjustValue(rowVar, 1));
            args.append(*createAttribute(noBoundCheckAtom));
            args.append(*createAttribute(forceAllCheckAtom));
            OwnedHqlExpr src = createValue(no_index, LINK(values->queryType()->queryChildType()), args);
            OwnedHqlExpr tgt = createSelectExpr(LINK(self), LINK(queryOnlyField(record)));
            buildAssign(func.ctx, tgt, src);
            buildReturnRecordSize(func.ctx, selfCursor);
        }
    }

    doBuildUnsigned64Function(instance->startctx, "numRows", rowsExpr);

    doBuildTempTableFlags(instance->startctx, expr, values->isConstant(), false);

    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}

//NB: Also used to create row no_null as an activity.
ABoundActivity * HqlCppTranslator::doBuildActivityCreateRow(BuildCtx & ctx, IHqlExpression * expr, bool isDataset)
{
    bool valuesAreConstant = false;
    StringBuffer valueText;
    IValue * singleValue = NULL;
    node_operator op = expr->getOperator();
    if (op == no_createrow)
    {
        IHqlExpression * transform = expr->queryChild(0);
        if (transform->isConstant())
        {
            IHqlExpression * assign = queryTransformSingleAssign(transform);
            if (assign)
            {
                singleValue = assign->queryChild(1)->queryValue();
                if (singleValue)
                    singleValue->generateECL(valueText);
            }
            valuesAreConstant = true;
        }
        if (!isDataset && containsSkip(transform))
            reportError(queryLocation(transform), ECODETEXT(HQLERR_SkipInsideCreateRow));
    }
    else if (op == no_null)
    {
        valueText.append("Blank");
        valuesAreConstant = true;
    }

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKinlinetable, expr, "InlineRow");
    if (valueText.length())
    {
        StringBuffer graphLabel;
        elideString(valueText, MAX_ROW_VALUE_TEXT_LEN);
        graphLabel.append("Inline Row\n{").append(valueText).append("}");
        instance->graphLabel.set(graphLabel.str());
    }
    else
        instance->graphLabel.set("Inline Row");


    //-----------------
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    {
// Ignoring row argument, since engines will stop at numRows(), which is 1
        MemberFunction func(*this, instance->startctx, "virtual size32_t getRow(ARowBuilder & crSelf, __uint64 row)");
        ensureRowAllocated(func.ctx, "crSelf");
        BoundRow * selfCursor = bindSelf(func.ctx, instance->dataset, "crSelf");
        IHqlExpression * self = selfCursor->querySelector();

        if (isDataset)
            associateSkipReturnMarker(func.ctx, queryZero(), selfCursor);

        LinkedHqlExpr cseExpr = expr;
        if (options.spotCSE)
            cseExpr.setown(spotScalarCSE(cseExpr, NULL, options.spotCseInIfDatasetConditions));

        buildAssign(func.ctx, self, cseExpr);
        buildReturnRecordSize(func.ctx, selfCursor);
    }

    doBuildTempTableFlags(instance->startctx, expr, valuesAreConstant, false);

    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityInlineTable(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * values = expr->queryChild(0);
    if (values->numChildren() == 1)
    {
        OwnedHqlExpr rowValue = createRow(no_createrow, LINK(values->queryChild(0)));
        OwnedHqlExpr row = expr->cloneAllAnnotations(rowValue);
        return doBuildActivityCreateRow(ctx, row, true);
    }

    if (values->isConstant() && !expr->hasAttribute(distributedAtom))
    {
        CHqlBoundExpr bound;
        if (doBuildConstantDatasetInlineTable(expr, bound, FormatNatural))
        {
            OwnedHqlExpr constDataset = bound.getTranslatedExpr();

            Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKlinkedrawiterator, expr, "LinkedRawIterator");
            instance->graphLabel.set("Inline Dataset");
            instance->setImplementationClass(newLibraryConstantRawIteratorArgId);
            buildActivityFramework(instance);

            buildInstancePrefix(instance);
            instance->addConstructorParameter(constDataset);
            buildInstanceSuffix(instance);

            return instance->getBoundActivity();
        }
    }

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKinlinetable, expr, "InlineTable");

    //-----------------
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    unsigned maxRows = values->numChildren();
    bool canFilter = false;

    {
        MemberFunction func(*this, instance->startctx, "virtual size32_t getRow(ARowBuilder & crSelf, __uint64 row)");
        associateSkipReturnMarker(func.ctx, queryZero(), NULL);

        ensureRowAllocated(func.ctx, "crSelf");
        BoundRow * selfCursor = bindSelf(func.ctx, instance->dataset, "crSelf");
        IHqlExpression * self = selfCursor->querySelector();

        if (maxRows)
        {
            StringBuffer s;
            BuildCtx switchctx(func.ctx);
            switchctx.addQuotedCompoundLiteral("switch (row)");

            unsigned row;
            for (row = 0; row < maxRows; row++)
            {
                BuildCtx casectx(switchctx);
                casectx.addQuotedCompound(s.clear().append("case ").append(row).append(":"), nullptr);

                IHqlExpression * cur = values->queryChild(row);
                if (containsSkip(cur))
                    canFilter = true;
                OwnedHqlExpr rowValue = createRow(no_createrow, LINK(cur));
                buildAssign(casectx, self, rowValue);
                buildReturnRecordSize(casectx, selfCursor);
            }
        }
        func.ctx.addReturn(queryZero());
    }

    OwnedHqlExpr rowsExpr = getSizetConstant(maxRows);
    doBuildUnsigned64Function(instance->startctx, "numRows", rowsExpr);

    doBuildTempTableFlags(instance->startctx, expr, values->isConstant(), canFilter);

    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityCountTransform(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * count = expr->queryChild(0);
    IHqlExpression * transform = queryNewColumnProvider(expr);
    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKinlinetable, expr, "InlineTable");
    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    {
        // size32_t getRow()
        MemberFunction func(*this, instance->startctx, "virtual size32_t getRow(ARowBuilder & crSelf, __uint64 row)");
        ensureRowAllocated(func.ctx, "crSelf");
        BoundRow * selfCursor = bindSelf(func.ctx, instance->dataset, "crSelf");
        IHqlExpression * self = selfCursor->querySelector();
        associateCounter(func.ctx, counter, "(row+1)");
        buildTransformBody(func.ctx, transform, NULL, NULL, instance->dataset, self);
    }

    // unsigned numRows() - count is guaranteed by lexer
    doBuildUnsigned64Function(instance->startctx, "numRows", count);

    // unsigned getFlags()
    doBuildTempTableFlags(instance->startctx, expr, isConstantTransform(transform), containsSkip(transform));

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

void HqlCppTranslator::buildHTTPtoXml(BuildCtx & ctx)
{
    MemberFunction func(*this, ctx, "virtual void toXML(const byte *, IXmlWriter & out)");
    //Contains nothing
}

//---------------------------------------------------------------------------

void HqlCppTranslator::buildSOAPtoXml(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * transform, IHqlExpression * selSeq)
{
    MemberFunction func(*this, ctx);

    //virtual void toXML(const byte * self, StringBuffer & out) = 0;
    if (dataset)
    {
        func.start("virtual void toXML(const byte * left, IXmlWriter & out)");
        if (transform->getOperator() == no_newtransform)
            bindTableCursor(func.ctx, dataset, "left");
        else
            bindTableCursor(func.ctx, dataset, "left", no_left, selSeq);
    }
    else
        func.start("virtual void toXML(const byte *, IXmlWriter & out)");

    // Bind left to "left" and right to RIGHT
    HqlExprArray assigns;
    filterExpandAssignments(func.ctx, NULL, assigns, transform);
    OwnedHqlExpr self = getSelf(transform);
    buildXmlSerialize(func.ctx, transform->queryRecord(), self, &assigns);
}

void HqlCppTranslator::associateLocalJoinTransformFlags(BuildCtx & ctx, const char * name, IHqlExpression *ds, node_operator side, IHqlExpression *selSeq)
{
    __int64 mask = 0;
    if (side==no_right)
        mask = JTFmatchedright;
    else if (side==no_left)
        mask = JTFmatchedleft;

    OwnedIValue maskValue = createIntValue(mask, 4, false);
    OwnedHqlExpr flagsVariable = createVariable(name, makeIntType(4, false));
    OwnedHqlExpr matchedRowExpr  = createValue(no_band, makeIntType(4, false), flagsVariable.getClear(), createConstant(maskValue.getClear()));

    OwnedHqlExpr markerExpr = createValue(no_matched_injoin, makeBoolType(), createSelector(side, ds, selSeq));
    OwnedHqlExpr testExpr = createValue(no_ne, makeBoolType(), matchedRowExpr.getClear(), createConstant(createIntValue(0, 4, false)));
    ctx.associateExpr(markerExpr,  testExpr);
}

IHqlExpression * HqlCppTranslator::associateLocalFailure(BuildCtx & ctx, const char * exceptionName)
{
    OwnedHqlExpr activeFailMarker = createAttribute(activeFailureAtom);
    OwnedHqlExpr activeFailVariable = createVariable(exceptionName, makeBoolType());
    ctx.associateExpr(activeFailMarker,  activeFailVariable);
    return activeFailVariable;
}

void HqlCppTranslator::validateExprScope(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * expr, const char * opName, const char * argName)
{
    if (dataset && exprReferencesDataset(expr, dataset) && !resolveSelectorDataset(ctx, dataset))
        throwError2(HQLERR_OpArgDependsDataset, opName, argName);
}

void HqlCppTranslator::doBuildHttpHeaderStringFunction(BuildCtx &ctx, IHqlExpression * expr)
{
    HqlExprArray headerExprs;
    gatherAttributes(headerExprs, httpHeaderAtom, expr);
    if (headerExprs.length())
    {
        Owned<ITypeInfo> string2Type = makeStringType(2);
        OwnedHqlExpr endName = createConstant(createStringValue(": ", LINK(string2Type)));
        OwnedHqlExpr endLine = createConstant(createStringValue("\r\n", LINK(string2Type)));

        HqlExprArray headerStringExprs;
        ForEachItemIn(i, headerExprs)
        {
            IHqlExpression * httpHeader = &headerExprs.item(i);
            headerStringExprs.append(*LINK(httpHeader->queryChild(0)));
            headerStringExprs.append(*LINK(endName));
            headerStringExprs.append(*LINK(httpHeader->queryChild(1)));
            headerStringExprs.append(*LINK(endLine));
        }

        OwnedHqlExpr concatHeaders = createBalanced(no_concat, unknownVarStringType, headerStringExprs);
        concatHeaders.setown(foldHqlExpression(concatHeaders));
        doBuildVarStringFunction(ctx, "getHttpHeaders", concatHeaders);
    }

}

ABoundActivity * HqlCppTranslator::doBuildActivitySOAP(BuildCtx & ctx, IHqlExpression * expr, bool isSink, bool isRoot)
{
    ThorActivityKind tak;
    const char * helper;
    unsigned firstArg = 0;
    IHqlExpression * dataset = NULL;
    Owned<ABoundActivity> boundDataset;
    IHqlExpression * selSeq = querySelSeq(expr);
    if (expr->getOperator() == no_newsoapcall)
    {
        if (isSink)
        {
            tak = TAKsoap_rowaction;
            helper = "SoapAction";
        }
        else
        {
            tak = TAKsoap_rowdataset;
            helper = "SoapCall";
        }
    }
    else
    {
        if (isSink)
        {
            tak = TAKsoap_datasetaction;
            helper = "SoapAction";
        }
        else
        {
            tak = TAKsoap_datasetdataset;
            helper = "SoapCall";
        }
        dataset = expr->queryChild(0);
        boundDataset.setown(buildCachedActivity(ctx, dataset));
        firstArg = 1;
    }

    StringBuffer s;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, tak, expr, helper);

    //-----------------
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    IHqlExpression * hosts = expr->queryChild(firstArg);
    IHqlExpression * service = expr->queryChild(firstArg+1);

    //Because of scope handling in parser it is possible for these expressions to be unexpectedly dependent on the dataset
    const char * opText = getOpString(expr->getOperator());
    validateExprScope(instance->startctx, dataset, hosts, opText, "host url");
    validateExprScope(instance->startctx, dataset, service, opText, "service");

    //virtual const char * getHosts() = 0;
    doBuildVarStringFunction(instance->startctx, "getHosts", hosts);

    //virtual const char * getService() = 0;
    doBuildVarStringFunction(instance->startctx, "getService", service);

    //virtual void toXML(const byte * self, StringBuffer & out) = 0;
    buildSOAPtoXml(instance->startctx, dataset, expr->queryChild(firstArg+3), selSeq);

    //virtual const char * queryOutputIteratorPath()
    IHqlExpression * separator = expr->queryAttribute(separatorAtom);
    if (separator)
        doBuildVarStringFunction(instance->startctx, "queryOutputIteratorPath", separator->queryChild(0));

    //virtual const char * getHeader()
    //virtual const char * getFooter()
    IHqlExpression * header = expr->queryAttribute(headingAtom);
    if (header)
    {
        doBuildVarStringFunction(instance->startctx, "getHeader", header->queryChild(0));
        doBuildVarStringFunction(instance->startctx, "getFooter", header->queryChild(1));
    }

    IHqlExpression * action = expr->queryAttribute(soapActionAtom);
    if (action)
        doBuildVarStringFunction(instance->startctx, "getSoapAction", action->queryChild(0));

    doBuildHttpHeaderStringFunction(instance->startctx, expr);

    IHqlExpression * proxyAddress = expr->queryAttribute(proxyAddressAtom);
    if (proxyAddress)
        doBuildVarStringFunction(instance->startctx, "getProxyAddress", proxyAddress->queryChild(0));

    IHqlExpression * namespaceAttr = expr->queryAttribute(namespaceAtom);
    IHqlExpression * responseAttr = expr->queryAttribute(responseAtom);
    IHqlExpression * logText = NULL;
    bool logMin = false;
    bool logXml = false;
    ForEachChildFrom(i, expr, 1)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (cur->isAttribute() && cur->queryName()==logAtom)
        {
            IHqlExpression * opt = cur->queryChild(0);
            if (!opt)
                logXml = true;
            else if (!opt->isAttribute())
                logText = opt;
            else if (opt->queryName() == minAtom)
                logMin = true;
        }
    }

    //virtual unsigned numParallelThreads()
    doBuildUnsignedFunction(instance->startctx, "numParallelThreads", queryAttributeChild(expr, parallelAtom, 0));

    //virtual unsigned numRecordsPerBatch()
    doBuildUnsignedFunction(instance->startctx, "numRecordsPerBatch", queryAttributeChild(expr, mergeAtom, 0));

    //virtual int numRetries()
    doBuildSignedFunction(instance->startctx, "numRetries", queryAttributeChild(expr, retryAtom, 0));

    //virtual double getTimeout()
    doBuildDoubleFunction(instance->startctx, "getTimeout", queryAttributeChild(expr, timeoutAtom, 0));

    //virtual double getTimeLimit()
    doBuildDoubleFunction(instance->startctx, "getTimeLimit", queryAttributeChild(expr, timeLimitAtom, 0));

    if (namespaceAttr)
    {
        doBuildVarStringFunction(instance->startctx, "getNamespaceName", namespaceAttr->queryChild(0));
        if (namespaceAttr->queryChild(1))
            doBuildVarStringFunction(instance->startctx, "getNamespaceVar", namespaceAttr->queryChild(1));
    }

    if (logText)
    {
        MemberFunction func(*this, instance->startctx, "virtual void getLogText(size32_t & __lenResult, char * & __result, const void * _left)");
        if (dataset)
        {
            func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
            bindTableCursor(func.ctx, dataset, "left");
            bindTableCursor(func.ctx, dataset, "left", no_left, selSeq);
        }
        doBuildFunctionReturn(func.ctx, unknownStringType, logText);
    }
    bool usesContents = false;
    if (!isSink)
    {
        //virtual IXmlToRowTransformer * queryTransformer()
        doBuildXmlReadMember(*instance, expr, "queryInputTransformer", usesContents);

        //virtual const char * getInputIteratorPath()
        IHqlExpression * xpath = expr->queryAttribute(xpathAtom);
        if (xpath)
            doBuildVarStringFunction(instance->classctx, "getInputIteratorPath", xpath->queryChild(0));

        IHqlExpression * onFail = expr->queryAttribute(onFailAtom);
        if (onFail)
        {
            IHqlExpression * onFailTransform = onFail->queryChild(0);
            if (onFailTransform->isTransform())
                assertex(recordTypesMatch(expr, onFailTransform));

            //virtual unsigned onFailTransform(ARowBuilder & crSelf, const void * _left, IException * e)
            MemberFunction func(*this, instance->startctx, "virtual unsigned onFailTransform(ARowBuilder & crSelf, const void * _left, IException * except)");
            ensureRowAllocated(func.ctx, "crSelf");
            associateLocalFailure(func.ctx, "except");
            buildTransformBody(func.ctx, onFailTransform, dataset, NULL, expr, selSeq);
        }
    }
    //virtual unsigned getFlags()
    {
        StringBuffer flags;
        if (expr->hasAttribute(groupAtom))
            flags.append("|SOAPFgroup");
        if (expr->hasAttribute(onFailAtom))
            flags.append("|SOAPFonfail");
        if (logXml)
            flags.append("|SOAPFlog");
        if (expr->hasAttribute(trimAtom))
            flags.append("|SOAPFtrim");
        if (expr->hasAttribute(literalAtom))
            flags.append("|SOAPFliteral");
        if (namespaceAttr)
            flags.append("|SOAPFnamespace");
        if (expr->hasAttribute(encodingAtom))
            flags.append("|SOAPFencoding");
        if (responseAttr && responseAttr->hasAttribute(noTrimAtom))
            flags.append("|SOAPFpreserveSpace");
        if (logMin)
            flags.append("|SOAPFlogmin");
        if (logText)
            flags.append("|SOAPFlogusermsg");
        if (expr->hasAttribute(httpHeaderAtom))
            flags.append("|SOAPFhttpheaders");
        if (usesContents)
            flags.append("|SOAPFusescontents");

        if (flags.length())
            doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);
    }
    buildInstanceSuffix(instance);
    if (boundDataset)
        buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    if (isSink)
        return NULL;
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityHTTP(BuildCtx & ctx, IHqlExpression * expr, bool isSink, bool isRoot)
{
    ThorActivityKind tak;
    const char * helper = "HttpCall";
    unsigned firstArg = 0;
    IHqlExpression * dataset = NULL;
    Owned<ABoundActivity> boundDataset;
    IHqlExpression * selSeq = querySelSeq(expr);
    assertex(!isSink);
    tak = TAKhttp_rowdataset;

    StringBuffer s;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, tak, expr, helper);

    //-----------------
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    //virtual const char * getHosts() = 0;
    doBuildVarStringFunction(instance->startctx, "getHosts", expr->queryChild(firstArg));

    //virtual const char * getService() = 0;
    doBuildVarStringFunction(instance->startctx, "getService", expr->queryChild(firstArg+1));

    //virtual const char * getAcceptType() = 0;
    doBuildVarStringFunction(instance->startctx, "getAcceptType", expr->queryChild(firstArg+2));

    //virtual void toXML(const byte * self, StringBuffer & out) = 0;
    buildHTTPtoXml(instance->startctx);

    doBuildHttpHeaderStringFunction(instance->startctx, expr);

    IHqlExpression * proxyAddress = expr->queryAttribute(proxyAddressAtom);
    if (proxyAddress)
        doBuildVarStringFunction(instance->startctx, "getProxyAddress", proxyAddress->queryChild(0));

    //virtual const char * queryOutputIteratorPath()
    IHqlExpression * separator = expr->queryAttribute(separatorAtom);
    if (separator)
        doBuildVarStringFunction(instance->startctx, "queryOutputIteratorPath", separator->queryChild(0));

    IHqlExpression * namespaceAttr = expr->queryAttribute(namespaceAtom);
    IHqlExpression * logText = NULL;
    bool logMin = false;
    bool logXml = false;
    ForEachChildFrom(i, expr, 1)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (cur->isAttribute() && cur->queryName()==logAtom)
        {
            IHqlExpression * opt = cur->queryChild(0);
            if (!opt)
                logXml = true;
            else if (!opt->isAttribute())
                logText = opt;
            else if (opt->queryName() == minAtom)
                logMin = true;
        }
    }

    //virtual unsigned numParallelThreads()
    doBuildUnsignedFunction(instance->classctx, "numParallelThreads", queryAttributeChild(expr, parallelAtom, 0));

    //virtual unsigned numRecordsPerBatch()
    doBuildUnsignedFunction(instance->classctx, "numRecordsPerBatch", queryAttributeChild(expr, mergeAtom, 0));

    //virtual int numRetries()
    doBuildSignedFunction(instance->classctx, "numRetries", queryAttributeChild(expr, retryAtom, 0));

    //virtual double getTimeout()
    doBuildDoubleFunction(instance->classctx, "getTimeout", queryAttributeChild(expr, timeoutAtom, 0));

    //virtual double getTimeLimit()
    doBuildDoubleFunction(instance->classctx, "getTimeLimit", queryAttributeChild(expr, timeLimitAtom, 0));

    if (namespaceAttr)
    {
        doBuildVarStringFunction(instance->startctx, "getNamespaceName", namespaceAttr->queryChild(0));
        if (namespaceAttr->queryChild(1))
            doBuildVarStringFunction(instance->startctx, "getNamespaceVar", namespaceAttr->queryChild(1));
    }

    if (logText)
    {
        MemberFunction func(*this, instance->startctx, "virtual void getLogText(size32_t & __lenResult, char * & __result, const void * _left)");
        doBuildFunctionReturn(func.ctx, unknownStringType, logText);
    }

    bool usesContents = false;
    if (!isSink)
    {
        //virtual IXmlToRowTransformer * queryTransformer()
        doBuildXmlReadMember(*instance, expr, "queryInputTransformer", usesContents);

        //virtual const char * getInputIteratorPath()
        IHqlExpression * xpath = expr->queryAttribute(xpathAtom);
        if (xpath)
            doBuildVarStringFunction(instance->classctx, "getInputIteratorPath", xpath->queryChild(0));

        IHqlExpression * onFail = expr->queryAttribute(onFailAtom);
        if (onFail)
        {
            MemberFunction func(*this, instance->startctx, "virtual unsigned onFailTransform(ARowBuilder & crSelf, const void * _left, IException * except)");
            ensureRowAllocated(func.ctx, "crSelf");
            associateLocalFailure(func.ctx, "except");
            buildTransformBody(func.ctx, onFail->queryChild(0), dataset, NULL, expr, selSeq);
        }
    }
    //virtual unsigned getFlags()
    {
        StringBuffer flags;
        if (expr->hasAttribute(groupAtom))
            flags.append("|SOAPFgroup");
        if (expr->hasAttribute(onFailAtom))
            flags.append("|SOAPFonfail");
        if (logXml)
            flags.append("|SOAPFlog");
        if (expr->hasAttribute(trimAtom))
            flags.append("|SOAPFtrim");
        if (expr->hasAttribute(literalAtom))
            flags.append("|SOAPFliteral");
        if (namespaceAttr)
            flags.append("|SOAPFnamespace");
        if (logMin)
            flags.append("|SOAPFlogmin");
        if (logText)
            flags.append("|SOAPFlogusermsg");
        if (expr->hasAttribute(httpHeaderAtom))
            flags.append("|SOAPFhttpheaders");
        if (usesContents)
            flags.append("|SOAPFusescontents");

        if (flags.length())
            doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);
    }
    buildInstanceSuffix(instance);
    if (boundDataset)
        buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    if (isSink)
        return NULL;

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

IHqlExpression * HqlCppTranslator::doBuildRegexCompileInstance(BuildCtx & ctx, IHqlExpression * pattern, bool isUnicode, bool isCaseSensitive)
{
    OwnedHqlExpr searchKey = createAttribute(_regexInstance_Atom, LINK(pattern), createConstant(isUnicode), createConstant(isCaseSensitive));
    HqlExprAssociation * match = ctx.queryMatchExpr(searchKey);
    if (match)
        return match->queryExpr();

    BuildCtx * initCtx = &ctx;
    BuildCtx * declareCtx = &ctx;
    if (pattern->isConstant())
        getInvariantMemberContext(ctx, &declareCtx, &initCtx, true, false);

    StringBuffer tempName;
    getUniqueId(tempName.append("regex"));
    ITypeInfo * type = makeClassType(isUnicode ? "rtlCompiledUStrRegex" : "rtlCompiledStrRegex");
    OwnedHqlExpr regexInstance = createVariable(tempName.str(), type);
    declareCtx->addDeclare(regexInstance);

    HqlExprArray args;
    args.append(*LINK(regexInstance));
    args.append(*LINK(pattern));
    args.append(*createConstant(isCaseSensitive));
    IIdAtom * func = isUnicode ? regexNewSetUStrPatternId : regexNewSetStrPatternId;
    buildFunctionCall(*initCtx, func, args);
    declareCtx->associateExpr(searchKey, regexInstance);

    return regexInstance;
}

IHqlExpression * HqlCppTranslator::doBuildRegexFindInstance(BuildCtx & ctx, IHqlExpression * compiled, IHqlExpression * search, bool cloneSearch)
{
    OwnedHqlExpr searchKey = createAttribute(_regexFindInstance_Atom, LINK(compiled), LINK(search), createConstant(cloneSearch));
    HqlExprAssociation * match = ctx.queryMatchExpr(searchKey);
    if (match)
        return match->queryExpr();

    bool isUnicode = isUnicodeType(search->queryType());
    StringBuffer tempName;
    getUniqueId(tempName.append("fi"));
    ITypeInfo * type = makeClassType(isUnicode ? "rtlUStrRegexFindInstance" : "rtlStrRegexFindInstance");
    OwnedHqlExpr regexInstance = createVariable(tempName.str(), type);
    ctx.addDeclare(regexInstance);

    //Would be better if I allowed classes in my external functions instead of faking booleans
    OwnedHqlExpr castCompiled = createValue(no_typetransfer, makeBoolType(), LINK(compiled));

    HqlExprArray args;
    args.append(*LINK(regexInstance));
    args.append(*createTranslated(castCompiled));
    args.append(*LINK(search));
    if (!isUnicode)
        args.append(*createConstant(cloneSearch));
    IIdAtom * func = isUnicode ? regexNewUStrFindId : regexNewStrFindId;
    buildFunctionCall(ctx, func, args);
    ctx.associateExpr(searchKey, regexInstance);

    return regexInstance;
}

void HqlCppTranslator::doBuildNewRegexFindReplace(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, CHqlBoundExpr * bound)
{
    CHqlBoundExpr boundMatch;
    if (ctx.getMatchExpr(expr, boundMatch))
    {
        if (bound)
            bound->set(boundMatch);
        else
            assign(ctx, *target, boundMatch);
        return;
    }

    IHqlExpression * pattern = expr->queryChild(0);
    IHqlExpression * search = expr->queryChild(1);
    bool isUnicode = isUnicodeType(search->queryType());
    IHqlExpression * compiled = doBuildRegexCompileInstance(ctx, pattern, isUnicode, !expr->hasAttribute(noCaseAtom));

    if (expr->getOperator() == no_regex_replace)
    {
        HqlExprArray args;
        args.append(*LINK(compiled));
        args.append(*LINK(search));
        args.append(*LINK(expr->queryChild(2)));
        IIdAtom * func = isUnicode ? regexNewUStrReplaceXId : regexNewStrReplaceXId;
        OwnedHqlExpr call = bindFunctionCall(func, args);
        //Need to associate???
        buildExprOrAssign(ctx, target, call, bound);
        return;
    }

    // Because the search instance is created locally, the search parameter is always going to be valid
    // as long as the find instance.  Only exception could be if call created a temporary class instance.
    bool cloneSearch = false;
    IHqlExpression * findInstance = doBuildRegexFindInstance(ctx, compiled, search, cloneSearch);
    if(expr->queryType() == queryBoolType())
    {
        HqlExprArray args;
        args.append(*LINK(findInstance));
        IIdAtom * func= isUnicode ? regexNewUStrFoundId : regexNewStrFoundId;
        OwnedHqlExpr call = bindFunctionCall(func, args);
        buildExprOrAssign(ctx, target, call, bound);
    }
    else
    {
        HqlExprArray args;
        args.append(*LINK(findInstance));
        args.append(*LINK(expr->queryChild(2)));
        IIdAtom * func= isUnicode ? regexNewUStrFoundXId : regexNewStrFoundXId;
        OwnedHqlExpr call = bindFunctionCall(func, args);
        buildExprOrAssign(ctx, target, call, bound);
    }
}

void HqlCppTranslator::doBuildExprRegexFindReplace(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & bound)
{
    doBuildNewRegexFindReplace(ctx, NULL, expr, &bound);
}


void HqlCppTranslator::doBuildAssignRegexFindReplace(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr)
{
    doBuildNewRegexFindReplace(ctx, &target, expr, NULL);
}

void HqlCppTranslator::doBuildExprRegexFindSet(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & bound)
{
    CHqlBoundExpr boundMatch;
    if (ctx.getMatchExpr(expr, boundMatch))
    {
        bound.set(boundMatch);
        return;
    }

    IHqlExpression * pattern = expr->queryChild(0);
    IHqlExpression * search = expr->queryChild(1);
    bool isUnicode = isUnicodeType(search->queryType());
    IHqlExpression * compiled = doBuildRegexCompileInstance(ctx, pattern, isUnicode, !expr->hasAttribute(noCaseAtom));

    HqlExprArray args;
    args.append(*LINK(compiled));
    args.append(*LINK(search));
    IIdAtom * func = isUnicode ? regexUStrMatchSetId : regexMatchSetId;
    OwnedHqlExpr call = bindFunctionCall(func, args);
    buildExprOrAssign(ctx, NULL, call, &bound);
}

//---------------------------------------------------------------------------

void HqlCppTranslator::buildStartTimer(BuildCtx & ctx, CHqlBoundExpr & boundTimer, CHqlBoundExpr & boundStart, const char * name)
{
    BuildCtx * initCtx = &ctx;
    BuildCtx * declareCtx = &ctx;
    getInvariantMemberContext(ctx, &declareCtx, &initCtx, true, false);

    unsigned activityId = 0;
    ActivityInstance * activity = queryCurrentActivity(ctx);
    if (activity)
        activityId = activity->activityId;

    HqlExprArray registerArgs;
    registerArgs.append(*getSizetConstant(activityId));
    registerArgs.append(*createConstant(name));
    OwnedHqlExpr call = bindFunctionCall(registerTimerId, registerArgs);

    if (!declareCtx->getMatchExpr(call, boundTimer))
    {
        Owned<ITypeInfo> timerType = makePointerType(makeClassType("ISectionTimer"));
        OwnedHqlExpr timer = declareCtx->getTempDeclare(timerType, NULL);
        boundTimer.expr.set(timer);
        declareCtx->associateExpr(call, boundTimer);
        initCtx->addAssign(boundTimer.expr, call);
    }

    HqlExprArray nowArgs;
    nowArgs.append(*boundTimer.getTranslatedExpr());
    OwnedHqlExpr now = bindFunctionCall(getStartCyclesId, nowArgs);
    buildTempExpr(ctx, now, boundStart);
}

void HqlCppTranslator::buildStopTimer(BuildCtx & ctx, const CHqlBoundExpr & boundTimer, const CHqlBoundExpr & boundStart)
{
    HqlExprArray nowArgs;
    nowArgs.append(*boundTimer.getTranslatedExpr());
    nowArgs.append(*boundStart.getTranslatedExpr());
    OwnedHqlExpr done = bindFunctionCall(noteSectionTimeId, nowArgs);
    buildStmt(ctx, done);
}

//---------------------------------------------------------------------------
//-- no_null [DATASET] --

ABoundActivity * HqlCppTranslator::doBuildActivityNull(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    StringBuffer s;
    ThorActivityKind kind = expr->isAction() ? TAKemptyaction : TAKnull;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr,"Null");
    if (options.minimizeActivityClasses)
        instance->setImplementationClass(newNullArgId);

    //-----------------
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);
    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivitySideEffect(BuildCtx & ctx, IHqlExpression * expr, bool isRoot, bool expandChildren)
{
    //Something that is treated like an input, but causes something else to happen - e.g., a failure
    StringBuffer s;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKsideeffect, expr,"Action");

    //-----------------
    instance->graphLabel.set(getOpString(expr->getOperator()));         // label node as "fail"
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    {
        MemberFunction func(*this, instance->startctx, "virtual void action()", MFopt);
        if (expandChildren)
        {
            unsigned numChildren = expr->numChildren();
            for (unsigned idx=1; idx < numChildren; idx++)
            {
                IHqlExpression * cur = expr->queryChild(idx);
                if (!cur->isAttribute())
                    buildStmt(func.ctx, cur);
            }
        }
        else
        {
            buildStmt(func.ctx, expr);
        }
    }

    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}


ABoundActivity * HqlCppTranslator::doBuildActivityAction(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    StringBuffer s;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKsimpleaction, expr, "Action");

    //-----------------
    instance->graphLabel.set(getOpString(expr->getOperator()));         // label node as "fail"
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    {
        MemberFunction func(*this, instance->startctx, "virtual void action()", MFopt);
        buildStmt(func.ctx, expr);
    }

    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
// if (ctx->currentWorkflowId() == number)
//    doCode();

void HqlCppTranslator::buildWorkflowItem(BuildCtx & ctx, IHqlStmt * switchStmt, unsigned wfid, IHqlExpression * expr)
{
    OwnedHqlExpr value = getSizetConstant(wfid);

    BuildCtx condctx(ctx);
    IHqlStmt * caseStmt = condctx.addCase(switchStmt, value);

    //Unwind the statement list to prevent very deep recursion.
    HqlExprArray exprs;
    unwindCommaCompound(exprs, expr);
    ForEachItemIn(i, exprs)
        buildStmt(condctx, &exprs.item(i));

    if (caseStmt->numChildren() == 0)
        condctx.addGroup();             // ensure a break statement is generated...
}

void HqlCppTranslator::buildWorkflowPersistCheck(BuildCtx & ctx, IHqlExpression * expr)
{

    OwnedHqlExpr resultName = ::createResultName(queryAttributeChild(expr, namedAtom, 0));
    resultName.setown(ensureExprType(resultName, unknownVarStringType));

    IHqlExpression * filesRead = expr->queryAttribute(_files_Atom);
    DependenciesUsed dependencies(true);
    if (filesRead)
    {
        ForEachChild(i, filesRead)
            dependencies.tablesRead.append(*getNormalizedFilename(filesRead->queryChild(i)));
    }
    IHqlExpression * resultsRead = expr->queryAttribute(_results_Atom);
    if (resultsRead)
        unwindChildren(dependencies.resultsRead, resultsRead);

    IHqlExpression *  crcVal = queryAttributeChild(expr, _codehash_Atom, 0);
    OwnedHqlExpr crcExpr = calculatePersistInputCrc(ctx, dependencies);
    HqlExprArray args;
    args.append(*LINK(resultName));
    args.append(*LINK(crcVal));
    args.append(*LINK(crcExpr));
    args.append(*createConstant(expr->hasAttribute(fileAtom)));
    buildFunctionCall(ctx, returnPersistVersionId, args);
}

void HqlCppTranslator::buildWorkflow(WorkflowArray & workflow)
{
    //Generate a #define that can be used to optimize a particular function.
    BuildCtx optimizectx(*code, includeAtom);
    if (options.optimizeCriticalFunctions)
    {
        switch (options.targetCompiler)
        {
#ifndef __APPLE__
        case GccCppCompiler:
            optimizectx.addQuoted("#define OPTIMIZE __attribute__((optimize(3)))");
            break;
#endif
        default:
            optimizectx.addQuoted("#define OPTIMIZE");
            break;
        }
    }
    else
    {
        optimizectx.addQuoted("#define OPTIMIZE");
    }


    BuildCtx classctx(*code, goAtom);
    classctx.addQuotedCompoundLiteral("struct MyEclProcess : public EclProcess", ";");

    classctx.addQuotedLiteral("virtual unsigned getActivityVersion() const { return ACTIVITY_INTERFACE_VERSION; }");

    MemberFunction performFunc(*this, classctx, "virtual int perform(IGlobalCodeContext * gctx, unsigned wfid)");
    performFunc.ctx.addQuotedLiteral("ICodeContext * ctx;");
    performFunc.ctx.addQuotedLiteral("ctx = gctx->queryCodeContext();");

    performFunc.ctx.associateExpr(globalContextMarkerExpr, globalContextMarkerExpr);
    performFunc.ctx.associateExpr(codeContextMarkerExpr, codeContextMarkerExpr);

    OwnedHqlExpr function = createQuoted("wfid", LINK(unsignedType));
    BuildCtx switchctx(performFunc.ctx);
    IHqlStmt * switchStmt = switchctx.addSwitch(function);

    ForEachItemIn(idx, workflow)
    {
        WorkflowItem & action = workflow.item(idx);
        HqlExprArray & exprs = action.queryExprs();
        unsigned wfid = action.queryWfid();

        optimizePersists(exprs);
        bool isEmpty = exprs.ordinality() == 0;
        if (exprs.ordinality() == 1 && (exprs.item(0).getOperator() == no_workflow_action))
            isEmpty = true;

        if (!isEmpty)
        {
            if (action.isFunction())
            {
                OwnedHqlExpr function = action.getFunction();
                buildFunctionDefinition(function);
            }
            else
            {
                OwnedHqlExpr expr = createActionList(action.queryExprs());

                IHqlExpression * persistAttr = expr->queryAttribute(_workflowPersist_Atom);
                curWfid = wfid;
                if (persistAttr)
                {
                    if (!options.freezePersists)
                    {
                        HqlExprArray args2;
                        unwindChildren(args2, expr);
                        OwnedHqlExpr setResult = createSetResult(args2);
                        buildWorkflowItem(switchctx, switchStmt, wfid, setResult);
                    }
                }
                else
                    buildWorkflowItem(switchctx, switchStmt, wfid, expr);
                curWfid = 0;
            }
        }
    }

    OwnedHqlExpr returnExpr = getSizetConstant(maxSequence);
    performFunc.ctx.addReturn(returnExpr);
}

//---------------------------------------------------------------------------

void HqlCppTranslator::doBuildStmtWait(BuildCtx & ctx, IHqlExpression * expr)
{
    throwError(HQLERR_WaitNotSupported);
}

void HqlCppTranslator::doBuildStmtNotify(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * event = expr->queryChild(0);
    IHqlExpression * target = queryRealChild(expr, 1);
    HqlExprArray args;
    args.append(*LINK(event->queryChild(0)));
    args.append(*LINK(event->queryChild(1)));
    if (target)
    {
        args.append(*LINK(target));
        buildFunctionCall(ctx, doNotifyTargetId, args);
    }
    else
        buildFunctionCall(ctx, doNotifyId, args);
}

//---------------------------------------------------------------------------
// no_thorresult
// no_thorremoteresult


ABoundActivity * HqlCppTranslator::doBuildActivitySetResult(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
    IHqlExpression * persist = expr->queryAttribute(_workflowPersist_Atom);

    HqlExprAttr dataset, row, attribute;
    if (expr->getOperator() == no_extractresult)
    {
        row.set(expr->queryChild(0));
        dataset.set(row->queryNormalizedSelector(true));
        attribute.set(expr->queryChild(1));
    }
    else
    {
        if (!options.canGenerateSimpleAction)
        {
            row.setown(createDataset(no_null, LINK(queryNullRecord()), NULL));
            dataset.set(row);
        }
        attribute.set(expr->queryChild(0));
    }
//  splitSetResultValue(dataset, row, attribute, value);
    if (attribute->isAction())
    {
        //This code is decidedly strange - as far as I can see there is no explicit link between this activity and
        //the child root activity, it works because they happen to be generated in the same graph
        switch (attribute->getOperator())
        {
        case no_output:
            buildRootActivity(ctx, attribute);
            break;
        default:
            buildStmt(ctx, attribute);
            break;
        }
        attribute.set(queryBoolExpr(true));
    }

    Owned<ABoundActivity> boundDataset;
    ThorActivityKind kind = row ? TAKremoteresult : TAKsimpleaction;
    if (row)
        boundDataset.setown(buildCachedActivity(ctx, row));

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, kind == TAKremoteresult ? "RemoteResult" : "Action");

    if (sequence)
    {
        StringBuffer graphLabel;
        graphLabel.append("Store\n");
        getStoredDescription(graphLabel, sequence, name, true);
        instance->graphLabel.set(graphLabel.str());
    }
    buildActivityFramework(instance, isRoot && !isInternalSeq(sequence));

    buildInstancePrefix(instance);

    if (insideChildQuery(ctx))
    {
        StringBuffer description;
        getStoredDescription(description, sequence, name, true);
        reportWarning(CategoryUnusual, SeverityError, queryLocation(expr), ECODETEXT(HQLWRN_OutputScalarInsideChildQuery), description.str());
    }

    noteResultDefined(ctx, instance, sequence, name, isRoot);
    if (attribute->isDatarow())
        attribute.setown(::ensureSerialized(attribute, diskAtom));

    if (kind == TAKremoteresult)
    {
        doBuildSequenceFunc(instance->classctx, sequence, true);

        MemberFunction func(*this, instance->startctx, "virtual void sendResult(const void * _self)");
        func.ctx.addQuotedLiteral("const unsigned char * self = (const unsigned char *)_self;");

        if (dataset->isDatarow())
        {
            OwnedHqlExpr bound = createVariable("self", makeRowReferenceType(dataset));
            bindRow(func.ctx, dataset, bound);
        }
        else
            bindTableCursor(func.ctx, dataset, "self");
        buildSetResultInfo(func.ctx, expr, attribute, NULL, (persist != NULL), false);
    }
    else
    {
        MemberFunction func(*this, instance->startctx, "virtual void action()");
        buildSetResultInfo(func.ctx, expr, attribute, NULL, (persist != NULL), false);
    }

    buildInstanceSuffix(instance);
    if (boundDataset)
        buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    associateRemoteResult(*instance, sequence, name);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
//-- no_distribution

static void getInterfaceName(StringBuffer & name, ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_boolean:
        name.append("IBoolDistributionTable");
        break;
    case type_real:
        name.append("IRealDistributionTable");
        break;
    case type_string:
    case type_data:
    case type_qstring:
        assertex(type->getSize() != UNKNOWN_LENGTH);
        name.append("IStringDistributionTable");
        break;
    case type_int:
    case type_swapint:
    case type_packedint:
        if (type->isSigned())
        {
            if (type->getSize() == 8)
                name.append("IInt64DistributionTable");
            else
                name.append("IIntDistributionTable");
        }
        else
        {
            if (type->getSize() == 8)
                name.append("IUInt64DistributionTable");
            else
                name.append("IUIntDistributionTable");
        }
        break;
    default:
        UNIMPLEMENTED;
    }
}

static bool expandFieldName(StringBuffer & s, IHqlExpression * e)
{
    if (e->getOperator() == no_select)
    {
        if (expandFieldName(s, e->queryChild(0)))
            s.append('.');
        const char * name = str(e->queryChild(1)->queryName());
        s.appendLower(strlen(name), name);
        return true;
    }
    return false;
}

void HqlCppTranslator::doBuildDistributionClearFunc(BuildCtx & ctx, IHqlExpression * dataset, HqlExprArray & selects)
{
    StringBuffer s, funcName;

    MemberFunction func(*this, ctx, "virtual void clearAggregate(IDistributionTable * * tables)");
    ForEachItemIn(idx, selects)
    {
        IHqlExpression * original = &selects.item(idx);
        ITypeInfo * type = original->queryType();
        getInterfaceName(funcName.clear().append("create"), type);
        s.clear().append("tables[").append(idx).append("] = ").append(funcName).append("(\"");
        expandFieldName(s, original);
        s.append("\", ").append(type->getSize()).append(");");
        func.ctx.addQuoted(s);
    }
}

void HqlCppTranslator::doBuildDistributionNextFunc(BuildCtx & ctx, IHqlExpression * dataset, HqlExprArray & selects)
{
    StringBuffer s;

    MemberFunction func(*this, ctx, "virtual void process(IDistributionTable * * tables, const void * _src)");
    func.ctx.addQuotedLiteral("unsigned char * src = (unsigned char *) _src;");

    bindTableCursor(func.ctx, dataset, "src");

    ForEachItemIn(idx, selects)
    {
        IHqlExpression * original = &selects.item(idx);
        ITypeInfo * type = original->queryType();
        CHqlBoundExpr bound;

        buildExpr(func.ctx, original, bound);
        s.clear().append("((");
        getInterfaceName(s, type);
        s.append(" *)tables[").append(idx).append("])->noteValue(");
        switch (type->getTypeCode())
        {
        case type_string:
        case type_data:
        case type_qstring:
            {
                if (bound.length)
                    generateExprCpp(s, bound.length);
                else
                    s.append(type->getSize());
                s.append(",");
                OwnedHqlExpr addr = getElementPointer(bound.expr);
                generateExprCpp(s, addr);
            }
            break;
        default:
            generateExprCpp(s, bound.expr);
            break;
        }
        s.append(");");
        func.ctx.addQuoted(s);
    }
}

void HqlCppTranslator::doBuildDistributionFunc(BuildCtx & funcctx, unsigned numFields, const char * action)
{
    StringBuffer s;

    s.clear().append("for (unsigned i=0;i<").append(numFields).append(";i++)");
    funcctx.addQuotedCompound(s, nullptr);
    s.clear().append("tables[i]->").append(action).append(";");
    funcctx.addQuoted(s);
}


void HqlCppTranslator::doBuildDistributionDestructFunc(BuildCtx & ctx, unsigned numFields)
{
    BuildCtx funcctx(ctx);
    funcctx.addQuotedFunction("virtual void destruct(IDistributionTable * * tables)");
    doBuildDistributionFunc(funcctx, numFields, "Release()");
}

void HqlCppTranslator::doBuildDistributionSerializeFunc(BuildCtx & ctx, unsigned numFields)
{
    BuildCtx funcctx(ctx);
    funcctx.addQuotedFunction("virtual void serialize(IDistributionTable * * tables, MemoryBuffer & out)");
    doBuildDistributionFunc(funcctx, numFields, "serialize(out)");
}

void HqlCppTranslator::doBuildDistributionMergeFunc(BuildCtx & ctx, unsigned numFields)
{
    BuildCtx funcctx(ctx);
    funcctx.addQuotedFunction("virtual void merge(IDistributionTable * * tables, MemoryBuffer & in)");
    doBuildDistributionFunc(funcctx, numFields, "merge(in)");
}

void HqlCppTranslator::doBuildDistributionGatherFunc(BuildCtx & ctx, unsigned numFields)
{
    BuildCtx funcctx(ctx);
    funcctx.addQuotedFunction("virtual void gatherResult(IDistributionTable * * tables, StringBuffer & out)");
    doBuildDistributionFunc(funcctx, numFields, "report(out)");
}


static void expandDistributionFields(IHqlExpression * record, HqlExprArray & selects, IHqlExpression * selector)
{
    ForEachChild(idx, record)
    {
        IHqlExpression * cur = record->queryChild(idx);
        switch (cur->getOperator())
        {
        case no_ifblock:
            expandDistributionFields(cur->queryChild(1), selects, selector);
            break;
        case no_record:
            expandDistributionFields(cur, selects, selector);
            break;
        case no_field:
            {
                OwnedHqlExpr selected = selector ? createSelectExpr(LINK(selector), LINK(cur)) : LINK(cur);

                if (cur->queryType()->getTypeCode() == type_row)
                {
                    expandDistributionFields(cur->queryRecord(), selects, selected);
                    return;
                }

                selects.append(*selected.getClear());
                break;
            }
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            break;
        default:
            UNIMPLEMENTED;
            break;
        }
    }
}


ABoundActivity * HqlCppTranslator::doBuildActivityDistribution(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * fields = queryRealChild(expr, 1);
    IHqlExpression * sequence = expr->queryAttribute(sequenceAtom);

    if (!sequence)
        throwError(HQLERR_DistributionNoSequence);

    useInclude("rtldistr.hpp");

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKdistribution, expr, "Distribution");
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    HqlExprArray selects;
    if (fields)
        unwindChildren(selects, fields);
    else
        expandDistributionFields(dataset->queryRecord(), selects, dataset);

    ForEachItemIn(i, selects)
    {
        IHqlExpression & cur = selects.item(i);
        ITypeInfo * type = cur.queryType();
        switch (type->getTypeCode())
        {
        case type_boolean:
        case type_real:
        case type_int:
        case type_swapint:
        case type_packedint:
            break;
        case type_string:
        case type_data:
        case type_qstring:
            if (type->getSize() == UNKNOWN_LENGTH)
                throwError1(HQLERR_DistributionVariableLengthX, str(cur.queryChild(1)->queryId()));
            break;
        default:
            {
                StringBuffer typeName;
                getFriendlyTypeStr(type, typeName);
                throwError2(HQLERR_DistributionUnsupportedTypeXX, str(cur.queryChild(1)->queryId()), typeName.str());
            }
        }
    }

    unsigned numFields = selects.ordinality();
    doBuildDistributionClearFunc(instance->startctx, dataset, selects);
    doBuildDistributionNextFunc(instance->startctx, dataset, selects);
    doBuildDistributionDestructFunc(instance->startctx, numFields);
    doBuildDistributionGatherFunc(instance->startctx, numFields);
    doBuildDistributionMergeFunc(instance->startctx, numFields);
    doBuildDistributionSerializeFunc(instance->startctx, numFields);

    //Need an extra meta information for the internal aggregate record
    {
        HqlExprArray fields;
        fields.append(*createField(unnamedId, makeDataType(numFields*sizeof(void*)), NULL, NULL));
        OwnedHqlExpr tempRecord = createRecord(fields);
        buildMetaMember(instance->classctx, tempRecord, false, "queryInternalRecordSize");
    }

    //Generate the send Result method().
    {
        MemberFunction func(*this, instance->startctx, "virtual void sendResult(size32_t length, const char * text)");

        CHqlBoundExpr bound;
        HqlExprAttr translated;
        bound.length.setown(createVariable("length", makeIntType(sizeof(size32_t), false)));
        bound.expr.setown(createVariable("text", makeStringType(UNKNOWN_LENGTH, NULL, NULL)));
        translated.setown(bound.getTranslatedExpr());
        buildSetResultInfo(func.ctx, expr, translated, NULL, false, false);
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}


//---------------------------------------------------------------------------
//-- pure hole table --

void HqlCppTranslator::addFileDependency(IHqlExpression * name, ABoundActivity * whoAmI)
{
    if (name && activeGraphCtx)
    {
        OwnedHqlExpr search = createAttribute(fileAtom, getNormalizedFilename(name));
        HqlExprAssociation * match = activeGraphCtx->queryMatchExpr(search);
        if (match)
            addDependency(*activeGraphCtx, ((ABoundActivity *)match->queryExpr()->queryUnknownExtra()), whoAmI, sourceAtom);
    }
}

//---------------------------------------------------------------------------

StringBuffer &expandDotLiteral(StringBuffer &s, const char *f)
{
    unsigned lines = 0;
    unsigned chars = 0;
    char c;
    while ((c = *f++) != 0)
    {
        switch (c)
        {
        case '\t':
            s.append(" ");
            break;
        case '\r':
            break;
        case '\n':
            s.append("\\l");  // Means left justify.
            if (lines++ > 10)
                return s;
            break;
        case '}':
        case '{':
        case '<':
        case '>':  // Special chars in dot graphs

        case '\\':
        case '\"':
        case '\'':
            s.append('\\');
            // fall into...
        default:
            if (chars++ > 1000)
                return s;
            s.append(c);
            break;
        }
    }
    return s;
}


void HqlCppTranslator::logGraphEdge(IPropertyTree * subGraph, unsigned __int64 source, unsigned __int64 target, unsigned outputIndex, unsigned inputIndex, const char * label, bool nWay)
{
    addSimpleGraphEdge(subGraph, source, target, outputIndex, inputIndex, NULL, label, nWay);
}

void HqlCppTranslator::buildActivityFramework(ActivityInstance * instance)
{
    assertex(!instance->isAction());        // All actions should be calling the 2 parameter version below instead
    buildActivityFramework(instance, false);
}

void HqlCppTranslator::buildActivityFramework(ActivityInstance * instance, bool alwaysExecuted)
{
    instance->createGraphNode(activeGraph->xgmml, alwaysExecuted);
    if (options.trackDuplicateActivities)
    {
        IHqlExpression * search = instance->dataset;
        node_operator op = search->getOperator();
        if ((op != no_select) && (op != no_workunit_dataset))
        {
            IHqlExpression * searchNorm = queryLocationIndependent(search);
            unsigned crc = getExpressionCRC(search);
            ForEachItemIn(i, tracking.activityExprs)
            {
                if (search == &tracking.activityExprs.item(i))
                    instance->addAttributeInt("_duplicateActivity_", tracking.activityIds.item(i));
                else if (crc == tracking.activityCrcs.item(i))
                    instance->addAttributeInt("_duplicateCrcActivity_", tracking.activityIds.item(i));
                else if (searchNorm == &tracking.activityNorms.item(i))
                    instance->addAttributeInt("_duplicateNormActivity_", tracking.activityIds.item(i));
            }
            tracking.activityExprs.append(*LINK(search));
            tracking.activityNorms.append(*LINK(searchNorm));
            tracking.activityIds.append(instance->activityId);
            tracking.activityCrcs.append(crc);
        }
    }
}

void HqlCppTranslator::buildConnectOrders(BuildCtx & ctx, ABoundActivity * slaveActivity, ABoundActivity * masterActivity)
{
    //I'm not sure we even use this information, but at least it's there if needed.
    if (targetThor())
    {
        IPropertyTree *edge = createPTree();
        edge->setPropInt64("@target", slaveActivity->queryActivityId());
        edge->setPropInt64("@source", masterActivity->queryActivityId());
        addGraphAttributeBool(edge, "cosort", true);

        SubGraphInfo * activeSubgraph = queryActiveSubGraph(ctx);
        assertex(activeSubgraph);
        activeSubgraph->tree->addPropTree("edge", edge);
    }
}


bool HqlCppTranslator::useRowAccessorClass(IHqlExpression * record, bool isTargetRow)
{
    if (isTargetRow)
        return false;
    if (isFixedRecordSize(record))
        return false;
    if (!canCreateRtlTypeInfo(record))
        return false;
    return getVarSizeFieldCount(record, true) >= options.varFieldAccessorThreshold;
}


ColumnToOffsetMap * HqlCppTranslator::queryRecordOffsetMap(IHqlExpression * record, bool useAccessorClass)
{
    if (record)
        return recordMap.queryMapping(record, options.maxRecordSize, useAccessorClass);
    return NULL;
}

unsigned HqlCppTranslator::getFixedRecordSize(IHqlExpression * record)
{
    assertex(isFixedSizeRecord(record));
    return getMinRecordSize(record);
}


bool HqlCppTranslator::isFixedRecordSize(IHqlExpression * record)
{
    return ::isFixedSizeRecord(record);
}

void HqlCppTranslator::buildReturnRecordSize(BuildCtx & ctx, BoundRow * cursor)
{
    OwnedHqlExpr size = getRecordSize(cursor->querySelector());
    buildReturn(ctx, size);
}


bool HqlCppTranslator::recordContainsIfBlock(IHqlExpression * record)
{
    return ::containsIfBlock(record);
}


void HqlCppTranslator::buildRowAccessors()
{
    HashIterator iter(recordMap);
    ForEach(iter)
    {
        ColumnToOffsetMap * map = static_cast<ColumnToOffsetMap *>(&iter.query());
        buildRowAccessor(map);
    }

}

void HqlCppTranslator::buildRowAccessor(ColumnToOffsetMap * map)
{
    if (!map->usesAccessor())
        return;

    const bool isRead = true;
    IHqlExpression * record = map->queryRecord();
    BuildCtx declarectx(*code, declareAtom);
    BuildCtx ctx(declarectx);
    OwnedHqlExpr search = createAttribute(accessorAtom, LINK(record), createConstant(isRead));
    if (ctx.queryMatchExpr(search))
        return;

    StringBuffer accessorName;
    map->buildAccessor(accessorName, *this, ctx, NULL);

    declarectx.associateExpr(search, search);
}

//-- Code to transform the expressions ready for generating source code.

static void logECL(const LogMsgCategory & category, size32_t len, const char * ecl)
{
    const size32_t chunkSize = 31000;
    while (len > chunkSize)
    {
        const char * next = strchr(ecl+chunkSize, '\n');
        if (!next || !next[1])
            break;
        unsigned size = next-ecl;
        if (ecl[size-1] == '\r')
            size--;
        LOG(category, unknownJob, "%.*s", size, ecl);
        len -= (next+1-ecl);
        ecl = next+1;
    }
    LOG(category, unknownJob, "%s", ecl);
}


void HqlCppTranslator::traceExpression(const char * title, IHqlExpression * expr, unsigned level)
{
    if (!expr)
        return;

    checkAbort();

    LOG(MCdebugInfo(200), unknownJob, "Tracing expressions: %s", title);
    LogMsgCategory debug500 = MCdebugInfo(level);
    if(REJECTLOG(debug500))
        return;

    if (options.traceIR)
    {
        EclIR::dbglogIR(expr);
    }
    else
    {
        StringBuffer s;
        processedTreeToECL(expr, s);
        logECL(debug500, s.length(), s.str());
    }
}


void HqlCppTranslator::traceExpressions(const char * title, HqlExprArray & exprs, unsigned level)
{
    OwnedHqlExpr compound = createComma(exprs);
    traceExpression(title, compound, level);
}

void HqlCppTranslator::traceExpressions(const char * title, WorkflowArray & workflow)
{
    checkAbort();

    // PrintLog(title);
    LOG(MCdebugInfo(200), unknownJob, "Tracing expressions: %s", title);
    static LogMsgCategory debug500 = MCdebugInfo(500);
    static LogMsgCategory debug5000 = MCdebugInfo(5000);
    if(REJECTLOG(debug500))
        return;

    ForEachItemIn(idx1, workflow)
    {
        WorkflowItem & cur = workflow.item(idx1);
        OwnedHqlExpr compound = createComma(cur.queryExprs());

        if (compound)
        {
            LOG(debug500, unknownJob, "%s: #%d: id[%d]", title, idx1, cur.queryWfid());

            if (options.traceIR)
            {
                EclIR::dbglogIR(compound);
            }
            else
            {
                StringBuffer s;
                processedTreeToECL(compound, s);
                logECL(debug500, s.length(), s.str());
            }
        }
    }
}


void HqlCppTranslator::checkNormalized(WorkflowArray & workflow)
{
    ForEachItemIn(i, workflow)
    {
        checkNormalized(workflow.item(i).queryExprs());
    }
}

void HqlCppTranslator::checkNormalized(IHqlExpression * expr)
{
    if (options.paranoidCheckDependencies)
        checkDependencyConsistency(expr);

    if (options.paranoidCheckNormalized)
    {
        ::checkNormalized(expr);
    }

    if (options.paranoidCheckSelects)
        checkSelectConsistency(expr);
}

void HqlCppTranslator::checkNormalized(HqlExprArray & exprs)
{
    if (options.paranoidCheckDependencies)
        checkDependencyConsistency(exprs);

    ForEachItemIn(i, exprs)
    {
        if (options.paranoidCheckNormalized)
            ::checkNormalized(&exprs.item(i));
        if (options.paranoidCheckSelects)
            checkSelectConsistency(&exprs.item(i));
    }
}


void HqlCppTranslator::checkNormalized(BuildCtx & ctx, IHqlExpression * expr)
{
    if (options.paranoidCheckDependencies)
        checkDependencyConsistency(expr);

    if (options.paranoidCheckNormalized)
    {
        HqlExprArray activeTables;
        //Added in reverse order, but normalize checker doesn't care
        RowAssociationIterator iter(ctx);
        ForEach(iter)
        {
            BoundRow & cur = iter.get();
            if ((cur.querySide() != no_self) && !cur.isBuilder())
                activeTables.append(*LINK(cur.querySelector()));
        }

        ::checkNormalized(expr, activeTables);
    }

    if (options.paranoidCheckSelects)
        checkSelectConsistency(expr);
}


void createCompoundEnsure(HqlExprArray & exprs, unsigned first, unsigned last)
{
    if (first >= last || last == NotFound)
        return;

    IHqlExpression * action = &exprs.item(last);
    OwnedHqlExpr actionExpr = createActionList(exprs, first, last);
    OwnedHqlExpr compound = createCompound(actionExpr.getClear(), LINK(action->queryChild(0)));
    OwnedHqlExpr newAction = replaceChild(action, 0, compound);
    exprs.replace(*newAction.getClear(), first);
    exprs.removen(first+1, (last-first));
}


//move any set results inside a no_ensureresult so they don't get evaluated unless necessary.
void HqlCppTranslator::optimizePersists(HqlExprArray & exprs)
{
    //If there is a single ensure result, and no set results created from other workflow items
    //then move all previous actions inside the ensure result
    unsigned max = exprs.ordinality();
    if (max == 0)
        return;

    if (exprs.item(max-1).getOperator() != no_ensureresult)
        return;

    for (unsigned i=0; i < max-1; i++)
    {
        IHqlExpression & cur = exprs.item(i);

        if ((cur.getOperator() == no_ensureresult) || cur.hasAttribute(_workflow_Atom))
            return;
    }
    createCompoundEnsure(exprs, 0, max-1);
}

IHqlExpression * HqlCppTranslator::convertSetResultToExtract(IHqlExpression * expr)
{
    SetResultToExtractTransformer transformer;

    return transformer.transformRoot(expr);
}


IHqlExpression * HqlCppTranslator::extractGlobalCSE(IHqlExpression * expr)
{
    AutoScopeMigrateTransformer transformer(wu(), *this);

    HqlExprArray exprs;
    unwindCommaCompound(exprs, expr);
    transformer.analyseArray(exprs, 0);
    if (!transformer.worthTransforming())
        return LINK(expr);

    HqlExprArray results;
    transformer.transformRoot(exprs, results);
    return createActionList(results);
}

IHqlExpression * HqlCppTranslator::spotGlobalCSE(IHqlExpression * _expr)
{
    if (!_expr->isAction())
        return LINK(_expr);

    LinkedHqlExpr expr = _expr;
    switch (expr->getOperator())
    {
    case no_if:
        {
            IHqlExpression * left = expr->queryChild(1);
            IHqlExpression * right = expr->queryChild(2);
            HqlExprArray args;
            args.append(*LINK(expr->queryChild(0)));
            args.append(*extractGlobalCSE(left));
            if (right)
                args.append(*extractGlobalCSE(right));
            if ((left != &args.item(1)) || (right && right != &args.item(2)))
            {
                unwindChildren(args, expr, 3);
                expr.setown(_expr->clone(args));
            }
            break;
        }
    case no_sequential:
        {
            HqlExprArray args;
            ForEachChild(i, expr)
                args.append(*extractGlobalCSE(expr->queryChild(i)));
            expr.setown(_expr->clone(args));
            break;
        }
    case no_nothor:
        return LINK(expr);
    }

    bool same = true;
    HqlExprArray args;
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        IHqlExpression * next = spotGlobalCSE(cur);
        args.append(*next);
        if (cur != next)
            same = false;
    }
    if (same)
        return expr.getClear();
    return expr->clone(args);
}


void HqlCppTranslator::spotGlobalCSE(HqlExprArray & exprs)
{
    HqlExprArray results;
    AutoScopeMigrateTransformer transformer(wu(), *this);

    transformer.analyseArray(exprs, 0);
    if (transformer.worthTransforming())
    {
        transformer.transformRoot(exprs, results);
        replaceArray(exprs, results);
    }

    if (!options.resourceConditionalActions)
    {
        //Now need to recursively walk actions, and if any conditional actions could do with things being hoisted within them
        ForEachItemIn(i, exprs)
            exprs.replace(*spotGlobalCSE(&exprs.item(i)), i);
    }
}

void HqlCppTranslator::spotGlobalCSE(WorkflowItem & curWorkflow)
{
    if (!insideLibrary() && options.globalAutoHoist)
    {
        spotGlobalCSE(curWorkflow.queryExprs());
    }
}

void HqlCppTranslator::flattenDatasets(WorkflowArray & array)
{
    //MORE: Should project fields needed outside <ds>.<ds> so that they are available.
}

// Code to check whether thor is required for a query.  It should err towards true.
// The idea is to prevent some very simple queries going to thor, mainly when users are examining data.  The main examples are:
// 1. Unfiltered table count
// 2. Filtered index count.
// 3. Restricted set of records from an index/table.

enum { NRTfiltered = 0x0001, NRTcount = 0x0002, NRTlimited = 0x0004 };
static bool needsRealThor(IHqlExpression *expr, unsigned flags)
{
    unsigned numChildrenToCheck = (unsigned)-1;
    switch (expr->getOperator())
    {
    case no_table:
        //only allow non filtered limited outputs, and non filtered counts
        return !((flags == NRTlimited) || (flags == NRTcount));

    case no_newkeyindex:
    case no_keyindex:
    case no_compound_indexread:
        //Don't allow count(choosen(...)) otherwise likely to be better in hthor
        if (flags & NRTcount)
            return (flags & NRTlimited) != 0;
        //unfiltered index read should go via thor
        if (flags == 0)
            return true;
        //filtered index reads likely to be much better in hthor.
        return false;

    case no_attr:
    case no_attr_expr:
    case no_attr_link:
    case no_datasetfromrow:
    case no_rows:
    case no_libraryinput:
    case no_fail:
    case no_persist_check:
        return false;

    case no_distribution:
    case no_buildindex:
    case no_keydiff:
    case no_keypatch:
    case no_forcelocal:
    case no_forcenolocal:
    case no_allnodes:
    case no_thisnode:
        return true;

    case no_hqlproject:
        //If count project, a count will not be done as a compound operation
        if (expr->hasAttribute(_countProject_Atom) && (flags & NRTcount))
            return true;
        break;
    case no_compound_indexcount:
    case no_transformascii:
    case no_transformebcdic:
    case no_selectfields:
    case no_thor:
    case no_apply:
    case no_distributed:
    case no_unordered:
    case no_preservemeta:
    case no_sorted:
    case no_limit:
    case no_catchds:
    case no_keyedlimit:
    case no_assertsorted:
    case no_assertgrouped:
    case no_assertdistributed:
    case no_section:
    case no_sectioninput:
    case no_forcegraph:
    case no_nofold:
    case no_nohoist:
    case no_nocombine:
    case no_actionlist:
    case no_orderedactionlist:
    case no_compound_fetch:
    case no_addfiles:
    case no_nonempty:
    case no_dataset_alias:
        //i.e. go through children...
        break;
    case no_compound:
    case no_comma:
    case no_executewhen:
        numChildrenToCheck = expr->numChildren();
        break;

    case no_choosen:
    case no_selectnth:
        flags |= NRTlimited;
        break;

    case no_filter:
        flags |= NRTfiltered;
        break;

    case no_newaggregate:
    case no_newusertable:
        if (isAggregateDataset(expr))
        {
            //Only allow aggregates we can do on an index directly
            if (datasetHasGroupBy(expr))
                return true;
            node_operator aggOp = querySingleAggregate(expr, false, false, true);
            if ((aggOp != no_exists) && (aggOp != no_count))
                return true;
            flags |= NRTcount;
        }
        break;

    case no_if:
    case no_choose:
    case no_chooseds:
        {
            if (needsRealThor(expr->queryChild(0), 0))
                return true;
            ForEachChildFrom(i, expr, 1)
            {
                if (needsRealThor(expr->queryChild(i), flags))
                    return true;
            }
            return false;
        }

    case no_colon:
    case no_globalscope:
    case no_extractresult:
        return needsRealThor(expr->queryChild(0), flags);

    case no_call:
    case no_externalcall:
        if (isDistributedFunctionCall(expr))
            return true;
        //MORE: check for streamed inputs.
        break;

    case no_fetch:
        return needsRealThor(expr->queryChild(1), flags);

    case no_output:
        {
            //Assume any output to files needs to stay where it is.
            IHqlExpression *child0 = expr->queryChild(0);
            IHqlExpression *filename = queryRealChild(expr, 1);
            if (filename)
                return true;
            return needsRealThor(child0, flags);
        }

    case no_ensureresult:
    case no_setresult:
    case no_evaluate_stmt:
    case no_return_stmt:
        {
            IHqlExpression * child0 = expr->queryChild(0);
            if (!child0->queryType()->isScalar())
                return needsRealThor(child0, flags);
            if (child0->getOperator() == no_evalonce)
                child0 = child0->queryChild(0);
            switch (child0->getOperator())
            {
            case no_externalcall:
            case no_constant:
            case no_all:
                return false;
            case no_select:
                return needsRealThor(child0->queryChild(0), flags);
            }
            if (!containsAnyDataset(child0))
                return false;
//          return needsRealThor(child0, isFiltered);
            //fallthrough...
        }

    default:
        if (expr->isDataset())
            return true;

        ITypeInfo * type = expr->queryType();
        if (!type || (type->getTypeCode() != type_void))
            return false;           //MORE Doesn't cope with scalar expressions that require thor e.g., counts of sorts of ....
        //MORE: This means that lots of scalar expressions go to thor instead of hthor.
        return true;
    }

    if (numChildrenToCheck == (unsigned)-1)
        numChildrenToCheck = expr->isDataset() ? getNumChildTables(expr) : expr->numChildren();
    for (unsigned idx=0; idx < numChildrenToCheck; idx++)
    {
        if (needsRealThor(expr->queryChild(idx), flags))
            return true;
    }
    return false;
}

bool needsRealThor(IHqlExpression *expr)
{
    return needsRealThor(expr, 0);
}


IHqlExpression * HqlCppTranslator::getDefaultOutputAttr(IHqlExpression * expr)
{
    return createAttribute(workunitAtom);
}

void HqlCppTranslator::modifyOutputLocations(HqlExprArray & exprs)
{
    ForEachItemIn(idx, exprs)
    {
        IHqlExpression &expr = exprs.item(idx);
        IHqlExpression * filename = queryRealChild(&expr, 1);

        //Deduce whether OUTPUT(x) should goes to SDS or a disk
        if (expr.getOperator()==no_output && !filename)
        {
            if (!expr.hasAttribute(workunitAtom) && !expr.hasAttribute(firstAtom) && !expr.hasAttribute(diskAtom))
            {
                IHqlExpression * attr = getDefaultOutputAttr(&expr);
                HqlExprArray args;
                expr.unwindList(args, no_output);
                args.append(*attr);

                IHqlExpression * transformed = expr.clone(args);
                exprs.replace(*transformed, idx);
            }
        }
    }
}


void HqlCppTranslator::pickBestEngine(HqlExprArray & exprs)
{
    // At this point it is not too late to decide whether real thor is needed.
    // Basically, we will use real thor if available, unless it matches a very minimal set of patterns:
    // 1. output(holequery)
    // 2. output(choosen(holequery))
    // 3. output([filtered/projected/firstn] thordiskfile); (not sure about the filtered)

    // These correspond closely to the things that can eventually get turned into lazy remote views
    //It would be more sensible to do this much earlier.....or not at all.

    if (targetThor())
    {
        ForEachItemIn(idx, exprs)
        {
            if (needsRealThor(&exprs.item(idx)))
                return;
        }
        // if we got this far, thor not required
        setTargetClusterType(HThorCluster);
        DBGLOG("Thor query redirected to hthor instead");
    }
}


void HqlCppTranslator::pickBestEngine(WorkflowArray & workflow)
{
    if (targetThor())
    {
        ForEachItemIn(idx2, workflow)
        {
            HqlExprArray & exprs = workflow.item(idx2).queryExprs();
            ForEachItemIn(idx, exprs)
            {
                if (needsRealThor(&exprs.item(idx)))
                    return;
            }
            // if we got this far, thor not required
        }
        setTargetClusterType(HThorCluster);
        DBGLOG("Thor query redirected to hthor instead");
    }
}

unsigned getVirtualFieldSize(IHqlExpression * record)
{
    unsigned size = 0;
    ForEachChild(idx, record)
    {
        IHqlExpression * cur = record->queryChild(idx);
        switch (cur->getOperator())
        {
        case no_field:
            {
                ITypeInfo * type = cur->queryType();
                if (type->isScalar())
                {
                    if (cur->hasAttribute(virtualAtom))
                        size += type->getSize();
                }
                else
                    size += getVirtualFieldSize(cur->queryRecord());
                break;
            }
        case no_ifblock:
            size += getVirtualFieldSize(cur->queryChild(1));
            break;
        case no_record:
            size += getVirtualFieldSize(cur);
            break;
        }
    }
    return size;
}

// buildCompareFuncHelper() creates helper compare helper function.
// Tasks:
// 1) create a helper function within the nestedctx to return an ICompare *.  The helper function will be named "query"+compareFuncName (first letter capitalized)
// 2) create an ICompare derived class and object
//    - within the nested class (when useGlobalCompareClass=false) or
//    - as a global class (when useGlobalCompareClass=true)
void buildCompareFuncHelper(HqlCppTranslator & translator, ActivityInstance & instance, const char * compareFuncName, IHqlExpression * sortList, const DatasetReference & dsRef)
{
    assertex(compareFuncName[0]); // make sure func name is at least 1 char
    StringBuffer compareClassInstance;
    translator.buildCompareClass(instance.nestedctx, compareFuncName, sortList, dsRef, compareClassInstance);

    StringBuffer s;
    s.set("virtual ICompare * query").append(static_cast<char>(toupper(compareFuncName[0]))).append(compareFuncName+1).append("()");
    MemberFunction func(translator, instance.classctx, s, MFdynamicproto);

    s.set("return &").append(compareClassInstance).append(";");
    func.ctx.addQuoted(s);
}

//---------------------------------------------------------------------------

/*
  The following transforms are applied to the graphs before the code is generated:

o replaceStoredValues(exprs, foldStored);
  - Replaces any #stored values in the graph.  Done first so everything remains consistent.

o normalizeHqlTree(exprs);
  - Converts expressions to their normal form.  Main change is to remove default values from fields, so the graph can be
    transformed without having to remap fields its value (e.g., dataset on a select) changes.  Impossible to do anything without
    this stage.
  - Also converts x : global to global(x) and normalizes a few simple constructs e.g., trim,right->trim

o allocateSequenceNumbers(exprs);
  - Adds sequence numbers to all outputs

o foldHqlExpression
  - does a global constant fold to simplify the graph before it gets broken up at all.

o optimizeHqlExpression
  - could globally optimize the graph, but not enabled at the moment.  I'm not sure why, maybe because we still don't trust it enough...

o extractWorkflow(exprs)
  - Converts a list of expressions into a list of workflow items.  Once this is done each workflow item can be treated independently.

--- The following aim to work out where each part of the query will be executed, and gather all parts that will be executed in the same place
--- together so increase likely hood for cses and reduce connections to the query engines.

o migrateExprToNaturalLevel [NewScopeMigrateTransformer]
  - Ensure expressions are evaluated at the best level - e.g., counts moved to most appropriate level.
  * global(x) is extracted to a global set/get result pair.
    - This includes datasets, and specifically ignores any conditional context
  * SET(dataset, field) is converted to a workunit output, workunit read pair
  !!Needs revisiting for child queries
  * global count(), max() on global tables used inside an activity are always hoisted
  !!regardless of whether they are conditional or not.
  * local(x) and global() interactions are processed
  !!Once aggregate sources are implemented, this may not be so useful, or even needed.

o markThorBoundaries
  - work out which engine is going to perform which operation.

o normalizeResultFormat
  * convert thor(scalar|row) into compound(setresult,getresult) or global setresult, local getresult
  !!It should common up conditional expressions with non conditional
  * IF(count(x)...) inside thor, hoist the count(x) so it is global
  * IF(ds[n]...) inside thor, hoist it so it is global.
  !!Conditionals should be handled differently I am sure...

o flattenDatasets(array);
  - Currently does nothing.  It should add projects to hole to ensure fields are available.

o mergeThorGraphs
  - Make sure thor graphs are together as much as possible to save transfers to thor, and maximise the cse chances.
  - Combine results of above.  Should probably just be a single transformation.

o spotGlobalCSE
  - Spot CSEs between different graphs.  E.g., f(a), if(x, g(a), h(a)) Should ensure a is spilled.
  !!Current problem is that global can create a implicit dependency which isn't spotted.
  !!also doesn't handle commoning up scalars very well.

o removeTrivialGraphs
  - don't implement setresult(getresult) in thor - a waste of time....

o convertLogicalToActivities
  - change representation from logical to actual implementation.  E.g., dedup(a, b) becomes group(dedup(group(a,b,all)))

*/
