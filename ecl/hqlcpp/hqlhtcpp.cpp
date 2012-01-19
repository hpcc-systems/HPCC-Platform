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
#include "jliball.hpp"
#include "hql.hpp"

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "jdebug.hpp"

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

//The following are include to ensure they call compile...
#include "eclhelper.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield_imp.hpp"
#include "rtlds_imp.hpp"
#include "eclhelper_base.hpp"

#define MAX_ROWS_OUTPUT_TO_SDS              1000
#define PERSIST_VERSION                     1           // Increment when implementation is incompatible.
#define MAX_SAFE_RECORD_SIZE                10000000
#define DEFAULT_EXPIRY_PERIOD               7
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
IHqlExpression * getMetaUniqueKey(IHqlExpression * dataset)
{
    IHqlExpression * record = dataset->queryRecord();
    if (record) record = record->queryBody();
    LinkedHqlExpr search = record;
    ITypeInfo * type = dataset->queryType();
    if (type && type->queryGroupInfo() != NULL)
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
            if (!child->queryType()->queryGroupInfo())
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
    switch (name->getOperator())
    {
    case no_constant:
        return LINK(name);
    default:
        return LINK(name);
        UNIMPLEMENTED;
        break;
    }
    return NULL;
}

//---------------------------------------------------------------------------

void extractAtmostArgs(IHqlExpression * atmost, SharedHqlExpr & atmostCond, SharedHqlExpr & atmostLimit)
{
    atmostLimit.set(queryZero());
    if (atmost)
    {
        IHqlExpression * arg0 = atmost->queryChild(0);
        if (arg0->isBoolean())
        {
            atmostCond.set(arg0);
            atmostLimit.set(atmost->queryChild(1));
        }
        else
            atmostLimit.set(arg0);
    }
}

static bool matchesAtmostCondition(IHqlExpression * cond, HqlExprArray & atConds, unsigned & numMatched)
{
    if (atConds.find(*cond) != NotFound)
    {
        numMatched++;
        return true;
    }
    if (cond->getOperator() != no_assertkeyed)
        return false;
    unsigned savedMatched = numMatched;
    HqlExprArray conds;
    cond->queryChild(0)->unwindList(conds, no_and);
    ForEachItemIn(i, conds)
    {
        if (!matchesAtmostCondition(&conds.item(i), atConds, numMatched))
        {
            numMatched = savedMatched;
            return false;
        }
    }
    return true;
}

void HqlCppTranslator::splitFuzzyCondition(IHqlExpression * condition, IHqlExpression * atmostCond, SharedHqlExpr & fuzzy, SharedHqlExpr & hard)
{
    if (atmostCond)
    {
        //If join condition has evaluated to a constant then allow any atmost condition.
        if (!condition->isConstant())
        {
            HqlExprArray conds, atConds;
            condition->unwindList(conds, no_and);
            atmostCond->unwindList(atConds, no_and);
            unsigned numAtmostMatched = 0;
            ForEachItemIn(i, conds)
            {
                IHqlExpression & cur = conds.item(i);
                if (matchesAtmostCondition(&cur, atConds, numAtmostMatched))
                    extendConditionOwn(hard, no_and, LINK(&cur));
                else
                    extendConditionOwn(fuzzy, no_and, LINK(&cur));
            }
            if (atConds.ordinality() != numAtmostMatched)
            {
                StringBuffer s;
                getExprECL(atmostCond, s);
                throwError1(HQLERR_AtmostFailMatchCondition, s.str());
            }
        }
    }
    else
        hard.set(condition);
}

//---------------------------------------------------------------------------

ColumnToOffsetMap * RecordOffsetMap::queryMapping(IHqlExpression * record, unsigned maxRecordSize)
{
    ColumnToOffsetMap * match = find(record);
    if (!match)
    {
        match = new ColumnToOffsetMap(record, 1, maxRecordSize, false);
        match->init(*this);
        addOwn(*match);
        if (maxRecordSize == 0)
            match->checkValidMaxSize();
    }
    return match;
}

//---------------------------------------------------------------------------

MemberFunction::MemberFunction(HqlCppTranslator & _translator, BuildCtx & classctx, const char * text, unsigned _flags) : translator(_translator), ctx(classctx)
{
    ctx.addQuotedCompound(text);
    translator.pushMemberFunction(*this);
    flags = _flags;
}

MemberFunction::MemberFunction(HqlCppTranslator & _translator, BuildCtx & classctx, StringBuffer & text, unsigned _flags) : translator(_translator), ctx(classctx)
{
    ctx.addQuotedCompound(text);
    translator.pushMemberFunction(*this);
    flags = _flags;
}

MemberFunction::~MemberFunction()
{
    translator.popMemberFunction();
}

//---------------------------------------------------------------------------

class ChildSpotterInfo : public HoistingTransformInfo
{
public:
    ChildSpotterInfo(IHqlExpression * _original) : HoistingTransformInfo(_original) { spareByte2 = false; }
    
    inline bool getHoist() { return spareByte2 != 0; }
    inline void setHoist() { spareByte2 = true; }

private:
    using HoistingTransformInfo::spareByte2;                //prevent derived classes from also using this spare byte
};


static HqlTransformerInfo childDatasetSpotterInfo("ChildDatasetSpotter");
class ChildDatasetSpotter : public HoistingHqlTransformer
{
public:
    ChildDatasetSpotter(HqlCppTranslator & _translator, BuildCtx & _ctx) 
        : HoistingHqlTransformer(childDatasetSpotterInfo, HTFnoteallconditionals), translator(_translator), ctx(_ctx) 
    { 
        candidate = false; 
    }

    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr)
    {
        return CREATE_NEWTRANSFORMINFO(ChildSpotterInfo, expr);
    }
    inline ChildSpotterInfo * queryExtra(IHqlExpression * expr)     { return static_cast<ChildSpotterInfo *>(queryTransformExtra(expr)); }

    virtual void analyseExpr(IHqlExpression * expr)
    {
        if (pass == 0)
        {
            if (!analyseThis(expr))
                return;
            switch (expr->getOperator())
            {
            case no_if:
            case no_and:
            case no_which:
            case no_rejected:
            case no_or:
            case no_map:
                {
                    analyseExpr(expr->queryChild(0));
                    conditionDepth++;
                    ForEachChildFrom(i, expr, 1)
                        analyseExpr(expr->queryChild(i));
                    conditionDepth--;
                    break;
                }
            case no_choose:
            case no_case:
                {
                    analyseExpr(expr->queryChild(0));
                    analyseExpr(expr->queryChild(1));
                    conditionDepth++;
                    ForEachChildFrom(i, expr, 2)
                        analyseExpr(expr->queryChild(i));
                    conditionDepth--;
                    break;
                }
            default:
                doAnalyseExpr(expr);
                break;
            }
        }
        else
        {
            if (!alreadyVisited(expr))
                markHoistPoints(expr);
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
        switch (op)
        {
        case no_transform:
        case no_assign:
        case no_assignall:
            break;
        default:
            if (!containsNonActiveDataset(expr))
                return;
            break;
        }

        if (expr->isDataset() || (expr->isDatarow() && (op != no_select)))
        {
            if (isUsedUnconditionallyEnough(expr) && !translator.canAssignInline(&ctx, expr))
            {
                candidate = true;
                queryExtra(expr)->setHoist();
                return;
            }
            if (walkFurtherDownTree(expr))
                doAnalyseExpr(expr);
            return;
        }

        switch (op)
        {
        case no_createset:
            //not really good enough - need to prevent anything within these from being hoisted.
            return;
        case no_select:
        case no_if:
        case no_choose:
        case no_case:
        case no_and:
        case no_which:
        case no_rejected:
        case no_or:
        case no_range:
        case no_rangeto:
        case no_rangefrom:
        case no_rangecommon:
        case no_list:
        case no_selectnth:
        case no_mul:
        case no_div:
        case no_modulus:
        case no_negate:
        case no_add:
        case no_sub:
        case no_exp:
        case no_power:
        case no_round:
        case no_roundup:
        case no_ln:
        case no_log10:
        case no_sin:
        case no_cos:
        case no_tan:
        case no_asin:
        case no_acos:
        case no_atan:
        case no_atan2:
        case no_sinh:
        case no_cosh:
        case no_tanh:
        case no_sqrt:
        case no_truncate:
        case no_cast:
        case no_implicitcast:
        case no_abs:
        case no_charlen:
        case no_sizeof:
        case no_offsetof:
        case no_band:
        case no_bor:
        case no_bxor:
        case no_bnot:
        case no_order:          //?? also a comparison
        case no_rank:
        case no_ranked:
        case no_hash:
        case no_typetransfer:
        case no_lshift:
        case no_rshift:
        case no_crc:
        case no_random:
        case no_counter:
        case no_address:
        case no_hash32:
        case no_hash64:
        case no_wuid:
        case no_existslist:
        case no_countlist:
        case no_maxlist:
        case no_minlist:
        case no_sumlist:
        case no_unicodeorder:
        case no_assertkeyed:
        case no_hashmd5:
        case no_concat:
        case no_substring:
        case no_asstring:
        case no_intformat:
        case no_realformat:
        case no_trim:
        case no_fromunicode:
        case no_tounicode:
        case no_keyunicode:
        case no_rowdiff:
        case no_regex_find:
        case no_regex_replace:
        case no_eq:
        case no_ne:
        case no_lt:
        case no_le:
        case no_gt:
        case no_ge:
        case no_not:
        case no_notnot:
        case no_xor:
        case no_notin:
        case no_in:
        case no_notbetween:
        case no_between:
        case no_is_valid:
        case no_alias:
        case no_transform:
        case no_assign:
        case no_alias_scope:
        case no_getenv:
        case no_assignall:
            doAnalyseExpr(expr);
            break;
        default:
            doAnalyseExpr(expr);
            break;
        }
    }

    inline bool isUsedUnconditionallyEnough(IHqlExpression * expr)
    {
        IHqlExpression * search = expr;
        loop
        {
            if (isUsedUnconditionally(search))
                return true;
            switch (search->getOperator())
            {
            case no_selectnth:
            case no_newaggregate:
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


    IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        OwnedHqlExpr transformed = HoistingHqlTransformer::createTransformed(expr);
        updateOrphanedSelectors(transformed, expr);

        if ((expr->isDataset() || expr->isDatarow()) && (expr->getOperator() != no_select))
        {
            if (queryExtra(expr)->getHoist() && !translator.canAssignInline(&ctx, transformed))
            {
                if (!builder)
                    builder.setown(new ChildGraphBuilder(translator));
                return builder->addDataset(expr);
            }
        }

        return transformed.getClear();
    }

    virtual IHqlExpression * transformIndependent(IHqlExpression * expr) { return LINK(expr); }

    bool hasCandidate() const { return candidate; }
    ChildGraphBuilder * getBuilder() { return builder.getClear(); };


protected:
    HqlCppTranslator & translator;
    Owned<ChildGraphBuilder> builder;
    BuildCtx & ctx;
    bool candidate;
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

    void clear()
    {
        builder.clear();
        pending.kill();
        processed = false;
    }

    bool hasGraphPending()
    {
        spotChildDatasets();
        return builder != NULL;
    }

    void flush(BuildCtx & ctx)
    {
        spotChildDatasets();
        if (builder)
        {
            builder->generateGraph(ctx);
            builder.clear();
        }
        combineConditions();
        optimizeAssigns();
        ForEachItemIn(i, pending)
            translator.buildStmt(ctx, &pending.item(i));
        pending.kill();
    }

    void generatePrefetch(BuildCtx & ctx, OwnedHqlExpr * retGraphExpr, OwnedHqlExpr * retResultsExpr)
    {
        builder->generatePrefetchGraph(ctx, retGraphExpr, retResultsExpr);
        builder.clear();
    }

protected:
    //Combine multiple conditional assigns, where the guard condition is the same.
    void combineConditions()
    {
        pending.combineConditions();
    }

    void spotChildDatasets()
    {
        if (!processed && translator.queryCommonUpChildGraphs())
        {
            ChildDatasetSpotter spotter(translator, buildctx);
            for (unsigned pass=0; pass < 2; pass++)
            {
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
                        spotter.analyse(value, pass);
                }
            }

            if (spotter.hasCandidate())
            {
                HqlExprArray replacement;
                ForEachItemIn(i, pending)
                    replacement.append(*spotter.transformRoot(&pending.item(i)));
                replaceArray(pending, replacement);
                builder.setown(spotter.getBuilder());
            }
            processed = true;
        }
    }

    virtual void optimizeAssigns() {}


protected:
    HqlCppTranslator & translator;
    BuildCtx buildctx;
    StatementCollection pending;
    Owned<ChildGraphBuilder> builder;
    bool processed;
};

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
            DelayedStatementExecutor(other.translator, _ctx), mapper(other.mapper), assigns(other.assigns)
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
        switch (transform->getAnnotationKind())
        {
        case annotate_meta:
            translator.queryWarningProcessor().processMetaAnnotation(transform);
            break;
        case annotate_symbol:
            {
                WarningProcessor::OnWarningStateSymbolBlock saved(translator.queryWarningProcessor(), transform);
                doTransform(ctx, body, self);
                return;
            }
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
    buildTransformChildren(ctx, self->queryRecord(), self->querySelector());
    flush(ctx);
    checkAssigned();
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
        expr.setown(spotScalarCSE(expr));
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


unsigned HqlCppTranslator::cppIndexNextActivity(bool isChildActivity)
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
        }
    }
    return curCppFile;
}

//---------------------------------------------------------------------------

static IHqlExpression * createResultAttribute(IHqlExpression * seq, IHqlExpression * name)
{
    //if a named user output then set seq to the name so that workunit reads from the named symbol get commoned up correctly
    if (name && !name->queryType()->isInteger() && seq->queryValue()->getIntValue() >= 0)
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
    if (recordRequiresSerialization(record) || options.finalizeAllRows)
        return true;
    if (options.finalizeAllVariableRows && isVariableSizeRecord(record))
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

    if ((!targetIsOwnedRow && isFixedWidthDataset(record) && !options.alwaysCreateRowBuilder) || !options.supportDynamicRows)
    {
        LinkedHqlExpr targetArg = boundTarget;
        if (targetIsOwnedRow)
        {
            OwnedHqlExpr allocator = createRowAllocator(ctx, record);

            StringBuffer valueText;
            valueText.append("(byte *)");
            generateExprCpp(valueText, allocator).append("->createRow()");

            StringBuffer setText;
            generateExprCpp(setText, boundTarget);
            setText.append(".setown(").append(valueText).append(");");
            ctx.addQuoted(setText);
            targetArg.setown(getPointer(boundTarget));
        }

        BoundRow * self = bindSelf(ctx, targetRow->queryDataset(), targetArg, NULL);
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

BoundRow * HqlCppTranslator::declareLinkedRow(BuildCtx & ctx, IHqlExpression * expr, bool isMember)
{
    assertex(expr->isDatarow());

    StringBuffer rowName;
    getUniqueId(rowName.append('r'));

    IHqlExpression * record = expr->queryRecord();
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

    if (declarectx != &ctx)
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
    bool createRowDynamically = tempRowRequiresFinalize(record) || ((maxRecordSize > options.maxStaticRowSize) && (maxRecordSize > options.maxLocalRowSize));
    if (createRowDynamically)
    {
        return declareLinkedRow(ctx, expr, &ctx != &codectx);
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
    IHqlExpression * rowBuilder = builder->queryBound();
    bool targetIsOwnedRow = hasWrapperModifier(targetRow->queryType());

    IHqlExpression * record = rowBuilder->queryRecord();
    if (builder->queryBuilder() && targetIsOwnedRow)
    {
        OwnedHqlExpr createdRowSize = getRecordSize(builder->querySelector());
        HqlExprArray args;
        args.append(*LINK(builder->queryBuilder()));
        args.append(*LINK(createdRowSize));
        OwnedHqlExpr call = bindFunctionCall(finalizeRowClearAtom, args, targetRow->queryType());
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
    if (searchFilename == filename.get())
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

MetaInstance::MetaInstance(HqlCppTranslator & translator, IHqlExpression * _dataset)
{
    setDataset(translator, _dataset);
}

IHqlExpression * MetaInstance::queryRecord()
{
    return dataset->queryRecord();
}

void MetaInstance::setDataset(HqlCppTranslator & translator, IHqlExpression * _dataset)
{
    StringBuffer s,recordBase;

    dataset = _dataset;
    searchKey.setown(::getMetaUniqueKey(dataset));

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
    translator(_translator), classctx(ctx), startctx(ctx), createctx(ctx), nestedctx(ctx), onstartctx(ctx)
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
        if ((op == no_output) && dataset->hasProperty(_spill_Atom) && queryRealChild(dataset, 1))
            outputDataset = dataset->queryChild(0);
    }
    if ((op == no_setgraphresult) && translator.queryOptions().minimizeActivityClasses)
        outputDataset = dataset->queryChild(0);

    meta.setDataset(translator, outputDataset);

    activityId = translator.nextActivityId();

    StringBuffer s;
    className.set(s.clear().append("cAc").append(activityId).str());
    factoryName.set(s.clear().append("fAc").append(activityId).str());
    instanceName.set(s.clear().append("iAc").append(activityId).str());
    argsName.set(s.clear().append("oAc").append(activityId).str());

    OwnedHqlExpr boundName = createVariable(instanceName, dataset->getType());
    table = new ThorBoundActivity(dataset, boundName, activityId, translator.curSubGraphId(ctx), kind);
    isMember = false;
    instanceIsLocal = false;
    classStmt = NULL;
    classGroupStmt = NULL;
    hasChildActivity = false;

    includedInHeader = false;
    isCoLocal = false;
    executedRemotely = translator.targetThor();// && !translator.isNeverDistributed(dataset);
    containerActivity = NULL;
    subgraph = queryActiveSubGraph(ctx);
    onCreateStmt = NULL;
    onCreateMarker = 0;

    //count index and count disk need to be swapped to the new (much simpler) mechanism
    //until then, they need to be special cased.
    activityLocalisation = GraphNoAccess;
    containerActivity = translator.queryCurrentActivity(ctx);
    parentEvalContext.set(translator.queryEvalContext(ctx));
    parentExtract.set(static_cast<ParentExtract*>(ctx.queryFirstAssociation(AssocExtract)));

    if (parentExtract)
    {
        GraphLocalisation localisation = parentExtract->queryLocalisation();
        activityLocalisation = translator.isAlwaysCoLocal() ? GraphCoLocal : queryActivityLocalisation(dataset);

        if (translator.targetThor() && !translator.insideChildQuery(ctx))
            executedRemotely = true;
        else
            executedRemotely = ((activityLocalisation == GraphNonLocal) || (localisation == GraphRemote));

        isCoLocal = containerActivity && !executedRemotely && (localisation != GraphNonLocal) && (activityLocalisation != GraphNoAccess);    // if we supported GraphNonCoLocal the last test would not be needed

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
            GraphLocalisation localisation = queryActivityLocalisation(dataset);
            if ((kind == TAKsimpleaction) || (localisation == GraphNoAccess))
                executedRemotely = false;
        }
    }

    if (!parentExtract && (translator.getTargetClusterType() == RoxieCluster))
        executedRemotely = isNonLocal(dataset);

    if (containerActivity)
        containerActivity->hasChildActivity = true;
}

ActivityInstance::~ActivityInstance()
{
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

void ActivityInstance::addAttributeBool(const char * name, bool value)
{
    addGraphAttributeBool(graphNode, name, value);
}


void ActivityInstance::addLocationAttribute(IHqlExpression * location)
{
    if (!translator.queryOptions().reportLocations)
        return;

    unsigned line = location->getStartLine();
    if (line == 0)
        return;

    if (!locations.queryNewLocation(location))
        return;

    ISourcePath * sourcePath = location->querySourcePath();
    unsigned column = location->getStartColumn();
    StringBuffer s;
    s.append(sourcePath->str()).append("(").append(line);
    if (column)
        s.append(",").append(column);
    s.append(")");
    addAttribute("definition", s.str());
}


void ActivityInstance::addNameAttribute(IHqlExpression * symbol)
{
    //Not so sure about adding a location for a named symbol if there are other locations already present....
    //We should probably perform some deduping instead.
    addLocationAttribute(symbol);

    _ATOM name = symbol->queryName();
    if (!name)
        return;

    ForEachItemIn(i, names)
    {
        if (names.item(i).queryName() == name)
            return;
    }
    names.append(*symbol);
    addAttribute("name", name->str());
}

void ActivityInstance::removeAttribute(const char * name)
{
    removeGraphAttribute(graphNode, name);
}

static void expandHintValue(StringBuffer & s, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_constant:
        expr->queryValue()->getStringValue(s);
        break;
    case no_comma:
        expandHintValue(s, expr->queryChild(0));
        expandHintValue(s.append(","), expr->queryChild(1));
        break;
    case no_range:
        expandHintValue(s, expr->queryChild(0));
        expandHintValue(s.append(".."), expr->queryChild(1));
        break;
    case no_rangefrom:
        expandHintValue(s, expr->queryChild(0));
        s.append("..");
        break;
    case no_rangeto:
        expandHintValue(s.append(".."), expr->queryChild(0));
        break;
    case no_list:
        {
            s.append("[");
            ForEachChild(i, expr)
            {
                if (i)
                    s.append(",");
                expandHintValue(s, expr->queryChild(i));
            }
            s.append("]");
            break;
        }
    case no_attr:
        s.append(expr->queryName());
        break;
    default:
        s.append("?");
        break;
    }
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
                _ATOM name = cur->queryName();
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
    loop
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
    _ATOM name = attr->queryName();
    StringBuffer value;
    ForEachChild(i, attr)
    {
        if (i) 
            value.append(",");
        expandHintValue(value, attr->queryChild(i));
    }
    if (value.length() == 0)
        value.append("1");

    IPropertyTree * att = createPTree();
    att->setProp("@name", name->str());
    att->setProp("@value", value.str());
    graphNode->addPropTree("hint", att);
}

void ActivityInstance::processSection(IHqlExpression * section)
{
    StringBuffer sectionName;
    getStringValue(sectionName, section->queryChild(0));
    addAttribute("section", sectionName);
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


void ActivityInstance::createGraphNode(IPropertyTree * defaultSubGraph, bool alwaysExecuted)
{
    IPropertyTree * parentGraphNode = subgraph ? subgraph->tree.get() : defaultSubGraph;
    if (!parentGraphNode)
        return;
    assertex(kind < TAKlast);
    graphNode.set(parentGraphNode->addPropTree("node", createPTree()));

    graphNode->setPropInt64("@id", activityId);
    StringBuffer label;
    if (isGrouped)
        label.append("Grouped ");
    else if (isLocal)
        label.append("Local ");
    label.append(getActivityText(kind));

    graphNode->setProp("@label", graphLabel ? graphLabel.get() : label.str());

    IHqlExpression * cur = dataset;
    loop
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
    assertex(dataset->isAction() == isActivitySink(kind));
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

    if (graphEclText.length() == 0)
        toECL(dataset->queryBody(), graphEclText, false, true);

    if (graphEclText.length() > MAX_GRAPH_ECL_LENGTH)
    {
        graphEclText.setLength(MAX_GRAPH_ECL_LENGTH);
        graphEclText.append("...");
    }
    if (strcmp(graphEclText.str(), "<>") != 0)
        addAttribute("ecl", graphEclText.str());

    if (translator.queryOptions().includeHelperInGraph)
        addAttribute("helper", factoryName);

    if (translator.queryOptions().showMetaInGraph)
    {
        StringBuffer s;
        ITypeInfo * type = dataset->queryType();
        if (translator.targetThor())
        {
            IHqlExpression * distribution = queryDistribution(type);
            if (distribution && distribution->queryName() != localAtom)
                addAttribute("metaDistribution", getExprECL(distribution, s.clear(), true).str());
        }

        IHqlExpression * grouping = (IHqlExpression *)type->queryGroupInfo();
        if (grouping)
            addAttribute("metaGrouping", getExprECL(grouping, s.clear(), true).str());

        if (translator.targetThor())
        {
            IHqlExpression * globalSortOrder = (IHqlExpression *)type->queryGlobalSortInfo();
            if (globalSortOrder)
                addAttribute("metaGlobalSortOrder", getExprECL(globalSortOrder, s.clear(), true).str());
        }

        IHqlExpression * localSortOrder = (IHqlExpression *)type->queryLocalUngroupedSortInfo();
        if (localSortOrder)
            addAttribute("metaLocalSortOrder", getExprECL(localSortOrder, s.clear(), true).str());

        IHqlExpression * groupSortOrder = (IHqlExpression *)type->queryGroupSortInfo();
        if (groupSortOrder)
            addAttribute("metaGroupSortOrder", getExprECL(groupSortOrder, s.clear(), true).str());
    }

    if (translator.queryOptions().noteRecordSizeInGraph)
    {
        IHqlExpression * record = dataset->queryRecord();
        if (!record && (getNumChildTables(dataset) == 1))
            record = dataset->queryChild(0)->queryRecord();
        if (record)
        {
            size32_t maxSize = getMaxRecordSize(record, translator.getDefaultMaxRecordSize());
            if (isVariableSizeRecord(record))
            {
                size32_t minSize = getMinRecordSize(record);
                size32_t expectedSize = getExpectedRecordSize(record);
                StringBuffer temp;
                temp.append(minSize).append("..").append(maxSize).append("(").append(expectedSize).append(")");
                addAttribute("recordSize", temp.str());
            }
            else
                addAttributeInt("recordSize", maxSize);
        }
    }

    if (translator.queryOptions().showRecordCountInGraph && !dataset->isAction())
    {
        StringBuffer text;
        getRecordCountText(text, dataset);
        addAttribute("recordCount", text);
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
    StringBuffer s;

    sourceFileSequence.setown(getSizetConstant(translator.cppIndexNextActivity(isChildActivity())));
    if (containerActivity && colocalMember)
        containerActivity->noteChildActivityLocation(sourceFileSequence);

    classctx.set(helperAtom);
    classGroupStmt = classctx.addGroupPass(sourceFileSequence);

    classctx.associate(*this);
    classctx.addGroup();

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
            oncreatectx.addQuotedCompound("virtual void onCreate(ICodeContext * _ctx, IHThorArg * _colocal, MemoryBuffer * in)");
            oncreatectx.addQuoted(s.clear().append("colocal = (").append(containerActivity->className).append("*)_colocal;"));
        }
        else
        {
            onCreateStmt = oncreatectx.addQuotedCompound("virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in)");
        }

        oncreatectx.associateExpr(insideOnCreateMarker, NULL);
        oncreatectx.addQuoted("ctx = _ctx;");

        evalContext->onCreate.createFunctionStructure(translator, oncreatectx, true, executedRemotely ? "serializeCreateContext" : NULL);
        if (onCreateStmt)
            onCreateMarker = calcTotalChildren(onCreateStmt);
        
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
    }
}


void ActivityInstance::buildSuffix()
{
    //If onCreate() doesn't do anything special, then use an implementation in the base
    if (onCreateStmt && (calcTotalChildren(onCreateStmt) == onCreateMarker))
        onCreateStmt->setIncluded(false);

    const HqlCppOptions & options = translator.queryOptions();
    if (classStmt && (options.spotComplexClasses || options.showActivitySizeInGraph))
    {
        unsigned approxSize = calcTotalChildren(classStmt);
        if (options.spotComplexClasses && (approxSize >= options.complexClassesThreshold))
        {
            if ((options.complexClassesActivityFilter == 0) || (kind == options.complexClassesActivityFilter))
                translator.WARNING2(HQLWRN_ComplexHelperClass, activityId, approxSize);
        }
        if (options.showActivitySizeInGraph)
            addAttributeInt("approxClassSize", approxSize);
    }

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
            globalctx.addQuotedCompound(s);
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

    IHqlExpression * dataset = meta.dataset;
    ITypeInfo * type = dataset->queryType();
    if (type && type->getTypeCode() != type_void)
    {
        StringBuffer s;
        s.append("virtual IOutputMetaData * queryOutputMeta() { return &").append(meta.queryInstanceObject()).append("; }");
        classctx.addQuoted(s);
    }
}


void ActivityInstance::addConstructorMetaParameter()
{
    translator.buildMetaInfo(meta);

    ITypeInfo * type = meta.dataset->queryType();
    if (type && type->getTypeCode() != type_void)
    {
        StringBuffer s;
        s.append("&").append(meta.queryInstanceObject());

        OwnedHqlExpr metaExpr = createQuoted(s.str(), makeBoolType());
        constructorArgs.append(*metaExpr.getClear());
    }
}

ParentExtract * ActivityInstance::createNestedExtract()
{
    if (!nestedExtract)
    {
        nestedExtract.setown(new ParentExtract(translator, PETnested, GraphCoLocal, evalContext));
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


void DatasetSelector::buildDeserialize(BuildCtx & ctx, IHqlExpression * helper)
{
    column->buildDeserialize(translator, ctx, this, helper);
}


void DatasetSelector::buildSerialize(BuildCtx & ctx, IHqlExpression * helper)
{
    column->buildSerialize(translator, ctx, this, helper);
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
                _ATOM name = field->queryName();
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
        classctx.addQuoted("ICodeContext * ctx;");
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
    onCreateStmt = oncreatectx.addQuotedCompound("void onCreate(ICodeContext * _ctx)");
    oncreatectx.associateExpr(insideOnCreateMarker, NULL);
    oncreatectx.addQuoted("ctx = _ctx;");

    evalContext->onCreate.createFunctionStructure(translator, oncreatectx, true, NULL);
    onCreateMarker = calcTotalChildren(onCreateStmt);
}

void GlobalClassBuilder::completeClass(unsigned priority)
{
    if (onCreateStmt && (calcTotalChildren(onCreateStmt) == onCreateMarker))
        onCreateStmt->setIncluded(false);

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
        accessctx.addQuotedCompound(prototype);
        accessctx.addQuoted(s.clear().append(className).append("* p = new ").append(className).append("(activityId); "));
        accessctx.addQuoted("p->onCreate(ctx);");
        accessctx.addQuoted("return p;");

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
}


//---------------------------------------------------------------------------

bool HqlCppTranslator::insideOnCreate(BuildCtx & ctx)
{
    return ctx.queryMatchExpr(insideOnCreateMarker) != NULL;
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

void HqlCppTranslator::beginNestedClass(BuildCtx & ctx, const char * member, const char * bases, const char * memberExtra, ParentExtract * extract)
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

    ctx.addQuotedCompound(begin.str(), end.str());

    OwnedHqlExpr colocalName = createVariable("activity", makeVoidType());
    ActivityInstance * activity = queryCurrentActivity(ctx);
    if (activity)
    {
        Owned<ParentExtract> nestedUse = extract ? LINK(extract) : activity->createNestedExtract();
        NestedEvalContext * nested = new NestedEvalContext(*this, member, nestedUse, queryEvalContext(ctx), colocalName, ctx, ctx);
        ctx.associateOwn(*nested);
        nested->initContext();
    }
}

void HqlCppTranslator::endNestedClass()
{
}

void HqlCppTranslator::pushMemberFunction(MemberFunction & func)
{
}

void HqlCppTranslator::popMemberFunction()
{
}


void HqlCppTranslator::doBuildFunctionReturn(BuildCtx & ctx, ITypeInfo * type, IHqlExpression * value)
{
    bool returnByReference = false;
    CHqlBoundTarget target;

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
    case type_table:
    case type_groupedtable:
    case type_row:
        initBoundStringTarget(target, type, "__lenResult", "__result");
        returnByReference = true;
        break;
    case type_set:
        target.isAll.setown(createVariable("__isAllResult", makeBoolType()));
        target.length.setown(createVariable("__lenResult", LINK(sizetType)));
        target.expr.setown(createVariable("__result", makeReferenceModifier(LINK(type))));
        returnByReference = true;
        break;
    }

    if (returnByReference)
        buildExprAssign(ctx, target, value);
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
    StringBuffer s;
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound(s.append("virtual const char * ").append(name).append("()"));
    if (value)
        buildReturn(funcctx, value, constUnknownVarStringType);
    else
        funcctx.addReturn(queryQuotedNullExpr());
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
            cseValue.setown(spotScalarCSE(cseValue));

        BuildCtx funcctx(ctx);
        if (false)
        {
            HqlExprArray parameters;
            OwnedHqlExpr entrypoint = createAttribute(entrypointAtom, createConstant(name));
            OwnedHqlExpr body = createValue(no_null, LINK(type), LINK(entrypoint));
            OwnedHqlExpr formals = createValue(no_sortlist, makeSortListType(NULL), parameters);
            OwnedHqlExpr attrs = createAttribute(virtualAtom);
            OwnedHqlExpr function = createFunctionDefinition(NULL, LINK(body), LINK(formals), NULL, LINK(attrs));
            funcctx.addFunction(function);
        }
        else
        {
            StringBuffer s, returnParameters;
            s.append("virtual ");
            generateFunctionReturnType(s, returnParameters, type, NULL, options.targetCompiler);
            s.append(" ").append(name).append("(").append(returnParameters).append(")");
            funcctx.addQuotedCompound(s);
        }

        doBuildFunctionReturn(funcctx, type, cseValue);
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
    IHqlExpression * filename = NULL;
    if (table)
    {
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
    ColumnToOffsetMap * map = queryRecordOffsetMap(record);
    if (!map)
        return 0;
    return map->getMaxSize();
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
    IHqlExpression * maxLength = queryProperty(maxLengthAtom, attrs);
    if (maxLength)
        return (unsigned)getIntValue(maxLength->queryChild(0), 0);
    return MAX_CSV_RECORD_SIZE;
}


bool HqlCppTranslator::isFixedWidthDataset(IHqlExpression * dataset)
{
    IHqlExpression * record = dataset->queryRecord();
    return queryRecordOffsetMap(record)->isFixedWidth();
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

    BuildCtx * declarectx = &ctx;
    BuildCtx * callctx = &ctx;
    getInvariantMemberContext(ctx, &declarectx, &callctx, true, false);

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


void HqlCppTranslator::buildMetaSerializerClass(BuildCtx & ctx, IHqlExpression * record, const char * serializerName)
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
        BuildCtx serializectx(serializer.startctx);
        serializectx.addQuotedCompound("virtual void serialize(IRowSerializerTarget & out, const byte * self)");
        OwnedHqlExpr helper = createVariable("out", makeBoolType());
        BoundRow * row = bindTableCursor(serializectx, dataset, "self");
        OwnedHqlExpr size = getRecordSize(row->querySelector());
        CHqlBoundExpr boundSize;
        buildExpr(serializectx, size, boundSize);
        if (recordRequiresSerialization(record))
        {
            Owned<IReferenceSelector> selector = buildActiveRow(serializectx, row->querySelector());
            selector->buildSerialize(serializectx, helper);
        }
        else
        {
            HqlExprArray args;
            args.append(*LINK(helper));
            args.append(*LINK(boundSize.expr));
            args.append(*LINK(row->queryBound()));
            OwnedHqlExpr call = bindTranslatedFunctionCall(serializerPutAtom, args);
            serializectx.addExpr(call);
        }
    }

    serializer.setIncomplete(false);
    serializer.completeClass(RowMetaPrio);
}

void HqlCppTranslator::buildMetaDeserializerClass(BuildCtx & ctx, IHqlExpression * record, const char * deserializerName)
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
        BuildCtx deserializectx(deserializer.startctx);
        deserializectx.addQuotedCompound("virtual size32_t deserialize(ARowBuilder & crSelf, IRowDeserializerSource & in)");
        BoundRow * row = bindSelf(deserializectx, dataset, "crSelf");
        ensureRowAllocated(deserializectx, "crSelf");

        OwnedHqlExpr helper = createVariable("in", makeBoolType());
        Owned<IReferenceSelector> selector = buildActiveRow(deserializectx, row->querySelector());
        selector->buildDeserialize(deserializectx, helper);
        buildReturnRecordSize(deserializectx, row);
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
        BuildCtx prefetchctx(prefetcher.startctx);
        IHqlStmt * stmt = prefetchctx.addQuotedCompound("virtual void readAhead(IRowDeserializerSource & in)");
        OwnedHqlExpr helper = createVariable("in", makeBoolType());

        ok = queryRecordOffsetMap(record)->buildReadAhead(*this, prefetchctx, helper);
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
    switch  (expr->getOperator())
    {
    case no_field:
        switch (expr->queryType()->getTypeCode())
        {
        case type_bitfield:
            {
                ColumnToOffsetMap * map = queryRecordOffsetMap(rowRecord);
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
                return createField(expr->queryName(), LINK(fieldType), args);
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
        return createAttribute(rtlFieldKeyMarkerAtom, LINK(expr), LINK(rowRecord));
    return LINK(expr);
}

unsigned HqlCppTranslator::buildRtlField(StringBuffer * instanceName, IHqlExpression * fieldKey)
{
    BuildCtx declarectx(*code, declareAtom);
    HqlExprAssociation * match = declarectx.queryMatchExpr(fieldKey);
    if (match)
    {
        IHqlExpression * mapped = match->queryExpr();
        if (instanceName)
            mapped->queryChild(0)->toString(*instanceName);
        return (unsigned)getIntValue(mapped->queryChild(1));
    }

    IHqlExpression * field = fieldKey;
    IHqlExpression * rowRecord = NULL;
    if (field->isAttribute())
    {
        field = fieldKey->queryChild(0);
        rowRecord = fieldKey->queryChild(1);
    }

    StringBuffer name;
    unsigned typeFlags = 0;
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
        }

        StringBuffer typeName;
        typeFlags = buildRtlType(typeName, fieldType);

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


        StringBuffer xpathName, xpathItem;
        switch (fieldType->getTypeCode())
        {
        case type_set:
            extractXmlName(xpathName, &xpathItem, NULL, field, "Item", false);
            break;
        case type_table:
        case type_groupedtable:
            extractXmlName(xpathName, &xpathItem, NULL, field, "Row", false);
            //Following should be in the type processing, and the type should include the information
            if (field->hasProperty(sizeAtom) || field->hasProperty(countAtom))
                typeFlags |= RFTMinvalidxml;
            break;
        default:
            extractXmlName(xpathName, NULL, NULL, field, NULL, false);
            break;
        }

        if (xpathName.length() && (xpathName.charAt(0) == '@'))
            typeFlags |= RFTMhasxmlattr;

        //Format of the xpath field is (nested-item 0x01 repeated-item)
        StringBuffer xpathFull, xpathCppText;
        xpathFull.append(xpathName);
        if (xpathItem.length())
            xpathFull.append(xpathCompoundSeparatorChar).append(xpathItem);

        if (strcmp(lowerName, xpathFull) != 0)
            appendStringAsQuotedCPP(xpathCppText, xpathFull.length(), xpathFull.str(), false);
        else
            xpathCppText.append("NULL");

        StringBuffer definition;
        definition.append("const RtlFieldStrInfo ").append(name).append("(\"").append(lowerName).append("\",").append(xpathCppText).append(",&").append(typeName).append(");");

        BuildCtx fieldctx(declarectx);
        fieldctx.setNextPriority(TypeInfoPrio);
        fieldctx.addQuoted(definition);

        name.insert(0, "&");
    }
    OwnedHqlExpr nameExpr = createVariable(name.str(), makeBoolType());
    OwnedHqlExpr mapped = createAttribute(fieldAtom, LINK(nameExpr), getSizetConstant(typeFlags));
    declarectx.associateExpr(fieldKey, mapped);
    if (instanceName)
        instanceName->append(name);
    return typeFlags;
}


unsigned HqlCppTranslator::buildRtlIfBlockField(StringBuffer & instanceName, IHqlExpression * ifblock, IHqlExpression * rowRecord)
{
    StringBuffer typeName, s;
    BuildCtx declarectx(*code, declareAtom);

    //First generate a pseudo type entry for an ifblock.
    unsigned fieldType = type_ifblock|RFTMcontainsifblock;
    {
        unsigned length = 0;
        StringBuffer childTypeName;
        unsigned childType = buildRtlRecordFields(childTypeName, ifblock->queryChild(1), rowRecord);
        fieldType |= (childType & (RFTMcontainsunknown|RFTMinvalidxml|RFTMhasxmlattr));

        StringBuffer className;
        typeName.append("ty").append(++nextTypeId);
        className.append("tyc").append(nextFieldId);

        //The ifblock needs a unique instance of the class to evaluate the test
        BuildCtx fieldclassctx(declarectx);
        fieldclassctx.setNextPriority(TypeInfoPrio);
        fieldclassctx.addQuotedCompound(s.clear().append("struct ").append(className).append(" : public RtlIfBlockTypeInfo"), ";");
        fieldclassctx.addQuoted(s.clear().append(className).append("() : RtlIfBlockTypeInfo(0x").appendf("%x", fieldType).append(",").append(0).append(",").append(childTypeName).append(") {}"));

        OwnedHqlExpr anon = createDataset(no_anon, LINK(rowRecord));
        BuildCtx condctx(fieldclassctx);
        condctx.addQuotedCompound("virtual bool getCondition(const byte * self) const");
        BoundRow * self = bindTableCursor(condctx, anon, "self");
        OwnedHqlExpr cond = self->bindToRow(ifblock->queryChild(0), querySelfReference());
        buildReturn(condctx, cond);


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
        StringBuffer next;
        unsigned childType = 0;
        switch (cur->getOperator())
        {
        case no_field:
        case no_ifblock:
            {
                OwnedHqlExpr fieldKey = getRtlFieldKey(cur, rowRecord);
                childType = buildRtlField(&fieldListText, fieldKey);
                fieldListText.append(",");
                break;
            }
        case no_record:
            childType = expandRtlRecordFields(fieldListText, cur, rowRecord);
            break;
        }
        fieldType |= (childType & (RFTMcontainsunknown|RFTMinvalidxml|RFTMhasxmlattr));
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

unsigned HqlCppTranslator::getRtlFieldInfo(StringBuffer & fieldInfoName, IHqlExpression * field, IHqlExpression * rowRecord)
{
    OwnedHqlExpr fieldKey = getRtlFieldKey(field, rowRecord);
    return buildRtlField(&fieldInfoName, fieldKey);
}

unsigned HqlCppTranslator::buildRtlType(StringBuffer & instanceName, ITypeInfo * type)
{
    assertex(type);
    type_t tc = type->getTypeCode();
    if (tc == type_record)
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

    StringBuffer name, className, arguments;
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

    unsigned fieldType= 0;
    if (tc == type_alien)
    {
        ITypeInfo * physicalType = queryAlienType(type)->queryPhysicalType();
        if (physicalType->getSize() != UNKNOWN_LENGTH)
        {
            //Don't use the generated class for xml generation since it will generate physical rather than logical
            fieldType |= (RFTMalien|RFTMinvalidxml);
            type = physicalType;
            tc = type->getTypeCode();
        }
        else
        {
            fieldType |= RFTMunknownsize;
            //can't work out the size of the field - to keep it as unknown for the moment.
            //until the alien field type is supported
        }
    }
    fieldType |= tc;
    unsigned length = type->getSize();
    if (length == UNKNOWN_LENGTH)
    {
        fieldType |= RFTMunknownsize;
        length = 0;
    }

    unsigned childType = 0;
    switch (tc)
    {
    case type_boolean:
        className.append("RtlBoolTypeInfo");
        break;
    case type_real:
        className.append("RtlRealTypeInfo");
        break;
    case type_date:
    case type_enumerated:
    case type_int:
        className.append("RtlIntTypeInfo");
        if (!type->isSigned()) 
            fieldType |= RFTMunsigned;
        break;
    case type_swapint:
        className.append("RtlSwapIntTypeInfo");
        if (!type->isSigned()) 
            fieldType |= RFTMunsigned;
        break;
    case type_packedint:
        className.append("RtlPackedIntTypeInfo");
        if (!type->isSigned()) 
            fieldType |= RFTMunsigned;
        break;
    case type_decimal:
        className.append("RtlDecimalTypeInfo");
        if (!type->isSigned()) 
            fieldType |= RFTMunsigned;
        length = type->getDigits() | (type->getPrecision() << 16);
        break;
    case type_char:
        className.append("RtlCharTypeInfo");
        break;
    case type_data:
        className.append("RtlDataTypeInfo");
        break;
    case type_qstring:
        className.append("RtlQStringTypeInfo");
        length = type->getStringLen();
        break;
    case type_varstring:
        className.append("RtlVarStringTypeInfo");
        if (type->queryCharset() && type->queryCharset()->queryName()==ebcdicAtom)
            fieldType |= RFTMebcdic;
        length = type->getStringLen();
        break;
    case type_string:
        className.append("RtlStringTypeInfo");
        if (type->queryCharset() && type->queryCharset()->queryName()==ebcdicAtom)
            fieldType |= RFTMebcdic;
        break;
    case type_bitfield:
        {
        className.append("RtlBitfieldTypeInfo");
        unsigned size = type->getSize();
        unsigned bitsize = type->getBitSize();
        unsigned offset = (unsigned)getIntValue(queryPropertyChild(type, bitfieldOffsetAtom, 0),-1);
        bool isLastBitfield = (queryProperty(type, isLastBitfieldAtom) != NULL);
        if (isLastBitfield)
            fieldType |= RFTMislastbitfield;
        if (!type->isSigned()) 
            fieldType |= RFTMunsigned;
        length = size | (bitsize << 8) | (offset << 16);
        break;
        }
    case type_record:
        {
            IHqlExpression * record = ::queryRecord(type);
            className.append("RtlRecordTypeInfo");
            arguments.append(",");
            childType = buildRtlRecordFields(arguments, record, record);
//          fieldType |= (childType & RFTMcontainsifblock);
            length = getMaxRecordSize(record);
            if (!isFixedRecordSize(record))
                fieldType |= RFTMunknownsize;
            break;
        }
    case type_row:
        {
            className.clear().append("RtlRowTypeInfo");
            arguments.append(",&");
            childType = buildRtlType(arguments, ::queryRecordType(type));
//          fieldType |= (childType & RFTMcontainsifblock);
            if (hasLinkCountedModifier(type))
                fieldType |= RFTMlinkcounted;
            break;
        }
    case type_table:
    case type_groupedtable:
        {
            className.clear().append("RtlDatasetTypeInfo");
            arguments.append(",&");
            childType = buildRtlType(arguments, ::queryRecordType(type));
            if (hasLinkCountedModifier(type))
                fieldType |= RFTMlinkcounted;
            break;
        }
    case type_set:
        className.clear().append("RtlSetTypeInfo");
        arguments.append(",&");
        childType = buildRtlType(arguments, type->queryChildType());
        break;
    case type_unicode:
        className.clear().append("RtlUnicodeTypeInfo");
        arguments.append(", \"").append(type->queryLocale()).append("\"").toLowerCase();
        length = type->getStringLen();
        break;
    case type_varunicode:
        className.clear().append("RtlVarUnicodeTypeInfo");
        arguments.append(", \"").append(type->queryLocale()).append("\"").toLowerCase();
        length = type->getStringLen();
        break;
    case type_utf8:
        className.clear().append("RtlUtf8TypeInfo");
        arguments.append(", \"").append(type->queryLocale()).append("\"").toLowerCase();
        length = type->getStringLen();
        break;
    case type_blob:
    case type_pointer:
    case type_class:
    case type_array:
    case type_void:
    case type_alien:
    case type_none:
    case type_any:
    case type_pattern:
    case type_rule:
    case type_token:
    case type_feature:
    case type_event:
    case type_null:
    case type_scope:
    case type_transform:
    default:
        className.append("RtlUnimplementedTypeInfo");
        fieldType |= (RFTMcontainsunknown|RFTMinvalidxml);
        break;
    }
    fieldType |= (childType & (RFTMcontainsunknown|RFTMinvalidxml|RFTMhasxmlattr));

    StringBuffer definition;
    definition.append("const ").append(className).append(" ").append(name).append("(0x").appendf("%x", fieldType).append(",").append(length).append(arguments).append(");");

    BuildCtx typectx(declarectx);
    typectx.setNextPriority(TypeInfoPrio);
    typectx.addQuoted(definition);

    OwnedHqlExpr nameExpr = createVariable(name.str(), makeVoidType());
    OwnedHqlExpr mapped = createAttribute(fieldAtom, LINK(nameExpr), getSizetConstant(fieldType));
    declarectx.associateExpr(search, mapped);
    instanceName.append(name);
    return fieldType;
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

    IHqlExpression * dataset = instance.dataset;
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
    StringBuffer serializerName, deserializerName, prefetcherName;

    StringBuffer endText;

    endText.append(" ").append(instance.instanceName).append(";");
    BuildCtx metactx(declarectx);

    ITypeInfo * type = dataset->queryType();
    IHqlExpression * record = dataset->queryRecord();
    ColumnToOffsetMap * map = queryRecordOffsetMap(record);
    
    unsigned flags = MDFhasserialize;       // we always generate a serialize since 
    bool useTypeForXML = false;
    if (type && type->getTypeCode() == type_groupedtable)
        flags |= MDFgrouped;
    if (map)
        flags |= MDFhasxml;
    if (record)
    {
        if (recordRequiresDestructor(record))
            flags |= MDFneeddestruct;
        if (recordRequiresSerialization(record))
            flags |= MDFneedserialize;
        if (maxRecordSizeUsesDefault(record))
            flags |= MDFunknownmaxlength;
        useTypeForXML = true;
    }

    if (type && type->getTypeCode() == type_groupedtable)
    {
        OwnedHqlExpr ungrouped = createDataset(no_group, LINK(dataset));
        MetaInstance ungroupedMeta(*this, ungrouped);
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
        if (map && (!map->isFixedWidth() || (flags & MDFneedserialize)))
        {
            //Base class provides a default variable width implementation
            if (flags & MDFneedserialize)
            {
                serializerName.append("s").append(instance.metaName);
                buildMetaSerializerClass(declarectx, record, serializerName.str());
            }
            //still generate a deserialize for the variable width case because it offers protection
            //against accessing out of bounds data
            deserializerName.append("d").append(instance.metaName);
            buildMetaDeserializerClass(declarectx, record, deserializerName.str());

            //The base class implements prefetch using the serialized meta so no need to generate...
            if (!(flags & MDFneedserialize))
            {
                prefetcherName.append("p").append(instance.metaName);
                if (!buildMetaPrefetcherClass(declarectx, record, prefetcherName))
                    prefetcherName.clear();
            }
        }

        s.append("struct ").append(instance.metaName).append(" : public ");
        if (!map)
            s.append("CActionOutputMetaData");
        else if (map->isFixedWidth())
            s.append("CFixedOutputMetaData");
        else
            s.append("CVariableOutputMetaData");

        metactx.setNextPriority(RowMetaPrio);
        IHqlStmt * metaclass = metactx.addQuotedCompound(s, endText.str());
        metaclass->setIncomplete(true);

        if (map)
        {
            if (map->isFixedWidth())
            {
                unsigned fixedSize = map->getFixedRecordSize();
                s.clear().append("inline ").append(instance.metaName).append("() : CFixedOutputMetaData(").append(fixedSize).append(") {}");
                metactx.addQuoted(s);
            }
            else
            {
                unsigned minSize = getMinRecordSize(record);
                unsigned maxLength = map->getMaxSize();
                assertex(maxLength >= minSize);
#ifdef _DEBUG
                //Paranoia check to ensure the two methods agree.
                unsigned calcMinSize = map->getTotalMinimumSize();
                assertex(minSize == calcMinSize);
#endif
                //These use a CVariableOutputMetaData base class instead, and trade storage for number of virtuals
                s.clear().append("inline ").append(instance.metaName).append("() : CVariableOutputMetaData(").append(minSize).append(") {}");
                metactx.addQuoted(s);

                if (options.testIgnoreMaxLength)
                    maxLength = minSize;

                BuildCtx getctx(metactx);
                s.clear().append("virtual size32_t getRecordSize(const void * data)");
                getctx.addQuotedCompound(s);
                s.clear().append("if (!data) return ").append(maxLength).append(";");
                getctx.addQuoted(s.str());
                getctx.addQuoted("const unsigned char * left = (const unsigned char *)data;");

                LinkedHqlExpr selfDs = dataset;
                if (!selfDs->isDataset() || !selfDs->isDatarow())
                    selfDs.setown(createDataset(no_null, LINK(dataset->queryRecord())));

                BoundRow * selfRow = bindTableCursorOrRow(getctx, selfDs, "left");
                OwnedHqlExpr size = getRecordSize(selfRow->querySelector());
                buildReturn(getctx, size);

            }
            assertex(!(type && type->getTypeCode() == type_groupedtable));

            StringBuffer typeName;
            unsigned recordTypeFlags = buildRtlType(typeName, record->queryType());
            s.clear().append("virtual const RtlTypeInfo * queryTypeInfo() const { return &").append(typeName).append("; }");
            metactx.addQuoted(s);

            if (record->numChildren() != 0)
            {
                OwnedHqlExpr anon = createDataset(no_anon, LINK(dataset->queryRecord()));
                if (!useTypeForXML || (recordTypeFlags & (RFTMinvalidxml|RFTMhasxmlattr)))
                    buildXmlSerialize(metactx, anon, "toXML", true);
            }

            if (record)
                generateMetaRecordSerialize(metactx, record, serializerName.str(), deserializerName.str(), prefetcherName.str());

            if (flags != (MDFhasserialize|MDFhasxml))
                doBuildUnsignedFunction(metactx, "getMetaFlags", flags);

            if (flags & MDFneedserialize)
            {
                OwnedHqlExpr serializedRecord = getSerializedForm(record);
                OwnedHqlExpr serializedDataset = createDataset(no_anon, LINK(serializedRecord));

                MetaInstance serializedMeta(*this, serializedDataset);
                buildMetaInfo(serializedMeta);
                StringBuffer s;
                s.append("virtual IOutputMetaData * querySerializedMeta() { return &").append(serializedMeta.queryInstanceObject()).append("; }");
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
        MetaInstance childMeta(translator, selected);
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
                    case type_table:
                    case type_groupedtable:
                        {
                            OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(cur));
                            IHqlExpression * record = cur->queryRecord();
                            if (cur->hasProperty(_linkCounted_Atom))
                            {
                                //releaseRowset(ctx, count, rowset)
                                MetaInstance childMeta(translator, selected);
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
        translator.buildFunctionCall(ctx, destructMetaMemberAtom, args);
    }

    virtual void processRowset(BuildCtx & ctx, IHqlExpression * selected, MetaInstance & childMeta)
    {
        HqlExprArray args;
        args.append(*LINK(selected));
        translator.buildFunctionCall(ctx, releaseRowsetAtom, args);
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
        translator.buildFunctionCall(ctx, walkIndirectMetaMemberAtom, args);
    }

    virtual void processRowset(BuildCtx & ctx, IHqlExpression * selected, MetaInstance & childMeta)
    {
        HqlExprArray args;
        args.append(*LINK(visitor));
        args.append(*LINK(selected));
        translator.buildFunctionCall(ctx, IIndirectMemberVisitor_visitRowsetAtom, args);
    }

protected:
    LinkedHqlExpr visitor;
};



void HqlCppTranslator::generateMetaRecordSerialize(BuildCtx & ctx, IHqlExpression * record, const char * serializerName, const char * deserializerName, const char * prefetcherName)
{
    OwnedHqlExpr dataset = createDataset(no_null, LINK(record));

    if (recordRequiresDestructor(record))
    {
        BuildCtx destructctx(ctx);
        destructctx.addQuotedCompound("virtual void destruct(byte * self)");
        bindTableCursor(destructctx, dataset, "self");
        MetaDestructCallback builder(*this);
        builder.walkRecord(destructctx, dataset, record);
    }

    if (recordRequiresDestructor(record))
    {
        BuildCtx walkctx(ctx);
        OwnedHqlExpr visitor = createVariable("visitor", makeBoolType());       // makeClassType("IIndirectMemberVisitor");
        walkctx.addQuotedCompound("virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor)");
        bindTableCursor(walkctx, dataset, "self");
        MetaWalkIndirectCallback builder(*this, visitor);
        builder.walkRecord(walkctx, dataset, record);
    }

    if (serializerName && *serializerName)
    {
        BuildCtx serializectx(ctx);
        serializectx.addQuotedCompound("virtual IOutputRowSerializer * createRowSerializer(ICodeContext * ctx, unsigned activityId)");
        
        StringBuffer s;
        s.append("return cr").append(serializerName).append("(ctx, activityId);");
        serializectx.addQuoted(s);
    }

    if (deserializerName && *deserializerName)
    {
        BuildCtx deserializectx(ctx);
        deserializectx.addQuotedCompound("virtual IOutputRowDeserializer * createRowDeserializer(ICodeContext * ctx, unsigned activityId)");
        
        StringBuffer s;
        s.append("return cr").append(deserializerName).append("(ctx, activityId);");
        deserializectx.addQuoted(s);
    }

    if (prefetcherName && *prefetcherName)
    {
        BuildCtx deserializectx(ctx);
        deserializectx.addQuotedCompound("virtual CSourceRowPrefetcher * createRawRowPrefetcher(unsigned activityId)");
        
        StringBuffer s;
        s.append("return new ").append(prefetcherName).append("(activityId);");
        deserializectx.addQuoted(s);
    }
}

IHqlExpression * HqlCppTranslator::buildMetaParameter(IHqlExpression * arg)
{
    OwnedHqlExpr dataset = createDataset(no_anon, LINK(arg->queryRecord()));
    MetaInstance meta(*this, dataset);
    buildMetaInfo(meta);
    return createQuoted(meta.queryInstanceObject(), makeBoolType());
}

void HqlCppTranslator::buildMetaMember(BuildCtx & ctx, IHqlExpression * datasetOrRecord, const char * name)
{
    LinkedHqlExpr dataset = datasetOrRecord;
    if (datasetOrRecord->getOperator() == no_record)
        dataset.setown(createDataset(no_anon, LINK(datasetOrRecord)));

    MetaInstance meta(*this, dataset);
    StringBuffer s;

    buildMetaInfo(meta);
    s.append("virtual IOutputMetaData * ").append(name).append("() { return &").append(meta.queryInstanceObject()).append("; }");
    ctx.addQuoted(s);
}

void HqlCppTranslator::buildMetaForRecord(StringBuffer & name, IHqlExpression * record)
{
    OwnedHqlExpr dataset = createDataset(no_anon, LINK(record));
    MetaInstance meta(*this, dataset);
    buildMetaInfo(meta);
    name.append(meta.queryInstanceObject());
}

void HqlCppTranslator::buildMetaForSerializedRecord(StringBuffer & name, IHqlExpression * record, bool isGrouped)
{
    if (isGrouped)
    {
        HqlExprArray args;
        unwindChildren(args, record);
        args.append(*createField(__eogAtom, makeBoolType(), NULL, NULL));
        OwnedHqlExpr groupedRecord = record->clone(args);
        buildMetaForRecord(name, groupedRecord);
    }
    else
        buildMetaForRecord(name, record);
}

void HqlCppTranslator::ensureRowSerializer(StringBuffer & serializerName, BuildCtx & ctx, IHqlExpression * record, _ATOM kind)
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
    if (kind == serializerAtom)
        s.append("Owned<IOutputRowSerializer> ").append(uid).append(";");
    else
        s.append("Owned<IOutputRowDeserializer> ").append(uid).append(";");
    declarectx->addQuoted(s);

    OwnedHqlExpr ds = createDataset(no_anon, LINK(record));
    MetaInstance meta(*this, ds);
    buildMetaInfo(meta);

    s.clear().append(uid).append(".setown(").append(meta.queryInstanceObject());
    if (kind == serializerAtom)
        s.append(".createRowSerializer");
    else
        s.append(".createRowDeserializer");
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

    OwnedHqlExpr ds = createDataset(no_anon, LINK(record));
    MetaInstance meta(*this, ds);
    buildMetaInfo(meta);

    s.clear().append(uid).append(".setown(").append(meta.queryInstanceObject());
    s.append(".createRowPrefetcher(ctx, ");
    OwnedHqlExpr activityId = getCurrentActivityId(ctx);
    generateExprCpp(s, activityId);
    s.append("));");
    callctx->addQuoted(s);

    OwnedHqlExpr value = createVariable(uid.str(), makeBoolType());
    declarectx->associateExpr(marker, value);
    prefetcherName.append(uid);
}


IHqlExpression * HqlCppTranslator::createRowSerializer(BuildCtx & ctx, IHqlExpression * record, _ATOM kind)
{
    StringBuffer serializerName;
    ensureRowSerializer(serializerName, ctx, record, kind);
    return createQuoted(serializerName.str(), makeBoolType());
}

IHqlExpression * HqlCppTranslator::createResultName(IHqlExpression * name, bool expandLogicalName)
{
    IHqlExpression * resultName = ::createResultName(name);
    if (!expandLogicalName)
        return resultName;
    
    HqlExprArray args;
    args.append(*resultName);
    return bindFunctionCall(getExpandLogicalNameAtom, args);
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
    case type_table:
    case type_groupedtable:
        record.set(bound.expr->queryRecord());
        break;
    case type_set:
    case type_array:
        {
            ITypeInfo * elementType = type->queryChildType();
            HqlExprArray fields;
            fields.append(*createField(valueAtom, LINK(elementType), NULL));
            record.setown(createRecord(fields));
            break;
        }
    default:
        UNIMPLEMENTED;
    }

    ColumnToOffsetMap * map = queryRecordOffsetMap(record);
    if (map->isFixedWidth())
    {
        unsigned fixedSize = map->getFixedRecordSize();
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

    _ATOM func;
    if (getCount)
    {
        args.append(*getBoundSize(bound));
        args.append(*LINK(bound.expr));
        func = countRowsAtom;
    }
    else
    {
        args.append(*LINK(bound.count));
        args.append(*LINK(bound.expr));
        func = countToSizeAtom;
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
                break;
        }
    }
}

void HqlCppTranslator::buildGetResultInfo(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr * boundTarget, const CHqlBoundTarget * targetAssign)
{
    IHqlExpression * seq = queryPropertyChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryPropertyChild(expr, namedAtom, 0);
    if (!name)
        name = queryPropertyChild(expr, nameAtom, 0);

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
    assertex(!expr->hasProperty(internalAtom) && !expr->hasProperty(_internal_Atom));
    bool expandLogical = (seqValue == ResultSequencePersist) && !expr->hasProperty(_internal_Atom);

    HqlExprArray args;
    args.append(*createResultName(name, expandLogical));
    args.append(*LINK(seq));

    _ATOM func;
    ITypeInfo * type = expr->queryType();
    type_t ttc = type->getTypeCode();
    OwnedITypeInfo overrideType;
    switch(ttc)
    {
    case type_int:      func = getResultIntAtom; break;
    case type_swapint:  func = getResultIntAtom; break;
    case type_boolean:  func = getResultBoolAtom; break;
    case type_data:     func = getResultDataAtom; break;
    case type_table:
    case type_groupedtable:
    case type_set:
        //MORE: type_row...
        {
            OwnedHqlExpr record;
            bool ensureSerialized = true;
            if (ttc == type_table || ttc == type_groupedtable)
            {
                overrideType.set(type);
                record.set(::queryRecord(type));
                //NB: The result type (including grouping) will be overridden then this function is bound
                func = getResultDatasetAtom;
                bool defaultLCR = targetAssign ? hasLinkedRow(targetAssign->queryType()) : options.tempDatasetsUseLinkedRows;
                if (hasLinkCountedModifier(type) || defaultLCR)
                {
                    ensureSerialized = false;
                    args.append(*createRowAllocator(ctx, record));
                    args.append(*createRowSerializer(ctx, record, deserializerAtom));
                    args.append(*createConstant(isGrouped(expr)));
                    overrideType.setown(setLinkCountedAttr(overrideType, true));
                    func = getResultRowsetAtom;
                }
            }
            else
            {
                overrideType.set(type);
                ITypeInfo * elementType = type->queryChildType();
                OwnedHqlExpr field = createField(valueAtom, LINK(elementType), NULL);
                record.setown(createRecord(field));
                func = getResultSetAtom;
            }

            if (ensureSerialized && record)
                record.setown(getSerializedForm(record));

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
            break;
        }
    case type_string:
        {
            func = getResultStringAtom; 
            if ((type->queryCharset()->queryName() != asciiAtom) || !targetAssign)
                break;
            ITypeInfo * targetType = targetAssign->queryType();
            if ((targetType->getTypeCode() != type_string) || (targetType->getSize() == UNKNOWN_LENGTH) || 
                (targetType->queryCharset() != type->queryCharset()))
                break;
            //more: if (options.checkOverflow && queryUnqualifiedType(targetType) != queryUnqualifiedType(type)
            args.add(*targetAssign->getTranslatedExpr(), 0);
            buildFunctionCall(ctx, getResultStringFAtom, args);
            return;
        }
    case type_qstring:  func = getResultStringAtom; break;      
    case type_varstring:func = getResultVarStringAtom; break;
    case type_unicode:  func = getResultUnicodeAtom; break;
    case type_varunicode:func = getResultVarUnicodeAtom; break;
    case type_utf8:     func = getResultUnicodeAtom; break;
    case type_real:     func = getResultRealAtom; break;
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
            callProcedure(ctx, getResultDecimalAtom, args);
            if (boundTarget)
                boundTarget->setFromTarget(*getTarget);
            return;
        }
    case type_row:      UNIMPLEMENTED; break; //should be translated to rawData.
    default:
        PrintLog("%d", ttc);
        assertex(!"No getResult defined for this type");
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

    r2xctx.addQuotedCompound("virtual void toXML(bool isAll, size32_t length, const byte * self, IXmlWriter & out)");
    OwnedHqlExpr itemName = createConstant("Item");
    OwnedHqlExpr value = boundValue.getTranslatedExpr();
    buildXmlSerializeSetValues(r2xctx, value, itemName, true);

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

void HqlCppTranslator::buildSetResultInfo(BuildCtx & ctx, IHqlExpression * originalExpr, IHqlExpression * value, ITypeInfo * type, bool isPersist, bool associateResult)
{
    IHqlExpression * seq = queryPropertyChild(originalExpr, sequenceAtom, 0);
    IHqlExpression * name = queryPropertyChild(originalExpr, namedAtom, 0);

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
    _ATOM func;
    CHqlBoundExpr valueToSave;
    LinkedHqlExpr castValue = value;
    switch(retType)
    {
    case type_int:      
    case type_swapint:      
        {
            bool isSigned = schemaType->isSigned();
            func = isSigned ? setResultIntAtom : setResultUIntAtom;
            schemaType.setown(makeIntType(8, isSigned)); 
            break;
        }
    case type_boolean:  func = setResultBoolAtom; break;
    case type_string:   func = setResultStringAtom; schemaType.setown(makeStringType(UNKNOWN_LENGTH, NULL, NULL)); break;
    case type_unicode:  func = setResultUnicodeAtom; schemaType.setown(makeUnicodeType(UNKNOWN_LENGTH, 0)); break;
    case type_utf8:     func = setResultUnicodeAtom; schemaType.setown(makeUnicodeType(UNKNOWN_LENGTH, 0)); castValue.setown(ensureExprType(value, schemaType)); associateResult = false; break;
    case type_qstring:  func = setResultStringAtom; schemaType.setown(makeStringType(UNKNOWN_LENGTH, NULL, NULL)); break;
    case type_data:     func = setResultDataAtom; schemaType.setown(makeDataType(UNKNOWN_LENGTH)); break;
    case type_varstring:func = setResultVarStringAtom; schemaType.setown(makeStringType(UNKNOWN_LENGTH, NULL, NULL)); break;
    case type_varunicode:func = setResultVarUnicodeAtom; schemaType.setown(makeUnicodeType(UNKNOWN_LENGTH, 0)); break;
    case type_real:     func = setResultRealAtom; schemaType.setown(makeRealType(8)); break;
    case type_decimal:  func = setResultDecimalAtom; break;
    case type_row:
        {
            CHqlBoundExpr boundLength;
            OwnedHqlExpr serialized = ::ensureSerialized(value);
            func = setResultRawAtom; 
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
            func = setResultSetAtom;
            ITypeInfo * elementType = LINK(schemaType->queryChildType());
            if (!elementType)
                elementType = makeStringType(UNKNOWN_LENGTH, NULL, NULL);
            schemaType.setown(makeSetType(elementType));
        }
        break;
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
            cseValue.setown(spotScalarCSE(cseValue));

        if ((retType == type_set) && isComplexSet(resultType, false) && castValue->getOperator() == no_list && !isNullList(castValue))
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
        if (func == setResultSetAtom)
        {
            StringBuffer helper, s;
            buildSetXmlSerializer(helper, resultType);
            s.clear().append("&").append(helper);
            args.append(*createQuoted(s, makeBoolType()));
        }

        buildFunctionCall(ctx, func, args);
    }

    if(wu())
    {
        if (retType == type_row)
        {
            Owned<IWUResult> result = createDatasetResultSchema(seq, name, ::queryRecord(schemaType), false, false);
            if (result)
                result->setResultTotalRowCount(1);
        }
        else
        {
            // Bit of a mess - should split into two procedures
            int sequence = (int) seq->queryValue()->getIntValue();
            Owned<IWUResult> result = createWorkunitResult(sequence, name);
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

                MemoryBuffer schema;
                schema.append(fieldName.str());
                schemaType->serialize(schema);
                schema.append("").append((unsigned char) type_void);
                schema.append((unsigned)0);
                result->setResultSchemaRaw(schema.length(), schema.toByteArray());

                StringBuffer xml;
                {
                    XmlSchemaBuilder xmlbuilder(false);
                    xmlbuilder.addField(fieldName, *schemaType);
                    xmlbuilder.getXml(xml);
                }
                addSchemaResource(sequence, resultName.str(), xml.length()+1, xml.str());
            }
        }
    }
}

void HqlCppTranslator::buildCompareClass(BuildCtx & ctx, const char * name, IHqlExpression * orderExpr, IHqlExpression * datasetLeft, IHqlExpression * datasetRight, IHqlExpression * selSeq)
{
    BuildCtx classctx(ctx);
    beginNestedClass(classctx, name, "ICompare");

    BuildCtx funcctx(classctx);
    funcctx.addQuotedCompound("virtual int docompare(const void * _left, const void * _right) const");
    funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
    funcctx.addQuoted("const unsigned char * right = (const unsigned char *) _right;");
    funcctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);

    bindTableCursor(funcctx, datasetLeft, "left", no_left, selSeq);
    bindTableCursor(funcctx, datasetRight, "right", no_right, selSeq);
    if (orderExpr->getOperator() == no_order)
        doBuildReturnCompare(funcctx, orderExpr, no_order, false);
    else
        buildReturn(funcctx, orderExpr);

    endNestedClass();
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
    beginNestedClass(classctx, name, "ICompareEq");

    BuildCtx funcctx(classctx);
    funcctx.addQuotedCompound("virtual bool match(const void * _left, const void * _right) const");
    funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
    funcctx.addQuoted("const unsigned char * right = (const unsigned char *) _right;");
    funcctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);

    bindTableCursor(funcctx, datasetLeft, "left", no_left, selSeq);
    bindTableCursor(funcctx, datasetRight, "right", no_right, selSeq);
    if (orderExpr->getOperator() == no_order)
        doBuildReturnCompare(funcctx, orderExpr, no_eq, true);
    else
        buildReturn(funcctx, orderExpr);

    endNestedClass();
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
    beginNestedClass(classctx, name, "INaryCompareEq");

    BuildCtx funcctx(classctx);
    funcctx.addQuotedCompound("virtual bool match(unsigned numRows, const void * * _rows) const");
    funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _rows[0];");
    funcctx.addQuoted("unsigned char * * rows = (unsigned char * *) _rows;");
    funcctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);

    bindTableCursor(funcctx, dataset, "left", no_left, selSeq);
    bindRows(funcctx, no_left, selSeq, rowsid, dataset, "numRows", "rows", false);

    buildReturn(funcctx, expr);

    endNestedClass();
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
    beginNestedClass(classctx, name, "IHash");

    BuildCtx funcctx(classctx);
    s.clear().append("virtual unsigned hash(const void * _self)");
    funcctx.addQuotedCompound(s);
    s.clear().append("const unsigned char * self = (const unsigned char *) _self;");
    funcctx.addQuoted(s);

    bindTableCursor(funcctx, dataset.queryDataset(), "self", dataset.querySide(), dataset.querySelSeq());
    buildReturn(funcctx, orderExpr);

    endNestedClass();
}


void HqlCppTranslator::buildCompareClass(BuildCtx & ctx, const char * name, IHqlExpression * sortList, const DatasetReference & dataset)
{
    BuildCtx comparectx(ctx);
    beginNestedClass(comparectx, name, "ICompare");

    BuildCtx funcctx(comparectx);
    funcctx.addQuotedCompound("virtual int docompare(const void * _left, const void * _right) const");
    funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
    funcctx.addQuoted("const unsigned char * right = (const unsigned char *) _right;");
    funcctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);

    buildReturnOrder(funcctx, sortList, dataset);

    endNestedClass();
}


void HqlCppTranslator::buildHashOfExprsClass(BuildCtx & ctx, const char * name, IHqlExpression * cond, const DatasetReference & dataset, bool compareToSelf)
{
    IHqlExpression * attr = compareToSelf ? createAttribute(internalAtom) : NULL;
    OwnedHqlExpr hash = createValue(no_hash32, LINK(unsignedType), LINK(cond), attr);

    buildHashClass(ctx, name, hash, dataset);
}

//---------------------------------------------------------------------------

IHqlExpression * queryImplementationInterface(IHqlExpression * moduleFunc)
{
    IHqlExpression * module = moduleFunc->queryChild(0);
    IHqlExpression * library = module->queryProperty(libraryAtom);
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

bool HqlCppTranslator::prepareToGenerate(IHqlExpression * exprlist, WorkflowArray & actions, bool isEmbeddedLibrary)
{
    bool createLibrary = isLibraryScope(exprlist);

    OwnedHqlExpr query = LINK(exprlist);
    if (createLibrary)
    {
        if (query->getOperator() != no_funcdef)
            throwError(HQLERR_LibraryMustBeFunctional);

        ::Release(outputLibrary);
        outputLibrary = NULL;
        outputLibraryId.setown(createAttribute(graphAtom, getSizetConstant(nextActivityId())));
        outputLibrary = new HqlCppLibraryImplementation(*this, queryImplementationInterface(query), outputLibraryId, targetClusterType);

        if (!isEmbeddedLibrary)
        {
            SCMStringBuffer libraryName;
            wu()->getJobName(libraryName);
            wu()->setLibraryInformation(libraryName.str(), outputLibrary->getInterfaceHash(), getLibraryCRC(query));
        }
    }
    else
    {
        if (options.applyInstantEclTransformations)
            query.setown(doInstantEclTransformations(query, options.applyInstantEclTransformationsLimit));
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
    case ThorCluster:
         clusterTypeText = "thor";
         break;
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



bool HqlCppTranslator::buildCode(IHqlExpression * exprlist, const char * embeddedLibraryName, bool isEmbeddedLibrary)
{
    unsigned time = msTick();
    WorkflowArray workflow;
    bool ok = prepareToGenerate(exprlist, workflow, isEmbeddedLibrary);
    if (ok)
    {
        //This is done late so that pickBestEngine has decided which engine we are definitely targeting.
        if (!isEmbeddedLibrary)
            updateClusterType();

        if (options.addTimingToWorkunit)
            wu()->setTimerInfo("EclServer: tree transform", NULL, msTick()-time, 1, 0);

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
                buildLibraryGraph(ctx, graph, embeddedLibraryName);
            }
            else
                buildWorkflow(workflow);
        }
        else
            buildWorkflow(workflow);

        if (options.calculateComplexity)
        {
            unsigned time = msTick();
            StringBuffer complexityText;
            complexityText.append(getComplexity(workflow));
            wu()->setDebugValue("__Calculated__Complexity__", complexityText, true);
            if (options.addTimingToWorkunit)
                wu()->setTimerInfo("EclServer: calculate complexity", NULL, msTick()-time, 1, 0);
        }
    }

    ::Release(outputLibrary);
    outputLibrary = NULL;
    outputLibraryId.clear();
    return ok;
}

bool HqlCppTranslator::buildCpp(IHqlCppInstance & _code, IHqlExpression * exprlist)
{
    if (!internalScope)
        return false;

    try
    {
        unsigned time = msTick();

        wu()->setCodeVersion(ACTIVITY_INTERFACE_VERSION,BUILD_TAG,LANGUAGE_VERSION);
        StringAttrAdaptor adaptor(defaultCluster);
        wu()->getClusterName(adaptor);
        curCluster.set(defaultCluster);
        cacheOptions();

        useLibrary(ECLRTL_LIB);
        useInclude("eclrtl.hpp");

        HqlExprArray internalLibraries;
        OwnedHqlExpr query = separateLibraries(exprlist, internalLibraries);

        //General internal libraries first, in dependency order
        ForEachItemIn(i, internalLibraries)
        {
            IHqlExpression & cur = internalLibraries.item(i);
            assertex(cur.getOperator() == no_funcdef);
            IHqlExpression * moduleExpr = cur.queryChild(0);
            IHqlExpression * definition = queryPropertyChild(moduleExpr, internalAtom, 0);
            IHqlExpression * name = queryPropertyChild(moduleExpr, nameAtom, 0);

            StringBuffer internalLibraryName;
            name->queryValue()->getStringValue(internalLibraryName);
            overrideOptionsForLibrary();
            if (!buildCode(definition, internalLibraryName.str(), true))
                return false;
        }

        if (isLibraryScope(exprlist))
            overrideOptionsForLibrary();
        else
            overrideOptionsForQuery();
        if (!buildCode(query, NULL, false))
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
            Owned<IWUGraph> wug = wu()->updateGraph(cur.name);
            wug->setXGMMLTree(cur.graph.getClear());
            wug->setType(GraphTypeActivities);
        }

        code->processIncludes();
        if (options.peephole)
        {
            cycle_t time = msTick();
            peepholeOptimize(*code, *this);
            DEBUG_TIMER("EclServer: peephole optimize", msTick()-time);
        }
    }
    catch (IException *)
    {
        ensureWorkUnitUpdated();
        throw;
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

    virtual void report(const char *name, const __int64 totaltime, const __int64 maxtime, const unsigned count)
    {
        wu->setTimerInfo(name, NULL, (unsigned)totaltime, count, (unsigned)maxtime);
    }

protected:
    IWorkUnit * wu;
};


void HqlCppTranslator::ensureWorkUnitUpdated()
{
    if (timeReporter && options.addTimingToWorkunit)
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

    if (!prepareToGenerate(exprlist, workflow, false))
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
            IHqlExpression * field = expr->queryChild(1);
            Owned<IReferenceSelector> selector;
            if (expr->hasProperty(newAtom))
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
    WarningProcessor::OnWarningStateBlock saved(warningProcessor);

    //Process any annotations first - but still pass the original expr to the doBuildActivtyXXX functions.
    IHqlExpression * cur = expr;
    loop
    {
        IHqlExpression * body = cur->queryBody(true);
        if (cur == body)
            break;

        switch (cur->getAnnotationKind())
        {
        case annotate_meta:
            warningProcessor.processMetaAnnotation(cur);
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
                warningProcessor.setSymbol(cur);
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
            case no_id2blob:
            case no_typetransfer:
            case no_rows:
            case no_xmlproject:
            case no_libraryinput:
            case no_activetable:
            case no_translated:
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
                    if ((getNumActivityArguments(expr) == 0) && canProcessInline(&ctx, row))
                        result = doBuildActivityCreateRow(ctx, row, false);
                    else
                        result = buildCachedActivity(ctx, row);
                    break;
                }
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
                    if (isCurrentActiveGraph(ctx, expr->queryChild(1)))
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
            case no_iterate:
                result = doBuildActivityIterate(ctx, expr);
                break;
            case no_process:
                result = doBuildActivityProcess(ctx, expr);
                break;
            case no_group:
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
            case no_sorted:
            case no_distributed:
            case no_preservemeta:
            case no_grouped:
            case no_nofold:
            case no_nohoist:
            case no_globalscope:
            case no_thisnode:
            case no_forcegraph:
            case no_keyed:
                result = buildCachedActivity(ctx, expr->queryChild(0));
                break;
            case no_dataset_alias:
                if (!expr->hasProperty(_normalized_Atom))
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
                result = doBuildActivityMetaActivity(ctx, expr);
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
                result = doBuildActivitySequentialParallel(ctx, expr, isRoot);
                break;
            case no_activerow:
                {
                    OwnedHqlExpr row = createDatasetFromRow(LINK(expr));
                    return buildCachedActivity(ctx, row);
                }
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
            case no_thor:
                UNIMPLEMENTED;
                break;
            default:
                if (expr->isAction())
                    return doBuildActivityAction(ctx, expr, isRoot);
                if (expr->isDatarow())
                {
                    OwnedHqlExpr row = createDatasetFromRow(LINK(expr));
                    return buildCachedActivity(ctx, row);
                }
                else
                {
                    UNIMPLEMENTED_XY("Activity", getOpString(op));
                }
        }
    }
    catch (IException * e)
    {
        if (dynamic_cast<IECLError *>(e))
            throw;
        IHqlExpression * location = queryActiveActivityLocation();
        if (location)
        {
            IECLError * error = annotateExceptionWithLocation(e, location);
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
        OwnedHqlExpr optimized = insideChildGraph(ctx) ? LINK(expr) : optimizeActivityAliasReferences(expr);
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
    switch (expr->getOperator())
    {
    case no_compound:
    case no_parallel:
    case no_actionlist:
        {
            ForEachChild(idx, expr)
                buildRootActivity(ctx, expr->queryChild(idx));
            break;
        }
    case no_null:
        if (expr->isAction())
            return;
        //fall through
    default:
        {
            WarningProcessor::OnWarningStateBlock saved(warningProcessor);
            ::Release(buildCachedActivity(ctx, expr, true));
            break;
        }
    }
}


void HqlCppTranslator::buildRecordSerializeExtract(BuildCtx & ctx, IHqlExpression * memoryRecord)
{
    OwnedHqlExpr serializedRecord = getSerializedForm(memoryRecord);
    OwnedHqlExpr serializedDataset = createDataset(no_null, LINK(serializedRecord));
    OwnedHqlExpr memoryDataset = createDataset(no_anon, LINK(memoryRecord));

    MetaInstance meta(*this, memoryDataset);
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

        ctx.addQuoted("byte * self = crSelf.ensureCapacity(size, NULL);");
        ctx.addQuoted("memcpy(crSelf.row(), _left, size);");
        ctx.addQuoted("return size;");
    }
    else
    {
        ctx.addQuoted("const byte * left = (const byte *)_left;");
        BoundRow * self = bindSelf(ctx, serializedDataset, "crSelf");
        BoundRow * left = bindTableCursor(ctx, memoryDataset, "left");
        OwnedHqlExpr rhs = ensureActiveRow(left->querySelector());

        OwnedHqlExpr serializedRow = ::ensureSerialized(rhs);

        buildAssign(ctx, self->querySelector(), serializedRow);
        buildReturnRecordSize(ctx, self);
    }
}

//---------------------------------------------------------------------------

BoundRow * HqlCppTranslator::bindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * bound, node_operator side, IHqlExpression * selSeq)
{
    BoundRow * cursor = createTableCursor(dataset, bound, side, selSeq);
    ctx.associateOwn(*cursor);
    return cursor;
}

BoundRow * HqlCppTranslator::bindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, const char * name, bool isLinkCounted, node_operator side, IHqlExpression * selSeq)
{
    Owned<ITypeInfo> type = makeRowReferenceType(NULL);
    if (isLinkCounted)
        type.setown(makeAttributeModifier(type.getClear(), getLinkCountedAttr()));

    Owned<IHqlExpression> bound = createVariable(name, type.getClear());
//  Owned<IHqlExpression> bound = createVariable(name, makeRowReferenceType(dataset));
    return bindTableCursor(ctx, dataset, bound, side, selSeq);
}

BoundRow * HqlCppTranslator::rebindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, BoundRow * row, node_operator side, IHqlExpression * selSeq)
{
    BoundRow * cursor = recreateTableCursor(dataset, row, side, selSeq);
    ctx.associateOwn(*cursor);
    return cursor;
}


BoundRow * HqlCppTranslator::createTableCursor(IHqlExpression * dataset, IHqlExpression * bound, node_operator side, IHqlExpression * selSeq)
{
    return new BoundRow(dataset, bound, queryRecordOffsetMap(dataset->queryRecord()), side, selSeq);
}

BoundRow * HqlCppTranslator::recreateTableCursor(IHqlExpression * dataset, BoundRow * row, node_operator side, IHqlExpression * selSeq)
{
    return new BoundRow(row, dataset, side, selSeq);
}

BoundRow * HqlCppTranslator::createTableCursor(IHqlExpression * dataset, const char * name, bool isLinkCounted, node_operator side, IHqlExpression * selSeq)
{
    Owned<ITypeInfo> type = makeRowReferenceType(NULL);
    if (isLinkCounted)
        type.setown(makeAttributeModifier(type.getClear(), getLinkCountedAttr()));
    Owned<IHqlExpression> bound = createVariable(name, type.getClear());
    return createTableCursor(dataset, bound, side, selSeq);
}

BoundRow * HqlCppTranslator::bindXmlTableCursor(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * bound, node_operator side, IHqlExpression * selSeq, bool translateVirtuals)
{
    Owned<ColumnToOffsetMap> xmlMap = new XmlColumnToOffsetMap(dataset->queryRecord(), getDefaultMaxRecordSize(), translateVirtuals);
    xmlMap->init(recordMap);
    BoundRow * cursor = new BoundRow(dataset, bound, xmlMap, side, selSeq);
    ctx.associateOwn(*cursor);
    return cursor;
}

BoundRow * HqlCppTranslator::bindXmlTableCursor(BuildCtx & ctx, IHqlExpression * dataset, const char * name, node_operator side, IHqlExpression * selSeq, bool translateVirtuals)
{
    OwnedHqlExpr bound = createVariable(name, makeRowReferenceType(NULL));
//  OwnedHqlExpr bound = createVariable(name, makeRowReferenceType(dataset));
    return bindXmlTableCursor(ctx, dataset, bound, side, selSeq, translateVirtuals);
}

BoundRow * HqlCppTranslator::bindCsvTableCursor(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * bound, node_operator side, IHqlExpression * selSeq, bool translateVirtuals, _ATOM encoding)
{
    Owned<ColumnToOffsetMap> csvMap = new CsvColumnToOffsetMap(dataset->queryRecord(), getDefaultMaxRecordSize(), translateVirtuals, encoding);
    csvMap->init(recordMap);
    BoundRow * cursor = new BoundRow(dataset, bound, csvMap, side, selSeq);
    ctx.associateOwn(*cursor);
    return cursor;
}

BoundRow * HqlCppTranslator::bindCsvTableCursor(BuildCtx & ctx, IHqlExpression * dataset, const char * name, node_operator side, IHqlExpression * selSeq, bool translateVirtuals, _ATOM encoding)
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
    return new BoundRow(dataset->queryBody(), bound, queryRecordOffsetMap(dataset->queryRecord()));
}

BoundRow * HqlCppTranslator::bindSelectorAsSelf(BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * expr)
{
    BoundRow * rootRow = selector->queryRootRow();
    if (!rootRow->queryBuilder())
    {
        if (options.alwaysCreateRowBuilder || (options.supportDynamicRows && !isFixedWidthDataset(rootRow->queryRecord())))
            UNIMPLEMENTED_X("expected a row builder");
    }
    if (selector->isRoot())
    {
        if (rootRow->querySide() == no_self)
        {
            ctx.associate(*rootRow);
            return rootRow;
        }
        return bindSelf(ctx, expr, rootRow->queryBound(), rootRow->queryBuilder());
    }

    //Need to bind a delta address to a new variable.
//  throwUnexpected();  // check this is actually called
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
    ctx.removeAssociation(self);
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

void HqlCppTranslator::addDependency(BuildCtx & ctx, ABoundActivity * element, ABoundActivity * dependent, _ATOM kind, const char * label, int whenId)
{
    ABoundActivity * sourceActivity = element;
    ABoundActivity * sinkActivity = dependent;
    unsigned outputIndex = 0;
    if (kind != childAtom)
        outputIndex = sourceActivity->nextOutputCount();

    StringBuffer idText;
    idText.append(sourceActivity->queryActivityId()).append('_').append(sinkActivity->queryActivityId());

#if 0
    StringBuffer edgeText;
    edgeText.append("edge[@id=\").append(idText).append("\"]");
    if (graph->hasProp(edgePath))
        return;
#endif

//  if (outputIndex)
//      idText.append("_").append(outputIndex);

    IPropertyTree *edge = createPTree();
    edge->setProp("@id", idText.str());
    edge->setPropInt64("@target", sinkActivity->queryGraphId());
    edge->setPropInt64("@source", sourceActivity->queryGraphId());
    if (targetHThor())
    {
        if (sinkActivity->queryGraphId() == sourceActivity->queryGraphId())
            throwError1(HQLERR_DependencyWithinGraph, sinkActivity->queryGraphId());
    }
    if (label)
        edge->setProp("@label", label);
    if (targetRoxie())
    {
        if (outputIndex)
            addGraphAttributeInt(edge, "_sourceIndex", outputIndex);
    }

    if (kind == dependencyAtom)
        addGraphAttributeBool(edge, "_dependsOn", true);
    else if (kind == childAtom)
        addGraphAttributeBool(edge, "_childGraph", true);

    if (whenId)
        addGraphAttributeInt(edge, "_when", whenId);


    addGraphAttributeInt(edge, "_sourceActivity", sourceActivity->queryActivityId());
    addGraphAttributeInt(edge, "_targetActivity", sinkActivity->queryActivityId());
    graph->addPropTree("edge", edge);
}

void HqlCppTranslator::buildClearRecord(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * record, int direction)
{
    Owned<IReferenceSelector> selector = buildActiveRow(ctx, dataset);
    selector->buildClear(ctx, direction);
}


IHqlExpression * HqlCppTranslator::getClearRecordFunction(IHqlExpression * record, int direction)
{
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
    if (record)
    {
        IHqlStmt * func = clearctx.addQuotedCompound(s);
        func->setIncomplete(true);

        OwnedHqlExpr dataset = createDataset(no_anon, LINK(record));
        BoundRow * cursor = bindSelf(clearctx, dataset, "crSelf");
        ensureRowAllocated(clearctx, "crSelf");
        buildClearRecord(clearctx, cursor->querySelector(), record, direction);
        buildReturnRecordSize(clearctx, cursor);
        func->setIncomplete(false);
    }
    else
        clearctx.addQuotedCompound(s.append(" {}"));

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

void HqlCppTranslator::doBuildSerialize(BuildCtx & ctx, _ATOM name, IHqlExpression * length, CHqlBoundExpr & bound, const char * bufferName)
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

void HqlCppTranslator::ensureSerialized(const CHqlBoundTarget & variable, BuildCtx & serializectx, BuildCtx & deserializectx, const char * inBufferName, const char * outBufferName)
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

        _ATOM serializeName, deserializeName;
        OwnedITypeInfo serializedType;
        type_t tc = type->getTypeCode();
        switch (tc)
        {
        case type_varstring:
            serializeName = serializeCStringXAtom;
            deserializeName = deserializeCStringXAtom;
            break;
        case type_string:
            serializeName = serializeStringXAtom;
            deserializeName = deserializeStringXAtom;
            break;
        case type_data:
            serializeName = serializeDataXAtom;
            deserializeName = deserializeDataXAtom;
            break;
        case type_set:
            serializeName = serializeSetAtom;
            deserializeName = deserializeSetAtom;
            break;
        case type_qstring:
            serializeName = serializeQStrXAtom;
            deserializeName = deserializeQStrXAtom;
            break;
        case type_unicode:
            serializeName = serializeUnicodeXAtom;
            deserializeName = deserializeUnicodeXAtom;
            break;
        case type_varunicode:
            serializeName = serializeUnicodeXAtom;
            deserializeName = deserializeVUnicodeXAtom;
            break;
        case type_utf8:
            serializeName = serializeUtf8XAtom;
            deserializeName = deserializeUtf8XAtom;
            break;
        case type_table:
        case type_groupedtable:
            {
                IHqlExpression * record = ::queryRecord(type);
                if (hasLinkCountedModifier(type))
                {
                    deserializeArgs.append(*createRowSerializer(deserializectx, record, deserializerAtom));

                    serializeArgs.append(*createRowSerializer(serializectx, record, serializerAtom));
                    if (tc == type_table)
                    {
                        serializeName = serializeRowsetXAtom;
                        deserializeName = deserializeRowsetXAtom;
                    }
                    else
                    {
                        serializeName = serializeGroupedRowsetXAtom;
                        deserializeName = deserializeGroupedRowsetXAtom;
                    }
                }
                else
                {
                    assertex(!recordRequiresSerialization(record));
                    if (tc == type_table)
                    {
                        serializeName = serializeDatasetXAtom;
                        deserializeName = deserializeDatasetXAtom;
                    }
                    else
                    {
                        serializeName = serializeGroupedDatasetXAtom;
                        deserializeName = deserializeGroupedDatasetXAtom;
                    }
                }
                serializedType.set(type);
                break;
            }
        case type_row:
            {
                IHqlExpression * record = ::queryRecord(type);
                assertex(hasWrapperModifier(type));

                serializeArgs.append(*createRowSerializer(serializectx, record, serializerAtom));
                serializeArgs.append(*createVariable(outBufferName, makeBoolType()));
                buildFunctionCall(serializectx, serializeRowAtom, serializeArgs);

                
                deserializeArgs.append(*createRowAllocator(deserializectx, record));
                deserializeArgs.append(*createRowSerializer(deserializectx, record, deserializerAtom));
                deserializeArgs.append(*createVariable(inBufferName, makeBoolType()));
                Owned<ITypeInfo> resultType = makeReferenceModifier(makeAttributeModifier(makeRowType(record->getType()), getLinkCountedAttr()));
                OwnedHqlExpr call = bindFunctionCall(deserializeRowAtom, deserializeArgs, resultType);
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
                    ColumnToOffsetMap * map = queryRecordOffsetMap(record);
                    length.setown(getSizetConstant(map->getMaxSize()));
                }
                break;
            }
        case type_bitfield:
            UNIMPLEMENTED;
        }
        doBuildSerialize(serializectx, serializeRawAtom, length, value, outBufferName);
        doBuildSerialize(deserializectx, deserializeRawAtom, length, value, inBufferName);
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
    IHqlExpression * seq = queryPropertyChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryPropertyChild(expr, namedAtom, 0);
    if (!name)
        name = queryPropertyChild(expr, nameAtom, 0);
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
            doBuildAliasValue(ctx, expr, tgt);
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
    ExpressionFormat format = (hasLinkCountedModifier(exprType) || options.tempDatasetsUseLinkedRows) ? FormatLinkedDataset : FormatBlockedDataset;
    CHqlBoundTarget tempTarget;
    createTempFor(ctx, exprType, tempTarget, typemod_none, format);
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


void HqlCppTranslator::pushCluster(BuildCtx & ctx, IHqlExpression * cluster, StringAttr & savedCluster)
{
    savedCluster.set(curCluster);

    HqlExprArray args;
    args.append(*LINK(cluster));
    callProcedure(ctx, selectClusterAtom, args);

    StringBuffer clusterText;
    cluster->queryValue()->getStringValue(clusterText);
    ctxCallback->noteCluster(clusterText.str());
    curCluster.set(clusterText.str());
}


void HqlCppTranslator::popCluster(BuildCtx & ctx, const char * savedCluster)
{
    HqlExprArray args;
    callProcedure(ctx, restoreClusterAtom, args);
    curCluster.set(savedCluster);
}


void HqlCppTranslator::doBuildStmtSetResult(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * seq = queryPropertyChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryPropertyChild(expr, namedAtom, 0);
    IHqlExpression * persist = expr->queryProperty(_workflowPersist_Atom);
    IHqlExpression * cluster = expr->queryProperty(clusterAtom);

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

    StringAttr prevClusterName;
    if (cluster)
        pushCluster(subctx, cluster->queryChild(0), prevClusterName);

    switch (value->queryType()->getTypeCode())
    {
    case type_void:
        {
            buildStmt(subctx, value);
            
            IHqlExpression * result = queryBoolExpr(true);
            if (expr->queryProperty(checkpointAtom))
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
                    callProcedure(atendctx, deleteFileAtom, args);
                }
            }

            if (!expr->hasProperty(noSetAtom))
                buildSetResultInfo(subctx, expr, result, NULL, (persist != NULL), false);
        }
        break;
    case type_set:
        {
            ITypeInfo * setType = NULL;
            IHqlExpression  * original = queryPropertyChild(expr, _original_Atom, 0);
            if (original)
                setType = original->queryType();

            OwnedHqlExpr normalized = normalizeListCasts(value);
            buildSetResultInfo(subctx, expr, normalized, setType, (persist != NULL), true);
            break;
        }
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
                buildFunctionCall(subctx, setResultSetAtom, args);
                Owned<IWUResult> result = createDatasetResultSchema(seq, name, value->queryRecord(), true, false);
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
        popCluster(subctx, prevClusterName);

    if (matchesConstantValue(seq, ResultSequenceStored) || matchesConstantValue(seq, ResultSequencePersist))
        graphLabel.clear();
}

static bool isFilePersist(IHqlExpression * expr)
{
    loop
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
            return (queryRealChild(expr, 1) != NULL);
        case no_actionlist:
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

        OwnedHqlExpr function = bindFunctionCall(getDatasetHashAtom, args);
        buildAssignToTemp(ctx, crcExpr, function);
    }

    ForEachItemIn(idx2, dependencies.resultsRead)
    {
        IHqlExpression & cur = dependencies.resultsRead.item(idx2);
        IHqlExpression * seq = cur.queryChild(0);
        IHqlExpression * name = cur.queryChild(1);

        //Not sure if we need to do this if the result is internal.  Leave on for the moment.
        //if (seq->queryValue()->getIntValue() != ResultSequenceInternal)
        bool expandLogical = matchesConstantValue(seq, ResultSequencePersist) && !cur.hasProperty(_internal_Atom);
        HqlExprArray args;
        args.append(*createResultName(name, expandLogical));
        args.append(*LINK(seq));
        OwnedHqlExpr call = bindFunctionCall(getResultHashAtom, args);
        OwnedHqlExpr value = createValue(no_bxor, crcExpr->getType(), LINK(crcExpr), ensureExprType(call, crcExpr->queryType()));
        buildAssignToTemp(ctx, crcExpr, value);
    }
    return crcExpr.getClear();
}

void HqlCppTranslator::doBuildStmtEnsureResult(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * value = expr->queryChild(0);
    IHqlExpression * seq = queryPropertyChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryPropertyChild(expr, namedAtom, 0);
    
    OwnedHqlExpr resultName = ::createResultName(name);
    resultName.setown(ensureExprType(resultName, unknownVarStringType));

    HqlExprArray args;
    args.append(*LINK(resultName));
    args.append(*LINK(seq));
    OwnedHqlExpr checkExists = createValue(no_not, makeBoolType(), bindFunctionCall(isResultAtom, args));
    if ((value->getOperator() == no_thor) && (value->queryChild(0)->getOperator() == no_output))
    {
        IHqlExpression * filename = queryRealChild(value->queryChild(0), 1);
        if (filename)
        {
            args.append(*LINK(filename));
            IHqlExpression * fileExists = createValue(no_not, makeBoolType(), bindFunctionCall(fileExistsAtom, args));
            checkExists.setown(createBoolExpr(no_or, checkExists.getClear(), fileExists));
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
    doBuildAliasValue(ctx, value, result);
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
    IHqlExpression * limitExpr = expr->queryChild(1);
    ITypeInfo * type = child->queryType();
    if (expr->hasProperty(maxAtom))
    {
        if (type)
        {
            unsigned size = UNKNOWN_LENGTH;
            switch (type->getTypeCode())
            {
            case type_table:
            case type_groupedtable:
            case type_record:
            case type_row:
                {
                    OwnedHqlExpr record = getSerializedForm(child->queryRecord());
                    ColumnToOffsetMap * map = queryRecordOffsetMap(record);
                    if (map->isFixedWidth())
                        size = map->getFixedRecordSize();
                    else
                        size = map->getMaxSize();
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

    if (expr->hasProperty(minAtom))
    {
        if (type)
        {
            unsigned size = UNKNOWN_LENGTH;
            switch (type->getTypeCode())
            {
            case type_table:
            case type_groupedtable:
            case type_record:
            case type_row:
                {
                    OwnedHqlExpr record = getSerializedForm(child->queryRecord());
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
            case type_table:
            case type_groupedtable:
            case type_record:
            case type_row:
                {
                    e->Release();
                    OwnedHqlExpr record = getSerializedForm(child->queryRecord());
                    ColumnToOffsetMap * map = queryRecordOffsetMap(record);
                    if (map->isFixedWidth())
                    {
                        tgt.expr.setown(getSizetConstant(map->getFixedRecordSize()));
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

void HqlCppTranslator::doBuildExprRowDiff(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, IHqlExpression * rightRecord, IHqlExpression * leftSelector, IHqlExpression * rightSelector, StringBuffer & selectorText, bool isCount)
{
    switch (expr->getOperator())
    {
    case no_field:
        {
            _ATOM name = expr->queryName();
            IHqlSimpleScope * rightScope = rightRecord->querySimpleScope();
            OwnedHqlExpr match = rightScope ? rightScope->lookupSymbol(name) : NULL;
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
            case type_table:
            case type_groupedtable:
                UNIMPLEMENTED;
            }

            StringBuffer fullName;
            fullName.append(selectorText).append(name);

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
            buildFunctionCall(condctx, concatExtendAtom, args);

            //else if same...
            if (isCount)
            {
                condctx.selectElse(cond);
                args.append(*LINK(special));
                args.append(*createConstant(",0"));
                buildFunctionCall(condctx, concatExtendAtom, args);
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
    doBuildExprRowDiff(ctx, tempTarget, leftRecord, left, rightRecord, right, selectorText, expr->hasProperty(countAtom));

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

static void unwindAddFiles(HqlExprArray & args, IHqlExpression * expr, bool isOrdered, bool isOrderedPull)
{
    if ((expr->getOperator() == no_addfiles) && (expr->hasProperty(_ordered_Atom) == isOrdered) && (expr->hasProperty(_orderedPull_Atom) == isOrderedPull))
    {
        unwindAddFiles(args, expr->queryChild(0), isOrdered, isOrderedPull);
        unwindAddFiles(args, expr->queryChild(1), isOrdered, isOrderedPull);
    }
    else
        args.append(*LINK(expr));
}

ABoundActivity * HqlCppTranslator::doBuildActivityConcat(BuildCtx & ctx, IHqlExpression * expr)
{
    HqlExprArray inExprs;
    bool ordered = expr->hasProperty(_ordered_Atom);
    bool orderedPull = expr->hasProperty(_orderedPull_Atom);
    unwindAddFiles(inExprs, expr, ordered, orderedPull);

    //If all coming from disk, probably better to pull them in order.
    bool allFromDisk = options.orderDiskFunnel;
    CIArray bound;
    ForEachItemIn(idx, inExprs)
    {
        IHqlExpression * cur = &inExprs.item(idx);
        bound.append(*buildCachedActivity(ctx, cur));

        loop
        {
            node_operator curOp = cur->getOperator();
            if ((curOp != no_nofold) && (curOp != no_section) && (curOp != no_sectioninput) && (curOp != no_preservemeta))
                break;
            cur = cur->queryChild(0);
        }

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

    bool useImplementationClass = options.minimizeActivityClasses && targetRoxie();
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKfunnel, expr, "Funnel");
    if (useImplementationClass)
        instance->setImplementationClass(newFunnelArgAtom);

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
    IHqlExpression * sortAttr = expr->queryProperty(sortedAtom);
    HqlExprArray sorts;
    unwindChildren(sorts, sortAttr);

    if (sorts.ordinality() != 0)
    {
        OwnedHqlExpr sortOrder = createValueSafe(no_sortlist, makeSortListType(NULL), sorts);
        instance->startctx.addQuoted("virtual ICompare * queryCompare() { return &compare; }");

        DatasetReference dsRef(dataset, no_activetable, NULL);
        buildCompareClass(instance->nestedctx, "compare", sortOrder, dsRef);
        if (!instance->isLocal)
            generateSerializeKey(instance->nestedctx, no_none, dsRef, sorts, !instance->isChildActivity(), true, false);
    }
    else
        throwError(HQLERR_InputMergeNotSorted);

    if (expr->hasProperty(dedupAtom))
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
    if ((expr->getOperator() == no_nonempty) && (expr->hasProperty(localAtom) == isLocal))
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
    unwindNonEmpty(inExprs, expr, expr->hasProperty(localAtom));

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
        instance->setImplementationClass(newSplitArgAtom);

    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    //IHqlExpression * numWays = expr->queryChild(1);
    OwnedHqlExpr numWaysCallback = createUnknown(no_callback, LINK(sizetType), countAtom, instance->createOutputCountCallback());
    OwnedHqlExpr numWays = createTranslated(numWaysCallback);
    bool balanced = expr->hasProperty(balancedAtom);
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


//---------------------------------------------------------------------------

bool HqlCppTranslator::isCurrentActiveGraph(BuildCtx & ctx, IHqlExpression * graphTag)
{
    SubGraphInfo * activeSubgraph = queryActiveSubGraph(ctx);
    assertex(activeSubgraph);
    return (graphTag == activeSubgraph->graphTag);
}


ABoundActivity * HqlCppTranslator::doBuildActivityLoop(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * count = queryRealChild(expr, 1);
    IHqlExpression * filter = queryRealChild(expr, 2);
    IHqlExpression * loopCond = queryRealChild(expr, 3);
    IHqlExpression * body = expr->queryChild(4);
    assertex(body->getOperator() == no_loopbody);

    IHqlExpression * counter = queryPropertyChild(expr, _countProject_Atom, 0);
    IHqlExpression * rowsid = expr->queryProperty(_rowsid_Atom);
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
        BuildCtx funcctx(instance->startctx);
        funcctx.addQuotedCompound("virtual bool sendToLoop(unsigned counter, const void * _self)");
        funcctx.addQuoted("unsigned char * self = (unsigned char *) _self;");

        associateCounter(funcctx, counter, "counter");
        bindTableCursor(funcctx, dataset, "self", no_left, selSeq);
        buildReturn(funcctx, filter);
    }

    if (count)
        doBuildUnsignedFunction(instance->startctx, "numIterations", count);

    if (loopCond)
    {
        BuildCtx funcctx(instance->startctx);
        funcctx.addQuotedCompound("virtual bool loopAgain(unsigned counter, unsigned numRows, const void * * _rows)");
        funcctx.addQuoted("unsigned char * * rows = (unsigned char * *) _rows;");

        associateCounter(funcctx, counter, "counter");

        bindRows(funcctx, no_left, selSeq, rowsid, dataset, "numRows", "rows", options.mainRowsAreLinkCounted);
        buildReturn(funcctx, loopCond);
    }

    IHqlExpression * parallel = expr->queryProperty(parallelAtom);
    if (parallel && (targetHThor() || !count || loopCond))
        parallel = NULL;

    if (parallel)
    {
        IHqlExpression * arg0 = parallel->queryChild(0);
        IHqlExpression * arg1 = parallel->queryChild(1);

        LinkedHqlExpr parallelList;
        LinkedHqlExpr numThreads;
        if (arg0)
        {
            if (arg1)
            {
                parallelList.set(arg0);
                numThreads.set(arg1);
            }
            else if (arg0->isList())
                parallelList.set(arg0);
            else
                numThreads.set(arg0);
        }
        if (numThreads)
            doBuildUnsignedFunction(instance->startctx, "defaultParallelIterations", numThreads);

        if (parallelList)
        {
            Owned<ITypeInfo> setType = makeSetType(LINK(unsignedType));
            BuildCtx funcctx(instance->startctx);
            funcctx.addQuotedCompound("virtual void numParallelIterations(size32_t & __lenResult, void * & __result)");
            funcctx.addQuoted("bool __isAllResult;");
            doBuildFunctionReturn(funcctx, setType, parallelList);
        }
    }

    StringBuffer flags;
    if (counter) flags.append("|LFcounter");
    if (parallel) flags.append("|LFparallel");
    if (filter) flags.append("|LFfiltered");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    BuildCtx subctx(instance->startctx);
    subctx.addQuotedCompound("virtual void createParentExtract(rtlRowBuilder & builder)");

    //Now need to generate the body of the loop.
    //output dataset is result 0
    //input dataset is fed in using result 1
    //counter (if required) is fed in using result 2[0].counter;
    unique_id_t loopId = buildLoopSubgraph(subctx, dataset, selSeq, rowsid, body->queryChild(0), counter, instance->activityId, (parallel != NULL));
    instance->addAttributeInt("_loopid", loopId);

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
    IHqlExpression * counter = queryPropertyChild(expr, _countProject_Atom, 0);
    IHqlExpression * rowsid = expr->queryProperty(_rowsid_Atom);
    IHqlExpression * selSeq = querySelSeq(expr);
    IHqlExpression * parallel = expr->queryProperty(parallelAtom);
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

    BuildCtx subctx(instance->startctx);
    subctx.addQuotedCompound("virtual void createParentExtract(rtlRowBuilder & builder)");

    //Now need to generate the body of the loop.
    //output dataset is result 0
    //input dataset is fed in using result 1
    //counter (if required) is fed in using result 2[0].counter;
    unique_id_t loopId = buildGraphLoopSubgraph(subctx, dataset, selSeq, rowsid, body->queryChild(0), counter, instance->activityId, (parallel != NULL));
    instance->addAttributeInt("_loopid", loopId);

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
    IHqlExpression * rowlimit = expr->queryProperty(rowLimitAtom);

    if (rowlimit)
    {
        doBuildUnsigned64Function(instance->startctx, "getRowLimit", rowlimit->queryChild(0));
        IHqlExpression * fail = queryChildOperator(no_fail, rowlimit);
        if (fail)
        {
            BuildCtx ctx(instance->startctx);
            ctx.addQuotedCompound("virtual void onLimitExceeded()");
            buildStmt(ctx, fail);
        }
    }

    BuildCtx subctx(instance->startctx);
    subctx.addQuotedCompound("virtual void createParentExtract(rtlRowBuilder & builder)");

    //output dataset is result 0
    unique_id_t remoteId = buildRemoteSubgraph(subctx, dataset, instance->activityId);
    
    instance->addAttributeInt("_graphid", remoteId);

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
    LinkedHqlExpr locationAttr = expr->queryProperty(_location_Atom);
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

    if (expr->hasProperty(constAtom))
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
        args.append(*createConstant(location.sourcePath->str()));
    else
        args.append(*getNullStringPointer(true));

    args.append(*getSizetConstant(location.lineno));
    args.append(*getSizetConstant(location.column));
    args.append(*createConstant(expr->hasProperty(failAtom)));

    action.setown(bindFunctionCall(addWorkunitAssertFailureAtom, args));

    buildStmt(condctx, action);
}


void HqlCppTranslator::doBuildStmtCluster(BuildCtx & ctx, IHqlExpression * expr)
{
    StringAttr prevClusterName;
    pushCluster(ctx, expr->queryChild(1), prevClusterName);
    buildStmt(ctx, expr->queryChild(0));
    popCluster(ctx, prevClusterName);
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
            builder->generateGraph(ctx);

        builder.clear();
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
            builder.setown(new ChildGraphBuilder(translator));
        builder->buildStmt(ctx, expr);
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
    Owned<ChildGraphBuilder> builder;
};

ABoundActivity * HqlCppTranslator::doBuildActivityApply(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    StringBuffer s;
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * action = expr->queryChild(1);
    IHqlExpression * start = expr->queryProperty(beforeAtom);
    IHqlExpression * end = expr->queryProperty(afterAtom);
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKapply, expr, "Apply");
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    doBuildSequenceFunc(instance->classctx, querySequence(expr), false);

    BuildCtx transformctx(instance->startctx);
    transformctx.addQuotedCompound("virtual void apply(const void * _self)");
    s.clear().append("unsigned char * self = (unsigned char *) _self;");
    transformctx.addQuoted(s);

    bindTableCursor(transformctx, dataset, "self");
    unsigned max = expr->numChildren();
    
    ApplyStmtBuilder builder(*this);
    for (unsigned i=1; i < max; i++)
        builder.buildStmt(transformctx, expr->queryChild(i));
    builder.flush(transformctx);

    if (start) 
    {
        BuildCtx startctx(instance->startctx);
        startctx.addQuotedCompound("virtual void start()");
        builder.buildStmt(startctx, start->queryChild(0));
        builder.flush(startctx);
    }

    if (end)
    {
        BuildCtx endctx(instance->startctx);
        endctx.addQuotedCompound("virtual void end()");
        builder.buildStmt(endctx, end->queryChild(0));
        builder.flush(endctx);
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

bool HqlCppTranslator::insideChildGraph(BuildCtx & ctx)
{
    FilteredAssociationIterator iter(ctx, AssocSubGraph);
    ForEach(iter)
    {
        SubGraphInfo & cur = static_cast<SubGraphInfo &>(iter.get());
        if (cur.type == SubGraphChild)
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
    SubGraphInfo * activeSubgraph = queryActiveSubGraph(ctx);
    IPropertyTree * node = createPTree("node");
    if (activeSubgraph)
        node = activeSubgraph->tree->addPropTree("node", node);
    else
        node = graph->addPropTree("node", node);

    unsigned thisId = reservedId ? reservedId : nextActivityId();
    node->setPropInt("@id", thisId);

    IPropertyTree * graphAttr = node->addPropTree("att", createPTree("att"));
    IPropertyTree * subGraph = graphAttr->addPropTree("graph", createPTree("graph"));

    Owned<SubGraphInfo> graphInfo = new SubGraphInfo(subGraph, thisId, graphTag, kind);
    ctx.associate(*graphInfo);

    IHqlExpression * numResultsAttr = expr->queryProperty(numResultsAtom);
    if (numResultsAttr)
        addGraphAttributeInt(subGraph, "_numResults", getIntValue(numResultsAttr->queryChild(0), 0));
    if (expr->hasProperty(multiInstanceAtom))
        subGraph->setPropBool("@multiInstance", true);
    if (expr->hasProperty(delayedAtom))
        subGraph->setPropBool("@delayed", true);
    if (expr->queryProperty(childAtom))
        subGraph->setPropBool("@child", true);

    if (insideChildGraph(ctx))
    {
        graphAttr->setProp("@name", "_kind");
        graphAttr->setPropInt("@value", TAKsubgraph);

        ActivityInstance * curActivityInstance = queryCurrentActivity(ctx);
        if (curActivityInstance)
            addGraphAttributeInt(node, "_parentActivity", curActivityInstance->activityId);
    }

    OwnedHqlExpr idExpr = createConstant((__int64)thisId);
    ctx.associateExpr(expr, idExpr);

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

    bool needToCreateGraph = !graph;
    if (!graphTag && outputLibraryId)
        graphTag = outputLibraryId;
    if (needToCreateGraph)
        beginGraph(graphctx);

    unsigned thisId = doBuildThorChildSubGraph(graphctx, expr, kind, reservedId, graphTag);

    if (needToCreateGraph)
        endGraph();

    return thisId;
}


void HqlCppTranslator::beginGraph(BuildCtx & ctx, const char * _graphName)
{
    if (activeGraphName)
    {
        //buildStmt(ctx, expr->queryChild(0));
        //return;
        throwError(HQLERR_NestedThorNodes);
    }

    graphSeqNumber++;
    StringBuffer graphName;
    if (!_graphName)
        graphName.append("graph").append(graphSeqNumber);
    else
        graphName.append(_graphName);
    activeGraphName.set(graphName.str());

    graph.setown(createPTree("graph"));
    if (graphLabel)
    {
        graph->setProp("@label", graphLabel);
        graphLabel.clear();
    }
    if (insideLibrary())
        graph->setPropBool("@library", true);
}


void HqlCppTranslator::endGraph()
{
    graphs.append(* new GeneratedGraphInfo(activeGraphName, graph));
    graph.clear();
    activeGraphName.set(NULL);
}

void HqlCppTranslator::clearGraph()
{
    graph.clear();
    activeGraphName.clear();
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
    unsigned time = msTick();

    CompoundSourceTransformer transformer(*this, flags);
    OwnedHqlExpr ret = transformer.process(expr);
    DEBUG_TIMER("EclServer: tree transform: optimize disk read", msTick()-time);
    return ret.getClear();
}

IHqlExpression * HqlCppTranslator::optimizeGraphPostResource(IHqlExpression * expr, unsigned csfFlags)
{
    LinkedHqlExpr resourced = expr;
    // Second attempt to spot compound disk reads - this time of spill files for thor.
    resourced.setown(optimizeCompoundSource(resourced, csfFlags));
    //insert projects after compound created...
    if (options.optimizeResourcedProjects)
    {
        cycle_t time = msTick();
        OwnedHqlExpr optimized = insertImplicitProjects(*this, resourced.get(), options.optimizeSpillProject);
        DEBUG_TIMER("EclServer: implicit projects", msTick()-time);
        traceExpression("AfterResourcedImplicit", resourced);

        if (optimized != resourced)
            resourced.setown(optimizeCompoundSource(optimized, csfFlags));
    }

    //Now call the optimizer again - the main purpose is to move projects over limits and into compound index/disk reads
    if (options.optimizeGraph)
    {
        unsigned time = msTick();
        traceExpression("BeforeOptimize2", resourced);
        resourced.setown(optimizeHqlExpression(resourced, getOptimizeFlags()|HOOcompoundproject));
        traceExpression("AfterOptimize2", resourced);
        DEBUG_TIMER("EclServer: optimize graph", msTick()-time);
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

    // Call optimizer before resourcing so items get moved over conditions, and remove other items
    // which would otherwise cause extra spills.
    traceExpression("BeforeOptimize", resourced);
    unsigned optFlags = getOptimizeFlags();

    checkNormalized(resourced);
    if (options.optimizeGraph)
    {
        unsigned time = msTick();
        resourced.setown(optimizeHqlExpression(resourced, optFlags|HOOfiltersharedproject));
        //have the following on an "aggressive fold" option?  If no_selects extract constants it can be quite impressive (jholt22.hql)
        //resourced.setown(foldHqlExpression(resourced));
        DEBUG_TIMER("EclServer: optimize graph", msTick()-time);
    }
    traceExpression("AfterOptimize", resourced);
    checkNormalized(resourced);

    if (true)
        resourced.setown(optimizeCompoundSource(resourced, CSFpreload|csfFlags));

    //Now resource the graph....
    unsigned numNodes = 0;
//  Owned<IConstWUClusterInfo> clusterInfo = wu()->getClusterInfo(curCluster);
//  if (clusterInfo)
//      numNodes = clusterInfo->getSize();
    if (options.specifiedClusterSize != 0)
        numNodes = options.specifiedClusterSize;

    traceExpression("BeforeResourcing", resourced);

    cycle_t time = msTick();
    if (outputLibraryId)
    {
        unsigned numResults = outputLibrary->numResultsUsed();
        resourced.setown(resourceLibraryGraph(*this, resourced, targetClusterType, numNodes, outputLibraryId, &numResults));
        HqlExprArray children;
        unwindCommaCompound(children, resourced);
        children.append(*createAttribute(numResultsAtom, getSizetConstant(numResults)));
        children.append(*createAttribute(multiInstanceAtom));       // since can be called from multiple places.
        resourced.setown(createValue(no_subgraph, makeVoidType(), children));
    }
    else
        resourced.setown(resourceThorGraph(*this, resourced, targetClusterType, numNodes, graphIdExpr));

    if (!resourced)
        return NULL;

    DEBUG_TIMER("EclServer: resource graph", msTick()-time);
    traceExpression("AfterResourcing", resourced);

    if (options.regressionTest)
        checkDependencyConsistency(resourced);

    checkNormalized(resourced);

    resourced.setown(optimizeGraphPostResource(resourced, csfFlags));
    if (options.optimizeSpillProject)
    {
        resourced.setown(convertSpillsToActivities(resourced));
        resourced.setown(optimizeGraphPostResource(resourced, csfFlags));
    }

    checkNormalized(resourced);
    //Finally create a couple of special compound activities.
    //e.g., filtered fetch, limited keyed join
    {
        unsigned time = msTick();
        CompoundActivityTransformer transformer(targetClusterType);
        resourced.setown(transformer.transformRoot(resourced));
        traceExpression("AfterCompoundActivity", resourced);
        DEBUG_TIMER("EclServer: tree transform: compound activity", msTick()-time);
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
        beginGraph(ctx);

        unsigned id = 0;
        OwnedHqlExpr graphTag = NULL;//WIP:createAttribute(graphAtom, createConstant((__int64)id));
        OwnedHqlExpr resourced = getResourcedGraph(expr->queryChild(0), graphTag);
        if (resourced)
        {
            traceExpression("beforeGenerate", resourced);
            BuildCtx graphctx(ctx);
            graphctx.addGroup();

            Owned<SubGraphInfo> graphInfo;
            if (graphTag)
            {
                graphInfo.setown(new SubGraphInfo(graph, 0, graphTag, SubGraphRoot));
                graphctx.associate(*graphInfo);
            }

            activeGraphCtx = &graphctx;
            buildStmt(graphctx, resourced);
            activeGraphCtx = NULL;

            graphctx.removeAssociation(graphInfo);

            HqlExprArray args;
            args.append(*createConstant(activeGraphName));
            args.append(*createConstant(targetThor()));
            args.append(*createConstant(0));
            args.append(*createValue(no_nullptr, makeReferenceModifier(makeRowType(queryNullRecord()->getType()))));
            callProcedure(ctx, executeGraphAtom, args);
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

void HqlCppTranslator::buildCsvListFunc(BuildCtx & classctx, const char * func, IHqlExpression * attr, const char * defaultValue)
{
    BuildCtx funcctx(classctx);
    StringBuffer s;

    s.clear().append("virtual const char * ").append(func).append("(unsigned idx)");
    funcctx.addQuotedCompound(s);
    if (attr || defaultValue)
    {
        OwnedHqlExpr idxVar = createVariable("idx", LINK(unsignedType));
        IHqlExpression * value = attr ? attr->queryChild(0) : NULL;

        if (!value || !isEmptyList(value))
        {
            IHqlStmt * caseStmt = funcctx.addSwitch(idxVar);
            if (value)
            {
                if (!value->isList())
                {
                    OwnedHqlExpr label = createConstant((__int64)0);
                    funcctx.addCase(caseStmt, label);
                    buildReturnCsvValue(funcctx, value);
                }
                else
                {
                    ForEachChild(idx, value)
                    {
                        OwnedHqlExpr label = createConstant((__int64)idx);
                        funcctx.addCase(caseStmt, label);
                        buildReturnCsvValue(funcctx, value->queryChild(idx));
                    }
                }
            }
            else
            {
                unsigned entry = 0;
                const char * start  = defaultValue;
                loop
                {
                    const char * end = strchr(start, '|');
                    if (!end) end = start+strlen(start);
                    s.clear().append("case ").append(entry++).append(": return ");
                    appendStringAsQuotedCPP(s, end-start, start, false);
                    s.append(";");
                    funcctx.addQuoted(s);
                    if (!*end)
                        break;
                    start = end+1;
                }
            }
            funcctx.addDefault(caseStmt);
        }
    }
    funcctx.addReturn(queryQuotedNullExpr());
}

static void expandDefaultString(StringBuffer & out, IHqlExpression * property, const char * defaultValue)
{
    IHqlExpression * value = property ? property->queryChild(0) : NULL;
    if (value && value->queryValue())
        value->queryValue()->getStringValue(out);
    else
        out.append(defaultValue);
}

void HqlCppTranslator::buildCsvParameters(BuildCtx & subctx, IHqlExpression * csvAttr, IHqlExpression * record, bool isReading)
{
    HqlExprArray attrs;
    if (csvAttr)
        unwindChildren(attrs, csvAttr);

    BuildCtx classctx(subctx);
    StringBuffer s;
    beginNestedClass(classctx, "csv", "ICsvParameters");

    doBuildBoolFunction(classctx, "queryEBCDIC", queryProperty(ebcdicAtom, attrs)!=NULL);

    bool singleHeader = false;
    bool manyHeader = false;
    IHqlExpression * headerAttr = queryProperty(headerAtom, attrs);
    IHqlExpression * terminator = queryProperty(terminatorAtom, attrs);
    IHqlExpression * separator = queryProperty(separatorAtom, attrs);
    if (headerAttr)
    {
        IHqlExpression * header = queryRealChild(headerAttr, 0);
        if (header)
        {
            if (header->queryType()->isInteger())
            {
                classctx.addQuoted("virtual const char * queryHeader() { return NULL; }");
                doBuildUnsignedFunction(classctx, "queryHeaderLen", header);
            }
            else
            {
                doBuildVarStringFunction(classctx, "queryHeader", header);
                classctx.addQuoted("virtual unsigned queryHeaderLen() { return 1; }");
            }
        }
        else
        {
            StringBuffer names;
            if (!isReading)
            {
                StringBuffer comma;
                expandDefaultString(comma, separator, ",");
                expandFieldNames(names, record, comma.str(), queryPropertyChild(headerAttr, formatAtom, 0));
                expandDefaultString(names, terminator, "\n");
            }
            OwnedHqlExpr namesExpr = createConstant(names.str());
            doBuildVarStringFunction(classctx, "queryHeader", namesExpr);
            classctx.addQuoted("virtual unsigned queryHeaderLen() { return 1; }");
        }

        if (isReading)
        {
            manyHeader = headerAttr->hasProperty(manyAtom) && !headerAttr->hasProperty(singleAtom);
            singleHeader = !manyHeader;
        }
        else
        {
            if (queryRealChild(headerAttr, 1))
                doBuildVarStringFunction(classctx, "queryFooter", headerAttr->queryChild(1));
            if (headerAttr->hasProperty(singleAtom))
                singleHeader = true;
            else
                manyHeader = true;
        }
    }
    else
    {
        classctx.addQuoted("virtual const char * queryHeader() { return NULL; }");
        classctx.addQuoted("virtual unsigned queryHeaderLen() { return 0; }");
    }


    doBuildSizetFunction(classctx, "queryMaxSize", getCsvMaxLength(csvAttr));

    buildCsvListFunc(classctx, "queryQuote", queryProperty(quoteAtom, attrs), isReading ? "'" : NULL);
    buildCsvListFunc(classctx, "querySeparator", separator, ",");
    buildCsvListFunc(classctx, "queryTerminator", terminator, isReading ? "\r\n|\n" : "\n");

    StringBuffer flags;
    if (!queryProperty(quoteAtom, attrs))       flags.append("|defaultQuote");
    if (!queryProperty(separatorAtom, attrs))   flags.append("|defaultSeparate");
    if (!queryProperty(terminatorAtom, attrs))  flags.append("|defaultTerminate");
    if (singleHeader)                           flags.append("|singleHeaderFooter");
    if (manyHeader)                             flags.append("|manyHeaderFooter");
    if (queryProperty(noTrimAtom, attrs))       flags.append("|preserveWhitespace");
    if (flags.length() == 0)                    flags.append("|0");

    doBuildUnsignedFunction(classctx, "getFlags", flags.str()+1);

    endNestedClass();

    subctx.addQuoted("virtual ICsvParameters * queryCsvParameters() { return &csv; }");
}

void HqlCppTranslator::buildCsvWriteScalar(BuildCtx & ctx, IHqlExpression * expr, _ATOM encoding)
{
    ITypeInfo * type = expr->queryType()->queryPromotedType();
    type_t tc = type->getTypeCode();
    LinkedHqlExpr value = expr;
    _ATOM func;
    if (type->isInteger() || tc == type_boolean)
    {
        if (type->isSigned())
            func = writeSignedAtom;
        else
            func = writeUnsignedAtom;
    }
    else if (tc == type_real)
        func = writeRealAtom;
    else if (tc == type_utf8)
    {
        func = writeUtf8Atom;
        value.setown(createValue(no_trim, makeUtf8Type(UNKNOWN_LENGTH, NULL), LINK(value)));
    }
    else if (isUnicodeType(type))
    {
        func = writeUnicodeAtom;
        value.setown(createValue(no_trim, makeUnicodeType(UNKNOWN_LENGTH, NULL), LINK(value)));
    }
    else
    {
        func = writeStringAtom;
        value.setown(createValue(no_trim, LINK(unknownStringType), ensureExprType(value, unknownStringType)));
    }

    if (encoding == asciiAtom)
    {
        func = writeStringAtom;
        Owned<ITypeInfo> type = makeStringType(UNKNOWN_LENGTH, getCharset(asciiAtom), NULL);
        value.setown(ensureExprType(value, type));
    }
    else if (encoding == ebcdicAtom)
    {
        func = writeEbcdicAtom;
        Owned<ITypeInfo> type = makeStringType(UNKNOWN_LENGTH, getCharset(ebcdicAtom), NULL);
        value.setown(ensureExprType(value, type));
    }
    else if (encoding == unicodeAtom)
    {
        func = writeUnicodeAtom;
        Owned<ITypeInfo> type = makeUnicodeType(UNKNOWN_LENGTH, NULL);
        value.setown(ensureExprType(value, type));
    }

    HqlExprArray args;
    args.append(*createVariable("out", makeBoolType()));
    args.append(*LINK(value));

    buildFunctionCall(ctx, func, args);
}

void HqlCppTranslator::buildCsvWriteTransform(BuildCtx & subctx, IHqlExpression * expr, IHqlExpression * selector, _ATOM encoding)
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
                BoundRow * row = cursor->buildIterateLoop(loopctx, false);
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

void HqlCppTranslator::buildCsvWriteTransform(BuildCtx & subctx, IHqlExpression * dataset, _ATOM encoding)
{
    BuildCtx funcctx(subctx);
    funcctx.addQuotedCompound("void writeRow(const byte * self, ITypedOutputStream * out)");
    BoundRow * cursor = bindTableCursor(funcctx, dataset, "self");
    buildCsvWriteTransform(funcctx, dataset->queryRecord(), cursor->querySelector(), encoding);
}

void HqlCppTranslator::buildExpiryHelper(BuildCtx & ctx, IHqlExpression * expireAttr)
{
    if (expireAttr)
    {
        LinkedHqlExpr num = expireAttr->queryChild(0);
        if (!num)
            num.setown(getSizetConstant(DEFAULT_EXPIRY_PERIOD));
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
        BuildCtx subctx(ctx);
        subctx.addQuotedCompound("virtual void getUpdateCRCs(unsigned & eclCrc, unsigned __int64 & totalCRC)");
        OwnedHqlExpr eclCrcVar = createVariable("eclCrc", LINK(unsignedType));
        OwnedHqlExpr totalCrcVar = createVariable("totalCRC", makeIntType(8, false));

        IHqlExpression * originalCrc = updateAttr->queryChild(0);
        DependenciesUsed dependencies(true);
        IHqlExpression * filesRead = updateAttr->queryProperty(_files_Atom);
        if (filesRead)
        {
            ForEachChild(i, filesRead)
                dependencies.tablesRead.append(*getNormalizedFilename(filesRead->queryChild(i)));
        }
        IHqlExpression * resultsRead = updateAttr->queryProperty(_results_Atom);
        if (resultsRead)
            unwindChildren(dependencies.resultsRead, resultsRead);

        OwnedHqlExpr crcExpr = calculatePersistInputCrc(subctx, dependencies);
        buildAssignToTemp(subctx, eclCrcVar, originalCrc);
        buildAssignToTemp(subctx, totalCrcVar, crcExpr);

        if (!updateAttr->hasProperty(alwaysAtom))
            instance.addAttributeBool("_updateIfChanged", true);
    }
}

void HqlCppTranslator::buildClusterHelper(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * cluster = expr->queryProperty(clusterAtom);
    if (!cluster)
        return;

    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual const char * queryCluster(unsigned idx)");

    BuildCtx switchctx(funcctx);
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
    funcctx.addReturn(queryQuotedNullExpr());
}


void HqlCppTranslator::buildRecordEcl(BuildCtx & subctx, IHqlExpression * dataset, const char * methodName)
{
    StringBuffer eclFuncName;
    StringBuffer s;

    //Ensure the ECL for the record reflects its serialized form, not the internal form
    OwnedHqlExpr record = getSerializedForm(dataset->queryRecord());
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
        funcctx.addQuotedCompound(s);

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
    IHqlExpression * payload = expr ? expr->queryProperty(_payload_Atom) : NULL;
    OwnedHqlExpr exprToCrc = getSerializedForm(dataset->queryRecord());
    unsigned payloadSize = 1;
    if (payload)
        payloadSize = (unsigned)getIntValue(payload->queryChild(0)) + payloadDelta;

    exprToCrc.setown(createComma(exprToCrc.getClear(), getSizetConstant(payloadSize)));

    traceExpression("crc:", exprToCrc);
    OwnedHqlExpr crc = getSizetConstant(getExpressionCRC(exprToCrc));
    doBuildUnsignedFunction(ctx, name, crc);
}

static void createOutputIndexRecord(HqlMapTransformer & mapper, HqlExprArray & fields, IHqlExpression * record, bool isMainRecord, bool allowTranslate)
{
    unsigned numFields = record->numChildren();
    unsigned max = isMainRecord ? numFields-1 : numFields;
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
            if (cur->hasProperty(blobAtom))
            {
                newField = createField(cur->queryName(), makeIntType(8, false), NULL, NULL);
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
                    newField = createField(cur->queryName(), newRecord->getType(), args);
                }
                else
                {
                    OwnedHqlExpr select = createSelectExpr(getActiveTableSelector(), LINK(cur));
                    OwnedHqlExpr value = getHozedKeyValue(select);
                    ITypeInfo * newType = value->getType();
                    newField = createField(cur->queryName(), newType, NULL, extractFieldAttrs(cur));

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


static void createOutputIndexTransform(HqlExprArray & assigns, IHqlExpression * self, IHqlExpression * tgtRecord, IHqlExpression * srcRecord, IHqlExpression* srcDataset, bool isMainRecord, bool allowTranslate)
{
    unsigned numFields = srcRecord->numChildren();
    unsigned max = isMainRecord ? numFields-1 : numFields;
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

                if (cur->hasProperty(blobAtom))
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


void HqlCppTranslator::doBuildIndexOutputTransform(BuildCtx & ctx, IHqlExpression * record, SharedHqlExpr & rawRecord)
{
    OwnedHqlExpr srcDataset = createDataset(no_anon, LINK(record));

    HqlExprArray fields;
    HqlExprArray assigns;
    HqlMapTransformer mapper;
    createOutputIndexRecord(mapper, fields, record, true, true);

    OwnedHqlExpr newRecord = createRecord(fields);
    rawRecord.set(newRecord);
    OwnedHqlExpr self = getSelf(newRecord);
    createOutputIndexTransform(assigns, self, newRecord, record, srcDataset, true, true);

    OwnedHqlExpr tgtDataset = createDataset(no_anon, newRecord.getLink());
    OwnedHqlExpr transform = createValue(no_newtransform, makeTransformType(newRecord->getType()), assigns);
    BuildCtx subctx(ctx);
    subctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, IBlobCreator * blobs, unsigned __int64 & filepos)");
    ensureRowAllocated(subctx, "crSelf");
    subctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
    associateBlobHelper(subctx, srcDataset, "blobs");

    BoundRow * selfCursor = bindSelf(subctx, tgtDataset, "crSelf");
    bindTableCursor(subctx, srcDataset, "left");

    associateSkipReturnMarker(subctx, queryZero(), selfCursor);
    doTransform(subctx, transform, selfCursor);

    OwnedHqlExpr fposVar = createVariable("filepos", makeIntType(8, false));
    OwnedHqlExpr fposField = createSelectExpr(LINK(srcDataset), LINK(queryLastField(record)));
    buildAssignToTemp(subctx, fposVar, fposField);

    buildReturnRecordSize(subctx, selfCursor);

    buildMetaMember(ctx, tgtDataset, "queryDiskRecordSize");
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


static HqlTransformerInfo cHqlBlobTransformerInfo("CHqlBlobTransformer");
class CHqlBlobTransformer : public QuickHqlTransformer
{
public:
    CHqlBlobTransformer() : QuickHqlTransformer(cHqlBlobTransformerInfo, NULL) {}

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        OwnedHqlExpr transformed = QuickHqlTransformer::createTransformed(expr);
        if ((expr->getOperator() == no_field) && expr->hasProperty(blobAtom))
            return appendOwnedOperand(transformed, createAttribute(_isBlobInIndex_Atom));
        return transformed.getClear();
    }
};

IHqlExpression * annotateIndexBlobs(IHqlExpression * expr)
{
    CHqlBlobTransformer transformer;
    return transformer.transform(expr);
}


IDefRecordElement * HqlCppTranslator::createMetaRecord(IHqlExpression * record)
{
    TranslatorMaxSizeCallback callback(*this);
    return ::createMetaRecord(record, &callback);
}


IHqlExpression * HqlCppTranslator::getSerializedLayoutFunction(IHqlExpression * record, unsigned numKeyedFields)
{
    OwnedHqlExpr serializedRecord = getSerializedForm(record);
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
    layoutctx.addQuotedCompound(s);

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
    IHqlExpression * dataset  = expr->queryChild(0);
    IHqlExpression * filename = queryRealChild(expr, 1);
    IHqlExpression * record = dataset->queryRecord();
    IHqlDataset * baseTable = dataset->queryDataset()->queryRootTable();

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKindexwrite, expr, "IndexWrite");
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    //virtual const char * getFileName() { return "x.d00"; }
    buildFilenameFunction(*instance, instance->startctx, "getFileName", filename, hasDynamicFilename(expr));

    //virtual const char * getDatasetName() { return "x.d00"; }
    IHqlExpression * tableName = expr->queryProperty(nameAtom);
    if (tableName)
        doBuildVarStringFunction(instance->startctx, "getDatasetName", tableName->queryChild(0));

    //virtual unsigned getFlags() = 0;
    IHqlExpression * updateAttr = expr->queryProperty(updateAtom);
    IHqlExpression * compressAttr = expr->queryProperty(compressedAtom);
    IHqlExpression * widthExpr = queryPropertyChild(expr, widthAtom, 0);
    bool hasTLK = !expr->hasProperty(noRootAtom);
    bool singlePart = expr->hasProperty(fewAtom);
    if (matchesConstantValue(widthExpr, 1))
    {
        singlePart = true;
        widthExpr = NULL;
    }

    StringBuffer s;
    StringBuffer flags;
    if (expr->hasProperty(overwriteAtom)) flags.append("|TIWoverwrite");
    if (expr->hasProperty(noOverwriteAtom)) flags.append("|TIWnooverwrite");
    if (expr->hasProperty(backupAtom))    flags.append("|TIWbackup");
    if (!filename->isConstant())          flags.append("|TIWvarfilename");
    if (singlePart)                       flags.append("|TIWsmall");
    if (updateAttr)                       flags.append("|TIWupdatecrc");
    if (updateAttr && !updateAttr->queryProperty(alwaysAtom)) flags.append("|TIWupdate");
    if (!hasTLK && !singlePart)           flags.append("|TIWlocal");
    if (compressAttr)
    {
        if (compressAttr->hasProperty(rowAtom))   flags.append("|TIWrowcompress");
        if (!compressAttr->hasProperty(lzwAtom))  flags.append("|TIWnolzwcompress");
    }
    if (widthExpr) flags.append("|TIWhaswidth");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    IHqlExpression * indexNameAttr = expr->queryProperty(indexAtom);
    if (indexNameAttr)
        buildFilenameFunction(*instance, instance->startctx, "getDistributeIndexName", indexNameAttr->queryChild(0), hasDynamicFilename(expr));

    buildExpiryHelper(instance->createctx, expr->queryProperty(expireAtom));
    buildUpdateHelper(instance->createctx, *instance, dataset, updateAttr);
    buildClusterHelper(instance->classctx, expr);

    // virtual unsigned getKeyedSize()
    HqlExprArray fields;
    unwindChildren(fields, record);
    removeProperties(fields);
    fields.popn(numPayloadFields(expr));
    OwnedHqlExpr keyedRecord = createRecord(fields); // must be fixed length => no maxlength
    if (expr->hasProperty(_payload_Atom))
        instance->classctx.addQuoted(s.clear().append("virtual unsigned getKeyedSize() { return ").append(getFixedRecordSize(keyedRecord)).append("; }"));
    else
        instance->classctx.addQuoted(s.clear().append("virtual unsigned getKeyedSize() { return (unsigned) -1; }"));

    //virtual const char * queryRecordECL() = 0;
    buildRecordEcl(instance->createctx, dataset, "queryRecordECL");

    doBuildSequenceFunc(instance->classctx, querySequence(expr), false);
    Owned<IWUResult> result = createDatasetResultSchema(querySequence(expr), queryResultName(expr), dataset->queryRecord(), false, true);

    if (expr->hasProperty(setAtom))
    {
        BuildCtx subctx(instance->startctx);
        subctx.addQuotedCompound("virtual bool getIndexMeta(size32_t & lenName, char * & name, size32_t & lenValue, char * & value, unsigned idx)");

        CHqlBoundTarget nameTarget, valueTarget;
        initBoundStringTarget(nameTarget, unknownStringType, "lenName", "name");
        //more should probably be utf-8 rather than string
        initBoundStringTarget(valueTarget, unknownStringType, "lenValue", "value");

        OwnedHqlExpr idxVar = createVariable("idx", LINK(sizetType));
        BuildCtx casectx(subctx);
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
        buildReturn(subctx, queryBoolExpr(false));
    }

    OwnedHqlExpr rawRecord;
    doBuildIndexOutputTransform(instance->startctx, record, rawRecord);
    buildFormatCrcFunction(instance->classctx, "getFormatCrc", rawRecord, expr, 0);

    if (compressAttr && compressAttr->hasProperty(rowAtom))
    {
        if (!isFixedWidthDataset(rawRecord))
            throwError(HQLERR_RowCompressRequireFixedSize);
    }
    if (!expr->hasProperty(fixedAtom))
        buildSerializedLayoutMember(instance->classctx, record, "getIndexLayout", fields.ordinality());

    if (widthExpr)
    {
        doBuildUnsignedFunction(instance->startctx, "getWidth", widthExpr);

        if (!hasTLK)
        {
            HqlExprArray sorts;
            gatherIndexBuildSortOrder(sorts, expr, options.sortIndexPayload);
            OwnedHqlExpr sortOrder = createValueSafe(no_sortlist, makeSortListType(NULL), sorts);
            instance->startctx.addQuoted("virtual ICompare * queryCompare() { return &compare; }");

            DatasetReference dsRef(dataset);
            buildCompareClass(instance->nestedctx, "compare", sortOrder, dsRef);
        }
    }
    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

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

    IHqlExpression * rowAttr = xmlAttr->queryProperty(rowAtom);
    if (rowAttr)
        doBuildVarStringFunction(instance->startctx, "queryIteratorPath", rowAttr->queryChild(0));
    IHqlExpression * headerAttr = xmlAttr->queryProperty(headerAtom);
    if (headerAttr)
    {
        doBuildVarStringFunction(instance->startctx, "queryHeader", headerAttr->queryChild(0));
        doBuildVarStringFunction(instance->startctx, "queryFooter", headerAttr->queryChild(1));
    }
    StringBuffer xmlFlags;
    if (xmlAttr->hasProperty(trimAtom))
        xmlFlags.append("|XWFtrim");
    if (xmlAttr->hasProperty(optAtom))
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
    IHqlExpression * filename = queryRealChild(expr, 1);

    if (!filename)
        return doBuildActivityOutputWorkunit(ctx, expr, isRoot);

    IHqlExpression * program  = queryRealChild(expr, 2);
    IHqlExpression * csvAttr = expr->queryProperty(csvAtom);
    IHqlExpression * xmlAttr = expr->queryProperty(xmlAtom);
    IHqlExpression * expireAttr = expr->queryProperty(expireAtom);
    IHqlExpression * seq = querySequence(expr);

    IHqlExpression *pipe = NULL;
    if (program)
    {
        if (program->getOperator()==no_pipe)
            pipe = program->queryChild(0);
    }
    else if (filename->getOperator()==no_pipe)
        pipe = filename->queryChild(0);

    if (pipe)
        checkPipeAllowed();

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    ThorActivityKind kind = TAKdiskwrite;
    const char * activity = "DiskWrite";
    if (expr->getOperator() == no_spill)
    {
        kind = TAKspill;
        activity = "Spill";
    }
    else if (pipe)
    {
        kind = TAKpipewrite;
        activity = "PipeWrite";
    }
    else if (csvAttr)
    {
        kind = TAKcsvwrite;
        activity = "CsvWrite";
    }
    else if (xmlAttr)
    {
        kind = TAKxmlwrite;
        activity = "XmlWrite";
    }

    bool useImplementationClass = options.minimizeActivityClasses && targetRoxie() && expr->hasProperty(_spill_Atom);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, activity);
    //Output to a variable filename is either a user result, or a computed workflow spill, both need evaluating.

    if (useImplementationClass)
        instance->setImplementationClass(newMemorySpillSplitArgAtom);

    if ((kind == TAKdiskwrite) && filename->queryValue())
    {
        StringBuffer s;
        s.append(getActivityText(kind));
        if (expr->hasProperty(_spill_Atom))
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
    if (expr->hasProperty(_spill_Atom) || expr->hasProperty(jobTempAtom))
    {
        IPropertyTree * graphNode = NULL;
        if (targetRoxie() && expr->hasProperty(jobTempAtom))
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
            if (expr->hasProperty(repeatAtom))
            {
                //virtual const char * getPipeProgram() { return "grep"; }
                instance->startctx.addQuoted("virtual char * getPipeProgram() { return NULL; }");

                BuildCtx pipeCtx(instance->startctx);
                pipeCtx.addQuotedCompound("virtual char * getNameFromRow(const void * _self)");
                pipeCtx.addQuoted("const unsigned char * self = (const unsigned char *) _self;");
                bindTableCursor(pipeCtx, dataset, "self");
                buildReturn(pipeCtx, pipe, unknownVarStringType);
            }
            else
            {
                //virtual const char * getPipeProgram() { return "grep"; }
                BuildCtx pipeCtx(instance->startctx);
                pipeCtx.addQuotedCompound("virtual char * getPipeProgram()");
                buildReturn(pipeCtx, pipe, unknownVarStringType);
            }

            if (csvAttr)
                instance->classctx.addQuoted("virtual IHThorCsvWriteExtra * queryCsvOutput() { return this; }");
            if (xmlAttr)
                instance->classctx.addQuoted("virtual IHThorXmlWriteExtra * queryXmlOutput() { return this; }");

            StringBuffer flags;
            if (expr->hasProperty(repeatAtom))
                flags.append("|TPFrecreateeachrow");
            if (expr->hasProperty(optAtom))
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
                buildFilenameFunction(*instance, instance->startctx, "getFileName", filename, hasDynamicFilename(expr));
                if (!filename->isConstant())
                    constFilename = false;
            }
            else
            {
                BuildCtx getNameCtx(instance->startctx);
                getNameCtx.addQuotedCompound("virtual const char * getFileName()");
                getNameCtx.addReturn(queryQuotedNullExpr());
            }

            //virtual unsigned getFlags() = 0;
            IHqlExpression * updateAttr = expr->queryProperty(updateAtom);
            StringBuffer s;
            StringBuffer flags;
            if (expr->hasProperty(_spill_Atom)) flags.append("|TDXtemporary");
            if (expr->hasProperty(groupedAtom)) flags.append("|TDXgrouped");
            if (expr->hasProperty(compressedAtom)) flags.append("|TDWnewcompress");
            if (expr->hasProperty(__compressed__Atom)) flags.append("|TDXcompress");
            if (expr->hasProperty(extendAtom)) flags.append("|TDWextend");
            if (expr->hasProperty(overwriteAtom)) flags.append("|TDWoverwrite");
            if (expr->hasProperty(noOverwriteAtom)) flags.append("|TDWnooverwrite");
            if (expr->hasProperty(_workflowPersist_Atom)) flags.append("|TDWpersist");
            if (expr->hasProperty(_noReplicate_Atom)) flags.append("|TDWnoreplicate");
            if (expr->hasProperty(backupAtom)) flags.append("|TDWbackup");
            if (expr->hasProperty(resultAtom)) flags.append("|TDWowned|TDWresult");
            if (expr->hasProperty(ownedAtom)) flags.append("|TDWowned");
            if (!constFilename) flags.append("|TDXvarfilename");
            if (hasDynamicFilename(expr)) flags.append("|TDXdynamicfilename");
            if (expr->hasProperty(jobTempAtom)) flags.append("|TDXjobtemp");
            if (updateAttr) flags.append("|TDWupdatecrc");
            if (updateAttr && !updateAttr->queryProperty(alwaysAtom)) flags.append("|TDWupdate");

            if (flags.length())
                doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

            //virtual const char * queryRecordECL() = 0;
            buildRecordEcl(instance->createctx, dataset, "queryRecordECL");

            buildExpiryHelper(instance->createctx, expireAttr);
            buildUpdateHelper(instance->createctx, *instance, dataset, updateAttr);
        }

        doBuildSequenceFunc(instance->classctx, seq, true);
        if (tempCount)
        {
            if ((kind != TAKspill) || !matchesConstantValue(tempCount, 1))
            {
                BuildCtx ctx1(instance->classctx);
                ctx1.addQuotedCompound("virtual unsigned getTempUsageCount()");
                buildReturn(ctx1, tempCount, unsignedType);
            }
        }

        Owned<IWUResult> result = createDatasetResultSchema(seq, queryResultName(expr), dataset->queryRecord(), (kind != TAKcsvwrite) && (kind != TAKxmlwrite), true);
        if (expr->hasProperty(resultAtom))
            result->setResultRowLimit(-1);

        buildFormatCrcFunction(instance->classctx, "getFormatCrc", dataset, NULL, 0);

        LinkedHqlExpr diskDataset = dataset;
        if (!expr->hasProperty(groupedAtom) && isGroupedActivity(dataset))
            diskDataset.setown(createDataset(no_group, LINK(dataset), NULL));
        if ((kind != TAKspill) || (diskDataset->queryType() != expr->queryType()))
            buildMetaMember(instance->classctx, diskDataset, "queryDiskRecordSize");
        buildClusterHelper(instance->classctx, expr);

        //Both csv write and pipe with csv/xml format
        if (csvAttr)
            buildCsvWriteMembers(instance, dataset, csvAttr);
        if (xmlAttr)
            buildXmlWriteMembers(instance, dataset, xmlAttr);

        buildEncryptHelper(instance->startctx, expr->queryProperty(encryptAtom));
    }
    else
    {
        assertex(tempCount.get() && !hasDynamic(expr));
        instance->addConstructorParameter(tempCount);
        addFilenameConstructorParameter(*instance, "getFileName", filename);
    }

    instance->addAttributeBool("_isSpill", expr->hasProperty(_spill_Atom));
    if (targetRoxie())
        instance->addAttributeBool("_isSpillGlobal", expr->hasProperty(jobTempAtom));

    buildInstanceSuffix(instance);
    if (boundDataset)
    {
        buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    }

    OwnedHqlExpr dependency = createAttribute(fileAtom, getNormalizedFilename(filename));
    Owned<ABoundActivity> bound = instance->getBoundActivity();
    OwnedHqlExpr boundUnknown = createUnknown(no_attr, NULL, NULL, LINK(bound));
    activeGraphCtx->associateExpr(dependency, boundUnknown);

    IHqlExpression * name = queryPropertyChild(expr, namedAtom, 0);
    if (name)
        associateRemoteResult(*instance, seq, name);

    return instance->getBoundActivity();
}

void HqlCppTranslator::addSchemaField(IHqlExpression *field, MemoryBuffer &schema, IHqlExpression *selector)
{
    _ATOM name = field->queryName();
    StringBuffer schemaName;
    if (name)
    {
        schemaName.append(name->str());
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
        schemaType.set(schemaType->queryChildType());
        break;
    case type_bitfield:
        schemaType.set(schemaType->queryPromotedType());
        //fall through;
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
    OwnedHqlExpr record = getSerializedForm(deserializedRecord);
    if ((options.maxRecordSize != MAX_RECORD_SIZE) && maxRecordSizeUsesDefault(record))
    {
        //Add an explicit record size if default max record size
        size32_t maxSize = getMaxRecordSize(record);
        HqlExprArray args;
        unwindChildren(args, record);
        args.append(*createAttribute(maxLengthAtom, getSizetConstant(maxSize)));
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


void HqlCppTranslator::addSchemaResource(int seq, const char * name, IHqlExpression * record)
{
    StringBuffer xml;
    getRecordXmlSchema(xml, record, true);
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


IWUResult * HqlCppTranslator::createDatasetResultSchema(IHqlExpression * sequenceExpr, IHqlExpression * name, IHqlExpression * record, bool createTransformer, bool isFile)
{
    //Some spills have no sequence attached
    if (!sequenceExpr)
        return NULL;

    int sequence = (int)getIntValue(sequenceExpr);
    Owned<IWUResult> result = createWorkunitResult(sequence, name);
    if (!result)
        return NULL;

    MemoryBuffer schema;
    OwnedHqlExpr self = getSelf(record);
    addSchemaFields(record, schema, self);

    SCMStringBuffer resultName;
    result->getResultName(resultName);
    addSchemaResource(sequence, resultName.str(), record);

    result->setResultSchemaRaw(schema.length(), schema.toByteArray());
    result->setResultScalar(false);

    OwnedHqlExpr serialRecord = getSerializedForm(record);
    OwnedHqlExpr ds = createDataset(no_anon, LINK(serialRecord));
    MetaInstance meta(*this, ds);
    buildMetaInfo(meta);
    result->setResultRecordSizeEntry(meta.metaFactoryName);

    if (targetRoxie() && (sequence >= 0) && !isFile)
        result->setResultFormat(ResultFormatXml);

    if (createTransformer)
    {
        OwnedHqlExpr noVirtualRecord = removeVirtualAttributes(serialRecord);
        Owned<IHqlExpression> transformedRecord = getSimplifiedRecord(noVirtualRecord, false);
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
            transformctx.addQuotedCompound(s);

            BoundRow * selfCursor = bindSelf(transformctx, tds, "crSelf");
            bindTableCursor(transformctx, ds, "src", no_left, seq);
            associateSkipReturnMarker(transformctx, queryZero(), selfCursor);
            ensureRowAllocated(transformctx, "crSelf");

            doTransform(transformctx, transform, selfCursor);
            buildReturnRecordSize(transformctx, selfCursor);

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
        callProcedure(subctx, outputXmlSetAllAtom, args);
        subctx.selectElse(stmt);
    }
    CHqlBoundExpr boundCurElement;
    cursor->buildIterateLoop(subctx, boundCurElement, false);
    OwnedHqlExpr curElement = boundCurElement.getTranslatedExpr();

    buildXmlSerializeScalar(subctx, curElement, itemName);
}

void HqlCppTranslator::buildXmlSerializeBeginNested(BuildCtx & ctx, IHqlExpression * name, bool doIndent)
{
    if (name)
    {
        HqlExprArray args;
        args.append(*createVariable("out", makeBoolType()));
        args.append(*LINK(name));
        args.append(*createConstant(false));
        callProcedure(ctx, outputXmlBeginNestedAtom, args);
    }
}

void HqlCppTranslator::buildXmlSerializeEndNested(BuildCtx & ctx, IHqlExpression * name)
{
    if (name)
    {
        HqlExprArray args;
        args.append(*createVariable("out", makeBoolType()));
        args.append(*LINK(name));
        callProcedure(ctx, outputXmlEndNestedAtom, args);
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

    Owned<IHqlCppDatasetCursor> cursor = createDatasetSelector(ctx, value);
    BuildCtx subctx(ctx);
    BoundRow * sourceRow = cursor->buildIterateLoop(subctx, false);
    buildXmlSerializeBeginNested(subctx, rowName, true);

    StringBuffer boundRowText;
    generateExprCpp(boundRowText, sourceRow->queryBound());
    OwnedHqlExpr ds = createDataset(no_null, LINK(field->queryRecord()));
    buildXmlSerializeUsingMeta(subctx, ds, boundRowText.str());

    buildXmlSerializeEndNested(subctx, rowName);

    buildXmlSerializeEndNested(ctx, name);
}

void HqlCppTranslator::buildXmlSerializeScalar(BuildCtx & ctx, IHqlExpression * selected, IHqlExpression * name)
{
    ITypeInfo * type = selected->queryType()->queryPromotedType();
    LinkedHqlExpr value = selected;
    _ATOM func;
    switch (type->getTypeCode())
    {
    case type_boolean:
        func = outputXmlBoolAtom;
        break;
    case type_string:
    case type_varstring:
        func = outputXmlStringAtom;
        break;
    case type_qstring:
        func = outputXmlQStringAtom;
        break;
    case type_data:
        func = outputXmlDataAtom;
        break;
    case type_unicode:
    case type_varunicode:
        func = outputXmlUnicodeAtom;
        break;
    case type_utf8:
        func = outputXmlUtf8Atom;
        break;
    case type_real:
        func = outputXmlRealAtom;
        break;
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_bitfield:
        if (type->isSigned())
            func = outputXmlIntAtom;
        else
            func = outputXmlUIntAtom;
        break;
    case type_decimal:
        value.setown(ensureExprType(value, unknownStringType));
        func = outputXmlStringAtom;
        break;
    default:
        UNIMPLEMENTED;
    }

    HqlExprArray args;
    args.append(*createVariable("out", makeBoolType()));
    args.append(*value.getLink());
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
    StringBuffer s;
    BuildCtx funcctx(ctx);

    funcctx.addQuotedCompound(s.append("virtual void ").append(funcName).append("(const byte * self, IXmlWriter & out)"));
    if (!isMeta)
    {
        buildXmlSerializeUsingMeta(funcctx, dataset, "self");
    }
    else
    {
        BoundRow * selfCursor = bindTableCursor(funcctx, dataset, "self");
        buildXmlSerialize(funcctx, dataset->queryRecord(), selfCursor->querySelector(), NULL);
    }
}

void HqlCppTranslator::buildXmlSerializeUsingMeta(BuildCtx & ctx, IHqlExpression * dataset, const char * self)
{
    MetaInstance meta(*this, dataset);
    buildMetaInfo(meta);

    StringBuffer s;
    ctx.addQuoted(s.append(meta.queryInstanceObject()).append(".toXML(").append(self).append(", out);"));
}

//-------------------------------------------------------------------------------------------------------------------

//-------------------------------------------------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityOutputWorkunit(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * record = dataset->queryRecord();
    IHqlExpression * seq = querySequence(expr);
    IHqlExpression * name = queryResultName(expr);
    int sequence = (int)getIntValue(seq, ResultSequenceInternal);

    if (expr->hasProperty(diskAtom))
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
    bool useImplementationClass = options.minimizeActivityClasses && (sequence == ResultSequenceInternal);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKworkunitwrite, expr, "WorkUnitWrite");
    if (useImplementationClass)
        instance->setImplementationClass(newWorkUnitWriteArgAtom);

    graphLabel.append(getActivityText(instance->kind)).append("\n");
    getStoredDescription(graphLabel, seq, name, true);
    instance->graphLabel.set(graphLabel.str());
    buildActivityFramework(instance, isRoot && !isInternalSeq(seq));

    buildInstancePrefix(instance);

    noteResultDefined(ctx, instance, seq, name, isRoot);

    //virtual unsigned getFlags()
    StringBuffer flags;
    if (expr->hasProperty(extendAtom))
        flags.append("|POFextend");
    if (expr->hasProperty(groupedAtom))
        flags.append("|POFgrouped");

    if (!useImplementationClass)
    {
        doBuildSequenceFunc(instance->classctx, seq, true);
        if (name)
        {
            BuildCtx namectx(instance->startctx);
            namectx.addQuotedCompound("virtual const char * queryName()");
            buildReturn(namectx, name, constUnknownVarStringType);
        }

        Owned<IWUResult> result = createDatasetResultSchema(seq, name, record, true, false);
        if (result)
        {
            result->setResultRowLimit(-1);

            if (sequence >= 0)
                buildXmlSerialize(instance->startctx, dataset, "serializeXml", false);
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
    if (expr->hasProperty(groupedAtom) && (dataset->getOperator() != no_null))
        throwError1(HQLERR_NotSupportedInsideNoThor, "Grouped OUTPUT");

    LinkedHqlExpr seq = querySequence(expr);
    LinkedHqlExpr name = queryResultName(expr);
    assertex(seq != NULL);
    int sequence = (int)getIntValue(seq, (int)ResultSequenceInternal);
    if (!seq)
        seq.setown(getSizetConstant(sequence));
    if (!name)
        name.setown(createQuoted("NULL", LINK(constUnknownVarStringType)));

    Owned<IWUResult> result = createDatasetResultSchema(seq, name, dataset->queryRecord(), true, false);

    CHqlBoundExpr bound;
    buildDataset(ctx, dataset, bound, FormatNatural);
    OwnedHqlExpr count = getBoundCount(bound);

    HqlExprArray args;
    args.append(*LINK(name));
    args.append(*LINK(seq));
    args.append(*bound.getTranslatedExpr());
    args.append(*createTranslated(count));
    args.append(*LINK(queryBoolExpr(expr->hasProperty(extendAtom))));
    buildFunctionCall(ctx, setResultDatasetAtom, args);
}


//---------------------------------------------------------------------------


ABoundActivity * HqlCppTranslator::doBuildActivityPipeThrough(BuildCtx & ctx, IHqlExpression * expr)
{
    checkPipeAllowed();

    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * pipe = expr->queryChild(1);
    IHqlExpression * output = expr->queryProperty(outputAtom);
    IHqlExpression * csvToPipe = output ? output->queryProperty(csvAtom) : NULL;
    IHqlExpression * xmlToPipe = output ? output->queryProperty(xmlAtom) : NULL;
    IHqlExpression * csvFromPipe = expr->queryProperty(csvAtom);
    IHqlExpression * xmlFromPipe = expr->queryProperty(xmlAtom);

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

    if (expr->hasProperty(repeatAtom))
    {
        //virtual char * getPipeProgram() { return "grep"; }
        instance->startctx.addQuoted("virtual char * getPipeProgram() { return NULL; }");

        BuildCtx pipeCtx(instance->startctx);
        pipeCtx.addQuotedCompound("virtual char * getNameFromRow(const void * _self)");
        pipeCtx.addQuoted("const unsigned char * self = (const unsigned char *) _self;");
        bindTableCursor(pipeCtx, dataset, "self");
        buildReturn(pipeCtx, pipe, unknownVarStringType);
    }
    else
    {
        //virtual char * getPipeProgram() { return "grep"; }
        BuildCtx pipeCtx(instance->startctx);
        pipeCtx.addQuotedCompound("virtual char * getPipeProgram()");
        buildReturn(pipeCtx, pipe, unknownVarStringType);
    }

    if (csvToPipe)
    {
        buildCsvWriteMembers(instance, dataset, csvToPipe);
        instance->classctx.addQuoted("virtual IHThorCsvWriteExtra * queryCsvOutput() { return this; }");
    }

    if (xmlToPipe)
    {
        buildXmlWriteMembers(instance, dataset, xmlToPipe);
        instance->classctx.addQuoted("virtual IHThorXmlWriteExtra * queryXmlOutput() { return this; }");
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
        doBuildVarStringFunction(instance->classctx, "queryXmlIteratorPath", queryPropertyChild(xmlFromPipe, rowAtom, 0));
    }
    
    StringBuffer flags;
    if (expr->hasProperty(repeatAtom))
        flags.append("|TPFrecreateeachrow");
    if (expr->hasProperty(groupAtom))
        flags.append("|TPFgroupeachrow");
    if (expr->hasProperty(optAtom))
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
    if (xmlToPipe && xmlToPipe->hasProperty(noRootAtom))
        flags.append("|TPFwritenoroot");
    if (xmlFromPipe && xmlFromPipe->hasProperty(noRootAtom))
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
void HqlCppTranslator::doCompareLeftRight(BuildCtx & ctx, const char * funcname, const DatasetReference & datasetLeft, const DatasetReference & datasetRight, HqlExprArray & left, HqlExprArray & right)
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

void HqlCppTranslator::buildSlidingMatchFunction(BuildCtx & ctx, HqlExprArray & leftEq, HqlExprArray & rightEq, HqlExprArray & slidingMatches, const char * funcname, unsigned childIndex, const DatasetReference & datasetL, const DatasetReference & datasetR)
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

void HqlCppTranslator::generateSortCompare(BuildCtx & nestedctx, BuildCtx & ctx, node_operator side, const DatasetReference & dataset, HqlExprArray & sorts, bool canRemoveSort, IHqlExpression * noSortAttr, bool canReuseLeft, bool isLightweight)
{
    StringBuffer s, compareName;

    const char * sideText = (side == no_left) ? "Left" : "Right";
    compareName.append("compare").append(sideText);

    assertex(dataset.querySide() == no_activetable);
    bool noNeedToSort = canRemoveSort && isAlreadySorted(dataset.queryDataset(), sorts, canRemoveSort, true);
    if (noSortAttr)
    {
        IHqlExpression * child = noSortAttr->queryChild(0);
        if (!child)
            noNeedToSort = true;
        else
        {
            if (side == no_left)
                noNeedToSort = child->queryName() == leftAtom;
            else if (side == no_left)
                noNeedToSort = child->queryName() == rightAtom;
        }
    }
    
    if (noNeedToSort || isLightweight)
    {
        if (!noNeedToSort)
        {
            DBGLOG("Lightweight true, but code generator didn't think sort was required");
            ctx.addQuoted("//Forced by lightweight");
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
        beginNestedClass(classctx, compareName.str(), "ICompare");

        BuildCtx funcctx(classctx);
        funcctx.addQuotedCompound("virtual int docompare(const void * _left, const void * _right) const");
        funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
        funcctx.addQuoted("const unsigned char * right = (const unsigned char *) _right;");
        funcctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);

        OwnedHqlExpr groupOrder = createValueSafe(no_sortlist, makeSortListType(NULL), sorts);
        buildReturnOrder(funcctx, groupOrder, dataset);

        endNestedClass();
    }
}


void HqlCppTranslator::generateSerializeAssigns(BuildCtx & ctx, IHqlExpression * record, IHqlExpression * selector, IHqlExpression * selfSelect, IHqlExpression * leftSelect, const DatasetReference & srcDataset, const DatasetReference & tgtDataset, HqlExprArray & srcSelects, HqlExprArray & tgtSelects, bool needToClear)
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
                    buildAssign(ctx, self, left);
                    //Note, we could stop here if needToClear and all fields have been assigned, and all the following fields are fixed width.
                    // but not really sure it is worth it.
                }
                else if (cur->isDatarow())
                {
                    generateSerializeAssigns(ctx, cur->queryRecord(), selected, selfSelect, leftSelect, srcDataset, tgtDataset, srcSelects, tgtSelects, needToClear);
                }
                else if (needToClear)
                {
                    //MORE: Might want to recurse if a record
                    Owned<IHqlExpression> self = tgtDataset.mapScalar(selected, selfSelect);
                    buildClear(ctx, self);
                }
                break;
            }
        case no_record:
            generateSerializeAssigns(ctx, cur, selector, selfSelect, leftSelect, srcDataset, tgtDataset, srcSelects, tgtSelects, needToClear);
            break;
        case no_ifblock:
            //Filter on target...
            UNIMPLEMENTED;
            generateSerializeAssigns(ctx, cur->queryChild(1), selector, selfSelect, leftSelect, srcDataset, tgtDataset, srcSelects, tgtSelects, needToClear);
            break;
        }
    }
}


void HqlCppTranslator::generateSerializeFunction(BuildCtx & ctx, const char * funcName, bool serialize, const DatasetReference & srcDataset, const DatasetReference & tgtDataset, HqlExprArray & srcSelects, HqlExprArray & tgtSelects)
{
    StringBuffer s;

    BuildCtx r2kctx(ctx);
    s.append("virtual unsigned ").append(funcName).append("(ARowBuilder & crSelf, const void * _src, unsigned & thisRecordSize)");
    r2kctx.addQuotedCompound(s);
    ensureRowAllocated(r2kctx, "crSelf");
    r2kctx.addQuoted("const unsigned char * src = (const unsigned char *) _src;");

    OwnedHqlExpr selSeq = createDummySelectorSequence();
    BoundRow * tgtCursor = bindSelf(ctx, tgtDataset.queryDataset(), "crSelf");
    BoundRow * srcCursor = bindTableCursor(ctx, srcDataset.queryDataset(), "src", no_left, selSeq);

    IHqlExpression * leftSelect = srcCursor->querySelector();
    IHqlExpression * selfSelect = tgtCursor->querySelector();
    IHqlExpression * record = tgtDataset.queryDataset()->queryRecord();

    generateSerializeAssigns(r2kctx, record, tgtDataset.querySelector(), selfSelect, leftSelect, srcDataset, tgtDataset, srcSelects, tgtSelects, !isFixedRecordSize(record));

    BoundRow * recordCursor = serialize ? srcCursor : tgtCursor;
    OwnedHqlExpr recordSize = getRecordSize(recordCursor->querySelector());
    OwnedHqlExpr recordSizeVar = createVariable("thisRecordSize", LINK(unsignedType));
    buildAssignToTemp(r2kctx, recordSizeVar, recordSize);

    buildReturnRecordSize(r2kctx, serialize ? tgtCursor : srcCursor);
}

void HqlCppTranslator::generateSerializeKey(BuildCtx & nestedctx, node_operator side, const DatasetReference & dataset, HqlExprArray & sorts, bool isGlobal, bool generateCompares, bool canReuseLeft)
{
    //check if there are any ifblocks, and if so don't allow it.  Even more accurate would be no join fields used in ifblocks
    IHqlExpression * record = dataset.queryDataset()->queryRecord();
    bool canSerialize = targetThor() && isGlobal && !recordContainsIfBlock(record);
    const char * sideText = (side == no_none) ? "" : (side == no_left) ? "Left" : "Right";
    StringBuffer s, s2;

    HqlExprArray keyFields;
    HqlExprArray keySelects;
    HqlExprArray datasetSelects;
    HqlExprArray keyCompares;
    if (canSerialize)
    {
        ForEachItemIn(idx, sorts)
        {
            //MORE: Nested - this won't serialize the key if sorting by a field in a nested record
            //      If this is a problem we will need to create new fields for each value.
            IHqlExpression & cur = sorts.item(idx);
            IHqlExpression * value = &cur;
            if (value->getOperator() == no_negate)
                value=value->queryChild(0);
            if ((value->getOperator() == no_select) && (value->queryChild(0)->queryNormalizedSelector() == dataset.querySelector()))
            {
                if (value->queryType()->getTypeCode() == type_alien)
                {
                    //MORE: Really should check if a self contained alien data type.
                    canSerialize = false;
                    break;
                }

                OwnedHqlExpr serializedField = getSerializedForm(value->queryChild(1));
                OwnedHqlExpr mappedSelect = dataset.mapScalar(value,queryActiveTableSelector());
                keyFields.append(*LINK(serializedField));
                keySelects.append(*createSelectExpr(LINK(mappedSelect->queryChild(0)), LINK(serializedField)));
                datasetSelects.append(*LINK(value));
                keyCompares.append(*dataset.mapScalar(&cur,queryActiveTableSelector()));
            }
            else if (!value->isConstant())
            {
                canSerialize = false;
                break;
            }
        }
    }

    //The following test will need to change if we serialize when nested fields are used (see above)
    if (sorts.ordinality() >= getFlatFieldCount(record))
        canSerialize = false;

    if (canSerialize)
    {
        if (canReuseLeft)
        {
            assertex(!generateCompares);
            s.clear().append("virtual ISortKeySerializer * querySerialize").append(sideText).append("() { return &serializerLeft; }");
            nestedctx.addQuoted(s); 
        }
        else
        {
            StringBuffer memberName;
            memberName.append("serializer").append(sideText);

            BuildCtx classctx(nestedctx);
            beginNestedClass(classctx, memberName, "ISortKeySerializer");

            IHqlExpression * keyRecord = createRecordInheritMaxLength(keyFields, record);
            Owned<IHqlExpression> keyDataset = createDataset(no_anon, keyRecord);

            DatasetReference keyActiveRef(keyDataset, no_activetable, NULL);

            generateSerializeFunction(classctx, "recordToKey", true, dataset, keyActiveRef, datasetSelects, keySelects);
            generateSerializeFunction(classctx, "keyToRecord", false, keyActiveRef, dataset, keySelects, datasetSelects);
            buildMetaMember(classctx, keyDataset, "queryRecordSize");

            endNestedClass();

            s.clear().append("virtual ISortKeySerializer * querySerialize").append(sideText).append("() { return &serializer").append(sideText).append("; }");
            nestedctx.addQuoted(s); 

            if (generateCompares)
            {
                OwnedHqlExpr keyOrder = createValueSafe(no_sortlist, makeSortListType(NULL), keyCompares);
                buildCompareMember(nestedctx, "CompareKey", keyOrder, keyActiveRef);

                doCompareLeftRight(nestedctx, "CompareRowKey", dataset, keyActiveRef, sorts, keyCompares);
            }
        }
    }
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
        if (!rowlimit->hasProperty(skipAtom))
        {
            LinkedHqlExpr fail = queryChildOperator(no_fail, rowlimit);
            if (!fail)
                fail.setown(createFailAction("JOIN limit exceeded", rowlimit->queryChild(0), filename, instance.activityId));

            BuildCtx ctx(instance.startctx);
            ctx.addQuotedCompound("virtual void onMatchAbortLimitExceeded()");
            buildStmt(ctx, fail);
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
            WARNING2(HQLWRN_ImplicitJoinLimit, options.defaultImplicitKeyedJoinLimit, fname.str());
        }
    }
}


ABoundActivity * HqlCppTranslator::doBuildActivityJoinOrDenormalize(BuildCtx & ctx, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    assertex(op==no_join || op==no_selfjoin || op==no_denormalize || op==no_denormalizegroup);

    LinkedHqlExpr dataset1 = expr->queryChild(0);
    LinkedHqlExpr dataset2 = queryJoinRhs(expr);
    IHqlExpression * condition = expr->queryChild(2);
    IHqlExpression * transform = expr->queryChild(3);
    IHqlExpression * noSortAttr = expr->queryProperty(noSortAtom);
    IHqlExpression * rowlimit = expr->queryProperty(rowLimitAtom);
    IHqlExpression * selSeq = querySelSeq(expr);
    bool isLeftOuter = false;
    bool isRightOuter = false;
    bool excludeMatches = false;
    bool isAllJoin = false;
    bool isLightweight = expr->hasProperty(_lightweight_Atom);
    bool isManyLookup = expr->hasProperty(manyAtom);

    if (expr->hasProperty(leftouterAtom))
        isLeftOuter = true;
    if (expr->hasProperty(rightouterAtom))
        isRightOuter = true;
    if (expr->hasProperty(fullouterAtom))
    {
        isLeftOuter = true;
        isRightOuter = true;
    }
    if (expr->hasProperty(leftonlyAtom))
    {
        isLeftOuter = true;
        excludeMatches = true;
    }
    if (expr->hasProperty(rightonlyAtom))
    {
        isRightOuter = true;
        excludeMatches = true;
    }
    if (expr->hasProperty(fullonlyAtom))
    {
        isLeftOuter = true;
        isRightOuter = true;
        excludeMatches = true;
    }
    if (expr->hasProperty(allAtom))
        isAllJoin = true;

    bool isLookupJoin = expr->hasProperty(lookupAtom);
    bool isHashJoin = targetThor() && expr->hasProperty(hashAtom);
    bool isLocalJoin = !isHashJoin && expr->hasProperty(localAtom);
    bool joinToSelf = (op == no_selfjoin);
    bool allowAllToLookupConvert = !options.noAllToLookupConversion;
    IHqlExpression * atmost = expr->queryProperty(atmostAtom);
    //Delay removing ungroups until this point because they can be useful for reducing the size of spill files.
    if (isUngroup(dataset1) && !isLookupJoin)
        dataset1.set(dataset1->queryChild(0));
    if (isUngroup(dataset2))
        dataset2.set(dataset2->queryChild(0));

    if (expr->hasProperty(groupedAtom) && targetThor())
        WARNING(HQLWRN_GroupedJoinIsLookupJoin);

    if ((op == no_denormalize || op == no_denormalizegroup) && targetThor() && options.checkThorRestrictions)
    {
        if (isHashJoin)
            throwError1(HQLERR_ThorDenormNoFeatureX, "HASH");
        if (expr->hasProperty(firstAtom))
            throwError1(HQLERR_ThorDenormNoFeatureX, "FIRST");
        if (expr->hasProperty(firstLeftAtom))
            throwError1(HQLERR_ThorDenormNoFeatureX, "FIRST LEFT");
        if (expr->hasProperty(firstRightAtom))
            throwError1(HQLERR_ThorDenormNoFeatureX, "FIRST RIGHT");
        if (expr->hasProperty(partitionRightAtom))
            throwError1(HQLERR_ThorDenormNoFeatureX, "PARTITION RIGHT");
    }


    OwnedHqlExpr atmostCond, atmostLimit;
    extractAtmostArgs(atmost, atmostCond, atmostLimit);

    HqlExprArray leftSorts, rightSorts, slidingMatches;
    bool slidingAllowed = options.slidingJoins && canBeSlidingJoin(expr);
    OwnedHqlExpr match;

    OwnedHqlExpr fuzzy, hard;
    bool isLimitedSubstringJoin;
    splitFuzzyCondition(condition, atmostCond, fuzzy, hard);
    match.setown(findJoinSortOrders(hard, dataset1, dataset2, selSeq, leftSorts, rightSorts, isLimitedSubstringJoin, slidingAllowed ? &slidingMatches : NULL));

    if (atmost && match)
    {
        if (isAllJoin)
            allowAllToLookupConvert = false;
        else
        {
            StringBuffer s;
            throwError1(HQLERR_BadJoinConditionAtMost,getExprECL(match, s.append(" (")).append(")").str());
        }
    }
    extendConditionOwn(match, no_and, fuzzy.getClear());

    if (isAllJoin)
    {
        if (leftSorts.ordinality() && allowAllToLookupConvert)
        {
            //Convert an all join to a many lookup if it can be done that way - more efficient, and same resourcing/semantics ...
            isManyLookup = true;
            isAllJoin = false;
            isLookupJoin = true;
        }
    }
    else if (leftSorts.ordinality() == 0)
    {
        if (expr->hasProperty(_conditionFolded_Atom))
        {
            isAllJoin = true;
            WARNING(HQLWRN_JoinConditionFoldedNowAll);
        }
        else
        {
            StringBuffer name;
            if (expr->queryName())
                name.append(" ").append(expr->queryName());
            throwError1(HQLERR_JoinXTooComplex, name.str());
        }
    }

    Owned<ABoundActivity> boundDataset1 = buildCachedActivity(ctx, dataset1);
    Owned<ABoundActivity> boundDataset2;
    if (!joinToSelf)
        boundDataset2.setown(buildCachedActivity(ctx, dataset2));

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
        if ((kind == TAKselfjoinlight) || (kind == TAKselfjoin))
            instance->graphLabel.set("Lightweight Self Join");
        else
            instance->graphLabel.set("Lightweight Join");
    }

    instance->setLocal(isLocalJoin);
    buildActivityFramework(instance);

    buildInstancePrefix(instance);
    StringBuffer s,temp;

    LinkedHqlExpr keepLimit = queryPropertyChild(expr, keepAtom, 0);
    DatasetReference lhsDsRef(dataset1, no_activetable, NULL);
    DatasetReference rhsDsRef(dataset2, no_activetable, NULL);

    bool couldBeKeepOne = keepLimit && (!keepLimit->queryValue() || (keepLimit->queryValue()->getIntValue() <= 1));
    if (dataset1->queryRecord() == dataset2->queryRecord())
    {
        //more could use the compareLeftRight function instead of generating the same code 
        //several time....
    }
    bool canReuseLeft = recordTypesMatch(dataset1, dataset2) && arraysMatch(leftSorts, rightSorts);
    if (!isAllJoin)
    {
        bool canRemoveSort = isLocalJoin || !targetThor();
        //Lookup join doesn't need the left sort (unless it is reused elsewhere), or the right sort unless it is deduping.
        if (canReuseLeft || !isLookupJoin)
            generateSortCompare(instance->nestedctx, instance->classctx, no_left, lhsDsRef, leftSorts, canRemoveSort, noSortAttr, false, isLightweight);
        if (!(isLookupJoin && isManyLookup && !couldBeKeepOne && !targetThor()))            // many lookup doesn't need to dedup the rhs
            generateSortCompare(instance->nestedctx, instance->classctx, no_right, rhsDsRef, rightSorts, canRemoveSort, noSortAttr, canReuseLeft, isLightweight);

        bool isGlobal = !isLocalJoin && !instance->isChildActivity();
        generateSerializeKey(instance->nestedctx, no_left, lhsDsRef, leftSorts, isGlobal, false, false);
        generateSerializeKey(instance->nestedctx, no_right, rhsDsRef, rightSorts, isGlobal, false, canReuseLeft);
    }

    StringBuffer flags;
    if (excludeMatches) flags.append("|JFexclude");
    if (isLeftOuter)    flags.append("|JFleftouter");
    if (isRightOuter)   flags.append("|JFrightouter");
    if (expr->hasProperty(firstAtom)) flags.append("|JFfirst");
    if (expr->hasProperty(firstLeftAtom)) flags.append("|JFfirstleft");
    if (expr->hasProperty(firstRightAtom)) flags.append("|JFfirstright");
    if (expr->hasProperty(partitionRightAtom)) flags.append("|JFpartitionright");
    if (expr->hasProperty(parallelAtom)) flags.append("|JFparallel");
    if (expr->hasProperty(sequentialAtom)) flags.append("|JFsequential");
    if (transformContainsSkip(transform))
        flags.append("|JFtransformMaySkip");
    if (rowlimit && rowlimit->hasProperty(skipAtom))
        flags.append("|JFmatchAbortLimitSkips");
    if (rowlimit && rowlimit->hasProperty(countAtom))
        flags.append("|JFcountmatchabortlimit");
    if (slidingMatches.ordinality()) flags.append("|JFslidingmatch");
    if (match) flags.append("|JFmatchrequired");
    if (isLookupJoin && isManyLookup) flags.append("|JFmanylookup");
    if (expr->hasProperty(onFailAtom))
        flags.append("|JFonfail");
    if (transformReturnsSide(expr, no_left, 0))
        flags.append("|JFtransformmatchesleft");
    if (isLimitedSubstringJoin)
        flags.append("|JFlimitedprefixjoin");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getJoinFlags", flags.str()+1);

    if (!isAllJoin)
    {
        buildSkewThresholdMembers(instance->classctx, expr);

        if (!isZero(atmostLimit))
            doBuildUnsignedFunction(instance->startctx, "getJoinLimit", atmostLimit);
    }

    if (keepLimit)
        doBuildUnsignedFunction(instance->startctx, "getKeepLimit", keepLimit);

    // The transform function is pretty standard - no need for copies here
    BuildCtx transformctx(instance->startctx);
    switch (op)
    {
    case no_denormalize:
        {
            transformctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned counter)");
            ensureRowAllocated(transformctx, "crSelf");

            IHqlExpression * counter = queryPropertyChild(expr, _countProject_Atom, 0);
            associateCounter(transformctx, counter, "counter");
            buildTransformBody(transformctx, transform, dataset1, dataset2, instance->dataset, selSeq);
            break;
        }
    case no_denormalizegroup:
        {
            transformctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned numRows, const void * * _rows)");
            ensureRowAllocated(transformctx, "crSelf");
            transformctx.addQuoted("unsigned char * * rows = (unsigned char * *) _rows;");


            BoundRow * selfCursor = buildTransformCursors(transformctx, transform, dataset1, dataset2, instance->dataset, selSeq);
            bindRows(transformctx, no_right, selSeq, expr->queryProperty(_rowsid_Atom), dataset2, "numRows", "rows", options.mainRowsAreLinkCounted);
            doBuildTransformBody(transformctx, transform, selfCursor);
            break;
        }
    case no_join:
    case no_selfjoin:
        {
            transformctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right)");
            ensureRowAllocated(transformctx, "crSelf");
            buildTransformBody(transformctx, transform, dataset1, dataset2, instance->dataset, selSeq);
            break;
        }
    }

    IHqlExpression * onFail = expr->queryProperty(onFailAtom);
    if (onFail)
    {
        BuildCtx funcctx(instance->startctx);
        funcctx.addQuotedCompound("virtual size32_t onFailTransform(ARowBuilder & crSelf, const void * _left, const void * _right, IException * except)");
        ensureRowAllocated(funcctx, "crSelf");
        associateLocalFailure(funcctx, "except");
        buildTransformBody(funcctx, onFail->queryChild(0), dataset1, dataset2, instance->dataset, selSeq);
    }

    // The collate function is used to work out which side to read from or if we have a potentially matching record
    if (!isAllJoin)
    {
        //if left and right match, then leftright compare function is also the same
        if (isLimitedSubstringJoin)
        {
            HqlExprArray compareLeftSorts, compareRightSorts;
            unsigned max = leftSorts.ordinality()-1;
            for (unsigned i=0; i < max; i++)
            {
                compareLeftSorts.append(OLINK(leftSorts.item(i)));
                compareRightSorts.append(OLINK(rightSorts.item(i)));
            }
            doCompareLeftRight(instance->nestedctx, "CompareLeftRight", lhsDsRef, rhsDsRef, compareLeftSorts, compareRightSorts);
        }
        else if (canReuseLeft)
            instance->nestedctx.addQuoted("virtual ICompare * queryCompareLeftRight() { return &compareLeft; }");
        else
            doCompareLeftRight(instance->nestedctx, "CompareLeftRight", lhsDsRef, rhsDsRef, leftSorts, rightSorts);
    }

    doBuildJoinRowLimitHelper(*instance, rowlimit, NULL, false);

    //--function to clear left, used for right outer join and vice-versa
    bool createDefaultRight = onFail || isLeftOuter;
    if (isRightOuter)
        buildClearRecordMember(instance->createctx, "Left", dataset1);
    if (createDefaultRight)
        buildClearRecordMember(instance->createctx, "Right", dataset2);
    buildJoinMatchFunction(instance->startctx, "match", dataset1, dataset2, match, selSeq);

    if (slidingMatches.ordinality())
    {
        buildSlidingMatchFunction(instance->nestedctx, leftSorts, rightSorts, slidingMatches, "CompareLeftRightLower", 1, lhsDsRef, rhsDsRef);
        buildSlidingMatchFunction(instance->nestedctx, leftSorts, rightSorts, slidingMatches, "CompareLeftRightUpper", 2, lhsDsRef, rhsDsRef);
    }

    if (isHashJoin||isLookupJoin)
    {
        OwnedHqlExpr leftList = createValueSafe(no_sortlist, makeSortListType(NULL), leftSorts);
        buildHashOfExprsClass(instance->nestedctx, "HashLeft", leftList, lhsDsRef, false);

        if (!canReuseLeft)
        {
            OwnedHqlExpr rightList = createValueSafe(no_sortlist, makeSortListType(NULL), rightSorts);
            buildHashOfExprsClass(instance->nestedctx, "HashRight", rightList, rhsDsRef, false);
        }
        else
            instance->nestedctx.addQuoted("virtual IHash * queryHashRight() { return &HashLeft; }");
    }

    if (isLimitedSubstringJoin)
    {
        OwnedHqlExpr leftSelect = createSelector(no_left, dataset1, selSeq);
        OwnedHqlExpr rightSelect = createSelector(no_right, dataset2, selSeq);
        HqlExprArray args;
        args.append(*lhsDsRef.mapCompound(&leftSorts.tos(), leftSelect));
        args.append(*rhsDsRef.mapCompound(&rightSorts.tos(), rightSelect));
        _ATOM func = prefixDiffStrAtom;
        OwnedHqlExpr compare = bindFunctionCall(func, args);
        
        buildCompareMemberLR(instance->nestedctx, "PrefixCompare", compare, dataset1, dataset2, selSeq);
    }


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

    assertex(recordTypesMatch(self->queryRecord(), transform->queryRecord()));

    if (left)
        ctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
    if (right)
        ctx.addQuoted("const unsigned char * right = (const unsigned char *) _right;");

    // Bind left to "left" and right to RIGHT
    BoundRow * leftRow = NULL;
    BoundRow * rightRow = NULL;
    if (left)
        leftRow = bindTableCursor(ctx, left, "left", no_left, selSeq);
    if (right)
        rightRow = bindTableCursor(ctx, right, "right", no_right, selSeq);

    if (options.precalculateFieldOffsets)
        precalculateFieldOffsets(ctx, transform, leftRow);
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
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned __int64 counter)");
    ensureRowAllocated(funcctx, "crSelf");
    associateCounter(funcctx, counter, "counter");
    buildTransformBody(funcctx, transform, dataset, dataset, dataset, selSeq);
}


void HqlCppTranslator::buildRollupTransformFunction(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * transform, IHqlExpression * selSeq)
{   
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right)");
    ensureRowAllocated(funcctx, "crSelf");
    buildTransformBody(funcctx, transform, dataset, dataset, dataset, selSeq);
}


ABoundActivity * HqlCppTranslator::doBuildActivityIterate(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * transform = expr->queryChild(1);
    IHqlExpression * counter = queryPropertyChild(expr, _countProject_Atom, 0);
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
    while (expr->getOperator() == no_alias_scope)
    {
        expandAliasScope(ctx, expr);
        expr = expr->queryChild(0);
    }
    return expr;
}


void HqlCppTranslator::buildProcessTransformFunction(BuildCtx & ctx, IHqlExpression * expr)
{   
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * right = expr->queryChild(1);
    IHqlExpression * transformRow = expr->queryChild(2);
    IHqlExpression * transformRight = expr->queryChild(3);
    IHqlExpression * counter = queryPropertyChild(expr, _countProject_Atom, 0);
    IHqlExpression * selSeq = querySelSeq(expr);

    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, ARowBuilder & crSelfRight, const void * _left, const void * _right, unsigned __int64 counter)");
    associateCounter(funcctx, counter, "counter");

    if ((transformRow->getOperator() == no_skip) || (transformRight->getOperator() == no_skip))
    {
        funcctx.addReturn(queryZero());
        return;
    }

    ensureRowAllocated(funcctx, "crSelf");
    ensureRowAllocated(funcctx, "crSelfRight");
    funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
    funcctx.addQuoted("const unsigned char * right = (const unsigned char *) _right;");
    bindTableCursor(funcctx, dataset, "left", no_left, selSeq);
    bindTableCursor(funcctx, right, "right", no_right, selSeq);

    LinkedHqlExpr skipReturnValue = queryZero();
    associateSkipReturnMarker(funcctx, skipReturnValue, NULL);
    if (!recordTypesMatch(dataset, right))
    {
        //self won't clash, so can generate efficient code.
        //Perform cse on both transforms
        OwnedHqlExpr comma = createComma(LINK(transformRow), LINK(transformRight));
        comma.setown(spotScalarCSE(comma));
        if (comma->getOperator() == no_alias_scope)
            comma.set(comma->queryChild(0));

        HqlExprArray unwound;
        comma->unwindList(unwound, no_comma);
        unsigned max = unwound.ordinality();

        BoundRow * selfCursor = bindSelf(funcctx, dataset, "crSelf");
        BoundRow * selfRowCursor = bindSelf(funcctx, right, "crSelfRight");

        for (unsigned i=0; i<max-2; i++)
            buildStmt(funcctx, &unwound.item(i));

        IHqlExpression * newTransformRow = queryExpandAliasScope(funcctx, &unwound.item(max-2));
        IHqlExpression * newTransformRight = queryExpandAliasScope(funcctx, &unwound.item(max-1));
        assertex(newTransformRow->getOperator() == no_transform && newTransformRight->getOperator() == no_transform);

        doTransform(funcctx, newTransformRow, selfCursor);
        doTransform(funcctx, newTransformRight, selfRowCursor);
        buildReturnRecordSize(funcctx, selfCursor);
    }
    else
    {
        BuildCtx ctx1(funcctx);

        ctx1.addGroup();
        BoundRow * selfRowCursor = bindSelf(ctx1, right, "crSelfRight");
        doTransform(ctx1, transformRight, selfRowCursor);

        BuildCtx ctx2(funcctx);
        ctx2.addGroup();
        BoundRow * selfCursor = bindSelf(ctx2, dataset, "crSelf");
        doTransform(ctx2, transformRow, selfCursor);

        buildReturnRecordSize(ctx2, selfCursor);
    }
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

    buildMetaMember(instance->classctx, right->queryRecord(), "queryRightRecordSize");

    {
        BuildCtx initialctx(instance->startctx);
        initialctx.addQuotedCompound("virtual size32_t createInitialRight(ARowBuilder & crSelf)");

        ensureRowAllocated(initialctx, "crSelf");
        BoundRow * cursor = bindSelf(initialctx, right, "crSelf");
        Owned<IReferenceSelector> createdRef = createReferenceSelector(cursor);
        buildRowAssign(initialctx, createdRef, right);
        buildReturnRecordSize(initialctx, cursor);
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
        instance->setImplementationClass(newSelectNArgAtom);
    buildActivityFramework(instance);

    if (matchesConstantValue(index, 1))
        instance->graphLabel.set("Select 1st");

    buildInstancePrefix(instance);

    if (!useImplementationClass)
    {
        BuildCtx funcctx(instance->startctx);
        funcctx.addQuotedCompound("virtual unsigned __int64 getRowToSelect()");
        buildReturn(funcctx, index);

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

void HqlCppTranslator::doBuildAggregateClearFunc(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * tgtRecord = expr->queryChild(1);
    IHqlExpression * transform = expr->queryChild(2);

    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual size32_t clearAggregate(ARowBuilder & crSelf)");

    OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(tgtRecord));
    BoundRow * selfRow = bindSelf(funcctx, resultDataset, "crSelf");

    if (!isKnownTransform(transform))
    {
        OwnedHqlExpr clearCall = createClearRowCall(funcctx, selfRow);
        funcctx.addReturn(clearCall);
        return;
    }

    ensureRowAllocated(funcctx, "crSelf");

    unsigned numAggregates = transform->numChildren();
    unsigned idx;
    OwnedHqlExpr self = getSelf(tgtRecord);
    for (idx = 0; idx < numAggregates; idx++)
    {
        IHqlExpression * cur = transform->queryChild(idx);
        OwnedHqlExpr target = selfRow->bindToRow(cur->queryChild(0), self);
        IHqlExpression * src = cur->queryChild(1);

        switch (src->getOperator())
        {
        case no_countgroup:
        case no_maxgroup:
        case no_mingroup:
        case no_sumgroup:
        case no_existsgroup:
            buildClear(funcctx, target);
            break;
        case no_notexistsgroup:
            buildAssign(funcctx, target, queryBoolExpr(true));
            break;
        default:
            if (src->isConstant())
                buildAssign(funcctx, target, src);
            else
                buildClear(funcctx, target);
            break;
        }
    }
    buildReturnRecordSize(funcctx, selfRow);
}


void HqlCppTranslator::doBuildAggregateFirstFunc(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * tgtRecord = expr->queryChild(1);
    IHqlExpression * transform = expr->queryChild(2);

    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual size32_t processFirst(ARowBuilder & crSelf, const void * _src)");
    ensureRowAllocated(funcctx, "crSelf");
    funcctx.addQuoted("unsigned char * src = (unsigned char *) _src;");

    //NOTE: no_throughaggregate recordof(expr) != tgtRecord => we need to create a temporary dataset
    OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(tgtRecord));
    BoundRow * selfRow = bindSelf(funcctx, resultDataset, "crSelf");
    bindTableCursor(funcctx, dataset, "src");

    doBuildAggregateProcessTransform(funcctx, selfRow, expr, queryBoolExpr(false));
    buildReturnRecordSize(funcctx, selfRow);
}

void HqlCppTranslator::doBuildAggregateNextFunc(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * tgtRecord = expr->queryChild(1);

    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual size32_t processNext(ARowBuilder & crSelf, const void * _src)");
    //no need ensureRowAllocated(funcctx, "crSelf");
    funcctx.addQuoted("unsigned char * src = (unsigned char *) _src;");

    OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(tgtRecord));
    BoundRow * selfRow = bindSelf(funcctx, resultDataset, "crSelf");
    bindTableCursor(funcctx, dataset, "src");

    doBuildAggregateProcessTransform(funcctx, selfRow, expr, queryBoolExpr(true));
    buildReturnRecordSize(funcctx, selfRow);
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
    OwnedHqlExpr self = getSelf(expr->queryChild(1));
    for (idx = 0; idx < numAggregates; idx++)
    {
        IHqlExpression * cur = transform->queryChild(idx);
        if (cur->isAttribute())
            continue;

        OwnedHqlExpr target = selfRow->bindToRow(cur->queryChild(0), self);

        IHqlExpression * src = cur->queryChild(1);
        IHqlExpression * arg = src->queryChild(0);
        IHqlExpression * cond = src->queryChild(1);

        BuildCtx condctx(ctx);
        node_operator srcOp = src->getOperator();
        switch (srcOp)
        {
        case no_countgroup:
            {
                assertex(!(arg && isVariableOffset));
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
                assertex(!(cond && isVariableOffset));
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
        case no_notexistsgroup:
            assertex(!(arg && isVariableOffset));
            cond = arg;
            if (cond || !alwaysNextRow)
            {
                //The assign is conditional because unconditionally it is done in the AggregateFirst
                if (cond)
                    buildFilter(condctx, cond);
                buildAssign(condctx, target, queryBoolExpr(srcOp == no_existsgroup));
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
        if (target->queryType()->getSize() == UNKNOWN_LENGTH)
           isVariableOffset = true;
    }
}

void HqlCppTranslator::doBuildAggregateMergeFunc(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * tgtRecord = expr->queryChild(1);
    IHqlExpression * transform = expr->queryChild(2);

    OwnedHqlExpr selSeq = createDummySelectorSequence();
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual size32_t mergeAggregate(ARowBuilder & crSelf, const void * _right)");
    //ensureRowAllocated(funcctx, "crSelf"); must be non null
    funcctx.addQuoted("unsigned char * right = (unsigned char *) _right;");

    OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(tgtRecord));
    BoundRow * selfRow = bindSelf(funcctx, resultDataset, "crSelf");
    BoundRow * leftCursor = bindTableCursor(funcctx, resultDataset, "left", no_left, selSeq);
    BoundRow * rightCursor = bindTableCursor(funcctx, resultDataset, "right", no_right, selSeq);

    unsigned numAggregates = transform->numChildren();
    unsigned idx;
    IHqlExpression * left = leftCursor->querySelector();
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
                buildIncrementAssign(funcctx, target, src);
            }
            break;
        case no_maxgroup:
            {
                OwnedHqlExpr compare = createBoolExpr (no_gt, LINK(src), LINK(target));
                BuildCtx filteredctx(funcctx);
                buildFilter(filteredctx, compare);
                buildAssign(filteredctx, target, src);
            }
            break;
        case no_mingroup:
            {
                OwnedHqlExpr compare = createBoolExpr (no_lt, LINK(src), LINK(target));
                BuildCtx filteredctx(funcctx);
                buildFilter(filteredctx, compare);
                buildAssign(filteredctx, target, src);
            }
            break;
        case no_existsgroup:
            {
                BuildCtx condctx(funcctx);
                buildFilter(condctx, src);
                buildAssign(condctx, target, queryBoolExpr(true));
                break;
            }
        case no_notexistsgroup:
            {
                BuildCtx condctx(funcctx);
                buildFilter(condctx, target);
                buildAssign(condctx, target, src);
            }
            break;
        default:
            //already filled in and wouldn't be legal to have an expression in this case anyway...
            break;
        }
    }
    buildReturnRecordSize(funcctx, selfRow);
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
        IHqlExpression * rowsid = expr->queryProperty(_rowsid_Atom);
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
        IHqlExpression * rowsid = expr->queryProperty(_rowsid_Atom);
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
    IHqlExpression * mergeTransform = queryPropertyChild(expr, mergeTransformAtom, 0);
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
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * tgtRecord = expr->queryChild(1);
    IHqlExpression * transform = expr->queryChild(2);
    IHqlExpression * selSeq = querySelSeq(expr);
    if (!mergeTransform)
        throwError(HQLERR_AggregateNeedMergeTransform);

    IHqlExpression * rowsid = expr->queryProperty(_rowsid_Atom);
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual size32_t mergeAggregate(ARowBuilder & upRight1, const void * _right2)");
    funcctx.addQuoted("unsigned char * right2 = (unsigned char *) _right2;");
    BoundRow * rightCursor = bindTableCursor(funcctx, expr, "upRight1.row()", no_right, selSeq);
    BoundRow * leftCursor = bindTableCursor(funcctx, expr, "right2", no_left, selSeq);
    BoundRow * selfCursor = bindSelf(funcctx, expr, "upRight1");

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
        buildTempExpr(funcctx, rightCursor->querySelector(), boundRow1);
        OwnedHqlExpr rows = createVariable("rows", makeArrayType(LINK(rowType), 2));
        OwnedHqlExpr initializer = createValue(no_list, makeSetType(LINK(rowType)), LINK(boundRow1.expr), LINK(leftBound));
        funcctx.addDeclare(rows, initializer);
        bindRows(funcctx, no_right, selSeq, rowsid, expr, "2", "rows", options.mainRowsAreLinkCounted);
    }

    doUpdateTransform(funcctx, mappedTransform3, selfCursor, rightCursor, true);
    buildReturnRecordSize(funcctx, selfCursor);
}

void HqlCppTranslator::doBuildUserAggregateFuncs(BuildCtx & ctx, IHqlExpression * expr, bool & requiresOrderedMerge)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * tgtRecord = expr->queryChild(1);
    IHqlExpression * transform = expr->queryChild(2);
    IHqlExpression * selSeq = querySelSeq(expr);
    LinkedHqlExpr firstTransform;
    LinkedHqlExpr nextTransform;

    processUserAggregateTransform(expr, transform, firstTransform, nextTransform);

    {
        BuildCtx funcctx(ctx);
        funcctx.addQuotedCompound("virtual size32_t processFirst(ARowBuilder & crSelf, const void * _src)");
        ensureRowAllocated(funcctx, "crSelf");
        funcctx.addQuoted("unsigned char * src = (unsigned char *) _src;");

        BoundRow * selfRow = bindSelf(funcctx, expr, "crSelf");
        bindTableCursor(funcctx, dataset, "src", options.mainRowsAreLinkCounted, no_left, selSeq);

        doBuildUserAggregateProcessTransform(funcctx, selfRow, expr, firstTransform, queryBoolExpr(false));
        buildReturnRecordSize(funcctx, selfRow);
    }

    {
        BuildCtx funcctx(ctx);
        funcctx.addQuotedCompound("virtual size32_t processNext(ARowBuilder & crSelf, const void * _src)");
        ensureRowAllocated(funcctx, "crSelf");
        funcctx.addQuoted("unsigned char * src = (unsigned char *) _src;");

        BoundRow * leftCursor = bindTableCursor(funcctx, dataset, "src", options.mainRowsAreLinkCounted, no_left, selSeq);
        BoundRow * selfCursor = bindSelf(funcctx, expr, "crSelf");
        BoundRow * rightCursor = bindTableCursor(funcctx, expr, "crSelf.row()", no_right, querySelSeq(expr));

        doBuildUserAggregateProcessTransform(funcctx, selfCursor, expr, nextTransform, queryBoolExpr(true));
        buildReturnRecordSize(funcctx, selfCursor);
    }

    if (targetThor() && !isGrouped(dataset) && !expr->hasProperty(localAtom))
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
        }
        else if (specialOp == no_countgroup)
        {
            kind = TAKcountaggregate;
            activity = "CountAggregate";
        }
        else
            specialOp = no_none;
    }


    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, activity);
    if (passThrough)
    {
        StringBuffer graphLabel;
        graphLabel.append("Through Aggregate");
        ForEachChild(idx, expr)
        {
            IHqlExpression * cur = expr->queryChild(idx);
            if (cur->getOperator() == no_setresult || cur->getOperator() == no_extractresult)
            {
                IHqlExpression * sequence = queryPropertyChild(cur, sequenceAtom, 0);
                IHqlExpression * name = queryPropertyChild(cur, namedAtom, 0);
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
    if (specialOp == no_none)
    {
        doBuildAggregateClearFunc(instance->startctx, expr);
        if (op == no_aggregate)
        {
            bool requiresOrderedMerge = false;
            doBuildUserAggregateFuncs(instance->startctx, expr, requiresOrderedMerge);
            if (requiresOrderedMerge)
                flags.append("|TAForderedmerge");
        }
        else        
        {
            doBuildAggregateFirstFunc(instance->startctx, expr);
            doBuildAggregateNextFunc(instance->startctx, expr);

            if (targetThor() && !isGrouped(dataset) && !expr->hasProperty(localAtom))
                doBuildAggregateMergeFunc(instance->startctx, expr);
        }
    }

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
        BuildCtx sendctx(instance->startctx);
        sendctx.addQuotedCompound("virtual void sendResult(const void * _self)");
        sendctx.addQuoted("const unsigned char * self = (const unsigned char *)_self;");

        OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(expr->queryChild(1)), NULL);
        bindTableCursor(sendctx, resultDataset, "self");
        bindTableCursor(sendctx, dataset, "self");
        ForEachChild(idx, expr)
        {
            IHqlExpression * cur = expr->queryChild(idx);
            if (cur->getOperator() == no_setresult)
            {
                IHqlExpression * value = cur->queryChild(0);
                IHqlExpression * sequence = queryPropertyChild(cur, sequenceAtom, 0);
                IHqlExpression * name = queryPropertyChild(cur, namedAtom, 0);
                buildSetResultInfo(sendctx, cur, value, NULL, false, false);
                associateRemoteResult(*instance, sequence, name);
            }
            else if (cur->getOperator() == no_extractresult)
            {
                IHqlExpression * value = cur->queryChild(1);
                IHqlExpression * sequence = queryPropertyChild(cur, sequenceAtom, 0);
                IHqlExpression * name = queryPropertyChild(cur, namedAtom, 0);
                buildSetResultInfo(sendctx, cur, value, NULL, false, false);
                associateRemoteResult(*instance, sequence, name);
            }
        }
        buildMetaMember(instance->classctx, resultDataset, "queryAggregateRecordSize");
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityChildDataset(BuildCtx & ctx, IHqlExpression * expr)
{
    if ((options.mainRowsAreLinkCounted && options.supportsLinkedChildRows && options.useLinkedRawIterator) || isGrouped(expr))
        return doBuildActivityRawChildDataset(ctx, expr);


    StringBuffer s;

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKchilditerator, expr, "ChildIterator");
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
            doBuildAliasValue(instance->onstartctx, value, bound);
            value.setown(bound.getTranslatedExpr());
            break;
        }
    }

    Owned<IHqlCppDatasetCursor> iter = createDatasetSelector(instance->onstartctx, value);
    iter->buildIterateMembers(instance->startctx, instance->onstartctx);

    //virtual size32_t transform(ARowBuilder & crSelf) = 0;
    BuildCtx transformCtx(instance->startctx);
    transformCtx.addQuotedCompound("size32_t transform(ARowBuilder & crSelf)");
    ensureRowAllocated(transformCtx, "crSelf");
    BoundRow * selfCursor = bindSelf(transformCtx, expr, "crSelf");
    OwnedHqlExpr active = ensureActiveRow(value);
    buildAssign(transformCtx, selfCursor->querySelector(), active);
    buildReturnRecordSize(transformCtx, selfCursor);

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityStreamedCall(BuildCtx & ctx, IHqlExpression * expr)
{
    StringBuffer s;

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKstreamediterator, expr, "StreamedIterator");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    BuildCtx createctx(instance->startctx);
    createctx.addQuotedCompound("virtual IRowStream * createInput()");
    CHqlBoundExpr bound;
    doBuildExprCall(createctx, expr, bound);
    createctx.addReturn(bound.expr);

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityLinkedRawChildDataset(BuildCtx & ctx, IHqlExpression * expr)
{
    StringBuffer s;

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKlinkedrawiterator, expr, "LinkedRawIterator");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    OwnedHqlExpr value = expr->isDatarow() ? createDatasetFromRow(LINK(expr)) : LINK(expr);
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
    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual byte * next()");
    OwnedHqlExpr count = getBoundCount(boundDs);
    OwnedHqlExpr test = createValue(no_lt, makeBoolType(), LINK(boundActiveIndex.expr), LINK(count));
    BuildCtx subctx(funcctx);
    subctx.addFilter(test);
    OwnedHqlExpr ret = createValue(no_index, expr->getType(), LINK(boundDs.expr), createValue(no_postinc, LINK(sizetType), LINK(boundActiveIndex.expr)));
    subctx.addReturn(ret);
    funcctx.addReturn(queryQuotedNullExpr());

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityRawChildDataset(BuildCtx & ctx, IHqlExpression * expr)
{
    //If it is possible to create a linked child rows we need to use a linked raw child dataset iterator - 
    //otherwise the dataset may not be in the right format (and I can't work out how to efficiently force it otherwise.)
    //If main rows are link counted then it is always going to be as good or better to generate a link counted raw iterator.
    if (options.implicitLinkedChildRows || options.tempDatasetsUseLinkedRows)
        return doBuildActivityLinkedRawChildDataset(ctx, expr);
    
    StringBuffer s;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKrawiterator, expr, "RawIterator");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    OwnedHqlExpr value = LINK(expr);
    switch (value->getOperator())
    {
    case no_alias:
    case no_left:
    case no_right:
    case no_id2blob:
    case no_rows:
        break;
    case no_select:
        if (!isNewSelector(expr))
            break;
        //fall through
    default:
        {
            CHqlBoundExpr bound;
            doBuildAliasValue(instance->onstartctx, value, bound);
            value.setown(bound.getTranslatedExpr());
            break;
        }
    }


    //virtual void queryDataset(size_t & len, const void * & data)
    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual void queryDataset(size32_t & len, const void * & data)");
    CHqlBoundExpr boundValue;
    buildExpr(funcctx, value, boundValue);

    OwnedHqlExpr len = getBoundLength(boundValue);
    generateExprCpp(s.clear().append("len = "), len).append(";");
    funcctx.addQuoted(s);
    OwnedHqlExpr addr = getPointer(boundValue.expr);
    generateExprCpp(s.clear().append("data = "), addr).append(";");
    funcctx.addQuoted(s);

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
    OwnedHqlExpr equalExpr = createValue(no_sortlist, makeSortListType(NULL), exprs);
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

    BuildCtx funcctx(ctx);

    IHqlStmt * functionStmt = funcctx.addQuotedCompound("virtual bool matches(const void * _left, const void * _right)");
    funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
    funcctx.addQuoted("const unsigned char * right = (const unsigned char *) _right;");

    BuildCtx filterctx(funcctx);

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
        doBuildReturnCompare(filterctx, order, no_eq, true);
    }
    else if (comparisons.ordinality() == 0)
        functionStmt->setIncluded(false);           // Use the implementation in the base class
}

void HqlCppTranslator::buildDedupSerializeFunction(BuildCtx & ctx, const char * funcName, IHqlExpression * srcDataset, IHqlExpression * tgtDataset, HqlExprArray & srcValues, HqlExprArray & tgtValues, IHqlExpression * selSeq)
{
    StringBuffer s;

    BuildCtx r2kctx(ctx);
    s.append("virtual unsigned ").append(funcName).append("(ARowBuilder & crSelf, const void * _src)");
    r2kctx.addQuotedCompound(s);
    ensureRowAllocated(r2kctx, "crSelf");
    r2kctx.addQuoted("const unsigned char * src = (const unsigned char *) _src;");

    BoundRow * tgtCursor = bindSelf(ctx, tgtDataset, "crSelf");
    BoundRow * srcCursor = bindTableCursor(ctx, srcDataset, "src", no_left, selSeq);
    ForEachItemIn(idx2, srcValues)
    {
        Owned<IHqlExpression> self = tgtCursor->bindToRow(&tgtValues.item(idx2), queryActiveTableSelector());
        Owned<IHqlExpression> left = srcCursor->bindToRow(&srcValues.item(idx2), srcDataset);
        buildAssign(r2kctx, self, left);
    }

    buildReturnRecordSize(r2kctx, tgtCursor);
}

ABoundActivity * HqlCppTranslator::doBuildActivityDedup(BuildCtx & ctx, IHqlExpression * expr)
{
    StringBuffer s;
    DedupInfoExtractor info(expr);

    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * selSeq = querySelSeq(expr);
    bool isGrouped = (dataset->queryType()->queryGroupInfo() != NULL);
    bool isLocal = isLocalActivity(expr);
    bool useHash = expr->hasProperty(hashAtom);
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

    if (!useHash)
    {
        if (info.compareAllRows)
            doBuildBoolFunction(instance->classctx, "compareAll", info.compareAllRows);
        if (!info.keepLeft)
            doBuildBoolFunction(instance->classctx, "keepLeft", info.keepLeft);
        if (!matchesConstantValue(info.numToKeep, 1))
            doBuildUnsignedFunction(instance->startctx, "numToKeep", info.numToKeep);

        //MORE: If input is grouped (pretty likely), then no need to include fields in the filter function that are already included.
        if (instance->isGrouped)
        {
            HqlExprArray normalizedEqualities;
            ForEachItemIn(i1, info.equalities)
                normalizedEqualities.append(*replaceSelector(info.equalities.item(i1).queryBody(), dataset, queryActiveTableSelector()));

            IHqlExpression * grouping = static_cast<IHqlExpression *>(dataset->queryType()->queryGroupInfo());
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
                OwnedHqlExpr order = createValue(no_sortlist, makeSortListType(NULL), info.equalities);
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
                field = createField(createIdentifierAtom(name.str()), cur.getType(), NULL);
            }
            fields.append(*field);
            selects.append(*createSelectExpr(getActiveTableSelector(), LINK(field)));
        }

        OwnedHqlExpr keyDataset = createDataset(no_anon, createRecordInheritMaxLength(fields, dataset));

        //virtual IOutputMetaData * queryKeySize()
        buildMetaMember(instance->classctx, keyDataset, "queryKeySize");

        //virtual unsigned recordToKey(void * _key, const void * _record)
        buildDedupSerializeFunction(instance->startctx, "recordToKey", dataset, keyDataset, info.equalities, selects, selSeq);

        //virtual ICompare * queryKeyCompare()
        bool reuseCompare = false;
        OwnedHqlExpr keyOrder = createValueSafe(no_sortlist, makeSortListType(NULL), selects);
        if (recordTypesMatch(dataset, keyDataset))
        {
            OwnedHqlExpr globalOrder = replaceSelector(order, dataset, queryActiveTableSelector());
            if (keyOrder == globalOrder)
                reuseCompare = true;
        }

        if (!reuseCompare)
            buildCompareMember(instance->nestedctx, "KeyCompare", keyOrder, DatasetReference(keyDataset, no_activetable, NULL));
        else
            instance->nestedctx.addQuoted("virtual ICompare * queryKeyCompare() { return &Compare; }");
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------
// no_distribute

ABoundActivity * HqlCppTranslator::doBuildActivityDistribute(BuildCtx & ctx, IHqlExpression * expr)
{
    if (!targetThor())
        return buildCachedActivity(ctx, expr->queryChild(0));

    StringBuffer s;

    IHqlExpression * dataset = expr->queryChild(0);
    if (isUngroup(dataset))
        dataset = dataset->queryChild(0);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    IHqlExpression * cond = expr->queryChild(1);
    IHqlExpression * mergeOrder = queryPropertyChild(expr, mergeAtom, 0);
    if (cond->getOperator() == no_sortpartition)
    {
        Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKpartition, expr, "Merge");
        buildActivityFramework(instance);

        buildInstancePrefix(instance);

        HqlExprArray sorts;
        unwindChildren(sorts, cond);

        OwnedHqlExpr sortOrder = createValueSafe(no_sortlist, makeSortListType(NULL), sorts);
        instance->startctx.addQuoted("virtual ICompare * queryCompare() { return &compare; }");

        DatasetReference dsRef(dataset);
        buildCompareClass(instance->nestedctx, "compare", sortOrder, dsRef);
        if (!instance->isLocal)
            generateSerializeKey(instance->nestedctx, no_none, dsRef, sorts, true, true, false);

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
        if (expr->hasProperty(skewAtom))
            instance->graphLabel.set("Skew Distribute");
        buildActivityFramework(instance);

        buildInstancePrefix(instance);

        if (!expr->hasProperty(skewAtom))
            buildHashClass(instance->nestedctx, "Hash", cond, DatasetReference(dataset));
        doBuildBoolFunction(instance->classctx, "isPulled", expr->hasProperty(pulledAtom));
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

    buildDedupFilterFunction(instance->startctx, equalities, conds, dataset, selSeq);
    buildRollupTransformFunction(instance->startctx, dataset, transform, selSeq);
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

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual __int64 getLimit()");
    OwnedHqlExpr newLimit = ensurePositiveOrZeroInt64(limit);
    if (options.spotCSE)
        newLimit.setown(spotScalarCSE(newLimit));
    buildReturn(funcctx, newLimit);

    if (queryRealChild(expr, 2))
    {
        OwnedHqlExpr adjusted = adjustValue(expr->queryChild(2), -1);
        OwnedHqlExpr newAdjusted = ensurePositiveOrZeroInt64(adjusted);
        BuildCtx funcctx(instance->startctx);
        funcctx.addQuotedCompound("virtual __int64 numToSkip()");
        buildReturn(funcctx, newAdjusted);
    }

    if (expr->hasProperty(groupedAtom))
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
    bool isEnth = expr->hasProperty(enthAtom);

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
    BuildCtx funcctx(instance->classctx);
    funcctx.addQuotedCompound("virtual unsigned getNumSets()");
    OwnedHqlExpr numExpr = createConstant((int)numCategories);
    buildReturn(funcctx, numExpr, unsignedType);

    //virtual unsigned getRecordCategory(const void * _self) = 0;
    BuildCtx categoryctx(instance->startctx);
    categoryctx.addQuotedCompound("virtual unsigned getCategory(const void * _self)");
    categoryctx.addQuoted("const unsigned char * self = (const unsigned char *)_self;");
    bindTableCursor(categoryctx, dataset, "self");
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
    buildReturn(categoryctx, map);

    //virtual void getLimits(unsigned * counts) = 0;
    BuildCtx limitctx(instance->startctx);
    StringBuffer s;
    limitctx.addQuotedCompound("virtual void getLimits(__int64 * counts)");
    for (unsigned idx2 = 1; idx2 <= numCategories; idx2++)
    {
        IHqlExpression * cur = expr->queryChild(idx2);
        s.clear().append("counts[").append(idx2-1).append("]");
        OwnedHqlExpr target = createVariable(s.str(), LINK(defaultIntegralType));

        switch (cur->getOperator())
        {
        case no_mapto:
            buildAssignToTemp(limitctx, target, cur->queryChild(1));
            break;
        default:
            buildAssignToTemp(limitctx, target, cur);
            break;
        }
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityChooseSets(BuildCtx & ctx, IHqlExpression * expr)
{
    if (expr->hasProperty(enthAtom) || expr->hasProperty(lastAtom))
        return doBuildActivityChooseSetsEx(ctx, expr);

    IHqlExpression * dataset = expr->queryChild(0);
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKchoosesets, expr, "ChooseSets");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    unsigned numArgs = expr->numChildren();
    bool allowSpill = !expr->hasProperty(exclusiveAtom);
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

    BuildCtx funcctx(instance->classctx);
    funcctx.addQuotedCompound("virtual unsigned getNumSets()");
    OwnedHqlExpr numExpr = createConstant((int)numCategories);
    buildReturn(funcctx, numExpr, unsignedType);

    StringBuffer s;
    BuildCtx limitctx(instance->classctx);
    instance->startctx.addQuoted("unsigned * counts;");
    instance->startctx.addQuoted("unsigned numFull;");
    limitctx.addQuotedCompound("virtual bool setCounts(unsigned * data)");
    limitctx.addQuoted("counts = data;");
    limitctx.addQuoted("numFull = 0;");

    OwnedHqlExpr tally = createVariable("counts", makeIntType(4, false));
    BuildCtx validctx(instance->startctx);
    validctx.addQuotedCompound("virtual unsigned getRecordAction(const void * _self)");
    validctx.addQuoted("const unsigned char * self = (const unsigned char *)_self;");
    bindTableCursor(validctx, dataset, "self");

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

        BuildCtx condctx(validctx);
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

        BuildCtx limitCondCtx(limitctx);
        buildFilter(limitCondCtx, condDone);
        limitCondCtx.addQuoted("numFull++;");
    }
    buildReturn(validctx, queryZero());
    limitctx.addQuoted(s.clear().append("return numFull == ").append(numCategories).append(";"));

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

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual unsigned numExpandedRows(const void * _left)");
    funcctx.addQuoted("unsigned char * left = (unsigned char *) _left;");

    bindTableCursor(funcctx, dataset, "left", no_left, selSeq);
    bindTableCursor(funcctx, dataset, "left");
    buildReturn(funcctx, numRows);

    BuildCtx transformctx(instance->startctx);
    transformctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, unsigned counter)");
    ensureRowAllocated(transformctx, "crSelf");

    IHqlExpression * counter = queryPropertyChild(expr, _countProject_Atom, 0);
    associateCounter(transformctx, counter, "counter");
    buildTransformBody(transformctx, transform, dataset, NULL, instance->dataset, selSeq);

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

    if (options.useLinkedNormalize)
    {
        if (transformReturnsSide(expr, no_right, 1))
            return doBuildActivityNormalizeLinkedChild(ctx, expr);
    }

    StringBuffer s;
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKnormalizechild, expr,"NormalizeChild");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    //Generate queryChildRecordSize();
    buildMetaMember(instance->classctx, childDataset, "queryChildRecordSize");

    // INormalizeChildIterator * queryIterator();
    { 
        bool outOfLine = options.tempDatasetsUseLinkedRows;
        if (childDataset->isDatarow())
            childDataset.setown(createDatasetFromRow(childDataset.getClear()));
        if (childDataset->getOperator() == no_select)
            outOfLine = isArrayRowset(childDataset->queryType());
        if (hasLinkCountedModifier(childDataset))
            outOfLine = true;

        BuildCtx iterclassctx(instance->nestedctx);
        StringBuffer memberName, className;
        getUniqueId(memberName.append("m"));
        getMemberClassName(className, memberName.str());

        ExpressionFormat format;
        if (outOfLine)
        {
            beginNestedClass(iterclassctx, memberName, "CNormalizeLinkedChildIterator");
            format = FormatLinkedDataset;
        }
        else
        {
            beginNestedClass(iterclassctx, memberName, "CNormalizeChildIterator");
            format = FormatBlockedDataset;

            MetaInstance childmeta(*this, childDataset);
            buildMetaInfo(childmeta);
            s.clear().append(className).append("() : CNormalizeChildIterator(").append(childmeta.queryInstanceObject()).append(") {}");
            iterclassctx.addQuoted(s);
        }

        bool callFromActivity = false;
        BuildCtx activityinitctx(instance->startctx);
        BuildCtx funcctx(iterclassctx);
        funcctx.addQuotedCompound("virtual void init(const void * _left)");
        funcctx.addQuoted("const byte * left = (const byte *)_left;");


        CHqlBoundExpr bound;
        if (childDataset->getOperator() != no_select)
        {
            //Ugly......
            //If this is a complex expression, then ensure the temporary variable is a member of the activity class, and 
            //evaluate it in the function defined inside the activity (so the member variables don't need mangling)
            funcctx.addQuoted("activity->init(left);");

            BuildCtx * declarectx = NULL;
            instance->evalContext->getInvariantMemberContext(NULL, &declarectx, NULL, false, true);
            queryEvalContext(iterclassctx)->ensureHelpersExist();
            assertex(declarectx);

            activityinitctx.addQuotedCompound("void init(const byte * left)");
            bindTableCursor(activityinitctx, dataset, "left", no_left, selSeq);

            CHqlBoundTarget tempTarget;
            buildTempExpr(activityinitctx, *declarectx, tempTarget, childDataset, format, false);
            bound.setFromTarget(tempTarget);

            callFromActivity = true;
        }
        else
        {
            bindTableCursor(funcctx, dataset, "left", no_left, selSeq); 
            buildDataset(funcctx, childDataset, bound, format);
        }

        s.clear();
        if (callFromActivity)
            s.append(memberName).append(".");
        s.append("setDataset(");
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
        if (callFromActivity)
            activityinitctx.addQuoted(s);
        else
            funcctx.addQuoted(s);

        endNestedClass();

        s.clear().append("INormalizeChildIterator * queryIterator() { return &").append(memberName).append("; }");
        instance->startctx.addQuoted(s);
    }

    BuildCtx transformctx(instance->startctx);
    transformctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned counter)");
    ensureRowAllocated(transformctx, "crSelf");

    IHqlExpression * counter = queryPropertyChild(expr, _countProject_Atom, 0);
    associateCounter(transformctx, counter, "counter");
    buildTransformBody(transformctx, transform, dataset, childDataset, instance->dataset, selSeq);

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

    CHqlBoundTarget childTarget;
    CHqlBoundExpr boundChild;
    CHqlBoundTarget boundActive;
    createTempFor(*declarectx, value->queryType(), childTarget, typemod_none, FormatLinkedDataset);
    boundChild.setFromTarget(childTarget);
    assertex(boundChild.count != NULL);

    OwnedHqlExpr test;
    OwnedHqlExpr ret;
    //virtual byte * first(const void * parentRecord) = 0;
    {
        BuildCtx firstctx(instance->startctx);
        firstctx.addQuotedCompound("virtual byte * first(const void * parentRecord)");
        firstctx.addQuoted("const byte * left = (const byte *)parentRecord;");
        bindTableCursor(firstctx, dataset, "left", selectorOp, selSeq);
        buildDatasetAssign(firstctx, childTarget, value);

        OwnedHqlExpr zero = getSizetConstant(0);
        buildTempExpr(firstctx, *declarectx, boundActive, zero, FormatNatural, false);

        test.setown(createValue(no_lt, makeBoolType(), LINK(boundActive.expr), LINK(boundChild.count)));
        ret.setown(createValue(no_index, expr->getType(), LINK(boundChild.expr), LINK(boundActive.expr)));

        BuildCtx subctx(firstctx);
        subctx.addFilter(test);
        subctx.addReturn(ret);
        firstctx.addReturn(queryQuotedNullExpr());
    }
    
    {
        //virtual byte * next() = 0;
        BuildCtx nextctx(instance->startctx);
        nextctx.addQuotedCompound("virtual byte * next()");
        OwnedHqlExpr inc = createValue(no_postinc, LINK(sizetType), LINK(boundActive.expr));
        nextctx.addExpr(inc);
        BuildCtx subctx(nextctx);
        subctx.addFilter(test);
        subctx.addReturn(ret);
        nextctx.addReturn(queryQuotedNullExpr());
    }

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);


    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivitySelectNew(BuildCtx & ctx, IHqlExpression * expr)
{
    if (options.useLinkedNormalize)
        return doBuildActivityNormalizeLinkedChild(ctx, expr);

    IHqlExpression * ds = expr->queryChild(0);
//  if (!isContextDependent(ds) && !containsActiveDataset(ds))
//      return doBuildActivityChildDataset(ctx, expr);
    if (canEvaluateInline(&ctx, ds))
        return doBuildActivityChildDataset(ctx, expr);

    //Convert the ds.x to normalize(ds, left.x, transform(right));
    IHqlExpression * field = expr->queryChild(1); 
    OwnedHqlExpr selSeq = createDummySelectorSequence();
    OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
    OwnedHqlExpr selector = createSelectExpr(LINK(left), LINK(field));
    OwnedHqlExpr transformedExpr;
    if (field->isDatarow())
    {
        OwnedHqlExpr transform = createTransformFromRow(selector);
        transformedExpr.setown(createDatasetF(no_hqlproject, LINK(ds), LINK(transform), LINK(selSeq), NULL));       // MORE: UID
    }
    else
    {
        OwnedHqlExpr right = createSelector(no_right, expr, selSeq);
        OwnedHqlExpr transform = createTransformFromRow(right);
        transformedExpr.setown(createDatasetF(no_normalize, LINK(ds), LINK(selector), LINK(transform), LINK(selSeq), NULL));        // MORE: UID
    }
    return buildActivity(ctx, transformedExpr, false);
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityPrefetchProject(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * transform = queryNewColumnProvider(expr);
    IHqlExpression * counter = queryPropertyChild(expr, _countProject_Atom, 0);
    IHqlExpression * selSeq = querySelSeq(expr);
    IHqlExpression * prefetch = expr->queryProperty(prefetchAtom);
    IHqlExpression * lookahead = queryPropertyChild(expr, prefetchAtom, 0);
    IHqlExpression * record = expr->queryRecord();
#ifdef _DEBUG
    assertex((counter != NULL) == transformContainsCounter(transform, counter));
#endif

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, counter ? TAKprefetchcountproject : TAKprefetchproject, expr, "PrefetchProject");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    StringBuffer flags;
    if (prefetch && prefetch->hasProperty(parallelAtom)) flags.append("|PPFparallel");
    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    if (transformContainsSkip(transform))
        doBuildBoolFunction(instance->classctx, "canFilter", true);
    if (lookahead)
        doBuildUnsignedFunction(instance->startctx, "getLookahead", lookahead);

    //Similar code to project below.  First generate the post processing function (which all aliases etc. will get generated into)
    BuildCtx postctx(instance->startctx);
    postctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, IEclGraphResults * results, unsigned __int64 _counter)");
    ensureRowAllocated(postctx, "crSelf");
    postctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
    if (expr->getOperator() == no_hqlproject)
        bindTableCursor(postctx, dataset, "left", no_left, selSeq);
    else
        bindTableCursor(postctx, dataset, "left");
    BoundRow * selfCursor = bindSelf(postctx, expr, "crSelf");
    associateSkipReturnMarker(postctx, queryZero(), selfCursor);

    if (counter)
        associateCounter(postctx, counter, "counter");

    //Now process the transform
    HqlExprArray assigns;

    //Introduce a scope to ensure that mapper and builder have the minimum lifetime.
    {
        //Possibly cleaner if this was implemented inside a class derived from TransformBuilder
        TransformBuilder builder(*this, postctx, record, selfCursor, assigns);
        filterExpandAssignments(postctx, &builder, assigns, transform);
        builder.buildTransformChildren(postctx, record, selfCursor->querySelector());
        if (builder.hasGraphPending())
        {
            //Generate the extract preparation function
            BuildCtx prectx(instance->startctx);
            IHqlStmt * preStmt = prectx.addQuotedCompound("virtual bool preTransform(rtlRowBuilder & builder, const void * _left, unsigned __int64 _counter)");
            associateSkipReturnMarker(prectx, queryBoolExpr(false), NULL);
            prectx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
            if (expr->getOperator() == no_hqlproject)
                bindTableCursor(prectx, dataset, "left", no_left, selSeq);
            else
                bindTableCursor(prectx, dataset, "left");
            if (counter)
                associateCounter(prectx, counter, "counter");
        
            OwnedHqlExpr graph, results;
            builder.generatePrefetch(prectx, &graph, &results);
            prectx.addReturn(queryBoolExpr(true));

            BuildCtx childctx(instance->startctx);
            childctx.addQuotedCompound("virtual IThorChildGraph *queryChild()");
            childctx.addReturn(graph);

            //hack!  I need to change the way results are bound so they aren't nesc. the same as the query name
            StringBuffer s;
            s.append("IEclGraphResults * ");
            generateExprCpp(s, results);
            s.append(" = results;");
            postctx.addQuoted(s);
            
            //Add an association for the results into the transform function.
            postctx.associateExpr(results, results);
        }
        builder.flush(postctx);
    }

    buildReturnRecordSize(postctx, selfCursor);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityProject(BuildCtx & ctx, IHqlExpression * expr)
{
    if (expr->hasProperty(prefetchAtom) || options.usePrefetchForAllProjects)
        return doBuildActivityPrefetchProject(ctx, expr);

    const node_operator op = expr->getOperator();
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * normalized = dataset->queryNormalizedSelector();
    IHqlExpression * counter = queryPropertyChild(expr, _countProject_Atom, 0);
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
                    OwnedHqlExpr cond = extractFilterConditions(invariant, dataset, normalized, false);
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
    bool containsCounter = expr->hasProperty(_countProject_Atom);
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

    BuildCtx funcctx(instance->startctx);
    if (isFilterProject || containsCounter)
    {
        funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, unsigned __int64 counter)");
        if (containsCounter)
            associateCounter(funcctx, counter, "counter");
    }
    else
        funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left)");

    ensureRowAllocated(funcctx, "crSelf");
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
        funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");

        BoundRow * selfCursor = bindSelf(funcctx, expr, "crSelf");
        associateSkipReturnMarker(funcctx, queryZero(), selfCursor);
        bindTableCursor(funcctx, dataset, "left");

        doTransform(funcctx, transform, selfCursor);
        buildReturnRecordSize(funcctx, selfCursor);
    }
    else
        buildTransformBody(funcctx, transform, dataset, NULL, instance->dataset, selSeq);

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
    assertex(serialize);//Needs changing once interface has changed.

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKproject, expr, "Project");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left)");

    assertex(!options.limitMaxLength);      //need more thought


    // Bind left to "left" and right to RIGHT
    BoundRow * leftCursor = bindTableCursor(funcctx, dataset, "_left");
    BoundRow * selfCursor = bindSelf(funcctx, expr, "crSelf");

    //MORE: I don't have any examples that trigger this code as far as I know...
    _ATOM func = serialize ? rtlSerializeToBuilderAtom : rtlDeserializeToBuilderAtom;
    _ATOM kind = serialize ? serializerAtom : deserializerAtom;

    IHqlExpression * record = expr->queryRecord();
    HqlExprArray args;
    args.append(*createRowSerializer(ctx, record, kind));
    args.append(*ensureActiveRow(dataset));
    Owned<ITypeInfo> type = makeTransformType(record->getType());
    OwnedHqlExpr call = bindFunctionCall(func, args, type);
    doTransform(funcctx, call, selfCursor);
    buildReturnRecordSize(funcctx, selfCursor);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityDefineSideEffect(BuildCtx & ctx, IHqlExpression * expr)
{
    Owned<ABoundActivity> parentActivity = buildCachedActivity(ctx, expr->queryChild(0));
    OwnedHqlExpr attr = createAttribute(_sideEffect_Atom, LINK(expr->queryProperty(_uid_Atom)));
    OwnedHqlExpr unknown = createUnknown(no_attr, NULL, NULL, LINK(parentActivity));
    activeGraphCtx->associateExpr(attr, unknown);
    return parentActivity.getClear();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityCallSideEffect(BuildCtx & ctx, IHqlExpression * expr)
{
    OwnedHqlExpr attr = createAttribute(_sideEffect_Atom, LINK(expr->queryProperty(_uid_Atom)));
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
    if (expr->hasProperty(successAtom))
    {
        label = "Success";
        when = WhenSuccessId;
    }
    else if (expr->hasProperty(failureAtom))
    {
        label = "Failure";
        when = WhenFailureId;
    }
    else if (expr->hasProperty(parallelAtom))
    {
        label = "Parallel";
        when = WhenParallelId;
    }
    else
    {
        label = "Before";
        when = WhenDefaultId;
    }

    bool useImplementationClass = options.minimizeActivityClasses;
    ThorActivityKind kind = (expr->isAction() ? TAKwhen_action : TAKwhen_dataset);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, "WhenAction");
    if (useImplementationClass)
        instance->setImplementationClass(newWhenActionArgAtom);

    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);
    buildInstanceSuffix(instance);

    if (expr->isAction())
        addDependency(ctx, boundDataset, instance->queryBoundActivity(), dependencyAtom, NULL, 1);
    else
        buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    addDependency(ctx, associatedActivity, instance->queryBoundActivity(), dependencyAtom, label, when);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

IHqlExpression * extractFilterConditions(HqlExprAttr & invariant, IHqlExpression * expr, IHqlExpression * dataset, bool spotCSE)
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
        cond.setown(spotScalarCSE(cond));

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
    OwnedHqlExpr cond = extractFilterConditions(invariant, expr, dataset, options.spotCSE);

    //Base class returns true, so only generate if no non-invariant conditions
    if (cond)
    {
        BuildCtx funcctx(instance->startctx);
        funcctx.addQuotedCompound("virtual bool isValid(const void * _self)");
        funcctx.addQuoted("unsigned char * self = (unsigned char *) _self;");

        bindTableCursor(funcctx, dataset, "self");
        buildReturn(funcctx, cond);
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
    IHqlExpression * rowsid = expr->queryProperty(_rowsid_Atom);
    if (targetThor() && !isGrouped(dataset))
        throwError(HQLERR_ThorHavingMustBeGrouped);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKfiltergroup, expr,"FilterGroup");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    HqlExprAttr invariant;
    OwnedHqlExpr left = createSelector(no_left, dataset, selSeq);
    OwnedHqlExpr cond = extractFilterConditions(invariant, expr, left, options.spotCSE);

    //Base class returns true, so only generate if no non-invariant conditions
    if (cond)
    {
        BuildCtx funcctx(instance->startctx);
        funcctx.addQuotedCompound("virtual bool isValid(unsigned numRows, const void * * _rows)");
        funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _rows[0];");
        funcctx.addQuoted("unsigned char * * rows = (unsigned char * *) _rows;");

        bindTableCursor(funcctx, dataset, "left", no_left, selSeq);
        bindRows(funcctx, no_left, selSeq, rowsid, dataset, "numRows", "rows", options.mainRowsAreLinkCounted);
        
        buildReturn(funcctx, cond);
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

    if (targetThor() && !expr->hasProperty(localAtom))
        ERRORAT(queryLocation(expr), HQLERR_ThorCombineOnlyLocal);

    CIArray bound;
    bound.append(*buildCachedActivity(ctx, left));
    bound.append(*buildCachedActivity(ctx, right));

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKcombine, expr, "Combine");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, unsigned _num, const void * * _rows)");
    if (transform->getOperator() != no_skip)
    {
        funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _rows[0];");
        funcctx.addQuoted("const unsigned char * right = (const unsigned char *) _rows[1];");
        ensureRowAllocated(funcctx, "crSelf");

        bindTableCursor(funcctx, left, "left", no_left, selSeq);
        bindTableCursor(funcctx, right, "right", no_right, selSeq);
        BoundRow * selfCursor = bindSelf(funcctx, expr, "crSelf");
        associateSkipReturnMarker(funcctx, queryZero(), selfCursor);

        doTransform(funcctx, transform, selfCursor);
        buildReturnRecordSize(funcctx, selfCursor);
    }
    else
        funcctx.addReturn(queryZero());

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

    ITypeInfo * rowType = makeReferenceModifier(LINK(rowsExpr->queryType()->queryChildType()));
    if (rowsAreLinkCounted)
        rowType = makeAttributeModifier(rowType, getLinkCountedAttr());

    //Rows may be link counted, but rows() is not a linkable rowset
    OwnedITypeInfo rowsType = makeReferenceModifier(makeTableType(rowType, NULL, NULL, NULL));
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
    IHqlExpression * rowsid = expr->queryProperty(_rowsid_Atom);

    CIArray bound;
    bound.append(*buildCachedActivity(ctx, left));
    bound.append(*buildCachedActivity(ctx, right));

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKcombinegroup, expr, "CombineGroup");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, unsigned numRows, const void * * _rows)");
    if (transform->getOperator() != no_skip)
    {
        funcctx.addQuoted("const unsigned char * left = (const unsigned char *)_left;");
        funcctx.addQuoted("const unsigned char * right = (const unsigned char *) _rows[0];");
        funcctx.addQuoted("unsigned char * * rows = (unsigned char * *) _rows;");
        ensureRowAllocated(funcctx, "crSelf");

        bindTableCursor(funcctx, left, "left", no_left, selSeq);
        bindTableCursor(funcctx, right, "right", no_right, selSeq);
        bindRows(funcctx, no_right, selSeq, rowsid, right, "numRows", "rows", options.mainRowsAreLinkCounted);
        BoundRow * selfCursor = bindSelf(funcctx, expr, "crSelf");
        associateSkipReturnMarker(funcctx, queryZero(), selfCursor);

        doTransform(funcctx, transform, selfCursor);
        buildReturnRecordSize(funcctx, selfCursor);
    }
    else
        funcctx.addReturn(queryZero());

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
    IHqlExpression * rowsid = expr->queryProperty(_rowsid_Atom);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKrollupgroup, expr, "RollupGroup");
    instance->graphLabel.set("Rollup Group");       // Grouped Rollup Group looks silly
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, unsigned numRows, const void * * _rows)");
    if (transform->getOperator() != no_skip)
    {
        funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _rows[0];");
        funcctx.addQuoted("unsigned char * * rows = (unsigned char * *) _rows;");
        ensureRowAllocated(funcctx, "crSelf");

        bindTableCursor(funcctx, dataset, "left", no_left, selSeq);
        bindRows(funcctx, no_left, selSeq, rowsid, dataset, "numRows", "rows", options.mainRowsAreLinkCounted);

        BoundRow * selfCursor = bindSelf(funcctx, expr, "crSelf");
        associateSkipReturnMarker(funcctx, queryZero(), selfCursor);
        doTransform(funcctx, transform, selfCursor);
        buildReturnRecordSize(funcctx, selfCursor);
    }
    else
        funcctx.addReturn(queryZero());

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

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    unsigned num = args.ordinality();

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual bool isValid(const void * _self)");
    funcctx.addQuoted("unsigned char * self = (unsigned char *) _self;");
    bindTableCursor(funcctx, dataset, "self");

    for (unsigned i=1; i < num; i++)
    {
        IHqlExpression & cur = args.item(i);
        if (!cur.isAttribute())
            buildStmt(funcctx, &cur);
    }
    funcctx.addReturn(queryBoolExpr(true));

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

void HqlCppTranslator::buildLimitHelpers(BuildCtx & ctx, IHqlExpression * rowLimit, IHqlExpression * failAction, bool isSkip, IHqlExpression * filename, unique_id_t id)
{
    doBuildUnsigned64Function(ctx, "getRowLimit", rowLimit);

    if (isZero(rowLimit))
        WARNING(HQLWRN_LimitIsZero);

    if (!isSkip)
    {
        LinkedHqlExpr fail = failAction;
        if (!fail || fail->isAttribute())
        {
            if (!id)
                id = queryCurrentActivityId(ctx);
            fail.setown(createFailAction("Limit exceeded", rowLimit, filename, id));
        }
        BuildCtx ctx2(ctx);
        ctx2.addQuotedCompound("virtual void onLimitExceeded()");
        buildStmt(ctx2, fail);
    }
}


void HqlCppTranslator::buildLimitHelpers(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * filename, unique_id_t id)
{
    buildLimitHelpers(ctx, expr->queryChild(1), queryRealChild(expr, 2), expr->hasProperty(skipAtom), filename, id);

    IHqlExpression * transform = queryPropertyChild(expr, onFailAtom, 0);
    if (transform)
    {
        BuildCtx transformctx(ctx);
        transformctx.addQuotedCompound("virtual size32_t transformOnLimitExceeded(ARowBuilder & crSelf)");
        ensureRowAllocated(transformctx, "crSelf");
        buildTransformBody(transformctx, transform, NULL, NULL, expr, NULL);
    }
}



ABoundActivity * HqlCppTranslator::doBuildActivityLimit(BuildCtx & ctx, IHqlExpression * expr)
{
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, expr->queryChild(0));

    IHqlExpression * transform = queryPropertyChild(expr, onFailAtom, 0);
    ThorActivityKind kind = TAKlimit;
    const char * helper = "Limit";
    if (transform)
    {
        kind = TAKcreaterowlimit;
        helper = "CreateRowLimit";
    }
    else if (expr->hasProperty(skipAtom))
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

    IHqlExpression * transform = queryPropertyChild(expr, onFailAtom, 0);
    bool isSkip = expr->hasProperty(skipAtom);

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
        BuildCtx isMatchCtx(instance->startctx);
        isMatchCtx.addQuotedCompound("virtual bool isMatch(IException * except)");
        associateLocalFailure(isMatchCtx, "except");
        OwnedHqlExpr cseFilter = spotScalarCSE(filter);
        buildReturn(isMatchCtx, cseFilter, queryBoolType());
    }

    if (transform)
    {
        BuildCtx onFailCtx(instance->startctx);
        onFailCtx.addQuotedCompound("virtual unsigned transformOnExceptionCaught(ARowBuilder & crSelf, IException * except)");
        ensureRowAllocated(onFailCtx, "crSelf");
        associateLocalFailure(onFailCtx, "except");
        buildTransformBody(onFailCtx, transform, NULL, NULL, expr, NULL);
    } 
    else if (!isSkip)
    {
        LinkedHqlExpr fail = action;
        if (!fail)
            fail.setown(createFailAction("Missing failure", NULL, NULL, instance->activityId));
        
        BuildCtx throwctx(instance->startctx);
        throwctx.addQuotedCompound("virtual void onExceptionCaught()");
        buildStmt(throwctx, fail);
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
            _ATOM name= cur->queryName();
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
    if (expr->hasProperty(privateAtom))
        flags.append("|TSFprivate");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityMetaActivity(BuildCtx & ctx, IHqlExpression * expr)
{
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, expr->queryChild(0));
    if (!targetThor())
        return boundDataset.getClear();

    assertex(expr->hasProperty(pullAtom));

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKpull, expr, "Pull");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);
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

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual unsigned __int64 getProportionNumerator()");
    buildReturn(funcctx, numerator);

    BuildCtx funcctx2(instance->startctx);
    funcctx2.addQuotedCompound("virtual unsigned __int64 getProportionDenominator()");
    if (denominator && !denominator->isAttribute())
        buildReturn(funcctx2, denominator);
    else
    {
        OwnedHqlExpr notProvided = createConstant(counterType->castFrom(true, I64C(-1)));
        buildReturn(funcctx2, notProvided);
    }

    BuildCtx funcctx3(instance->startctx);
    funcctx3.addQuotedCompound("virtual unsigned getSampleNumber()");
    if (sample && !sample->isAttribute())
        buildReturn(funcctx3, sample);
    else
        funcctx3.addQuoted("return 1;");

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
    IHqlExpression * sampleExpr = queryRealChild(expr, 2);
    unsigned sample = (unsigned)getIntValue(sampleExpr, 1);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKsample, expr,"Sample");

    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    doBuildUnsignedFunction(instance->startctx, "getProportion", expr->queryChild(1));

    BuildCtx funcctx2(instance->startctx);
    funcctx2.addQuotedCompound("virtual unsigned getSampleNumber()");
    s.clear().append("return ").append(sample).append(";");
    funcctx2.addQuoted(s);

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

    return createValue(no_order, LINK(signedType), createValue(no_sortlist, makeSortListType(NULL), leftList), createValue(no_sortlist, makeSortListType(NULL), rightList));
}


void HqlCppTranslator::buildReturnOrder(BuildCtx & ctx, IHqlExpression *sortList, const DatasetReference & dataset)
{
    OwnedHqlExpr selSeq = createDummySelectorSequence();
    OwnedHqlExpr leftSelect = dataset.getSelector(no_left, selSeq);
    OwnedHqlExpr rightSelect = dataset.getSelector(no_right, selSeq);
    OwnedHqlExpr order = createOrderFromSortList(dataset, sortList, leftSelect, rightSelect);
    
    bindTableCursor(ctx, dataset.queryDataset(), "left", no_left, selSeq);
    bindTableCursor(ctx, dataset.queryDataset(), "right", no_right, selSeq);

    doBuildReturnCompare(ctx, order, no_order, false);
}

ABoundActivity * HqlCppTranslator::doBuildActivityGroup(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * child = dataset;
    while (child->getOperator() == no_group)
        child = child->queryChild(0);

    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, child);
    if (expr->queryType()->queryGroupInfo() == child->queryType()->queryGroupInfo())
        return boundDataset.getClear();

    IHqlExpression * sortlist = queryRealChild(expr, 1);
    if (!sortlist || ((sortlist->numChildren() == 0) && (sortlist->getOperator() != no_activetable)))
    {
        bool useImplementationClass = options.minimizeActivityClasses;
        Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKdegroup, expr,"Degroup");
        if (useImplementationClass)
            instance->setImplementationClass(newDegroupArgAtom);
        buildActivityFramework(instance);

        buildInstancePrefix(instance);
        buildInstanceSuffix(instance);

        buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
        return instance->getBoundActivity();
    }
    else
    {
        ThorActivityKind tak = (expr->getOperator() == no_group) ? TAKgroup: TAKgrouped;
        Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, tak, expr, "Group");
        buildActivityFramework(instance);

        buildInstancePrefix(instance);

        //virtual bool isSameGroup(const void *left, const void *right);
        BuildCtx funcctx(instance->startctx);
        funcctx.addQuotedCompound("virtual bool isSameGroup(const void * _left, const void * _right)");
        if (sortlist->getOperator() == no_activetable)
            buildReturn(funcctx, queryBoolExpr(false));
        else
        {
            funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
            funcctx.addQuoted("const unsigned char * right = (const unsigned char *) _right;");

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
                    orderResult.setown(createValue(no_order, LINK(signedType), createValue(no_sortlist, makeSortListType(NULL), leftValues), createValue(no_sortlist, makeSortListType(NULL), rightValues)));
            }

            if (args.ordinality() == 1)
                result.set(&args.item(0));
            else if (args.ordinality() != 0)
                result.setown(createValue(no_and, makeBoolType(), args));

            bindTableCursor(funcctx, dataset, "left", no_left, selSeq);
            bindTableCursor(funcctx, dataset, "right", no_right, selSeq);
            IHqlExpression * trueExpr = queryBoolExpr(true);
            if (result)
            {
                if (orderResult)
                {
                    buildFilteredReturn(funcctx, result, trueExpr);
                    doBuildReturnCompare(funcctx, orderResult, no_eq, true);
                }
                else
                {
                    buildReturn(funcctx, result);
                }
            }
            else
            {
                if (orderResult)
                    doBuildReturnCompare(funcctx, orderResult, no_eq, true);
                else
                    buildReturn(funcctx, trueExpr);
            }
        }

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
    IHqlExpression * cond = expr->queryChild(0);
    IHqlExpression * trueBranch = expr->queryChild(1);
    IHqlExpression * falseBranch = queryRealChild(expr, 2);
    if (falseBranch && (falseBranch->getOperator() == no_null))
        falseBranch = NULL;

    OwnedHqlExpr cseCond = options.spotCSE ? spotScalarCSE(cond) : LINK(cond);
    bool isChild = (insideChildGraph(ctx) || insideRemoteGraph(ctx) || insideLibrary());
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

        buildConnectInputOutput(ctx, instance, boundTrue, 0, 0, "True");
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
                addDependency(ctx, boundTrue, instance->queryBoundActivity(), dependencyAtom, "True", 1);
            if (boundFalse)
                addDependency(ctx, boundFalse, instance->queryBoundActivity(), dependencyAtom, "False", 2);
        }
        else
        {
            if (boundTrue)
                buildConnectInputOutput(ctx, instance, boundTrue, 0, 0, "True");
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
    Array boundActivities;
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
        addDependency(ctx, &cur, instance->queryBoundActivity(), dependencyAtom, temp.str(), j+1);
    }

    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityCase(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    node_operator op = expr->getOperator();
    bool isChild = (insideChildGraph(ctx) || insideRemoteGraph(ctx));
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

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual unsigned getBranch()");

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
            addDependency(ctx, boundBranch, instance->queryBoundActivity(), dependencyAtom, label.str(), branchIdx+1);
        else
            buildConnectInputOutput(ctx, instance, boundBranch, 0, idx-first, label.str());
    }

    args.append(*createConstant(unsignedType->castFrom(false, (__int64)max-1)));
    OwnedHqlExpr fullCond = createValue(op, LINK(unsignedType), args);
    fullCond.setown(foldHqlExpression(fullCond));
    if (options.spotCSE)
        fullCond.setown(spotScalarCSE(fullCond));
    buildReturn(funcctx, fullCond);

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
    if (getProperty(expr, thresholdAtom, temp.clear()))
    {
        s.clear().append("virtual unsigned __int64 getThreshold() { return ").append(temp).append("; }");
        ctx.addQuoted(s);
    }

    IHqlExpression * skew = expr->queryProperty(skewAtom);
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

    instance->classctx.addQuoted("virtual ICompare * queryCompare() { return &compare; }");

//  sortlist.setown(spotScalarCSE(sortlist));
    buildCompareClass(instance->nestedctx, "compare", sortlist, DatasetReference(dataset));

    IHqlExpression * record = dataset->queryRecord();
    OwnedHqlExpr serializedRecord = getSerializedForm(record);
    if (!targetRoxie())
    {
        if (record != serializedRecord)
        {
            instance->classctx.addQuoted("virtual ICompare * queryCompareSerializedRow() { return &compareSR; }");

            OwnedHqlExpr selSeq = createSelectorSequence();
            OwnedHqlExpr leftSelector = createSelector(no_left, dataset, selSeq);
            OwnedHqlExpr mappedSortlist = replaceSelector(sortlist, dataset, leftSelector);
            OwnedHqlExpr serializedSortlist = replaceMemorySelectorWithSerializedSelector(mappedSortlist, record, no_left, selSeq);
            OwnedHqlExpr serializedDataset = createDataset(no_null, LINK(serializedRecord));
            DatasetReference serializedRef(serializedDataset, no_left, selSeq);
            try
            {
                buildCompareClass(instance->nestedctx, "compareSR", serializedSortlist, serializedRef);
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
    generateSerializeKey(instance->nestedctx, no_none, DatasetReference(dataset), sorts, tryToSerializeKey, false, false);

    buildSkewThresholdMembers(instance->classctx, expr);

    if (limit)
    {
        OwnedHqlExpr newLimit = ensurePositiveOrZeroInt64(limit);
        BuildCtx funcctx(instance->startctx);
        funcctx.addQuotedCompound("virtual __int64 getLimit()");
        buildReturn(funcctx, newLimit, defaultIntegralType);
    }

    IHqlExpression * best = expr->queryProperty(bestAtom);
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
        OwnedHqlExpr order = createValue(no_order, LINK(signedType), createValue(no_sortlist, makeSortListType(NULL), sortValues), createValue(no_sortlist, makeSortListType(NULL), maxValues));

        BuildCtx funcctx(instance->startctx);
        funcctx.addQuotedCompound("virtual int compareBest(const void * _self)");
        funcctx.addQuoted("unsigned char * self = (unsigned char *) _self;");
        bindTableCursor(funcctx, dataset, "self");
        buildReturn(funcctx, order);
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

            buildMetaMember(instance->classctx, cosortDataset, "querySortedRecordSize");
        }
        else
        {
            instance->startctx.addQuoted("virtual const char * getSortedFilename() { return NULL; }");
            instance->classctx.addQuoted("virtual IOutputMetaData * querySortedRecordSize() { return NULL; }");

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

    if (expr->hasProperty(manyAtom))
        instance->classctx.addQuoted("virtual bool hasManyRecords() { return true; }");

    IHqlExpression * stable = expr->queryProperty(stableAtom);
    IHqlExpression * unstable = expr->queryProperty(unstableAtom);
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

    if (!method || method->isConstant())
        flags.append("|TAFconstant");

    if (method)
        doBuildVarStringFunction(instance->startctx, "queryAlgorithm", method);

    if (!streq(flags.str(), "|TAFconstant"))
        instance->classctx.addQuotedF("virtual unsigned getAlgorithmFlags() { return %s; }", flags.str()+1);

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

void HqlCppTranslator::doBuildXmlReadMember(ActivityInstance & instance, IHqlExpression * expr, const char * functionName, bool & usesContents)
{
    StringBuffer s, xmlInstanceName;
    usesContents = false;
    if (isValidXmlRecord(expr->queryRecord()))
    {
        StringBuffer xmlFactoryName;
        getUniqueId(xmlInstanceName.append("xml"));

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
    IHqlExpression * wuid = expr->queryProperty(wuidAtom);
    IHqlExpression * sequence = queryPropertyChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryPropertyChild(expr, nameAtom, 0);

    __int64 sequenceValue = sequence->queryValue()->getIntValue();
    bool isStored = (sequenceValue == ResultSequenceStored);
    bool useImplementationClass = options.minimizeActivityClasses && !wuid && (sequenceValue == ResultSequenceInternal);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKworkunitread, expr,"WorkunitRead");
    if (useImplementationClass)
        instance->setImplementationClass(newWorkUnitReadArgAtom);

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
            BuildCtx func2ctx(instance->classctx);
            func2ctx.addQuotedCompound("virtual int querySequence()");
            buildReturn(func2ctx, sequence, signedType);
        }

        if (wuid)
            doBuildVarStringFunction(instance->classctx, "queryWUID", wuid->queryChild(0));

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

    IHqlExpression * xmlAttr = expr->queryProperty(xmlAtom);
    //MORE: What encoding is the search text in???

    doBuildParseSearchText(instance->startctx, dataset, expr->queryChild(1), type_utf8, unknownStringType);
    doBuildVarStringFunction(instance->classctx, "queryIteratorPath", xmlAttr ? queryRealChild(xmlAttr, 0) : NULL);

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, IColumnProvider * parsed)");
    ensureRowAllocated(funcctx, "crSelf");
    funcctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");

    // Both left and the dataset are bound to left because it might be a new transform or a transform
    IHqlExpression * transform = expr->queryChild(3);
    BoundRow * selfCursor = bindSelf(funcctx, expr, "crSelf");
    if (transform->getOperator() == no_newtransform)
        bindTableCursor(funcctx, dataset, "left");
    else
        bindTableCursor(funcctx, dataset, "left", no_left, selSeq);
    associateSkipReturnMarker(funcctx, queryZero(), selfCursor);

    OwnedHqlExpr helperName = createQuoted("parsed", makeBoolType());
    funcctx.associateExpr(xmlColumnProviderMarkerExpr, helperName);
    xmlUsesContents = false;
    doTransform(funcctx, transform, selfCursor);
    buildReturnRecordSize(funcctx, selfCursor);

    if (xmlUsesContents)
        instance->classctx.addQuoted("virtual bool requiresContents() { return true; }");

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
    
    OwnedHqlExpr call = bindFunctionCall(columnGetStringXAtom, args);
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
    
    OwnedHqlExpr call = bindFunctionCall(columnGetUnicodeXAtom, args);
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

ABoundActivity * HqlCppTranslator::doBuildActivityTempTable(BuildCtx & ctx, IHqlExpression * expr)
{
    StringBuffer s;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKtemptable, expr, "TempTable");

    OwnedHqlExpr values = normalizeListCasts(expr->queryChild(0));
    IHqlExpression * record = expr->queryChild(1);
    IHqlExpression * defaults = expr->queryChild(2);

    assertex(values->getOperator() != no_recordlist);       // should have been transformed by now.

    //-----------------
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual size32_t getRow(ARowBuilder & crSelf, unsigned row)");
    ensureRowAllocated(funcctx, "crSelf");
    BoundRow * selfCursor = bindSelf(funcctx, instance->dataset, "crSelf");
    IHqlExpression * self = selfCursor->querySelector();
    OwnedHqlExpr clearAction;

    OwnedHqlExpr rowsExpr;
    OwnedHqlExpr rowVar = createVariable("row", makeIntType(4, false));
    if (expr->getOperator() == no_datasetfromrow)
    {
        BuildCtx subctx(funcctx);
        subctx.addFilter(rowVar);
        if (clearAction)
            subctx.addExpr(clearAction);
        subctx.addReturn(queryZero());

        buildAssign(funcctx, self, values);
        buildReturnRecordSize(funcctx, selfCursor);
        rowsExpr.setown(getSizetConstant(1));
    }
    else if ((values->getOperator() == no_list) || (values->getOperator() == no_null))
//  else if (((values->getOperator() == no_list) && 
//                  (!values->isConstant() || (values->queryType()->queryChildType()->getSize() == UNKNOWN_LENGTH))) || (values->getOperator() == no_null))
    {
        //
        unsigned maxRows = values->numChildren();
        if (maxRows)
        {
            unsigned dummyIdx = 0;
            OwnedHqlExpr tgt = createSelectExpr(LINK(self), LINK(queryNextRecordField(record, dummyIdx)));

            BuildCtx switchctx(funcctx);
            switchctx.addQuotedCompound("switch (row)");

            unsigned row;
            for (row = 0; row < maxRows; row++)
            {
                BuildCtx casectx(switchctx);
                casectx.addQuotedCompound(s.clear().append("case ").append(row).append(":"));

                buildAssign(casectx, tgt, values->queryChild(row));
                //casectx.setNextDestructor();
                buildReturnRecordSize(casectx, selfCursor);
            }
        }

        if (clearAction)
            funcctx.addExpr(clearAction);
        funcctx.addReturn(queryZero());
        
        rowsExpr.setown(getSizetConstant(maxRows));
    }
    else
    {
        CHqlBoundExpr bound;
        //MORE: This shouldn't be done this way...
        OwnedHqlExpr normalized = normalizeListCasts(values);

        if (normalized->getOperator() == no_alias)
            buildExpr(instance->startctx, normalized, bound);
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
        }
        rowsExpr.setown(getBoundCount(bound));
        rowsExpr.setown(createTranslated(rowsExpr));

        OwnedHqlExpr compare = createValue(no_ge, makeBoolType(), LINK(rowVar), LINK(rowsExpr));
        BuildCtx condctx(funcctx);
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
        buildAssign(funcctx, tgt, src);
        buildReturnRecordSize(funcctx, selfCursor);
    }

    doBuildUnsignedFunction(instance->startctx, "numRows", rowsExpr);
    if (!values->isConstant())
        doBuildBoolFunction(instance->startctx, "isConstant", false);

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

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKtemprow, expr,"TempRow");
    if (valueText.length())
    {
        StringBuffer graphLabel;
        if (valueText.length() > MAX_ROW_VALUE_TEXT_LEN)
        {
            valueText.setLength(MAX_ROW_VALUE_TEXT_LEN);
            valueText.append("...");
        }
        graphLabel.append(getActivityText(instance->kind)).append("\n{").append(valueText).append("}");
        instance->graphLabel.set(graphLabel.str());
    }

    //-----------------
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual size32_t getRowSingle(ARowBuilder & crSelf)");
    ensureRowAllocated(funcctx, "crSelf");
    BoundRow * selfCursor = bindSelf(funcctx, instance->dataset, "crSelf");
    IHqlExpression * self = selfCursor->querySelector();

    if (isDataset)
        associateSkipReturnMarker(funcctx, queryBoolExpr(false), selfCursor);

    buildAssign(funcctx, self, expr);
    buildReturnRecordSize(funcctx, selfCursor);

    if (!valuesAreConstant)
        doBuildBoolFunction(instance->startctx, "isConstant", false);

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

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKtemptable, expr,"TempTable");

    //-----------------
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual size32_t getRow(ARowBuilder & crSelf, unsigned row)");
    ensureRowAllocated(funcctx, "crSelf");
    BoundRow * selfCursor = bindSelf(funcctx, instance->dataset, "crSelf");
    IHqlExpression * self = selfCursor->querySelector();

    unsigned maxRows = values->numChildren();
    bool done = false;
    if (values->isConstant())
    {
        CHqlBoundExpr bound;
        if (doBuildDatasetInlineTable(funcctx, expr, bound, FormatNatural))
        {
            OwnedHqlExpr whichRow = createVariable("row", LINK(unsignedType));
            BuildCtx subctx(funcctx);
            OwnedHqlExpr test = createValue(no_ge, makeBoolType(), LINK(whichRow), getSizetConstant(maxRows));
            subctx.addFilter(test);
            buildReturn(subctx, queryZero());
            OwnedHqlExpr ds = bound.getTranslatedExpr();
            OwnedHqlExpr thisRow = createRowF(no_selectnth, LINK(ds), adjustValue(whichRow, 1), createAttribute(noBoundCheckAtom), NULL);
            buildAssign(funcctx, self, thisRow);
            buildReturnRecordSize(funcctx, selfCursor);
            done = true;
        }
    }

    if (!done)
    {
        if (maxRows)
        {
            StringBuffer s;
            BuildCtx switchctx(funcctx);
            switchctx.addQuotedCompound("switch (row)");

            unsigned row;
            for (row = 0; row < maxRows; row++)
            {
                BuildCtx casectx(switchctx);
                casectx.addQuotedCompound(s.clear().append("case ").append(row).append(":"));

                IHqlExpression * cur = values->queryChild(row);
                OwnedHqlExpr rowValue = createRow(no_createrow, LINK(cur));
                buildAssign(casectx, self, rowValue);
                buildReturnRecordSize(casectx, selfCursor);
            }
        }
        funcctx.addReturn(queryZero());
    }

    OwnedHqlExpr rowsExpr = getSizetConstant(maxRows);
    doBuildUnsignedFunction(instance->startctx, "numRows", rowsExpr);

    if (!values->isConstant())
        doBuildBoolFunction(instance->startctx, "isConstant", false);

    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityCountTransform(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * count = expr->queryChild(0);
    IHqlExpression * transform = queryNewColumnProvider(expr);
    IHqlExpression * counter = queryPropertyChild(expr, _countProject_Atom, 0);

    // Overriding IHThorTempTableArg
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKtemptable, expr,"TempTable");
    buildActivityFramework(instance);
    buildInstancePrefix(instance);

    // size32_t getRow()
    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual size32_t getRow(ARowBuilder & crSelf, unsigned row)");
    ensureRowAllocated(funcctx, "crSelf");
    BoundRow * selfCursor = bindSelf(funcctx, instance->dataset, "crSelf");
    IHqlExpression * self = selfCursor->querySelector();
    associateCounter(funcctx, counter, "row");
    // FIXME: this should be fixed in the engine
    funcctx.addQuoted("if (row == numRows()) return 0;");
    buildTransformBody(funcctx, transform, NULL, NULL, instance->dataset, self);

    // unsigned numRows() - count is guaranteed by lexer
    doBuildUnsignedFunction(instance->startctx, "numRows", count);

    // bool isConstant() - default is true
    if (!isConstantTransform(transform))
        doBuildBoolFunction(instance->startctx, "isConstant", false);

    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

void HqlCppTranslator::buildHTTPtoXml(BuildCtx & ctx)
{
    BuildCtx funcctx(ctx);

    //virtual void toXML(const byte * self, StringBuffer & out) = 0;
    funcctx.addQuotedCompound("virtual void toXML(const byte *, IXmlWriter & out)");
}

//---------------------------------------------------------------------------

void HqlCppTranslator::buildSOAPtoXml(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * transform, IHqlExpression * selSeq)
{
    BuildCtx funcctx(ctx);

    //virtual void toXML(const byte * self, StringBuffer & out) = 0;
    if (dataset)
    {
        funcctx.addQuotedCompound("virtual void toXML(const byte * left, IXmlWriter & out)");
        if (transform->getOperator() == no_newtransform)
            bindTableCursor(funcctx, dataset, "left");
        else
            bindTableCursor(funcctx, dataset, "left", no_left, selSeq);
    }
    else
        funcctx.addQuotedCompound("virtual void toXML(const byte *, IXmlWriter & out)");

    // Bind left to "left" and right to RIGHT
    HqlExprArray assigns;
    filterExpandAssignments(funcctx, NULL, assigns, transform);
    OwnedHqlExpr self = getSelf(transform);
    buildXmlSerialize(funcctx, transform->queryRecord(), self, &assigns);
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

    //virtual const char * queryHosts() = 0;
    doBuildVarStringFunction(instance->startctx, "queryHosts", hosts);

    //virtual const char * queryService() = 0;
    doBuildVarStringFunction(instance->startctx, "queryService", service);

    //virtual void toXML(const byte * self, StringBuffer & out) = 0;
    buildSOAPtoXml(instance->startctx, dataset, expr->queryChild(firstArg+3), selSeq);

    //virtual const char * queryOutputIteratorPath()
    IHqlExpression * separator = expr->queryProperty(separatorAtom);
    if (separator)
        doBuildVarStringFunction(instance->startctx, "queryOutputIteratorPath", separator->queryChild(0));

    //virtual const char * queryHeader()
    //virtual const char * queryFooter()
    IHqlExpression * header = expr->queryProperty(headerAtom);
    if (header)
    {
        doBuildVarStringFunction(instance->startctx, "queryHeader", header->queryChild(0));
        doBuildVarStringFunction(instance->startctx, "queryFooter", header->queryChild(1));
    }

    IHqlExpression * action = expr->queryProperty(soapActionAtom);
    if (action)
        doBuildVarStringFunction(instance->startctx, "querySoapAction", action->queryChild(0));

    IHqlExpression * httpHeader = expr->queryProperty(httpHeaderAtom);
    if (httpHeader)
    {
        doBuildVarStringFunction(instance->startctx, "queryHttpHeaderName", httpHeader->queryChild(0));
        doBuildVarStringFunction(instance->startctx, "queryHttpHeaderValue", httpHeader->queryChild(1));
    }

    IHqlExpression * proxyAddress = expr->queryProperty(proxyAddressAtom);
    if (proxyAddress)
    {
        doBuildVarStringFunction(instance->startctx, "queryProxyAddress", proxyAddress->queryChild(0));
    }

    IHqlExpression * namespaceAttr = expr->queryProperty(namespaceAtom);
    IHqlExpression * responseAttr = expr->queryProperty(responseAtom);
    //virtual unsigned getFlags()
    {
        StringBuffer flags;
        if (expr->hasProperty(groupAtom))
            flags.append("|SOAPFgroup");
        if (expr->hasProperty(onFailAtom))
            flags.append("|SOAPFonfail");
        if (expr->hasProperty(logAtom))
            flags.append("|SOAPFlog");
        if (expr->hasProperty(trimAtom))
            flags.append("|SOAPFtrim");
        if (expr->hasProperty(literalAtom))
            flags.append("|SOAPFliteral");
        if (namespaceAttr)
            flags.append("|SOAPFnamespace");
        if (expr->hasProperty(encodingAtom))
            flags.append("|SOAPFencoding");
        if (responseAttr && responseAttr->hasProperty(noTrimAtom))
            flags.append("|SOAPFpreserveSpace");

        if (flags.length())
            doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);
    }

    //virtual unsigned numParallelThreads()
    doBuildUnsignedFunction(instance->classctx, "numParallelThreads", queryPropertyChild(expr, parallelAtom, 0));

    //virtual unsigned numRecordsPerBatch()
    doBuildUnsignedFunction(instance->classctx, "numRecordsPerBatch", queryPropertyChild(expr, mergeAtom, 0));

    //virtual int numRetries()
    doBuildSignedFunction(instance->classctx, "numRetries", queryPropertyChild(expr, retryAtom, 0));

    //virtual unsigned getTimeout()
    doBuildUnsignedFunction(instance->classctx, "getTimeout", queryPropertyChild(expr, timeoutAtom, 0));

    //virtual unsigned getTimeLimit()
    doBuildUnsignedFunction(instance->classctx, "getTimeLimit", queryPropertyChild(expr, timeLimitAtom, 0));

    if (namespaceAttr)
    {
        doBuildVarStringFunction(instance->startctx, "queryNamespaceName", namespaceAttr->queryChild(0));
        if (namespaceAttr->queryChild(1))
            doBuildVarStringFunction(instance->startctx, "queryNamespaceVar", namespaceAttr->queryChild(1));
    }

    if (!isSink)
    {
        //virtual IXmlToRowTransformer * queryTransformer()
        bool usesContents = false;
        doBuildXmlReadMember(*instance, expr, "queryInputTransformer", usesContents);
        if (usesContents)
            throwError(HQLERR_ContentsInSoapCall);

        //virtual const char * queryInputIteratorPath()
        IHqlExpression * xpath = expr->queryProperty(xpathAtom);
        if (xpath)
            doBuildVarStringFunction(instance->classctx, "queryInputIteratorPath", xpath->queryChild(0));

        IHqlExpression * onFail = expr->queryProperty(onFailAtom);
        if (onFail)
        {
            IHqlExpression * onFailTransform = onFail->queryChild(0);
            if (onFailTransform->isTransform())
                assertex(recordTypesMatch(expr, onFailTransform));
            //virtual unsigned onFailTransform(ARowBuilder & crSelf, const void * _left, IException * e)
            BuildCtx onFailCtx(instance->startctx);
            onFailCtx.addQuotedCompound("virtual unsigned onFailTransform(ARowBuilder & crSelf, const void * _left, IException * except)");
            ensureRowAllocated(onFailCtx, "crSelf");
            associateLocalFailure(onFailCtx, "except");
            buildTransformBody(onFailCtx, onFailTransform, dataset, NULL, expr, selSeq);
        }
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

    //virtual const char * queryHosts() = 0;
    doBuildVarStringFunction(instance->startctx, "queryHosts", expr->queryChild(firstArg));

    //virtual const char * queryService() = 0;
    doBuildVarStringFunction(instance->startctx, "queryService", expr->queryChild(firstArg+1));

    //virtual const char * queryAcceptType() = 0;
    doBuildVarStringFunction(instance->startctx, "queryAcceptType", expr->queryChild(firstArg+2));

    //virtual void toXML(const byte * self, StringBuffer & out) = 0;
    buildHTTPtoXml(instance->startctx);

    //virtual const char * queryOutputIteratorPath()
    IHqlExpression * separator = expr->queryProperty(separatorAtom);
    if (separator)
        doBuildVarStringFunction(instance->startctx, "queryOutputIteratorPath", separator->queryChild(0));

    IHqlExpression * namespaceAttr = expr->queryProperty(namespaceAtom);
    //virtual unsigned getFlags()
    {
        StringBuffer flags;
        if (expr->hasProperty(groupAtom))
            flags.append("|SOAPFgroup");
        if (expr->hasProperty(onFailAtom))
            flags.append("|SOAPFonfail");
        if (expr->hasProperty(logAtom))
            flags.append("|SOAPFlog");
        if (expr->hasProperty(trimAtom))
            flags.append("|SOAPFtrim");
        if (expr->hasProperty(literalAtom))
            flags.append("|SOAPFliteral");
        if (namespaceAttr)
            flags.append("|SOAPFnamespace");

        if (flags.length())
            doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);
    }

    //virtual unsigned numParallelThreads()
    doBuildUnsignedFunction(instance->classctx, "numParallelThreads", queryPropertyChild(expr, parallelAtom, 0));

    //virtual unsigned numRecordsPerBatch()
    doBuildUnsignedFunction(instance->classctx, "numRecordsPerBatch", queryPropertyChild(expr, mergeAtom, 0));

    //virtual int numRetries()
    doBuildSignedFunction(instance->classctx, "numRetries", queryPropertyChild(expr, retryAtom, 0));

    //virtual unsigned getTimeout()
    doBuildUnsignedFunction(instance->classctx, "getTimeout", queryPropertyChild(expr, timeoutAtom, 0));

    //virtual unsigned getTimeLimit()
    doBuildUnsignedFunction(instance->classctx, "getTimeLimit", queryPropertyChild(expr, timeLimitAtom, 0));

    if (namespaceAttr)
    {
        doBuildVarStringFunction(instance->startctx, "queryNamespaceName", namespaceAttr->queryChild(0));
        if (namespaceAttr->queryChild(1))
            doBuildVarStringFunction(instance->startctx, "queryNamespaceVar", namespaceAttr->queryChild(1));
    }

    if (!isSink)
    {
        //virtual IXmlToRowTransformer * queryTransformer()
        bool usesContents = false;
        doBuildXmlReadMember(*instance, expr, "queryInputTransformer", usesContents);
        if (usesContents)
            throwError(HQLERR_ContentsInSoapCall);

        //virtual const char * queryInputIteratorPath()
        IHqlExpression * xpath = expr->queryProperty(xpathAtom);
        if (xpath)
            doBuildVarStringFunction(instance->classctx, "queryInputIteratorPath", xpath->queryChild(0));

        IHqlExpression * onFail = expr->queryProperty(onFailAtom);
        if (onFail)
        {
            //virtual unsigned onFailTransform(ARowBuilder & crSelf, const void * _left, IException * e)
            BuildCtx onFailCtx(instance->startctx);
            onFailCtx.addQuotedCompound("virtual unsigned onFailTransform(ARowBuilder & crSelf, const void * _left, IException * except)");
            ensureRowAllocated(onFailCtx, "crSelf");
            associateLocalFailure(onFailCtx, "except");
            buildTransformBody(onFailCtx, onFail->queryChild(0), dataset, NULL, expr, selSeq);
        }
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
    _ATOM func = isUnicode ? regexNewSetUStrPatternAtom : regexNewSetStrPatternAtom;
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
    _ATOM func = isUnicode ? regexNewUStrFindAtom : regexNewStrFindAtom;
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
    IHqlExpression * compiled = doBuildRegexCompileInstance(ctx, pattern, isUnicode, !expr->hasProperty(noCaseAtom));

    if (expr->getOperator() == no_regex_replace)
    {
        HqlExprArray args;
        args.append(*LINK(compiled));
        args.append(*LINK(search));
        args.append(*LINK(expr->queryChild(2)));
        _ATOM func = isUnicode ? regexNewUStrReplaceXAtom : regexNewStrReplaceXAtom;
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
        _ATOM func= isUnicode ? regexNewUStrFoundAtom : regexNewStrFoundAtom;
        OwnedHqlExpr call = bindFunctionCall(func, args);
        buildExprOrAssign(ctx, target, call, bound);
    }
    else
    {
        HqlExprArray args;
        args.append(*LINK(findInstance));
        args.append(*LINK(expr->queryChild(2)));
        _ATOM func= isUnicode ? regexNewUStrFoundXAtom : regexNewStrFoundXAtom;
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


//---------------------------------------------------------------------------
//-- no_null [DATASET] --

ABoundActivity * HqlCppTranslator::doBuildActivityNull(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    StringBuffer s;
    ThorActivityKind kind = expr->isAction() ? TAKemptyaction : TAKnull;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr,"Null");
    if (options.minimizeActivityClasses)
        instance->setImplementationClass(newNullArgAtom);

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

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompoundOpt("virtual void action()");
    if (expandChildren)
    {
        unsigned numChildren = expr->numChildren();
        for (unsigned idx=1; idx < numChildren; idx++)
        {
            IHqlExpression * cur = expr->queryChild(idx);
            if (!cur->isAttribute())
                buildStmt(funcctx, cur);
        }
    }
    else
    {
        buildStmt(funcctx, expr);
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

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompoundOpt("virtual void action()");
    buildStmt(funcctx, expr);

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
    IHqlExpression * original = queryPropertyChild(expr, _original_Atom, 0);

    OwnedHqlExpr resultName = ::createResultName(queryPropertyChild(expr, namedAtom, 0));
    resultName.setown(ensureExprType(resultName, unknownVarStringType));

    IHqlExpression * filesRead = expr->queryProperty(_files_Atom);
    DependenciesUsed dependencies(true);
    if (filesRead)
    {
        ForEachChild(i, filesRead)
            dependencies.tablesRead.append(*getNormalizedFilename(filesRead->queryChild(i)));
    }
    IHqlExpression * resultsRead = expr->queryProperty(_results_Atom);
    if (resultsRead)
        unwindChildren(dependencies.resultsRead, resultsRead);

    unsigned crc = getExpressionCRC(original) + PERSIST_VERSION;
    OwnedHqlExpr crcVal = createConstant((__int64)crc);
    OwnedHqlExpr crcExpr = calculatePersistInputCrc(ctx, dependencies);
    HqlExprArray args;
    args.append(*LINK(resultName));
    args.append(*LINK(crcVal));
    args.append(*LINK(crcExpr));
    args.append(*createConstant(expr->hasProperty(fileAtom)));
    buildFunctionCall(ctx, returnPersistVersionAtom, args);
}

void HqlCppTranslator::buildWorkflow(WorkflowArray & workflow)
{

    BuildCtx classctx(*code, goAtom);
    classctx.addQuotedCompound("struct MyEclProcess : public EclProcess", ";");

    BuildCtx performctx(classctx);
    performctx.addQuotedCompound("virtual int perform(IGlobalCodeContext * gctx, unsigned wfid)");
    performctx.addQuoted("ICodeContext * ctx;");
    performctx.addQuoted("ctx = gctx->queryCodeContext();");

    performctx.associateExpr(globalContextMarkerExpr, globalContextMarkerExpr);
    performctx.associateExpr(codeContextMarkerExpr, codeContextMarkerExpr);

    OwnedHqlExpr function = createQuoted("wfid", LINK(unsignedType));
    BuildCtx switchctx(performctx);
    IHqlStmt * switchStmt = switchctx.addSwitch(function);

    optimizePersists(workflow);
    ForEachItemIn(idx, workflow)
    {
        WorkflowItem & action = workflow.item(idx);
        HqlExprArray & exprs = action.queryExprs();
        unsigned wfid = action.queryWfid();

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

                IHqlExpression * persistAttr = expr->queryProperty(_workflowPersist_Atom);
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
            }
        }
    }

    OwnedHqlExpr returnExpr = getSizetConstant(maxSequence);
    performctx.addReturn(returnExpr);
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
        buildFunctionCall(ctx, doNotifyTargetAtom, args);
    }
    else
        buildFunctionCall(ctx, doNotifyAtom, args);
}

//---------------------------------------------------------------------------
// no_thorresult
// no_thorremoteresult


ABoundActivity * HqlCppTranslator::doBuildActivitySetResult(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IHqlExpression * sequence = queryPropertyChild(expr, sequenceAtom, 0);
    IHqlExpression * name = queryPropertyChild(expr, namedAtom, 0);
    IHqlExpression * persist = expr->queryProperty(_workflowPersist_Atom);

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

    noteResultDefined(ctx, instance, sequence, name, isRoot);
    if (attribute->isDatarow())
        attribute.setown(::ensureSerialized(attribute));

    if (kind == TAKremoteresult)
    {
        doBuildSequenceFunc(instance->classctx, sequence, true);

        BuildCtx sendctx(instance->startctx);
        sendctx.addQuotedCompound("virtual void sendResult(const void * _self)");
        sendctx.addQuoted("const unsigned char * self = (const unsigned char *)_self;");

        if (dataset->isDatarow())
        {
            OwnedHqlExpr bound = createVariable("self", makeRowReferenceType(dataset));
            bindRow(sendctx, dataset, bound);
        }
        else
            bindTableCursor(sendctx, dataset, "self");
        buildSetResultInfo(sendctx, expr, attribute, NULL, (persist != NULL), false);
    }
    else
    {
        BuildCtx sendctx(instance->startctx);
        sendctx.addQuotedCompound("virtual void action()");
        buildSetResultInfo(sendctx, expr, attribute, NULL, (persist != NULL), false);
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
        const char * name = e->queryChild(1)->queryName()->str();
        s.appendLower(strlen(name), name);
        return true;
    }
    return false;
}

void HqlCppTranslator::doBuildDistributionClearFunc(BuildCtx & ctx, IHqlExpression * dataset, HqlExprArray & selects)
{
    StringBuffer s, func;
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual void clearAggregate(IDistributionTable * * tables)");

    ForEachItemIn(idx, selects)
    {
        IHqlExpression * original = &selects.item(idx);
        ITypeInfo * type = original->queryType();
        getInterfaceName(func.clear().append("create"), type);
        s.clear().append("tables[").append(idx).append("] = ").append(func).append("(\"");
        expandFieldName(s, original);
        s.append("\", ").append(type->getSize()).append(");");
        funcctx.addQuoted(s);
    }
}

void HqlCppTranslator::doBuildDistributionNextFunc(BuildCtx & ctx, IHqlExpression * dataset, HqlExprArray & selects)
{
    StringBuffer s;
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual void process(IDistributionTable * * tables, const void * _src)");
    funcctx.addQuoted("unsigned char * src = (unsigned char *) _src;");

    bindTableCursor(funcctx, dataset, "src");

    ForEachItemIn(idx, selects)
    {
        IHqlExpression * original = &selects.item(idx);
        ITypeInfo * type = original->queryType();
        CHqlBoundExpr bound;

        buildExpr(funcctx, original, bound);
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
        funcctx.addQuoted(s);
    }
}

void HqlCppTranslator::doBuildDistributionFunc(BuildCtx & funcctx, unsigned numFields, const char * action)
{
    StringBuffer s;

    s.clear().append("for (unsigned i=0;i<").append(numFields).append(";i++)");
    funcctx.addQuotedCompound(s);
    s.clear().append("tables[i]->").append(action).append(";");
    funcctx.addQuoted(s);
}


void HqlCppTranslator::doBuildDistributionDestructFunc(BuildCtx & ctx, unsigned numFields)
{
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual void destruct(IDistributionTable * * tables)");
    doBuildDistributionFunc(funcctx, numFields, "Release()");
}

void HqlCppTranslator::doBuildDistributionSerializeFunc(BuildCtx & ctx, unsigned numFields)
{
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual void serialize(IDistributionTable * * tables, MemoryBuffer & out)");
    doBuildDistributionFunc(funcctx, numFields, "serialize(out)");
}

void HqlCppTranslator::doBuildDistributionMergeFunc(BuildCtx & ctx, unsigned numFields)
{
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual void merge(IDistributionTable * * tables, MemoryBuffer & in)");
    doBuildDistributionFunc(funcctx, numFields, "merge(in)");
}

void HqlCppTranslator::doBuildDistributionGatherFunc(BuildCtx & ctx, unsigned numFields)
{
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual void gatherResult(IDistributionTable * * tables, StringBuffer & out)");
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

                _ATOM name = cur->queryName();
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
    IHqlExpression * sequence = expr->queryProperty(sequenceAtom);
    IHqlExpression * name = queryPropertyChild(expr, namedAtom, 0);

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
        fields.append(*createField(unnamedAtom, makeDataType(numFields*sizeof(void*)), NULL, NULL));
        OwnedHqlExpr tempRecord = createRecord(fields);
        OwnedHqlExpr nullDataset = createDataset(no_anon, tempRecord.getLink());
        buildMetaMember(instance->classctx, nullDataset, "queryInternalRecordSize");
    }

    //Generate the send Result method().
    {
        BuildCtx funcctx(instance->startctx);
        funcctx.addQuotedCompound("virtual void sendResult(size32_t length, const char * text)");

        CHqlBoundExpr bound;
        HqlExprAttr translated;
        bound.length.setown(createVariable("length", makeIntType(sizeof(size32_t), false)));
        bound.expr.setown(createVariable("text", makeStringType(UNKNOWN_LENGTH, NULL, NULL)));
        translated.setown(bound.getTranslatedExpr());
        buildSetResultInfo(funcctx, expr, translated, NULL, false, false);
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
    instance->createGraphNode(graph, alwaysExecuted);
    if (options.trackDuplicateActivities)
    {
        IHqlExpression * search = instance->dataset;
        node_operator op = search->getOperator();
        if ((op != no_select) && (op != no_workunit_dataset))
        {
            OwnedHqlExpr searchNorm = getUnadornedExpr(search);
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
    

ColumnToOffsetMap * HqlCppTranslator::queryRecordOffsetMap(IHqlExpression * record)
{
    if (record)
        return recordMap.queryMapping(record, options.maxRecordSize);
    return NULL;
}

unsigned HqlCppTranslator::getFixedRecordSize(IHqlExpression * record)
{
    return queryRecordOffsetMap(record)->getFixedRecordSize();
}


bool HqlCppTranslator::isFixedRecordSize(IHqlExpression * record)
{
    return queryRecordOffsetMap(record)->isFixedWidth();
}

void HqlCppTranslator::buildReturnRecordSize(BuildCtx & ctx, BoundRow * cursor)
{
    OwnedHqlExpr size = getRecordSize(cursor->querySelector());
    buildReturn(ctx, size);
}


bool HqlCppTranslator::recordContainsIfBlock(IHqlExpression * record)
{
    return queryRecordOffsetMap(record)->queryContainsIfBlock();
}

IHqlExpression * HqlCppTranslator::createRecordInheritMaxLength(HqlExprArray & fields, IHqlExpression * donor)
{
    IHqlExpression * record = donor->queryRecord();
    unsigned prevLength = fields.ordinality();
    LinkedHqlExpr max;

    if (!queryProperty(maxLengthAtom, fields))
    {
        max.set(donor->queryProperty(maxLengthAtom));
        if (!max && hasMaxLength(record))
        {
            //maxlength inherited somewhere...
            OwnedHqlExpr serializedDonorRecord = getSerializedForm(record);
            ColumnToOffsetMap * map = queryRecordOffsetMap(serializedDonorRecord);
            max.setown(createAttribute(maxLengthAtom, getSizetConstant(map->getMaxSize())));
        }

        if (max)
            fields.append(*LINK(max));
    }

    OwnedHqlExpr ret = createRecord(fields);
    fields.trunc(prevLength);
    if (max)
    {
        OwnedHqlExpr serialized = getSerializedForm(ret);
        if (isFixedRecordSize(serialized))
            ret.setown(createRecord(fields));
    }
    return ret.getClear();
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
    checkAbort();

    LOG(MCdebugInfo(200), unknownJob, "Tracing expressions: %s", title);
    LogMsgCategory debug500 = MCdebugInfo(level);
    if(REJECTLOG(debug500))
        return;

    StringBuffer s;
    processedTreeToECL(expr, s);
    logECL(debug500, s.length(), s.str());
}


void HqlCppTranslator::traceExpressions(const char * title, HqlExprArray & exprs, unsigned level)
{
    checkAbort();

    // PrintLog(title);
    LOG(MCdebugInfo(200), unknownJob, "Tracing expressions: %s", title);
    LogMsgCategory debug500 = MCdebugInfo(level);
    if(REJECTLOG(debug500))
        return;

    OwnedHqlExpr compound = createComma(exprs);
    if (compound)
    {
        StringBuffer s;
        processedTreeToECL(compound, s);
        logECL(debug500, s.length(), s.str());
    }
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
            StringBuffer s;
            processedTreeToECL(compound, s);
            LOG(debug500, unknownJob, "%s: #%d: id[%d]", title, idx1, cur.queryWfid());
            logECL(debug500, s.length(), s.str());
        }
    }
}


void HqlCppTranslator::checkNormalized(WorkflowArray & workflow)
{
    if (options.paranoidCheckDependencies)
        checkDependencyConsistency(workflow);

    if (options.paranoidCheckNormalized)
    {
        ForEachItemIn(i, workflow)
        {
            OwnedHqlExpr compound = createActionList(workflow.item(i).queryExprs());
            ::checkNormalized(compound);
        }
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
}

void HqlCppTranslator::checkNormalized(HqlExprArray & exprs)
{
    if (options.paranoidCheckDependencies)
        checkDependencyConsistency(exprs);

    if (options.paranoidCheckNormalized)
    {
        ForEachItemIn(i, exprs)
            ::checkNormalized(&exprs.item(i));
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

        if ((cur.getOperator() == no_ensureresult) || cur.hasProperty(_workflow_Atom))
            return;
    }
    createCompoundEnsure(exprs, 0, max-1);
}

void HqlCppTranslator::optimizePersists(WorkflowArray & workflow)
{
    ForEachItemIn(idx, workflow)
        optimizePersists(workflow.item(idx).queryExprs());
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

void HqlCppTranslator::spotGlobalCSE(WorkflowArray & array)
{
    if (!insideLibrary() && options.globalAutoHoist)
    {
        unsigned startTime = msTick();
        ForEachItemIn(idx, array)
            spotGlobalCSE(array.item(idx).queryExprs());
        DEBUG_TIMER("EclServer: tree transform: spot global cse", msTick()-startTime);
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
        if (expr->hasProperty(_countProject_Atom) && (flags & NRTcount))
            return true;
        break;
    case no_compound_indexcount:
    case no_transformascii:
    case no_transformebcdic:
    case no_selectfields:
    case no_thor:
    case no_apply:
    case no_distributed:
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
    case no_actionlist:
    case no_externalcall:
    case no_call:
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
        {
            if (needsRealThor(expr->queryChild(0), 0))
                return true;
            if (needsRealThor(expr->queryChild(1), flags))
                return true;
            IHqlExpression * c2 = expr->queryChild(2);
            return (c2 && needsRealThor(c2, flags));
        }

    case no_colon:
    case no_globalscope:
    case no_extractresult:
        return needsRealThor(expr->queryChild(0), flags);

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
    return createAttribute(workunitAtom);       // backwards compatibility!
    IHqlExpression * dataset = expr->queryChild(0);
    if (dataset->getOperator() == no_selectfields)
        dataset = dataset->queryChild(0);
    if (dataset->getOperator()==no_choosen)
    {
        //If choosen() is specified, then output to SDS if small enough, else a temporary file.
        IHqlExpression * count = dataset->queryChild(1);
        if (count->queryValue())
        {
            unsigned __int64 value = count->queryValue()->getIntValue();
            if (value <= MAX_ROWS_OUTPUT_TO_SDS)
                return createAttribute(workunitAtom);
        }
        return createAttribute(diskAtom);
    }

    //No support yet in IFileView for delayed browsing - so output to disk.
    return createAttribute(diskAtom);
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
            if (!expr.hasProperty(workunitAtom) && !expr.hasProperty(firstAtom) && !expr.hasProperty(diskAtom))
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
        unsigned time = msTick();
        ForEachItemIn(idx, exprs)
        {
            if (needsRealThor(&exprs.item(idx)))
                return;
        }
        // if we got this far, thor not required
        setTargetClusterType(HThorCluster);
        DBGLOG("Thor query redirected to hthor instead");
        DEBUG_TIMER("EclServer: tree transform: pick engine", msTick()-time);
    }
}
    

void HqlCppTranslator::pickBestEngine(WorkflowArray & array)
{
    if (targetThor())
    {
        unsigned time = msTick();
        ForEachItemIn(idx2, array)
        {
            HqlExprArray & exprs = array.item(idx2).queryExprs();
            ForEachItemIn(idx, exprs)
            {
                if (needsRealThor(&exprs.item(idx)))
                    return;
            }
            // if we got this far, thor not required
        }
        setTargetClusterType(HThorCluster);
        DBGLOG("Thor query redirected to hthor instead");
        DEBUG_TIMER("EclServer: tree transform: pick engine", msTick()-time);
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
                    if (cur->hasProperty(virtualAtom))
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
