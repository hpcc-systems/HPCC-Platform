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
#include "hqlopt.ipp"
#include "hqlpmap.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"
#include "hqlutil.hpp"
#include "hqlfold.hpp"
#include "hqlthql.hpp"
#include "hqlerrors.hpp"

#include "hqlexpr.ipp"          // Not needed, but without it I don't see the symbols in the debugger.
#include "hqlattr.hpp"
#include "hqlmeta.hpp"

#define MIGRATE_JOIN_CONDITIONS             // This works, but I doubt it is generally worth the effort. - maybe on a flag.
//#define TRACE_USAGE

/*
Notes:
* Need to carefully keep track of usage counts after the expression tree has been transformed, otherwise activities end up being duplicated.
  o The usage count of the current expression doesn't matter since it won't be referenced any more...
  o Any replacement nodes need to inherit the link count of the item they are replacing.
  o Link counts for new children need to be incremented (they may already exist so don't set to 1).
  o Link counts for children that are no longer used should be decremented.  However since items are not
    combined if the children are shared they will no longer be referenced, so it won't be a disaster if
    that doesn't happen (note aggregate child stripping is an exception).
  o If removal of a node causes other child expressions to no longer be linked, the whole branch needs removing.
    (I don't think we currently have any examples).
  o I try and track new datasets created when projects are expanded.
  o Moving a filter over a project doesn't change the normalized inputs, so the selectorSequence doesn't need changing.

Known issues:
  o The usage counts are done at a global level, whilst the transformations are dependent on the context.  That means it might be possible
    to decrement a link count too many times, causing activities to appear unshared when in reality they are.
  o Sometimes the order the graph is traversed in produces a non optimal result.  For instance filter2(filter1(project1(x)) and filter1(project2(x))
    would best be converted to project1(filter2([filter1(x)])) and project2[filter1(x)] where filter1(x) is shared.  However it is just as likely to produce:
    project1(filter2,1(x)) and project2(filter1(x)) because the filters are also combined.
  o Similarly nodes can become unshared if
    i) an unshared node is optimized
    ii) a different (shared) node is then optimized to generate the same expression as the original.
    Because the second version is marked as shared it won't get transformed, but the first instance will have been.
    This has been worked around to a certain extent by moving some of the code into the null transformer.
  o Sharing between subqueries is too aggressive.  This is worked around by reoptimizing the subqueries.
  o Constant folding can create new datasets with no associated usage.  The code is now structured to allow the constant fold to
    be included, but I suspect it makes it too inefficient, and I don't know of any examples causing problems.

*/

//---------------------------------------------------------------------------

IHqlExpression * createFilterCondition(const HqlExprArray & conds)
{
    if (conds.ordinality() == 0)
        return createConstant(true);
    OwnedITypeInfo boolType = makeBoolType();
    return createBalanced(no_and, boolType, conds);
}


bool optimizeFilterConditions(HqlExprArray & conds)
{
    ForEachItemInRev(i, conds)
    {
        IHqlExpression & cur = conds.item(i);
        if (cur.isConstant())
        {
            OwnedHqlExpr folded = foldHqlExpression(&cur);
            IValue * value = folded->queryValue();
            if (value)
            {
                if (!value->getBoolValue())
                {
                    conds.kill();
                    conds.append(*folded.getClear());
                    return true;
                }

                conds.remove(i);
            }
        }
    }
    return conds.ordinality() == 0;
}


//---------------------------------------------------------------------------

ExpandMonitor::~ExpandMonitor()
{
    if (!complex)
    {
        unsigned max = datasetsChanged.ordinality();
        for (unsigned i=0; i < max; i+= 2)
        {
            IHqlExpression & newValue = datasetsChanged.item(i);
            IHqlExpression & oldValue = datasetsChanged.item(i+1);
            if (newValue.queryBody() != oldValue.queryBody())// && oldValue->queryTransformExtra())
                optimizer.inheritUsage(&newValue, &oldValue);
        }
    }
}

IHqlExpression * ExpandMonitor::onExpandSelector()
{
    //SELF.someField := LEFT
    complex = true;
    return NULL;
}

void ExpandMonitor::onDatasetChanged(IHqlExpression * newValue, IHqlExpression * oldValue)
{
    //NB: Cannot call inheritUsage here because a different transform is in operation
    datasetsChanged.append(*LINK(newValue));
    datasetsChanged.append(*LINK(oldValue));
}


//MORE: This needs improving... especially caching.  Probably stored in the expressions and used for filter scoring
//(cardinality, cost, ...)  - investigate some schemes + review hole implementation
static bool isComplexExpansion(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_select:
        {
            while (expr->getOperator() == no_select)
            {
                if (!expr->hasProperty(newAtom))
                    return false;
                expr = expr->queryChild(0);
            }
            return true;
        }
    case NO_AGGREGATE:
    case no_call:
    case no_externalcall:
    case no_rowdiff:
        return true;
    case no_constant:
        return false;
    }
    ForEachChild(i, expr)
        if (isComplexExpansion(expr->queryChild(i)))
            return true;
    return false;
}

void ExpandComplexityMonitor::analyseTransform(IHqlExpression * transform)
{
    ForEachChild(i, transform)
    {
        IHqlExpression * cur = transform->queryChild(i);
        switch (cur->getOperator())
        {
        case no_assignall:
            analyseTransform(cur);
            break;
        case no_assign:
            onExpand(cur->queryChild(0), cur->queryChild(1));
            break;
        case no_skip:
            if (isComplexExpansion(cur->queryChild(0)))
                complex = true;
            break;
        }
        if (complex)
            break;
    }
}


void ExpandComplexityMonitor::onExpand(IHqlExpression * select, IHqlExpression * newValue)
{
    if (complex)
        return;

    if (select->isDataset())
    {
        switch (newValue->getOperator())
        {
        case no_null:
        case no_select:
        case no_getresult:
        case no_id2blob:
            //MORE: Should be a common list somewhere...
            break;
        default:
            complex = true;
            return;
        }
    }
    if (!newValue->isPure())
        complex = true;
    else if (isComplexExpansion(newValue))
        complex = true;
}

//---------------------------------------------------------------------------
static HqlTransformerInfo cTreeOptimizerInfo("CTreeOptimizer");
CTreeOptimizer::CTreeOptimizer(unsigned _options) : PARENT(cTreeOptimizerInfo)
{
    options = _options;
    optimizeFlags |= TCOtransformNonActive;
}

IHqlExpression * CTreeOptimizer::extractFilterDs(HqlExprArray & conds, IHqlExpression * expr)
{
    if (expr->getOperator() != no_filter || isShared(expr))
        return expr;

    IHqlExpression * ds = extractFilterDs(conds, expr->queryChild(0));
    unsigned max = expr->numChildren();
    for (unsigned i = 1; i < max; i++)
    {
        IHqlExpression * cur = queryRealChild(expr, i);
        if (cur)
            cur->unwindList(conds, no_and);
    }
    return ds;
}

inline IHqlExpression * makeChildList(IHqlExpression * expr)
{
    IHqlExpression * exprList = NULL;
    unsigned num = expr->numChildren();
    for (unsigned i=1; i<num; i++)
        exprList = createComma(exprList, LINK(expr->queryChild(i)));

    return exprList;
}

IHqlExpression * CTreeOptimizer::removeChildNode(IHqlExpression * expr)
{
    IHqlExpression * child = expr->queryChild(0);
    DBGLOG("Optimizer: Node %s remove child: %s", queryNode0Text(expr), queryNode1Text(child));
    noteUnused(child);
    return replaceChild(expr, child->queryChild(0));
}

IHqlExpression * CTreeOptimizer::removeParentNode(IHqlExpression * expr)
{
    IHqlExpression * child = expr->queryChild(0);
    DBGLOG("Optimizer: Node %s remove self (now %s)", queryNode0Text(expr), queryNode1Text(child));

    // Need to dec link count of child because it is just about to inherited the link count from the parent
    decUsage(child);        
    return LINK(child);
}

IHqlExpression * CTreeOptimizer::swapNodeWithChild(IHqlExpression * parent)
{
    IHqlExpression * child = parent->queryChild(0);
    DBGLOG("Optimizer: Swap %s and %s", queryNode0Text(parent), queryNode1Text(child));
    OwnedHqlExpr newParent = swapDatasets(parent);
    //if this is the only reference to the child (almost certainly true) then no longer refd, so don't inc usage for child.
    noteUnused(child);
    if (!alreadyHasUsage(newParent))
        incUsage(newParent->queryChild(0));
    return newParent.getClear();
}

IHqlExpression * CTreeOptimizer::forceSwapNodeWithChild(IHqlExpression * parent)
{
    OwnedHqlExpr swapped = swapNodeWithChild(parent);
    return replaceOwnedProperty(swapped, getNoHoistAttr());
}

IHqlExpression * CTreeOptimizer::getNoHoistAttr()
{
    //Ensure the attribute is unique for each call to the optimizer - otherwise it stops items being hoisted that could be.
    if (!noHoistAttr)
        noHoistAttr.setown(createAttribute(_noHoist_Atom, createUniqueId()));
    return LINK(noHoistAttr);
}

IHqlExpression * CTreeOptimizer::swapNodeWithChild(IHqlExpression * parent, unsigned childIndex)
{
    IHqlExpression * child = parent->queryChild(0);
    DBGLOG("Optimizer: Swap %s and %s", queryNode0Text(parent), queryNode1Text(child));
    OwnedHqlExpr newChild = replaceChildDataset(parent, child->queryChild(childIndex), 0);
    OwnedHqlExpr swapped = insertChildDataset(child, newChild, childIndex);
    if (!alreadyHasUsage(swapped))
        incUsage(newChild);
    noteUnused(child);
    return swapped.getClear();
}

IHqlExpression * CTreeOptimizer::swapIntoIf(IHqlExpression * expr, bool force)
{
    IHqlExpression * child = expr->queryChild(0);
    //Can't optimize over a condition once a graph has been resourced, otherwise the activities aren't found.
    if (child->hasProperty(_resourced_Atom))
        return LINK(expr);

    IHqlExpression * body = expr->queryBody();
    IHqlExpression * cond = child->queryChild(0);
    IHqlExpression * left = child->queryChild(1);
    IHqlExpression * right = child->queryChild(2);

    OwnedHqlExpr newLeft = replaceChildDataset(body, left, 0);
    OwnedHqlExpr newRight = replaceChildDataset(body, right, 0);

    OwnedHqlExpr transformedLeft = transform(newLeft);
    OwnedHqlExpr transformedRight = transform(newRight);

    //Don't bother moving the condition over the if if it doesn't improve the code elsewhere
    if (force || (newLeft != transformedLeft) || (newRight != transformedRight))
    {
        //Need to call dec on all expressions that are no longer used... left and right still used by newLeft/newRight
        noteUnused(child);
        DBGLOG("Optimizer: Swap %s and %s", queryNode0Text(expr), queryNode1Text(child));
        HqlExprArray args;
        args.append(*LINK(cond));
        args.append(*LINK(transformedLeft));
        args.append(*LINK(transformedRight));
        OwnedHqlExpr ret = child->clone(args);
        if (!alreadyHasUsage(ret))
        {
            incUsage(transformedLeft);
            incUsage(transformedRight);
        }
        return ret.getClear();
    }
    return LINK(expr);
}


//NB: Similar logic to swapIntoIf()
IHqlExpression * CTreeOptimizer::swapIntoAddFiles(IHqlExpression * expr, bool force)
{
    IHqlExpression * child = expr->queryChild(0);
    IHqlExpression * body = expr->queryBody();
    bool changed = false;
    HqlExprArray replacedArgs;
    HqlExprArray transformedArgs;
    ForEachChild(idx, child)
    {
        IHqlExpression * in = child->queryChild(idx);
        if (in->isAttribute())
        {
            replacedArgs.append(*LINK(in));
            transformedArgs.append(*LINK(in));
        }
        else
        {
            IHqlExpression * next = replaceChild(body, in);
            replacedArgs.append(*next);

            //MORE: Will be linked too many times if changed and item already exists
            incUsage(next);             //Link  so values get correctly inherited if they are transformed.

            IHqlExpression * transformed = transform(next);
            transformedArgs.append(*transformed);
            if (transformed != next)
                changed = true;
        }
    }

    if (force || changed)
    {
        ForEachItemIn(i, replacedArgs)
        {
            if (&replacedArgs.item(i) != &transformedArgs.item(i))
                decUsage(&replacedArgs.item(i));            //If they are the same then inheritUsage wont't have been called, so don't decrement.
        }

        //Need to call dec on all expressions that are no longer used... grand children should not be decremented
        noteUnused(child);

        //And create the new funnel
        DBGLOG("Optimizer: Swap %s and %s", queryNode0Text(expr), queryNode1Text(child));
        return child->clone(transformedArgs);
    }

    //Note, replaced == args so no need to call decUsage on args
    ForEachItemIn(i, replacedArgs)
    {
        IHqlExpression & cur = replacedArgs.item(i);
        if (!cur.isAttribute())
            decUsage(&cur);         //If they are the same then inheritUsage wont't have been called, so don't decrement.
    }
    return LINK(expr);
}


IHqlExpression * CTreeOptimizer::moveFilterOverSelect(IHqlExpression * expr)
{
    IHqlExpression * select = expr->queryChild(0);
    if (!select->hasProperty(newAtom))
        return NULL;
    IHqlExpression * ds = select->queryChild(0);
    IHqlExpression * newScope = select->queryNormalizedSelector();
    HqlExprArray args, hoisted, notHoisted;
    HqlExprCopyArray inScope;
    unwindFilterConditions(args, expr);

    ForEachItemIn(i, args)
    {
        IHqlExpression & cur = args.item(i);
        inScope.kill();
        cur.gatherTablesUsed(NULL, &inScope);
        if (inScope.find(*newScope) == NotFound)
            hoisted.append(OLINK(cur));
        else
            notHoisted.append(OLINK(cur));
    }

    if (hoisted.ordinality() == 0)
        return NULL;

    DBGLOG("Optimizer: Move filter over select (%d/%d)", hoisted.ordinality(), args.ordinality());

    //Create a filtered dataset
    IHqlExpression * inDs = LINK(ds);
    if (inDs->isDatarow())
        inDs = createDatasetFromRow(inDs);
    hoisted.add(*inDs, 0);

    OwnedHqlExpr newDs = expr->clone(hoisted);

    //Now a select on that
    args.kill();
    unwindChildren(args, select);
    args.replace(*LINK(newDs), 0);
    OwnedHqlExpr newSelect = select->clone(args);

    if (!alreadyHasUsage(newSelect))
        incUsage(newDs);

    if (notHoisted.ordinality())
    {
        notHoisted.add(*LINK(select), 0);
        OwnedHqlExpr unhoistedFilter = expr->clone(notHoisted);
        OwnedHqlExpr ret = replaceChild(unhoistedFilter, newSelect);

        if (!alreadyHasUsage(ret))
            incUsage(newSelect);
        return ret.getClear();
    }
    return newSelect.getClear();
}


IHqlExpression * CTreeOptimizer::optimizeAggregateUnsharedDataset(IHqlExpression * expr, bool isSimpleCount)
{
    if (isShared(expr) || (getNumChildTables(expr) != 1))
        return LINK(expr);

    //Don't include any operations which rely on the order/distribution:
    bool childIsSimpleCount = isSimpleCount;
    node_operator op = expr->getOperator();
    IHqlExpression * ds = expr->queryChild(0);
    switch (op)
    {
    case no_filter:
    case no_aggregate:
        childIsSimpleCount = false;
        break;
    case no_hqlproject:
    case no_newusertable:
    case no_newaggregate:
    case no_sort:
    case no_distribute:
    case no_keyeddistribute:
    case no_fetch:
    case no_transformebcdic:
    case no_transformascii:
        if (childIsSimpleCount && !isPureActivity(expr))
            childIsSimpleCount = false;
        break;
    case no_compound_indexread:
    case no_compound_diskread:
        break;
    case no_limit:
        if (expr->hasProperty(onFailAtom))
            return LINK(expr);
        //fall through
    case no_choosen:
    case no_topn:
        if (isSimpleCount)
            break;
        return LINK(expr);
    default:
        return LINK(expr);
    }

    OwnedHqlExpr optimizedDs = optimizeAggregateUnsharedDataset(ds, childIsSimpleCount);

    //Remove items that are really inefficient and unnecessary, but don't for the moment remove projects or anything that changes the
    //record structure.
    switch (op)
    {
    case no_sort:
    case no_distribute:
    case no_keyeddistribute:
        noteUnused(expr);
        return optimizedDs.getClear();
    case no_topn:
        {
            assertex(isSimpleCount);
            noteUnused(expr);
            OwnedHqlExpr ret = createDataset(no_choosen, optimizedDs.getClear(), LINK(expr->queryChild(2)));
            incUsage(ret);
            return expr->cloneAllAnnotations(ret);
        }
    case no_hqlproject:
    case no_newusertable:
        if (isSimpleCount && (options & HOOinsidecompound))
        {
            if (expr->hasProperty(_countProject_Atom) || expr->hasProperty(prefetchAtom))
                break;
            if (isPureActivity(expr) && !isAggregateDataset(expr))
            {
                noteUnused(expr);
                return optimizedDs.getClear();
            }
        }
        break;
    }

    if (ds == optimizedDs)
        return LINK(expr);

    OwnedHqlExpr replaced = replaceChild(expr, optimizedDs);
    incUsage(replaced);
    noteUnused(expr);
    return replaced.getClear();
}

IHqlExpression * CTreeOptimizer::optimizeAggregateDataset(IHqlExpression * transformed)
{
    HqlExprArray children;
    unwindChildren(children, transformed);

    IHqlExpression * root = &children.item(0);
    HqlExprAttr ds = root;
    IHqlExpression * wrapper = NULL;
    node_operator aggOp = transformed->getOperator();
    bool insideShared = false;
    bool isScalarAggregate = (aggOp != no_newaggregate) && (aggOp != no_aggregate);
    bool isSimpleCount = isSimpleCountExistsAggregate(transformed, false, true);
    loop
    {
        node_operator dsOp = ds->getOperator();
        IHqlExpression * next = NULL;
        switch (dsOp)
        {
        case no_hqlproject:
        case no_newusertable:
            if (ds->hasProperty(prefetchAtom))
                break;

            //Don't remove projects for the moment because they can make counts of disk reads much less
            //efficient.  Delete the following lines once we have a count-diskread activity
            if (!isScalarAggregate && !(options & (HOOcompoundproject|HOOinsidecompound)) && !ds->hasProperty(_countProject_Atom) )
                break;
            if (isPureActivity(ds) && !isAggregateDataset(ds))
            {
                OwnedMapper mapper = getMapper(ds);
                ExpandSelectorMonitor expandMonitor(*this);
                HqlExprArray newChildren;
                unsigned num = children.ordinality();
                LinkedHqlExpr oldDs = ds;
                LinkedHqlExpr newDs = ds->queryChild(0);
                if (transformed->getOperator() == no_aggregate)
                {
                    oldDs.setown(createSelector(no_left, ds, querySelSeq(transformed)));
                    newDs.setown(createSelector(no_left, newDs, querySelSeq(transformed)));
                }
                for (unsigned idx = 1; idx < num; idx++)
                {
                    OwnedHqlExpr mapped = expandFields(mapper, &children.item(idx), oldDs, newDs, &expandMonitor);
                    if (containsCounter(mapped))
                        expandMonitor.setComplex();
                    newChildren.append(*mapped.getClear());
                }
                if (!expandMonitor.isComplex())
                {
                    for (unsigned idx = 1; idx < num; idx++)
                        children.replace(OLINK(newChildren.item(idx-1)), idx);
                    next = ds->queryChild(0);
                }
            }
            break;
        case no_fetch:
            if (ds->queryChild(3)->isPure())
                next = ds->queryChild(1);
            break;
        case no_group:
            if (isScalarAggregate)
                next = ds->queryChild(0);
            break;
        case no_sort:
        case no_sorted:
            //MORE: Allowed if the transform is commutative for no_aggregate
            if (aggOp != no_aggregate)
                next = ds->queryChild(0);
            break;
        case no_distribute:
        case no_distributed:
        case no_keyeddistribute:
        case no_preservemeta:
            if (isScalarAggregate || !isGrouped(ds->queryChild(0)))
                next = ds->queryChild(0);
            break;
        case no_preload:
            wrapper = ds;
            next = ds->queryChild(0);
            break;
        }

        if (!next)
            break;

        if (!insideShared)
        {
            insideShared = isShared(ds);
            noteUnused(ds);
        }
        ds.set(next);
    }

    //Not completely sure about usageCounting being maintained correctly
    if (!insideShared)
    {
        OwnedHqlExpr newDs = (aggOp != no_aggregate) ? optimizeAggregateUnsharedDataset(ds, isSimpleCount) : LINK(ds);
        if (newDs != ds)
        {
            HqlMapTransformer mapper;
            mapper.setMapping(ds, newDs);
            mapper.setSelectorMapping(ds, newDs);
            ForEachItemIn(i, children)
                children.replace(*mapper.transformRoot(&children.item(i)), i);
            ds.set(newDs);
        }
    }

    if (ds == root)
        return LINK(transformed);

    if (wrapper)
    {
        if (ds == root->queryChild(0))
        {
            incUsage(root);
            return LINK(transformed);
        }
    }

    //A different node is now shared between the graphs
    if (insideShared)
        incUsage(ds);

    if (wrapper)
    {
        HqlExprArray args;
        args.append(*ds.getClear());
        unwindChildren(args, wrapper, 1);
        ds.setown(wrapper->clone(args));
        incUsage(ds);
    }

    DBGLOG("Optimizer: Aggregate replace %s with %s", queryNode0Text(root), queryNode1Text(ds));
    children.replace(*ds.getClear(), 0);
    return transformed->clone(children);
}


IHqlExpression * CTreeOptimizer::optimizeDatasetIf(IHqlExpression * transformed)
{
    //if(cond, ds(filt1), ds(filt2)) => ds(if(cond,filt1,filt2))
    HqlExprArray leftFilter, rightFilter;
    IHqlExpression * left = extractFilterDs(leftFilter, transformed->queryChild(1));
    IHqlExpression * right = extractFilterDs(rightFilter, transformed->queryChild(2));
    if (left->queryBody() == right->queryBody())
    {
        HqlExprArray args;
        args.append(*LINK(left));
//                  intersectConditions(args, leftFilter, rightFilter);
        OwnedHqlExpr leftCond = createFilterCondition(leftFilter);
        OwnedHqlExpr rightCond = createFilterCondition(rightFilter);
        if (leftCond == rightCond)
        {
            args.append(*leftCond.getClear());
        }
        else
        {
            IHqlExpression * cond = transformed->queryChild(0);
            args.append(*createValue(no_if, cond->getType(), LINK(cond), leftCond.getClear(), rightCond.getClear()));
        }

        OwnedHqlExpr ret = createDataset(no_filter, args);

        DBGLOG("Optimizer: Convert %s to a filter", queryNode0Text(transformed));

        //NOTE: left and right never walk over any shared nodes, so don't need to decrement usage for 
        //child(1), child(2) or intermediate nodes to left/right, since not referenced any more.
        noteUnused(right);      // dataset is now used one less time
        return transformed->cloneAllAnnotations(ret);
    }
    return LINK(transformed);
}

IHqlExpression * CTreeOptimizer::optimizeIf(IHqlExpression * expr)
{
    IHqlExpression * trueExpr = expr->queryChild(1);
    IHqlExpression * falseExpr = expr->queryChild(2);

    if (!falseExpr)
        return NULL;

    if (trueExpr->queryBody() == falseExpr->queryBody())
    {
        noteUnused(trueExpr);       // inherit usage() will increase the usage again
        noteUnused(falseExpr);
        return LINK(trueExpr);
    }

    IHqlExpression * cond = expr->queryChild(0);
    IValue * condValue = cond->queryValue();
    if (condValue)
    {
        if (condValue->getBoolValue())
        {
            recursiveDecUsage(falseExpr);
            decUsage(trueExpr);     // inherit usage() will increase the usage again
            return LINK(trueExpr);
        }
        else
        {
            recursiveDecUsage(trueExpr);
            decUsage(falseExpr);        // inherit usage() will increase the usage again
            return LINK(falseExpr);
        }
    }

    //Usage counts aren't handled correctly for datarows, so only optimize datasets, otherwise it can get bigger.
    if (!expr->isDataset())
        return NULL;

    //if(c1, if(c2, x, y), z)   y==z => if(c1 &&  c2, x, z)
    //if(c1, if(c2, x, y), z)   x==z => if(c1 && !c2, y, z)
    //if(c1, z, if(c2, x, y))   x==z => if(c1 ||  c2, z, y)
    //if(c1, z, if(c2, x, y))   y==z => if(c1 || !c2, z, x)
    //Only do these changes if c2 has no additional dependencies than c1
    HqlExprArray args;
    if ((trueExpr->getOperator() == no_if) && !isShared(trueExpr))
    {
        IHqlExpression * childCond = trueExpr->queryChild(0);
        if (introducesNewDependencies(cond, childCond))
            return NULL;

        IHqlExpression * childTrue = trueExpr->queryChild(1);
        IHqlExpression * childFalse = trueExpr->queryChild(2);
        if (falseExpr->queryBody() == childFalse->queryBody())
        {
            args.append(*createBoolExpr(no_and, LINK(cond), LINK(childCond)));
            args.append(*LINK(childTrue));
            args.append(*LINK(falseExpr));
        }
        else if (falseExpr->queryBody() == childTrue->queryBody())
        {
            args.append(*createBoolExpr(no_and, LINK(cond), getInverse(childCond)));
            args.append(*LINK(childFalse));
            args.append(*LINK(falseExpr));
        }

        if (args.ordinality())
        {
            DBGLOG("Optimizer: Merge %s and %s", queryNode0Text(expr), queryNode1Text(trueExpr));
            noteUnused(falseExpr);
        }
    }
    if (args.empty() && (falseExpr->getOperator() == no_if) && !isShared(falseExpr))
    {
        IHqlExpression * childCond = falseExpr->queryChild(0);
        if (introducesNewDependencies(cond, childCond))
            return NULL;

        IHqlExpression * childTrue = falseExpr->queryChild(1);
        IHqlExpression * childFalse = falseExpr->queryChild(2);
        if (trueExpr->queryBody() == childTrue->queryBody())
        {
            args.append(*createBoolExpr(no_or, LINK(cond), LINK(childCond)));
            args.append(*LINK(trueExpr));
            args.append(*LINK(childFalse));
        }
        else if (trueExpr->queryBody() == childFalse->queryBody())
        {
            args.append(*createBoolExpr(no_or, LINK(cond), getInverse(childCond)));
            args.append(*LINK(trueExpr));
            args.append(*LINK(childTrue));
        }

        if (args.ordinality())
        {
            DBGLOG("Optimizer: Merge %s and %s", queryNode0Text(expr), queryNode1Text(falseExpr));
            noteUnused(trueExpr);
        }
    }

    if (args.ordinality())
        return expr->clone(args);
    return NULL;
}


bool CTreeOptimizer::expandFilterCondition(HqlExprArray & expanded, HqlExprArray & unexpanded, IHqlExpression * filter, bool moveOver, bool onlyKeyed)
{
    HqlExprArray conds;
    unwindFilterConditions(conds, filter);

    IHqlExpression * child = filter->queryChild(0);
    IHqlExpression * grandchild = child->queryChild(0);
    OwnedMapper mapper = getMapper(child);
    ForEachItemIn(i, conds)
    {
        IHqlExpression * cur = &conds.item(i);
        bool isKeyed = containsAssertKeyed(cur);
        if (!onlyKeyed || isKeyed || (options & HOOfiltersharedproject) )
        {
            ExpandComplexityMonitor expandMonitor(*this);
            OwnedHqlExpr expandedFilter;
            if (moveOver)
                expandedFilter.setown(expandFields(mapper, cur, child, grandchild, &expandMonitor));
            else
                expandedFilter.setown(mapper->expandFields(cur, child, grandchild, grandchild, &expandMonitor));

            if (expandedFilter->isConstant())
            {
                expandedFilter.setown(foldHqlExpression(expandedFilter));
                IValue * value = expandedFilter->queryValue();
                if (value && !value->getBoolValue())
                {
                    if (onlyKeyed)
                        DBGLOG("Optimizer: Merging filter over shared project always false");
                    expanded.kill();
                    expanded.append(*LINK(expandedFilter));
                    return true;
                }
            }

            if ((!onlyKeyed || isKeyed) && !expandMonitor.isComplex())
                expanded.append(*LINK(expandedFilter));
            else
                unexpanded.append(*LINK(cur));
        }
        else
            unexpanded.append(*LINK(cur));
    }
    return expanded.ordinality() != 0;
}


IHqlExpression * CTreeOptimizer::hoistMetaOverProject(IHqlExpression * expr)
{
    IHqlExpression * child = expr->queryChild(0);
    if (hasUnknownTransform(child))
        return NULL;

    IHqlExpression * grandchild = child->queryChild(0);
    IHqlExpression * active = queryActiveTableSelector();

    try
    {
        OwnedMapper mapper = getMapper(child);
        HqlExprArray args;
        args.append(*LINK(grandchild));
        ForEachChildFrom(i, expr, 1)
        {
            IHqlExpression * cur = expr->queryChild(i);
            args.append(*expandFields(mapper, cur, active, active, NULL));
        }

        OwnedHqlExpr newPreserve = expr->clone(args);
        OwnedHqlExpr newProject = replaceChild(child, newPreserve);
        decUsage(child);
        if (!alreadyHasUsage(newProject))
            incUsage(newPreserve);
        return newProject.getClear();
    }
    catch (IException * e)
    {
        //Can possibly occur if the field has been optimized away. (see bug #76896)
        e->Release();
        return NULL;
    }
}

IHqlExpression * CTreeOptimizer::hoistFilterOverProject(IHqlExpression * transformed, bool onlyKeyed)
{
    IHqlExpression * child = transformed->queryChild(0);

    //Should be able to move filters over count projects, as long as not filtering on the count fields.  
    //Would need to add a containsCounter() test in the expandFields code - cannot just test filterExpr
    //because counter may be there (e.g., countindex3.hql)
    if (child->hasProperty(_countProject_Atom) || child->hasProperty(prefetchAtom) || isAggregateDataset(child))
        return NULL;
    if (hasUnknownTransform(child))
        return NULL;

    HqlExprArray expanded, unexpanded;
    if (expandFilterCondition(expanded, unexpanded, transformed, true, onlyKeyed))
    {
        if (optimizeFilterConditions(expanded))
            return getOptimizedFilter(transformed, expanded);

        OwnedHqlExpr filterExpr = createFilterCondition(expanded);
        if (unexpanded.ordinality())
            DBGLOG("Optimizer: Move %d/%d filters over %s", expanded.ordinality(), expanded.ordinality()+unexpanded.ordinality(), queryNode1Text(child));
        else
            DBGLOG("Optimizer: Swap %s and %s", queryNode0Text(transformed), queryNode1Text(child));

        IHqlExpression * newGrandchild = child->queryChild(0);
        OwnedHqlExpr newFilter = createDataset(no_filter, LINK(newGrandchild), LINK(filterExpr));
        newFilter.setown(transformed->cloneAllAnnotations(newFilter));
        OwnedHqlExpr ret = replaceChild(child, newFilter);
        if (!alreadyHasUsage(ret))
            incUsage(newFilter);
        noteUnused(child);
        if (unexpanded.ordinality() == 0)
            return ret.getClear();

        unexpanded.add(*LINK(child), 0);
        OwnedHqlExpr unhoistedFilter = transformed->clone(unexpanded);
        OwnedHqlExpr newUnhoistedFilter = replaceChild(unhoistedFilter, ret);
        if (!alreadyHasUsage(newUnhoistedFilter))
            incUsage(ret);
        return newUnhoistedFilter.getClear();
    }

    return NULL;
}

IHqlExpression * CTreeOptimizer::getHoistedFilter(IHqlExpression * transformed, bool canHoistLeft, bool canMergeLeft, bool canHoistRight, bool canMergeRight, unsigned conditionIndex)
{
    HqlExprArray conds;
    unwindFilterConditions(conds, transformed);

    IHqlExpression * child = transformed->queryChild(0);
    IHqlExpression * left = child->queryChild(0);
    IHqlExpression * right = queryJoinRhs(child);
    IHqlExpression * seq = querySelSeq(child);
    OwnedHqlExpr leftSelector = createSelector(no_left, left, seq);
    OwnedHqlExpr rightSelector = createSelector(no_right, right, seq);
    OwnedHqlExpr activeLeft = ensureActiveRow(left);
    OwnedHqlExpr activeRight = ensureActiveRow(right);

    OwnedMapper mapper = getMapper(child);
    HqlExprArray expanded, unexpanded, leftFilters, rightFilters;;
    ForEachItemIn(i, conds)
    {
        ExpandComplexityMonitor expandMonitor(*this);
        IHqlExpression * cur = &conds.item(i);
        OwnedHqlExpr expandedFilter = mapper->expandFields(cur, child, NULL, NULL, &expandMonitor);
        bool matched = false;

        if (expandedFilter->isConstant())
        {
            expandedFilter.setown(foldHqlExpression(expandedFilter));
            IValue * value = expandedFilter->queryValue();
            if (value)
            {
                if (!value->getBoolValue())
                    return getOptimizedFilter(transformed, false);
                else
                    matched = true;
            }
        }

        if (!matched && !expandMonitor.isComplex())
        {
            OwnedHqlExpr leftMappedFilter = replaceSelector(expandedFilter, leftSelector, activeLeft);
            OwnedHqlExpr rightMappedFilter = replaceSelector(expandedFilter, rightSelector, activeRight);

            //MORE: Could also take join conditions into account to sent filter up both sides;
            if (rightMappedFilter==expandedFilter)
            {
                //Only contains LEFT.
                if (canHoistLeft)
                {
                    leftFilters.append(*LINK(leftMappedFilter));
                    matched = true;
                }
                else if (canMergeLeft && (conditionIndex != NotFound))
                {
                    expanded.append(*LINK(expandedFilter));
                    matched = true;
                }
                //If the filter expression is invariant of left and right then hoist up both paths.
                if (leftMappedFilter==expandedFilter && canHoistRight)
                {
                    rightFilters.append(*LINK(expandedFilter));
                    matched = true;
                }
            }
            else if (leftMappedFilter==expandedFilter)
            {
                //Only contains RIGHT.
                if (canHoistRight)
                {
                    rightFilters.append(*LINK(rightMappedFilter));
                    matched = true;
                }
                else if (canMergeRight && (conditionIndex != NotFound))
                {
                    expanded.append(*LINK(expandedFilter));
                    matched = true;
                }
            }
            else if (canMergeLeft && canMergeRight && conditionIndex != NotFound)
            {
                expanded.append(*LINK(expandedFilter));
                matched = true;
            }
        }

        if (!matched)
            unexpanded.append(*LINK(cur));
    }

    if (leftFilters.ordinality() || rightFilters.ordinality() || expanded.ordinality())
    {
        LinkedHqlExpr ret = child;
        //first insert filters on the left/right branches
        if (leftFilters.ordinality())
            ret.setown(createHoistedFilter(ret, leftFilters, 0, conds.ordinality()));
        if (rightFilters.ordinality())
            ret.setown(createHoistedFilter(ret, rightFilters, 1, conds.ordinality()));

        //extend the join condition where appropriate
        if (expanded.ordinality())
        {
            DBGLOG("Optimizer: Merge filters(%d/%d) into %s condition", expanded.ordinality(), conds.ordinality(), queryNode1Text(child));
            OwnedITypeInfo boolType = makeBoolType();
            HqlExprArray args;
            unwindChildren(args, ret);
            expanded.add(OLINK(args.item(conditionIndex)), 0);
            args.replace(*createBalanced(no_and, boolType, expanded), conditionIndex);
            ret.setown(ret->clone(args));
        }

        if (ret != child)
            noteUnused(child);

        //Now add the item that couldn't be hoisted.
        if (unexpanded.ordinality())
        {
            if (ret != child)
                incUsage(ret);

            unexpanded.add(*LINK(child), 0);
            OwnedHqlExpr unhoistedFilter = transformed->clone(unexpanded);
            ret.setown(replaceChild(unhoistedFilter, ret));
        }
        return ret.getClear();
    }
    else if (unexpanded.ordinality() == 0)
        //All filters expanded to true => remove the filter
        return getOptimizedFilter(transformed, true)        ;

    return NULL;
}


IHqlExpression * CTreeOptimizer::createHoistedFilter(IHqlExpression * expr, HqlExprArray & conditions, unsigned childIndex, unsigned maxConditions)
{
    IHqlExpression * grand = expr->queryChild(childIndex);
    DBGLOG("Optimizer: Hoisting filter(%d/%d) over %s.%d", conditions.ordinality(), maxConditions, queryNode0Text(expr), childIndex);
    conditions.add(*LINK(grand), 0);
    OwnedHqlExpr hoistedFilter = createDataset(no_filter, conditions);
    OwnedHqlExpr ret = insertChildDataset(expr, hoistedFilter, childIndex);
    if (!alreadyHasUsage(ret))
        incUsage(hoistedFilter);
    return ret.getClear();
}


IHqlExpression * CTreeOptimizer::queryPromotedFilter(IHqlExpression * expr, node_operator side, unsigned childIndex)
{
    IHqlExpression * child = expr->queryChild(0);
    IHqlExpression * grand = child->queryChild(childIndex);
    OwnedMapper mapper = getMapper(child);

    HqlExprArray conds;
    unwindFilterConditions(conds, expr);

    HqlExprArray hoisted, unhoisted;
    OwnedHqlExpr mapParent = createSelector(side, grand, querySelSeq(child));
    ForEachItemIn(i1, conds)
    {
        IHqlExpression & cur = conds.item(i1);
        bool ok = false;
        OwnedHqlExpr collapsed = mapper->collapseFields(&cur, child, grand, mapParent, &ok);
        if (ok)
            hoisted.append(*collapsed.getClear());
        else
            unhoisted.append(OLINK(cur));
    }

    if (hoisted.ordinality() == 0)
        return NULL;

    DBGLOG("Optimizer: Hoisting filter(%d/%d) over %s", hoisted.ordinality(), hoisted.ordinality()+unhoisted.ordinality(), queryNode0Text(child));

    OwnedHqlExpr newChild = createHoistedFilter(child, hoisted, childIndex, conds.ordinality());
    noteUnused(child);
    if (unhoisted.ordinality() == 0)
        return newChild.getLink();

    unhoisted.add(*LINK(child), 0);
    OwnedHqlExpr unhoistedFilter = createDataset(no_filter, unhoisted);
    OwnedHqlExpr newUnhoistedFilter = replaceChild(unhoistedFilter, newChild);
    if (!alreadyHasUsage(newUnhoistedFilter))
        incUsage(newChild);
    return newUnhoistedFilter.getClear();
}



bool CTreeOptimizer::extractSingleFieldTempTable(IHqlExpression * expr, SharedHqlExpr & retField, SharedHqlExpr & retValues)
{
    IHqlExpression * record = expr->queryRecord();
    IHqlExpression * field = NULL;
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
        case no_ifblock:
            return false;
        case no_field:
            if (cur->queryRecord() || field)
                return false;
            field = cur;
            break;
        }
    }
    if (!field)
        return false;

    OwnedHqlExpr values = normalizeListCasts(expr->queryChild(0));
    switch (values->getOperator())
    {
    case no_null:
        break;
    case no_recordlist:
        {
            HqlExprArray args;
            ITypeInfo * fieldType = field->queryType();
            ForEachChild(i, values)
            {
                IHqlExpression * cur = values->queryChild(i);
                if (cur->getOperator() != no_rowvalue)
                    return false;
                args.append(*ensureExprType(cur->queryChild(0), fieldType));
            }
            values.setown(createValue(no_list, makeSetType(LINK(fieldType)), args));
        }
        break;
    default:
        if (values->queryType()->getTypeCode() != type_set)
            return false;
        break;
    }

    retField.set(field);
    retValues.setown(values.getClear());
    return true;
}


IHqlExpression * mapJoinConditionToFilter(IHqlExpression * expr, IHqlExpression * search, IHqlExpression * replace)
{
    switch (expr->getOperator())
    {
    case no_and:
    case no_or:
        {
            HqlExprArray args;
            ForEachChild(i, expr)
            {
                IHqlExpression * mapped = mapJoinConditionToFilter(expr->queryChild(i), search, replace);
                if (!mapped)
                    return NULL;
                args.append(*mapped);
            }
            return expr->clone(args);
        }
    case no_eq:
        {
            IHqlExpression * l = expr->queryChild(0);
            IHqlExpression * r = expr->queryChild(1);
            if (l == search)
                return createValue(no_in, makeBoolType(), LINK(r), LINK(replace));
            if (r == search)
                return createValue(no_in, makeBoolType(), LINK(l), LINK(replace));
            break;
        }
    }

    OwnedHqlExpr temp = replaceExpression(expr, search, replace);
    if (temp != expr)
        return NULL;
    return LINK(expr);
}


/*
Convert join(inline-dataset, x, condition, transform, ...) to
project(x(condition'), t')
*/

IHqlExpression * CTreeOptimizer::optimizeInlineJoin(IHqlExpression * expr)
{
    //This doesn't really work because the input dataset could contain duplicates, which would generate duplicate
    //values for the keyed join, but not for the index read.
    //I could spot a dedup(ds, all) and then allow it, but it's a bit messy.
    return NULL;

    if (!isSimpleInnerJoin(expr) || expr->hasProperty(keyedAtom))
        return NULL;

    //Probably probably keep the following...
    if (expr->hasProperty(allAtom) || expr->hasProperty(_lightweight_Atom) || expr->hasProperty(lookupAtom) || 
        expr->hasProperty(hashAtom))
        return NULL;

    if (expr->hasProperty(localAtom) || expr->hasProperty(atmostAtom) || expr->hasProperty(onFailAtom))
        return NULL;

    IHqlExpression * key = expr->queryChild(1);
    switch (key->getOperator())
    {
    case no_newkeyindex:
        //more - e.g., inline child query stuff
        break;
    default:
        //probably always more efficient.
        break;
        return false;
    }

    IHqlExpression * tempTable = expr->queryChild(0);
    if (tempTable->getOperator() != no_temptable)
        return NULL;

    OwnedHqlExpr field, values;
    if (!extractSingleFieldTempTable(tempTable, field, values))
        return NULL;

    IHqlExpression * joinSeq = querySelSeq(expr);
    OwnedHqlExpr newSeq = createSelectorSequence();
    OwnedHqlExpr left = createSelector(no_left, tempTable, joinSeq);
    OwnedHqlExpr right = createSelector(no_right, key, joinSeq);
    OwnedHqlExpr rightAsLeft = createSelector(no_left, key, newSeq);
    OwnedHqlExpr selectLeft = createSelectExpr(LINK(left), LINK(field));
    OwnedHqlExpr activeDs = ensureActiveRow(key);

    //Transform can't refer to left hand side.
    IHqlExpression * transform = expr->queryChild(3);
    OwnedHqlExpr mapped = replaceExpression(transform, left, right);
    if (mapped != transform)
        return NULL;

    OwnedHqlExpr cond = replaceSelector(expr->queryChild(2), right, activeDs);
    OwnedHqlExpr mappedCond = mapJoinConditionToFilter(cond, selectLeft, values);
    if (!mappedCond)
        return NULL;

    OwnedHqlExpr replacement = createDataset(no_filter, LINK(key), mappedCond.getClear());

    OwnedHqlExpr newTransform = replaceExpression(transform, right, rightAsLeft);
    replacement.setown(createDataset(no_hqlproject, replacement.getClear(), createComma(newTransform.getClear(), LINK(newSeq))));
    return replacement.getClear();
}

    
IHqlExpression * splitJoinFilter(IHqlExpression * expr, HqlExprArray * leftOnly, HqlExprArray * rightOnly)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_assertkeyed:
    case no_and:
        {
            HqlExprArray args;
            ForEachChild(i, expr)
            {
                IHqlExpression * next = splitJoinFilter(expr->queryChild(i), leftOnly, rightOnly);
                if (next)
                    args.append(*next);
            }
            unsigned numRealArgs = args.ordinality() - numAttributes(args);
            if (numRealArgs == 0)
                return NULL;
            if ((numRealArgs == 1) && (op == no_and))
                return LINK(&args.item(0));
            return cloneOrLink(expr, args);
        }
    }

    HqlExprCopyArray scopeUsed;
    expr->gatherTablesUsed(NULL, &scopeUsed);
    if (scopeUsed.ordinality() == 1)
    {
        node_operator scopeOp = scopeUsed.item(0).getOperator();
        if (leftOnly && scopeOp == no_left)
        {
            leftOnly->append(*LINK(expr));
            return NULL;
        }
        if (rightOnly && scopeOp == no_right)
        {
            rightOnly->append(*LINK(expr));
            return NULL;
        }
    }
    return LINK(expr);
}


IHqlExpression * CTreeOptimizer::optimizeJoinCondition(IHqlExpression * expr)
{
    //Look at the join condition and move any conditions just on left/right further up the tree
    //can help after other constant folding....
    if (!isSimpleInnerJoin(expr) || expr->hasProperty(keyedAtom) || expr->hasProperty(atmostAtom))
        return NULL;

    IHqlExpression * cond = expr->queryChild(2);
    IHqlExpression * seq = querySelSeq(expr);
    HqlExprArray leftOnly, rightOnly;
    OwnedHqlExpr newCond = splitJoinFilter(cond, &leftOnly, isKeyedJoin(expr) ? (HqlExprArray *)NULL : &rightOnly);
    if ((leftOnly.ordinality() == 0) && (rightOnly.ordinality() == 0))
        return NULL;

    HqlExprArray args;
    unwindChildren(args, expr);

    if (leftOnly.ordinality())
    {
        DBGLOG("Optimizer: Hoist %d LEFT conditions out of %s", leftOnly.ordinality(), queryNode0Text(expr));
        IHqlExpression * lhs = expr->queryChild(0);
        OwnedHqlExpr left = createSelector(no_left, lhs, seq);
        OwnedHqlExpr leftFilter = createFilterCondition(leftOnly);
        OwnedHqlExpr newFilter = replaceSelector(leftFilter, left, lhs->queryNormalizedSelector());
        args.replace(*createDataset(no_filter, LINK(lhs), LINK(newFilter)), 0);
        incUsage(&args.item(0));
    }

    if (rightOnly.ordinality())
    {
        DBGLOG("Optimizer: Hoist %d RIGHT conditions out of %s", rightOnly.ordinality(), queryNode0Text(expr));
        IHqlExpression * rhs = expr->queryChild(1);
        OwnedHqlExpr right = createSelector(no_right, rhs, seq);
        OwnedHqlExpr rightFilter = createFilterCondition(rightOnly);
        OwnedHqlExpr newFilter = replaceSelector(rightFilter, right, rhs->queryNormalizedSelector());
        args.replace(*createDataset(no_filter, LINK(rhs), LINK(newFilter)), 1);
        incUsage(&args.item(1));
    }

    if (!newCond)
        newCond.setown(createConstant(true));

    if (!queryProperty(_conditionFolded_Atom, args))
        args.append(*createAttribute(_conditionFolded_Atom));
    args.replace(*newCond.getClear(), 2);
    return expr->clone(args);
}


//DISTRIBUTE(DEDUP(ds, x, y, all), hash(trim(x)))
//It is likely that the following would be better since it removes one distribute:
//DEDUP(DISTRIBUTE(ds, hash(trim(x))), x, y, all, LOCAL)
IHqlExpression * CTreeOptimizer::optimizeDistributeDedup(IHqlExpression * expr)
{
    IHqlExpression * child = expr->queryChild(0);

    if (!child->hasProperty(allAtom) || child->hasProperty(localAtom) || isGrouped(child))
        return NULL;

    DedupInfoExtractor info(child);
    if (info.equalities.ordinality() == 0)
        return NULL;

    IHqlExpression * dist = expr->queryChild(1);
    if (!matchDedupDistribution(dist, info.equalities))
        return NULL;

    DBGLOG("Optimizer: Swap %s and %s", queryNode0Text(expr), queryNode1Text(child));
    
    
    OwnedHqlExpr distn;
    if (expr->hasProperty(manyAtom))
    {
        //DEDUP(DISTRIBUTE(DEDUP(ds, x, y, all, local), hash(trim(x))), x, y, all, LOCAL)

        HqlExprArray localDedupArgs;
        unwindChildren(localDedupArgs, child);
        localDedupArgs.append(*createLocalAttribute());
        localDedupArgs.append(*createAttribute(hashAtom));
        OwnedHqlExpr localDedup = child->clone(localDedupArgs);

        distn.setown(replaceChildDataset(expr, localDedup, 0));
    }
    else
    {
        //DEDUP(DISTRIBUTE(ds, hash(trim(x))), x, y, all, LOCAL)
        distn.setown(replaceChildDataset(expr, child->queryChild(0), 0));
    }

    HqlExprArray args;
    args.append(*LINK(distn));
    unwindChildren(args, child, 1);
    args.append(*createLocalAttribute());
    //We would have generated a global hash dedup, so adding hash to the local dedup makes sense.
    args.append(*createAttribute(hashAtom));

    OwnedHqlExpr ret = child->clone(args);
    if (!alreadyHasUsage(ret))
        incUsage(distn);
    return ret.getClear();
}


IHqlExpression * CTreeOptimizer::optimizeProjectInlineTable(IHqlExpression * transformed, bool childrenAreShared)
{
    IHqlExpression * child = transformed->queryChild(0);
    IHqlExpression * values = child->queryChild(0);
    //MORE If trivial projection then might be worth merging with multiple items, but unlikely to occur in practice
    if (!isPureInlineDataset(child) || transformed->hasProperty(prefetchAtom))
        return NULL;

    bool onlyFoldConstant = false;
    if (values->numChildren() != 1)
    {
        if (options & HOOfoldconstantdatasets)
        {
            if (!isConstantDataset(child))
                return NULL;
            onlyFoldConstant = true;
        }
        else
            return NULL;
    }

    if (childrenAreShared)
    {
        if (!isConstantDataset(child))
            return NULL;
    }


    IHqlExpression * transformedCountProject = transformed->queryProperty(_countProject_Atom);
    IHqlExpression * seq = querySelSeq(transformed);
    node_operator projectOp = transformed->getOperator();
    OwnedHqlExpr oldSelector = (projectOp == no_hqlproject) ? createSelector(no_left, child, seq) : LINK(child->queryNormalizedSelector());
    IHqlExpression * curTransform = queryNewColumnProvider(transformed);
    if (!isKnownTransform(curTransform))
        return NULL;

    ExpandSelectorMonitor monitor(*this);
    HqlExprArray newValues;
    ForEachChild(i, values)
    {
        TableProjectMapper mapper;
        mapper.setMapping(values->queryChild(i), NULL);

        OwnedHqlExpr next = expandFields(&mapper, curTransform, oldSelector, NULL, &monitor);
        //Expand counter inline!
        if (transformedCountProject)
        {
            OwnedHqlExpr counter = createConstant(createIntValue(i+1, 8, false));
            next.setown(replaceExpression(next, transformedCountProject->queryChild(0), counter));
        }

        if (!next || monitor.isComplex())
            return NULL;

        if (onlyFoldConstant && !isConstantTransform(next))
            return NULL;
        newValues.append(*ensureTransformType(next, no_transform));
    }

    DBGLOG("Optimizer: Merge %s and %s", queryNode0Text(transformed), queryNode1Text(child));
    HqlExprArray args;
    args.append(*createValue(no_transformlist, makeNullType(), newValues));
    if (projectOp == no_newusertable)
        args.append(*LINK(transformed->queryChild(1)));
    else
        args.append(*LINK(transformed->queryRecord()));
    unwindChildren(args, child, 2);
    noteUnused(child);
    OwnedHqlExpr ret = child->clone(args);
    return transformed->cloneAllAnnotations(ret);
}

void CTreeOptimizer::analyseExpr(IHqlExpression * expr)
{
    if (incUsage(expr))
        return;

    switch (expr->getOperator())
    {
    case no_filepos:
    case no_file_logicalname:
    case no_sizeof:
    case no_offsetof:
        return;
    case no_table:
        //only look at the filename - not the parent files.
        analyseExpr(expr->queryChild(0));
        return;
    }

    PARENT::analyseExpr(expr);
}


bool CTreeOptimizer::noteUnused(IHqlExpression * expr)
{
//  return false;
    return decUsage(expr);
}


bool CTreeOptimizer::decUsage(IHqlExpression * expr)
{
    OptTransformInfo * extra = queryBodyExtra(expr);
#ifdef TRACE_USAGE
    if (expr->isDataset() || expr->isDatarow())
        DBGLOG("%lx dec %d [%s]", (unsigned)expr, extra->useCount, queryNode0Text(expr));
#endif
    if (extra->useCount)
        return extra->useCount-- == 1;
    return false;
}

bool CTreeOptimizer::alreadyHasUsage(IHqlExpression * expr)
{
    OptTransformInfo * extra = queryBodyExtra(expr);
    return (extra->useCount != 0);
}

bool CTreeOptimizer::incUsage(IHqlExpression * expr)
{
    OptTransformInfo * extra = queryBodyExtra(expr);
#ifdef TRACE_USAGE
    if (expr->isDataset() || expr->isDatarow())
        DBGLOG("%lx inc %d [%s]", (unsigned)expr, extra->useCount, queryNode0Text(expr));
#endif
    return (extra->useCount++ != 0);
}

IHqlExpression * CTreeOptimizer::inheritUsage(IHqlExpression * newExpr, IHqlExpression * oldExpr)
{
    OptTransformInfo * newExtra = queryBodyExtra(newExpr);
    OptTransformInfo * oldExtra = queryBodyExtra(oldExpr);
#ifdef TRACE_USAGE
    if (newExpr->isDataset() || newExpr->isDatarow())
        DBGLOG("%lx inherit %d,%d (from %lx) [%s]", (unsigned)newExpr, newExtra->useCount, oldExtra->useCount, (unsigned)oldExpr, queryNode0Text(newExpr));
    //assertex(extra->useCount);
    if ((oldExtra->useCount == 0) && (newExpr->isDataset() || newExpr->isDatarow()))
        DBGLOG("Inherit0: %lx inherit %d,%d (from %lx)", (unsigned)newExpr, newExtra->useCount, oldExtra->useCount, (unsigned)oldExpr);
#endif
    newExtra->useCount += oldExtra->useCount;
    return newExpr;
}



bool CTreeOptimizer::isComplexTransform(IHqlExpression * transform)
{
    ExpandComplexityMonitor monitor(*this);
    monitor.analyseTransform(transform);
    return monitor.isComplex();
}


IHqlExpression * CTreeOptimizer::expandProjectedDataset(IHqlExpression * child, IHqlExpression * transform, IHqlExpression * childSelector, IHqlExpression * expr)
{
    if (hasUnknownTransform(child))
        return NULL;

    OwnedMapper mapper = getMapper(child);
    ExpandSelectorMonitor monitor(*this);
    OwnedHqlExpr expandedTransform = expandFields(mapper, transform, childSelector, NULL, &monitor);
    IHqlExpression * onFail = child->queryProperty(onFailAtom);
    OwnedHqlExpr newOnFail;
    if (onFail)
    {
        IHqlExpression * oldFailTransform = onFail->queryChild(0);
        OwnedMapper onFailMapper = createProjectMapper(oldFailTransform, NULL);
        OwnedHqlExpr onFailTransform = expandFields(onFailMapper, transform, childSelector, NULL, &monitor);
        if (onFailTransform)
            newOnFail.setown(createExprAttribute(onFailAtom, ensureTransformType(onFailTransform, oldFailTransform->getOperator())));
    }
    if (expandedTransform && (!onFail || newOnFail) && !monitor.isComplex())
    {
        unsigned transformIndex = queryTransformIndex(child);
        IHqlExpression * oldTransform = child->queryChild(transformIndex);
        expandedTransform.setown(ensureTransformType(expandedTransform, oldTransform->getOperator()));

        DBGLOG("Optimizer: Merge %s and %s", queryNode0Text(expr), queryNode1Text(child));
        HqlExprArray args;
        unwindChildren(args, child);
        args.replace(*expandedTransform.getClear(), transformIndex);
        if (onFail)
            args.replace(*newOnFail.getClear(), args.find(*onFail));
        noteUnused(child);
        return child->clone(args);
    }
    return NULL;
}


IHqlExpression * CTreeOptimizer::optimizeAggregateCompound(IHqlExpression * transformed)
{
    //Keep in sync with code in CompoundSourceTransformer
    IHqlExpression * child = transformed->queryChild(0);
    if (isLimitedDataset(child, true))
        return NULL;
    IHqlExpression * tableExpr = queryRoot(transformed);
    node_operator modeOp = queryTableMode(tableExpr);
    if (modeOp == no_csv || modeOp == no_xml)
        return NULL;

    if (isLimitedDataset(child) && !isSimpleCountExistsAggregate(transformed, true, false))
        return NULL;

    node_operator newOp = no_none;
    node_operator childOp = child->getOperator();

    if (queryRealChild(transformed, 3))
    {
        //Grouped aggregate
        switch (childOp)
        {
        case no_compound_diskread:
        case no_compound_disknormalize:
            newOp = no_compound_diskgroupaggregate;
            break;
        case no_compound_indexread:
        case no_compound_indexnormalize:
            newOp = no_compound_indexgroupaggregate;
            break;
        case no_compound_childread:
        case no_compound_childnormalize:
            newOp = no_compound_childgroupaggregate;
            break;
        }
    }
    else
    {
        switch (childOp)
        {
        case no_compound_diskread:
        case no_compound_disknormalize:
            newOp = no_compound_diskaggregate;
            break;
        case no_compound_indexread:
        case no_compound_indexnormalize:
            newOp = no_compound_indexaggregate;
            break;
        case no_compound_childread:
        case no_compound_childnormalize:
            newOp = no_compound_childaggregate;
            break;
        case no_compound_inline:
            newOp = no_compound_inline;
            break;
        }
    }
    if (newOp)
        return createDataset(newOp, removeChildNode(transformed));
    return NULL;
}

bool CTreeOptimizer::childrenAreShared(IHqlExpression * expr)
{
    if (expr->isDataset() || expr->isDatarow())
    {
        switch (getChildDatasetType(expr))
        {
        case childdataset_none:
            return false;
        case childdataset_dataset: 
        case childdataset_datasetleft: 
        case childdataset_left:
        case childdataset_same_left_right:
        case childdataset_top_left_right:
        case childdataset_dataset_noscope:
            {
                IHqlExpression * ds = expr->queryChild(0);
                //Don't restrict the items that can be combined with no_null.
                return isShared(ds);
            }
        case childdataset_leftright:
            return isShared(expr->queryChild(0)) || isShared(expr->queryChild(1));
        case childdataset_evaluate:
        case childdataset_if:
        case childdataset_case:
        case childdataset_map:
        case childdataset_nway_left_right:
            return true;    // stop any folding of these...
        case childdataset_addfiles:
        case childdataset_merge:
            {
                ForEachChild(i, expr)
                {
                    IHqlExpression * cur  = expr->queryChild(i);
                    if (!cur->isAttribute() && isShared(cur))
                        return true;
                }
                return false;
            }
        default:
            UNIMPLEMENTED;
        }
    }
    switch (expr->getOperator())
    {
    case no_select:
        if (!expr->hasProperty(newAtom))
            return false;
        return isShared(expr->queryChild(0));
    case NO_AGGREGATE:
        return isShared(expr->queryChild(0));
    }
    return false;
}

bool CTreeOptimizer::isWorthMovingProjectOverLimit(IHqlExpression * project)
{
    if (noHoistAttr && project->queryProperty(_noHoist_Atom) == noHoistAttr)
        return false;

    IHqlExpression * expr = project->queryChild(0);
    loop
    {
        switch (expr->getOperator())
        {
        case no_limit:
        case no_keyedlimit:
        case no_choosen:
            expr = expr->queryChild(0);
            break;
        case no_compound_diskread:
        case no_compound_disknormalize:
        case no_compound_indexread:
        case no_compound_indexnormalize:
        case no_compound_childread:
        case no_compound_childnormalize:
        case no_compound_selectnew:
        case no_compound_inline:
            //if (options & HOOcompoundproject)
            return true;
        case no_join:
            if (isKeyedJoin(expr))
                return false;
        case no_selfjoin:
        case no_fetch:
        case no_normalize:
        case no_newparse:
        case no_newxmlparse:
            return true;
        case no_null:
            return true;
        case no_newusertable:
            if (isAggregateDataset(expr))
                return false;
            //fallthrough.
        case no_hqlproject:
            if (!isPureActivity(expr) || expr->hasProperty(_countProject_Atom) || expr->hasProperty(prefetchAtom))
                return false;
            return true;
        default:
            return false;
        }
        if (isShared(expr))
            return false;
    }
}

IHqlExpression * CTreeOptimizer::moveProjectionOverSimple(IHqlExpression * transformed, bool noMoveIfFail, bool errorIfFail)
{
    IHqlExpression * child = transformed->queryChild(0);
    IHqlExpression * grandchild = child->queryChild(0);
    IHqlExpression * newProject = replaceChild(transformed, grandchild);
    HqlExprArray args;
    args.append(*newProject);

    OwnedMapper mapper = getMapper(transformed);
    ForEachChild(idx, child)
    {
        if (idx != 0)
        {
            bool ok = false;
            IHqlExpression * cur = child->queryChild(idx);
            IHqlExpression * collapsed = mapper->collapseFields(cur, grandchild, newProject, &ok);
            if (!ok)
            {
                ::Release(collapsed);
                if (errorIfFail)
                {
                    StringBuffer cause;
                    if (cur->getOperator() == no_sortlist)
                    {
                        ForEachChild(i, cur)
                        {
                            IHqlExpression * elem = cur->queryChild(i);
                            OwnedHqlExpr collapsed = mapper->collapseFields(elem, grandchild, newProject, &ok);
                            if (!ok)
                            {
                                cause.append(" expression: ");
                                getExprECL(elem, cause);
                                break;
                            }
                        }
                    }

                    throwError1(HQLERR_BadProjectOfStepping, cause.str());
                }

                if (noMoveIfFail)
                    return LINK(transformed);

                //NB: Always succeed for distributed/sorted/grouped, because it is needed for the disk read/index read processing.
                if (cur->getOperator() == no_sortlist)
                    collapsed = createValue(no_sortlist, makeSortListType(NULL), createAttribute(unknownAtom));
                else
                    collapsed = createAttribute(unknownAtom);
            }
            args.append(*collapsed);
        }
    }
    
    DBGLOG("Optimizer: Swap %s and %s", queryNode0Text(transformed), queryNode1Text(child));
    OwnedHqlExpr swapped = child->clone(args);
    if (!alreadyHasUsage(swapped))
        incUsage(newProject);
    noteUnused(child);
    return swapped.getClear();
}

IHqlExpression * CTreeOptimizer::moveProjectionOverLimit(IHqlExpression * transformed)
{
    IHqlExpression * child = transformed->queryChild(0);
    IHqlExpression * grandchild = child->queryChild(0);
    IHqlExpression * newProject = replaceChild(transformed, grandchild);

    HqlExprArray args;
    args.append(*newProject);

    ExpandSelectorMonitor monitor(*this);
    ForEachChildFrom(idx, child, 1)
    {
        IHqlExpression * cur = child->queryChild(idx);
        if (cur->isAttribute() && cur->queryName() == onFailAtom)
        {
            IHqlExpression * oldFailTransform = cur->queryChild(0);
            if (!isKnownTransform(oldFailTransform))
                return LINK(transformed);

            OwnedMapper onFailMapper = createProjectMapper(oldFailTransform, NULL);

            IHqlExpression * projectionTransformer = queryNewColumnProvider(transformed);
            OwnedHqlExpr parentSelector = getParentDatasetSelector(transformed);

            OwnedHqlExpr onFailTransform = expandFields(onFailMapper, projectionTransformer, parentSelector, NULL, &monitor);
            args.append(*createExprAttribute(onFailAtom, ensureTransformType(onFailTransform, oldFailTransform->getOperator())));
        }
        else
            args.append(*LINK(cur));
    }

    if (monitor.isComplex())
        return LINK(transformed);

    DBGLOG("Optimizer: Swap %s and %s", queryNode0Text(transformed), queryNode1Text(child));
    OwnedHqlExpr swapped = child->clone(args);
    if (!alreadyHasUsage(swapped))
        incUsage(newProject);
    noteUnused(child);
    return swapped.getClear();
}

IHqlExpression * CTreeOptimizer::insertChild(IHqlExpression * expr, IHqlExpression * newChild)
{
    return insertChildDataset(expr, newChild, 0);
}

IHqlExpression * CTreeOptimizer::replaceChild(IHqlExpression * expr, IHqlExpression * newChild)
{
    return replaceChildDataset(expr, newChild, 0);
}

void CTreeOptimizer::unwindReplaceChild(HqlExprArray & args, IHqlExpression * expr, IHqlExpression * newChild)
{
    HqlMapTransformer mapper;
    mapper.setMapping(expr->queryChild(0), newChild);
    mapper.setSelectorMapping(expr->queryChild(0), newChild);
    ForEachChild(idx, expr)
        args.append(*mapper.transformRoot(expr->queryChild(idx)));
}

ANewTransformInfo * CTreeOptimizer::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(OptTransformInfo, expr);
}

IHqlExpression * CTreeOptimizer::expandFields(TableProjectMapper * mapper, IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IExpandCallback * _expandCallback)
{
    OwnedHqlExpr expandedFilter = mapper->expandFields(expr, oldDataset, newDataset, _expandCallback);
    if (options & HOOfold)
        expandedFilter.setown(foldHqlExpression(expandedFilter));
    return expandedFilter.getClear();
}

IHqlExpression * CTreeOptimizer::inheritSkips(IHqlExpression * newTransform, IHqlExpression * oldTransform, IHqlExpression * oldSelector, IHqlExpression * newSelector)
{
    HqlExprArray args;
    ForEachChild(i, oldTransform)
    {
        IHqlExpression * cur = oldTransform->queryChild(i);
        if (cur->getOperator() == no_skip)
            args.append(*replaceSelector(cur, oldSelector, newSelector));
    }
    if (args.ordinality() == 0)
        return LINK(newTransform);
    unwindChildren(args, newTransform);
    return newTransform->clone(args);
}


IHqlExpression * CTreeOptimizer::createTransformed(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_field:
    case no_record:
        return LINK(expr);
    }

    //Do this first, so that any references to a child dataset that changes are correctly updated, before proceeding any further.
    OwnedHqlExpr dft = defaultCreateTransformed(expr);
#ifndef USE_MERGING_TRANSFORM
    updateOrphanedSelectors(dft, expr);
#endif

    OwnedHqlExpr ret = doCreateTransformed(dft, expr);
    if (ret->queryBody() == expr->queryBody())
        return ret.getClear();

    inheritUsage(ret, expr);
    if (ret == dft)
        return ret.getClear();

    return transform(ret);
}


IHqlExpression * CTreeOptimizer::getOptimizedFilter(IHqlExpression * transformed, bool alwaysTrue)
{
    if (alwaysTrue)
        return removeParentNode(transformed);
    else
    {
        noteUnused(transformed->queryChild(0));
        //MORE: Really wants to walk down the entire chain until we hit something that is shared.
        IHqlExpression * ret = createNullDataset(transformed);
        DBGLOG("Optimizer: Replace %s with %s", queryNode0Text(transformed), queryNode1Text(ret));
        return ret;
    }
}

IHqlExpression * CTreeOptimizer::getOptimizedFilter(IHqlExpression * transformed, HqlExprArray const & filters)
{
    return getOptimizedFilter(transformed, filters.ordinality() == 0);
}


void CTreeOptimizer::recursiveDecUsage(IHqlExpression * expr)
{
    if (decUsage(expr))
        recursiveDecChildUsage(expr);
}

void CTreeOptimizer::recursiveDecChildUsage(IHqlExpression * expr)
{
    switch (getChildDatasetType(expr))
    {
    case childdataset_none:
        break;
    case childdataset_dataset: 
    case childdataset_datasetleft:
    case childdataset_left:
    case childdataset_same_left_right:
    case childdataset_top_left_right:
    case childdataset_dataset_noscope:
        recursiveDecUsage(expr->queryChild(0));
        break;
    case childdataset_leftright:
        recursiveDecUsage(expr->queryChild(0));
        recursiveDecUsage(expr->queryChild(0));
        break;
    case childdataset_if:
        recursiveDecUsage(expr->queryChild(1));
        if (expr->queryChild(2))
            recursiveDecUsage(expr->queryChild(2));
        break;
    case childdataset_evaluate:
    case childdataset_case:
    case childdataset_map:
    case childdataset_nway_left_right:
        break;  // who knows?
    case childdataset_addfiles:
    case childdataset_merge:
        {
            ForEachChild(i, expr)
                recursiveDecUsage(expr->queryChild(i));
            break;
        }
    default:
        UNIMPLEMENTED;
    }
}


IHqlExpression * CTreeOptimizer::replaceWithNull(IHqlExpression * transformed)
{
    IHqlExpression * ret = createNullExpr(transformed);
    DBGLOG("Optimizer: Replace %s with %s", queryNode0Text(transformed), queryNode1Text(ret));

    recursiveDecChildUsage(transformed);
    return ret;
}


IHqlExpression * CTreeOptimizer::replaceWithNullRow(IHqlExpression * expr)
{
    IHqlExpression * ret = createRow(no_null, LINK(expr->queryRecord()));
    DBGLOG("Optimizer: Replace %s with %s", queryNode0Text(expr), queryNode1Text(ret));
    recursiveDecChildUsage(expr);
    return ret;

}
IHqlExpression * CTreeOptimizer::replaceWithNullRowDs(IHqlExpression * expr)
{
    assertex(!isGrouped(expr));
    IHqlExpression * ret = createDatasetFromRow(createRow(no_null, LINK(expr->queryRecord())));
    DBGLOG("Optimizer: Replace %s with %s", queryNode0Text(expr), queryNode1Text(ret));
    recursiveDecChildUsage(expr);
    return ret;

}


IHqlExpression * CTreeOptimizer::transformExpanded(IHqlExpression * expr)
{
    return transform(expr);
}

IHqlExpression * CTreeOptimizer::queryMoveKeyedExpr(IHqlExpression * transformed)
{
    //Need to swap with these, regardless of whether the input is shared, because the keyed limit only makes sense
    //inside a compound source
    IHqlExpression * child = transformed->queryChild(0);
    node_operator childOp = child->getOperator();
    switch(childOp)
    {
    case no_compound_indexread:
    case no_compound_diskread:
    case no_assertsorted:
    case no_assertdistributed:
    case no_section:            // no so sure...
    case no_sectioninput:
    case no_executewhen:
        return swapNodeWithChild(transformed);
    case no_compound:
        return swapNodeWithChild(transformed, 1);
    case no_if:
        return swapIntoIf(transformed, true);
    case no_nonempty:
    case no_addfiles:
        return swapIntoAddFiles(transformed, true);
    //Force the child to be keyed if it is surrounded by something that needs to be keyed, to ensure both migrate up the tree
    case no_hqlproject:
    case no_newusertable:
    case no_aggregate:
    case no_newaggregate:
    case no_choosen:
    case no_limit:
    case no_keyedlimit:
    case no_sorted:
    case no_stepped:
    case no_distributed:
    case no_preservemeta:
    case no_grouped:
    case no_nofold:
    case no_nohoist:
    case no_filter:
        {
            OwnedHqlExpr newChild = queryMoveKeyedExpr(child);
            if (newChild)
            {
                OwnedHqlExpr moved = replaceChildDataset(transformed, newChild, 0);
                decUsage(child);
                if (!alreadyHasUsage(moved))
                    incUsage(newChild);
                return moved.getClear();
            }
        }
    }
    return NULL;
}

IHqlExpression * CTreeOptimizer::doCreateTransformed(IHqlExpression * transformed, IHqlExpression * _expr)
{
    OwnedHqlExpr folded = foldNullDataset(transformed);
    if (folded && folded != transformed)
        return folded.getClear();

    node_operator op = transformed->getOperator();
    IHqlExpression * child = transformed->queryChild(0);

    //Any optimizations that remove the current node, or modify the current node don't need to check if the children are shared
    //Removing child nodes could be included, but it may create more spillers/spliters - which may be significant in thor.
    switch (op)
    {
    case no_if:
        {
            OwnedHqlExpr ret = optimizeIf(transformed);
            if (ret)
                return ret.getClear();

            //Processed hereThis won't split shared nodes, but one of the children may be shared - so proce
            if (transformed->isDataset())
                return optimizeDatasetIf(transformed);
            break;
        }
    case no_keyedlimit:
        {
            IHqlExpression * ret = queryMoveKeyedExpr(transformed);
            if (ret)
                return ret;
            break;
        }
    case no_filter:
        if (filterIsKeyed(transformed))
        {
            IHqlExpression * ret = queryMoveKeyedExpr(transformed);
            if (ret)
                return ret;
        }
        break;
    case no_hqlproject:
    case no_newusertable:
        if (transformed->hasProperty(keyedAtom))
        {
            IHqlExpression * ret = queryMoveKeyedExpr(transformed);
            if (ret)
                return ret;
        }
        break;
    case no_join:
        {
#ifdef MIGRATE_JOIN_CONDITIONS
            OwnedHqlExpr ret = optimizeJoinCondition(transformed);
            if (ret)
                return ret.getClear();
#endif
            IHqlExpression * ret2 = optimizeInlineJoin(transformed);
            if (ret2)
                return ret2;

            //MORE:
            //If left outer join, and transform doesn't reference RIGHT, and only one rhs record  could match each lhs record (e.g., it was rolled
            //up, or a non-many lookup join, then the join could be converted into a project
            //Can occur once fields get implicitly removed from transforms etc. - e.g., bc10.xhql, although that code has since been fixed.
            break;
        }
    case no_dedup:
        {
            node_operator childOp = child->getOperator();

            switch(childOp)
            {
            case no_dedup:
                {
                    DedupInfoExtractor dedup1(transformed);     // slightly costly to create
                    DedupInfoExtractor dedup2(child);
                    switch (dedup1.compareWith(dedup2))
                    {
                        //In roxie this would probably be better, in thor it may create extra spills
                    //case DedupInfoExtractor::DedupDoesAll:
                    //  return removeChildNode(transformed);
                    case DedupInfoExtractor::DedupDoesNothing:
                        return removeParentNode(transformed);
                    }
                    break;
                }
            }
            break;
        }
    case no_aggregate:
    case no_newaggregate:
        {
            node_operator childOp = child->getOperator();
            if (transformed->hasProperty(keyedAtom))
            {
                IHqlExpression * moved = NULL;
                switch(childOp)
                {
                case no_compound_diskread:
                case no_compound_disknormalize:
                case no_compound_indexread:
                case no_compound_indexnormalize:
                case no_compound_childread:
                case no_compound_childnormalize:
                    if (!isGrouped(queryRoot(child)) && (options & HOOhascompoundaggregate))
                        moved = optimizeAggregateCompound(transformed);
                    break;
                default:
                    moved = queryMoveKeyedExpr(transformed);
                    break;
                }

                if (moved)
                    return moved;
            }
            IHqlExpression * folded = NULL;
            switch(childOp)
            {
            case no_thisnode:
                return swapNodeWithChild(transformed);
            case no_inlinetable:
                if ((options & HOOfoldconstantdatasets) && isPureInlineDataset(child))
                    folded = queryOptimizeAggregateInline(transformed, child->queryChild(0)->numChildren());
                break;
            default:
                if ((options & HOOfoldconstantdatasets) && hasSingleRow(child))
                    folded = queryOptimizeAggregateInline(transformed, 1);
                break;
            }
            if (folded)
            {
                recursiveDecUsage(child);
                return folded;
            }

            //MORE: The OHOinsidecompound isn't really good enough - because might remove projects from
            //nested child aggregates which could benifit from them.  Probably not as long as all compound 
            //activities support aggregation.  In fact test should be removable everywhere once all 
            //engines support the new activities.
            if (isGrouped(transformed->queryChild(0)) || (queryRealChild(transformed, 3) && !(options & HOOinsidecompound)))
                break;
            OwnedHqlExpr ret = optimizeAggregateDataset(transformed);
            if (ret != transformed)
                return ret.getClear();
            break;
        }
    case NO_AGGREGATE:
        return optimizeAggregateDataset(transformed);
    case no_selectnth:
        {
            node_operator childOp = child->getOperator();
            switch(childOp)
            {
            case no_inlinetable:
                {
                    __int64 index = getIntValue(transformed->queryChild(1), -1);
                    if (index == -1)
                        break;

                    IHqlExpression * values = child->queryChild(0);
                    if (!values->isPure())
                        break;

                    if (index < 1 || index > values->numChildren())
                        return replaceWithNull(transformed);
        
                    //MORE If trivial projection then might be worth merging with multiple items, but unlikely to occur in practice
                    OwnedHqlExpr ret = createRow(no_createrow, LINK(values->queryChild((unsigned)index-1)));
                    noteUnused(child);
                    DBGLOG("Optimizer: Replace %s with %s", queryNode0Text(transformed), queryNode1Text(ret));
                    return ret.getClear();
                }
            case no_datasetfromrow:
                {
                    __int64 index = getIntValue(transformed->queryChild(1), -1);
                    if (index == -1)
                        break;

                    if (index != 1)
                        return replaceWithNull(transformed);
        
                    IHqlExpression * ret = child->queryChild(0);
                    noteUnused(child);
                    decUsage(ret);  // will inherit later
                    DBGLOG("Optimizer: Replace %s with %s", queryNode0Text(transformed), queryNode1Text(ret));
                    return LINK(ret);
                }
#if 0
            //This works (with either condition used), but I don't tink it is worth the cycles..
            case no_choosen:
                {
                    __int64 index = getIntValue(transformed->queryChild(1), -1);
                    __int64 choosenMax = getIntValue(child->queryChild(1), -1);
                    //choosen(x,<n>)[m] == x[m]  iff n >= m
//                  if ((index == 1) && (choosenMax == 1) && !queryRealChild(child, 2))
                    if ((index > 0) && (choosenMax >= index) && !queryRealChild(child, 2) && !isGrouped(child->queryChild(0)))
                        return removeChildNode(transformed);
                }
                break;
#endif
            }
            break;
        }
    case no_select:
        {
            if (transformed->hasProperty(newAtom))
            {
                node_operator childOp = child->getOperator();
                switch (childOp)
                {
                case no_createrow:
                    {
                        OwnedHqlExpr match = getExtractSelect(child->queryChild(0), transformed->queryChild(1));
                        if (match)
                        {
                            IHqlExpression * cur = match;
                            while (isCast(cur))
                                cur = cur->queryChild(0);
                            switch (cur->getOperator())
                            {
                            case no_constant:
                            case no_select:
                            case no_null:
                            case no_getresult:
                                DBGLOG("Optimizer: Extract value %s from %s", queryNode0Text(match), queryNode1Text(transformed));
                                noteUnused(child);
                                return match.getClear();
                            }
                        }
                    }
                    break;
                case no_datasetfromrow:
                    {
                        HqlExprArray args;
                        args.append(*LINK(child->queryChild(0)));
                        unwindChildren(args, transformed, 1);
                        noteUnused(child);
                        return transformed->clone(args);
                    }
                    break;
                case no_inlinetable:
                    {
                        IHqlExpression * values = child->queryChild(0);
                        if (values->numChildren() == 1) 
                        {
                            IHqlExpression * transform = values->queryChild(0);
                            OwnedHqlExpr match = getExtractSelect(transform, transformed->queryChild(1));
                            if (match)
                            {
                                IHqlExpression * cur = match;
                                while (isCast(cur))
                                    cur = cur->queryChild(0);
                                switch (cur->getOperator())
                                {
                                case no_constant:
                                case no_select:
                                case no_null:
                                case no_getresult:
                                case no_inlinetable:
                                case no_left:
                                case no_right:
                                    {
                                        DBGLOG("Optimizer: Extract value %s from %s", queryNode0Text(match), queryNode1Text(transformed));
                                        noteUnused(child);
                                        return match.getClear();
                                    }
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }
        break;
    case no_extractresult:
        {
            //Very similar to the transform above, but needs to be done separately because of the new representation of no_extractresult.
            //extract(inline-table(single-row), somefield) -> single-row.somefield if simple valued.
            node_operator childOp = child->getOperator();
            switch (childOp)
            {
            case no_inlinetable:
                {
                    IHqlExpression * extracted = transformed->queryChild(1);
                    if ((extracted->getOperator() == no_select) && (extracted->queryChild(0) == child->queryNormalizedSelector()))
                    {
                        IHqlExpression * values = child->queryChild(0);
                        if (values->numChildren() == 1) 
                        {
                            IHqlExpression * transform = values->queryChild(0);
                            OwnedHqlExpr match = getExtractSelect(transform, extracted->queryChild(1));
                            if (match)
                            {
                                IHqlExpression * cur = match;
                                while (isCast(cur))
                                    cur = cur->queryChild(0);
                                switch (cur->getOperator())
                                {
                                case no_constant:
                                case no_select:
                                case no_null:
                                case no_getresult:
                                    {
                                        DBGLOG("Optimizer: Extract value %s from %s", queryNode0Text(match), queryNode1Text(transformed));
                                        noteUnused(child);

                                        HqlExprArray args;
                                        args.append(*match.getClear());
                                        unwindChildren(args, transformed, 2);
                                        return createValue(no_setresult, makeVoidType(), args);
                                    }
                                }
                            }
                        }
                    }
                }
                break;
            }
        }
        break;
    case no_keyeddistribute:
    case no_distribute:
        {
            //If distribution matches existing and grouped then don't distribute, but still remove grouping.
            IHqlExpression * distn = queryDistribution(transformed);
            if (distn == queryDistribution(child))
            {
                assertex(isGrouped(child)); // not grouped handled already.
                OwnedHqlExpr ret = createDataset(no_group, LINK(child));
                DBGLOG("Optimizer: replace %s with %s", queryNode0Text(transformed), queryNode1Text(ret));
                return transformed->cloneAllAnnotations(ret);
            }
            break;
        }
    case no_choosen:
        {
            IValue * num = transformed->queryChild(1)->queryValue();
            if (num && (num->getIntValue() >= 1) && !queryRealChild(transformed, 2))
            {
                if (hasNoMoreRowsThan(child, 1))
                    return removeParentNode(transformed);
            }
            break;
        }
    case no_preservemeta:
        {
            node_operator childOp = child->getOperator();
            switch(childOp)
            {
            case no_hqlproject:
            case no_newusertable:
                {
                    IHqlExpression * ret = hoistMetaOverProject(transformed);
                    if (ret)
                        return ret;
                    break;
                }
                //more; iterate, join? others?
            case no_compound_diskread:
            case no_compound_disknormalize:
            case no_compound_indexread:
            case no_compound_indexnormalize:
            case no_compound_childread:
            case no_compound_childnormalize:
            case no_compound_selectnew:
            case no_compound_inline:
                return swapNodeWithChild(transformed);
            }
            break;
        }
    }

    bool shared = childrenAreShared(transformed);
    if (shared)
    {
        bool okToContinue = false;
        switch (op)
        {
        case no_filter:
            {
                node_operator childOp = child->getOperator();
                switch(childOp)
                {
                case no_hqlproject:
                case no_newusertable:
                    {
                        IHqlExpression * ret = hoistFilterOverProject(transformed, true);
                        if (ret)
                            return ret;
                        break;
                    }
                case no_inlinetable:
                    //shared is checked within the code below....
                    okToContinue = true;
                    break;
                }
            }
        case no_hqlproject:
            {
                node_operator childOp = child->getOperator();
                switch(childOp)
                {
                case no_inlinetable:
                    okToContinue = true;
                    break;
                }
                break;
            }
        case no_addfiles:
            //It is generally worth always combining inlinetable + inlinetable because it opens the scope
            //for more optimizations (e.g., filters on inlinetables) and the counts also become a known constant.
            okToContinue = true;
            break;
        }

        if (!okToContinue)
            return LINK(transformed);
    }

    switch (op)
    {
    case no_choosen:
        {
            //worth moving a choosen over an activity that doesn't read a record at a time.
            //also worth moving if it brings two projects closer togther, if
            //that doesn't mess up a projected disk read.
            IHqlExpression * const1 = transformed->queryChild(1);
            IValue * val1 = const1->queryValue();
            if (val1)
            {
                __int64 limit = val1->getIntValue();
                if ((limit == CHOOSEN_ALL_LIMIT) && !transformed->queryChild(2))
                    return removeParentNode(transformed);
                //if (limit == 0)
                //.,..
            }

            node_operator childOp = child->getOperator();
            switch(childOp)
            {
            case no_choosen:
                {
                    if (transformed->queryChild(2) || child->queryChild(2))
                    {
                        //choosen(choosen(x, a, b), c, d))
                        //could generate choosen(x, (b+d-1), min(c, a)) but I doubt it is worth it....
                        break;
                    }
                    IHqlExpression * const2 = child->queryChild(1);
                    IValue * val2 = const2->queryValue();
                    if (val1 && val2)
                    {
                        __int64 ival1 = val1->getIntValue();
                        __int64 ival2 = val2->getIntValue();
                        IHqlExpression * newLimit;
                        if (ival1 < ival2)
                            newLimit = const1;
                        else
                            newLimit = const2;

                        DBGLOG("Optimizer: Merge %s and %s", queryNode0Text(transformed), queryNode1Text(child));
                        return createDataset(no_choosen, LINK(child->queryChild(0)), LINK(newLimit));
                        //don't bother to transform
                    }
                    break;
                }

            //This can be done, but I think it makes matters worse.  The choosen() will short circuit the reading anyway,
            //so no advantage of swapping with the project, and makes things worse, since stops projects commoning up.
            case no_hqlproject:
            case no_newusertable:
            case no_transformascii:
            case no_transformebcdic:
                {
                    if (isPureActivity(child) && !isAggregateDataset(child))
                    {
                        //Don't move a choosen with a start value over a count project - we could if we also adjust the counter
                        if (!child->queryProperty(_countProject_Atom) || !queryRealChild(transformed, 2))
                            return forceSwapNodeWithChild(transformed);
                    }
                    break;
                }
            case no_fetch:              //NB: Not filtered fetch
                {
                    if (isPureActivity(child))
                        return swapNodeWithChild(transformed, 1);
                    break;
                }
            case no_if:
                return swapIntoIf(transformed);
            case no_nonempty:
                return swapIntoAddFiles(transformed);
            case no_sort:
                {
                    unsigned __int64 topNLimit = 1000;
                    OwnedHqlExpr topn = queryConvertChoosenNSort(transformed, topNLimit);
                    if (topn)
                    {
                        noteUnused(child);
                        return topn.getClear();
                    }
                    break;
                }
            }
            break;
        }
    case no_limit:
        {
            node_operator childOp = child->getOperator();
            switch(childOp)
            {
            case no_hqlproject:
            case no_newusertable:
                {
                    if (isPureActivity(child) && !isAggregateDataset(child) && !transformed->hasProperty(onFailAtom))
                        return forceSwapNodeWithChild(transformed);
                    break;
                }
            case no_fetch:
                {
                    if (isPureActivity(child))
                        return swapNodeWithChild(transformed, 1);
                    break;
                }
            case no_if:
                return swapIntoIf(transformed);
            case no_nonempty:
                return swapIntoAddFiles(transformed);
            case no_limit:
                {
                    //Could be cleverer... but this is safer
                    if (transformed->queryProperty(skipAtom) != child->queryProperty(skipAtom))
                        break;
                    if (transformed->queryProperty(onFailAtom) != child->queryProperty(onFailAtom))
                        break;
                    OwnedHqlExpr parentLimit = foldHqlExpression(transformed->queryChild(1));
                    OwnedHqlExpr childLimit = foldHqlExpression(child->queryChild(1));
                    if (parentLimit == childLimit)
                        return removeParentNode(transformed);

                    IValue * parentLimitValue = parentLimit->queryValue();
                    IValue * childLimitValue = childLimit->queryValue();
                    if (parentLimitValue && childLimitValue)
                    {
                        if (parentLimitValue->getIntValue() <= childLimitValue->getIntValue())
                            return removeParentNode(transformed);
                    }
                    break;
                }
            case no_compound_indexread:
            case no_compound_diskread:
                if (!isLimitedDataset(child))
                {
                    if (transformed->hasProperty(skipAtom) || transformed->hasProperty(onFailAtom))
                    {
                        //only merge if roxie
                    }
                    else
                    {
                        if ((options & HOOnoclonelimit) || ((options & HOOnocloneindexlimit) && (childOp == no_compound_indexread)))
                            return swapNodeWithChild(transformed);

                        OwnedHqlExpr childLimit = ::replaceChild(transformed, 0, child->queryChild(0));
                        OwnedHqlExpr localLimit = appendLocalAttribute(childLimit);
                        OwnedHqlExpr newCompound = ::replaceChild(child, 0, localLimit);
                        incUsage(localLimit);
                        incUsage(newCompound);
                        decUsage(child);
                        return ::replaceChild(transformed, 0, newCompound);
                    }
                }
                break;
            case no_choosen:
                {
                    OwnedHqlExpr parentLimit = foldHqlExpression(transformed->queryChild(1));
                    OwnedHqlExpr childLimit = foldHqlExpression(child->queryChild(1));
                    if (getIntValue(parentLimit, 0) > getIntValue(childLimit, I64C(0x7fffffffffffffff)))
                        return removeParentNode(transformed);
                    break;
                }
            case no_topn:
                {
                    OwnedHqlExpr parentLimit = foldHqlExpression(transformed->queryChild(1));
                    OwnedHqlExpr childLimit = foldHqlExpression(child->queryChild(2));
                    if (getIntValue(parentLimit, 0) > getIntValue(childLimit, I64C(0x7fffffffffffffff)))
                        return removeParentNode(transformed);
                    break;
                }
            }
            break;
        }
    case no_dedup:
        {
            node_operator childOp = child->getOperator();

            switch(childOp)
            {
            case no_dedup:
                {
                    DedupInfoExtractor dedup1(transformed);     // slightly costly to create
                    DedupInfoExtractor dedup2(child);
                    switch (dedup1.compareWith(dedup2))
                    {
                    case DedupInfoExtractor::DedupDoesAll:
                        return removeChildNode(transformed);
                    }
                    break;
                }
            }
            break;
        }
    case no_filter:
        {
            node_operator childOp = child->getOperator();
            IHqlExpression * newGrandchild = child->queryChild(0);
            switch(childOp)
            {
            case no_filter:
                {
                    DBGLOG("Optimizer: Merge %s and %s", queryNode0Text(transformed), queryNode1Text(child));
                    HqlExprArray args;
                    unwindChildren(args, child);
                    unwindChildren(args, transformed, 1);
                    OwnedHqlExpr combined = child->clone(args);
                    return transformed->cloneAllAnnotations(combined);
                }
            case no_hqlproject:
            case no_newusertable:
                {
                    IHqlExpression * ret = hoistFilterOverProject(transformed, false);
                    if (ret)
                        return ret;
                    break;
                }
                //more; iterate, join? others?
            case no_compound_diskread:
            case no_compound_disknormalize:
            case no_compound_indexread:
            case no_compound_indexnormalize:
            case no_compound_childread:
            case no_compound_childnormalize:
            case no_compound_selectnew:
            case no_compound_inline:
                if (!isLimitedDataset(child))// && child->isPure())
                    return swapNodeWithChild(transformed);
                break;
            case no_sorted:
            case no_stepped:
            case no_distributed:
            case no_distribute:
            case no_group:
            case no_grouped:
            case no_keyeddistribute:
            case no_sort:
            case no_preload:
            case no_assertsorted:
            case no_assertgrouped:
            case no_assertdistributed:
                return swapNodeWithChild(transformed);
            case no_keyedlimit:
                {
                    //It is ugly this is forced.... but ensures filters get combined
                    OwnedHqlExpr ret = swapNodeWithChild(transformed);

                    //Need to add the filter as a skip on the onFail() transform
                    IHqlExpression * onFail = ret->queryProperty(onFailAtom);
                    if (!onFail)
                        return ret.getClear();

                    IHqlExpression * limitTransform = onFail->queryChild(0);
                    if (!isKnownTransform(limitTransform))
                        return ret.getClear();

                    NewProjectMapper2 mapper;
                    mapper.setMapping(limitTransform);
                    HqlExprArray filterArgs;
                    unwindChildren(filterArgs, transformed, 1);
                    OwnedITypeInfo boolType = makeBoolType();
                    OwnedHqlExpr cond = createBalanced(no_and, boolType, filterArgs);
                    OwnedHqlExpr skipFilter = mapper.expandFields(cond, child, NULL, NULL, NULL);
                    OwnedHqlExpr skip = createValue(no_skip, makeVoidType(), getInverse(skipFilter));
                    OwnedHqlExpr newTransform = appendOwnedOperand(limitTransform, skip.getClear());
                    OwnedHqlExpr newOnFail = createExprAttribute(onFailAtom, newTransform.getClear());
                    return replaceOwnedProperty(ret, newOnFail.getClear());
                }
            case no_if:
                return swapIntoIf(transformed);
            case no_nonempty:
                return swapIntoAddFiles(transformed);
            case no_fetch:
                if (isPureActivity(child) && !hasUnknownTransform(child))
                {
                    IHqlExpression * ret = getHoistedFilter(transformed, false, false, true, true, NotFound);
                    if (ret)
                        return ret;
                }
                break;
            case no_iterate:
                //Should be possible to move a filter over a iterate, but only really same if the filter fields match the grouping criteria
#if 0
                if (isPureActivity(child))
                {
                    OwnedHqlExpr ret = queryPromotedFilter(transformed, no_right, 0);
                    if (ret)
                        return ret.getClear();
                }
#endif
                break;
            case no_rollup:
                //I don't think you can't move a filter over a rollup because it might affect the records rolled up.
                //unless the filter fields match the grouping criteria
#if 0
                if (isPureActivity(child))
                {
                    OwnedHqlExpr ret = queryPromotedFilter(transformed, no_left, 0);
                    if (ret)
                        return ret.getClear();
                }
#endif
                break;
            case no_selfjoin:
                if (isPureActivity(child) && !hasUnknownTransform(child) && !isLimitedJoin(child) && !child->hasProperty(fullouterAtom) && !child->hasProperty(fullonlyAtom))
                {
                    //Strictly speaking, we could hoist conditions that can be hoisted for left only (or even full) joins etc. if the fields that are filtered
                    //are based on equalities in the join condition.  However, that can wait....  (same for join below...)
                    bool canHoistLeft = !child->hasProperty(rightouterAtom) && !child->hasProperty(rightonlyAtom) &&
                                        !child->hasProperty(leftouterAtom) && !child->hasProperty(leftonlyAtom);
                    bool canMergeLeft = isInnerJoin(child);
                    bool canHoistRight = false;
                    bool canMergeRight = canMergeLeft;

                    IHqlExpression * ret = getHoistedFilter(transformed, canHoistLeft, canMergeLeft, canHoistRight, canMergeRight, 2);
                    if (ret)
                        return ret;
                }
                break;
            case no_join:
                if (isPureActivity(child) && !hasUnknownTransform(child) && !isLimitedJoin(child) && !child->hasProperty(fullouterAtom) && !child->hasProperty(fullonlyAtom))
                {
                    bool canHoistLeft = !child->hasProperty(rightouterAtom) && !child->hasProperty(rightonlyAtom);
                    bool canMergeLeft = isInnerJoin(child);
                    bool canHoistRight = !child->hasProperty(leftouterAtom) && !child->hasProperty(leftonlyAtom) && !isKeyedJoin(child);
                    bool canMergeRight = canMergeLeft;

                    IHqlExpression * ret = getHoistedFilter(transformed, canHoistLeft, canMergeLeft, canHoistRight, canMergeRight, 2);
                    if (ret)
                        return ret;
                }
                break;
            case no_select:
                {
                    IHqlExpression * ret = moveFilterOverSelect(transformed);
                    if (ret)
                        return ret;
                }
                break;
            case no_inlinetable:
                if (options & HOOfoldconstantdatasets)
                {
                    HqlExprArray conditions;
                    unwindChildren(conditions, transformed, 1);
                    OwnedITypeInfo boolType = makeBoolType();
                    OwnedHqlExpr filterCondition = createBalanced(no_and, boolType, conditions);

                    HqlExprArray filtered;
                    IHqlExpression * values = child->queryChild(0);
                    unsigned numValues = values->numChildren();
                    unsigned numOk = 0;
                    //A vague rule of thumb for the maximum proportion to retain if the dataset is shared.
                    unsigned maxSharedFiltered = (numValues >= 10) ? numValues / 10 : 1;
                    ForEachChild(i, values)
                    {
                        IHqlExpression * curTransform = values->queryChild(i);
                        if (!isKnownTransform(curTransform))
                            break;

                        NewProjectMapper2 mapper;
                        mapper.setMapping(curTransform);
                        OwnedHqlExpr expandedFilter = mapper.expandFields(filterCondition, child, NULL, NULL);
                        //This can prematurely ignore some expressions e.g., x and (' ' = ' '), but saves lots of
                        //additional constant folding on non constant expressions, so worthwhile.
                        if (!expandedFilter->isConstant())
                            break;

                        OwnedHqlExpr folded = foldHqlExpression(expandedFilter);
                        IValue * value = folded->queryValue();
                        if (!value)
                            break;

                        if (value->getBoolValue())
                        {
                            filtered.append(*LINK(curTransform));

                            //Only break sharing on an inline dataset if it generates something significantly smaller.
                            if (shared && (filtered.ordinality() > maxSharedFiltered))
                                break;
                        }

                        numOk++;
                    }
                    if (numOk == numValues)
                    {
                        if (filtered.ordinality() == 0)
                            return replaceWithNull(transformed);
                        if (filtered.ordinality() == values->numChildren())
                            return removeParentNode(transformed);

                        DBGLOG("Optimizer: Node %s reduce values in child: %s from %d to %d", queryNode0Text(transformed), queryNode1Text(child), values->numChildren(), filtered.ordinality());
                        HqlExprArray args;
                        args.append(*values->clone(filtered));
                        unwindChildren(args, child, 1);
                        decUsage(child);
                        return child->clone(args);
                    }
                }
                break;
            }
            break;

        }
    case no_keyedlimit:
        {
            node_operator childOp = child->getOperator();
            switch(childOp)
            {
            case no_distributed:
            case no_sorted:
            case no_stepped:
            case no_limit:
            case no_choosen:
            case no_compound_indexread:
            case no_compound_diskread:
            case no_assertsorted:
            case no_assertdistributed:
                return swapNodeWithChild(transformed);
            case no_if:
                return swapIntoIf(transformed);
            case no_nonempty:
                return swapIntoAddFiles(transformed);
            }
            break;
        }
    case no_hqlproject:
        {
            node_operator childOp = child->getOperator();
            IHqlExpression * transformedCountProject = transformed->queryProperty(_countProject_Atom);
            if (transformed->hasProperty(prefetchAtom))
                break;      // play safe
            IHqlExpression * transformKeyed = transformed->queryProperty(keyedAtom);
            IHqlExpression * transform = transformed->queryChild(1);
            switch(childOp)
            {
            case no_if:
                if (isComplexTransform(transform))
                    break;
                return swapIntoIf(transformed);
            case no_nonempty:
                if (isComplexTransform(transform))
                    break;
                return swapIntoAddFiles(transformed);
            case no_newusertable:
                if (isAggregateDataset(child))
                    break;
            case no_hqlproject:
                {
                    if (!isPureActivityIgnoringSkip(child) || hasUnknownTransform(child))
                        break;

                    IHqlExpression * childCountProject = child->queryProperty(_countProject_Atom);
                    //Don't merge two count projects - unless we go through and replace counter instances.
                    if (transformedCountProject && childCountProject)
                        break;
                    IHqlExpression * childKeyed = child->queryProperty(keyedAtom);
                    if (childKeyed && !transformKeyed)
                        break;

                    OwnedMapper mapper = getMapper(child);
                    IHqlExpression * transformedSeq = querySelSeq(transformed);
                    OwnedHqlExpr oldLeft = createSelector(no_left, child, transformedSeq);
                    OwnedHqlExpr newLeft = createSelector(no_left, child->queryChild(0), transformedSeq);
                    ExpandSelectorMonitor monitor(*this);
                    OwnedHqlExpr expandedTransform = expandFields(mapper, transform, oldLeft, newLeft, &monitor);
                    if (expandedTransform && !monitor.isComplex())
                    {
                        expandedTransform.setown(inheritSkips(expandedTransform, child->queryChild(1), mapper->queryTransformSelector(), newLeft));
                        DBGLOG("Optimizer: Merge %s and %s", queryNode0Text(transformed), queryNode1Text(child));
                        //NB: Merging a project with a count project can actually remove the count project..
                        IHqlExpression * countProjectAttr = transformedCountProject;
                        if (childCountProject && transformContainsCounter(expandedTransform, childCountProject->queryChild(0)))
                            countProjectAttr = childCountProject;
                        if (countProjectAttr)
                            expandedTransform.setown(createComma(LINK(expandedTransform), LINK(countProjectAttr)));
                        noteUnused(child);
                        OwnedHqlExpr ret = createDataset(op, LINK(child->queryChild(0)), createComma(expandedTransform.getClear(), LINK(transformedSeq), LINK(transformKeyed)));
                        ret.setown(child->cloneAllAnnotations(ret));
                        return transformed->cloneAllAnnotations(ret);
                    }
                    break;
                }
            case no_join:
                if (isKeyedJoin(child))
                    break;
                //fall through
            case no_selfjoin:
            case no_fetch:
            case no_normalize:
            case no_newparse:
            case no_newxmlparse:
            case no_rollupgroup:
                {
                    if (!isPureActivity(child) || !isPureActivity(transformed) || transformed->queryProperty(_countProject_Atom))
                        break;

                    IHqlExpression * transformedSeq = querySelSeq(transformed);
                    OwnedHqlExpr oldLeft = createSelector(no_left, child, transformedSeq);
                    IHqlExpression * ret = expandProjectedDataset(child, transform, oldLeft, transformed);
                    if (ret)
                        return ret;
                    break;
                }
            case no_preload:
                if (!transformedCountProject)
                    return swapNodeWithChild(transformed);
                break;
            case no_sort:
                if (transformedCountProject)
                    break;
                if (increasesRowSize(transformed))
                    break;
                return moveProjectionOverSimple(transformed, true, false);
            case no_distribute:
                if (increasesRowSize(transformed))
                    break;
                return moveProjectionOverSimple(transformed, true, false);
            case no_distributed:
            case no_sorted:
            case no_grouped:
                return moveProjectionOverSimple(transformed, false, false);
            case no_stepped:
                return moveProjectionOverSimple(transformed, true, false);
            case no_keyedlimit:
                if (isWorthMovingProjectOverLimit(transformed))
                {
                    if (child->hasProperty(onFailAtom))
                        return moveProjectionOverLimit(transformed);
                    return swapNodeWithChild(transformed);
                }
                break;
            case no_catchds:
                //could treat like a limit, but not at the moment
                break;
            case no_limit:
            case no_choosen:
                if (isWorthMovingProjectOverLimit(transformed))
                {
                    //MORE: Later this is going to be worth moving aggregates.... when we have a compound aggregates.
                    if (isPureActivity(transformed) && !isAggregateDataset(transformed) && !transformedCountProject)
                    {
                        if (child->hasProperty(onFailAtom))
                            return moveProjectionOverLimit(transformed);
                        return swapNodeWithChild(transformed);
                    }
                }
                break;
            case no_inlinetable:
                {
                    if (transformContainsSkip(transform))
                        break;
                    IHqlExpression * ret = optimizeProjectInlineTable(transformed, shared);
                    if (ret)
                        return ret;
                    break;
                }
            case no_compound_diskread:
            case no_compound_disknormalize:
            case no_compound_indexread:
            case no_compound_indexnormalize:
            case no_compound_childread:
            case no_compound_childnormalize:
            case no_compound_selectnew:
            case no_compound_inline:
                if (!transformedCountProject)
                    return swapNodeWithChild(transformed);
                break;
            case no_addfiles:
                if (transformedCountProject || isComplexTransform(transform))
                    break;
                return swapIntoAddFiles(transformed);
            }
            break;
        }
    case no_projectrow:
        {
            node_operator childOp = child->getOperator();
            switch(childOp)
            {
            case no_if:
                if (isComplexTransform(transformed->queryChild(1)))
                    break;
                return swapIntoIf(transformed);
            case no_createrow:
            case no_projectrow:
                {
                    if (!isPureActivity(child) || !isPureActivity(transformed) || hasUnknownTransform(child))
                        break;

                    IHqlExpression * transform = transformed->queryChild(1);
                    IHqlExpression * transformedSeq = querySelSeq(transformed);
                    OwnedHqlExpr oldLeft = createSelector(no_left, child, transformedSeq);
                    OwnedMapper mapper = getMapper(child);
                    ExpandSelectorMonitor monitor(*this);
                    OwnedHqlExpr expandedTransform = expandFields(mapper, transform, oldLeft, NULL, &monitor);
                    if (expandedTransform && !monitor.isComplex())
                    {
                        DBGLOG("Optimizer: Merge %s and %s", queryNode0Text(transformed), queryNode1Text(child));
                        HqlExprArray args;
                        unwindChildren(args, child);
                        args.replace(*expandedTransform.getClear(), queryTransformIndex(child));
                        noteUnused(child);
                        return createRow(child->getOperator(), args);
                    }
                    break;
                }
            }
            break;
        }
    case no_selectfields:
    case no_usertable:
        //shouldn't really have any, because we can't really process them properly.
        break;
    case no_newusertable:
        {
            node_operator childOp = child->getOperator();
            switch(childOp)
            {
            case no_if:
                if (isComplexTransform(transformed->queryChild(2)))
                    break;
                return swapIntoIf(transformed);
            case no_nonempty:
                if (isComplexTransform(transformed->queryChild(2)))
                    break;
                return swapIntoAddFiles(transformed);
            case no_newusertable:
                if (isAggregateDataset(child))
                    break;
                //fallthrough.
            case no_hqlproject:
                {
                    if (!isPureActivity(child) || hasUnknownTransform(child))
                        break;

                    if (child->hasProperty(_countProject_Atom) || child->hasProperty(prefetchAtom))
                        break;
    
                    IHqlExpression * transformKeyed = transformed->queryProperty(keyedAtom);
                    IHqlExpression * childKeyed = child->queryProperty(keyedAtom);
                    if (childKeyed && !transformKeyed)
                        break;

                    IHqlExpression * grandchild = child->queryChild(0);
                    OwnedMapper mapper = getMapper(child);

                    HqlExprArray args;
                    args.append(*LINK(grandchild));
                    args.append(*LINK(transformed->queryChild(1)));
                    
                    ExpandSelectorMonitor monitor(*this);
                    IHqlExpression * transformExpr = transformed->queryChild(2);
                    HqlExprArray assigns;
                    ForEachChild(idxt, transformExpr)
                    {
                        IHqlExpression * cur = transformExpr->queryChild(idxt);
                        IHqlExpression * tgt = cur->queryChild(0);
                        IHqlExpression * src = cur->queryChild(1);
                        assigns.append(*createAssign(LINK(tgt), expandFields(mapper, src, child, grandchild, &monitor)));
                    }
                    OwnedHqlExpr expandedTransform = transformExpr->clone(assigns);
                    args.append(*LINK(expandedTransform));

                    unsigned max = transformed->numChildren();
                    for(unsigned idx=3; idx < max; idx++)
                        args.append(*expandFields(mapper, transformed->queryChild(idx), child, grandchild, &monitor));

                    if (!monitor.isComplex())
                    {
                        DBGLOG("Optimizer: Merge %s and %s", queryNode0Text(transformed), queryNode1Text(child));
                        removeProperty(args, _internal_Atom);
                        noteUnused(child);
                        return transformed->clone(args);
                    }
                    break;
                }
            case no_join:
                if (isKeyedJoin(child))
                    break;
                //fall through
            case no_selfjoin:
            case no_fetch:
            case no_normalize:
            case no_newparse:
            case no_newxmlparse:
            case no_rollupgroup:
                {
                    if (!isPureActivity(child) || !isPureActivity(transformed))
                        break;

                    IHqlExpression * transform = transformed->queryChild(2);
                    IHqlExpression * ret = expandProjectedDataset(child, transform, child, transformed);
                    if (ret)
                        return ret;
                    break;
                }
            case no_preload:
                return swapNodeWithChild(transformed);
            case no_distribute:
            case no_sort:
                if (increasesRowSize(transformed))
                    break;
                return moveProjectionOverSimple(transformed, true, false);
            case no_distributed:
            case no_sorted:
            case no_grouped:
                return moveProjectionOverSimple(transformed, false, false);
            case no_stepped:
                return moveProjectionOverSimple(transformed, false, true);
            case no_keyedlimit:
            case no_limit:
            case no_choosen:
                if (isWorthMovingProjectOverLimit(transformed))
                {
                    if (isPureActivity(transformed) && !isAggregateDataset(transformed))
                    {
                        if (child->hasProperty(onFailAtom))
                            return moveProjectionOverLimit(transformed);
                        return swapNodeWithChild(transformed);
                    }
                }
                break;
            case no_compound_diskread:
            case no_compound_disknormalize:
            case no_compound_indexread:
            case no_compound_indexnormalize:
            case no_compound_childread:
            case no_compound_childnormalize:
            case no_compound_selectnew:
            case no_compound_inline:
                if (!isAggregateDataset(transformed))
                    return swapNodeWithChild(transformed);
                break;
            case no_addfiles:
                if (isComplexTransform(transformed->queryChild(2)))
                    break;
                return swapIntoAddFiles(transformed);
            case no_inlinetable:
                {
                    IHqlExpression * ret = optimizeProjectInlineTable(transformed, shared);
                    if (ret)
                        return ret;
                    break;
                }
            }
            break;
        }
    case no_group:
        {
            switch (child->getOperator())
            {
            case no_group:
                {
                    IHqlExpression * newChild = child;
                    bool isLocal = transformed->hasProperty(localAtom);
                    while (newChild->getOperator() == no_group)
                    {
                        if (newChild->queryProperty(allAtom))
                            break;

                        if (queryRealChild(newChild, 1))
                        {
                            //Don't allow local groups to remove non-local groups.
                            if (isLocal && !newChild->hasProperty(localAtom))
                                break;
                        }
                        noteUnused(newChild);
                        newChild = newChild->queryChild(0);
                    }
                    if (child == newChild)
                        break;

                    if (queryGrouping(transformed) == queryGrouping(newChild))
                    {
                        decUsage(newChild);         // since will inherit usage on return
                        return LINK(newChild);
                    }
                    return replaceChild(transformed, newChild);
                }
            case no_hqlproject:
            case no_newusertable:
                //Move ungroups() over projects to increase the likely hood of combining projects and removing groups
//              if (!queryRealChild(transformed, 1) && !child->hasProperty(_countProject_Atom) && !isAggregateDataset(child))
//                  return swapNodeWithChild(transformed);
                break;
            }
            break;
        }
    //GH->Ilka no_enth now has a different format, may want to do something with that as well.
    case no_sample:
        {
            IValue * const1 = transformed->queryChild(1)->queryValue();
            if (const1)
            {
                __int64 val1 = const1->getIntValue();
                if (val1 == 1)
                    return removeParentNode(transformed);

                node_operator childOp = child->getOperator();
                switch(childOp)
                {
                case no_hqlproject:
                case no_newusertable:
                    if (isPureActivity(child) && !child->hasProperty(_countProject_Atom) && !child->hasProperty(prefetchAtom) && !isAggregateDataset(child))
                        return swapNodeWithChild(transformed);
                    break;
                }
            }
            break;
        }
    case no_sort:
        {
            switch(child->getOperator())
            {
            case no_sort:
                if (!isLocalActivity(transformed) || isLocalActivity(child))
                    return removeChildNode(transformed);
                break;
            case no_distributed:
            case no_distribute:
            case no_keyeddistribute:
                if (!isLocalActivity(transformed))
                    return removeChildNode(transformed);        // no transform()
                break;
            }
            break;
        }
    case no_keyeddistribute:
    case no_distribute:
        {
            if (transformed->hasProperty(skewAtom))
                break;
            //If distribution matches existing and grouped then don't distribute, but still remove grouping.
            IHqlExpression * distn = queryDistribution(transformed);
            switch(child->getOperator())
            {
            case no_distributed:
            case no_distribute:
            case no_keyeddistribute:
            case no_sort:
                if (!transformed->hasProperty(mergeAtom))
                    return removeChildNode(transformed);
                break;
            case no_dedup:
                {
                    IHqlExpression * ret = optimizeDistributeDedup(transformed);
                    if (ret)
                        return ret;
                    break;
                }
            case no_addfiles:
                if ((distn == queryDistribution(child->queryChild(0))) ||
                    (distn == queryDistribution(child->queryChild(1))))
                    return swapIntoAddFiles(transformed);
                break;
            }
            break;
        }
    case no_distributed:
        {
            switch(child->getOperator())
            {
            case no_distribute:
            case no_distributed:
                if (transformed->queryChild(1) == child->queryChild(1))
                    return removeParentNode(transformed);
                break;
            case no_compound_diskread:
            case no_compound_disknormalize:
            case no_compound_indexread:
            case no_compound_indexnormalize:
                return swapNodeWithChild(transformed);
            }
            break;
        }
    case no_sorted:
        {
            switch(child->getOperator())
            {
            case no_compound_diskread:
            case no_compound_disknormalize:
            case no_compound_indexread:
            case no_compound_indexnormalize:
                return swapNodeWithChild(transformed);
            }
            break;
        }
    case no_aggregate:
    case no_newaggregate:
        {
            node_operator childOp = child->getOperator();
            switch(childOp)
            {
            case no_if:
                return swapIntoIf(transformed);
            case no_nonempty:
                return swapIntoAddFiles(transformed);
            case no_compound_diskread:
            case no_compound_disknormalize:
            case no_compound_indexread:
            case no_compound_indexnormalize:
            case no_compound_childread:
            case no_compound_childnormalize:
                if (!isGrouped(child) && (options & HOOhascompoundaggregate) && !transformed->hasProperty(localAtom))
                {
                    IHqlExpression * ret = optimizeAggregateCompound(transformed);
                    if (ret)
                        return ret;
                }
                break;
            case no_thisnode:
                return swapNodeWithChild(transformed);
            }

            //MORE: The OHOinsidecompound isn't really good enough - because might remove projects from
            //nested child aggregates which could benifit from them.  Probably not as long as all compound 
            //activities support aggregation.  In fact test should be removable everywhere once all 
            //engines support the new activities.
            if (isGrouped(transformed->queryChild(0)) || (queryRealChild(transformed, 3) && !(options & HOOinsidecompound)))
                break;
            return optimizeAggregateDataset(transformed);
        }
    case NO_AGGREGATE:
        return optimizeAggregateDataset(transformed);

    case no_fetch:
        {
            //NB: Required for fetch implementation
            node_operator childOp = child->getOperator();
            switch(childOp)
            {
            case no_newusertable:
                if (isAggregateDataset(child))
                    break;
                //fallthrough.
            case no_hqlproject:
                if (!hasUnknownTransform(child))
                {
                    OwnedMapper mapper = getMapper(child);
                    IHqlExpression * selSeq = querySelSeq(transformed);
                    OwnedHqlExpr oldLeft = createSelector(no_left, child, selSeq);
                    OwnedHqlExpr newLeft = createSelector(no_left, child->queryChild(0), selSeq);
                    IHqlExpression * expanded = expandFields(mapper, transformed->queryChild(3), oldLeft, newLeft);
                    if (expanded)
                    {
                        DBGLOG("Optimizer: Merge %s and %s", queryNode0Text(transformed), queryNode1Text(child));
                        HqlExprArray args;
                        args.append(*LINK(child->queryChild(0)));
                        args.append(*LINK(transformed->queryChild(1)));
                        args.append(*LINK(transformed->queryChild(2)));
                        args.append(*expanded);
                        args.append(*LINK(selSeq));
                        return transformed->clone(args);
                    }
                }
                break;
            }
            break;
        }
    case no_addfiles:
        {
            //MORE: This is possibly worth doing even if the children are shared.
            HqlExprArray allTransforms;
            bool ok = true;
            ForEachChild(i, transformed)
            {
                IHqlExpression * cur = transformed->queryChild(i);
                if (!cur->isAttribute())
                {
                    if (cur->getOperator() != no_inlinetable)
                    {
                        ok = false;
                        break;
                    }
                    cur->queryChild(0)->unwindList(allTransforms, no_transformlist);
                }
            }
            if (!ok)
                break;
            DBGLOG("Optimizer: Merge inline tables for %s", queryNode0Text(transformed));
            HqlExprArray args;
            args.append(*createValue(no_transformlist, makeNullType(), allTransforms));
            args.append(*LINK(child->queryRecord()));

            ForEachChild(i2, transformed)
            {
                IHqlExpression * cur = transformed->queryChild(i2);
                if (!cur->isAttribute())
                    decUsage(cur);
            }

            OwnedHqlExpr ret = createDataset(no_inlinetable, args);
            return transformed->cloneAllAnnotations(ret);
        }

#if 0
        //Something like the following might theoretically be useful, but seems to cause problems not commoning up
    case no_select:
        if (transformed->hasProperty(newAtom) && !childrenAreShared(child))
        {
            OwnedHqlExpr ret = transformTrivialSelectProject(transformed);
            if (ret)
            {
                DBGLOG("Optimizer: Select %s from %s optimized", ret->queryChild(1)->queryName()->str(), queryNode1Text(child));
                noteUnused(child);
                return ret.getClear();
            }
        }
        break;
#endif

    case no_datasetfromrow:
        {
            node_operator childOp = child->getOperator();
            switch (childOp)
            {
            case no_projectrow:
                {
                    break;
                    IHqlExpression * grand = child->queryChild(0);
                    IHqlExpression * base = createDatasetFromRow(LINK(grand));
                    HqlExprArray args;
                    unwindChildren(args, child);
                    args.replace(*base, 0);
                    return createDataset(no_hqlproject, args);
                }
            case no_createrow:
                {
                    DBGLOG("Optimizer: Merge %s and %s to Inline table", queryNode0Text(transformed), queryNode1Text(child));
                    HqlExprArray args;
                    args.append(*createValue(no_transformlist, makeNullType(), LINK(child->queryChild(0))));
                    args.append(*LINK(child->queryRecord()));
                    OwnedHqlExpr ret = createDataset(no_inlinetable, args);
                    ret.setown(child->cloneAllAnnotations(ret));
                    return transformed->cloneAllAnnotations(ret);
                }
            }
            break;
        }
    case no_join:
        {
            if (isKeyedJoin(transformed) || transformed->hasProperty(lookupAtom))
            {
                node_operator childOp = child->getOperator();
                switch (childOp)
                {
                case no_newusertable:
                case no_hqlproject:
                    {
                        if (!isPureActivity(child) || child->queryProperty(_countProject_Atom) || child->hasProperty(prefetchAtom))
                            break;
                        IHqlExpression * transform = queryNewColumnProvider(child);
                        if (transformContainsSkip(transform) || !isSimpleTransformToMergeWith(transform))
                            break;

                        OwnedMapper mapper = getMapper(child);
                        IHqlExpression * transformedSeq = querySelSeq(transformed);
                        OwnedHqlExpr oldLeft = createSelector(no_left, child, transformedSeq);
                        OwnedHqlExpr newLeft = createSelector(no_left, child->queryChild(0), transformedSeq);

                        bool ok = true;
                        HqlExprArray args;
                        args.append(*LINK(child->queryChild(0)));
                        args.append(*LINK(transformed->queryChild(1)));

                        ExpandSelectorMonitor monitor(*this);
                        ForEachChildFrom(i, transformed, 2)
                        {
                            OwnedHqlExpr expanded = expandFields(mapper, transformed->queryChild(i), oldLeft, newLeft, &monitor);
                            if (expanded && !monitor.isComplex())
                            {
                                args.append(*expanded.getClear());
                            }
                            else
                            {
                                ok = false;
                                break;
                            }
                        }

                        if (ok)
                        {
                            //If expanding the project removed all references to left (very silly join....) make it an all join
                            if (transformed->hasProperty(lookupAtom) && !exprReferencesDataset(&args.item(2), newLeft))
                                args.append(*createAttribute(allAtom));
                            DBGLOG("Optimizer: Merge %s and %s", queryNode0Text(transformed), queryNode1Text(child));
                            noteUnused(child);
                            return transformed->clone(args);
                        }
                        break;
                    }
                }
            }
            break;
        }
    case no_selectnth:
        {
            node_operator childOp = child->getOperator();
            switch(childOp)
            {
            case no_sort:
                {
                    IHqlExpression * index = transformed->queryChild(1);
                    if (getIntValue(index, 99999) <= 100 && !isGrouped(child))
                    {
                        HqlExprArray topnArgs;
                        unwindChildren(topnArgs, child);
                        topnArgs.add(*LINK(index), 2);
                        OwnedHqlExpr topn = createDataset(no_topn, topnArgs);
                        incUsage(topn);
                        DBGLOG("Optimizer: Replace %s with %s", queryNode0Text(child), queryNode1Text(topn));
                        HqlExprArray selectnArgs;
                        selectnArgs.append(*child->cloneAllAnnotations(topn));
                        unwindChildren(selectnArgs, transformed, 1);
                        return transformed->clone(selectnArgs);
                    }
                    break;
                }
            }
        }
    }

    return LINK(transformed);
}


IHqlExpression * CTreeOptimizer::defaultCreateTransformed(IHqlExpression * expr)
{
    return PARENT::createTransformed(expr);
}

TableProjectMapper * CTreeOptimizer::getMapper(IHqlExpression * expr)
{
    return new TableProjectMapper(expr);
}

bool CTreeOptimizer::isShared(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_null:
        return false;
    case no_spillgraphresult:
    case no_spill:
    case no_split:
    case no_throughaggregate:
    case no_commonspill:
        return true;
    }
    return (queryBodyExtra(expr)->useCount > 1);
}

bool CTreeOptimizer::isSharedOrUnknown(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_null:
        return false;
    case no_spillgraphresult:
    case no_spill:
    case no_split:
    case no_throughaggregate:
    case no_commonspill:
        return true;
    }
    OptTransformInfo * extra = queryBodyExtra(expr);
    return (extra->useCount != 1);
}

IHqlExpression * optimizeHqlExpression(IHqlExpression * expr, unsigned options)
{
    //The no_compound can get very heavily nested => unwind to save stack traversal.  We really should support nary no_compound
    HqlExprArray args, newArgs;
    unwindCommaCompound(args, expr);
    optimizeHqlExpression(newArgs, args, options);
    return createActionList(newArgs);
}


void optimizeHqlExpression(HqlExprArray & target, HqlExprArray & source, unsigned options)
{
    CTreeOptimizer optimizer(options);
    optimizer.analyseArray(source, 0);
    optimizer.transformRoot(source, target);
}


/*
Implementation issues:
1. References to transformed items.

x := project(w, ...);
y := filter(x, ...);
z := distibute(y, x.fx);

when x and y are switched, all references to x need to be replaced by x'

y' := filter(w, ...);
x' := project(y', ...);
z := distibute(x', x'.fx);

Need to map an selector, where selector->queryNormalized() == oldDataset->queryNormalized() and replace with newDataset->queryNormalized()

However, the mapping is context dependant - depending on what the parent dataset is.

Could either have transformed[parentDataset] or could post process the transformed expression.

So to process efficiently, we need:
a) transformedSelector[parentCtx];
b) transformed[parentCtx]
c) on dataset transform, set dataset->queryNormalizedSelector()->transformedSelector[ctx] to newDataset->queryNormalizedSelector();
d) on mapping, replace with i) queryTransformed(x) or queryNomalizedSelector()->transformedSelector[ctx];

Could either have
expr->queryExtra()->transformedSelector[parentCtx]
or
::transformSelector[parentCtx, expr]

First is not likely to affect many nodes - since only will be set on datasets.
Second is likely to use much less memory, and probably as quick - trading an extra indirection+construction time with an assign to a structure.

Have a noComma(top-ds, prev-ctx) to mark the current context.  
*** Only need to change if dataset is visible inside the arguments to the ECL syntax ***
Use an array of ctx, where tos is current don't seed with a dummy value - because will cause commas to be created 

The idea of the transformedSelector should also be generalized: 
if (!transformed) try transformedSelector, and set transformedSelector to result.



- should we replace the boolean flags in CHqlExpression with a mask?
  i) would make anding /oring more efficient.
  ii) would make adding code generator helpers much less painful - use 32bits and allocate from top down for the code generator.
  
Useful flags
- context free - not getresults or access to fields in unrelated tables.
- unconditional?
- look at transforms and see what causes pain.

2. optimizing shared items.
* When is it worthwhile?
  o removing duplicate sorts?
  o when it only removes a node e.g., count(project).
  o when would enable operation to be done more efficiently.  ??Eg.

* Need to differentiate between a use and a reference - only link count former.

*/
