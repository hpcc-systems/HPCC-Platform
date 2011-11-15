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

#include "platform.h"
#include "jlib.hpp"
#include "jset.hpp"

#include "hql.hpp"
#include "hqlutil.hpp"
#include "hqltrans.ipp"

#include "hqlalias.hpp"

//---------------------------------------------------------------------------------------------------------------------

static bool isSubsetOf(IHqlExpression * search, IHqlExpression * expr)
{
    loop
    {
        if (search->queryBody() == expr->queryBody())
            return true;
        if (expr->getOperator() != no_and)
            return false;
        //MORE: Remove if think it is worth doing more work
//        return false;
        //Check in order most likely to no recurse too deeply.
        if (isSubsetOf(search, expr->queryChild(1)))
            return true;
        expr = expr->queryChild(0);
    }
}

ConditionItem::ConditionItem(IHqlExpression * _expr, ConditionSet * _parent)
: expr(_expr), parent(_parent)
{
}

bool ConditionItem::isAlwaysConditionalOn(IHqlExpression * search) const
{
    if (isSubsetOf(search, expr))
        return true;
    if (!parent)
        return false;
    return parent->isAlwaysConditionalOn(search);
}

//---------------------------------------------------------------------------------------------------------------------

bool ConditionSet::addOrCondition(IHqlExpression * expr, ConditionSet * parent)
{
    if (unconditional)
        return false;

    if (!expr)
    {
        setUnconditional();
        return true;
    }

    ForEachItemIn(iMerge, conditions)
    {
        ConditionItem & cur = conditions.item(iMerge);
        if (cur.equals(expr, parent))
            return false;
    }

    conditions.append(*new ConditionItem(expr, parent));
    return true;
}

void ConditionSet::setUnconditional()
{
    unconditional = true;
    conditions.kill();
}

/*
IHqlExpression * ConditionSet::getGuardCondition() const
{
    if (unconditional)
        return NULL;
    HqlExprArray values;
    ForEachItemIn(i, conditions)
        values.append(*conditions.item(i).getCondition());

    OwnedITypeInfo boolType = makeBoolType();
    return createBalanced(no_or, boolType, values);
}
*/

bool ConditionSet::isAlwaysConditionalOn(IHqlExpression * expr)
{
    if (!expr)
        return true;

    if (unconditional)
        return false;

    //Cache the previous search value to avoid a potentially exponential search of the caller tree.
    if (expr == isAlwaysCache.search)
        return isAlwaysCache.value;

    bool matches = true;
    ForEachItemIn(i, conditions)
    {
        ConditionItem & cur = conditions.item(i);
        if (!cur.isAlwaysConditionalOn(expr))
        {
            matches = false;
            break;
        }
    }

    isAlwaysCache.search.set(expr);
    isAlwaysCache.value = matches;
    return matches;
}



//---------------------------------------------------------------------------------------------------------------------

void ConditionTracker::pushCondition(IHqlExpression * expr, ConditionSet * parent)
{
    assertex(expr);
    conditionStack.append(expr);
    parentStack.append(parent);
}

void ConditionTracker::popCondition()
{
    conditionStack.pop();
    parentStack.pop();
}

bool ConditionTracker::addActiveCondition(ConditionSet & conditions)
{
    if (conditionStack.ordinality() == 0)
        return conditions.addOrCondition(NULL, NULL);
    return conditions.addOrCondition(conditionStack.tos(), parentStack.tos());
}



//---------------------------------------------------------------------------------------------------------------------

class NestedIfInfo : public NewTransformInfo
{
public:
    NestedIfInfo(IHqlExpression * _original) : NewTransformInfo(_original)
    {
        isShared = false;
        containsIf = false;
        conditions = NULL;
    }
    ~NestedIfInfo() { delete conditions; }

    ConditionSet * queryConditions()
    {
        if (!conditions)
            conditions = new ConditionSet;
        return conditions;
    }
public:
    ConditionSet * conditions;
    bool isShared;
    bool containsIf;
};



//MORE: Could remove dependancy on insideCompound if it was ok to have compound operators scattered through the
//		contents of a compound item.  Probably would cause few problems, and would make life simpler
class NestedIfTransformer : public NewHqlTransformer
{
public:
    NestedIfTransformer();

    IHqlExpression * process(IHqlExpression * expr);
    bool process(const HqlExprArray & exprs, HqlExprArray & transformed);

protected:
    void analyseGatherIfs(IHqlExpression * expr);
    void analyseNoteConditions(IHqlExpression * expr);
    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr)
    {
        return new NestedIfInfo(expr);
    }

    inline NestedIfInfo * queryBodyExtra(IHqlExpression * expr)	{ return static_cast<NestedIfInfo *>(queryTransformExtra(expr->queryBody())); }

protected:
    unsigned numIfs;
    ConditionTracker tracker;
};


static HqlTransformerInfo nestedIfTransformerInfo("NestedIfTransformer");
NestedIfTransformer::NestedIfTransformer() : NewHqlTransformer(nestedIfTransformerInfo)
{
    numIfs = 0;
}

void NestedIfTransformer::analyseExpr(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    switch (pass)
    {
    case 0:
        analyseGatherIfs(body);
        break;
    case 1:
        analyseNoteConditions(body);
        break;
    }
}


void NestedIfTransformer::analyseGatherIfs(IHqlExpression * expr)
{
    if (expr->getOperator() == no_if)
        numIfs++;

    NestedIfInfo * extra = queryBodyExtra(expr);
    if (alreadyVisited(expr))
    {
        extra->isShared = true;
        if (extra->containsIf)
            numIfs++;
        return;
    }

    unsigned prevIfCount = numIfs;
    NewHqlTransformer::analyseExpr(expr);
    if (prevIfCount != numIfs)
        extra->containsIf = true;
}


void NestedIfTransformer::analyseNoteConditions(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    NestedIfInfo * extra = queryBodyExtra(expr);
    if (extra->isShared || (op == no_if))
    {
        if (!tracker.addActiveCondition(*extra->queryConditions()))
            return;
    }

    if (!extra->containsIf)
        return;

    if (op == no_if)
    {
        IHqlExpression * cond = expr->queryChild(0);
        OwnedHqlExpr normalCond = getNormalizedCondition(cond);
        analyseExpr(cond);
        tracker.pushCondition(normalCond, extra->queryConditions());
        analyseExpr(expr->queryChild(1));
        tracker.popCondition();

        IHqlExpression * falseExpr = queryRealChild(expr, 2);
        if (falseExpr)
        {
            OwnedHqlExpr inverseCond = getInverse(normalCond);
            tracker.pushCondition(inverseCond, extra->queryConditions());
            analyseExpr(falseExpr);
            tracker.popCondition();
        }
    }
    else
    {
       NewHqlTransformer::analyseExpr(expr);
    }
}


IHqlExpression * NestedIfTransformer::createTransformed(IHqlExpression * expr)
{
    if (expr->getOperator() == no_if)
    {
        IHqlExpression * cond = expr->queryChild(0);
        IHqlExpression * falseExpr = queryRealChild(expr, 2);

        OwnedHqlExpr normalCond = getNormalizedCondition(cond);
        NestedIfInfo * extra = queryBodyExtra(expr);
        IHqlExpression * selected = NULL;
        if (extra->queryConditions()->isAlwaysConditionalOn(normalCond))
        {
            selected = expr->queryChild(1);
        }
        else if (falseExpr)
        {
            OwnedHqlExpr inverseCond = getInverse(normalCond);
            if (extra->queryConditions()->isAlwaysConditionalOn(inverseCond))
                selected = falseExpr;
        }

        if (selected)
        {
            const char * branch = (selected == falseExpr) ? "false" : "true";
            StringBuffer exprText, locationText;
            appendLocation(locationText, queryLocation(expr), ": ");
            DBGLOG("%s%s replaced with %s branch since condition always %s", locationText.str(), queryChildNodeTraceText(exprText, expr), branch, branch);
            OwnedHqlExpr ret = transform(selected);
            return cloneMissingAnnotations(expr, ret);
        }
    }
    return NewHqlTransformer::createTransformed(expr);
}


IHqlExpression * NestedIfTransformer::process(IHqlExpression * expr)
{
    analyse(expr, 0);
    if (numIfs < 2)
        return LINK(expr);
    analyse(expr, 1);
    return transformRoot(expr);
}


bool NestedIfTransformer::process(const HqlExprArray & exprs, HqlExprArray & transformed)
{
    ForEachItemIn(i1, exprs)
        analyse(&exprs.item(i1), 0);
    if (numIfs < 2)
        return false;
    ForEachItemIn(i2, exprs)
        analyse(&exprs.item(i2), 1);
    transformRoot(exprs, transformed);
    return true;
}


IHqlExpression * optimizeNestedConditional(IHqlExpression * expr)
{
    NestedIfTransformer transformer;
    return transformer.process(expr);
}


void optimizeNestedConditional(HqlExprArray & exprs)
{
    NestedIfTransformer transformer;
    HqlExprArray transformed;
    if (transformer.process(exprs, transformed))
        exprs.swapWith(transformed);
}
