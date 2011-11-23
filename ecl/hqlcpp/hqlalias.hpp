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

#ifndef __HQLALIAS_HPP_
#define __HQLALIAS_HPP_

#include "hqlexpr.hpp"

class ConditionSet;
class ConditionItem : public CInterface
{
public:
    ConditionItem(IHqlExpression * _expr, ConditionSet * _parent);

    bool equals(IHqlExpression * _expr, ConditionSet * _parent) const
    {
        return (expr == _expr) && (parent == _parent);
    }

    bool isAlwaysConditionalOn(IHqlExpression * expr) const;

protected:
    LinkedHqlExpr expr;
    ConditionSet * parent;
};


class ConditionSet
{
public:
    ConditionSet() { unconditional = false; nesting = 0; }

    bool addOrCondition(IHqlExpression * expr, ConditionSet * parent);

    bool isAlwaysConditionalOn(IHqlExpression * expr);
    bool isUnconditonal() const { return unconditional; }
    IHqlExpression * getGuardCondition() const;
    void setUnconditional();

protected:
    CIArrayOf<ConditionItem> conditions;
    unsigned nesting;
    bool unconditional;
    struct
    {
        HqlExprAttr search;
        bool value;
    } isAlwaysCache;
};


class ConditionTracker
{
public:
    void pushCondition(IHqlExpression * expr, ConditionSet * parent);
    void popCondition();
    bool addActiveCondition(ConditionSet & conditions);

protected:
    PointerArrayOf<IHqlExpression> conditionStack;
    PointerArrayOf<ConditionSet> parentStack;
};


IHqlExpression * optimizeNestedConditional(IHqlExpression * expr);
void optimizeNestedConditional(HqlExprArray & exprs);

#endif
