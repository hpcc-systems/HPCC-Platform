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
