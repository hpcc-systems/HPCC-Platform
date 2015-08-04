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
#include "hql.hpp"

#include "platform.h"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "javahash.hpp"
#include "jmd5.hpp"
#include "jfile.hpp"
#include "eclhelper.hpp"

#include "hqlfunc.hpp"

#include "hqlattr.hpp"
#include "hqlcpp.ipp"
#include "hqlpopt.hpp"

//Optimize IF(a,b,c) op x to IF(a,b op x, c OP x)
//But be careful because it uncommons attributes increasing the size of the queries.
static IHqlExpression * peepholeOptimizeCompare(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * lhs = expr->queryChild(0);
    if (ctx.queryMatchExpr(lhs))
        return LINK(expr);

    IHqlExpression * rhs = expr->queryChild(1);
    if (!rhs->isConstant() || (lhs->getOperator() != no_if))
        return LINK(expr);

    IHqlExpression * ifCond = lhs->queryChild(0);
    IHqlExpression * ifTrue = lhs->queryChild(1);
    IHqlExpression * ifFalse = lhs->queryChild(2);
    assertex(ifFalse);

    node_operator op = expr->getOperator();
    OwnedHqlExpr newTrue = createValue(op, makeBoolType(), LINK(ifTrue), LINK(rhs));
    OwnedHqlExpr newFalse = createValue(op, makeBoolType(), LINK(ifFalse), LINK(rhs));
    OwnedHqlExpr newIf = createValue(no_if, makeBoolType(), LINK(ifCond), peepholeOptimize(ctx, newTrue), peepholeOptimize(ctx, newFalse));
    return expr->cloneAllAnnotations(newIf);
}


//Optimize search [not] in set(ds, value) to
//[not] exists(ds, value=search); - really need link counts on the expressions, and should be done in the real optimizer

IHqlExpression * peepholeOptimize(BuildCtx & ctx, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_eq:
    case no_ne:
    case no_le:
    case no_lt:
    case no_ge:
    case no_gt:
        return peepholeOptimizeCompare(ctx, expr);
    case no_and:
    case no_or:
    case no_not:
        {
            HqlExprArray args;
            bool same = true;
            ForEachChild(i, expr)
            {
                IHqlExpression * cur = expr->queryChild(i);
                IHqlExpression * optimized = peepholeOptimize(ctx, expr);
                args.append(*optimized);
                if (cur != optimized)
                    same = false;
            }
            if (!same)
                return expr->clone(args);
            break;
        }
    }
    return LINK(expr);
}

