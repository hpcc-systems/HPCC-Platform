/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "workunit.hpp"

#include "hql.hpp"
#include "hqlexpr.hpp"
#include "hqltrans.ipp"
#include "hqlctrans.hpp"
#include "hqlir.hpp"

//===========================================================================

/*
 * Expressions like IF(cond, a, FAIL) will prevent common sub expressions being created
 * However the outer-most expression will be executed unconditionally, so it is best to wrap it in a no_pure()
 * so it is no longer marked as throwing an exception - which means it can then be commoned up and optimized.
 */
static HqlTransformerInfo conditionalErrorTransformerInfo("ConditionalErrorTransformer");
class ConditionalErrorTransformer : public NewHqlTransformer
{
public:
    ConditionalErrorTransformer() : NewHqlTransformer(conditionalErrorTransformerInfo) {}

    void analyseExpr(IHqlExpression * expr)
    {
        IHqlExpression * body = expr->queryBody();
        const node_operator op = body->getOperator();
        const bool revisited = alreadyVisited(body);
        const bool wasWrapped = alreadyWrapped;

        //Find the outermost conditions that surround conditional errors
        if (containsScalarThrow(body))
        {
            if (!alreadyWrapped && canWrap(body))
            {
                //Indicate this condition should be wrapped with no_pure()
                queryTransformExtra(body)->spareByte1 = true;

                if (!revisited)
                {
                    //Allow the conditions for if() and case() to also be wrapped
                    unsigned first = 0;
                    if (op == no_if || op == no_case)
                    {
                        analyseExpr(body->queryChild(0));
                        first++;
                    }

                    alreadyWrapped = true;
                    ForEachChildFrom(i, body, first)
                        analyseExpr(body->queryChild(i));
                    alreadyWrapped = false;
                }
                return;
            }
        }
        else
            alreadyWrapped = false;

        if (!revisited)
        {
            NewHqlTransformer::analyseExpr(body);

            //Prevent pure from being wrapped around an expression twice
            if (op == no_pure)
                queryTransformExtra(body->queryChild(0)->queryBody())->spareByte1 = false;
        }

        alreadyWrapped = wasWrapped;
   }


    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        //Do not transform within a no_pat_instance - if it no longer matches the definition pattern it will fail to match
        switch (expr->getOperator())
        {
        case no_pat_instance:
            return LINK(expr);
        }

        IHqlExpression * body = expr->queryBody();
        if (body != expr)
        {
            OwnedHqlExpr transformedBody = transform(body);
            return expr->cloneAllAnnotations(transformedBody);
        }

        OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
        //Check if transformed contains a scalar throw in addition to the flag set in analyse. It may have changed
        //if an expression further down the tree also had a no_pure() added because it was executed unconditionally
        if (containsScalarThrow(transformed))
        {
            if (queryTransformExtra(body)->spareByte1)
               return createWrapper(no_pure, transformed.getClear(), createAttribute(throwAtom));
        }
        return transformed.getClear();
    }

protected:
    bool canWrap(IHqlExpression * expr) const
    {
        if (expr->isAction())
            return false;

        switch (expr->getOperator())
        {
        case no_if:
        case no_case:
        case no_map:
            return true;
        }
        return false;
    }


protected:
    bool alreadyWrapped = false;
};


IHqlExpression * optimizeConditionalErrors(IHqlExpression * expr)
{
    ConditionalErrorTransformer transformer;
    transformer.analyse(expr, 0);
    return transformer.transformRoot(expr);
}

void optimizeConditionalErrors(HqlExprArray & exprs)
{
    ConditionalErrorTransformer transformer;
    transformer.analyseArray(exprs, 0);

    HqlExprArray result;
    ForEachItemIn(i, exprs)
        result.append(*transformer.transformRoot(&exprs.item(i)));
    exprs.swapWith(result);
}
