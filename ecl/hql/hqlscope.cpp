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
#include "jmisc.hpp"
#include "jfile.hpp"
#include "jiter.ipp"
#include "jexcept.hpp"
#include "jmutex.hpp"
#include "jutil.hpp"

#include "hqlscope.ipp"
#include "hqlerrors.hpp"
#include "hqlthql.hpp"
#include "hqlerror.hpp"

//---------------------------------------------------------------------------

void ScopeCheckerBase::analyseExpr(IHqlExpression * expr)
{
    ScopeCheckerInfo * info = (ScopeCheckerInfo *)queryTransformExtra(expr);
    IHqlExpression * curScope = queryCurrentScope();
    if (info->lastScope == curScope)
        return;
    info->lastScope.set(curScope);

    if (expr->isNamedSymbol())
        named.append(*expr);
    doAnalyseExpr(expr);
    if (expr->isNamedSymbol())
        named.pop();
}


ANewTransformInfo * ScopeCheckerBase::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(ScopeCheckerInfo, expr);
}


IHqlExpression * ScopeCheckerBase::queryCurrentScope()
{
    if (!curScopeState)
        curScopeState.setown(getScopeState());
    return curScopeState;
}

void ScopeCheckerBase::pushScope(IHqlExpression * context)  { curScopeState.clear(); ScopedTransformer::pushScope(context); }
void ScopeCheckerBase::pushEvaluateScope(IHqlExpression * expr, IHqlExpression * transformed) { curScopeState.clear(); ScopedTransformer::pushEvaluateScope(expr, transformed); }
void ScopeCheckerBase::popScope()                               { curScopeState.clear(); ScopedTransformer::popScope(); }
void ScopeCheckerBase::suspendScope()                           { curScopeState.clear(); ScopedTransformer::suspendScope(); }
void ScopeCheckerBase::restoreScope()                           { curScopeState.clear(); ScopedTransformer::restoreScope(); }
void ScopeCheckerBase::clearDataset(bool nested)                { curScopeState.clear(); ScopedTransformer::clearDataset(nested); }
bool ScopeCheckerBase::setDataset(IHqlExpression * _dataset, IHqlExpression * _transformed) 
                                                            { curScopeState.clear(); return ScopedTransformer::setDataset(_dataset, _transformed); }
bool ScopeCheckerBase::setDatasetLeft(IHqlExpression * _dataset, IHqlExpression * _transformed, IHqlExpression * seq)   
                                                            { curScopeState.clear(); return ScopedTransformer::setDatasetLeft(_dataset, _transformed, seq); }
bool ScopeCheckerBase::setLeft(IHqlExpression * _left, IHqlExpression * seq)            
                                                            { curScopeState.clear(); return ScopedTransformer::setLeft(_left, seq); }
bool ScopeCheckerBase::setLeftRight(IHqlExpression * _left, IHqlExpression * _right, IHqlExpression * seq) 
                                                            { curScopeState.clear(); return ScopedTransformer::setLeftRight(_left, _right, seq); }
bool ScopeCheckerBase::setTopLeftRight(IHqlExpression * _dataset, IHqlExpression * _transformed, IHqlExpression * seq)
                                                            { curScopeState.clear(); return ScopedTransformer::setTopLeftRight(_dataset, _transformed, seq); }

void ScopeCheckerBase::suspendAllScopes(ScopeSuspendInfo & info)
{
    curScopeState.clear();
    ScopedTransformer::suspendAllScopes(info);
}

void ScopeCheckerBase::restoreScopes(ScopeSuspendInfo & info)
{
    curScopeState.clear(); 
    ScopedTransformer::restoreScopes(info);
}


//---------------------------------------------------------------------------

static HqlTransformerInfo scopeConsistencyCheckerInfo("ScopeConsistencyChecker");
ScopeConsistencyChecker::ScopeConsistencyChecker() : ScopeCheckerBase(scopeConsistencyCheckerInfo)
{
}

void ScopeConsistencyChecker::checkConsistent(IHqlExpression * root, const HqlExprArray & _activeTables)
{
    ForEachItemIn(i, _activeTables)
        activeTables.append(OLINK(_activeTables.item(i)));
    if (root->isDataset())
        pushScope(root);
    analyse(root, 0);
    if (root->isDataset())
        popScope();
}


void ScopeConsistencyChecker::ensureActive(IHqlExpression * ds)
{
    if (isActive(ds))
        return;
    StringBuffer s;
    if (isActive(ds->queryNormalizedSelector()))
        s.append("Dataset isn't normalized");
    else
        s.append("Dataset isn't in scope");
    throw MakeStringException(ERR_ASSERT_WRONGSCOPING, "%s", s.str());
}


bool ScopeConsistencyChecker::isActive(IHqlExpression * ds)
{
    switch (ds->getOperator())
    {
    case no_self:
    case no_selfref:
    case no_activetable:
        return true;
    }
    
    if (activeTables.find(*ds) != NotFound)
        return true;

    ForEachItemIn(i, scopeStack)
    {
        ScopeInfo & cur = scopeStack.item(i);
        
        switch (ds->getOperator())
        {
        case no_left:
            if (cur.left && recordTypesMatch(cur.left, ds))
                return true;
            break;
        case no_right:
            if (cur.right && recordTypesMatch(cur.right, ds))
                return true;
            break;
        default:
            if (cur.dataset && cur.dataset->queryNormalizedSelector() == ds)
                return true;
            break;
        }
    }
    return false;
}

void ScopeConsistencyChecker::doAnalyseExpr(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_activerow:
        ensureActive(expr->queryChild(0));
        return;
    case no_select:
        {
            bool isNew;
            IHqlExpression * ds = querySelectorDataset(expr, isNew);
            if (isNew)
                ScopedTransformer::analyseChildren(expr);
            else
                ensureActive(ds);
            return;
        }
    }

    ScopedTransformer::analyseExpr(expr);
}



extern HQL_API void checkIndependentOfScope(IHqlExpression * expr)
{
    if (!expr || expr->isIndependentOfScope())
        return;

    HqlExprCopyArray scopeUsed;
    expr->gatherTablesUsed(scopeUsed);

    HqlExprArray exprs;
    exprs.append(*LINK(expr));
    ForEachItemIn(i, scopeUsed)
        exprs.append(OLINK(scopeUsed.item(i)));

    EclIR::dbglogIR(exprs);
}

extern HQL_API void checkNormalized(IHqlExpression * expr)
{
    if (!expr)
        return;

    ScopeConsistencyChecker checker;
    HqlExprArray activeTables;
    checker.checkConsistent(expr, activeTables);
}

extern HQL_API void checkNormalized(IHqlExpression * expr, HqlExprArray & activeTables)
{
    if (!expr)
        return;
    ScopeConsistencyChecker checker;
    checker.checkConsistent(expr, activeTables);
}

