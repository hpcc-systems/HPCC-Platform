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

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/hql/hqlscope.cpp $ $Id: hqlscope.cpp 66009 2011-07-06 12:28:32Z ghalliday $");

static void getECL(IHqlExpression * expr, StringBuffer & s)
{
    toUserECL(s, expr, false);
    if (s.length() > 2)
        s.setLength(s.length()-2);
}

static const char * queryName(StringBuffer & s, IHqlExpression * expr)
{
    if (expr->queryName())
        return s.append(expr->queryName()).str();
    getECL(expr, s);
    return s.str();
}

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

void ScopeCheckerBase::pushScope()                              { curScopeState.clear(); ScopedTransformer::pushScope(); }
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
        pushScope();
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
    throw MakeStringException(ERR_ASSERT_WRONGSCOPING, s.str());
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

