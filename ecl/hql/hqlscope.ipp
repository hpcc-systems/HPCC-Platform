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
#ifndef HQLSCOPE_IPP
#define HQLSCOPE_IPP

#include "hqlscope.hpp"
#include "hqltrans.ipp"
#include "hqlutil.hpp"


class HQL_API ScopeCheckerInfo : public ANewTransformInfo
{
public:
    ScopeCheckerInfo(IHqlExpression * _original) : ANewTransformInfo(_original) {}

    virtual IHqlExpression * queryTransformed()                 { UNIMPLEMENTED; }
    virtual IHqlExpression * queryTransformedSelector()         { UNIMPLEMENTED; }
    virtual void setTransformed(IHqlExpression * expr)          { UNIMPLEMENTED; }
    virtual void setTransformedSelector(IHqlExpression * expr)  { UNIMPLEMENTED; }

public:
    HqlExprAttr         lastScope;
};


class ScopeCheckerBase : public ScopedTransformer
{
protected:
    ScopeCheckerBase(HqlTransformerInfo & _info) : ScopedTransformer(_info) {}

    virtual void analyseExpr(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

    IHqlExpression * queryCurrentScope();

// overload to keep track of the current scope.
    virtual void pushScope();
    virtual void pushEvaluateScope(IHqlExpression * expr, IHqlExpression * _transformed);
    virtual void popScope();

    virtual void suspendScope();
    virtual void restoreScope();

    virtual void clearDataset(bool nested);
    virtual bool setDataset(IHqlExpression * _dataset, IHqlExpression * _transformed);
    virtual bool setDatasetLeft(IHqlExpression * _dataset, IHqlExpression * _transformed, IHqlExpression * _seq);
    virtual bool setLeft(IHqlExpression * _left, IHqlExpression * _seq);
    virtual bool setLeftRight(IHqlExpression * _left, IHqlExpression * _right, IHqlExpression * _seq);
    virtual bool setTopLeftRight(IHqlExpression * _dataset, IHqlExpression * _transformed, IHqlExpression * _seq);

    virtual void suspendAllScopes(ScopeSuspendInfo & info);
    virtual void restoreScopes(ScopeSuspendInfo & info);

    virtual void doAnalyseExpr(IHqlExpression * expr) = 0;

protected:
    HqlExprAttr curScopeState;
    HqlExprCopyArray named;
};


//This is only valid when executed on a normalized tree.
class ScopeConsistencyChecker : public ScopeCheckerBase
{
public:
    ScopeConsistencyChecker();

    void checkConsistent(IHqlExpression * root, const HqlExprArray & _activeTables);

protected:
    void ensureActive(IHqlExpression * ds);
    virtual void doAnalyseExpr(IHqlExpression * expr);
    bool isActive(IHqlExpression * ds);

protected:
    HqlExprArray activeTables;
};

#endif
