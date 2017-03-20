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
    virtual void pushScope(IHqlExpression * context);
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
