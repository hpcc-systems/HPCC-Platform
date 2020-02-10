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
#ifndef __HQLFOLD_HPP_
#define __HQLFOLD_HPP_

#ifdef HQL_EXPORTS
#define HQLFOLD_API DECL_EXPORT
#else
#define HQLFOLD_API DECL_IMPORT
#endif

#include "jlib.hpp"
#include "hqlexpr.hpp"
#include "hqlstack.hpp"

//Options for the constant folding...
enum { 
    HFOx_op_not_x       = 0x0001,                       // constant fold X AND NOT X => false, X OR NOT X => true
    HFOcanbreakshared   = 0x0002,                   // ok to perform folding that may mean expressions are no longer shared
    HFOfoldimpure       = 0x0004,
    HFOthrowerror       = 0x0008,
    HFOloseannotations  = 0x0020,
    HFOforcefold        = 0x0040,
    HFOfoldfilterproject= 0x0080,
    HFOconstantdatasets = 0x0100,
    HFOpercolateconstants = 0x0200,
    HFOpercolatefilters = 0x0400,
};


/* Fold a single IHqlExpression * node */
extern HQLFOLD_API IHqlExpression * foldConstantOperator(IHqlExpression * expr, unsigned options);

/* Fold an expression tree, but does not do anything with no_select/transform, so valid on non-normalized trees */
extern HQLFOLD_API IHqlExpression * quickFoldExpression(IHqlExpression * expr, unsigned options=0);
extern HQLFOLD_API void quickFoldExpressions(HqlExprArray & target, const HqlExprArray & source, unsigned options);

extern HQLFOLD_API IHqlExpression * foldHqlExpression(IHqlExpression * expr); // No errors reported.
extern HQLFOLD_API IHqlExpression * foldHqlExpression(IErrorReceiver & errorProcessor, IHqlExpression * expr, unsigned options=0);
extern HQLFOLD_API IHqlExpression * foldScopedHqlExpression(IErrorReceiver & errorProcessor, IHqlExpression * dataset, IHqlExpression * expr, unsigned options=0);
extern HQLFOLD_API void foldHqlExpression(IErrorReceiver & errorProcessor, HqlExprArray & tgt, HqlExprArray & src, unsigned options=0);
extern HQLFOLD_API IHqlExpression * getLowerCaseConstantExpr(IHqlExpression * expr);
extern HQLFOLD_API IHqlExpression * normalizeFilenameExpr(IHqlExpression * expr);
extern HQLFOLD_API IHqlExpression * foldExprIfConstant(IHqlExpression * expr);
extern HQLFOLD_API bool queryCompareConstantValues(int & result, IHqlExpression * left, IHqlExpression * right);
extern HQLFOLD_API IHqlExpression * foldConstantCaseExpr(IHqlExpression * expr);
extern HQLFOLD_API IHqlExpression * foldConstantMapExpr(IHqlExpression * expr);

extern HQLFOLD_API bool areExclusiveConditions(IHqlExpression * left, IHqlExpression * right);

#endif
