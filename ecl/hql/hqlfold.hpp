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
#ifndef __HQLFOLD_HPP_
#define __HQLFOLD_HPP_

#ifdef HQL_EXPORTS
#define HQLFOLD_API __declspec(dllexport)
#else
#define HQLFOLD_API __declspec(dllimport)
#endif

#include "jlib.hpp"
#include "hqlexpr.hpp"
#include "hqlstack.hpp"

interface ITemplateContext;

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
extern HQLFOLD_API IHqlExpression * foldConstantOperator(IHqlExpression * expr, unsigned options, ITemplateContext *context);

/* Fold an expression tree, but does not do anything with no_select/transform, so valid on non-normalized trees */
extern HQLFOLD_API IHqlExpression * quickFoldExpression(IHqlExpression * expr, ITemplateContext *context=NULL, unsigned options=0);
extern HQLFOLD_API void quickFoldExpressions(HqlExprArray & target, const HqlExprArray & source, ITemplateContext *context, unsigned options);

extern HQLFOLD_API IHqlExpression * foldHqlExpression(IHqlExpression * expr, ITemplateContext *context=NULL, unsigned options=0);
extern HQLFOLD_API IHqlExpression * foldScopedHqlExpression(IHqlExpression * dataset, IHqlExpression * expr, unsigned options=0);
extern HQLFOLD_API void foldHqlExpression(HqlExprArray & tgt, HqlExprArray & src, unsigned options=0);
extern HQLFOLD_API IHqlExpression * lowerCaseHqlExpr(IHqlExpression * expr);
extern HQLFOLD_API IHqlExpression * foldExprIfConstant(IHqlExpression * expr);

#endif
