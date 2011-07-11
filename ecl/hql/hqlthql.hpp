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
#ifndef HQLTHQL_INCL
#define HQLTHQL_INCL

#include "jlib.hpp"
#include "hql.hpp"
#include "hqlexpr.hpp"

extern "C" HQL_API StringBuffer &toECL(IHqlExpression * expr, StringBuffer &s, bool recurse=false, bool xgmmlGraphText = false);
extern "C" HQL_API StringBuffer &toECLSimple(IHqlExpression * expr, StringBuffer &s);
extern "C" HQL_API void splitECL(IHqlExpression * expr, StringBuffer &s, StringBuffer &d);
extern "C" HQL_API StringBuffer &toUserECL(StringBuffer &s, IHqlExpression * expr, bool recurse);
extern "C" HQL_API StringBuffer &getExprECL(IHqlExpression * expr, StringBuffer & out, bool minimalSelectors=false, bool xgmmlGraphText=false);
extern "C" HQL_API StringBuffer &getRecordECL(IHqlExpression * expr, StringBuffer & out);
extern "C" HQL_API StringBuffer & processedTreeToECL(IHqlExpression * expr, StringBuffer &s);
extern "C" HQL_API StringBuffer &getExprIdentifier(StringBuffer & out, IHqlExpression * expr);
extern "C" HQL_API void dbglogExpr(IHqlExpression * expr);

#endif
