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
#ifndef HQLTHQL_INCL
#define HQLTHQL_INCL

#include "jlib.hpp"
#include "hql.hpp"
#include "hqlexpr.hpp"

extern HQL_API StringBuffer &regenerateECL(IHqlExpression * expr, StringBuffer &s);
extern HQL_API StringBuffer &regenerateDefinition(IHqlExpression * expr, StringBuffer &s);
extern HQL_API StringBuffer &toECL(IHqlExpression * expr, StringBuffer &s, bool recurse=false, bool xgmmlGraphText = false);
extern HQL_API StringBuffer &toECLSimple(IHqlExpression * expr, StringBuffer &s);
extern HQL_API void splitECL(IHqlExpression * expr, StringBuffer &s, StringBuffer &d);
extern HQL_API StringBuffer &toUserECL(StringBuffer &s, IHqlExpression * expr, bool recurse);
extern HQL_API StringBuffer &getExprECL(IHqlExpression * expr, StringBuffer & out, bool minimalSelectors=false, bool xgmmlGraphText=false);
extern HQL_API StringBuffer &getRecordECL(IHqlExpression * expr, StringBuffer & out);
extern HQL_API StringBuffer & processedTreeToECL(IHqlExpression * expr, StringBuffer &s);
extern HQL_API StringBuffer &getExprIdentifier(StringBuffer & out, IHqlExpression * expr);
extern HQL_API void dbglogExpr(IHqlExpression * expr);

#endif
