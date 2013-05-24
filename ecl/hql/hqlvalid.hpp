/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#ifndef __HQLVALID_HPP_
#define __HQLVALID_HPP_

#include "hqlexpr.hpp"

//Checking functions
extern HQL_API void reportAbstractModule(IErrorReceiver * errors, IHqlExpression * expr, const ECLlocation & errpos);
extern HQL_API IHqlExpression * checkCreateConcreteModule(IErrorReceiver * errors, IHqlExpression * expr, const ECLlocation & errpos);
extern HQL_API IHqlExpression * checkCreateConcreteModule(IErrorReceiver * errors, IHqlExpression * expr, const IHqlExpression * locationExpr);
extern HQL_API IHqlExpression * createLocationAttr(ISourcePath * filename, int lineno, int column, int position);

extern HQL_API IHqlExpression * queryAmbiguousRollupCondition(IHqlExpression * expr, bool strict);
extern HQL_API IHqlExpression * queryAmbiguousRollupCondition(IHqlExpression * expr, const HqlExprArray & selects);
extern HQL_API void filterAmbiguousRollupCondition(HqlExprArray & ambiguousSelects, HqlExprArray & selects, IHqlExpression * expr);

#endif
