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
#ifndef __HQLOPT_HPP_
#define __HQLOPT_HPP_

#include "jlib.hpp"
#include "hqlexpr.hpp"

enum
{
    HOOfold                     = 0x0001,
    HOOcompoundproject          = 0x0002,
    HOOnoclonelimit             = 0x0004,
    HOOnocloneindexlimit        = 0x0008,
    HOOinsidecompound           = 0x0010,
    HOOfiltersharedproject      = 0x0020,
    HOOhascompoundaggregate     = 0x0040,
    HOOfoldconstantdatasets     = 0x0080,
    HOOalwayslocal              = 0x0100,
    HOOexpensive                = 0x0200,   // include potentially expensive optimizations
};

extern HQL_API IHqlExpression * optimizeHqlExpression(IHqlExpression * expr, unsigned options);
extern HQL_API void optimizeHqlExpression(HqlExprArray & target, HqlExprArray & source, unsigned options);

#endif
