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
};

extern HQL_API IHqlExpression * optimizeHqlExpression(IHqlExpression * expr, unsigned options);
extern HQL_API void optimizeHqlExpression(HqlExprArray & target, HqlExprArray & source, unsigned options);

#endif
