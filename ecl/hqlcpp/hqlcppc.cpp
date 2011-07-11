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
#include "platform.h"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"

#include "hql.hpp"
#include "hqlexpr.hpp"
#include "hqlfunc.hpp"
#include "hqlcpputil.hpp"

#include "hqlstmt.hpp"
#include "hqlwcpp.hpp"
#include "hqlcpp.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/hqlcpp/hqlcppc.cpp $ $Id: hqlcppc.cpp 63158 2011-03-11 22:39:30Z ghalliday $");

IHqlExpression * convertAddressToValue(IHqlExpression * address, ITypeInfo * columnType)
{
    if (isTypePassedByAddress(columnType) && !columnType->isReference())
    {
        Owned<ITypeInfo> refType = makeReferenceModifier(LINK(columnType));
        assertex(address->getOperator() == no_externalcall || refType == address->queryType());
        return createValue(no_implicitcast, LINK(refType), LINK(address));
    }

    Owned<ITypeInfo> pointerType = makePointerType(LINK(columnType));
    assertex(address->getOperator() == no_externalcall || pointerType == address->queryType());
    IHqlExpression * temp = createValue(no_implicitcast, LINK(pointerType), LINK(address));
    return createValue(no_deref, LINK(columnType), temp);
}


