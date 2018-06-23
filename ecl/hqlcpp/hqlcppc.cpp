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
    if (columnType->getTypeCode()==type_table)
        pointerType.setown(makePointerType(makeConstantModifier(LINK(columnType))));
    IHqlExpression * temp = createValue(no_implicitcast, LINK(pointerType), LINK(address));
    return createValue(no_deref, LINK(columnType), temp);
}


