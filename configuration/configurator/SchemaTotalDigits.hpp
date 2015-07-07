/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#ifndef _SCHEMA_TOTAL_DIGITS_HPP_
#define _SCHEMA_TOTAL_DIGITS_HPP_

#include "SchemaCommon.hpp"

class CTotalDigits : public CXSDNodeWithRestrictions<CTotalDigits>
{
    friend class CXSDNodeWithRestrictions<CTotalDigits>;
public:

    virtual ~CTotalDigits()
    {
    }
    static CTotalDigits* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTERINT(TotalDigits)

protected:

    CTotalDigits(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CTotalDigits>::CXSDNodeWithRestrictions(pParentNode, XSD_TOTAL_DIGITS), m_nTotalDigits(-1)
    {
    }
};

#endif // _SCHEMA_TOTAL_DIGITS_HPP_
