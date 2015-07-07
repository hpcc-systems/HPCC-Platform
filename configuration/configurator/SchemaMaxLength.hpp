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

#ifndef _SCHEMA_MAX_LENGTH_HPP_
#define _SCHEMA_MAX_LENGTH_HPP_

#include "SchemaCommon.hpp"

class CMaxLength : public CXSDNodeWithRestrictions<CMaxLength>
{
    friend class CXSDNodeWithRestrictions<CMaxLength>;
public:

    virtual ~CMaxLength()
    {
    }
    static CMaxLength* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTERINT(MaxLength)

protected:

    CMaxLength(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CMaxLength>::CXSDNodeWithRestrictions(pParentNode, XSD_MAX_LENGTH), m_nMaxLength(-1)
    {
    }
};

#endif // _SCHEMA_MAX_LENGTH_HPP_
