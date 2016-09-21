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

#ifndef _SCHEMA_MIN_INCLUSIVE_HPP_
#define _SCHEMA_MIN_INCLUSIVE_HPP_

#include "SchemaCommon.hpp"

namespace CONFIGURATOR
{

class CMinInclusive : public CXSDNodeWithRestrictions<CMinInclusive>
{
    friend class CXSDNodeWithRestrictions<CMinInclusive>;
public:

    virtual ~CMinInclusive()
    {
    }
    static CMinInclusive* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTERINT(MinInclusive)

protected:

    CMinInclusive(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CMinInclusive>::CXSDNodeWithRestrictions(pParentNode, XSD_MIN_INCLUSIVE), m_nMinInclusive(-1)
    {
    }
};

}
#endif // _SCHEMA_MIN_INCLUSIVE_HPP_
