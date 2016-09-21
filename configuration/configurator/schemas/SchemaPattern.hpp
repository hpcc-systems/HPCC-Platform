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

#ifndef _SCHEMA_PATTERN_HPP_
#define _SCHEMA_PATTERN_HPP_

#include "SchemaCommon.hpp"

namespace CONFIGURATOR
{

class CPattern : public CXSDNodeWithRestrictions<CPattern>
{
    friend class CXSDNodeWithRestrictions<CPattern>;
public:

    virtual ~CPattern()
    {
    }
    static CPattern* load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath);

    GETTERSETTER(Pattern)

private:

    CPattern(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithRestrictions<CPattern>::CXSDNodeWithRestrictions(pParentNode, XSD_PATTERN)
    {
    }
};

}
#endif // _SCHEMA_PATTERN_HPP_
