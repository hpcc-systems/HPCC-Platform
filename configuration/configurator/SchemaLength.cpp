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

#include "jptree.hpp"
#include "SchemaLength.hpp"
#include "ConfigSchemaHelper.hpp"

using namespace CONFIGURATOR;

#define IPropertyTree ::IPropertyTree

CLength* CLength::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    if (pSchemaRoot == NULL)
        return NULL;

    CLength *pLength = NULL;

    if (xpath && *xpath)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);
        if (pTree == NULL)
            return NULL; // no xs:length node

        const char* pValue = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_VALUE);
        if (pValue != NULL)
        {
            if (atoi(pValue) < 0)
                 throw MakeExceptionFromMap(EX_STR_MISSING_VALUE_ATTRIBUTE_IN_LENGTH);

            pLength = new CLength(pParentNode);
            pLength->setXSDXPath(xpath);
            pLength->setValue(pValue);
        }
        else
        {
            assert(!"value attribute can be empty!");
            throw MakeExceptionFromMap(EX_STR_MISSING_VALUE_ATTRIBUTE_IN_LENGTH);
        }
    }
    return pLength;
}

void CLength::dump(::std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_LENGTH_STR, offset);
    QUICK_OUT(cout, Value, offset);
    QuickOutFooter(cout, XSD_LENGTH_STR, offset);
}

