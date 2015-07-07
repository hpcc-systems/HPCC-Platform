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

#include "SchemaFractionDigits.hpp"
#include "XMLTags.h"
#include "jptree.hpp"
#include <cstdlib>

CFractionDigits* CFractionDigits::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    if (pSchemaRoot == NULL)
        return NULL;

    CFractionDigits *pFractionDigits = new CFractionDigits(pParentNode);
    pFractionDigits->setXSDXPath(xpath);

    if (xpath != NULL && *xpath != 0)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == NULL)
            return pFractionDigits;

        const char* pValue = pTree->queryProp(XML_ATTR_VALUE);

        if (pValue != NULL && *pValue != 0)
        {
            pFractionDigits->setFractionDigits(pValue);
            pFractionDigits->setValue(pValue);
        }

        if (pFractionDigits->getFractionDigits() < 0)  // not set or bad length value
        {
            delete pFractionDigits;
            pFractionDigits = NULL;

            //throw MakeExceptionFromMap(EX_STR_LENGTH_VALUE_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ZERO, EACTION_FRACTION_DIGITS_HAS_BAD_LENGTH);
            assert(false);
        }
    }
    return pFractionDigits;
}

void CFractionDigits::dump(std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_FRACTION_DIGITS_STR, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT(cout, Value, offset);
    QUICK_OUT(cout, FractionDigits, offset);
    QuickOutFooter(cout, XSD_FRACTION_DIGITS_STR, offset);
}
