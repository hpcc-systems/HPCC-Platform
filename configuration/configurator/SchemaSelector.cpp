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

#include "SchemaSelector.hpp"
#include "SchemaCommon.hpp"

using namespace CONFIGURATOR;

#define IPropertyTree ::IPropertyTree
#define StringBuffer ::StringBuffer

CSelector* CSelector::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    assert(pParentNode != NULL);
    assert(pParentNode->getNodeType() == XSD_KEY || pParentNode->getNodeType() == XSD_KEYREF || pParentNode->getNodeType() == XSD_UNIQUE);

    if (pSchemaRoot == NULL || pParentNode == NULL)
    {
        // TODO: Throw Exception
        assert(false);
        return NULL;
    }

    CSelector *pSelector = NULL;

    if (xpath != NULL && *xpath != 0)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == NULL)
        {
            assert(!"Selector required");
            // TODO: throw MakeExceptionFromMap("EX_STR_MISSING_SELECTOR_MISSING");
        }
        const char* pXPath = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_XPATH);
        assert(pXPath != NULL && *pXPath != 0);

        if (pXPath == NULL || *pXPath == 0)
        {
            assert(!"Throw Exception");
            return NULL;
             // TODO: throw exception
        }
        if (pXPath != NULL)
        {
            pSelector = new CSelector(pParentNode);
            pSelector->setXSDXPath(xpath);
            pSelector->setXPath(pXPath);
        }
        else
        {
            assert(!"selector can not be be empty!");
            // TODO: throw MakeExceptionFromMap(EX_STR_MISSING_VALUE_ATTRIBUTE_IN_LENGTH);
        }

        const char *pID = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_ID);
        if (pID != NULL)
         pSelector->setID(pID);
    }
    return pSelector;
}

void CSelector::dump(::std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_SELECTOR_STR, offset);
    QUICK_OUT(cout, XPath, offset);
    QUICK_OUT(cout, ID, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QuickOutFooter(cout, XSD_SELECTOR_STR, offset);
}

void CSelector::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    this->setEnvXPath(&(strXPath.str()[1]));  // strip out .
    PROGLOG("Function: %s() at %s:%d", __func__, __FILE__, __LINE__);
    PROGLOG("Setting selector %s to EnvXPath = %s", this->getXSDXPath(), this->getEnvXPath());
}
