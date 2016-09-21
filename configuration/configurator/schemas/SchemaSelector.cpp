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
    assert(pSchemaRoot != nullptr);
    assert(pParentNode != nullptr);
    assert(pParentNode->getNodeType() == XSD_KEY || pParentNode->getNodeType() == XSD_KEYREF || pParentNode->getNodeType() == XSD_UNIQUE);

    if (pSchemaRoot == nullptr || pParentNode == nullptr)
    {
        // TODO: Throw Exception
        assert(false);
        return nullptr;
    }

    CSelector *pSelector = nullptr;

    if (xpath != nullptr && *xpath != 0)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == nullptr)
        {
            assert(!"Selector required");
            // TODO: throw MakeExceptionFromMap("EX_STR_MISSING_SELECTOR_MISSING");
        }
        const char* pXPath = pTree->queryProp(XML_ATTR_XPATH);
        assert(pXPath != nullptr && *pXPath != 0);

        if (pXPath == nullptr || *pXPath == 0)
        {
            assert(!"Throw Exception");
            return nullptr;
             // TODO: throw exception
        }

        pSelector = new CSelector(pParentNode);
        pSelector->setXSDXPath(xpath);
        pSelector->setXPath(pXPath);

        const char *pID = pTree->queryProp(XML_ATTR_ID);
        if (pID != nullptr)
            pSelector->setID(pID);
    }
    return pSelector;
}

void CSelector::dump(::std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    quickOutHeader(cout, XSD_SELECTOR_STR, offset);
    QUICK_OUT(cout, XPath, offset);
    QUICK_OUT(cout, ID, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    quickOutFooter(cout, XSD_SELECTOR_STR, offset);
}

void CSelector::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    this->setEnvXPath(&(strXPath.str()[1]));  // strip out .
    PROGLOG("Function: %s() at %s:%d", __func__, __FILE__, __LINE__);
    PROGLOG("Setting selector %s to EnvXPath = %s", this->getXSDXPath(), this->getEnvXPath());
}
