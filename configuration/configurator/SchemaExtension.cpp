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

#include "XMLTags.h"
#include "jptree.hpp"
#include "SchemaExtension.hpp"
#include "SchemaComplexType.hpp"
#include "SchemaSchema.hpp"
#include "ConfigSchemaHelper.hpp"

static const char* DEFAULT_ENVIRONMENT_XSD("Environment.xsd");

void CExtension::dump(std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_EXTENSION_STR, offset);
    QUICK_OUT(cout, Base, offset);
    QUICK_OUT(cout, XSDXPath,  offset);

    if (this->getBaseNode() != NULL)
        this->getBaseNode()->dump(cout, offset);

    QuickOutFooter(cout, XSD_EXTENSION_STR, offset);
}

const char* CExtension::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0 && m_pXSDNode != NULL)
        m_pXSDNode->getXML(NULL);

    return NULL;
}

void CExtension::initExtension()
{
    NODE_TYPES eNodeType[] = { XSD_SIMPLE_TYPE, XSD_SIMPLE_CONTENT };

    const CXSDNodeBase *pBaseNode = (dynamic_cast<const CXSDNodeBase*>(this));

    if (pBaseNode != NULL)
    {
        pBaseNode->getNodeByTypeAndNameAscending( eNodeType, this->getBase());
        this->setBaseNode(const_cast<CXSDNodeBase*>(pBaseNode));
    }
}

CExtension* CExtension::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL)
        return NULL;

    CExtension *pExtension = NULL;

    if (xpath && *xpath)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == NULL)
            return NULL; // no xs:extension node

        const char* pBase = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_BASE);

        if (pBase != NULL)
        {
            pExtension = new CExtension(pParentNode);
            pExtension->setXSDXPath(xpath);
            pExtension->setBase(pBase);
        }
    }

    if (pExtension != NULL)
        CConfigSchemaHelper::getInstance()->addExtensionToBeProcessed(pExtension);

    return pExtension;
}
