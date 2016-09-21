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

#include "SchemaSimpleContent.hpp"
#include "SchemaAnnotation.hpp"
#include "SchemaExtension.hpp"
#include "SchemaRestriction.hpp"

using namespace CONFIGURATOR;

#define IPropertyTree ::IPropertyTree

CSimpleContent* CSimpleContent::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    if (pParentNode == nullptr || pSchemaRoot == nullptr)
        return nullptr;

    CExtension *pExtension = nullptr;
    CAnnotation *pAnnotation = nullptr;
    CRestriction *pRestriction = nullptr;
    IPropertyTree *pTree = pSchemaRoot->queryPropTree(xpath);

    if(pTree == nullptr)
        return nullptr;

    StringBuffer strXPathExt(xpath);
    strXPathExt.append("/").append(XSD_TAG_EXTENSION);

    if (pSchemaRoot->queryPropTree(strXPathExt.str()) != nullptr)
    {
        pExtension = CExtension::load(nullptr, pSchemaRoot, strXPathExt.str());

        if (pExtension != nullptr)
            pExtension->initExtension();
    }

    strXPathExt.set(xpath);
    strXPathExt.append("/").append(XSD_TAG_ANNOTATION);

    if (pSchemaRoot->queryPropTree(strXPathExt.str()) != nullptr)
        pAnnotation = CAnnotation::load(nullptr, pSchemaRoot, strXPathExt.str());

    strXPathExt.set(xpath);
    strXPathExt.append("/").append(XSD_TAG_RESTRICTION);

    if (pSchemaRoot->queryPropTree(strXPathExt.str()) != nullptr)
        pRestriction = CRestriction::load(nullptr, pSchemaRoot, strXPathExt.str());

    const char* pID =  nullptr;
    pID = pTree->queryProp(XML_ATTR_ID);

    CSimpleContent *pSimpleContent = new CSimpleContent(pParentNode, pID);

    SETPARENTNODE(pExtension, pSimpleContent);
    SETPARENTNODE(pAnnotation, pSimpleContent);
    SETPARENTNODE(pRestriction, pSimpleContent);

    return pSimpleContent;
}

CSimpleContent::~CSimpleContent()
{
    delete m_pRestriction;
    delete m_pAnnotation;
    delete m_pExtension;
}

bool CSimpleContent::checkConstraint(const char *pValue) const
{
    return true;
}
