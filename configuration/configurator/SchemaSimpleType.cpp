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
#include "XMLTags.h"
#include "SchemaSimpleType.hpp"
#include "SchemaRestriction.hpp"
#include "ConfigSchemaHelper.hpp"
#include "SchemaMapManager.hpp"
#include "jlib.hpp"

using namespace CONFIGURATOR;

#define StringBuffer ::StringBuffer
#define IPropertyTree ::IPropertyTree

CXSDNodeBase* CSimpleType::getNodeByTypeAndNameAscending(NODE_TYPES eNodeType, const char *pName)
{
    return (this->checkSelf(eNodeType, pName, this->getName()) ? this : NULL);
}

CXSDNodeBase* CSimpleType::getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName)
{
    return (this->checkSelf(eNodeType, pName, this->getName()) ? this : NULL);
}

void CSimpleType::dump(::std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_SIMPLE_TYPE_STR, offset);
    QUICK_OUT_2(Name);
    QUICK_OUT_2(ID);
    QUICK_OUT(cout, XSDXPath,   offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT(cout, EnvValueFromXML,  offset);

    if (m_pRestriction != NULL)
        m_pRestriction->dump(cout, offset);

    QuickOutFooter(cout, XSD_SIMPLE_TYPE_STR, offset);
}

void CSimpleType::getDocumentation(StringBuffer &strDoc) const
{
    if (m_pRestriction != NULL)
        m_pRestriction->getDocumentation(strDoc);
}

void CSimpleType::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    this->setEnvXPath(strXPath);

    if (this->m_pRestriction != NULL)
        this->m_pRestriction->populateEnvXPath(strXPath);
}

void CSimpleType::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert(pEnvTree != NULL);

    if (this->m_pRestriction != NULL)
        this->m_pRestriction->loadXMLFromEnvXml(pEnvTree);
}

CSimpleType* CSimpleType::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL || pParentNode == NULL)
        return NULL;

    IPropertyTree *pTree = pSchemaRoot->queryPropTree(xpath);

    if (pTree == NULL)
        return NULL;

    const char* pName =  NULL;
    const char* pID =  NULL;

    pName = pTree->queryProp(XML_ATTR_NAME);
    pID = pTree->queryProp(XML_ATTR_ID);

    CSimpleType* pSimpleType = new CSimpleType(pParentNode, pName,pID);
    pSimpleType->setXSDXPath(xpath);
    assert(pSimpleType != NULL);

    if (pSimpleType == NULL)
        return NULL;

    StringBuffer strXPathExt(xpath);
    strXPathExt.append("/").append(XSD_TAG_RESTRICTION);

    CRestriction *pRestriction = CRestriction::load(pSimpleType, pSchemaRoot, strXPathExt.str());

    if (pRestriction != NULL)
        pSimpleType->setRestriciton(pRestriction);
    if (pName != NULL)
        CConfigSchemaHelper::getInstance()->getSchemaMapManager()->setSimpleTypeWithName(pName, pSimpleType);

    return pSimpleType;
}

const char* CSimpleType::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length () == 0)
    {
        m_strXML.append("<").append(getName()).append("\n");
        m_strXML.append("<").append(getID()).append("\n");

        if (m_pRestriction != NULL)
            m_strXML.append(m_pRestriction->getXML(NULL));

        m_strXML.append("/>\n");
    }
    return m_strXML.str();
}

bool CSimpleType::checkConstraint(const char *pValue) const
{
    if (this->getRestriction() == NULL)
        return true;
    else
        return this->getRestriction()->checkConstraint(pValue);
}

void CSimpleTypeArray::dump(::std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_SIMPLE_TYPE_ARRAY_STR, offset);
    QUICK_OUT(cout, XSDXPath, offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT(cout, EnvValueFromXML,  offset);
    QUICK_OUT_ARRAY(cout, offset);
    QuickOutFooter(cout, XSD_SIMPLE_TYPE_ARRAY_STR, offset);
}

CSimpleTypeArray* CSimpleTypeArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pParentNode != NULL);
    assert(pSchemaRoot != NULL);
    if (pSchemaRoot == NULL)
        return NULL;

    CSimpleTypeArray *pSimpleTypeArray = new CSimpleTypeArray(pParentNode);
    pSimpleTypeArray->setXSDXPath(xpath);

    Owned<IPropertyTreeIterator> elemIter = pSchemaRoot->getElements(xpath);

    int count = 1;
    ForEach(*elemIter)
    {
        StringBuffer strXPathExt(xpath);
        strXPathExt.appendf("[%d]", count);

        CSimpleType *pSimpleType = CSimpleType::load(pSimpleTypeArray, pSchemaRoot, strXPathExt.str());

        assert(pSimpleType != NULL);
        pSimpleTypeArray->append(*pSimpleType);

        count++;
    }
    return pSimpleTypeArray;
}

void CSimpleTypeArray::getDocumentation(StringBuffer &strDoc) const
{
    QUICK_DOC_ARRAY(strDoc);
}

void CSimpleTypeArray::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    assert(index == 1);  // Only 1 array of elements per node

    this->setEnvXPath(strXPath);
    QUICK_ENV_XPATH(strXPath)
}

void CSimpleTypeArray::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert(pEnvTree != NULL);
    QUICK_LOAD_ENV_XML(pEnvTree)
}
