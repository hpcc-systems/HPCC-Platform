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

#include "SchemaAttributeGroup.hpp"
#include "DocumentationMarkup.hpp"
#include "ConfigSchemaHelper.hpp"
#include "jptree.hpp"
#include "SchemaMapManager.hpp"
#include"JSONMarkUp.hpp"

using namespace CONFIGURATOR;

#define StringBuffer ::StringBuffer
#define IPropertyTree ::IPropertyTree

const CXSDNodeBase* CAttributeGroup::getNodeByTypeAndNameAscending(NODE_TYPES eNodeType, const char *pName) const
{
    const CXSDNodeBase* pMatchingNode = NULL;

    if (eNodeType == this->getNodeType() && (pName != NULL ? !strcmp(pName, this->getNodeTypeStr()) : true))
    {
        assert(pName != NULL); // for now pName should always be populated
        return this;
    }

    if (m_pAttributeArray != NULL)
        pMatchingNode =  m_pAttributeArray->getNodeByTypeAndNameAscending(eNodeType, pName);
    if (pMatchingNode == NULL && m_pAttributeArray != NULL)
         pMatchingNode =  m_pAttributeArray->getNodeByTypeAndNameDescending(eNodeType, pName);
    return pMatchingNode;
}

CAttributeGroup::~CAttributeGroup()
{
}

const CXSDNodeBase* CAttributeGroup::getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName) const
{
    const CXSDNodeBase* pMatchingNode = NULL;

    if (eNodeType == this->getNodeType() && (pName != NULL ? !strcmp(pName, this->getNodeTypeStr()) : true))
    {
        assert(pName != NULL); // for now pName should always be populated
        return this;
    }

    if (m_pAttributeArray != NULL)
        pMatchingNode =  m_pAttributeArray->getNodeByTypeAndNameDescending(eNodeType, pName);
    return pMatchingNode;
}

void CAttributeGroup::dump(::std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_ATTRIBUTE_GROUP_STR, offset);

    QUICK_OUT(cout, Name,       offset);
    QUICK_OUT(cout, Ref,        offset);
    QUICK_OUT(cout, ID,         offset);
    QUICK_OUT(cout, XSDXPath,   offset);
    QUICK_OUT(cout, EnvXPath,   offset);

    if (m_pAttributeArray != NULL)
        m_pAttributeArray->dump(cout, offset);

    QuickOutFooter(cout, XSD_ATTRIBUTE_GROUP_STR, offset);
}

void CAttributeGroup::getDocumentation(StringBuffer &strDoc) const
{
    if (this->getRef() != NULL && this->getRef()[0] != 0 && m_pRefAttributeGroup != NULL)
    {
        strDoc.appendf("%s%s%s", DM_TITLE_BEGIN, m_pRefAttributeGroup->getName(), DM_TITLE_END);
        DEBUG_MARK_STRDOC;

        if (m_pRefAttributeGroup->getConstAttributeArray() != NULL)
            m_pRefAttributeGroup->getConstAttributeArray()->getDocumentation(strDoc);
    }
}

void CAttributeGroup::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    if (this->getRef() != NULL && this->getRef()[0] != 0 && m_pRefAttributeGroup != NULL)
    {
        if (m_pRefAttributeGroup->getConstAttributeArray() != NULL)
            m_pRefAttributeGroup->getAttributeArray()->populateEnvXPath(strXPath);
    }
    this->setEnvXPath(strXPath);
}

void CAttributeGroup::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert(pEnvTree != NULL);

    if (m_pAttributeArray != NULL)
    {
        try
        {
            m_pAttributeArray->loadXMLFromEnvXml(pEnvTree);
        }
        catch (...)
        {
            // validation check needed here
        }
    }
    if (m_pAnnotation != NULL)
    {
        try
        {
            m_pAnnotation->loadXMLFromEnvXml(pEnvTree);
        }
        catch (...)
        {
            // validation check needed here
        }
    }
    if (m_pRefAttributeGroup != NULL)
    {
        try
        {
            m_pRefAttributeGroup->loadXMLFromEnvXml(pEnvTree);
        }
        catch (...)
        {
            // validation check needed here
        }
    }
}

const char* CAttributeGroup::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0 && m_pAttributeArray != NULL)
    {
        for (int idx = 0; idx < m_pAttributeArray->length(); idx++)
        {
            m_strXML.append("\n").append(m_pAttributeArray->item(idx).getXML(NULL));
        }
    }
    return m_strXML.str();
}

CAttributeGroup* CAttributeGroup::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    if (pSchemaRoot == NULL || xpath == NULL)
        return NULL;

    assert(pParentNode->getNodeType() != XSD_ATTRIBUTE_GROUP);
    CAttributeGroup *pAttributeGroup = new CAttributeGroup(pParentNode);

    pAttributeGroup->setXSDXPath(xpath);
    assert(pAttributeGroup);

    if (pAttributeGroup == NULL)
        return NULL;

    IPropertyTree *pTree = pSchemaRoot->queryPropTree(xpath);
    if (pTree == NULL)
        return NULL;

    pAttributeGroup->setName(pTree->queryProp(XML_ATTR_NAME));
    pAttributeGroup->setID(pTree->queryProp(XML_ATTR_ID));
    pAttributeGroup->setRef(pTree->queryProp(XML_ATTR_REF));

    if (pAttributeGroup->getRef() != NULL && pAttributeGroup->getRef()[0] != 0)
    {
        if (pAttributeGroup->getName() != NULL && pAttributeGroup->getName()[0] != 0)
        {
            assert(false); //can't have both nameand ref set
            return NULL;
        }
        else
            CConfigSchemaHelper::getInstance()->addAttributeGroupToBeProcessed(pAttributeGroup);
    }
    else if (pAttributeGroup->getName() != NULL && pAttributeGroup->getName()[0] != 0)
        CConfigSchemaHelper::getInstance()->getSchemaMapManager()->setAttributeGroupTypeWithName(pAttributeGroup->getName(), pAttributeGroup);

    StringBuffer strXPath(xpath);
    strXPath.append("/").append(XSD_TAG_ATTRIBUTE);

    CAttributeArray *pAttribArray = CAttributeArray::load(pAttributeGroup, pSchemaRoot, strXPath.str());
    if (pAttribArray != NULL)
        pAttributeGroup->setAttributeArray(pAttribArray);

    strXPath.setf("%s/%s",xpath, XSD_TAG_ANNOTATION);
    pAttributeGroup->setAnnotation(CAnnotation::load(pAttributeGroup, pSchemaRoot, strXPath.str()));

    return pAttributeGroup;
}

void CAttributeGroup::getJSON(StringBuffer &strJSON, unsigned int offset, int idx) const
{
    assert(this->getRef() != NULL);

    if (this->getRef() != NULL && this->getRef()[0] != 0 && m_pRefAttributeGroup != NULL)
    {
        if (m_pRefAttributeGroup->getConstAttributeArray() != NULL && m_pRefAttributeGroup->getConstAttributeArray()->length() > 0)
        {
            QuickOutPad(strJSON, offset);
            CONTENT_INNER_CONTENT_BEGIN
            m_pRefAttributeGroup->getConstAttributeArray()->getJSON(strJSON, offset);
        }
    }
}

CAttributeGroupArray::~CAttributeGroupArray()
{
}

CAttributeGroupArray* CAttributeGroupArray::load(const char* pSchemaFile)
{
    assert(false);  // Should never call this?

    if (pSchemaFile == NULL)
        return NULL;

    Linked<IPropertyTree> pSchemaRoot;
    StringBuffer schemaPath;

    schemaPath.appendf("%s%s", DEFAULT_SCHEMA_DIRECTORY, pSchemaFile);
    pSchemaRoot.setown(createPTreeFromXMLFile(schemaPath.str()));

    return CAttributeGroupArray::load(NULL, pSchemaRoot, XSD_TAG_ATTRIBUTE_GROUP);
}

CAttributeGroupArray* CAttributeGroupArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL || xpath == NULL)
        return NULL;

    CAttributeGroupArray *pAttribGroupArray = new CAttributeGroupArray(pParentNode);
    pAttribGroupArray->setXSDXPath(xpath);

    StringBuffer strXPathExt(xpath);

    // to iterate over xs:attributeGroup nodes at same level in tree
    Owned<IPropertyTreeIterator> attribGroupsIter = pSchemaRoot->getElements(strXPathExt.str());

    int count = 1;
    ForEach(*attribGroupsIter)
    {
        strXPathExt.clear().appendf("%s[%d]", xpath, count);

        CAttributeGroup *pAttribGroup = CAttributeGroup::load(pAttribGroupArray, pSchemaRoot, strXPathExt.str());

        if (pAttribGroup != NULL)
            pAttribGroupArray->append(*pAttribGroup);

        count++;
    }

    if (pAttribGroupArray->length() == 0)
    {
        delete pAttribGroupArray;
        return NULL;
    }
    return pAttribGroupArray;
}

void CAttributeGroupArray::dump(::std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout,XSD_ATTRIBUTE_GROUP_ARRAY_STR, offset);

    QUICK_OUT(cout, XSDXPath, offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT_ARRAY(cout, offset);

    QuickOutFooter(cout,XSD_ATTRIBUTE_GROUP_ARRAY_STR, offset);
}

void CAttributeGroupArray::getDocumentation(StringBuffer &strDoc) const
{
    StringBuffer strDocDupe1(strDoc);

    QUICK_DOC_ARRAY(strDocDupe1);

    if (strDocDupe1.length() == strDoc.length())  // hack
       return;

    for (int idx=0; idx < this->length(); idx++)
    {
        strDoc.append(DM_SECT3_BEGIN);
        (this->item(idx)).getDocumentation(strDoc);
        strDoc.append(DM_SECT3_END);
    }
}

void CAttributeGroupArray::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    assert(index == 1);  // Only 1 array of elements per node

    for (int idx=0; idx < this->length(); idx++)
    {
        (this->item(idx)).populateEnvXPath(strXPath, 1);
    }
    this->setEnvXPath(strXPath);
}

void CAttributeGroupArray::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    QUICK_LOAD_ENV_XML(pEnvTree)
}

void CAttributeGroupArray::getJSON(StringBuffer &strJSON, unsigned int offset, int idx) const
{
    int nLength = this->length();

    offset += STANDARD_OFFSET_1;
    QuickOutPad(strJSON, offset);

    for (int lidx = 0; lidx < nLength; lidx++)
    {
        if (lidx != 0)
        {
            if (lidx != 0)
            {
                QuickOutPad(strJSON, offset);
                strJSON.append(",");
            }
        }

        strJSON.append("{");
        CJSONMarkUpHelper::createUIContent(strJSON, offset, JSON_TYPE_TAB, this->item(lidx).getRef(), this->getEnvXPath());

        if (lidx != 0)
        {
            strJSON.append("\n");
        }

        this->item(lidx).getJSON(strJSON, offset, lidx);
        strJSON.append("]}}\n");

        if (lidx+1 < nLength)
        {
            QuickOutPad(strJSON, offset);
        }
    }
}
