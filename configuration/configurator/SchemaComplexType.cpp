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

#include <cassert>
#include "jstring.hpp"
#include "jptree.hpp"
#include "XMLTags.h"
#include "SchemaComplexType.hpp"
#include "SchemaSequence.hpp"
#include "SchemaComplexContent.hpp"
#include "SchemaAttributes.hpp"
#include "SchemaChoice.hpp"
#include "SchemaComplexType.hpp"
#include "SchemaSchema.hpp"
#include "ConfigSchemaHelper.hpp"
#include "DocumentationMarkup.hpp"
#include "ExceptionStrings.hpp"
#include "SchemaMapManager.hpp"
#include "JSONMarkUp.hpp"

using namespace CONFIGURATOR;

#define StringBuffer ::StringBuffer
#define IPropertyTree ::IPropertyTree

const CXSDNodeBase* CComplexType::getNodeByTypeAndNameAscending(NODE_TYPES eNodeType, const char *pName) const
{
    const CXSDNodeBase* pMatchingNode = NULL;

    if (eNodeType == this->getNodeType() && (pName != NULL ? !strcmp(pName, this->getNodeTypeStr()) : true))
        return this;

    if (m_pSequence != NULL)
        pMatchingNode =  m_pSequence->getNodeByTypeAndNameAscending(eNodeType, pName);

    return pMatchingNode;
}

const CXSDNodeBase* CComplexType::getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName) const
{
    const CXSDNodeBase* pMatchingNode = NULL;

    if (eNodeType == this->getNodeType() && (pName != NULL ? !strcmp(pName, this->getNodeTypeStr()) : true))
        return this;

    if (m_pSequence != NULL)
        pMatchingNode = m_pSequence->getNodeByTypeAndNameDescending(eNodeType, pName);
    if (pMatchingNode == NULL && m_pSequence != NULL)
        pMatchingNode = m_pComplexContent->getNodeByTypeAndNameDescending(eNodeType, pName);
    if (pMatchingNode == NULL && m_pAttributeArray != NULL)
        pMatchingNode = m_pSequence->getNodeByTypeAndNameDescending(eNodeType, pName);
    if (pMatchingNode == NULL && m_pChoice != NULL)
        pMatchingNode = m_pSequence->getNodeByTypeAndNameDescending(eNodeType, pName);

    return pMatchingNode;
}

void CComplexType::dump(::std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;
    QuickOutHeader(cout, XSD_COMPLEX_TYPE_STR, offset);

    QUICK_OUT_2(Name)
    QUICK_OUT(cout, XSDXPath,   offset);

    if (m_pSequence != NULL)
        m_pSequence->dump(cout, offset);
    if (m_pComplexContent != NULL)
        m_pComplexContent->dump(cout, offset);
    if (m_pAttributeArray != NULL)
        m_pAttributeArray->dump(cout, offset);
    if (m_pChoice != NULL)
        m_pChoice->dump(cout, offset);
    if (m_pAttributeGroupArray != NULL)
        m_pAttributeGroupArray->dump(cout, offset);
    if (m_pAnnotation != NULL)
        m_pAnnotation->dump(cout, offset);

    QuickOutFooter(cout, XSD_COMPLEX_TYPE_STR, offset);
}

void CComplexType::getDocumentation(StringBuffer &strDoc) const
{
    if (m_pSequence != NULL)
        m_pSequence->getDocumentation(strDoc);

    if (m_pComplexContent != NULL)
    {
        strDoc.append(DM_SECT3_BEGIN);
        DEBUG_MARK_STRDOC
        m_pComplexContent->getDocumentation(strDoc);
        strDoc.append(DM_SECT3_END);
    }

    if (m_pAttributeArray != NULL)
    {
        if (this->getConstParentNode()->getConstParentNode()->getNodeType() == XSD_SCHEMA)
            strDoc.appendf("<%s>\n", DM_TABLE_ROW);

        m_pAttributeArray->getDocumentation(strDoc);

        if (this->getConstParentNode()->getConstParentNode()->getNodeType() == XSD_SCHEMA)
            strDoc.appendf("</%s>\n", DM_TABLE_ROW);
    }

    if (m_pChoice != NULL)
        m_pChoice->getDocumentation(strDoc);
    if (m_pAttributeGroupArray != NULL)
        m_pAttributeGroupArray->getDocumentation(strDoc);
}

void CComplexType::getJSON(StringBuffer &strJSON, unsigned int offset, int idx) const
{
	bool bAddComma = false;

    if ((m_pAttributeArray != NULL && m_pAttributeArray->length() > 0) || m_pSequence != NULL || m_pAttributeGroupArray != NULL)
	{
        strJSON.append("\n");
        offset += STANDARD_OFFSET_2;
        QuickOutPad(strJSON, offset);
        CONTENT_INNER_CONTENT_BEGIN

        //bAddComma = true;
	}

    if (m_pAttributeArray != NULL && m_pAttributeArray->length() > 0)
    {
        QuickOutPad(strJSON, offset);
        //if (bAddComma)
        {
           strJSON.append("{");
        }

        const CElement *pElement = dynamic_cast<const CElement*>(this->getParentNodeByType(XSD_ELEMENT));
        assert(pElement != NULL);

        if (pElement && (pElement->getMaxOccursInt() > 0 || strlen(pElement->getMinOccurs()) > 0))
        {
            CJSONMarkUpHelper::createUIContent(strJSON, offset, JSON_TYPE_TABLE, "Attributes", this->getEnvXPath());
        }
        else if (m_pAttributeArray != NULL && m_pAttributeArray->length() > 0)
        {
            CJSONMarkUpHelper::createUIContent(strJSON, offset, JSON_TYPE_TAB, "Attributes", this->getEnvXPath());
        }

        CONTENT_INNER_CONTENT_BEGIN

        m_pAttributeArray->getJSON(strJSON, offset);
        bAddComma = true;

        INNER_CONTENT_END

        strJSON.append("}");

        if (bAddComma)
        {
            strJSON.append("}");
        }

        bAddComma = true;
    }
    if (m_pSequence != NULL)
    {
        if (bAddComma)
        {
            strJSON.append(",");
        }

        m_pSequence->getJSON(strJSON, offset);
        bAddComma = true;
    }

    if (m_pChoice != NULL)
    {
        if (bAddComma)
        {
            strJSON.append(",");
        }

        m_pChoice->getJSON(strJSON, offset);
        //bAddComma = true;
    }

    if(m_pAttributeGroupArray != NULL && m_pAttributeGroupArray->length() > 0)
    {
        if (bAddComma)
        {
            strJSON.append(",");
        }
        m_pAttributeGroupArray->getJSON(strJSON, offset);
    }
    INNER_CONTENT_END
    CONTENT_CONTENT_END
}

void CComplexType::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    this->setEnvXPath(strXPath);

    if (m_pSequence != NULL)
        m_pSequence->populateEnvXPath(strXPath, index);
    if (m_pComplexContent != NULL)
        m_pComplexContent->populateEnvXPath(strXPath, index);
    if (m_pAttributeArray != NULL)
        m_pAttributeArray->populateEnvXPath(strXPath, index);
    if (m_pChoice != NULL)
        m_pChoice->populateEnvXPath(strXPath, index);
    if (m_pAttributeGroupArray != NULL)
        m_pAttributeGroupArray->populateEnvXPath(strXPath, index);
}

void CComplexType::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    if (m_pSequence != NULL)
        m_pSequence->loadXMLFromEnvXml(pEnvTree);
    if (m_pComplexContent != NULL)
        m_pComplexContent->loadXMLFromEnvXml(pEnvTree);
    if (m_pAttributeArray != NULL)
        m_pAttributeArray->loadXMLFromEnvXml(pEnvTree);
    if (m_pChoice != NULL)
        m_pChoice->loadXMLFromEnvXml(pEnvTree);
    if (m_pAttributeGroupArray != NULL)
        m_pAttributeGroupArray->loadXMLFromEnvXml(pEnvTree);
}

const char* CComplexType::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0)
    {
        if (m_pComplexContent != NULL)
            m_strXML.append(m_pComplexContent->getXML(NULL));
        if (m_pAttributeArray != NULL)
            m_strXML.append(m_pAttributeArray->getXML(NULL));
        if (m_pChoice != NULL)
            m_strXML.append(m_pChoice->getXML(NULL));
    }
    return m_strXML.str();
}

bool CComplexType::hasChildElements() const
{
    if (this->m_pSequence != NULL)
        return m_pSequence->hasChildElements();

    return false;
}

CComplexType* CComplexType::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    CComplexContent *pComplexContent = NULL;
    CAttributeArray *pAttributeArray =  NULL;
    CChoice *pChoice = NULL;
    CSequence *pSequence  = NULL;
    CAttributeGroupArray *pAttributeGroupArray = NULL;
    CAnnotation *pAnnotation = NULL;

    if (pSchemaRoot == NULL)
        return NULL;

    IPropertyTree *pTree = pSchemaRoot->queryPropTree(xpath);
    const char* pName = pTree->queryProp(XML_ATTR_NAME);
    StringBuffer strXPathExt(xpath);

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_SEQUENCE);
    pSequence = CSequence::load(NULL, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_ANNOTATION);
    pAnnotation = CAnnotation::load(NULL, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_COMPLEX_CONTENT);
    pComplexContent = CComplexContent::load(NULL, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_ATTRIBUTE);
    pAttributeArray = CAttributeArray::load(NULL, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_CHOICE);
    pChoice = CChoice::load(NULL, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_ATTRIBUTE_GROUP);
    pAttributeGroupArray = CAttributeGroupArray::load(NULL, pSchemaRoot, strXPathExt.str());

    CComplexType *pComplexType = new CComplexType(pParentNode, pName, pSequence, pComplexContent, pAttributeArray, pChoice, pAttributeGroupArray, pAnnotation);
    pComplexType->setXSDXPath(xpath);

    assert(pComplexType != NULL);

    if (pComplexType != NULL)
    {
        SETPARENTNODE(pSequence, pComplexType)
        SETPARENTNODE(pComplexContent, pComplexType)
        SETPARENTNODE(pAttributeArray, pComplexType)
        SETPARENTNODE(pChoice, pComplexType)
        SETPARENTNODE(pAttributeGroupArray, pComplexType)
        SETPARENTNODE(pAnnotation, pComplexType);

        if (pName != NULL)
            CConfigSchemaHelper::getInstance()->getSchemaMapManager()->setComplexTypeWithName(pName, pComplexType);
    }
    return pComplexType;
}

void CComplexTypeArray::dump(::std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_COMPLEX_TYPE_ARRAY_STR, offset);

    QUICK_OUT(cout, XSDXPath,   offset);
    QUICK_OUT_ARRAY(cout, offset);

    QuickOutFooter(cout, XSD_COMPLEX_TYPE_ARRAY_STR, offset);
}

void CComplexTypeArray::getDocumentation(StringBuffer &strDoc) const
{
    QUICK_DOC_ARRAY(strDoc);
}

void CComplexTypeArray::getJSON(StringBuffer &strJSON, unsigned int offset, int idx) const
{
    for (int idx=0; idx < this->length(); idx++)
    {
        (this->item(idx)).getJSON(strJSON, offset);
    }
}

const char* CComplexTypeArray::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0)
    {
        int length = this->length();

        for (int idx = 0; idx < length; idx++)
        {
            CComplexType &ComplexType = this->item(idx);

            m_strXML.append(ComplexType.getXML(NULL));

            if (idx+1 < length)
                m_strXML.append("\n");
        }
    }
    return m_strXML.str();
}

void CComplexTypeArray::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    this->setEnvXPath(strXPath);
    QUICK_ENV_XPATH_WITH_INDEX(strXPath, 1); // can only have 1 complex type array
}

void CComplexTypeArray::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert(pEnvTree != NULL);
    if (pEnvTree->hasProp(this->getEnvXPath()) == false)
        throw MakeExceptionFromMap(EX_STR_XPATH_DOES_NOT_EXIST_IN_TREE);
    else
        QUICK_LOAD_ENV_XML(pEnvTree);
}

CComplexTypeArray* CComplexTypeArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL)
        return NULL;

    CComplexTypeArray *pComplexTypeArray = new CComplexTypeArray(pParentNode);

    pComplexTypeArray->setXSDXPath(xpath);

    Owned<IPropertyTreeIterator> complexTypeIter = pSchemaRoot->getElements(xpath);

    int count = 1;

    ForEach(*complexTypeIter)
    {
        StringBuffer strXPathExt(xpath);
        strXPathExt.appendf("[%d]", count);

        CComplexType *pComplexType = CComplexType::load(pComplexTypeArray, pSchemaRoot, strXPathExt.str());

        assert(pComplexType != NULL);

        if (pComplexType != NULL)
            pComplexTypeArray->append(*pComplexType);

        count++;
    }

    if (pComplexTypeArray->length() == 0)
    {
        delete pComplexTypeArray;
        pComplexTypeArray = NULL;
    }

    return pComplexTypeArray;
}

CComplexTypeArray* CComplexTypeArray::load(CXSDNodeBase* pParentNode, const char* pSchemaFile)
{
    assert(false);  // why do still need to call this?
    assert(pSchemaFile != NULL);

    if (pSchemaFile == NULL)
        return NULL;

    if (pParentNode == NULL)
    {
        Linked<IPropertyTree> pSchemaRoot;

        StringBuffer schemaPath;

        schemaPath.appendf("%s%s", DEFAULT_SCHEMA_DIRECTORY, pSchemaFile);
        pSchemaRoot.setown(createPTreeFromXMLFile(schemaPath.str()));

        return CComplexTypeArray::load(pParentNode, pSchemaRoot, XSD_TAG_COMPLEX_TYPE);
    }
    else
    {
        CSchema *pSchema = (dynamic_cast<CSchema*>(pParentNode));

        if (pSchema != NULL)
            return pSchema->getComplexTypeArray();
    }
    return NULL;
}
