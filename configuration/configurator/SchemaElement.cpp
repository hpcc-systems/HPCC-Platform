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
#include "jptree.hpp"
#include "jstring.hpp"
#include "jarray.hpp"
#include "jhash.hpp"
#include "XMLTags.h"


//#define ::StringBuffer ::::StringBuffer
//#define ::IPropertyTree ::::IPropertyTree

#include "SchemaAnnotation.hpp"
#include "SchemaCommon.hpp"
#include "SchemaElement.hpp"
#include "SchemaComplexType.hpp"
#include "SchemaElement.hpp"
#include "SchemaAttributes.hpp"
#include "SchemaAppInfo.hpp"
#include "SchemaDocumentation.hpp"
#include "DocumentationMarkup.hpp"
#include "ConfigSchemaHelper.hpp"
#include "ConfigSchemaHelper.hpp"
#include "SchemaMapManager.hpp"
#include "ConfiguratorMain.hpp"
#include "JSONMarkUp.hpp"

using namespace CONFIGURATOR;

const CXSDNodeBase* CElement::getNodeByTypeAndNameAscending(NODE_TYPES eNodeType, const char *pName) const
{
    const CXSDNodeBase* pMatchingNode = NULL;

    if (eNodeType == this->getNodeType() && (pName != NULL ? !strcmp(pName, this->getNodeTypeStr()) : true))
        return this;

    if (eNodeType == XSD_ELEMENT)
        pMatchingNode = (dynamic_cast<CElement*>(this->getParentNode()))->getNodeByTypeAndNameAscending(XSD_ELEMENT, pName);
    if (pMatchingNode == NULL)
        pMatchingNode = (dynamic_cast<CElementArray*>(this->getParentNode()))->getNodeByTypeAndNameAscending(eNodeType, pName);

    return pMatchingNode;
}

const CXSDNodeBase* CElement::getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName) const
{
    const CXSDNodeBase* pMatchingNode = NULL;

    if (eNodeType == this->getNodeType() && (pName != NULL ? !strcmp(pName, this->getNodeTypeStr()) : true))
        return this;
    if (m_pComplexTypeArray != NULL)
        pMatchingNode = m_pComplexTypeArray->getNodeByTypeAndNameDescending(eNodeType, pName);

    return pMatchingNode;
}

CElement* CElement::load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath, bool bIsInXSD)
{
    assert(pSchemaRoot != NULL);
    assert(pParentNode != NULL);

    if (pSchemaRoot == NULL || pParentNode == NULL)
        return NULL;

    CElement *pElement = new CElement(pParentNode);

    pElement->setIsInXSD(bIsInXSD);
    pElement->setXSDXPath(xpath);

    ::IPropertyTree *pTree = pSchemaRoot->queryPropTree(xpath);

    if (pElement != NULL && pTree != NULL)
        pElement->setName(pTree->queryProp(XML_ATTR_NAME));

    Owned<IAttributeIterator> iterAttrib = pTree->getAttributes(true);

    ForEach(*iterAttrib)
    {
        if (strcmp(iterAttrib->queryName(), XML_ATTR_NAME) == 0)
        {
            const char *pName = iterAttrib->queryValue();

            if (pName != NULL && *pName != 0)
                pElement->setName(pName);
        }
        else if (strcmp(iterAttrib->queryName(), XML_ATTR_MAXOCCURS) == 0)
            pElement->setMaxOccurs(iterAttrib->queryValue());
        else if (strcmp(iterAttrib->queryName(), XML_ATTR_MINOCCURS) == 0)
            pElement->setMinOccurs(iterAttrib->queryValue());
        else if (strcmp(iterAttrib->queryName(), XML_ATTR_TYPE) == 0)
        {
            const char *pType = iterAttrib->queryValue();

            assert(pType != NULL && *pType != 0);

            if (pType != NULL && *pType != 0)
            {
                pElement->setType(pType);
                CConfigSchemaHelper::getInstance()->addNodeForTypeProcessing(pElement);
            }
        }
        else if (strcmp(iterAttrib->queryName(), XML_ATTR_REF) == 0)
        {
            const char *pRef = iterAttrib->queryValue();

            assert (pRef != NULL && *pRef != 0 && pElement->getConstAncestorNode(3)->getNodeType() != XSD_SCHEMA);

            if (pRef != NULL && *pRef != 0 && pElement->getConstAncestorNode(3)->getNodeType() != XSD_SCHEMA)
            {
                pElement->setRef(pRef);
                CConfigSchemaHelper::getInstance()->addElementForRefProcessing(pElement);
            }
            else
            {
                // TODO:  throw exception
            }
        }
        else if (strcmp(iterAttrib->queryName(), XML_ATTR_DEFAULT) == 0)
        {
            const char *pDefault = iterAttrib->queryValue();

            assert(pDefault != NULL);
            assert(strlen(pElement->getDefault()) == 0);
            pElement->setDefault(pDefault);
        }
        assert(iterAttrib->queryValue() != NULL);
    }
    assert(strlen(pElement->getName()) > 0);

    if (strlen(pElement->getRef()) != 0)
    {
        assert(pElement->getComplexTypeArray()->length() == 0);
        assert(pElement->getSimpleType() == NULL);
        assert(strlen(pElement->getDefault()) == 0);
        assert(strlen(pElement->getType()) == 0);
        /*assert(pElement->getKey() == NULL);
        assert(pElement->getKeyRef() == NULL);
        assert(pElement->getUnique() == NULL);*/
    }

    ::StringBuffer strXPathExt(xpath);

    strXPathExt.append("/").append(XSD_TAG_ANNOTATION);
    pElement->m_pAnnotation = CAnnotation::load(pElement, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_COMPLEX_TYPE);
    pElement->m_pComplexTypeArray = CComplexTypeArray::load(pElement, pSchemaRoot, strXPathExt.str());

    if (pElement->m_pAnnotation != NULL && pElement->m_pAnnotation->getAppInfo() != NULL && strlen(pElement->m_pAnnotation->getAppInfo()->getTitle()) > 0)
    {
        /****  MUST FIX TO HAVE CORRECT UI TAB LABELS (but getName is expected to return the XPATH name *****/
        //pElement->setName(pElement->m_pAnnotation->getAppInfo()->getTitle());
        pElement->setTitle(pElement->m_pAnnotation->getAppInfo()->getTitle());
    }
    else
        pElement->setTitle(pElement->getName());

/*    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_KEY);
    pElement->m_pKeyArray = CKeyArray::load(pElement, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_KEYREF);
    pElement->m_pKeyRefArray = CKeyRefArray::load(pElement, pSchemaRoot, strXPathExt.str());
*/
    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_SIMPLE_TYPE);
    pElement->m_pSimpleType = CSimpleType::load(pElement, pSchemaRoot, strXPathExt.str());

    SETPARENTNODE(pElement, pParentNode);

    return pElement;
}

const CElement* CElement::getTopMostElement(const CXSDNodeBase *pNode)
{
    if (pNode == NULL)
        return NULL;
    else if (pNode->getNodeType() == XSD_ELEMENT)
    {
        if (pNode->getParentNodeByType(XSD_ELEMENT) == NULL)
        {
            assert(dynamic_cast<const CElement*>(pNode) != NULL);
            return dynamic_cast<const CElement*>(pNode);
        }
    }
    return getTopMostElement(pNode->getParentNodeByType(XSD_ELEMENT));
}

const CSchema* CElement::getConstSchemaNode() const
{
    const CSchema *pSchema = dynamic_cast<const CSchema*>(CElement::getTopMostElement(this)->getParentNodeByType(XSD_SCHEMA));
    return pSchema;
}

const char* CElement::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length () == 0)
    {
        m_strXML.append("\n<").append(getName()).append(" ");

        if (m_pAnnotation != NULL)
            m_strXML.append(m_pAnnotation->getXML(NULL));
        if (m_pComplexTypeArray != NULL)
            m_strXML.append(m_pComplexTypeArray->getXML(NULL));
        if (m_pKeyArray != NULL)
            m_strXML.append(m_pKeyArray->getXML(NULL));
        if (m_pKeyRefArray != NULL)
            m_strXML.append(m_pKeyRefArray->getXML(NULL));
    }
    return m_strXML.str();
}

void CElement::dump(::std::ostream &cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_ELEMENT_STR, offset);
    QUICK_OUT(cout, Name, offset);
    QUICK_OUT(cout, Type, offset);
    QUICK_OUT(cout, MinOccurs, offset);
    QUICK_OUT(cout, MaxOccurs, offset);
    QUICK_OUT(cout, Title,  offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT(cout, EnvValueFromXML,  offset);
    QUICK_OUT(cout, Ref, offset);

    if (this->getTypeNode() != NULL)
        this->getTypeNode()->dump(cout, offset);
    if (m_pAnnotation != NULL)
        m_pAnnotation->dump(cout, offset);
    if (m_pComplexTypeArray != NULL)
        m_pComplexTypeArray->dump(cout, offset);
    if (m_pKeyArray != NULL)
        m_pKeyArray->dump(cout, offset);
    if (m_pKeyRefArray != NULL)
        m_pKeyRefArray->dump(cout, offset);
    if (m_pSimpleType != NULL)
        m_pSimpleType->dump(cout, offset);

    if (this->getRef() != NULL)
    {
        CElement *pElement = CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getElementWithName(this->getRef());

        assert(pElement != NULL);
        if (pElement != NULL)
            pElement->dump(cout, offset);
    }
    QuickOutFooter(cout, XSD_ELEMENT_STR, offset);
}

void CElement::getDocumentation(::StringBuffer &strDoc) const
{
    const CXSDNodeBase *pGrandParentNode = this->getConstParentNode()->getConstParentNode();

    assert(pGrandParentNode != NULL);
    if (pGrandParentNode == NULL)
        return;

    if (m_pAnnotation != NULL && m_pAnnotation->getAppInfo() != NULL && m_pAnnotation->getAppInfo()->getViewType() != NULL && stricmp(m_pAnnotation->getAppInfo()->getViewType(), "none") == 0)
        return;

    if (this->getName() != NULL && (stricmp(this->getName(), "Instance") == 0 || stricmp(this->getName(), "Note") == 0 || stricmp(this->getName(), "Notes") == 0 ||  stricmp(this->getName(), "Topology") == 0 ))
        return; // don't document instance

    assert(strlen(this->getName()) > 0);

    if (pGrandParentNode->getNodeType() == XSD_SCHEMA)
    {
        ::StringBuffer strName(this->getName());

        strName.replace(' ', '_');
        strDoc.append(DM_HEADING);

        // component name would be here
        strDoc.appendf("<%s %s=\"%s%s\">\n", DM_SECT2, DM_ID, strName.str(),"_mod");
        strDoc.appendf("<%s>%s</%s>\n", DM_TITLE_LITERAL, this->getName(), DM_TITLE_LITERAL);

        if (m_pAnnotation!= NULL)
        {
            m_pAnnotation->getDocumentation(strDoc);
            DEBUG_MARK_STRDOC;
        }

        strDoc.append(DM_SECT3_BEGIN);
        DEBUG_MARK_STRDOC;
        strDoc.append(DM_TITLE_BEGIN).append(DM_TITLE_END);

        if (m_pComplexTypeArray != NULL)
            m_pComplexTypeArray->getDocumentation(strDoc);

        strDoc.append(DM_SECT3_END);
        return;
    }
    else if (m_pComplexTypeArray != NULL)
    {
        if (m_pAnnotation!= NULL)
        {
            m_pAnnotation->getDocumentation(strDoc);
            DEBUG_MARK_STRDOC;
        }

        if (pGrandParentNode->getNodeType() == XSD_CHOICE)
            strDoc.appendf("%s%s%s", DM_PARA_BEGIN, this->getTitle(), DM_PARA_END);
        else
            strDoc.appendf("%s%s%s", DM_TITLE_BEGIN, this->getTitle(), DM_TITLE_END);

        DEBUG_MARK_STRDOC;
        m_pComplexTypeArray->getDocumentation(strDoc);
    }
    else if (m_pComplexTypeArray == NULL)
    {
        if (m_pAnnotation!= NULL)
        {
            m_pAnnotation->getDocumentation(strDoc);
            DEBUG_MARK_STRDOC;
        }

        strDoc.appendf("%s%s%s", DM_PARA_BEGIN, this->getName(), DM_PARA_END);
        DEBUG_MARK_STRDOC;

        if (m_pAnnotation != NULL && m_pAnnotation->getDocumentation() != NULL)
        {
            m_pAnnotation->getDocumentation(strDoc);
            DEBUG_MARK_STRDOC;
        }
    }
}

void CElement::getJSON(::StringBuffer &strJSON, unsigned int offset, int idx) const
{
    assert(this->getConstAncestorNode(3) != NULL);

//    if (m_pAnnotation != NULL && m_pAnnotation->getAppInfo() != NULL && m_pAnnotation->getAppInfo()->getViewType() != NULL && stricmp(m_pAnnotation->getAppInfo()->getViewType(), "none") == 0)
//        return;

    assert(strlen(this->getName()) > 0);

    StringBuffer strXPath(this->getEnvXPath());

    stripTrailingIndex(strXPath);

    if (m_pComplexTypeArray != NULL  && m_pComplexTypeArray->length() > 0)
    {
        //CJSONMarkUpHelper::createUIContent(strJSON, offset, (this->getMaxOccursInt() > 1 || strlen(this->getMinOccurs()) > 0) ? JSON_TYPE_TABLE : JSON_TYPE_TAB, this->getTitle(), this->getEnvXPath());

        //CJSONMarkUpHelper::createUIContent(strJSON, offset, (this->getMaxOccursInt() > 1 || strlen(this->getMinOccurs()) > 0) ? JSON_TYPE_TABLE : JSON_TYPE_TAB, this->getTitle(), strXPath.str());
        CJSONMarkUpHelper::createUIContent(strJSON, offset, JSON_TYPE_TAB, this->getTitle(), strXPath.str());

        m_pComplexTypeArray->getJSON(strJSON, offset);
    }
    else
    {
        //CJSONMarkUpHelper::createUIContent(strJSON, offset, JSON_TYPE_TABLE, this->getTitle(), this->getEnvXPath());
        CJSONMarkUpHelper::createUIContent(strJSON, offset, JSON_TYPE_TABLE, this->getTitle(), strXPath.str());
        // add values
        //CJSONMarkUpHelper::createUIContent(strJSON, offset, JSON_TYPE_INPUT, this->getTitle(),  strXPath.str());
    }
}


bool CElement::isATab() const
{
    const CComplexTypeArray *pComplexTypArray = this->getComplexTypeArray();
    const CAttributeGroupArray *pAttributeGroupArray = (pComplexTypArray != NULL && pComplexTypArray->length() > 0) ? pComplexTypArray->item(0).getAttributeGroupArray() : NULL;
    const CAttributeArray *pAttributeArray = (pComplexTypArray != NULL && pComplexTypArray->length() > 0) ? pComplexTypArray->item(0).getAttributeArray() : NULL;

    if (this->getConstAncestorNode(3)->getNodeType() != XSD_SCHEMA && \
            (this->hasChildElements() == true || \
             (this->hasChildElements() == false && (static_cast<const CElementArray*>(this->getConstParentNode()))->anyElementsHaveMaxOccursGreaterThanOne() == false)/* || \
             (this->isTopLevelElement() == true && (pAttributeGroupArray != NULL || pAttributeArray != NULL))*/))

    {
        return true;
    }
    else
    {
        return false;
    }
/*    if (stricmp(this->getMaxOccurs(), TAG_UNBOUNDED) == 0)
    {
        return false;
    }
    // Any element that is in sequence of complex type will be a tab
    else*/ if (this->getConstAncestorNode(3)->getNodeType() == XSD_SEQUENCE && this->getConstAncestorNode(3)->getNodeType() == XSD_COMPLEX_TYPE)
    {
        return true;
    }
    else if (/*this->getConstAncestorNode(3)->getNodeType == XSD_COMPLEX_TYPE &&*/ this->getConstAncestorNode(3)->getNodeType() == XSD_ELEMENT)
    {
        const CElement *pElement = dynamic_cast<const CElement*>(this->getConstAncestorNode(3));

        assert(pElement != NULL);
        if (pElement != NULL)
            return pElement->isATab();
    }
    return false;
}

bool CElement::isLastTab(const int idx) const
{
    assert(this->isATab() == true);

    const CElementArray *pElementArray = dynamic_cast<const CElementArray*>(this->getConstParentNode());

    if (pElementArray == NULL)
    {
        assert(!"Corrupt XSD??");
        return false;
    }
    if (pElementArray->length()-1 == idx)
        return true;

    return false;
}


void CElement::populateEnvXPath(::StringBuffer strXPath, unsigned int index)
{
    assert(strXPath.length() > 0);

    //strXPath.append("/").append(this->getName()).append("[").append(index).append("]");
    strXPath.append("[").append(index).append("]");

    //PROGLOG("Setting element to envpath of %s, previous path: %s", strXPath.str(), this->getEnvXPath());
    this->setEnvXPath(strXPath);

    CConfigSchemaHelper::getInstance()->getSchemaMapManager()->addMapOfXPathToElement(this->getEnvXPath(), this);

    if (m_pComplexTypeArray != NULL)
        m_pComplexTypeArray->populateEnvXPath(strXPath, index);
    if (m_pSimpleType != NULL)
        m_pSimpleType->populateEnvXPath(strXPath, index);
    if (m_pKeyArray != NULL)
        m_pKeyArray->populateEnvXPath(strXPath, index);
}

void CElement::loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree)
{
    //PROGLOG("Mapping element with XPATH of %s to %p", this->getEnvXPath(), this);
    //CConfigSchemaHelper::getInstance()->getSchemaMapManager()->addMapOfXPathToElement(this->getEnvXPath(), this);

    if (m_pComplexTypeArray != NULL)
    {
        try
        {
            m_pComplexTypeArray->loadXMLFromEnvXml(pEnvTree);
        }
        catch (...)
        {
            // node described in XSD doesn't exist in XML
            // time to do validation?
        }
    }
    if (m_pSimpleType != NULL)
    {
        try
        {
            m_pSimpleType->loadXMLFromEnvXml(pEnvTree);
        }
        catch(...)
        {
        }
    }

    if (m_pComplexTypeArray == NULL)
    {
        const char* pValue =  pEnvTree->queryPropTree(this->getEnvXPath())->queryProp("");

        if (pValue != NULL)
        {
            this->setEnvValueFromXML(pValue);
            CConfigSchemaHelper::getInstance()->appendElementXPath(this->getEnvXPath());
        }
    }

    const char* pInstanceName =  pEnvTree->queryPropTree(this->getEnvXPath())->queryProp(XML_ATTR_NAME);

    if (pInstanceName != NULL && *pInstanceName != 0)
        this->setInstanceName(pInstanceName);
}

bool CElement::isTopLevelElement() const
{
    return m_bTopLevelElement;
}

const char * CElement::getViewType() const
{
    if(m_pAnnotation != NULL && m_pAnnotation->getAppInfo() != NULL)
        return m_pAnnotation->getAppInfo()->getViewType();
    return NULL;
}

void CElementArray::dump(::std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_ELEMENT_ARRAY_STR, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT_ARRAY(cout, offset);
    QuickOutFooter(cout, XSD_ELEMENT_ARRAY_STR, offset);
}

void CElementArray::getDocumentation(::StringBuffer &strDoc) const
{
    QUICK_DOC_ARRAY(strDoc);
}

void CElementArray::getJSON(::StringBuffer &strJSON, unsigned int offset, int idx) const
{
    offset += STANDARD_OFFSET_2;
    QuickOutPad(strJSON, offset);

    int lidx = (idx == -1 ? 0 : idx);

    strJSON.append("{");

    (this->item(lidx)).getJSON(strJSON, offset+STANDARD_OFFSET_2, lidx);

    offset += STANDARD_OFFSET_2;
    QuickOutPad(strJSON, offset);
    strJSON.append("}");
}

void CElementArray::populateEnvXPath(::StringBuffer strXPath, unsigned int index)
{
    strXPath.appendf("/%s", this->item(0).getName());
    this->setEnvXPath(strXPath);

    for (int idx=0; idx < this->length(); idx++)
    {
        //strXPath.appendf("[%d]", idx+1);
        this->item(idx).populateEnvXPath(strXPath.str(), idx+1);
        CConfigSchemaHelper::stripXPathIndex(strXPath);
    }
}

const char* CElementArray::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0)
    {
        int length = this->length();

        for (int idx = 0; idx < length; idx++)
        {
            CElement &Element = this->item(idx);
            m_strXML.append(Element.getXML(NULL));

            if (idx+1 < length)
                m_strXML.append("\n");
        }
    }
    return m_strXML.str();
}

void CElementArray::loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree)
{
    ::StringBuffer strEnvXPath(this->getEnvXPath());
    int subIndex = 1;

    do
    {
        CElement *pElement = NULL;

        strEnvXPath.appendf("[%d]", subIndex);
        if (pEnvTree->hasProp(strEnvXPath.str()) == false)
             return;

        if (subIndex == 1)
        {
            pElement =  CConfigSchemaHelper::getInstance()->getSchemaMapManager()->getElementFromXPath(strEnvXPath.str());
        }
        else
        {
            pElement = CElement::load(this, this->getSchemaRoot(), this->getXSDXPath(), false);
            this->append(*pElement);
        }

        assert(pElement);

        if (subIndex > 1)
        {
            pElement->populateEnvXPath(this->getEnvXPath(), subIndex);
        }

        pElement->setTopLevelElement(false);
        pElement->loadXMLFromEnvXml(pEnvTree);

        CConfigSchemaHelper::stripXPathIndex(strEnvXPath);

        subIndex++;
    } while (true);
}

CElementArray* CElementArray::load(const char* pSchemaFile)
{
    assert(pSchemaFile != NULL);
    if (pSchemaFile == NULL)
        return NULL;

    typedef ::IPropertyTree jlibIPropertyTree;
    ::Linked<jlibIPropertyTree> pSchemaRoot;
    ::StringBuffer schemaPath;

    schemaPath.appendf("%s%s", DEFAULT_SCHEMA_DIRECTORY, pSchemaFile);
    pSchemaRoot.setown(createPTreeFromXMLFile(schemaPath.str()));

    CElementArray *pElemArray = CElementArray::load(NULL, pSchemaRoot, XSD_TAG_ELEMENT);

    //PROGLOG("Function: %s() at %s:%d", __func__, __FILE__, __LINE__);
    //PROGLOG("pElemArray = %p", pElemArray);

    return pElemArray;
}

CElementArray* CElementArray::load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    if (pSchemaRoot == NULL)
        return NULL;

    CElementArray *pElemArray = new CElementArray(pParentNode);
    assert(pElemArray != NULL);

    pSchemaRoot->Link();
    pElemArray->setSchemaRoot(pSchemaRoot);

    ::StringBuffer strXPathExt(xpath);
    pElemArray->setXSDXPath(xpath);

    CElement *pElem = CElement::load(pElemArray, pSchemaRoot, strXPathExt.str());

    assert(pElem);
    pElemArray->append(*pElem);

    SETPARENTNODE(pElemArray, pParentNode);

    return pElemArray;


/*    typedef ::IPropertyTreeIterator jlibIPropertyTreeIterator;
    Owned<jlibIPropertyTreeIterator> elemIter = pSchemaRoot->getElements(xpath);

    CElement *pElem = CElement::load(pElemArray, pSchemaRoot, strXPathExt.str());

    assert(pElem);
    pElemArray->append(*pElem);

    SETPARENTNODE(pElemArray, pParentNode);

    return pElemArray;

    int count = 1;

    ForEach(*elemIter)
    {
        strXPathExt.set(xpath);
        strXPathExt.appendf("[%d]", count);

        CElement *pElem = CElement::load(pElemArray, pSchemaRoot, strXPathExt.str());

        assert(pElem);
        pElemArray->append(*pElem);

        count++;
    }

    if (pElemArray->length() == 0)
    {
        delete pElemArray;
        return NULL;
    }

    SETPARENTNODE(pElemArray, pParentNode);

    return pElemArray;*/
}

int CElementArray::getCountOfSiblingElements(const char *pXPath) const
{
    assert(pXPath != NULL && *pXPath != 0);

    int count = 0;

    for (int idx=0; idx < this->length(); idx++)
    {
        if (strcmp(this->item(idx).getXSDXPath(), pXPath) == 0)
            count++;
    }
    return count;
}

const CXSDNodeBase* CElementArray::getNodeByTypeAndNameAscending(NODE_TYPES eNodeType, const char *pName) const
{
    assert(pName != NULL);

    for (int idx = 1; idx < this->length() && eNodeType == XSD_ELEMENT; idx++)
    {
        if (strcmp ((static_cast<CElement>(this->item(idx))).getName(), pName) == 0)
            return &(this->item(idx));
    }
    return (this->getParentNode()->getNodeByTypeAndNameAscending(eNodeType, pName));
}

const CXSDNodeBase* CElementArray::getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName) const
{
    assert(pName != NULL);

    if (eNodeType == this->getNodeType())
        return this;

    return (this->getParentNode()->getNodeByTypeAndNameDescending(eNodeType, pName));
}

const CElement* CElementArray::getElementByNameAscending(const char *pName) const
{
    for (int idx = 1; idx < this->length() ;idx++)
    {
        if (strcmp ((static_cast<CElement>(this->item(idx))).getName(), pName) == 0)
            return &(this->item(idx));
    }

    assert(!("Control should not reach here, unknown pName?"));
    return NULL;
}

const CElement* CElementArray::getElementByNameDescending(const char *pName) const
{
    for (int idx = 1; idx < this->length(); idx++)
    {
        if (strcmp ((static_cast<CElement>(this->item(idx))).getName(), pName) == 0)
            return &(this->item(idx));
    }
    return NULL;
}

int CElementArray::getSiblingIndex(const char* pXSDXPath, const CElement* pElement)
{
    assert(pXSDXPath != NULL && *pXSDXPath != 0 && pElement != NULL);

    int nSiblingIndex = 0;

    for (int idx=0; idx < this->length(); idx++)
    {
        if (strcmp(this->item(idx).getXSDXPath(), pXSDXPath) == 0)
        {
            if (&(this->item(idx)) == pElement)
                break;

            nSiblingIndex++;
        }
    }
    return nSiblingIndex;
}

bool CElementArray::anyElementsHaveMaxOccursGreaterThanOne() const
{
    int len = this->length();

    for (int i = 0; i < len; i++)
    {
        if ((this->item(i)).getMaxOccursInt() > 1 || this->item(i).getMaxOccursInt() == -1)
            return true;
    }
    return false;
}
void CElement::setIsInXSD(bool b)
{
    m_bIsInXSD = b;

    if (m_bIsInXSD == true)
    {
        CElementArray *pElemArray = dynamic_cast<CElementArray*>(this->getParentNode());
        assert(pElemArray != NULL);

        if (pElemArray != NULL)
            pElemArray->incCountOfElementsInXSD();
    }
}

bool CElement::hasChildElements() const
{
    const CComplexTypeArray* pComplexTypeArray = this->getComplexTypeArray();

    if (pComplexTypeArray != NULL && pComplexTypeArray->length() != 0)
    {
        int nLen = pComplexTypeArray->length();

        for (int i = 0; i < nLen; i++)
        {
            if (pComplexTypeArray->item(i).hasChildElements() == true)
                return true;
        }
    }
    return false;
}


CArrayOfElementArrays* CArrayOfElementArrays::load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    if (pSchemaRoot == NULL)
        return NULL;

    CArrayOfElementArrays *pArrayOfElementArrays = new CArrayOfElementArrays(pParentNode);

    /*pSchemaRoot->Link();
    pArrayOfElementArrays->setSchemaRoot(pSchemaRoot);*/

    ::StringBuffer strXPathExt(xpath);
    pArrayOfElementArrays->setXSDXPath(xpath);

    typedef ::IPropertyTreeIterator jlibIPropertyTreeIterator;
    Owned<jlibIPropertyTreeIterator> elemIter = pSchemaRoot->getElements(xpath);

    int count = 1;

    ForEach(*elemIter)
    {
        strXPathExt.set(xpath);
        strXPathExt.appendf("[%d]", count);

        CElementArray *pElemArray = CElementArray::load(pArrayOfElementArrays, pSchemaRoot, strXPathExt.str());

        assert(pElemArray);
        pArrayOfElementArrays->append(*pElemArray);

        count++;
    }

    return pArrayOfElementArrays;
}

void CArrayOfElementArrays::getDocumentation(::StringBuffer &strDoc) const
{
    for (int i = 0; i < this->length(); i++)
    {
        this->item(i).getDocumentation(strDoc);
    }
}

void CArrayOfElementArrays::populateEnvXPath(::StringBuffer strXPath, unsigned int index)
{
    ::StringBuffer strCopy(strXPath);
    CConfigSchemaHelper::stripXPathIndex(strCopy);
    this->setEnvXPath(strCopy);

    for (int i = 0; i < this->length(); i++)
    {
        this->item(i).populateEnvXPath(strXPath.str());
        CConfigSchemaHelper::getInstance()->getSchemaMapManager()->addMapOfXSDXPathToElementArray(this->item(i).getXSDXPath(), &(this->item(i)));
    }
}

void CArrayOfElementArrays::loadXMLFromEnvXml(const ::IPropertyTree *pEnvTree)
{
    for (int i = 0; i < this->ordinality(); i++)
    {
        this->item(i).loadXMLFromEnvXml(pEnvTree);
    }
}

void CArrayOfElementArrays::getJSON(::StringBuffer &strJSON, unsigned int offset, int idx) const
{
    for (int i = 0; i < this->ordinality(); i++)
    {
        if (i != 0)
            strJSON.append(",\n");
        this->item(i).getJSON(strJSON, offset);
    }
}
