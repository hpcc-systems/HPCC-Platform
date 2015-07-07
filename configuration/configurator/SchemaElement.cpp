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
#include "QMLMarkup.hpp"
#include "SchemaMapManager.hpp"
#include "ConfiguratorMain.hpp"

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

CElement* CElement::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath, bool bIsInXSD)
{
    assert(pSchemaRoot != NULL);
    assert(pParentNode != NULL);

    if (pSchemaRoot == NULL || pParentNode == NULL)
        return NULL;

    CElement *pElement = new CElement(pParentNode);

    pElement->setIsInXSD(bIsInXSD);
    pElement->setXSDXPath(xpath);

    IPropertyTree *pTree = pSchemaRoot->queryPropTree(xpath);

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

            assert (pRef != NULL && *pRef != 0 && pElement->getConstAncestorNode(2)->getNodeType() != XSD_SCHEMA);

            if (pRef != NULL && *pRef != 0 && pElement->getConstAncestorNode(2)->getNodeType() != XSD_SCHEMA)
            {
                pElement->setRef(pRef);
                CConfigSchemaHelper::getInstance()->addElementForRefProcessing(pElement);
            }
            else
            {
                // TODO:  throw exception
            }
        }
        assert(iterAttrib->queryValue() != NULL);
    }
    assert(strlen(pElement->getName()) > 0);

    StringBuffer strXPathExt(xpath);

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

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_KEY);
    pElement->m_pKeyArray = CKeyArray::load(pElement, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_KEYREF);
    pElement->m_pKeyRefArray = CKeyRefArray::load(pElement, pSchemaRoot, strXPathExt.str());

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

void CElement::dump(std::ostream &cout, unsigned int offset) const
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

void CElement::getDocumentation(StringBuffer &strDoc) const
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
        StringBuffer strName(this->getName());

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


void CElement::getQML(StringBuffer &strQML, int idx) const
{
    // Handle HPCC Specific tag
    if (m_pAnnotation != NULL && m_pAnnotation->getAppInfo() != NULL && m_pAnnotation->getAppInfo()->getViewType() != NULL)
    {
        if (stricmp(m_pAnnotation->getAppInfo()->getViewType(), "none") == 0)
            return;
    }
    if (this->isATab())/// || this->isTopLevelElement() == true)  // Tabs will be made for all elements in a sequence
    {
        if (idx == 0)
        {
            strQML.append(QML_TAB_VIEW_BEGIN);
            DEBUG_MARK_QML;
            //strQML.append(QML_GRID_LAYOUT_BEGIN_1);
            //DEBUG_MARK_QML;
        }
        CQMLMarkupHelper::getTabQML(strQML, /*this->isTopLevelElement() == true ? "Attributes" : */this->getTitle());
        DEBUG_MARK_QML;
        strQML.append(QML_GRID_LAYOUT_BEGIN_1);
        DEBUG_MARK_QML;

        if (m_pAnnotation != NULL)
            m_pAnnotation->getQML(strQML);
        if (m_pComplexTypeArray != NULL)
            m_pComplexTypeArray->getQML(strQML,idx);

        strQML.append(QML_GRID_LAYOUT_END);
        DEBUG_MARK_QML;
        strQML.append(QML_FLICKABLE_HEIGHT).append(CQMLMarkupHelper::getImplicitHeight() * 1.5);
        DEBUG_MARK_QML;
        strQML.append(QML_FLICKABLE_END);
        DEBUG_MARK_QML;
        strQML.append(QML_TAB_END);
        DEBUG_MARK_QML;

        if (static_cast<CElementArray*>(this->getParentNode())->getCountOfElementsInXSD()-1 == idx)
        {
            //strQML.append(QML_GRID_LAYOUT_END);
            //DEBUG_MARK_QML;
            strQML.append(QML_TAB_VIEW_HEIGHT).append(CQMLMarkupHelper::getImplicitHeight());
            DEBUG_MARK_QML;
            strQML.append(QML_TAB_VIEW_STYLE);
            DEBUG_MARK_QML;

            const CComplexType *pComplexType = dynamic_cast<const CComplexType*>(this->getConstAncestorNode(3));

            if (pComplexType != NULL)
            {
                if ((pComplexType->getAttributeArray() != NULL && pComplexType->getAttributeArray()->length() > 0) ||\
                        (pComplexType->getAttributeGroupArray() != NULL && pComplexType->getAttributeGroupArray()->length() > 0))
                {
                    DEBUG_MARK_QML;
                }
            }
            else
            {
                strQML.append(QML_TAB_VIEW_END);
                DEBUG_MARK_QML;
            }
            //strQML.append(QML_TAB_TEXT_STYLE);
            //DEBUG_MARK_QML;
            //strQML.append(QML_TAB_VIEW_END);
            //DEBUG_MARK_QML;
        }
    }
    else
    {
        if (0 == idx)
        {
            //strQML.append(QML_GRID_LAYOUT_BEGIN_1);
            //DEBUG_MARK_QML;
        }
        if (m_pAnnotation != NULL)
            m_pAnnotation->getQML(strQML);
        if (m_pComplexTypeArray != NULL)
            m_pComplexTypeArray->getQML(strQML,idx);

        if (static_cast<CElementArray*>(this->getParentNode())->getCountOfElementsInXSD()-1 == idx)
        {
            //strQML.append(QML_GRID_LAYOUT_END);
            //DEBUG_MARK_QML;
        }
    }

}

void CElement::getQML2(StringBuffer &strQML, int idx) const
{
    // Handle HPCC Specific tag
    if (m_pAnnotation != NULL && m_pAnnotation->getAppInfo() != NULL && m_pAnnotation->getAppInfo()->getViewType() != NULL)
    {
        if (stricmp(m_pAnnotation->getAppInfo()->getViewType(), "none") == 0)
            return;
    }

    if (this->getParentNode()->getUIType() == QML_UI_EMPTY)
    {
        this->setUIType(QML_UI_TAB);
        if (m_pComplexTypeArray != NULL && m_pComplexTypeArray->length() > 0)
        {
            DEBUG_MARK_QML2(this);
            m_pComplexTypeArray->getQML2(strQML);
            DEBUG_MARK_QML2(this);
        }
    }
    else if (this->getMaxOccursInt() > 1 && this->hasChildElements() == false) // must be a table
    {
        this->setUIType(QML_UI_TABLE);

        strQML.append(QML_TABLE_VIEW_BEGIN);
        DEBUG_MARK_QML;

        strQML.append(QML_MODEL).append(getTableDataModelName(CConfigSchemaHelper::getInstance(0)->getNumberOfTables())).append(QML_STYLE_NEW_LINE);
        DEBUG_MARK_QML;

        strQML.append(QML_PROPERTY_STRING_TABLE_BEGIN).append(getTableDataModelName(CConfigSchemaHelper::getInstance(0)->getNumberOfTables())).append(QML_PROPERTY_STRING_TABLE_PART_1).append(this->getXSDXPath()).append(QML_PROPERTY_STRING_TABLE_END);
        DEBUG_MARK_QML;

        CConfigSchemaHelper::getInstance(0)->incTables();

        if (m_pComplexTypeArray != NULL)
        {
            DEBUG_MARK_QML;
            m_pComplexTypeArray->getQML2(strQML);
            DEBUG_MARK_QML;
        }

        DEBUG_MARK_QML;
        strQML.append(QML_TABLE_VIEW_END);
        DEBUG_MARK_QML;

    }
    else if (this->hasChildElements() == false && this->getConstParentNode()->getUIType() == QML_UI_TAB && this->getMaxOccursInt() > 1) // must be table
    {
        this->setUIType(QML_UI_TABLE);

        strQML.append(QML_TABLE_VIEW_BEGIN);
        DEBUG_MARK_QML;

        strQML.append(QML_MODEL).append(getTableDataModelName(CConfigSchemaHelper::getInstance(0)->getNumberOfTables())).append(QML_STYLE_NEW_LINE);
        DEBUG_MARK_QML;

        strQML.append(QML_PROPERTY_STRING_TABLE_BEGIN).append(getTableDataModelName(CConfigSchemaHelper::getInstance(0)->getNumberOfTables())).append(QML_PROPERTY_STRING_TABLE_PART_1).append(this->getXSDXPath()).append(QML_PROPERTY_STRING_TABLE_END);
        DEBUG_MARK_QML;

        CConfigSchemaHelper::getInstance(0)->incTables();

        if (m_pComplexTypeArray != NULL)
        {
            DEBUG_MARK_QML;
            m_pComplexTypeArray->getQML2(strQML);
            DEBUG_MARK_QML;
        }

        DEBUG_MARK_QML;
        strQML.append(QML_TABLE_VIEW_END);
        DEBUG_MARK_QML;
    }
    else // must be a tab
    {
        const CElement *pElem = dynamic_cast<const CElement*>(this->getAncestorElement(this));

        if (pElem->isTopLevelElement() == false)
        {
            //strQML.append(QML_ROW_BEGIN);
            DEBUG_MARK_QML;

            if (idx == 0)
            {
                strQML.append(QML_TAB_VIEW_BEGIN);
                DEBUG_MARK_QML;
            }
        }

        this->setUIType(QML_UI_TAB);

        CQMLMarkupHelper::getTabQML(strQML, this->getTitle());
        DEBUG_MARK_QML;

        strQML.append(QML_GRID_LAYOUT_BEGIN);
        DEBUG_MARK_QML;

        strQML.append(QML_ROW_BEGIN);
        DEBUG_MARK_QML;

        if (m_pComplexTypeArray != NULL && m_pComplexTypeArray->length() > 0)
        {
            m_pComplexTypeArray->getQML2(strQML);
            DEBUG_MARK_QML2(this);
        }

        strQML.append(QML_ROW_END);
        DEBUG_MARK_QML;
        strQML.append(QML_GRID_LAYOUT_END);
        DEBUG_MARK_QML;
        strQML.append(QML_FLICKABLE_END);
        DEBUG_MARK_QML;
        strQML.append(QML_TAB_END);
        DEBUG_MARK_QML;

        const CElementArray *pElementArray = dynamic_cast<const CElementArray*>(this->getConstParentNode());
        assert(pElementArray != NULL);

        if (pElementArray != NULL && pElementArray->length()-1 == idx && pElem->isTopLevelElement() == false)
        {
            strQML.append(QML_TAB_VIEW_STYLE);
            DEBUG_MARK_QML;

            strQML.append(QML_TAB_VIEW_END);
            DEBUG_MARK_QML;

            if (pElem->isTopLevelElement() == false)
            {
                //strQML.append(QML_ROW_END);
                DEBUG_MARK_QML;
            }

            strQML.append(QML_TAB_TEXT_STYLE);
            DEBUG_MARK_QML;
        }
        else
        {
            //strQML.append(QML_ROW_END);
            DEBUG_MARK_QML;
        }
    }
}
void CElement::getQML3(StringBuffer &strQML, int idx) const
{
    DEBUG_MARK_QML;
    if (m_pComplexTypeArray != NULL)
    {
        if(this->getUIType() == QML_UI_TABLE_LIST)
        {
            DEBUG_MARK_QML;
            m_pComplexTypeArray->setUIType(this->getUIType());
            m_pComplexTypeArray->getQML3(strQML);
        }
        else 
        {
            const char * altTitle = "";
            if((m_pComplexTypeArray->item(0)).getAttributeArray() != NULL && (m_pComplexTypeArray->item(0)).getAttributeArray()->findAttributeWithName("name") != NULL)
                altTitle = (m_pComplexTypeArray->item(0)).getAttributeArray()->findAttributeWithName("name")->getEnvXPath();
    
            DEBUG_MARK_QML;
            CQMLMarkupHelper::buildAccordionStart(strQML, this->getTitle(), altTitle);
            DEBUG_MARK_QML;
            m_pComplexTypeArray->getQML3(strQML);
            strQML.append(QML_DOUBLE_END_BRACKET);
            DEBUG_MARK_QML;
        }
    }
    DEBUG_MARK_QML;
}

bool CElement::isATab() const
{
    const CComplexTypeArray *pComplexTypArray = this->getComplexTypeArray();
    const CAttributeGroupArray *pAttributeGroupArray = (pComplexTypArray != NULL && pComplexTypArray->length() > 0) ? pComplexTypArray->item(0).getAttributeGroupArray() : NULL;
    const CAttributeArray *pAttributeArray = (pComplexTypArray != NULL && pComplexTypArray->length() > 0) ? pComplexTypArray->item(0).getAttributeArray() : NULL;

    if (this->getConstAncestorNode(2)->getNodeType() != XSD_SCHEMA && \
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
    else*/ if (this->getConstAncestorNode(2)->getNodeType() == XSD_SEQUENCE && this->getConstAncestorNode(3)->getNodeType() == XSD_COMPLEX_TYPE)
    {
        return true;
    }
    else if (/*this->getConstAncestorNode(2)->getNodeType == XSD_COMPLEX_TYPE &&*/ this->getConstAncestorNode(3)->getNodeType() == XSD_ELEMENT)
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


void CElement::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    assert(strXPath.length() > 0);

    strXPath.append("/").append(this->getName()).append("[").append(index).append("]");

    PROGLOG("Setting element to envpath of %s, previous path: %s", strXPath.str(), this->getEnvXPath());
    this->setEnvXPath(strXPath);

    if (m_pComplexTypeArray != NULL)
        m_pComplexTypeArray->populateEnvXPath(strXPath, index);
    if (m_pSimpleType != NULL)
        m_pSimpleType->populateEnvXPath(strXPath, index);
    if (m_pKeyArray != NULL)
        m_pKeyArray->populateEnvXPath(strXPath, index);
}

void CElement::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    //PROGLOG("Mapping element with XPATH of %s to %p", this->getEnvXPath(), this);
    CConfigSchemaHelper::getInstance()->getSchemaMapManager()->addMapOfXPathToElement(this->getEnvXPath(), this);

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

void CElementArray::dump(std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_ELEMENT_ARRAY_STR, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT_ARRAY(cout, offset);
    QuickOutFooter(cout, XSD_ELEMENT_ARRAY_STR, offset);
}

void CElementArray::getDocumentation(StringBuffer &strDoc) const
{
    QUICK_DOC_ARRAY(strDoc);
}

void CElementArray::getQML(StringBuffer &strQML, int idx) const
{
    for (int idx=0; idx < this->length(); idx++)
    {
        if ((this->item(idx)).getIsInXSD() == true)
            (this->item(idx)).getQML(strQML, idx);
    }
}

void CElementArray::getQML2(StringBuffer &strQML, int idx) const
{
    DEBUG_MARK_QML;
    if (this->getParentNode()->getUIType() == QML_UI_TAB)
    {
        DEBUG_MARK_QML;
        this->setUIType(QML_UI_TAB);
        //strQML.append(QML_TAB_VIEW_BEGIN);
        //DEBUG_MARK_QML;
        for (int i = 0; i < this->length(); i++)
        {
            if ((this->item(i)).hasChildElements() == true)
            {
                DEBUG_MARK_QML;
                //(this->item(i)).setUIType(QML_UI_TAB);
            }
            else if ((this->item(i)).getMaxOccursInt() > 1)
            {
                DEBUG_MARK_QML;
                //(this->item(i)).setUIType(QML_UI_TABLE);
            }
            else
            {
                DEBUG_MARK_QML;
                //(this->item(i)).setUIType(QML_UI_TEXT_FIELD);
            }
            DEBUG_MARK_QML;
            (this->item(i)).getQML2(strQML,i);
        }
    }
    else if (this->getUIType() == QML_UI_EMPTY)
    {
        DEBUG_MARK_QML;
        this->item(0).getQML2(strQML,0);
        DEBUG_MARK_QML;
    }
}

void CElementArray::getQML3(StringBuffer &strQML, int idx) const
{
    DEBUG_MARK_QML;
    if(this->length() > 1)
    {
        StringArray keyspace;
        MapStringTo<bool,bool> isList;
        MapStringTo<StringBuffer,const char *> elementGroups;

        for (int i = 0; i < this->length(); i++) // Go through Element Array to initialize elementGroups
        {            
            if(elementGroups.getValue((this->item(i)).getName()) == NULL) // If this is a unique element name
            {
                StringBuffer temp("");
                CQMLMarkupHelper::buildAccordionStart(temp,(this->item(i)).getTitle()); // Build Accordion

                // If list or instance, append table boilerplate, make mapping for later
                if( (this->item(i)).getViewType() != NULL && 
                    (!stricmp((this->item(i)).getViewType(),"list") || !stricmp((this->item(i)).getViewType(),"instance")) &&
                    (this->item(i)).getComplexTypeArray() != NULL &&
                    ((this->item(i)).getComplexTypeArray()->item(0)).getSequence() == NULL &&
                    ((this->item(i)).getComplexTypeArray()->item(0)).getAttributeGroupArray() == NULL && 
                    ((this->item(i)).getComplexTypeArray()->item(0)).getAttributeArray() != NULL && 
                    ((this->item(i)).getComplexTypeArray()->item(0)).getAttributeArray()->length() > 0
                    )
                {
                    StringArray names;
                    StringArray titles;

                    isList.setValue((this->item(i)).getName(),true);
                    temp.append(QML_TABLE_START);

                    ((this->item(i)).getComplexTypeArray()->item(0)).getAttributeArray()->getAttributeNames(names,titles);
                    CQMLMarkupHelper::buildColumns(temp, names, titles);

                    temp.append(QML_TABLE_CONTENT_START);
                }
                else
                    isList.setValue((this->item(i)).getName(),false);

                // Save grouping, save keyspace for reference
                elementGroups.setValue((this->item(i)).getName(),temp.str());
                keyspace.append((this->item(i)).getName());
            }
        }
        for (int i = 0; i < this->length(); i++)
        {
            assert(*elementGroups.getValue((this->item(i)).getName()) != NULL);
            if(*elementGroups.getValue((this->item(i)).getName()) != NULL)
            {
                StringBuffer temp = *elementGroups.getValue((this->item(i)).getName());

                if(*isList.getValue((this->item(i)).getName()) == true)
                    (this->item(i)).setUIType(QML_UI_TABLE_LIST);

                (this->item(i)).getQML3(temp,i);
                elementGroups.setValue((this->item(i)).getName(),temp.str());
            }
        }
        for(int i = 0; i < keyspace.length(); i++)
        {
            strQML.append(*elementGroups.getValue(keyspace[i]));
            DEBUG_MARK_QML;

            if(*isList.getValue(keyspace[i]) == true)
                strQML.append(QML_DOUBLE_END_BRACKET);

            strQML.append(QML_DOUBLE_END_BRACKET);
            DEBUG_MARK_QML;
        }
    }
    else
        (this->item(0)).getQML3(strQML,0);
}

void CElementArray::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    int elemCount = 1;

    this->setEnvXPath(strXPath);

    for (int idx=0; idx < this->length(); idx++)
    {
        if (this->item(idx).getIsInXSD() == true)
            elemCount == 1;
        else
            elemCount++;

        this->item(idx).populateEnvXPath(strXPath, elemCount);
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

void CElementArray::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    int nUniqueElements = this->length();

    for (int idx = 0; idx < nUniqueElements; idx++)
    {
        StringBuffer strXPath(this->item(idx).getEnvXPath());

        if (pEnvTree->hasProp(strXPath.str()) == false)
            continue;

        int subIndex = 1;

        do
        {
            StringBuffer strEnvXPath(this->item(idx).getEnvXPath());
            CConfigSchemaHelper::stripXPathIndex(strEnvXPath);

            strEnvXPath.appendf("[%d]",subIndex);

            if (pEnvTree->queryPropTree(strEnvXPath.str()) == NULL)
                break;

            CElement *pElement = NULL;
            if (subIndex > 1)
            {
                pElement = CElement::load(this, this->getSchemaRoot(), this->item(idx).getXSDXPath(), false);

                int nIndexOfElement =  (static_cast<CElementArray*>(pElement->getParentNode()))->getCountOfSiblingElements(pElement->getXSDXPath())+1;
                pElement->populateEnvXPath(this->getEnvXPath(), subIndex);

                PROGLOG("XSD Xpath to non-native element to %s", pElement->getXSDXPath());
                PROGLOG("XML Xpath to non-native element to %s", pElement->getEnvXPath());

                pElement->setTopLevelElement(false);

                CConfigSchemaHelper::getInstance()->getSchemaMapManager()->addMapOfXPathToElement(pElement->getEnvXPath(), pElement);
                CConfigSchemaHelper::getInstance()->getSchemaMapManager()->addMapOfXSDXPathToElementArray(pElement/*->getConstParentNode()*/->getXSDXPath(), (static_cast<CElementArray*>(pElement->getParentNode())));

                this->append(*pElement);
                PROGLOG("Added element %p with xsd xpath=%s array is size=%d with xpath of %s", pElement, pElement->getXSDXPath(),this->length(), this->getXSDXPath());
            }
            else
            {
                pElement = &(this->item(idx));

                if (pElement->getConstAncestorNode(2)->getNodeType() == XSD_SCHEMA)
                    pElement->setTopLevelElement(true);

                CConfigSchemaHelper::getInstance()->getSchemaMapManager()->addMapOfXSDXPathToElementArray(pElement/*->getConstParentNode()*/->getXSDXPath(), (static_cast<CElementArray*>(pElement->getParentNode())));
                PROGLOG("Added element %p with xsd xpath=%s array is size=%d with xpath of %s", pElement, pElement->getXSDXPath(),this->length(), this->getXSDXPath());
            }
            pElement->loadXMLFromEnvXml(pEnvTree);

            subIndex++;
        }
        while (true);
    }
}

CElementArray* CElementArray::load(const char* pSchemaFile)
{
    assert(pSchemaFile != NULL);
    if (pSchemaFile == NULL)
        return NULL;

    Linked<IPropertyTree> pSchemaRoot;
    StringBuffer schemaPath;

    schemaPath.appendf("%s%s", DEFAULT_SCHEMA_DIRECTORY, pSchemaFile);
    pSchemaRoot.setown(createPTreeFromXMLFile(schemaPath.str()));

    CElementArray *pElemArray = CElementArray::load(NULL, pSchemaRoot, XSD_TAG_ELEMENT);

    PROGLOG("Function: %s() at %s:%d", __func__, __FILE__, __LINE__);
    PROGLOG("pElemArray = %p", pElemArray);

    return pElemArray;
}

CElementArray* CElementArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    if (pSchemaRoot == NULL)
        return NULL;

    CElementArray *pElemArray = new CElementArray(pParentNode);
    assert(pElemArray != NULL);

    pSchemaRoot->Link();
    pElemArray->setSchemaRoot(pSchemaRoot);

    StringBuffer strXPathExt(xpath);
    pElemArray->setXSDXPath(xpath);

    Owned<IPropertyTreeIterator> elemIter = pSchemaRoot->getElements(xpath);

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

    return pElemArray;
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
