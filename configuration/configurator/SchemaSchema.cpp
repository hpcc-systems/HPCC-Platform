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
#include "SchemaSchema.hpp"
#include "ConfigSchemaHelper.hpp"
#include "DocumentationMarkup.hpp"
#include "SchemaMapManager.hpp"
#include "QMLMarkup.hpp"

CSchema::~CSchema()
{
    assert(this->getLinkCount() == 1);
}

CSchema* CSchema::load(const char* pSchemaLocation, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    if (pSchemaRoot == NULL)
        return NULL;

    CConfigSchemaHelper *pSchemaHelper = (CConfigSchemaHelper::getInstance());
    if (pSchemaHelper->getSchemaMapManager()->getSchemaForXSD(pSchemaLocation) != NULL)  // check to see if the this schema has already been processed
        return pSchemaHelper->getSchemaMapManager()->getSchemaForXSD(pSchemaLocation);


    CSchema* pSchema = new CSchema(pSchemaLocation);
    pSchema->setXSDXPath(xpath);

    IPropertyTree *pTree = pSchemaRoot->queryPropTree(xpath);
    if (pTree == NULL)
        return NULL;

    pSchema->setXMLNS_XS(pTree->queryProp(XML_ATTR_XMLNS_XS));
    pSchema->setElementFormDefault(pTree->queryProp(XML_ATTR_ELEMENTFORMDEFAULT));
    pSchema->setAttributeFormDefault(pTree->queryProp(XML_ATTR_ATTRIBUTEFORMDEFAULT));

    StringBuffer strXPathExt(xpath);
    strXPathExt.clear().append(xpath).append(XSD_TAG_INCLUDE);

    CIncludeArray* pIncludeArray = CIncludeArray::load(pSchema, pSchemaRoot, strXPathExt);

    strXPathExt.clear().append(xpath).append(XSD_TAG_SIMPLE_TYPE);

    CSimpleTypeArray* pSimpleTypeArray = CSimpleTypeArray::load(pSchema, pSchemaRoot, strXPathExt);

    strXPathExt.clear().append(xpath).append(XSD_TAG_COMPLEX_TYPE);
    CComplexTypeArray* pComplexTypeArray = CComplexTypeArray::load(pSchema, pSchemaRoot, strXPathExt);

    strXPathExt.clear().append(xpath).append(XSD_TAG_ELEMENT);
    CElementArray* pElemArray = CElementArray::load(pSchema, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append(XSD_TAG_ATTRIBUTE_GROUP);
    CAttributeGroupArray* pAttributeGroupArray = CAttributeGroupArray::load(pSchema, pSchemaRoot, strXPathExt);

    strXPathExt.clear().append(xpath).append(XSD_TAG_ANNOTATION);
    CAnnotation* pAnnotation = CAnnotation::load(pSchema, pSchemaRoot, strXPathExt);
    pSchema->m_pAnnotation = pAnnotation;

    pSchema->m_pElementArray = pElemArray;
    pSchema->m_pComplexTypeArray = pComplexTypeArray;

    if (pSchema->m_pAttributeGroupArray == NULL)
        pSchema->m_pAttributeGroupArray = pAttributeGroupArray;
    else
        // copy contents from from pAttributeGroupArray to pSchema->m_pAttributeGroupArray

    pSchema->m_pSimpleTypeArray = pSimpleTypeArray;
    pSchema->m_pIncludeArray = pIncludeArray;

    pSchemaHelper->getSchemaMapManager()->setSchemaForXSD(pSchemaLocation, pSchema);

    CConfigSchemaHelper::getInstance()->processAttributeGroupArr();
    CConfigSchemaHelper::getInstance()->processExtensionArr();
    CConfigSchemaHelper::getInstance()->processNodeWithTypeArr(pSchema);

    return pSchema;
}

CSchema* CSchema::load(const char* pSchemaLocation, CXSDNodeBase* pParentNode)
{
    if (pSchemaLocation == NULL)
        return NULL;

    Linked<IPropertyTree> pSchemaRoot;
    StringBuffer schemaPath;

    if (CConfigSchemaHelper::getInstance()->getBasePath() == NULL)
        schemaPath.appendf("%s%s", DEFAULT_SCHEMA_DIRECTORY, pSchemaLocation);
    else
        schemaPath.appendf("%s/%s", CConfigSchemaHelper::getInstance()->getBasePath(), pSchemaLocation);

    try
    {
       pSchemaRoot.setown(createPTreeFromXMLFile(schemaPath.str()));
    }
    catch (...)
    {
        // TODO: Hanlde exceptions
        std::cout << "Can't open " << schemaPath.str() << std::endl;
        exit(-1);
    }

    CSchema *pSchema = CSchema::load(pSchemaLocation, pSchemaRoot, XSD_TAG_SCHEMA);
    SETPARENTNODE(pSchema, pParentNode)

    return pSchema;
}

void CSchema::dump(std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_SCHEMA_STR, offset);
    QUICK_OUT_2(XMLNS_XS);
    QUICK_OUT_2(ElementFormDefault);
    QUICK_OUT_2(AttributeFormDefault);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);

    if (m_pElementArray != NULL)
        m_pElementArray->dump(cout, offset);
    if (m_pComplexTypeArray != NULL)
        m_pComplexTypeArray->dump(cout, offset);
    if (m_pSimpleTypeArray != NULL)
        m_pSimpleTypeArray->dump(cout, offset);
    if (m_pIncludeArray != NULL)
        m_pIncludeArray->dump(cout, offset);
    if (m_pAnnotation != NULL)
        m_pAnnotation->dump(cout, offset);
    if (m_pAttributeGroupArray != NULL)
        m_pAttributeGroupArray->dump(cout, offset);

    QuickOutFooter(cout, XSD_SCHEMA_STR, offset);
}

void CSchema::getDocumentation(StringBuffer &strDoc) const
{
    if (m_pElementArray != NULL)
        m_pElementArray->getDocumentation(strDoc);
    if (m_pComplexTypeArray != NULL)
        m_pComplexTypeArray->getDocumentation(strDoc);
    if (m_pAttributeGroupArray != NULL)
        m_pAttributeGroupArray->getDocumentation(strDoc);
    if (m_pSimpleTypeArray != NULL)
        m_pSimpleTypeArray->getDocumentation(strDoc);
    if (m_pIncludeArray != NULL)
        m_pIncludeArray->getDocumentation(strDoc);

    strDoc.append(DM_SECT2_END);
}

void CSchema::getQML(StringBuffer &strQML, int idx) const
{
    strQML.append(QML_START);
    DEBUG_MARK_QML;
    strQML.append(QML_VERTICAL_SCROLL_BAR);
    DEBUG_MARK_QML;
    strQML.append(QML_HORIZONTAL_SCROLL_BAR);
    DEBUG_MARK_QML;
    strQML.append(QML_SCROLL_BAR_TRANSITIONS);
    DEBUG_MARK_QML;

    if (m_pElementArray != NULL)
        m_pElementArray->getQML(strQML, idx);
    if (m_pComplexTypeArray != NULL)
        m_pComplexTypeArray->getQML(strQML);
    if (m_pAttributeGroupArray != NULL)
        m_pAttributeGroupArray->getQML(strQML);
    if (m_pSimpleTypeArray != NULL)
        m_pSimpleTypeArray->getQML(strQML);
    if (m_pIncludeArray != NULL)
        m_pIncludeArray->getQML(strQML);

    strQML.append(QML_END);
    DEBUG_MARK_QML;
}

void CSchema::getQML2(StringBuffer &strQML, int idx) const
{
    DEBUG_MARK_QML;
    strQML.append(QML_START);
    DEBUG_MARK_QML;
    strQML.append(QML_TAB_VIEW_BEGIN);
    DEBUG_MARK_QML;

    if (m_pElementArray != NULL)
    {
        DEBUG_MARK_QML;
        m_pElementArray->setUIType(QML_UI_EMPTY);
        m_pElementArray->getQML2(strQML, idx);
        DEBUG_MARK_QML;
    }
    if (m_pComplexTypeArray != NULL)
    {
        DEBUG_MARK_QML;
        assert(!"Is this valid?");
        m_pComplexTypeArray->getQML2(strQML);
        DEBUG_MARK_QML;
    }
    if (m_pAttributeGroupArray != NULL)
    {
        DEBUG_MARK_QML;
        m_pAttributeGroupArray->getQML2(strQML);
        DEBUG_MARK_QML;
    }
    if (m_pSimpleTypeArray != NULL)
    {
        DEBUG_MARK_QML;
        m_pSimpleTypeArray->getQML2(strQML);
        DEBUG_MARK_QML;
    }
    if (m_pIncludeArray != NULL)
    {
        DEBUG_MARK_QML;
        m_pIncludeArray->getQML2(strQML);
        DEBUG_MARK_QML;
    }
    strQML.append(QML_TAB_VIEW_STYLE);
    DEBUG_MARK_QML;
    strQML.append(QML_TAB_VIEW_END);
    DEBUG_MARK_QML;
    strQML.append(QML_TAB_TEXT_STYLE);
    DEBUG_MARK_QML;
    DEBUG_MARK_QML;
    strQML.append(QML_END);
    DEBUG_MARK_QML;
}

void CSchema::getQML3(StringBuffer &strQML, int idx) const
{
    // Generates QML ListModel
    DEBUG_MARK_QML;
    strQML.append(QML_FILE_START);

    if (m_pElementArray != NULL)
    {
        DEBUG_MARK_QML;
        m_pElementArray->getQML3(strQML, idx);
        DEBUG_MARK_QML;
    }
    strQML.append(QML_DOUBLE_END_BRACKET);
}

void  CSchema::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    strXPath.append("./").append(XML_TAG_SOFTWARE);

    if (m_pElementArray != NULL)
    {
        m_pElementArray->populateEnvXPath(strXPath);
        CConfigSchemaHelper::getInstance()->getSchemaMapManager()->addMapOfXSDXPathToElementArray(m_pElementArray->getXSDXPath(), m_pElementArray);
    }
    if (m_pAttributeGroupArray != NULL)
        m_pAttributeGroupArray->populateEnvXPath(strXPath);
    if (m_pSimpleTypeArray != NULL)
        m_pSimpleTypeArray->populateEnvXPath(strXPath);
    if (m_pIncludeArray != NULL)
        m_pIncludeArray->populateEnvXPath(strXPath);
    if (m_pComplexTypeArray != NULL)
    {
        assert(m_pComplexTypeArray != 0);
        m_pComplexTypeArray->populateEnvXPath(strXPath);
    }
    this->setEnvXPath(strXPath);
}

void CSchema::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert(pEnvTree != NULL);

    if (m_pElementArray != NULL)
        m_pElementArray->loadXMLFromEnvXml(pEnvTree);
    if (m_pComplexTypeArray != NULL)
        m_pComplexTypeArray->loadXMLFromEnvXml(pEnvTree);
    if (m_pAttributeGroupArray != NULL)
        m_pAttributeGroupArray->loadXMLFromEnvXml(pEnvTree);
    if (m_pSimpleTypeArray != NULL)
        m_pSimpleTypeArray->loadXMLFromEnvXml(pEnvTree);
    if (m_pIncludeArray != NULL)
        m_pIncludeArray->loadXMLFromEnvXml(pEnvTree);
    if (m_pAttributeGroupArray != NULL)
        m_pAttributeGroupArray->loadXMLFromEnvXml(pEnvTree);
}
const char* CSchema::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0)
    {
        int length =  m_pElementArray->length();

        if (m_pElementArray != NULL)
        {
            for (int idx = 0; idx < length; idx++)
            {
                CElement &Element = m_pElementArray->item(idx);
                m_strXML.append(Element.getXML(NULL));

                if (idx+1 < length)
                    m_strXML.append("\n");
            }
        }
        if (m_pAttributeGroupArray != NULL)
        {
            length = m_pAttributeGroupArray->length();
            for (int idx = 0; idx < length; idx++)
            {
                CAttributeGroup &AttributeGroup  = m_pAttributeGroupArray->item(idx);
                m_strXML.append(AttributeGroup.getXML(NULL));

                if (idx+1 < length)
                    m_strXML.append("\n");
            }
        }

        m_strXML.append("/>\n");

        if (m_pComplexTypeArray != NULL)
        {
            length = m_pComplexTypeArray->length();
            for (int idx = 0; idx < length; idx++)
            {
                CComplexType &ComplexType = m_pComplexTypeArray->item(idx);
                m_strXML.append(ComplexType.getXML(NULL));

                if (idx+1 < length)
                     m_strXML.append("\n");
            }
        }
        if (m_pSimpleTypeArray != NULL)
        {
            length = m_pSimpleTypeArray->length();
            for (int idx = 0; idx < length; idx++)
            {
                CSimpleType &SimpleType = m_pSimpleTypeArray->item(idx);

                m_strXML.append(SimpleType.getXML(NULL));

                if (idx+1 < length)
                    m_strXML.append("\n");
            }
        }
        if (m_pIncludeArray != NULL)
        {
            length = m_pIncludeArray->length();
            for (int idx = 0; idx < length; idx++)
            {
                CInclude &Include = m_pIncludeArray->item(idx);
                m_strXML.append(Include.getXML(NULL));

                if (idx+1 < length)
                    m_strXML.append("\n");
            }
        }
    }
    return m_strXML.str();
}

CXSDNode* CSchema::getExtensionType(const char* pExtensionTypeName) const
{
    if (pExtensionTypeName == NULL)
        return NULL;

    if (m_pSimpleTypeArray != NULL)
    {
        int length = m_pSimpleTypeArray->length();

        for (int idx = 0; idx < length; idx++)
        {
            CSimpleType &SimpleType = m_pSimpleTypeArray->item(idx);

            if (strcmp(SimpleType.getName(), pExtensionTypeName) == 0)
                return &SimpleType;
        }
    }
    if (m_pComplexTypeArray != NULL)
    {
        int length = m_pComplexTypeArray->length();

        for (int idx = 0; idx < length; idx++)
        {
            CComplexType &ComplexType = m_pComplexTypeArray->item(idx);

            if (strcmp(ComplexType.getName(), pExtensionTypeName) == 0)
                return &ComplexType;
        }
    }
    return NULL;
}

const char* CSchema::getSchemaFileName() const
{
    String tempString(m_strSchemaLocation.str());

    int idx = tempString.lastIndexOf('\\');

    if (idx == -1)
        return m_strSchemaLocation.str();
    else
        return &((m_strSchemaLocation.str())[idx]);
}
