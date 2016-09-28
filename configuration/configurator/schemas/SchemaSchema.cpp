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
#include "JSONMarkUp.hpp"

using namespace CONFIGURATOR;

#define StringBuffer ::StringBuffer
#define IPropertyTree ::IPropertyTree

CSchema::~CSchema()
{
}

CSchema* CSchema::load(const char* pSchemaLocation, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != nullptr);
    if (pSchemaRoot == nullptr)
        return nullptr;

    CConfigSchemaHelper *pSchemaHelper = (CConfigSchemaHelper::getInstance());
    if (pSchemaHelper->getSchemaMapManager()->getSchemaForXSD(pSchemaLocation) != nullptr)  // check to see if the this schema has already been processed
        return pSchemaHelper->getSchemaMapManager()->getSchemaForXSD(pSchemaLocation);


    CSchema* pSchema = new CSchema(pSchemaLocation);
    pSchema->setXSDXPath(xpath);

    IPropertyTree *pTree = pSchemaRoot->queryPropTree(xpath);
    if (pTree == nullptr)
        return nullptr;

    pSchema->setXMLNS_XS(pTree->queryProp(XML_ATTR_XMLNS_XS));
    pSchema->setElementFormDefault(pTree->queryProp(XML_ATTR_ELEMENTFORMDEFAULT));
    pSchema->setAttributeFormDefault(pTree->queryProp(XML_ATTR_ATTRIBUTEFORMDEFAULT));

    StringBuffer strXPathExt(xpath);

    strXPathExt.append(XSD_TAG_INCLUDE);
    CIncludeArray* pIncludeArray = CIncludeArray::load(pSchema, pSchemaRoot, strXPathExt);

    strXPathExt.setf("%s%s",xpath, XSD_TAG_SIMPLE_TYPE);
    CSimpleTypeArray* pSimpleTypeArray = CSimpleTypeArray::load(pSchema, pSchemaRoot, strXPathExt);

    strXPathExt.setf("%s%s",xpath, XSD_TAG_COMPLEX_TYPE);
    CComplexTypeArray* pComplexTypeArray = CComplexTypeArray::load(pSchema, pSchemaRoot, strXPathExt);

    strXPathExt.setf("%s%s",xpath, XSD_TAG_ELEMENT);
    CArrayOfElementArrays* pArrayOfElemArray = CArrayOfElementArrays::load(pSchema, pSchemaRoot, strXPathExt.str());

    strXPathExt.setf("%s%s",xpath, XSD_TAG_ATTRIBUTE_GROUP);
    CAttributeGroupArray* pAttributeGroupArray = CAttributeGroupArray::load(pSchema, pSchemaRoot, strXPathExt);

    strXPathExt.setf("%s%s",xpath, XSD_TAG_ANNOTATION);
    CAnnotation* pAnnotation = CAnnotation::load(pSchema, pSchemaRoot, strXPathExt);
    pSchema->m_pAnnotation = pAnnotation;

    pSchema->m_pArrayElementOfArrays = pArrayOfElemArray;
    pSchema->m_pComplexTypeArray = pComplexTypeArray;

    if (pSchema->m_pAttributeGroupArray == nullptr)
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
    if (pSchemaLocation == nullptr)
        return nullptr;

    Linked<IPropertyTree> pSchemaRoot;
    StringBuffer schemaPath;

    if (CConfigSchemaHelper::getInstance()->getBasePath() == nullptr)
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
        ::std::cout << "Can't open " << schemaPath.str() << ::std::endl;
        exit(-1);
    }

    CSchema *pSchema = CSchema::load(pSchemaLocation, pSchemaRoot, XSD_TAG_SCHEMA);
    SETPARENTNODE(pSchema, pParentNode)

    return pSchema;
}

void CSchema::dump(::std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    quickOutHeader(cout, XSD_SCHEMA_STR, offset);
    QUICK_OUT_2(XMLNS_XS);
    QUICK_OUT_2(ElementFormDefault);
    QUICK_OUT_2(AttributeFormDefault);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);

    if (m_pArrayElementOfArrays != nullptr)
        m_pArrayElementOfArrays->dump(cout, offset);
    if (m_pComplexTypeArray != nullptr)
        m_pComplexTypeArray->dump(cout, offset);
    if (m_pSimpleTypeArray != nullptr)
        m_pSimpleTypeArray->dump(cout, offset);
    if (m_pIncludeArray != nullptr)
        m_pIncludeArray->dump(cout, offset);
    if (m_pAnnotation != nullptr)
        m_pAnnotation->dump(cout, offset);
    if (m_pAttributeGroupArray != nullptr)
        m_pAttributeGroupArray->dump(cout, offset);

    quickOutFooter(cout, XSD_SCHEMA_STR, offset);
}

void CSchema::getDocumentation(StringBuffer &strDoc) const
{
    if (m_pArrayElementOfArrays != nullptr)
        m_pArrayElementOfArrays->getDocumentation(strDoc);
    if (m_pComplexTypeArray != nullptr)
        m_pComplexTypeArray->getDocumentation(strDoc);
    if (m_pAttributeGroupArray != nullptr)
        m_pAttributeGroupArray->getDocumentation(strDoc);
    if (m_pSimpleTypeArray != nullptr)
        m_pSimpleTypeArray->getDocumentation(strDoc);
    if (m_pIncludeArray != nullptr)
        m_pIncludeArray->getDocumentation(strDoc);

    strDoc.append(DM_SECT2_END);
}

void CSchema::getJSON(StringBuffer &strJSON, unsigned int offset, int idx) const
{
    strJSON.append(JSON_BEGIN);
    offset += STANDARD_OFFSET_1;
    quickOutPad(strJSON, offset);

    //offset -= STANDARD_OFFSET_1;
    if (m_pArrayElementOfArrays != nullptr)
    {
        //strJSON.append("{");
        m_pArrayElementOfArrays->item(0).getJSON(strJSON, offset, idx);
        //strJSON.append("}");
        //m_pArrayElementOfArrays->getJSON(strJSON, offset, idx);
        //DEBUG_MARK_JSON;
    }
    if (m_pComplexTypeArray != nullptr)
    {
        //m_pComplexTypeArray->getJSON(strJSON, offset);
        //DEBUG_MARK_JSON;
    }
    //offset -= STANDARD_OFFSET_1;
    strJSON.append(JSON_END);

}

void  CSchema::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    strXPath.append("./").append(XML_TAG_SOFTWARE);

    if (m_pArrayElementOfArrays != nullptr)
        m_pArrayElementOfArrays->populateEnvXPath(strXPath);
    if (m_pAttributeGroupArray != nullptr)
        m_pAttributeGroupArray->populateEnvXPath(strXPath);
    if (m_pSimpleTypeArray != nullptr)
        m_pSimpleTypeArray->populateEnvXPath(strXPath);
    if (m_pIncludeArray != nullptr)
        m_pIncludeArray->populateEnvXPath(strXPath);
    if (m_pComplexTypeArray != nullptr)
        m_pComplexTypeArray->populateEnvXPath(strXPath);
    this->setEnvXPath(strXPath);
}

void CSchema::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert(pEnvTree != nullptr);

    if (m_pArrayElementOfArrays != nullptr)
        m_pArrayElementOfArrays->loadXMLFromEnvXml(pEnvTree);
    if (m_pComplexTypeArray != nullptr)
        m_pComplexTypeArray->loadXMLFromEnvXml(pEnvTree);
    if (m_pAttributeGroupArray != nullptr)
        m_pAttributeGroupArray->loadXMLFromEnvXml(pEnvTree);
    if (m_pSimpleTypeArray != nullptr)
        m_pSimpleTypeArray->loadXMLFromEnvXml(pEnvTree);
    if (m_pIncludeArray != nullptr)
        m_pIncludeArray->loadXMLFromEnvXml(pEnvTree);
    if (m_pAttributeGroupArray != nullptr)
        m_pAttributeGroupArray->loadXMLFromEnvXml(pEnvTree);

    CConfigSchemaHelper::getInstance()->processAttributeGroupArr();
}

const char* CSchema::getXML(const char* /*pComponent*/)
{
    int length;

    if (m_strXML.length() == 0)
    {
        if (m_pArrayElementOfArrays != nullptr)
        {
            length =  m_pArrayElementOfArrays->length();
            for (int idx = 0; idx < length; idx++)
            {
                CElementArray &ElementArray = m_pArrayElementOfArrays->item(idx);

                for (int idx2 = 0; idx2 < ElementArray.ordinality(); idx2++)
                {
                    CElement &Element = ElementArray.item(idx2);

                    m_strXML.append(Element.getXML(nullptr));

                    if (idx+2 < length)
                        m_strXML.append("\n");
                }
            }
        }
        if (m_pAttributeGroupArray != nullptr)
        {
            length = m_pAttributeGroupArray->length();
            for (int idx = 0; idx < length; idx++)
            {
                CAttributeGroup &AttributeGroup  = m_pAttributeGroupArray->item(idx);
                m_strXML.append(AttributeGroup.getXML(nullptr));

                if (idx+1 < length)
                    m_strXML.append("\n");
            }
        }

        m_strXML.append("/>\n");

        if (m_pComplexTypeArray != nullptr)
        {
            length = m_pComplexTypeArray->length();
            for (int idx = 0; idx < length; idx++)
            {
                CComplexType &ComplexType = m_pComplexTypeArray->item(idx);
                m_strXML.append(ComplexType.getXML(nullptr));

                if (idx+1 < length)
                     m_strXML.append("\n");
            }
        }
        if (m_pSimpleTypeArray != nullptr)
        {
            length = m_pSimpleTypeArray->length();
            for (int idx = 0; idx < length; idx++)
            {
                CSimpleType &SimpleType = m_pSimpleTypeArray->item(idx);

                m_strXML.append(SimpleType.getXML(nullptr));

                if (idx+1 < length)
                    m_strXML.append("\n");
            }
        }
        if (m_pIncludeArray != nullptr)
        {
            length = m_pIncludeArray->length();
            for (int idx = 0; idx < length; idx++)
            {
                CInclude &Include = m_pIncludeArray->item(idx);
                m_strXML.append(Include.getXML(nullptr));

                if (idx+1 < length)
                    m_strXML.append("\n");
            }
        }
    }
    return m_strXML.str();
}

CXSDNode* CSchema::getExtensionType(const char* pExtensionTypeName) const
{
    if (pExtensionTypeName == nullptr)
        return nullptr;

    if (m_pSimpleTypeArray != nullptr)
    {
        int length = m_pSimpleTypeArray->length();

        for (int idx = 0; idx < length; idx++)
        {
            CSimpleType &SimpleType = m_pSimpleTypeArray->item(idx);

            if (strcmp(SimpleType.getName(), pExtensionTypeName) == 0)
                return &SimpleType;
        }
    }
    if (m_pComplexTypeArray != nullptr)
    {
        int length = m_pComplexTypeArray->length();

        for (int idx = 0; idx < length; idx++)
        {
            CComplexType &ComplexType = m_pComplexTypeArray->item(idx);

            if (strcmp(ComplexType.getName(), pExtensionTypeName) == 0)
                return &ComplexType;
        }
    }
    return nullptr;
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
