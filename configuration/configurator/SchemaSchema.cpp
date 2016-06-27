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

    CIncludeArray* pIncludeArray = NULL;//CIncludeArray::load(pSchema, pSchemaRoot, strXPathExt); // change this back to be uncommented

    strXPathExt.clear().append(xpath).append(XSD_TAG_SIMPLE_TYPE);

    CSimpleTypeArray* pSimpleTypeArray = CSimpleTypeArray::load(pSchema, pSchemaRoot, strXPathExt);

    strXPathExt.clear().append(xpath).append(XSD_TAG_COMPLEX_TYPE);
    CComplexTypeArray* pComplexTypeArray = CComplexTypeArray::load(pSchema, pSchemaRoot, strXPathExt);

    strXPathExt.clear().append(xpath).append(XSD_TAG_ELEMENT);
    //CElementArray* pElemArray = CElementArray::load(pSchema, pSchemaRoot, strXPathExt.str());
    CArrayOfElementArrays* pArrayOfElemArray = CArrayOfElementArrays::load(pSchema, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append(XSD_TAG_ATTRIBUTE_GROUP);
    CAttributeGroupArray* pAttributeGroupArray = CAttributeGroupArray::load(pSchema, pSchemaRoot, strXPathExt);

    strXPathExt.clear().append(xpath).append(XSD_TAG_ANNOTATION);
    CAnnotation* pAnnotation = CAnnotation::load(pSchema, pSchemaRoot, strXPathExt);
    pSchema->m_pAnnotation = pAnnotation;

    pSchema->m_pArrayElementOfArrays = pArrayOfElemArray;
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

    QuickOutHeader(cout, XSD_SCHEMA_STR, offset);
    QUICK_OUT_2(XMLNS_XS);
    QUICK_OUT_2(ElementFormDefault);
    QUICK_OUT_2(AttributeFormDefault);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);

    if (m_pArrayElementOfArrays != NULL)
        m_pArrayElementOfArrays->dump(cout, offset);
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
    if (m_pArrayElementOfArrays != NULL)
        m_pArrayElementOfArrays->getDocumentation(strDoc);
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

void CSchema::getJSON(StringBuffer &strJSON, unsigned int offset, int idx) const
{
    strJSON.append(JSON_BEGIN);
    offset += STANDARD_OFFSET_1;
    QuickOutPad(strJSON, offset);

    //offset -= STANDARD_OFFSET_1;
    if (m_pArrayElementOfArrays != NULL)
    {
        //strJSON.append("{");
        m_pArrayElementOfArrays->item(0).getJSON(strJSON, offset, idx);
        //strJSON.append("}");
        //m_pArrayElementOfArrays->getJSON(strJSON, offset, idx);
        //DEBUG_MARK_JSON;
    }
    if (m_pComplexTypeArray != NULL)
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

    if (m_pArrayElementOfArrays != NULL)
    {
        m_pArrayElementOfArrays->populateEnvXPath(strXPath);
        //CConfigSchemaHelper::getInstance()->getSchemaMapManager()->addMapOfXSDXPathToElementArray(m_pArrayElementOfArrays->getXSDXPath(), m_pArrayElementOfArrays);
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

    if (m_pArrayElementOfArrays != NULL)
        m_pArrayElementOfArrays->loadXMLFromEnvXml(pEnvTree);
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

    CConfigSchemaHelper::getInstance()->processAttributeGroupArr();
}

const char* CSchema::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0)
    {
        int length =  m_pArrayElementOfArrays->length();

        if (m_pArrayElementOfArrays != NULL)
        {
            for (int idx = 0; idx < length; idx++)
            {
                CElementArray &ElementArray = m_pArrayElementOfArrays->item(idx);

                for (int idx2 = 0; idx2 < ElementArray.ordinality(); idx2++)
                {
                    CElement &Element = ElementArray.item(idx2);

                    m_strXML.append(Element.getXML(NULL));

                    if (idx+2 < length)
                        m_strXML.append("\n");
                }
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
