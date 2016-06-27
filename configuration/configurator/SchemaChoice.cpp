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
#include "SchemaChoice.hpp"
#include "SchemaElement.hpp"
#include "DocumentationMarkup.hpp"

using namespace CONFIGURATOR;

#define StringBuffer ::StringBuffer
#define IPropertyTree ::IPropertyTree

CChoice* CChoice::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL || xpath == NULL)
         return NULL;

    if (pSchemaRoot->queryPropTree(xpath) == NULL)
        return NULL; // no xs:choice node found

    StringBuffer strXPathExt(xpath);
    strXPathExt.append("/").append(XSD_TAG_ELEMENT);

    CArrayOfElementArrays *pArrayOfElemArrays = CArrayOfElementArrays::load(NULL, pSchemaRoot, strXPathExt.str());
    assert(pArrayOfElemArrays);

    if (pArrayOfElemArrays == NULL)
        return NULL;

    CChoice *pChoice  = new CChoice(pParentNode, pArrayOfElemArrays);
    assert(pChoice);

    if (pChoice == NULL)
        return NULL;

    pChoice->setXSDXPath(xpath);

    SETPARENTNODE(pArrayOfElemArrays, pChoice);

    return pChoice;
}

const char* CChoice::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0)
        if (m_pArrayOfElementArrays != NULL)
            m_strXML.append(m_pArrayOfElementArrays->getXML(NULL));

    return m_strXML.str();
}

void CChoice::dump(::std::ostream &cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_CHOICE_STR, offset);

    QUICK_OUT(cout, ID,         offset);
    QUICK_OUT(cout ,MinOccurs,  offset);
    QUICK_OUT(cout, MaxOccurs,  offset);
    QUICK_OUT(cout, XSDXPath,   offset);

    if (m_pArrayOfElementArrays != NULL)
        m_pArrayOfElementArrays->dump(cout, offset);

    QuickOutFooter(cout, XSD_CHOICE_STR, offset);
}

void CChoice::getDocumentation(StringBuffer &strDoc) const
{
    if (m_pArrayOfElementArrays != NULL)
        m_pArrayOfElementArrays->getDocumentation(strDoc);
}

void CChoice::getJSON(StringBuffer &strJSON, int idx) const
{
    // this is really not correct but roxie.xsd is not correct and probably others
    bool bSkip = true;
    for (int i = 0; m_pArrayOfElementArrays->length() > i; i++)
    {
        if (m_pArrayOfElementArrays->item(i).length() == 0)
            continue;

        if (bSkip == false)
        {
            strJSON.append(",");
        }
        bSkip = false;

        m_pArrayOfElementArrays->item(i).getJSON(strJSON);
    }
}

void CChoice::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    this->setEnvXPath(strXPath);

    if (m_pArrayOfElementArrays != NULL)
        m_pArrayOfElementArrays->populateEnvXPath(strXPath);
}

void CChoice::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert (pEnvTree != NULL);

    if (m_pArrayOfElementArrays != NULL)
    {
        try
        {
            m_pArrayOfElementArrays->loadXMLFromEnvXml(pEnvTree);
        }
        catch (...)
        {
        }
    }
}
