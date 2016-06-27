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
#include "SchemaInclude.hpp"
#include "SchemaSchema.hpp"
#include "ConfigSchemaHelper.hpp"

using namespace CONFIGURATOR;

#define IPropertyTree ::IPropertyTree

void CInclude::dump(::std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_INCLUDE_STR, offset);
    QUICK_OUT(cout, SchemaLocation, offset);
    QUICK_OUT(cout, XSDXPath,  offset);

    if (this->getIncludeSchema() != NULL)
        this->getIncludeSchema()->dump(cout, offset);

    QuickOutFooter(cout, XSD_INCLUDE_STR, offset);
}

void CInclude::getDocumentation(StringBuffer &strDoc) const
{
}

const char* CInclude::getXML(const char* /*pComponent*/)
{
    return m_strXML.str();
}

CInclude* CInclude::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    //return NULL; // TODO: Fix this to properly work with includes... temporary for testing

    if (pParentNode == NULL || pSchemaRoot == NULL || xpath == NULL)
        return NULL;

    CInclude *pInclude = NULL;
    IPropertyTree *pTree = pSchemaRoot->queryPropTree(xpath);

    if (pTree != NULL)
    {
        const char *pSchemaLocation = pSchemaRoot->queryPropTree(xpath)->queryProp(XML_ATTR_SCHEMA_LOCATION);

        if (pSchemaLocation != NULL)
        {
            CSchema* pSchema = CSchema::load(pSchemaLocation, NULL); // no parent across XSD files

            pInclude = new CInclude(NULL, pSchemaLocation);
            pInclude->setXSDXPath(xpath);
            pInclude->setIncludedSchema(pSchema);
        }
    }
    return pInclude;
}

void CInclude::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    assert(this->m_pIncludedSchema != NULL);
    this->m_pIncludedSchema->populateEnvXPath(strXPath.str());
}


void CIncludeArray::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    //this->setEnvXPath(strXPath);
    //strXPath.clear();
    //QUICK_ENV_XPATH_WITH_INDEX(strXPath, index);
}

CIncludeArray* CIncludeArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char *xpath)
{
    if (pSchemaRoot == NULL)
        return NULL;

    CIncludeArray *pIncludeArray = new CIncludeArray(pParentNode);
    pIncludeArray->setXSDXPath(xpath);

    Owned<IPropertyTreeIterator> elemIter = pSchemaRoot->getElements(xpath);

    int count = 1;

    ForEach(*elemIter)
    {
        StringBuffer strXPathExt(xpath);
        strXPathExt.appendf("[%d]", count);

        CInclude *pInclude = CInclude::load(pIncludeArray, pSchemaRoot, strXPathExt.str());

        if (pInclude != NULL)
            pIncludeArray->append(*pInclude);

        count++;
    }

    if (pIncludeArray->length() == 0)
        return NULL;

    return pIncludeArray;
}

const char* CIncludeArray::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0)
    {
        int length = this->length();

        for (int idx = 0; idx < length; idx++)
        {
            CInclude &Include = this->item(idx);
            m_strXML.append(Include.getXML(NULL));

            if (idx+1 < length)
                m_strXML.append("\n");
        }
    }
    return m_strXML.str();
}

void CIncludeArray::dump(::std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_INCLUDE_ARRAY_STR, offset);
    QUICK_OUT_ARRAY(cout, offset);
    QuickOutFooter(cout, XSD_INCLUDE_ARRAY_STR, offset);
}

void CIncludeArray::getDocumentation(StringBuffer &strDoc) const
{
    QUICK_DOC_ARRAY(strDoc);
}
