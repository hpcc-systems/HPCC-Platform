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
#include "XMLTags.h"
#include "SchemaSequence.hpp"
#include "SchemaElement.hpp"
#include "JSONMarkUp.hpp"

using namespace CONFIGURATOR;

#define StringBuffer ::StringBuffer
#define IPropertyTree ::IPropertyTree

const CXSDNodeBase* CSequence::getNodeByTypeAndNameDescending(NODE_TYPES eNodeType, const char *pName) const
{
    const CXSDNodeBase* pMatchingNode = NULL;

    if (eNodeType == this->getNodeType() && (pName != NULL ? !strcmp(pName, this->getNodeTypeStr()) : true))
        return this;
    if (m_pArrayOfElementArrays != NULL)
        pMatchingNode = m_pArrayOfElementArrays->getNodeByTypeAndNameDescending(eNodeType, pName);

    return pMatchingNode;
}

CSequence* CSequence::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);
    CSequence *pSequence = NULL;

    if (pSchemaRoot == NULL)
        return NULL;
    if (pSchemaRoot->queryPropTree(xpath) == NULL)
        return NULL;  // no sequence node

    StringBuffer strXPath(xpath);
    strXPath.append("/").append(XSD_TAG_ELEMENT);

    CArrayOfElementArrays *pArrayOfElemArrays = CArrayOfElementArrays::load(NULL, pSchemaRoot, strXPath.str());

    if (pArrayOfElemArrays != NULL)
    {
        pSequence = new CSequence(pParentNode, pArrayOfElemArrays);
        pSequence->setXSDXPath(xpath);
    }
    SETPARENTNODE(pArrayOfElemArrays, pSequence)

    return pSequence;
}

void CSequence::dump(::std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_SEQUENCE_STR, offset);
    QUICK_OUT(cout, XSDXPath,  offset);

    if (m_pArrayOfElementArrays != NULL)
        m_pArrayOfElementArrays->dump(cout, offset);

    QuickOutFooter(cout, XSD_SEQUENCE_STR, offset);
}

void CSequence::getDocumentation(StringBuffer &strDoc) const
{
    if (m_pArrayOfElementArrays != NULL)
        m_pArrayOfElementArrays->getDocumentation(strDoc);
}

void CSequence::getJSON(StringBuffer &strJSON, unsigned int offset, int idx) const
{
    if (m_pArrayOfElementArrays != NULL)
    {
        m_pArrayOfElementArrays->getJSON(strJSON, offset);
    }
}

void CSequence::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    if (m_pArrayOfElementArrays != NULL)
        m_pArrayOfElementArrays->populateEnvXPath(strXPath, index);

    this->setEnvXPath(strXPath);
}

void CSequence::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    if (m_pArrayOfElementArrays != NULL)
        m_pArrayOfElementArrays->loadXMLFromEnvXml(pEnvTree);
}

bool CSequence::hasChildElements() const
{
    if (this->m_pArrayOfElementArrays->length() > 0)
    {
        for(int i = 0; i < m_pArrayOfElementArrays->ordinality(); i++)
        {
            if (m_pArrayOfElementArrays->item(i).length() > 0)
                return true;
        }
    }
    return false;
}
