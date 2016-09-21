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
#include "SchemaComplexContent.hpp"
#include "SchemaExtension.hpp"
#include "ConfigSchemaHelper.hpp"

using namespace CONFIGURATOR;

#define StringBuffer ::StringBuffer
#define IPropertyTree ::IPropertyTree

CComplexContent* CComplexContent::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot);

    if (pSchemaRoot == nullptr)
        return nullptr;

    StringBuffer strXPathExt(xpath);
    strXPathExt.append("/").append(XSD_TAG_EXTENSION);

    if (pSchemaRoot->queryPropTree(strXPathExt.str()) == nullptr || IS_EXCLUDED(XSD_EXTENSION) == false)
        return nullptr;

    CExtension* pExtension = CExtension::load(nullptr, pSchemaRoot, strXPathExt.str());
    CComplexContent *pComplexContent = new CComplexContent(pParentNode, pExtension);

    pComplexContent->setXSDXPath(xpath);
    assert(pExtension != nullptr);

    assert(pComplexContent != nullptr);
    if (pExtension != nullptr && pComplexContent != nullptr)
    {
        SETPARENTNODE(pExtension, pComplexContent);
        pExtension->initExtension();
    }
    return pComplexContent;
}

void CComplexContent::dump(::std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    quickOutHeader(cout, XSD_COMPLEX_CONTENT_STR, offset);
    QUICK_OUT(cout, XSDXPath,  offset);

    if (m_pExtension != nullptr)
        m_pExtension->dump(cout, offset);

    quickOutFooter(cout, XSD_COMPLEX_CONTENT_STR, offset);
}


void CComplexContent::getDocumentation(StringBuffer &strDoc) const
{
    return;
}

const char* CComplexContent::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0 && m_pExtension != nullptr)
        m_strXML.append(m_pExtension->getXML(nullptr));

    return m_strXML.str();
}
