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
#include "jstring.hpp"
#include "SchemaDocumentation.hpp"
#include "DocumentationMarkup.hpp"

void CDocumentation::dump(std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_DOCUMENTATION_STR, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout,Documentation, offset);
    QuickOutFooter(cout, XSD_DOCUMENTATION_STR, offset);
}

CDocumentation* CDocumentation::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    if (pSchemaRoot == NULL)
        return NULL;
    if (pSchemaRoot->queryPropTree(xpath) == NULL)
        return NULL; // no documentation node

    StringBuffer strDoc;

    if (xpath && *xpath)
        strDoc.append(pSchemaRoot->queryPropTree(xpath)->queryProp(""));

    CDocumentation *pDocumentation = new CDocumentation(pParentNode, strDoc.str());
    pDocumentation->setXSDXPath(xpath);

    return pDocumentation;
}

void  CDocumentation::getDocumentation(StringBuffer &strDoc) const
{
    strDoc.appendf("%s%s%s", DM_PARA_BEGIN, this->getDocString(), DM_PARA_END);
}
