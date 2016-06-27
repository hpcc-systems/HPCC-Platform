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
#include "XMLTags.h"
#include "jptree.hpp"

#include "SchemaAnnotation.hpp"
#include "SchemaDocumentation.hpp"
#include "SchemaAppInfo.hpp"

using namespace CONFIGURATOR;

#define StringBuffer ::StringBuffer
#define IPropertyTree ::IPropertyTree

CAnnotation* CAnnotation::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL)
        return NULL;

    StringBuffer strXPathExt(xpath);

    strXPathExt.append("/").append(XSD_TAG_DOCUMENTATION);
    CDocumentation *pDocumentation = CDocumentation::load(NULL, pSchemaRoot, strXPathExt.str());

    strXPathExt.clear().append(xpath).append("/").append(XSD_TAG_APP_INFO);
    CAppInfo *pAnnInfo = CAppInfo::load(NULL, pSchemaRoot, strXPathExt.str());

    CAnnotation *pAnnotation = new CAnnotation(pParentNode, pDocumentation, pAnnInfo);
    pAnnotation->setXSDXPath(xpath);

    SETPARENTNODE(pDocumentation, pAnnotation);
    SETPARENTNODE(pAnnInfo, pAnnotation);

    return pAnnotation;
}

void CAnnotation::dump(::std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;
    QuickOutHeader(cout, XSD_ANNOTATION_STR, offset);

    QUICK_OUT(cout, XSDXPath, offset);

    if (m_pAppInfo != NULL)
        m_pAppInfo->dump(cout, offset);

    if (m_pDocumentation != NULL)
        m_pDocumentation->dump(cout, offset);

    QuickOutFooter(cout, XSD_ANNOTATION_STR, offset);
}

void CAnnotation::getDocumentation(StringBuffer &strDoc) const
{
    if (m_pAppInfo != NULL)
        m_pAppInfo->getDocumentation(strDoc);
    if (m_pDocumentation != NULL)
        m_pDocumentation->getDocumentation(strDoc);
}

void CAnnotation::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    if (m_pAppInfo != NULL)
        m_pAppInfo->loadXMLFromEnvXml(pEnvTree);

    if (m_pDocumentation != NULL)
        m_pDocumentation->loadXMLFromEnvXml(pEnvTree);
}
