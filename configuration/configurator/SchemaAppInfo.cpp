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
#include "SchemaAppInfo.hpp"
#include "DocumentationMarkup.hpp"

using namespace CONFIGURATOR;

#define IPropertyTree ::IPropertyTree

CAppInfo* CAppInfo::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    if (pSchemaRoot == NULL)
        return NULL;

    if (pSchemaRoot->queryPropTree(xpath) == NULL)
        return NULL;   // No AppInfo node

    StringBuffer strXPathViewType(xpath);
    strXPathViewType.append("/").append(TAG_VIEWTYPE);
    StringBuffer strXPathColIndex(xpath);
    strXPathColIndex.append("/").append(TAG_COLINDEX);
    StringBuffer strXPathToolTip(xpath);
    strXPathToolTip.append("/").append(TAG_TOOLTIP);
    StringBuffer strXPathTitle(xpath);
    strXPathTitle.append("/").append(TAG_TITLE);
    StringBuffer strXPathWidth(xpath);
    strXPathWidth.append("/").append(TAG_WIDTH);
    StringBuffer strXPathAutoGenDefaultValue(xpath);
    strXPathAutoGenDefaultValue.append("/").append(TAG_AUTOGENWIZARD);
    StringBuffer strXPathAutoGenForWizard(xpath);
    strXPathAutoGenForWizard.append("/").append(TAG_AUTOGENDEFAULTVALUE);
    StringBuffer strXPathAutoGenDefaultForMultiNode(xpath);
    strXPathAutoGenDefaultForMultiNode.append("/").append(TAG_AUTOGENDEFAULTVALUEFORMULTINODE);
    StringBuffer strXPathViewChildNodes(xpath);
    strXPathViewChildNodes.append("/").append(TAG_VIEWCHILDNODES);
    StringBuffer strXPathXPath(xpath);
    strXPathXPath.append("/").append(TAG_XPATH);
    StringBuffer strXPathDocID(xpath);
    strXPathDocID.append("/").append(TAG_DOC_ID);
    StringBuffer strXPathDocLineBreak(xpath);
    strXPathDocLineBreak.append("/").append(TAG_DOC_USE_LINE_BREAK);

    StringBuffer strViewType;
    StringBuffer strColIndex;
    StringBuffer strToolTip;
    StringBuffer strTitle;
    StringBuffer strWidth;
    StringBuffer strAutoGenForWizard;
    StringBuffer strAutoGenDefaultValue;
    StringBuffer strAutoGenDefaultForMultiNode;
    StringBuffer strViewChildNodes;
    StringBuffer strXPath;
    StringBuffer strDocTableID;
    bool bDocLineBreak = false;

    if (pSchemaRoot->queryPropTree(strXPathViewType.str()) != NULL)
        strViewType.append(pSchemaRoot->queryPropTree(strXPathViewType.str())->queryProp(""));
    if (pSchemaRoot->queryPropTree(strXPathColIndex.str()) != NULL)
        strColIndex.append(pSchemaRoot->queryPropTree(strXPathColIndex.str())->queryProp(""));
    if (pSchemaRoot->queryPropTree(strXPathToolTip.str()) != NULL)
        strToolTip.append(pSchemaRoot->queryPropTree(strXPathToolTip.str())->queryProp(""));
    if (pSchemaRoot->queryPropTree(strXPathTitle.str()) != NULL)
        strTitle.append(pSchemaRoot->queryPropTree(strXPathTitle.str())->queryProp(""));
    if (pSchemaRoot->queryPropTree(strXPathWidth.str()) != NULL)
        strWidth.append(pSchemaRoot->queryPropTree(strXPathWidth.str())->queryProp(""));
    if (pSchemaRoot->queryPropTree(strXPathAutoGenForWizard.str()) != NULL)
        strAutoGenForWizard.append(pSchemaRoot->queryPropTree(strXPathAutoGenForWizard.str())->queryProp(""));
    if (pSchemaRoot->queryPropTree(strXPathAutoGenDefaultValue.str()) != NULL)
        strAutoGenDefaultValue.append(pSchemaRoot->queryPropTree(strXPathAutoGenDefaultValue.str())->queryProp(""));
    if (pSchemaRoot->queryPropTree(strXPathAutoGenDefaultForMultiNode.str()) != NULL)
        strAutoGenDefaultForMultiNode.append(pSchemaRoot->queryPropTree(strXPathAutoGenDefaultForMultiNode.str())->queryProp(""));
    if (pSchemaRoot->queryPropTree(strXPathViewChildNodes.str()) != NULL)
        strViewChildNodes.append(pSchemaRoot->queryPropTree(strXPathViewChildNodes.str())->queryProp(""));
    if (pSchemaRoot->queryPropTree(strXPathXPath.str()) != NULL)
        strXPath.append(pSchemaRoot->queryPropTree(strXPathXPath.str())->queryProp(""));
    if (pSchemaRoot->queryPropTree(strXPathDocID.str()) != NULL)
        strDocTableID.append(pSchemaRoot->queryPropTree(strXPathDocID.str())->queryProp(""));
    if (pSchemaRoot->queryPropTree(strXPathDocLineBreak.str()) != NULL)
        bDocLineBreak = true;

    CAppInfo *pAppInfo = new CAppInfo(pParentNode, strViewType.str(),  strColIndex.str(), strToolTip.str(), strTitle.str(), strWidth.str(), strAutoGenForWizard.str(), strAutoGenDefaultValue.str(), NULL, strViewChildNodes.str(), strXPath.str(), strDocTableID.str(), bDocLineBreak);
    pAppInfo->setXSDXPath(xpath);

    return pAppInfo;
}

void CAppInfo::dump(::std::ostream &cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_APP_INFO_STR, offset);

    QUICK_OUT(cout, Title, offset);
    QUICK_OUT(cout, ViewType, offset);
    QUICK_OUT(cout, ToolTip, offset);
    QUICK_OUT(cout, ColIndex, offset);
    QUICK_OUT(cout, Width, offset);
    QUICK_OUT(cout, AutoGenForWizard, offset);
    QUICK_OUT(cout, AutoGenDefaultValue, offset);
    QUICK_OUT(cout, AutoGenDefaultValueForMultiNode, offset);
    QUICK_OUT(cout, ViewChildNodes, offset);
    QUICK_OUT(cout, XPath, offset);
    QUICK_OUT(cout, XSDXPath, offset);

    QuickOutFooter(cout, XSD_APP_INFO_STR, offset);
}

void CAppInfo::getDocumentation(StringBuffer &strDoc) const
{
}

void CAppInfo::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
}
