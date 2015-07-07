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

#include "QMLMarkup.hpp"
#include "SchemaCommon.hpp"
#include "jstring.hpp"
#include "jutil.hpp"
#include "jarray.hpp"
#include "jdebug.hpp"
#include "SchemaAttributes.hpp"
#include "SchemaElement.hpp"
#include "SchemaAppInfo.hpp"
#include "SchemaAnnotation.hpp"

int CQMLMarkupHelper::glImplicitHeight = -1;

void CQMLMarkupHelper::getTabQML(StringBuffer &strQML, const char *pName)
{
    assert(pName != NULL);

    strQML.append(QML_TAB_BEGIN);
    strQML.append(QML_TAB_TITLE_BEGIN).append(pName).append(QML_TAB_TITLE_END);
    strQML.append(QML_FLICKABLE_BEGIN);
    DEBUG_MARK_QML;
}

void CQMLMarkupHelper::getComboBoxListElement(const char* pLabel, StringBuffer &strID, const char* pDefault)
{
    static StringBuffer strBuf;
    strBuf.append(QML_LIST_ELEMENT_BEGIN).append(pLabel);
}

void CQMLMarkupHelper::getToolTipQML(StringBuffer &strQML, const char *pToolTip, const char* pTextAreaID)
{
    assert(pToolTip != NULL);

    StringBuffer strTimer1("timer1");
    StringBuffer strTimer2("timer2");
    StringBuffer strMouseArea("mousearea");
    StringBuffer strRectangle("rectangle");
    StringBuffer strTextArea(pTextAreaID);
    StringBuffer strToolTip(pToolTip);

    strToolTip.replace('\"','\'');

    CQMLMarkupHelper::getRandomID(&strTimer1);
    CQMLMarkupHelper::getRandomID(&strTimer2);
    CQMLMarkupHelper::getRandomID(&strMouseArea);
    CQMLMarkupHelper::getRandomID(&strRectangle);

    CQMLMarkupHelper::getToolTipRectangle(strQML, strToolTip.str(), strRectangle.str());
    DEBUG_MARK_QML;
    CQMLMarkupHelper::getToolTipTimer(strQML, strToolTip.str(), strRectangle.str(), strTimer1.str(), strTimer2.str(), strMouseArea.str());
    DEBUG_MARK_QML;
    CQMLMarkupHelper::getToolMouseArea(strQML, strToolTip.str(), strRectangle.str(), strTimer1.str(), strTimer2.str(), strMouseArea.str(), strTextArea.str());
    DEBUG_MARK_QML;
}

void CQMLMarkupHelper::getToolTipTimer(StringBuffer &strQML, const char *pToolTip, const char *pRectangleID, const char* pTimerID_1, const char* pTimerID_2, const char *pMouseAreaID)
{

    strQML.append(QML_TOOLTIP_TIMER_BEGIN)\
            .append(QML_TOOLTIP_TIMER_ON_TRIGGERED_BEGIN)\
                .append(QML_STYLE_INDENT).append(pRectangleID).append(QML_TOOLTIP_TIMER_RECTANGLE_APPEND_TRUE)\
                .append(QML_STYLE_INDENT).append(pTimerID_2).append(QML_TOOLTIP_TIMER_TIMER_APPEND_START)\
            .append(QML_TOOLTIP_TIMER_ON_TRIGGERED_END)\
            .append(QML_TOOLTIP_TIMER_ID).append(pTimerID_1)\
           .append(QML_TOOLTIP_TIMER_END);
}

void CQMLMarkupHelper::getToolTipRectangle(StringBuffer &strQML, const char *pToolTip, const char *pRectangleID)
{
    strQML.append(QML_TOOLTIP_TEXT_BEGIN).append(pRectangleID).append(QML_TOOLTIP_TEXT_PART_1).append(pToolTip).append(QML_TOOLTIP_TEXT_PART_2);
}

void CQMLMarkupHelper::getToolMouseArea(StringBuffer &strQML, const char *pToolTip, const char *pRectangleID, const char* pTimerID_1, const char* pTimerID_2, const char *pMouseAreaID, const char* pTextAreaID)
{
    strQML.append(QML_MOUSE_AREA_BEGIN)\
            .append(pMouseAreaID).append(QML_MOUSE_AREA_ID_APPEND)\
                .append(QML_MOUSE_AREA_ON_ENTERED_BEGIN)
                    .append(QML_STYLE_INDENT).append(pTimerID_1).append(QML_TOOLTIP_TIMER_TIMER_APPEND_START)\
                .append(QML_MOUSE_AREA_ON_ENTERED_END)\
                .append(QML_MOUSE_AREA_ON_EXITED_BEGIN)\
                    .append(QML_STYLE_INDENT).append(pTimerID_1).append(QML_TOOLTIP_TIMER_STOP)\
                    .append(QML_STYLE_INDENT).append(pRectangleID).append(QML_MOUSE_AREA_RECTANGLE_VISIBLE_FALSE)\
                .append(QML_MOUSE_AREA_ON_EXITED_END)\
                .append(QML_MOUSE_AREA_ON_POSITION_CHANGED_BEGIN)\
                        .append(QML_STYLE_INDENT).append(pTimerID_1).append(QML_TOOLTIP_TIMER_RESTART)\
                        .append(QML_STYLE_INDENT).append(pRectangleID).append(QML_MOUSE_AREA_RECTANGLE_VISIBLE_FALSE)\
                .append(QML_MOUSE_AREA_ON_POSITION_CHANGED_END).append(QML_STYLE_INDENT)\
                .append(QML_MOUSE_AREA_ON_PRESSED_BEGIN)\
                        .append(QML_STYLE_INDENT).append(pTextAreaID).append(QML_TEXT_AREA_FORCE_FOCUS)\
                .append(QML_MOUSE_AREA_ON_PRESSED_END)\
            .append(QML_MOUSE_AREA_END);
}

void CQMLMarkupHelper::getTableViewColumn(StringBuffer &strQML, const char* colTitle, const char *pRole)
{
    assert(colTitle != NULL);
    assert(pRole != NULL);

    if (colTitle != NULL && pRole != NULL)
    {
        strQML.append(QML_TABLE_VIEW_COLUMN_BEGIN).append(QML_TABLE_VIEW_COLUMN_TITLE_BEGIN).append(colTitle).append(QML_TABLE_VIEW_COLUMN_TITLE_END);
        strQML.append(QML_ROLE_BEGIN).append(pRole).append(QML_ROLE_END);
        strQML.append(QML_TABLE_VIEW_COLUMN_END);
        DEBUG_MARK_QML;
    }
}

unsigned CQMLMarkupHelper::getRandomID(StringBuffer *pID)
{
    Owned<IRandomNumberGenerator> random = createRandomNumberGenerator();
    random->seed(get_cycles_now());

    unsigned int retVal =  (random->next() % UINT_MAX);

    if (pID != NULL)
    {
        pID->trim();
        pID->append(retVal);
    }

    return retVal;
}

bool CQMLMarkupHelper::isTableRequired(const CAttribute *pAttrib)
{
    assert(pAttrib != NULL);

    if (pAttrib == NULL)
        return false;

    const char* pViewType = NULL;
    const char* pColumnIndex = NULL;
    const char* pXPath = NULL;
    const CXSDNodeBase *pGrandParentNode = pAttrib->getConstAncestorNode(2);
    const CElement *pNextHighestElement = dynamic_cast<const CElement*>(pAttrib->getParentNodeByType(XSD_ELEMENT));

    if (pAttrib->getAnnotation() != NULL && pAttrib->getAnnotation()->getAppInfo() != NULL)
    {
        const CAppInfo *pAppInfo = NULL;
        pAppInfo = pAttrib->getAnnotation()->getAppInfo();
        pViewType = pAppInfo->getViewType();
        pColumnIndex = (pAppInfo->getColIndex() != NULL && pAppInfo->getColIndex()[0] != 0) ? pAppInfo->getColIndex() : NULL;
        pXPath = (pAppInfo->getXPath() != NULL && pAppInfo->getXPath()[0] != 0) ? pAppInfo->getXPath() : NULL;
    }

    if ((pColumnIndex != NULL && pColumnIndex[0] != 0) || (pXPath != NULL && pXPath[0] != 0) || \
            (pGrandParentNode != NULL && pGrandParentNode->getNodeType() != XSD_ATTRIBUTE_GROUP && pGrandParentNode->getNodeType() != XSD_COMPLEX_TYPE && pGrandParentNode->getNodeType() != XSD_ELEMENT) || \
            (pGrandParentNode->getNodeType() == XSD_ELEMENT && stricmp( (dynamic_cast<const CElement*>(pGrandParentNode))->getMaxOccurs(), "unbounded") == 0) || \
            (pNextHighestElement != NULL && pNextHighestElement->getMaxOccurs() != NULL && pNextHighestElement->getMaxOccurs()[0] != 0))
        return true;
    else
        return false;
}

void CQMLMarkupHelper::buildAccordionStart(StringBuffer &strQML, const char * title, const char * altTitle, int idx)
{
    assert(title != NULL);

    StringBuffer result;

    result.append("AccordionItem {\n");
    result.append(CQMLMarkupHelper::printProperty("title",title));

    if(strlen(altTitle))
        result.append(CQMLMarkupHelper::printProperty("altTitle",altTitle));

    char * index = NULL;
    result.append("contentModel: VisualItemModel {\n");

    strQML.append(result);
}
void CQMLMarkupHelper::buildColumns(StringBuffer &strQML, StringArray &roles, StringArray &titles)
{  
    /*
    Sample QML for Column Instance
    columnNames: [{role:"key",title:"key"}, {role:"value",title:"value"},{role:"alphabet",title:"Alpha"}]
     */
    StringBuffer result;
    assert(roles.length() == titles.length());
    result.append("columnNames: [");
    for(int i = 0; i < roles.length(); i++)
    {
        const StringBuffer temprole = CQMLMarkupHelper::printProperty("role", roles[i], false);
        const StringBuffer temptitle = CQMLMarkupHelper::printProperty("title", titles[i], false);
        result.appendf("{%s,%s}", temprole.str(), temptitle.str());

        if(i != roles.length() - 1)
            result.append(",");
    }
    result.append("]\n");

    strQML.append(result);
}

void CQMLMarkupHelper::buildRole(StringBuffer &strQML, const char * role, StringArray &values, const char * type, const char * tooltip, const char * placeholder)
{
    StringBuffer result;
    StringBuffer escapedToolTip = tooltip;

    escapedToolTip.replaceString("\"","\\\"");

    StringBuffer escapedPlaceholder = placeholder;

    escapedPlaceholder.replaceString("\"","\\\"");
    result.append(role).append(": ListElement { \n").append("value:[");

    for(int i = 0; i < values.length(); i++)
    {
        result.append("ListElement {value: \"");
        result.append(values[i]);
        result.append("\"}");

        if(i != values.length()-1)
            result.append(",");
    }

    result.append("]\n");
    result.append(CQMLMarkupHelper::printProperty("type",type));
    result.append(CQMLMarkupHelper::printProperty("tooltip",escapedToolTip));
    result.append(CQMLMarkupHelper::printProperty("placeholder",escapedPlaceholder));
    result.append("}");
    strQML.append(result);
}

const StringBuffer CQMLMarkupHelper::printProperty(const char * property, const char * value, const bool newline)
{
    StringBuffer result;
    result.appendf("%s: \"%s\"",property,value);

    if(newline)
        result.append("\n");

    return result;
}
