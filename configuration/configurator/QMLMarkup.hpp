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

#ifndef _QMLMARKUP_HPP_
#define _QMLMARKUP_HPP_

//#include "jarray.hpp"
class StringBuffer;
class CAttribute;
class StringArray;
class String;
   

static const char* QML_TABLE_DATA_MODEL(" tableDataModel\n");
static const char* QML_PROPERTY_STRING_TABLE_BEGIN("\
         property string table: ");//tableDataModel.setActiveTable(\"");
static const char* QML_PROPERTY_STRING_TABLE_PART_1(".setActiveTable(\"");
static const char* QML_PROPERTY_STRING_TABLE_END("\")\n");

static const char* QML_APP_DATA_GET_VALUE_BEGIN(" ApplicationData.getValue(\"");
static const char* QML_APP_DATA_GET_VALUE_END("\")\n");
static const char* QML_APP_DATA_SET_VALUE_BEGIN(" ApplicationData.setValue(\"");
static const char* QML_APP_DATA_SET_VALUE_END(".text)\n");
static const char* QML_APP_DATA_GET_INDEX_BEGIN(" ApplicationData.getIndex(\"");
static const char* QML_APP_DATA_GET_INDEX_END("\")\n");
static const char* QML_APP_DATA_SET_INDEX_BEGIN(" ApplicationData.setIndex(\"");
static const char* QML_APP_DATA_SET_INDEX_END(".currentIndex)\n");

static const char* QML_ON_ACCEPTED("\n\
        onAccepted: ");

static const char* QML_MODEL("\n\
       model: ");
static const char* QML_ROLE_BEGIN("\
            role: \"");
static const char* QML_ROLE_END("\"\n");

static const char* QML_ON_CURRENT_INDEX_CHANGED("\n\
            onCurrentIndexChanged: ");

static const char* QML_START("\
 import QtQuick 2.1\n\
 import QtQuick.Controls 1.2\n\
 import QtQuick.Controls.Styles 1.0\n\
 import QtQuick.Particles 2.0\n\
 import QtQuick.Layouts 1.0\n\
 \n\
    Item {\n\
     id: root\n\
     width: 900\n\
     height: 700\n\
");

static const char* QML_VERTICAL_SCROLL_BAR("\n\
           ScrollBar {\n\
               id: verticalScrollBar\n\
               width: 12; height: view.height-12\n\
               anchors.right: view.right\n\
               opacity: 0\n\
               orientation: Qt.Vertical\n\
               position: view.visibleArea.yPosition\n\
               pageSize: view.visibleArea.heightRatio\n\
                    }\n");

static const char* QML_HORIZONTAL_SCROLL_BAR("\n\
           ScrollBar {\n\
              id: horizontalScrollBar\n\
              width: view.width-12; height: 12\n\
              anchors.bottom: view.bottom\n\
              opacity: 0\n\
              orientation: Qt.Horizontal\n\
              position: view.visibleArea.xPosition\n\
              pageSize: view.visibleArea.widthRatio\n\
                    }\n");

static const char* QML_FLICKABLE_BEGIN("\n\
           Flickable { id: view \n\
                       anchors.fill: parent\n\
                       contentWidth: 512\n\
                       /*contentHeight: 24\n*/\
                       ");

static const char* QML_FLICKABLE_HEIGHT("\n\
                       contentHeight: ");

static const char* QML_FLICKABLE_END("\n\
                        } // QML_FLICKABLE_END\n");

static const char* QML_SCROLL_BAR_TRANSITIONS("\n\
           states: State {\n\
               name: \"ShowBars\"\n\
               when: view.movingVertically || view.movingHorizontally\n\
               PropertyChanges { target: verticalScrollBar; opacity: 1 }\n\
               PropertyChanges { target: horizontalScrollBar; opacity: 1 }\n\
           }\n\
           transitions: Transition {\n\
               NumberAnimation { properties: \"opacity\"; duration: 400 }\n\
           }\n");

//static const char* QML_HORIZONTAL_SCROLL_BAR_TRANSITION();

static const char* QML_END("\
}\n\
");

static const char* QML_TEXT_FIELD_STYLE("\
property Component textfieldStyle: TextFieldStyle {\n\
  background: Rectangle {\n\
      implicitWidth: columnWidth\n\
      implicitHeight: 22\n\
      color: \"#f0f0f0\"\n\
      antialiasing: true\n\
      border.color: \"gray\"\n\
      radius: height/2\n\
      Rectangle {\n\
          anchors.fill: parent\n\
          anchors.margins: 1\n\
          color: \"transparent\"\n\
          antialiasing: true\n\
          border.color: \"#aaffffff\"\n\
          radius: height/2\n\
      }\n\
  }\n\
}n\
}\n\
");

static const char* QML_TAB_VIEW_BEGIN("\n\
    TabView {\n\
        Layout.row: 5\n\
        Layout.columnSpan: 3\n\
        Layout.fillWidth: true\n\
        implicitWidth: 530\n\
        /*implicitHeight: 1600*/\n\
");
static const char* QML_TAB_VIEW_HEIGHT("\
        implicitHeight: ");

static const char* QML_TAB_VIEW_END("\
    } // QML_TAB_VIEW_END");


static const char* QML_TAB_VIEW_STYLE("\
    style: TabViewStyle {\n\
                frameOverlap: 1\n\
                tab: Rectangle {\n\
//                    color: styleData.selected ? \"steelblue\" :\"lightsteelblue\"\n\
                    color: styleData.selected ? \"#98A9B1\" :\"lightsteelblue\"\n\
//                    border.color:  \"steelblue\"\n\
                    border.color:  \"#0E3860\"\n\
                    implicitWidth: Math.max(text.width + 4, 80)\n\
                    implicitHeight: 20\n\
                    radius: 2\n\
                    Text {\n\
                        id: text\n\
                        anchors.centerIn: parent\n\
                        text: styleData.title\n\
//                        color: styleData.selected ? \"white\" : \"black\"\n\
                          color: styleData.selected ? \"#0E3860\" : \"black\" \n\
                    }\n\
                }\n\
//                frame: Rectangle { color: \"steelblue\" }\n\
                frame: Rectangle { color: \"#98A9B1\" }\n\
      }\n");

static const char* QML_TAB_BEGIN("\
    Tab {\n");

static const char* QML_TAB_TITLE_BEGIN("\
         title: \"");
static const char* QML_TAB_TITLE_END("\"\n");
static const char* QML_TAB_END("\
    } // QML_TAB_END\n");

static const char* QML_TAB_TEXT_STYLE("\
\n\
          property Component textfieldStyle: TextFieldStyle {\n\
              background: Rectangle {\n\
                  implicitWidth: columnWidth\n\
                  implicitHeight: 22\n\
                  color: \"#f0f0f0\"\n\
                  antialiasing: true\n\
                  border.color: \"gray\"\n\
                  radius: height/2\n\
                  Rectangle {\n\
                      anchors.fill: parent\n\
                      anchors.margins: 1\n\
                      color: \"transparent\"\n\
                      antialiasing: true\n\
                      border.color: \"#aaffffff\"\n\
                      radius: height/2\n\
                  }\n\
              }\n\
          }\n\
\n\
");


static const char* QML_COLOR_BEGIN("\
        color: \"");

static const char* QML_COLOR_END("\"");

static const char* QML_ROW_BEGIN("\
        Row {\n");

static const char* QML_ROW_END("\n\
        } // QML_ROW_END\n");

static const char* QML_RECTANGLE_LIGHT_STEEEL_BLUE_BEGIN("\
         Rectangle {\n\
            color: \"lightsteelblue\"\n");

static const char* QML_RECTANGLE_DEFAULT_MIGUEL_1_BEGIN("\
         Rectangle {\n\
            color: \"#0E3860\"\n");

static const char* QML_RECTANGLE_DEFAULT_COLOR_SCHEME_1_BEGIN(QML_RECTANGLE_DEFAULT_MIGUEL_1_BEGIN);

static const char* QML_RECTANGLE_BEGIN("\n\
         Rectangle {\n\
                     ");
static const char* QML_RECTANGLE_END("\n\
            width: childrenRect.width\n\
            height: childrenRect.height\n\
          } // QML_RECTANGLE_END \n");

static const char* QML_RECTANGLE_LIGHT_STEEEL_BLUE_END(QML_RECTANGLE_END);

static const char* QML_TEXT_BEGIN("\
      Row {\n\
        Rectangle {\n\
          color: \"lightsteelblue\"\n\
          Text {\n\
               width: 275\n\
               height: 25\n\
               verticalAlignment: Text.AlignVCenter\n\
               horizontalAlignment: Text.AlignHCenter\n\
               text: \"");

static const char* QML_TEXT_END("\"\n\
            }\n\
        width: childrenRect.width\n\
        height: childrenRect.height\n\
         } // QML_TEXT_END\n");

static const char* QML_TEXT_BEGIN_2("\
          Text {\n\
//                 color: \"#98A9B1\"\n\
                 color: \"lightgray\"\n\
                 font.pointSize: 14\n\
                 width: 275\n\
                 height: 25\n\
                 verticalAlignment: Text.AlignVCenter\n\
                 horizontalAlignment: Text.AlignHLeft\n\
                 text: ");

static const char* QML_TEXT_END_2("\n\
           } // QML_TEXT_END_2 \n");

static const char* QML_TEXT_FIELD_PLACE_HOLDER_TEXT_BEGIN("\n\
        placeholderText: ");

static const char* QML_TEXT_FIELD_PLACE_HOLDER_TEXT_END("\n");

static const char* QML_TEXT_FIELD_BEGIN("\
        TextField {\n\
        style: TextFieldStyle {\n\
                textColor: \"black\"\n\
//                textColor: \"#0E3860\"\n\
        background: Rectangle {\n\
             radius: 2\n\
             implicitWidth: 250\n\
             implicitHeight: 25\n\
             border.color: \"#333\"\n\
             border.width: 1\n\
            }\n\
        }\n\
        horizontalAlignment: Text.AlignLeft\n\
        implicitWidth: 250\n\
        text: ");

static const char* QML_TEXT_FIELD_ID_BEGIN("\
        id: ");
static const char* QML_TEXT_FIELD_ID_END("\n");

static const char* QML_TEXT_FIELD_END("\n\
          } // QML_TEXT_FIELD_END\n");

static const char* QML_LIST_MODEL_BEGIN("\
              model: ListModel {\n");


static const char* QML_LIST_MODEL_END("\
                }\n");

static const char* QML_COMBO_BOX_BEGIN("\
    ComboBox {\n\
        implicitWidth: 250\n\
        id:\
");

static const char* QML_COMBO_BOX_CURRENT_INDEX("\
           currentIndex: ");

static const char* QML_COMBO_BOX_END("\
              }\n");

static const char* QML_LIST_ELEMENT_BEGIN("\
                ListElement { text: \"");

static const char* QML_LIST_ELEMENT_END("\" }\n");

static const char* QML_GRID_LAYOUT_BEGIN("\
          GridLayout {\n\
              rowSpacing: 1\n\
              columnSpacing: 1\n\
              columns: 1\n\
              flow: GridLayout.LeftToRight\n");

static const char* QML_GRID_LAYOUT_BEGIN_1("\
         GridLayout {\n\
             rowSpacing: 1\n\
             columnSpacing: 1\n\
             columns: 1\n\
             flow: GridLayout.LeftToRight\n");

static const char* QML_GRID_LAYOUT_END("\
          } // QML_GRID_LAYOUT_END");

static const char* QML_TOOLTIP_TEXT_BEGIN("\
\n\
        Rectangle {\n\
          color: \"lightblue\"\n\
          x: parent.x-275\n\
          y: parent.y-50\n\
          width: 250\n\
          height: 50\n\
          visible: false\n\
          radius: 5\n\
          smooth: true\n\
          id: ");

static const char* QML_TOOLTIP_TEXT_PART_1("\n\
        \n\
          Text {\n\
            verticalAlignment: Text.AlignVCenter\n\
            horizontalAlignment: Text.AlignHCenter\n\
            visible: true\n\
            width: 250\n\
            height: 50\n\
            wrapMode: Text.WordWrap\n\
            font.pointSize: 8\n\
            text: \"");

static const char* QML_TOOLTIP_TEXT_PART_2("\"\n\
                          }\n\
                       }\n");

static const char* QML_TOOLTIP_TIMER_BEGIN("\n\
     Timer {\n\
         interval: 2000\n\
         repeat: false\n\
         running: false\n");

static const char* QML_TOOLTIP_TIMER_END("\n\
     }");

static const char* QML_TOOLTIP_TIMER_ON_TRIGGERED_BEGIN("\n\
         onTriggered: {\n");

static const char* QML_TOOLTIP_TIMER_ON_TRIGGERED_END("\
          console.log(\"timer triggered\");\n\
         }\n");


static const char* QML_TOOLTIP_TIMER_RECTANGLE_APPEND_TRUE(".visible = true;\n");
static const char* QML_TOOLTIP_TIMER_RECTANGLE_APPEND_FALSE(".visible = false;\n");
static const char* QML_TOOLTIP_TIMER_MOUSE_AREA_APPEND_TRUE(".enabled = true;\n");
static const char* QML_TOOLTIP_TIMER_MOUSE_AREA_APPEND_FALSE(".enabled = false;\n");
static const char* QML_TOOLTIP_TIMER_TIMER_APPEND_START(".start();\n");
static const char* QML_TOOLTIP_TIMER_ID("\
        id: ");

static const char* QML_TOOLTIP_TIMER_STOP(".stop();\n");
static const char* QML_TOOLTIP_TIMER_RESTART(".restart();\n");

static const char* QML_MOUSE_AREA_BEGIN("\n\
     MouseArea {\n\
        height: 25\n\
        width: 250\n\
        visible: true\n\
        hoverEnabled: true;\n\
        id: ");

static const char* QML_MOUSE_AREA_END("\
    }\n");

static const char* QML_MOUSE_AREA_ID_APPEND("\n");

static const char* QML_MOUSE_AREA_ON_ENTERED_BEGIN("\
            onEntered: {\n\
            //console.log(\"entered mouse area\");\
                         \n");

static const char* QML_MOUSE_AREA_ON_ENTERED_END("\
             }\n");

static const char* QML_MOUSE_AREA_ON_EXITED_BEGIN("\
            onExited: {\n\
            //console.log(\"exited mouse area\");\
                        \n");

static const char* QML_MOUSE_AREA_ON_EXITED_END("\
            }\n");


static const char* QML_MOUSE_AREA_TIMER_APPEND(".start();\n\
            }\n\
            onExited:  {\n\
            //console.log(\"exited mouse area\");\n\
                         ");

static const char* QML_MOUSE_AREA_RECTANGLE_APPEND(".visible = false;\n\
        }\n");

static const char* QML_MOUSE_AREA_RECTANGLE_VISIBLE_FALSE(".visible = false;\n");

static const char* QML_MOUSE_AREA_ON_POSITION_CHANGED_BEGIN("\
            onPositionChanged: {\n");

static const char* QML_MOUSE_AREA_ON_POSITION_CHANGED_END("\n\
            //console.log(\"onPositionChanged\");\n\
            }");


static const char* QML_MOUSE_AREA_ON_PRESSED_BEGIN("\n\
            onPressed: {\n");

static const char* QML_MOUSE_AREA_ON_PRESSED_END("\
                //console.log(\"mouse area pressed\");\n\
            }\n");


static const char* QML_MOUSE_AREA_ENABLE("enable = true\n");

static const char* QML_TEXT_AREA_FORCE_FOCUS(".forceActiveFocus()\n");

static const char* QML_STYLE_INDENT("\t\t\t");

static const char* QML_STYLE_NEW_LINE("\n");

static const char* QML_TOOLTIP_TEXT_END("\
    }\n");

static const char* QML_TABLE_VIEW_BEGIN("\
    TableView {\n\
        width: 800\n\
        Layout.preferredHeight: 200\n\
        Layout.preferredWidth: 800\n");

static const char* QML_TABLE_VIEW_END("\n\
    } // QML_TABLE_VIEW_END\n");

static const char* QML_TABLE_VIEW_COLUMN_BEGIN("\
        TableViewColumn {\n\
            width: 120\n");

static const char* QML_TABLE_VIEW_COLUMN_END("\
        }// QML_TABLE_VIEW_COLUMN_END\n");

static const char* QML_TABLE_VIEW_COLUMN_TITLE_BEGIN("\
            title: \"");

static const char* QML_TABLE_VIEW_COLUMN_TITLE_END("\"\n");

// New Constants
static const char* QML_FILE_START("\
import QtQuick 2.1                                  \n\
import QtQuick.Controls 1.2                         \n\
import QtQuick.Controls.Styles 1.0                  \n\
import QtQuick.Particles 2.0                        \n\
import QtQuick.Layouts 1.0                          \n\
Item {                                              \n\
    id: gRoot                                       \n\
    width: 900                                      \n\
    height: 700                                     \n\
    property alias root: gRoot                      \n\
    ScrollView{                                     \n\
        id: scrollRoot                              \n\
        anchors.fill: parent                        \n\
        ListView{                                   \n\
            id: listRoot                            \n\
            property int nest: 0                    \n\
            property int visibility: 0              \n\
            anchors.fill: parent                    \n\
            spacing: 4                              \n\
            model: configModel                      \n\
            opacity: !visibility                    \n\
            transitions: Transition {               \n\
                NumberAnimation {                   \n\
                    properties: 'opacity'           \n\
                    easing.type: Easing.InOutQuad   \n\
                    duration: 1000                  \n\
                }                                   \n\
            }                                       \n\
        }                                           \n\
    }                                               \n\
    VisualItemModel {                               \n\
        id: configModel");
static const char * QML_DOUBLE_END_BRACKET("}\n}\n");
// Always start content before column
static const char * QML_TABLE_START("Table{                 \n");
static const char * QML_TABLE_CONTENT_START("tableContent: ListModel {                      \n");
static const char * QML_TABLE_ROW_START("ListElement {");
static const char * QML_TABLE_ROW_END("}\n");
static const char * QML_TABLE_CONTENT_END("}\n");
static const char * QML_TABLE_END("}\n");
// Table ends can be replaced with DOUBLE_END_BRACKET in appropriate situations (if buildColumns are done before Content)

// NewConstants end;
class CQMLMarkupHelper
{
public:
    // New Helper Functions
    static void buildAccordionStart(StringBuffer &strQML, const char * title, const char * altTitle = "", int idx = -1);
    // End Accordion with QML_DOUBLE_END_BRACKET
    static void buildColumns(StringBuffer &strQML, StringArray &roles, StringArray &titles);
    static void buildRole(StringBuffer &strQML, const char * role, StringArray &values, const char * type = "text", const char * tooltip = "", const char * placeholder = "");
    static const StringBuffer printProperty(const char * property, const char * value, const bool newline = true);
    // NewHelperFunctions end;
    static void getTabQML(StringBuffer &strQML, const char *pName);
    static void getComboBoxListElement(const char* pLabel, StringBuffer &strID, const char* pDefault = "");
    static void getToolTipQML(StringBuffer &strQML, const char *pToolTip, const char* pTextAreaID);
    static void getToolTipRectangle(StringBuffer &strQML, const char *pToolTip, const char *pRectangleID);
    static void getToolTipTimer(StringBuffer &strQML, const char *pToolTip, const char *pRectangleID, const char* pTimerID_1, const char* pTimerID_2, const char *pMouseAreaID);
    static void getToolMouseArea(StringBuffer &strQML, const char *pToolTip, const char *pRectangleID, const char* pTimerID_1, const char* pTimerID_2, const char *pMouseAreaID, const char* pTextAreaID);
    static void getTableViewColumn(StringBuffer &strQML, const char* colTitle, const char *pRole);
    static unsigned getRandomID(StringBuffer *pID = 0);
    static bool isTableRequired(const CAttribute *pAttrib);

    static int getImplicitHeight()
    {
        return CQMLMarkupHelper::glImplicitHeight;
    }

    static void setImplicitHeight(int val)
    {
        CQMLMarkupHelper::glImplicitHeight = val;
    }

protected:

    static int glImplicitHeight;
};

#endif // _QMLMARKUP_HPP_
