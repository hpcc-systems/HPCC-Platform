#ifndef _DOJOJSMARKUP_HPP_
#define _DOJOJSMARKUP_HPP_

#include <climits>
#include "jstring.hpp"
#include "jutil.hpp"
#include "jdebug.hpp"


static const char* DJ_START("define([\n\
                                 \"dojo/_base/declare\",\n\
                                 \"dojo/dom\",\n\
                                 \"dojo/dom-construct\",\n\
                                 \"dojo/store/Memory\",\n\
\n\
                                 \"dojox/layout/TableContainer\",\n\
                                 \"dojox/grid/DataGrid\",\n\
\n\
                                 \"dgrid/Grid\",\n\
                                 \"dgrid/Keyboard\",\n\
                                 \"dgrid/Selection\",\n\
\n\
                                 \"dijit/form/ComboBox\",\n\
                                 \"dijit/form/MultiSelect\",\n\
                                 \"dijit/Tooltip\",\n\
                                 \"dijit/layout/BorderContainer\",\n\
                                 \"dijit/layout/TabContainer\",\n\
                                 \"dijit/layout/ContentPane\",\n\
                                 \"dijit/form/Form\",\n\
                                 \"dijit/form/TextBox\",\n\
                                 \"dijit/_TemplatedMixin\",\n\
                                 \"dijit/_WidgetsInTemplateMixin\",\n\
                                 \"dijit/form/Select\",\n\
                                 \"dijit/registry\",\n\
\n\
                                 \"hpcc/_TabContainerWidget\",\n\
\n\
                                 \"dojo/text!../templates/ConfiguratorWidget.html\"\n\
\n\
                             ], function (declare, dom, domConstruct, Memory,\n\
                                     TableContainer, DataGrid,\n\
                                     DGrid, Keyboard, Selection,\n\
                                     ComboBox, MultiSelect, Tooltip, BorderContainer, TabContainer, ContentPane, Form, TextBox,\n\
                                     _TemplatedMixin, _WidgetsInTemplateMixin, Select, registry,\n\
                                     _TabConterWidget,\n\
                                     template) {\n\
\n\
                                 return declare(\"ConfiguratorWidget\", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {\n\
                                     templateString: template,\n\
                                     baseClass: \"ConfiguratorWidget\",\n\
                                 name: \"ConfiguratorWidget\",\n\
\n\
\n\
                                     postCreate: function (args) {\n\
                                         this.inherited(arguments);\n\
                                     },\n\
\n\
                                     resize: function (args) {\n\
                                         this.inherited(arguments);\n\
                                     },\n\
\n\
                                     layout: function (args) {\n\
                                         this.inherited(arguments);\n\
                                     },\n\
\n\
                                     _initControls: function () {\n\
                                         var context = this;\n\
                                         this.targetSelectControl = registry.byId(this.id + \"TargetSelect\");\n\
                                     top.mystuff = this;\n\
                                     top.mystuff2 = registry.byId(\"tc1\");\n\
                                     top.registry = registry;\n\
                                     top.form = Form;\n\
\n\
");

static const char* DJ_FINISH("\n},\n        init: function (params) {\n\
                                         if (this.initalized)\n\
                                             return;\n\
                                         this.initalized = true;\n\
\n\
                                     this._initControls();\n\
                                     }\n\
                                 });\n\
                             });");




static const char* DJ_TAB_PART_1("\nvar cp = new ContentPane({\n\
title: \"");

static const char* DJ_TAB_PART_2("\",\n\
style: \"overflow: auto; width: 100%;\",\n\
doLayout: \"true\",\n\
id: \"");


static const char* DJ_TAB_PART_3("\", });\n if (bc != null) cp.addChild(bc);\nvar layout = [[]];\n");

static const char* DJ_TABLE_PART_1("\nvar tc = new dojox.layout.TableContainer(\n\
{ cols: 2,\n\
customClass : \"labelsAndValues\",\n\
\"labelWidth\" : \"175\" });\n");

static const char* DJ_TABLE_PART_2("\n\
if (cp != null)\n\
    cp.placeAt(\"stubTabContainer\");\n\
if (tc != null && cp != null)\n\
{\n\
    cp.addChild(tc);\n\
}\n\
var temp_cp = cp;\n\
cp = null;\n\
tc = null;\n\
bc = null;\n\
layout = [[]];\n\
");

static const char* DJ_TABLE_END("temp_cp = null;\n");

static const char* DJ_TABLE_ROW_PART_1("var txt = new dijit.form.TextBox({label: \"");
static const char* DJ_TABLE_ROW_PART_PLACE_HOLDER("\", placeHolder: \"");
static const char* DJ_TABLE_ROW_PART_ID_BEGIN("\", id: \"");
static const char* DJ_TABLE_ROW_PART_ID_END("\", /*style: { width: '400px' }*/});\n");
static const char* DJ_TABLE_ROW_PART_2("\"});\n\
if (txt != null && tc != null) tc.addChild(txt);");

static const char* DJ_ADD_CHILD("\nif (txt != null && tc != null) tc.addChild(txt);\n");

static const char* DJ_TOOL_TIP_BEGIN("\nvar mytip = new dijit.Tooltip({");
static const char* DJ_TOOL_TIP_CONNECT_ID_BEGIN(" connectId: [\"");
static const char* DJ_TOOL_TIP_CONNECT_ID_END("\"], ");
static const char* DJ_TOOL_TIP_LABEL_BEGIN("label: \"");
static const char* DJ_TOOL_TIP_LABEL_END("\"");
static const char* DJ_TOOL_TIP_END("});");


static const char* DJ_LAYOUT_CONCAT_BEGIN("\nif (typeof(layout) == 'undefined')\n\tvar layout = [[]];\nlayout[0] = layout[0].concat(");
static const char* DJ_LAYOUT_CONCAT_END(");\n");

static const char* DJ_LAYOUT_BEGIN("\nvar layout = [[]];\n");
static const char* DJ_LAYOUT_END("\nvar CustomGrid = declare([ DGrid, Keyboard, Selection ]);\n\
\n\
if (layout != null && layout[0].length != 0) {\n\
var grid = new CustomGrid({\n\
columns: layout[0],\n\
selectionMode: \"single\",\n\
cellNavigation: false\n\
});\n\
grid.startup();\n}\n\
var bc = new BorderContainer({\n\
style: \"height: 300px; width: 75%;\",\n\
region: \"left\"\n\
});\n\
if (layout != null && layout[0].length > 0)\n\
{\n\
if (cp != null) cp.addChild(bc);\n\
if (grid != null) bc.addChild(grid);\n\
grid = null;\n\
}\n");


static const char* DJ_GRID("\nvar CustomGrid = declare([ DGrid, Keyboard, Selection ]);\n\
\n\
if (layout != null && layout[0].length != 0) {\n\
var grid = new CustomGrid({\n\
columns: layout[0],\n\
selectionMode: \"single\",\n\
cellNavigation: false\n\
});\n}\
var bc = new BorderContainer({\n\
style: \"height: 300px; width: 75%;\",\n\
region: \"left\"\n\
});\n\
if (cp != null) cp.addChild(bc);\n\
if (grid != null)\n{\tbc.addChild(grid);\n\tgrid.startup();\n\tgrid = null; layout = null;}\n");

static const char* DJ_MEMORY_BEGIN(\
"\nvar cbStore = new Memory({\n\
  data: [");

static const char* DJ_MEMORY_END("]});\n");

static const char* DJ_MEMORY_ENTRY_NAME_BEGIN("{name: \"");
static const char* DJ_MEMORY_ENTRY_NAME_END("\", ");
static const char* DJ_MEMORY_ENTRY_ID_BEGIN(" id:\"");
static const char* DJ_MEMORY_ENTRY_ID_END("\"},\n\t");


static const char* DJ_COMBOX_BOX_BEGIN("var comboBox = new ComboBox({");
static const char* DJ_COMBOX_BOX_END("});\n\nif (typeof(comboBox) != 'undefined' && tc != null) tc.addChild(comboBox); comboBox = null;");

static const char* DJ_COMBO_BOX_ID_BEGIN("\nid: \"");
static const char* DJ_COMBO_BOX_ID_END("\",");
static const char* DJ_COMBO_BOX_NAME_BEGIN("\nname: \"");
static const char* DJ_COMBO_BOX_NAME_END("\",");
static const char* DJ_COMBO_BOX_VALUE_BEGIN("\nvalue: \"");
static const char* DJ_COMBO_BOX_VALUE_END("\",");
static const char* DJ_COMBO_BOX_STORE_BEGIN("\nstore: ");
static const char* DJ_COMBO_BOX_STORE_END(",");
static const char* DJ_COMBO_BOX_SEARCHATTR_BEGIN("\nsearchAttr: \"");
static const char* DJ_COMBO_BOX_SEARCHATTR_END("\",");
static const char* DJ_COMBO_BOX_LABEL_BEGIN("\nlabel: \"");
static const char* DJ_COMBO_BOX_LABEL_END("\",");
static const char* DJ_DIV_HEADING_BEGIN("if (cp != null) dojo.place(\"<div><H1>");
static const char* DJ_DIV_HEADING_END("</H1></div>\", cp.containerNode, cp.containerNode.length);\n");

static unsigned getRandomID(StringBuffer *pID = NULL)
{
    Owned<IRandomNumberGenerator> random = createRandomNumberGenerator();
    random->seed(get_cycles_now());

    unsigned int retVal =  (random->next() % UINT_MAX);

    if (pID != NULL)
    {
        pID->append(retVal);
    }

    return retVal;
}

static const char* createDojoComboBox(const char* pLabel, StringBuffer &strID, const char* pDefault = "")
{
    static StringBuffer strBuf;

    strID.clear().append("CBID");

    getRandomID(&strID);

    strBuf.clear();
    strBuf.appendf("%s %s%s%s %s%s%s %s%s%s %s%s%s %s%s%s %s", \
      DJ_COMBOX_BOX_BEGIN, \
      DJ_COMBO_BOX_ID_BEGIN, strID.str(), DJ_COMBO_BOX_ID_END, \
      DJ_COMBO_BOX_VALUE_BEGIN, pDefault, DJ_COMBO_BOX_NAME_END, \
      DJ_COMBO_BOX_STORE_BEGIN, "cbStore", DJ_COMBO_BOX_STORE_END, \
      DJ_COMBO_BOX_SEARCHATTR_BEGIN, "name", DJ_COMBO_BOX_SEARCHATTR_END, \
      DJ_COMBO_BOX_LABEL_BEGIN, pLabel ,DJ_COMBO_BOX_LABEL_END, \
      DJ_COMBOX_BOX_END);

    return strBuf.str();
}

static const char* createDojoColumnLayout(const char* pName, unsigned uFieldId, const char* pWidth = "100px")
{
    assert(pName != NULL);
    assert(pWidth != NULL);

    static StringBuffer strBuf;

    strBuf.clear();
    strBuf.appendf("{'name': '%s', 'field': '%s', 'width': '%s', 'id': 'COLID_%u'}", pName, pName, pWidth, uFieldId);

    return strBuf.str();
}



static void genTabDojoJS(StringBuffer &strJS, const char *pName)
{
    assert(pName != NULL);

    if (pName == NULL)
    {
        return;
    }

    StringBuffer id("X");

    getRandomID(&id);

    strJS.append(DJ_TAB_PART_1).append(pName).append(DJ_TAB_PART_2).append(id.str()).append(DJ_TAB_PART_3);
}

static void genTableRow(StringBuffer &strJS, const char* pName)
{

}


#endif // _DOJOJSMARKUP_HPP_
