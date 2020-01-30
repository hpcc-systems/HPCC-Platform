define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",

    "hpcc/_TabContainerWidget",

    "dojo/text!../templates/LogVisualizationWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/Toolbar",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/form/ToggleButton"
], function (declare, i18n, nlsHPCC,
    _TabContainerWidget) {
    return declare("LogVisualizationWidget", [_TabContainerWidget], {
        i18n: nlsHPCC
    });
});
