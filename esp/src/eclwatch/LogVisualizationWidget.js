define([
    "dojo/_base/declare",
    "src/nlsHPCC",

    "hpcc/_TabContainerWidget",

    "dojo/text!../templates/LogVisualizationWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/Toolbar",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/form/ToggleButton"
], function (declare, nlsHPCCMod,
    _TabContainerWidget) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("LogVisualizationWidget", [_TabContainerWidget], {
        i18n: nlsHPCC
    });
});
