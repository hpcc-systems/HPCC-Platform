define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/on",
    "dojo/dom-class",
    "dojo/topic",

    "dijit/registry",
    "dijit/form/ToggleButton",
    "dijit/ToolbarSeparator",

    "dgrid/tree",
    "dgrid/extensions/ColumnHider",

    "src/WsELK",
    "src/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "hpcc/_TabContainerWidget",
    "src/ESPUtil",
    "src/Utility",

    "dojo/text!../templates/LogVisualizationWidget.html"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, domClass, topic,
    registry, ToggleButton, ToolbarSeparator,
    tree, ColumnHider,
    WsELK, ESPWorkunit, DelayLoadWidget, _TabContainerWidget, ESPUtil, Utility,
    template) {
        return declare("LogVisualizationWidget", [_TabContainerWidget], {
            i18n: nlsHPCC,

        });
    });
