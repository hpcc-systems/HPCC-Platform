/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/date",
    "dojo/on",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",
    "dijit/Dialog",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",
    "dijit/Tooltip",

    "dgrid/Grid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/_TabContainerWidget",
    "hpcc/WsWorkunits",
    "hpcc/ESPUtil",
    "hpcc/ESPWorkunit",
    "hpcc/ESPRequest",
    "hpcc/WUDetailsWidget",
    "hpcc/TargetSelectWidget",

    "dojo/text!../templates/HPCCPlatformWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/StackContainer",
    "dijit/layout/StackController",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/RadioButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer",

    "hpcc/DFUQueryWidget",
    "hpcc/LZBrowseWidget",
    "hpcc/GetDFUWorkunitsWidget",
    "hpcc/WUQueryWidget"

], function (declare, lang, arrayUtil, dom, domClass, domForm, date, on,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry, Dialog, Menu, MenuItem, MenuSeparator, PopupMenuItem, Tooltip,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, WsWorkunits, ESPUtil, ESPWorkunit, ESPRequest, WUDetailsWidget, TargetSelectWidget,
                template) {
    return declare("HPCCPlatformWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "HPCCPlatformWidget",

        postCreate: function (args) {
            this.inherited(arguments);
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        //  Hitched actions  ---
        _onAbout: function(evt) {
        },

        //  Implementation  ---
        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;
            this.initTab();

            new Tooltip({
                connectId: ["stubStackController_stub_ECL"],
                label: "Workunits",
                position: ["below"]
             });

            new Tooltip({
                connectId: ["stubStackController_stub_DFU"],
                label: "DFU Workunits",
                position: ["below"]
            });

            new Tooltip({
                connectId: ["stubStackController_stub_LF"],
                label: "Logical Files",
                position: ["below"]
            });
            new Tooltip({
                connectId: ["stubStackController_stub_Queries"],
                label: "Targets",
                position: ["below"]
            });
            new Tooltip({
                connectId: ["stubStackController_stub_LZ"],
                label: "Landing Zones",
                position: ["below"]
            });

            new Tooltip({
                connectId: ["stubStackController_stub_OPS"],
                label: "Operations",
                position: ["below"]
            });
        },

   
        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.id + "_Queries") {
                    currSel.set("content", dojo.create("iframe", {
                        src: ESPRequest.getBaseURL() + "/WUQuerySets",
                        style: "border: 0; width: 100%; height: 100%"
                    }));
                    currSel.initalized = true;
                } else if (currSel.init) {
                    currSel.init(currSel.params);
                }
            }
        }
    });
});
