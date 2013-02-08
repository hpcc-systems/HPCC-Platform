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
    "dojo/dom",
    "dojo/on",
    "dojo/data/ObjectStore",
    "dojo/date",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/form/Select",

    "dijit/PopupMenuItem",
    "dijit/Dialog",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/Pagination",
    "dojox/grid/enhanced/plugins/IndirectSelection",
    "dojox/widget/Calendar",

    "hpcc/_TabContainerWidget",
    "hpcc/FileSpray",
    "hpcc/DropZonesWidget",

    "dojo/text!../templates/DropZoneWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/RadioButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/form/DateTextBox"

], function (declare, dom, on, ObjectStore, date, Menu, MenuItem, MenuSeparator, Select, PopupMenuItem, Dialog,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry, EnhancedGrid, Pagination, IndirectSelection, Calendar,
                _TabContainerWidget, FileSpray, DropZonesWidget,
                template) {
    return declare("DropZoneWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "DropZoneWidget",
        legacyPane: null,
        legacyPaneLoaded: false,

        tabMap: [],

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.legacyPane = registry.byId(this.id + "_Legacy");
        },

        buildTabs: function (id, params) {
        },

        query: function (query, options) {
        },

        startup: function (args) {
            this.inherited(arguments);
            this.refreshActionState();
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        //  Hitched actions  ---
        getISOString: function () {
        },

        //  Implementation  ---
        init: function (params) {
            var context = this;
            FileSpray.GetDropZones({
                load: function (response) {
                    var firstTab = null;
                    for (var i = 0; i < response.length; ++i) {
                        var tab = context.ensurePane(context.id + "_dropZone" + i, {
                            netAddress: response[i].NetAddress
                        });
                        if (i == 0) {
                            firstTab = tab;
                        }
                    }
                    if (firstTab) {
                        context.selectChild(firstTab);
                    }
                }
            });
        },

        refreshGrid: function (args) {
        },

        refreshActionState: function () {
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel.id == this.id + "_Legacy") {
                if (!this.legacyPaneLoaded) {
                    this.legacyPaneLoaded = true;
                    this.legacyPane.set("content", dojo.create("iframe", {
                        src: "/FileSpray/DropZoneFiles",
                        style: "border: 0; width: 100%; height: 100%"
                    }));
                }
            } else {
                if (currSel && !currSel.initalized) {
                    currSel.init(currSel.params);
                }
            }
        },

        ensurePane: function (id, params) {
            var retVal = this.tabMap[id];
            if (!retVal) {
                var context = this;
                retVal = new DropZonesWidget({
                    id: id,
                    title: params.netAddress,
                    closable: true,
                    onClose: function () {
                        delete context.tabMap[id];
                        return true;
                    },
                    params: params
                });
                this.tabMap[id] = retVal;
                this.addChild(retVal, 0);
            }
            return retVal;
        },
    });
});