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
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/query",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/promise/all",

    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/ESPWorkunit",
    "hpcc/ESPQuery",
    "hpcc/WsWorkunits",
    "hpcc/_TabContainerWidget",
    "hpcc/QuerySetLogicalFilesWidget",
    "hpcc/QuerySetErrorsWidget",

    "dojo/text!../templates/QuerySetDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/form/CheckBox",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/TitlePane",

    "hpcc/WUDetailsWidget"

], function (declare, lang, dom, domAttr, domClass, query, Memory, Observable, all,
                registry,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                ESPWorkunit, ESPQuery, WsWorkunits, _TabContainerWidget, QuerySetLogicalFilesWidget, QuerySetErrorsWidget,
                template) {
    return declare("QuerySetDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "QuerySetDetailsWidget",
        
        query: null,

        initalized: false,
        summaryTab: null,
        summaryTabLoaded:false,
        errorsTab: null,
        errorsTabLoaded: false,
        graphsTab: null,
        graphsTabLoaded: false,
        logicalFilesTab: null,
        logicalFilesTabLoaded: false,
        superFilesTab: false,
        superFilesTabLoaded: null,
        workunitsTab: null,
        workunitsTabLoaded: false,
        loaded:false,

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryTab = registry.byId(this.id + "_Summary");
            this.errorsTab = registry.byId(this.id + "_Errors");
            this.graphsTab = registry.byId(this.id + "_Graphs");
            this.logicalFilesTab = registry.byId(this.id + "_QuerySetLogicalFiles");
            this.superFilesTab = registry.byId(this.id + "_QuerySetSuperFiles");
            this.workunitsTab = registry.byId(this.id + "_Workunit");
        },

        //  Hitched actions  ---
        _onSave: function (event) {
            var suspended = registry.byId(this.id + "Suspended").get("value");
            var activated = registry.byId(this.id + "Activated").get("value");
            var context = this;
            all({
                suspend: this.query.setSuspended(suspended),
                activate: this.query.setActivated(activated)
            });
        },
        _onDelete: function (event) {
            if (confirm('Delete selected workunits?')) {
                this.query.doDelete();
            }
        },
        _onRefresh: function () {
            this.query.refresh();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.query = ESPQuery.Get(params.Id);

            var context = this;
            var data = this.query.getData();
            for (var key in data) {
                this.updateInput(key, null, data[key]);
            }
            this.query.watch(function (name, oldValue, newValue) {
                context.updateInput(name, oldValue, newValue);
            });
            this.query.refresh();

            this.selectChild(this.summaryWidget, true);
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel.id == this.summaryTab.id && !this.summaryTabLoaded) {
                this.summaryTabLoaded = true;
            } else if (currSel.id == this.workunitsTab.id && !this.workunitsTabLoaded) {
                this.workunitsTabLoaded = true;
                this.workunitsTab.init({
                    Wuid: this.query.Wuid
                });
            } else if (currSel.id == this.errorsTab.id && !this.errorsTabLoaded) {
                this.errorsTabLoaded = true;
                this.errorsTab.init({
                    Query: this.query
                });
            } else if (currSel.id == this.graphsTab.id && !this.graphsTabLoaded) {
                this.graphsTabLoaded = true;
                this.graphsTab.init({
                    Query: this.query
                });
            } else if (currSel.id == this.logicalFilesTab.id && !this.logicalFilesTabLoaded) {
                this.logicalFilesTabLoaded = true;
                this.logicalFilesTab.init({
                    QueryId: this.query.Id,
                    QuerySet: this.query.QuerySet,
                    Query: this.query
                });
            }
        },

        updateInput: function (name, oldValue, newValue) {
           var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            } else {
                var domElem = dom.byId(this.id + name);
                if (domElem) {
                    switch (domElem.tagName) {
                        case "SPAN":
                        case "DIV":
                            domAttr.set(this.id + name, "innerHTML", newValue);
                            break;
                        case "INPUT":
                        case "TEXTAREA":
                            domAttr.set(this.id + name, "value", newValue);
                            break;
                        default:
                            alert(domElem.tagName);
                            break;
                    }
                }
            }
            if (name === "Wuid") {
                this.workunitsTab.set("title", newValue);
            }
            if (name === "Suspended") {
                dom.byId(this.id + "SuspendImg").src = newValue ? "img/suspended.png" : "img/unsuspended.png";
            }
            if (name === "Activated") {
                dom.byId(this.id + "ActiveImg").src = newValue ? "img/active.png" : "img/inactive.png";
            }

            else if (name === "CountGraphs" && newValue) {
                this.graphsTab.set("title", "Graphs " + "(" + newValue + ")");
            } else if (name === "graphs") {
                this.graphsTab.set("title", "Graphs " + "(" + newValue.length + ")");
                var tooltip = "";
                for (var i = 0; i < newValue.length; ++i) {
                    if (tooltip != "")
                        tooltip += "\n";
                    tooltip += newValue[i].Name;
                    if (newValue[i].Time)
                        tooltip += " " + newValue[i].Time;
                }
                this.graphsTab.set("tooltip", tooltip);
            }
            else if (name === "LogicalFiles") {
                if (lang.exists("Item.length", newValue)) {
                    this.logicalFilesTab.set("title", "Logical Files " + "(" + newValue.Item.length + ")");
                    var tooltip = "";
                    for (var i = 0; i < newValue.Item.length; ++i) {
                        if (tooltip != "")
                            tooltip += "\n";
                        tooltip += newValue.Item[i];
                    }
                    this.logicalFilesTab.set("tooltip", tooltip);
                }
            }
            else if (name === "Clusters") {
                if (lang.exists("ClusterQueryState.length", newValue)) {
                    this.errorsTab.set("title", "Errors / Status " + "(" + newValue.ClusterQueryState.length + ")");
                    var tooltip = "";
                    for (var i = 0; i < newValue.ClusterQueryState.length; ++i) {
                        if (tooltip != "")
                            tooltip += "\n";
                        tooltip += newValue.ClusterQueryState[i].Cluster + " (" + newValue.ClusterQueryState[i].State + ")";
                    }
                    this.errorsTab.set("tooltip", tooltip);
                }
            }
        }
    });
});
