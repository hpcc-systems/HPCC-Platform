/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-form",
    "dojo/dom-attr",
    "dojo/request/iframe",
    "dojo/dom-class",
    "dojo/query",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPWorkunit",
    "hpcc/ESPRequest",
    "hpcc/TargetSelectWidget",
    "hpcc/ECLSourceWidget",
    "hpcc/LogWidget",
    "hpcc/WsTopology",

    "dojo/text!../templates/TopologyDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Form",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/ValidationTextBox",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/form/TextBox",
    "dijit/Dialog",
    "dijit/form/SimpleTextarea",

    "hpcc/TableContainer"
], function (declare, lang, i18n, nlsHPCC, dom, domConstruct, domForm, domAttr, iframe, domClass, query, Memory, Observable,
                registry,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                _TabContainerWidget, ESPWorkunit, ESPRequest, TargetSelectWidget, ECLSourceWidget, LogWidget, WsTopology,
                template) {
    return declare("TopologyDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "TopologyDetailsWidget",
        i18n: nlsHPCC,

        summaryWidget: null,
        configurationWidget: null,
        configurationWidgetLoaded: false,
        logsWidget: null,
        logsWidgetLoaded: false,


        postCreate: function (args) {
            this.inherited(arguments);
            this.details = registry.byId(this.id + "_Details");
            this.configurationWidget = registry.byId(this.id + "_Configuration");
            this.logsWidget = registry.byId(this.id + "_Logs");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_TopologyDetails;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
        },

        //  Implementation  ---
        init: function (params) {
            if (this.params.__hpcc_id === params.__hpcc_id)
                return;

            this.initalized = false;
            this.widget._Summary.__hpcc_initalized = false;
            this.widget._Configuration.__hpcc_initalized = false;
            this.widget._Logs.__hpcc_initalized = false;
            this.inherited(arguments);
            if (this.params.hasConfig()) {
                this.widget._Configuration.set("disabled", false);
            } else {
                this.widget._Configuration.set("disabled", true);
                if (this.getSelectedChild().id === this.widget._Configuration.id) {
                    this.selectChild(this.widget._Summary.id);
                }
            }
            if (this.params.hasLogs()) {
                this.widget._Logs.set("disabled", false);
            } else {
                this.widget._Logs.set("disabled", true);
                if (this.getSelectedChild().id === this.widget._Logs.id) {
                    this.selectChild(this.widget._Summary.id);
                }
            }
            this.initTab();
        },

        initTab: function () {
            var context = this;
            var currSel = this.getSelectedChild();
            if (currSel.id == this.widget._Summary.id && !this.widget._Summary.__hpcc_initalized) {
                this.widget._Summary.__hpcc_initalized = true;
                var table = domConstruct.create("table", {});
                for (var key in this.params.__hpcc_treeItem) {
                    if (this.params.__hpcc_treeItem.hasOwnProperty(key) && !(this.params.__hpcc_treeItem[key] instanceof Object)) {
                        if (key.indexOf("__") !== 0) {
                            switch (key) {
                                case "Port":
                                case "Path":
                                case "ProcessNumber":
                                break;
                            default:
                                var tr = domConstruct.create("tr", {}, table);
                                domConstruct.create("td", {
                                    innerHTML: "<b>" + key + ":&nbsp;&nbsp;</b>"
                                }, tr);
                                domConstruct.create("td", {
                                    innerHTML: this.params.__hpcc_treeItem[key]
                                }, tr);
                            }
                        }
                    }
                }
                var tpMachine = null;
                if (this.params.__hpcc_treeItem.__hpcc_type === "TpMachine") {
                    tpMachine = this.params.__hpcc_treeItem;
                } else if (this.params.__hpcc_parentNode && this.params.__hpcc_parentNode.__hpcc_treeItem.__hpcc_type === "TpMachine") {
                    tpMachine = this.params.__hpcc_parentNode.__hpcc_treeItem;
                };
                var tpBinding = null;
                if (this.params.__hpcc_treeItem.__hpcc_type === "TpBinding") {
                    tpBinding = this.params.__hpcc_treeItem;
                } else if (this.params.__hpcc_parentNode && this.params.__hpcc_parentNode.__hpcc_treeItem.__hpcc_type === "TpBinding") {
                    tpBinding = this.params.__hpcc_parentNode.__hpcc_treeItem;
                };
                if (tpBinding && tpMachine) {
                    var tr = domConstruct.create("tr", {}, table);
                    domConstruct.create("td", {
                        innerHTML: "<b>URL:&nbsp;&nbsp;</b>"
                    }, tr);
                    var td = domConstruct.create("td", {
                    }, tr);
                    var url = tpBinding.Protocol + "://" + tpMachine.Netaddress + ":" + tpBinding.Port + "/";
                    domConstruct.create("a", {
                        href: url,
                        innerHTML: url
                    }, td);
                }
                this.details.setContent(table);
            } else if (currSel.id === this.widget._Configuration.id && !this.widget._Configuration.__hpcc_initalized) {
                this.widget._Configuration.__hpcc_initalized = true;
                this.params.getConfig().then(function (response) {
                    var xml = context.formatXml(response);
                    context.widget._Configuration.init({
                        sourceMode: "xml"
                    });
                    context.widget._Configuration.setText(xml);
                });
            } else if (currSel.id == this.widget._Logs.id && !this.widget._Logs.__hpcc_initalized) {
                this.widget._Logs.__hpcc_initalized = true;
                this.widget._Logs.init(this.params);
            }
        },

        updateInput: function (name, oldValue, newValue) {
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            }
        },

        refreshActionState: function () {
        }
    });
});