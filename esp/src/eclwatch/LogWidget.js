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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/date",
    "dojo/on",
    "dojo/topic",

    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPBase",
    "hpcc/ESPUtil",
    "hpcc/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/FilterDropDownWidget",
    "hpcc/ECLSourceWidget",
    "hpcc/WsTopology",

    "dojo/text!../templates/LogWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/CheckBox",
    "dijit/form/RadioButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domClass, domForm, date, on, topic,
                registry, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                selector,
                _TabContainerWidget, ESPBase, ESPUtil, ESPWorkunit, DelayLoadWidget, TargetSelectWidget, FilterDropDownWidget, ECLSourceWidget, WsTopology,
                template) {
    return declare("LogWidget", [_TabContainerWidget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "LogWidget",
        i18n: nlsHPCC,

        logTab: null,
        logGrid: null,
        filter: null,
        clusterTargetSelect: null,
        stateSelect: null,

        postCreate: function (args) {
            this.inherited(arguments);
            var context = this;

            this.logTab = registry.byId(this.id + "_Log");
            this.logTargetSelect = registry.byId(this.id + "logTargetSelect");
            this.logTargetSelect.on("change", function (evt) {
                context.refreshGrid();
            });
            this.reverseButton = registry.byId(this.id + "Reverse");
            this.filter = registry.byId(this.id + "Filter");
            this.filter.on("clear", function (evt) {
                context._onFilterType();
                context.refreshGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshGrid();
            });
            this.rawText = registry.byId(this.id + "LogText");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_Log;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _doDownload: function (zip) {
            var base = new ESPBase();
            var name = this.params.getLogDirectory() + "/" + this.logTargetSelect.get("value");
            var type = "tpcomp_log";
            window.open(base.getBaseURL("WsTopology") + "/SystemLog?Name=" + name + "&Type=" + type + "&Zip=" + zip, "_blank");
        },

        _onDownloadText: function (args) {
            this._doDownload(1);
        },

        _onDownloadZip: function (args) {
            this._doDownload(2);
        },

        _onDownloadGZip: function (args) {
            this._doDownload(3);
        },

        //  Implementation  ---
        getFilter: function () {
            var retVal = this.filter.toObject();
            if (retVal.FirstRows) {
                retVal.FilterType = 1;
            } else if (retVal.LastRows) {
                retVal.FilterType = 5;
            } else if (retVal.LastHours) {
                retVal.FilterType = 2;
            } else if (retVal.StartDate) {
                if (retVal.StartDate[0] === "T") {
                    retVal.StartDate = retVal.StartDate.substring(1);
                }
                if (retVal.EndDate[0] === "T") {
                    retVal.EndDate = retVal.EndDate.substring(1);
                }
                retVal.FilterType = 6;
            } else {
                retVal.FilterType = 4;
            }
            return retVal;
        },

        //  Implementation  ---
        init: function (params) {
            if (this.params.__hpcc_id === params.__hpcc_id)
                return;

            this.initalized = false;
            this.inherited(arguments);

            this.logTargetSelect.reset();
            this.logTargetSelect.init({
                Logs: true,
                includeBlank: false,
                treeNode: this.params
            });

            this.initLogGrid();
            if (!this.rawText.initialized) {
                this.rawText.initialized = true;
                this.rawText.init({
                    mode: "text"
                });
            }
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id == this.logTab.id) {
                } else {
                    if (!currSel.initalized) {
                    }
                }
            }
        },

        initLogGrid: function () {
            var context = this;
            var store = new WsTopology.CreateTpLogFileStore();
            store.on("pageLoaded", function (page) {
                context.rawText.setText(page);
            });
            if (!this.logGrid) {
                this.logGrid = new declare([ESPUtil.Grid(true, true, { rowsPerPage: 500, pageSizeOptions: [500, 1000] })])({
                    store: store,
                    query: this.getFilter(),
                    columns: {
                        lineNo: { label: this.i18n.Line, width: 80 },
                        details: { label: this.i18n.Details, width: 1024 },
                        date: { label: this.i18n.Date, width: 100 },
                        time: { label: this.i18n.Time, width: 100 },
                        pid: { label: "PID", width: 60 },
                        tid: { label: "TID", width: 60 },
                        dummy: { label: "", width: 0 }
                    }
                }, this.id + "LogGrid");

                var context = this;
                this.logGrid.on(".dgrid-row-url:click", function (evt) {
                    if (context._onRowDblClick) {
                        var item = context.logGrid.row(evt).data;
                        context._onRowDblClick(item.Wuid);
                    }
                });
                this.logGrid.on(".dgrid-row:dblclick", function (evt) {
                    if (context._onRowDblClick) {
                        var item = context.logGrid.row(evt).data;
                        context._onRowDblClick(item.Wuid);
                    }
                });
                this.logGrid.onSelectionChanged(function (event) {
                    context.refreshActionState();
                });
                this.logGrid.startup();
            }
        },

        refreshGrid: function (clearSelection) {
            this.rawText.setText(this.i18n.loadingMessage);
            var filter = lang.mixin(this.getFilter(), {
                Name: this.params.getLogDirectory() + "/" + this.logTargetSelect.get("value"),
                Type: "tpcomp_log",
                LoadData: 1
            });
            this.logGrid.set("query", filter);
            if (clearSelection) {
                this.logGrid.clearSelection();
            }
        },

        refreshActionState: function () {
            var selection = this.logGrid.getSelected();
        }
    });
});
