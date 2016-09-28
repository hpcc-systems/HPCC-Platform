/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/on",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-class",

    "dijit/registry",
    "dijit/form/ToggleButton",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",

    "hpcc/GridDetailsWidget",
    "hpcc/WsDFUXref",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil",

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, dom, domConstruct, domClass,
                registry, ToggleButton, ToolbarSeparator, Button,
                GridDetailsWidget, WsDFUXref, DelayLoadWidget, ESPUtil) {
    return declare("XrefFoundFilesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        gridTitle: nlsHPCC.title_ErrorsWarnings,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this._refreshActionState();
            this.refreshGrid();

            this.gridTab.set("title", this.i18n.title_ErrorsWarnings + ":" + this.params.Name);
        },

        _onRefresh: function (event) {
            this.refreshGrid();
        },

        createGrid: function (domID) {
            this.openButton = registry.byId(this.id + "Open");

            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                columns: {
                    File: {label: this.i18n.File, width:100, sortable: false},
                    Text: {label: this.i18n.Message, width: 100, sortable: false},
                    Status: {label: this.i18n.Status, width: 10, sortable: true,
                        renderCell: function (object, value, node, options) {
                            switch (value) {
                                case "Error":
                                    domClass.add(node, "ErrorCell");
                                    break;
                                case "Warning":
                                    domClass.add(node, "WarningCell");
                                    break;
                                case "Normal":
                                    domClass.add(node, "NormalCell");
                                    break;
                            }
                            node.innerText = value;
                        }
                    }
                }
            }, domID);

            return retVal;
        },

        refreshGrid: function () {
            var context = this;

            WsDFUXref.DFUXRefMessages({
                request: {
                    Cluster: this.params.Name
                }
            }).then(function (response) {
                var results = [];
                var newRows = [];
                if (lang.exists("DFUXRefMessagesQueryResponse.DFUXRefMessagesQueryResult", response)) {
                    results = response.DFUXRefMessagesQueryResponse.DFUXRefMessagesQueryResult;
                }
                arrayUtil.forEach(results.Warning, function (row, idx) {
                   newRows.push({
                        File: row.File,
                        Text: row.Text,
                        Status: context.i18n.Warning
                    });
                });

                arrayUtil.forEach(results.Error, function (row, idx) {
                   newRows.push({
                        File: row.File,
                        Text: row.Text,
                        Status: context.i18n.Error
                    });
                });
                context.store.setData(newRows);
                context.grid.set("query", {});
            });
        }
    });
});