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

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "hpcc/WsDFUXref",
    "hpcc/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil",
    "hpcc/XrefDetailsWidget",

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, dom, domConstruct, domClass,
                registry, ToggleButton, ToolbarSeparator, Button,
                selector,
                GridDetailsWidget, WsDFUXref, ESPWorkunit, DelayLoadWidget, ESPUtil, FilterDropDownWidget, XrefDetailsWidget) {
    return declare("XrefFoundFilesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        gridTitle: nlsHPCC.title_LostFilesFor,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this._refreshActionState();
            this.refreshGrid();

            this.gridTab.set("title", this.i18n.title_LostFilesFor + ":" + this.params.Name);
        },

        _onRefresh: function (event) {
            this.refreshGrid();
        },

        createGrid: function (domID) {

            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                columns: {
                    Name: {label: this.i18n.Name, width:100, sortable: false},
                    Modified: {label: this.i18n.Modified, width: 30, sortable: true},
                    Numparts: {label: this.i18n.TotalParts, width: 30, sortable: false},
                    Size: {label:this.i18n.Size, width: 30, sortable: true},
                    Partslost: {label:this.i18n.PartsLost, width: 30, sortable: true},
                    Primarylost: {label: this.i18n.PrimaryLost, width: 30, sortable: false},
                    Replicatedlost: {label: this.i18n.ReplicatedLost, width: 30, sortable: false}
                }
            }, domID);

            return retVal;
        },

        refreshGrid: function () {
            var context = this;

            WsDFUXref.DFUXRefLostFiles({
                request: {
                    Cluster: this.params.Name
                }
            }).then(function (response) {
                var results = [];
                var newRows = [];
                if (lang.exists("DFUXRefLostFilesQueryResponse.DFUXRefLostFilesQueryResult.File", response)) {
                    results = response.DFUXRefLostFilesQueryResponse.DFUXRefLostFilesQueryResultFile
                }
                arrayUtil.forEach(results, function (row, idx) {
                   newRows.push({
                        Name: row.Name,
                        Modified: row.Modified,
                        Numparts: row.Numparts,
                        Partslost: row.Partslost,
                        Primarylost: row.Primarylost,
                        Replicatedlost: row.Replicatedlost,
                        Size: row.Size
                    });
                });
                context.store.setData(newRows);
                context.grid.set("query", {});
            });
        }
    });
});