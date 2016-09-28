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
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil",
    "hpcc/FilterDropDownWidget",
    "hpcc/XrefDetailsWidget",

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, dom, domConstruct, domClass,
                registry, ToggleButton, ToolbarSeparator, Button,
                selector,
                GridDetailsWidget, WsDFUXref, DelayLoadWidget, ESPUtil) {
    return declare("XrefFoundFilesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        gridTitle: nlsHPCC.title_OrphanFilesFor,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this._refreshActionState();
            this.refreshGrid();

            this.gridTab.set("title", this.i18n.title_OrphanFilesFor + ":" + this.params.Name);
        },

        _onRefresh: function (event) {
            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;
            this.openButton = registry.byId(this.id + "Open");
            this.delete = new Button({
                id: this.id + "Delete",
                disabled: false,
                onClick: function (val) {
                    if (confirm(context.i18n.RunningServerStrain)) {
                        var selections = context.grid.getSelected();
                        WsDFUXref.DFUXRefBuild({
                            request: {
                                Cluster: selections[0].Name
                            }
                        }).then(function (response) {
                            if (response) {
                                context.refreshGrid();
                            }
                        });
                    }

                },
                label: this.i18n.Delete
            }).placeAt(this.openButton.domNode, "after");

            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                selectionMode: "single",
                allowSelectAll: false,
                columns: {
                    Name: {label: this.i18n.Name, width:100, sortable: false},
                    Modified: {label: this.i18n.Modified, width: 30, sortable: false},
                    PartsFound: {label: this.i18n.PartsFound, width: 30, sortable: false},
                    TotalParts: {label: this.i18n.TotalParts, width: 30, sortable: true},
                    Size: {label: this.i18n.Size, width: 30, sortable: true}
                }
            }, domID);

            return retVal;
        },

        refreshGrid: function () {
            var context = this;

            WsDFUXref.DFUXRefOrphanFiles({
                request: {
                    Cluster: this.params.Name
                }
            }).then(function (response) {
                var results = [];
                var newRows = [];
                if (lang.exists("DFUXRefOrphanFilesQueryResponse.DFUXRefOrphanFilesQueryResult.File", response)) {
                    results = response.DFUXRefOrphanFilesQueryResponse.DFUXRefOrphanFilesQueryResult.File
                }
                arrayUtil.forEach(results, function (row, idx) {
                   newRows.push({
                        Name: row.Partmask,
                        Modified: row.Modified,
                        PartsFound: row.Partsfound,
                        TotalParts: row.Numparts,
                        Size: row.Size
                    });
                });
                context.store.setData(newRows);
                context.grid.set("query", {});
            });
        }
    });
});