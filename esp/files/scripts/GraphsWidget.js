/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
    "dojo/on",

    "dijit/form/Button",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPWorkunit",
    "hpcc/GraphPageWidget",
    "hpcc/ESPUtil"

], function (declare, on,
                Button,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, ESPWorkunit, GraphPageWidget, ESPUtil) {
    return declare("GraphsWidget", [GridDetailsWidget], {

        gridTitle: "Graphs",
        idProperty: "Name",

        wu: null,

        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;

            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 == 0) {
                        context.refreshGrid();
                    }
                });
            }
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var context = this;
            this.openSafeMode = new Button({
                label: "Open (safe mode)",
                onClick: function (event) {
                    context._onOpen(event, {
                        safeMode: true
                    });
                }
            }, this.id + "ContainerNode");

            var retVal = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    Name: {
                        label: "Name", width: 72, sortable: true,
                        formatter: function (Name, idx) {
                            return "<a href='#' rowIndex=" + idx + " class='" + context.id + "GraphClick'>" + Name + "</a>";
                        }
                    },
                    Label: { label: "Label", sortable: true },
                    Complete: { label: "Completed", width: 72, sortable: true },
                    Time: { label: "Time", width: 72, sortable: true },
                    Type: { label: "Type", width: 72, sortable: true }
                }
            }, domID);

            var context = this;
            on(document, "." + this.id + "GraphClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        createDetail: function (id, row, params) {
            var safeMode = false;
            if (params && params.safeMode) {
                var safeMode = true;
            }
            return new GraphPageWidget({
                id: id,
                title: row.Name,
                closable: true,
                hpcc: {
                    type: "graph",
                    params: {
                        Wuid: this.wu.Wuid,
                        GraphName: row.Name,
                        SafeMode: safeMode
                    }
                }
            });
        },

        refreshGrid: function (args) {
            var context = this;
            this.wu.getInfo({
                onGetTimers: function (timers) {
                    //  Required to calculate Graphs Total Time  ---
                },
                onGetGraphs: function (graphs) {
                    context.store.setData(graphs);
                    context.grid.refresh();
                }
            });
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);

            this.openSafeMode.set("disabled", !selection.length);
        }
    });
});
