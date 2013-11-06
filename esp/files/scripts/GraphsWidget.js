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
    "dojo/_base/lang",
    "dojo/_base/array",
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
    "hpcc/TimingTreeMapWidget",
    "hpcc/ESPUtil"

], function (declare, lang, arrayUtil, on,
                Button,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, ESPWorkunit, GraphPageWidget, TimingTreeMapWidget, ESPUtil) {
    return declare("GraphsWidget", [GridDetailsWidget], {

        gridTitle: "Graphs",
        idProperty: "Name",

        wu: null,
        query: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.timingTreeMap = new TimingTreeMapWidget({
                id: this.id + "TimingTreeMap",
                region: "right",
                splitter: true,
                style: "width: 33%",
                minSize: 120
            });
            this.timingTreeMap.placeAt(this.gridTab, "last");
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;
            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 == 0) {
                        context.refreshGrid();
                    }
                });
            }
            else if (params.Query){
                this.query = params.Query;
                this.refreshGrid();
            }

            this.timingTreeMap.init(params);
            this.timingTreeMap.onClick = function (value) {
                context.syncSelectionFrom(context.timingTreeMap);
            }
            this.timingTreeMap.onDblClick = function (item) {
                context._onOpen(item, {
                    SubGraphId: item.SubGraphId
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
                    Time: {
                        label: "Time", width: 90, sortable: true,
                        formatter: function (totalSeconds, idx) {
                            var hours = Math.floor(totalSeconds / 3600);
                            totalSeconds %= 3600;
                            var minutes = Math.floor(totalSeconds / 60);
                            var seconds = (totalSeconds % 60).toFixed(2);
                            return (hours < 10 ? "0" : "") + hours + ":" + (minutes < 10 ? "0" : "") + minutes + ":" + (seconds < 10 ? "0" : "") + seconds;
                        }
                    },
                    Type: { label: "Type", width: 72, sortable: true }
                }
            }, domID);

            var context = this;
            retVal.on(".dgrid-row:click", function (evt) {
                context.syncSelectionFrom(context.grid);
            });

            on(document, "." + this.id + "GraphClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        createDetail: function (id, row, params) {
            var localParams = {}
            if (this.wu) {
                localParams = {
                    Wuid: this.wu.Wuid,
                    GraphName: row.Name,
                    GraphName: row.Name,
                    SubGraphId: (params && params.SubGraphId) ? params.SubGraphId : null,
                    SafeMode: (params && params.safeMode) ? true : false
                }
            } else if (this.query) {
                localParams = {
                    Target: this.query.QuerySet,
                    QueryId: this.query.QueryId,
                    GraphName: row.Name,
                    SubGraphId: (params && params.SubGraphId) ? params.SubGraphId : null,
                    SafeMode: (params && params.safeMode) ? true : false
                }
            }
            return new GraphPageWidget({
                id: id,
                title: row.Name,
                closable: true,
                hpcc: {
                    type: "graph",
                    params: localParams
                }
            });
        },

        refreshGrid: function (args) {
            if (this.wu) {
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
            } else if (this.query) {
                var graphs = [];
                if (lang.exists("GraphIds.Item", this.query)) {
                    arrayUtil.forEach(this.query.GraphIds.Item, function (item, idx) {
                        var graph = {
                            Name: item,
                            Label: "",
                            Completed: "",
                            Time: 0,
                            Type: ""
                        };
                        graphs.push(graph);
                    });
                }
                this.store.setData(graphs);
                this.grid.refresh();
            }
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);

            this.openSafeMode.set("disabled", !selection.length);
        },

        syncSelectionFrom: function (sourceControl) {
            var graphItems = [];
            var timingItems = [];

            //  Get Selected Items  ---
            if (sourceControl == this.grid) {
                arrayUtil.forEach(sourceControl.getSelected(), function (item, idx) {
                    timingItems.push(item);
                });
            }
            if (sourceControl == this.timingTreeMap) {
                arrayUtil.forEach(sourceControl.getSelected(), function (item, idx) {
                    if (item.children) {
                        if (item.children.length) {
                            graphItems.push({
                                Name: item.children[0].GraphName
                            })
                        }
                    } else {
                        graphItems.push({
                            Name: item.GraphName
                        })
                    }
                });
            }

            //  Set Selected Items  ---
            if (sourceControl != this.grid) {
                this.grid.setSelected(graphItems);
            }
            if (sourceControl != this.timingTreeMap) {
                this.timingTreeMap.setSelectedGraphs(timingItems);
            }
        }
    });
});
