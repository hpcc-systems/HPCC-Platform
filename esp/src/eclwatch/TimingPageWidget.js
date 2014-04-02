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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/on",

    "dijit/registry",
    "dijit/layout/BorderContainer",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "hpcc/TimingTreeMapWidget",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on,
            registry, BorderContainer,
            OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
            GridDetailsWidget, ESPWorkunit, DelayLoadWidget, TimingTreeMapWidget, ESPUtil) {
        return declare("TimingPageWidget", [GridDetailsWidget], {
            baseClass: "TimingPageWidget",

            gridTitle: nlsHPCC.Timers,
            idProperty: "__hpcc_id",

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

            //  Plugin wrapper  ---
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

                this.timingTreeMap.init(lang.mixin(params, {
                    query: {
                        graphsOnly: false,
                    }
                }));
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
                var retVal = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                    allowSelectAll: true,
                    deselectOnRefresh: false,
                    store: this.store,
                    columns: {
                        col1: selector({
                            width: 27,
                            selectorType: "checkbox",
                            disabled: function (item) {
                                return false;//!item.GraphName;
                            }
                        }),
                        __hpcc_id: { label: "##", width: 45 },
                        Name: {
                            label: this.i18n.Name,
                            sortable: true,
                            formatter: function (Name, row) {
                                if (row.GraphName) {
                                    return "<a href='#' class='" + context.id + "GraphClick'>" + Name + "</a>";
                                }
                                return Name;
                            }
                        },
                        Seconds: { label: this.i18n.TimeSeconds, width: 124 }
                    }
                }, domID);

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
                if (row.GraphName) {
                    localParams = {
                        Wuid: this.wu.Wuid,
                        GraphName: row.GraphName,
                        SubGraphId: row.SubGraphId ? row.SubGraphId : null,
                        SafeMode: (params && params.safeMode) ? true : false
                    }
                    return new DelayLoadWidget({
                        id: id,
                        title: row.Name,
                        closable: true,
                        delayWidget: "GraphPageWidget",
                        hpcc: {
                            type: "graph",
                            params: localParams
                        }
                    });
                }
                return null;
            },

            refreshGrid: function (args) {
                if (this.wu) {
                    var context = this;
                    this.wu.getInfo({
                        onGetTimers: function (timers) {
                            context.store.setData(timers);
                            context.grid.refresh();
                        }
                    });
                }
            },

            syncSelectionFrom: function (sourceControl) {
                var timerItems = [];

                //  Get Selected Items  ---
                if (sourceControl == this.grid) {
                    arrayUtil.forEach(sourceControl.getSelected(), function (item, idx) {
                        timerItems.push(item);
                    });
                }
                if (sourceControl == this.timingTreeMap) {
                    arrayUtil.forEach(sourceControl.getSelected(), function (item, idx) {
                        if (item.children) {
                            arrayUtil.forEach(item.children, function (childItem, idx) {
                                timerItems.push(childItem);
                            });
                        } else {
                            timerItems.push(item);
                        }
                    });
                }

                //  Set Selected Items  ---
                if (sourceControl != this.grid) {
                    this.grid.setSelected(timerItems);
                }
                if (sourceControl != this.timingTreeMap) {
                    this.timingTreeMap.setSelectedGraphs(timerItems);
                }
            },

            refreshActionState: function (selection) {
                var hasGraphSelection = false;
                arrayUtil.some(selection, function (item, idx) {
                    if (item.GraphName) {
                        hasGraphSelection = true;
                        return true;
                    }
                }, this);
                registry.byId(this.id + "Open").set("disabled", !hasGraphSelection);
            }
        });
    });
