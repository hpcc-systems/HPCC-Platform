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
    "dojo/has",

    "dijit/form/Button",
    "dijit/layout/ContentPane",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/ESPWorkunit",
    "src/ESPQuery",
    "src/ESPLogicalFile",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil",
    "src/Utility",

    "@hpcc-js/eclwatch"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, has,
                Button, ContentPane,
                selector,
                GridDetailsWidget, ESPWorkunit, ESPQuery, ESPLogicalFile, DelayLoadWidget, ESPUtil, Utility,
                hpccEclWatch) {
    return declare("GraphsWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_Graphs,
        idProperty: "Name",

        wu: null,
        query: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.isIE8 = has("ie") === 8;

            this.timelinePane = new ContentPane({
                id: this.id + "TimelinePane",
                region: "top",
                splitter: true,
                style: "height: 120px",
                minSize: 120
            });
            this.timelinePane.placeAt(this.gridTab, "last");
            var context = this;
            var origResize = this.timelinePane.resize;
            this.timelinePane.resize = function() {
                origResize.apply(this, arguments);
                if (context.timeline) {
                    context.timeline
                        .resize()
                        .lazyRender()
                    ;
                }
            }
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.alphanumSort["Name"] = true;

            var context = this;
            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 === 0) {
                        context.refreshGrid();
                    }
                });
            } else if (params.QuerySetId && params.Id) {
                this.query = ESPQuery.Get(params.QuerySetId, params.Id);
                this.refreshGrid();
            } else if (params.NodeGroup && params.LogicalName) {
                this.logicalFile = ESPLogicalFile.Get(params.NodeGroup, params.LogicalName);
                this.refreshGrid();
            }

            this.timeline = new hpccEclWatch.WUTimeline()
                .target(this.id + "TimelinePane")
                .overlapTolerence(1)
                .baseUrl("")
                .wuid(params.Wuid)
                .on("dblclick", function(item, d3Event, origDblClick) {
                    if (item && d3Event && d3Event.ctrlKey) {
                        var scope = item[3];
                        var descendents = scope.ScopeName.split(":");
                        for (var i = 0; i < descendents.length; ++i) {
                            const scopeName = descendents[i];
                            if (scopeName.indexOf("graph") === 0) {
                                var tab = context.ensurePane({ Name: scopeName }, { SubGraphId: item[0] });
                                context.selectChild(tab);         
                                break;
                            }
                        }
                    } else {
                        origDblClick.call(context.timeline, item, d3Event);
                    }
                }, true)
                .render()
            ;

            this._refreshActionState();
        },

        getStateImageName: function (row) {
            if (row.Complete) {
                return "workunit_completed.png";
            } else if (row.Running) {
                return "workunit_running.png";
            } else if (row.Failed) {
                return "workunit_failed.png";
            }
            return "workunit.png";
        },

        getStateImageHTML: function (row) {
            return Utility.getImageHTML(this.getStateImageName(row));
        },

        createGrid: function (domID) {
            var context = this;
            this.openLegacyMode = new Button({
                label: this.i18n.OpenLegacyMode,
                onClick: function (event) {
                    context._onOpen(event, {
                        legacyMode: true
                    });
                }
            }).placeAt(this.widget.Open.domNode, "after");
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    Name: {
                        label: this.i18n.Name, width: 99, sortable: true,
                        formatter: function (Name, row) {
                            return context.getStateImageHTML(row) + "&nbsp;<a href='#' class='dgrid-row-url'>" + Name + "</a>";
                        }
                    },
                    Label: { label: this.i18n.Label, sortable: true },
                    WhenStarted: {
                        label: this.i18n.Started, width: 90,
                        formatter: function (whenStarted) {
                            if (whenStarted) {
                                var dateTime = new Date(whenStarted);
                                return dateTime.toLocaleTimeString();
                            }
                            return "";
                        }
                    },
                    WhenFinished: {
                        label: this.i18n.Finished, width: 90,
                        formatter: function (whenFinished, idx) {
                            if (whenFinished) {
                                var dateTime = new Date(whenFinished);
                                return dateTime.toLocaleTimeString();
                            }
                            return "";
                        }
                    },
                    Time: {
                        label: this.i18n.Duration, width: 90, sortable: true,
                        formatter: function (totalSeconds, idx) {
                            var hours = Math.floor(totalSeconds / 3600);
                            totalSeconds %= 3600;
                            var minutes = Math.floor(totalSeconds / 60);
                            var seconds = (totalSeconds % 60).toFixed(2);
                            return (hours < 10 ? "0" : "") + hours + ":" + (minutes < 10 ? "0" : "") + minutes + ":" + (seconds < 10 ? "0" : "") + seconds;
                        }
                    },
                    Type: { label: this.i18n.Type, width: 72, sortable: true }
                }
            }, domID);

            retVal.on(".dgrid-row:click", function (evt) {
                context.syncSelectionFrom(context.grid);
            });

            retVal.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        getDetailID: function (row, params) {
            var retVal = "Detail" + row[this.idProperty];
            if (params && params.SubGraphId) {
                retVal += params.SubGraphId;
            }
            if (params && params.legacyMode) {
                retVal += "Legacy";
            }
            return retVal;
        },

        createDetail: function (id, row, params) {
            var localParams = {}
            if (this.wu) {
                localParams = {
                    Wuid: this.wu.Wuid,
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
            } else if (this.logicalFile) {
                localParams = {
                    Wuid: this.logicalFile.Wuid,
                    GraphName: row.Name,
                    SubGraphId: (params && params.SubGraphId) ? params.SubGraphId : null,
                    SafeMode: (params && params.safeMode) ? true : false
                }
            }
            var title = row.Name;
            var delayWidget = "Graph7Widget";
            var delayProps = {
                forceJS: true
            };
            if (params && params.SubGraphId) {
                title = params.SubGraphId + " - " + title;
            }
            if (params && params.legacyMode || this.isIE8) {
                delayWidget = "GraphTreeWidget";
                title += " (L)";
                delayProps = {};
            }
            return new DelayLoadWidget({
                id: id,
                title: title,
                closable: true,
                delayWidget: delayWidget,
                delayProps: delayProps,
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
                var context = this;
                this.query.refresh().then(function (response) {
                    var graphs = [];
                    if (lang.exists("WUGraphs.ECLGraph", context.query)) {
                        arrayUtil.forEach(context.query.WUGraphs.ECLGraph, function (item, idx) {
                            var graph = {
                                Name: item.Name,
                                Label: "",
                                Completed: "",
                                Time: 0,
                                Type: item.Type
                            };
                            graphs.push(graph);
                        });
                    } else if (lang.exists("WUGraphs.ECLGraph", context.query)) {
                        arrayUtil.forEach(context.query.WUGraphs.ECLGraph, function (item, idx) {
                            var graph = {
                                Name: item.Name || "",
                                Label: item.Label || "",
                                Completed: item.Completed || "",
                                Time: item.Time || 0,
                                Type: item.Type || ""
                            };
                            graphs.push(graph);
                        });
                    }
                    context.store.setData(graphs);
                    context.grid.refresh();
                });
            } else if (this.logicalFile) {
                var graphs = [];
                if (lang.exists("Graphs.ECLGraph", this.logicalFile)) {
                    arrayUtil.forEach(this.logicalFile.Graphs.ECLGraph, function (item, idx) {
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

            this.openLegacyMode.set("disabled", !selection.length);
        },

        syncSelectionFrom: function (sourceControl) {
            var graphItems = [];
            var timingItems = [];

            //  Get Selected Items  ---
            if (sourceControl === this.grid) {
                arrayUtil.forEach(sourceControl.getSelected(), function (item, idx) {
                    timingItems.push(item);
                });
            }

            //  Set Selected Items  ---
            if (sourceControl !== this.grid) {
                this.grid.setSelected(graphItems);
            }
        }
    });
});
