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

    "dijit/registry",
    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil",

    "@hpcc-js/eclwatch"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on,
            registry, BorderContainer, ContentPane,
            selector,
            GridDetailsWidget, ESPWorkunit, DelayLoadWidget, ESPUtil,
            hpccEclWatch) {
        return declare("TimingPageWidget", [GridDetailsWidget], {
            baseClass: "TimingPageWidget",

            gridTitle: nlsHPCC.Timers,
            idProperty: "__hpcc_id",

            postCreate: function (args) {
                this.inherited(arguments);
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

            //  Plugin wrapper  ---
            init: function (params) {
                if (this.inherited(arguments))
                    return;

                var context = this;
                if (params.Wuid) {
                    this.wu = ESPWorkunit.Get(params.Wuid);
                    var monitorCount = 4;
                    this.wu.monitor(function () {
                        if (context.wu.isComplete() || ++monitorCount % 5 === 0) {
                            context.refreshGrid();
                        }
                    });
                }

                this.timeline = new hpccEclWatch.WUTimeline()
                    .target(this.id + "TimelinePane")
                    .overlapTolerence(1)
                    .baseUrl("")
                    .wuid(params.Wuid)
                    .request({
                        ScopeFilter: {
                            MaxDepth: 2,
                            ScopeTypes: []
                        },
                        NestedFilter: {
                            Depth: 0,
                            ScopeTypes: []
                        },
                        PropertiesToReturn: {
                            AllProperties: true,
                            AllStatistics: true,
                            AllHints: true,
                            Properties: ["WhenStarted", "TimeElapsed"]
                        },
                        ScopeOptions: {
                            IncludeId: true,
                            IncludeScope: true,
                            IncludeScopeType: true
                        },
                        PropertyOptions: {
                            IncludeName: true,
                            IncludeRawValue: true,
                            IncludeFormatted: true,
                            IncludeMeasure: true,
                            IncludeCreator: false,
                            IncludeCreatorType: false
                        }
                    })
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

                var statsTabID = this.createChildTabID("Stats");
                var statsTab = new DelayLoadWidget({
                    id: statsTabID,
                    title: this.i18n.Stats,
                    closable: false,
                    delayWidget: "WUStatsWidget",
                    hpcc: {
                        type: "stats",
                        params: this.params
                    }
                });
                this.addChild(statsTab);
                this._refreshActionState();
            },

            createGrid: function (domID) {
                var context = this;
                var retVal = new declare([ESPUtil.Grid(false, true)])({
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
                                    return "<a href='#' class='dgrid-row-url'>" + Name + "</a>";
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

                retVal.on(".dgrid-row-url:click", function (evt) {
                    if (context._onRowDblClick) {
                        var row = retVal.row(evt).data;
                        context._onRowDblClick(row);
                    }
                });
                return retVal;
            },

            createDetail: function (id, row, params) {
                if (row.GraphName) {
                    var localParams = {
                        Wuid: this.wu.Wuid,
                        GraphName: row.GraphName,
                        SubGraphId: row.SubGraphId ? row.SubGraphId : null,
                        SafeMode: (params && params.safeMode) ? true : false
                    }
                    return new DelayLoadWidget({
                        id: id,
                        title: row.Name,
                        closable: true,
                        delayWidget: "GraphTreeWidget",
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
                if (sourceControl === this.grid) {
                    arrayUtil.forEach(sourceControl.getSelected(), function (item, idx) {
                        timerItems.push(item);
                    });
                }

                //  Set Selected Items  ---
                if (sourceControl !== this.grid) {
                    this.grid.setSelected(timerItems);
                }
                if (sourceControl !== this.timingTreeMap) {
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
