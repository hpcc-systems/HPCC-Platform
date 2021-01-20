define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/store/Observable",

    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "src/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil",
    "src/Timings",

    "@hpcc-js/comms",

    "dojo/text!../templates/TimingPageWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/Select",
    "dijit/form/MultiSelect",
    "dijit/form/DropDownButton",
    "dijit/TooltipDialog"

], function (declare, nlsHPCCMod, Observable,
    registry,
    _TabContainerWidget, ESPWorkunit, DelayLoadWidget, ESPUtil, srcTimings,
    hpccComms,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("TimingPageWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "TimingPageWidget",
        i18n: nlsHPCC,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            var context = this;

            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.timingsTab = registry.byId(this.id + "_Timings");
            this.timelinePane = registry.byId(this.id + "TimelinePane");
            this.timingTab2 = registry.byId(this.id + "TimingTab2");

            var origResize = this.timelinePane.resize;
            this.timelinePane.resize = function () {
                origResize.apply(this, arguments);
                if (context._timings) {
                    context._timings
                        .resizeTimeline()
                        ;
                }
            };

            var origResize2 = this.timelinePane.resize;
            this.timingTab2.resize = function () {
                origResize2.apply(this, arguments);
                if (context._timings) {
                    context._timings
                        .resizeChart()
                        ;
                }
            };
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        //  Implementation  ---
        _onRefresh: function () {
            this.refreshGrid(true);
        },

        _onMetricsType: function (evt) {
            this._metricFilter = this._timings.selectedMetricValues();
            this.refreshGrid();
        },

        _onMetricsClose: function (evt) {
        },

        _onReset: function () {
            this.doReset();
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;
            this._timings = new srcTimings.Timings(params.Wuid, this.id + "TimelinePane", this.id + "Chart", this.id + "MetricsType");
            this._timings.click = function (row, col, sel) {
                context.refreshGrid();
            };

            var store = new ESPUtil.UndefinedMemory({
                idProperty: "__hpcc_id",
                data: []
            });
            this.store = new Observable(store);
            this.grid = new declare([ESPUtil.Grid(false, true)])({
                store: this.store
            }, this.id + "Grid");
            this.grid.on(".dgrid-row-url:click", function (evt) {
                var row = context.grid.row(evt).data;
                var tab = context.ensurePane(row.Name, row);
                if (tab) {
                    context.selectChild(tab);
                }
            });
            this.grid.startup();

            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                this.wu2 = hpccComms.Workunit.attach({ baseUrl: "" }, params.Wuid);
                var monitorCount = 4;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 === 0) {
                        context.refreshGrid(true);
                    }
                });
            }
        },

        refreshGrid: function (forceRefresh) {
            var context = this;
            this._timings.refresh(forceRefresh).then(function (args) {
                var cols = args[0];
                var data = args[1];
                var columns = {
                    __hpcc_id: { label: "##", width: 45 },
                    Scope: {
                        label: context.i18n.Scope,
                        sortable: true,
                        width: 120,
                        formatter: function (cell, row) {
                            switch (row.Type) {
                                case "graph":
                                case "subgraph":
                                case "activity":
                                case "edge":
                                    return "<a href='#" + cell + "' class='dgrid-row-url'>" + cell + "</a>";
                                case "function":
                                    const activityScopeID = context._timings.activityScopeID(cell);
                                    return "<a href='#" + cell + "' class='dgrid-row-url'>" + activityScopeID + "</a>" + cell.substring(activityScopeID.length);
                            }
                            return cell;
                        }
                    }
                };
                cols.forEach(function (col) {
                    var formattedID = "__" + col;
                    columns[col] = {
                        label: col,
                        width: 120,
                        formatter: function (cell, row) {
                            var retVal = row[formattedID] && row[formattedID].Formatted || cell;
                            return retVal !== undefined ? retVal : "";
                        }
                    };
                });
                context.grid.set("columns", columns);
                context.store.setData(data.map(function (row, i) {
                    var GraphName;
                    var SubGraphId;
                    var ActivityId;
                    var EdgeId;
                    switch (row.type) {
                        case "graph":
                            GraphName = row.id;
                            break;
                        case "subgraph":
                            GraphName = context._timings.graphID(row.name);
                            SubGraphId = row.id;
                            break;
                        case "activity":
                            GraphName = context._timings.graphID(row.name);
                            SubGraphId = context._timings.subgraphID(row.name);
                            ActivityId = row.id;
                            break;
                        case "function":
                            GraphName = context._timings.graphID(row.name);
                            SubGraphId = context._timings.subgraphID(row.name);
                            ActivityId = context._timings.activityID(row.name);
                            break;
                        case "edge":
                            GraphName = context._timings.graphID(row.name);
                            SubGraphId = context._timings.subgraphID(row.name);
                            EdgeId = row.id;
                            break;
                    }
                    var dataRow = {
                        __hpcc_id: i + 1,
                        Name: row.id,
                        Type: row.type,
                        Scope: row.name,
                        GraphName: GraphName,
                        SubGraphId: SubGraphId,
                        ActivityId: ActivityId,
                        EdgeId: EdgeId
                    };
                    for (var key in row) {
                        dataRow[key] = row[key];
                    }
                    cols.forEach(function (col) {
                        dataRow[col] = row[col];
                    });
                    return dataRow;
                }));
                context.grid.refresh();
            });
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.timingsTab.id) {
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel.params);
                    }
                }
            }
        },

        openGraph: function (graphName, subgraphID) {
            var tab = this.ensurePane(subgraphID + " - " + graphName, {
                GraphName: graphName,
                SubGraphId: subgraphID
            });
            if (tab) {
                this.selectChild(tab);
            }
        },

        ensurePane: function (_id, params) {
            var id = this.createChildTabID(_id);
            var retVal = registry.byId(id);
            if (!retVal) {
                retVal = new DelayLoadWidget({
                    id: id,
                    title: _id,
                    closable: true,
                    delayWidget: "GraphTree7Widget",
                    delayProps: {
                        _hostPage: this
                    },
                    params: {
                        Wuid: this.wu.Wuid,
                        GraphName: params.GraphName,
                        SubGraphId: params.SubGraphId,
                        ActivityId: params.ActivityId,
                        EdgeId: params.EdgeId
                    }
                });
                if (retVal) {
                    this.addChild(retVal);
                }
            }
            return retVal;
        }

    });
});
