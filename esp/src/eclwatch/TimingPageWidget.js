define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/on",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",

    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "src/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil",
    "src/Timings",

    "@hpcc-js/comms",
    "@hpcc-js/common",
    "@hpcc-js/eclwatch",
    "@hpcc-js/chart",

    "dojo/text!../templates/TimingPageWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",
    "dijit/form/Select"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, Memory, Observable,
    registry, BorderContainer, TabContainer, ContentPane,
    selector,
    _TabContainerWidget, ESPWorkunit, DelayLoadWidget, ESPUtil, srcTimings,
    hpccComms, hpccCommon, hpccEclWatch, hpccChart,
    template) {
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
                }

                var origResize2 = this.timelinePane.resize;
                this.timingTab2.resize = function () {
                    origResize2.apply(this, arguments);
                    if (context._timings) {
                        context._timings
                            .resizeChart()
                            ;
                    }
                }
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
                this._metricFilter = evt.target.value;
                this.refreshGrid();
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
                }

                var store = new Memory({
                    idProperty: "__hpcc_id",
                    data: []
                });
                this.store = Observable(store);
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
                this._timings.refresh(forceRefresh).then(function (data) {
                    context.grid.set("columns", {
                        __hpcc_id: { label: "##", width: 45 },
                        Name: {
                            label: context.i18n.Name,
                            sortable: true,
                            formatter: function (Name, row) {
                                switch (row.Type) {
                                    case "graph":
                                    case "subgraph":
                                    case "activity":
                                    case "edge":
                                        return "<a href='#" + Name + "' class='dgrid-row-url'>" + Name + "</a>";
                                }
                                return Name;
                            }
                        },
                        Seconds: {
                            label: context._timings._metricSelectLabel,
                            width: 240
                        }
                    });
                    context.store.setData(data.map(function (row, i) {
                        var GraphName;
                        var SubGraphId;
                        var ActivityId;
                        switch (row.type) {
                            case "graph":
                                GraphName = row.id;
                                break;
                            case "subgraph":
                                GraphName = context._timings.graphID(row.name);
                                SubGraphId = row.id;
                                break;
                            case "activity":
                            case "edge":
                                GraphName = context._timings.graphID(row.name);
                                SubGraphId = context._timings.subgraphID(row.name);
                                ActivityId = row.id;
                                break;
                        }
                        return {
                            __hpcc_id: i + 1,
                            Name: row.id,
                            Seconds: row[context._timings._metricSelectValue],
                            Type: row.type,
                            Scope: row.name,
                            GraphName: GraphName,
                            SubGraphId: SubGraphId,
                            ActivityId: ActivityId
                        }
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

            ensurePane: function (id, params) {
                var retVal = registry.byId(id);
                if (!retVal) {
                    var context = this;
                    retVal = new DelayLoadWidget({
                        id: this.createChildTabID(id),
                        title: id,
                        closable: true,
                        delayWidget: "GraphTree7Widget",
                        params: {
                            Wuid: this.wu.Wuid,
                            GraphName: params.GraphName,
                            SubGraphId: params.SubGraphId,
                            ActivityId: params.ActivityId
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
