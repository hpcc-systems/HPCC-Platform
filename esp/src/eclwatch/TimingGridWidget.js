define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",

    "dgrid/selector",

    "hpcc/_Widget",
    "src/ESPUtil",
    "src/ESPWorkunit",

    "dojo/text!../templates/TimingGridWidget.html"
],
    function (declare, lang, i18n, nlsHPCC, arrayUtil, Memory, Observable,
        registry,
        selector,
        _Widget, ESPUtil, ESPWorkunit,
        template) {
        return declare("TimingGridWidget", [_Widget], {
            templateString: template,
            baseClass: "TimingGridWidget",
            i18n: nlsHPCC,

            timingGrid: null,
            dataStore: null,
            lastSelection: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
            },

            startup: function (args) {
                this.inherited(arguments);
                var store = new Memory({
                    idProperty: "id",
                    data: []
                });
                this.timingStore = Observable(store);

                this.timingGrid = new declare([ESPUtil.Grid(false, true)])({
                    columns: {
                        id: { label: "##", width: 45 },
                        Name: { label: this.i18n.Component },
                        Seconds: { label: this.i18n.TimeSeconds, width: 124 }
                    },
                    store: this.timingStore
                }, this.id + "TimingGrid");

                var context = this;
                this.timingGrid.on(".dgrid-row:click", function (evt) {
                    var item = context.timingGrid.row(evt).data;
                    context.onClick(item);
                });
                this.timingGrid.on(".dgrid-row:dblclick", function (evt) {
                    var item = context.timingGrid.row(evt).data;
                    context.onDblClick(item);
                });
                this.timingGrid.startup();
            },

            resize: function (args) {
                this.inherited(arguments);
                this.timingGrid.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            //  Plugin wrapper  ---
            onClick: function (items) {
            },

            onDblClick: function (item) {
            },

            init: function (params) {
                if (this.inherited(arguments))
                    return;

                this.defaultQuery = "*";
                if (params.query) {
                    this.defaultQuery = params.query;
                }

                if (params.Wuid) {
                    this.wu = ESPWorkunit.Get(params.Wuid);
                    var monitorCount = 4;
                    var context = this;
                    this.wu.monitor(function () {
                        if (context.wu.isComplete() || ++monitorCount % 5 === 0) {
                            context.wu.getInfo({
                                onGetTimers: function (timers) {
                                    context.loadTimings(timers);
                                }
                            });
                        }
                    });
                }
            },

            setQuery: function (graphName) {
                if (!graphName || graphName === "*") {
                    this.timingGrid.refresh();
                } else {
                    this.timingGrid.set("query", {
                        GraphName: graphName,
                        HasSubGraphId: true
                    });
                }
            },

            getSelected: function () {
                return this.timingGrid.getSelected();
            },

            setSelectedAsGlobalID: function (selItems) {
                var selectedItems = [];
                arrayUtil.forEach(this.timingStore.data, function (item, idx) {
                    if (item.SubGraphId) {
                        if (item.SubGraphId && arrayUtil.indexOf(selItems, item.SubGraphId) >= 0) {
                            selectedItems.push(item);
                        }
                    }
                });
                this.setSelected(selectedItems);
            },

            setSelected: function (selItems) {
                this.timingGrid.setSelected(selItems);
            },

            loadTimings: function (timers) {
                arrayUtil.forEach(timers, function (item, idx) {
                    lang.mixin(item, {
                        id: idx
                    });
                });
                this.timingStore.setData(timers);
                this.setQuery(this.defaultQuery);
            }
        });
    });
