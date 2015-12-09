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
    "dojo/_base/Deferred",
    "dojo/has",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-class",
    "dojo/dom-style",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/store/util/QueryResults",
    "dojo/Evented",

    "dijit/registry",
    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",

    "hpcc/_Widget",
    "hpcc/ESPUtil",

    "dojo/text!../templates/GraphWidget.html",

    "dijit/Toolbar", 
    "dijit/ToolbarSeparator", 

    "dijit/TooltipDialog",
    "dijit/form/Form",
    "dijit/form/CheckBox",
    "dijit/form/TextBox",
    "dijit/form/DropDownButton",
    "dijit/form/Button",
    "dijit/form/ComboBox",
    "dijit/form/NumberSpinner",
    "dijit/Fieldset",

    "hpcc/TableContainer"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, Deferred, has, dom, domConstruct, domClass, domStyle, Memory, Observable, QueryResults, Evented,
            registry, BorderContainer, ContentPane,
            _Widget, ESPUtil,
            template) {

    var GraphStore = declare("GraphStore", [Memory], {
        idProperty: "id",

        setData: function (data) {
            this.inherited(arguments);
            this.cacheColumns = {};
            this.calcColumns();
        },

        //  Helpers  ---
        isNumber: function (n) {
            return !isNaN(parseFloat(n)) && isFinite(n);
        },
        calcColumns: function () {
            arrayUtil.forEach(this.data, function (item, idx) {
                for (var key in item) {
                    if (key != "id" && key.substring(0, 1) != "_") {
                        if (!this.cacheColumns[key]) {
                            this.cacheColumns[key] = item[key].length;
                        } else if (item[key].length > this.cacheColumns[key]) {
                            this.cacheColumns[key] = item[key].length;
                        }
                    }
                    if (this.isNumber(item[key])) {
                        item[key] = parseFloat(item[key]);
                    }
                }
            }, this);
        },
        getColumnWidth: function(key) {
            var width = this.cacheColumns[key] * 9;
            if (width < 27) {
                width = 27;
            } else if (width > 300) {
                width = 300;
            }
            return width;
        },
        appendColumns: function (target, highPriority, lowPriority, skip) {
            if (!highPriority) {
                highPriority = [];
            }
            if (!lowPriority) {
                lowPriority = [];
            }
            var skip = skip || [];
            arrayUtil.forEach(target, function (item, idx) {
                skip.push(item.field);
            });
            arrayUtil.forEach(highPriority, function (key, idx) {
                if (skip.indexOf(key) === -1 && this.cacheColumns[key]) {
                    target.push({
                        field: key, label: key, width: this.getColumnWidth(key)
                    });
                }
            }, this);
            for (var key in this.cacheColumns) {
                if (skip.indexOf(key) === -1 && highPriority.indexOf(key) === -1 && lowPriority.indexOf(key) === -1 && key.substring(0, 1) != "_") {
                    target.push({
                        field: key, label: key, width: this.getColumnWidth(key)
                    });
                }
            }
            arrayUtil.forEach(lowPriority, function (key, idx) {
                if (skip.indexOf(key) === -1 && this.cacheColumns[key]) {
                    target.push({
                        field: key, label: key, width: this.getColumnWidth(key)
                    });
                }
            }, this);
        }
    });

    var GraphTreeStore = declare("GraphTreeStore", [GraphStore], {
        idProperty: "id",

        //  Store API  ---
        constructor: function (options) {
        },
        query: function (query, options) {
            return this.inherited(arguments);
        },
        setTree: function (data) {
            this.setData([]);
            this.inherited(arguments);
            this.cacheColumns = {};
            this.walkData(data);
        },
        walkData: function (data) {
            arrayUtil.forEach(data, function (item, idx) {
                if (item._children) {
                    item._children.sort(function (l, r) {
                        return l.id - r.id;
                    });
                    this.walkData(item._children);
                    lang.mixin(item, {
                        __hpcc_notActivity: true
                    });
                }
                this.add(item);

                for (var key in item) {
                    if (key != "id" && key.substring(0, 1) != "_") {
                        if (!this.cacheColumns[key]) {
                            this.cacheColumns[key] = item[key].length;
                        } else if (item[key].length > this.cacheColumns[key]) {
                            this.cacheColumns[key] = item[key].length;
                        }
                    }
                    if (this.isNumber(item[key])) {
                        item[key] = parseFloat(item[key]);
                    }
                }
            }, this);
        },

        //  Tree API  ---
        mayHaveChildren: function (object) {
            return object._children;
        },
        getChildren: function (parent, options) {
            var filter = {};
            if (options.originalQuery.__hpcc_notActivity) {
                filter = {
                    __hpcc_notActivity: true
                };
            }
            return QueryResults(this.queryEngine(filter, options)(parent._children));
        }
    });

    var GraphView = declare("GraphView", null, {
        sourceGraphWidget: null,
        rootGlobalIDs: null,
        id: null,
        depth: null,
        distance: null,
        xgmml: null,
        svg: null,

        constructor: function (sourceGraphWidget, rootGlobalIDs, depth, distance, selectedGlobalIDs) {
            this.sourceGraphWidget = sourceGraphWidget;

            rootGlobalIDs.sort();
            this.rootGlobalIDs = rootGlobalIDs;
            this.selectedGlobalIDs = selectedGlobalIDs ? selectedGlobalIDs : rootGlobalIDs;

            var id = "";
            arrayUtil.forEach(this.rootGlobalIDs, function (item, idx) {
                if (idx > 0) {
                    id += ":";
                }
                id += item;
            }, this);
            if (depth) {
                id += ":" + depth;
            }
            if (distance) {
                id += ":" + distance;
            }
            this.id = id;

            this.depth = depth;
            this.distance = distance;
        },

        changeRootItems: function (globalIDs, depth, distance) {
            return this.sourceGraphWidget.getGraphView(globalIDs, depth, distance);
        },

        changeScope: function (depth, distance) {
            return this.sourceGraphWidget.getGraphView(this.rootGlobalIDs, depth, distance, this.selectedGlobalIDs);
        },

        refreshXGMML: function (targetGraphWidget) {
            targetGraphWidget.setMessage(targetGraphWidget.i18n.FetchingData).then(lang.hitch(this, function (response) {
                var rootItems = this.sourceGraphWidget.getItems(this.rootGlobalIDs);
                var xgmml = this.sourceGraphWidget.getLocalisedXGMML(rootItems, this.depth, this.distance);
                if (targetGraphWidget.loadXGMML(xgmml, true)) {
                    this.svg = "";
                }
                targetGraphWidget.setMessage("");
            }));
        },

        refreshLayout: function (targetGraphWidget) {
            var context = this;
            targetGraphWidget.onLayoutFinished = function () {
                context.svg = this._plugin.getSVG();
                this.onLayoutFinished = null;
            };
            targetGraphWidget.startLayout("dot");
        },

        navigateTo: function (targetGraphWidget, noModifyHistory) {
            var deferred = new Deferred();
            if (!noModifyHistory) {
                targetGraphWidget.graphViewHistory.push(this);
            }
            if (targetGraphWidget.onLayoutFinished == null) {
                targetGraphWidget.setMessage(targetGraphWidget.i18n.FetchingData).then(lang.hitch(this, function (response) {
                    var rootItems = this.sourceGraphWidget.getItems(this.rootGlobalIDs);
                    var xgmml = this.sourceGraphWidget.getLocalisedXGMML(rootItems, this.depth, this.distance);
                    targetGraphWidget.setMessage(targetGraphWidget.i18n.LoadingData).then(lang.hitch(this, function (response) {
                        var context = this;
                        if (targetGraphWidget.loadXGMML(xgmml)) {
                            if (xgmml) {
                                targetGraphWidget.onLayoutFinished = function () {
                                    this.setSelectedAsGlobalID(context.selectedGlobalIDs);
                                    context.svg = this._plugin.getSVG();
                                    this.onLayoutFinished = null;
                                    if (!noModifyHistory && this.graphViewHistory.getLatest() !== context) {
                                        this.graphViewHistory.getLatest().navigateTo(this);
                                    }
                                    deferred.resolve("Layout Complete.");
                                    this.refreshRootState(context.rootGlobalIDs);
                                };
                                if (this.svg) {
                                    targetGraphWidget.startCachedLayout(this.svg);
                                } else {
                                    targetGraphWidget.startLayout("dot");
                                }
                            } else {
                                targetGraphWidget.setMessage(targetGraphWidget.i18n.NothingSelected);
                                deferred.resolve("No Selection.");
                            }
                        } else {
                            targetGraphWidget.setSelectedAsGlobalID(context.selectedGlobalIDs);
                            targetGraphWidget.setMessage("");
                            deferred.resolve("XGMML Did Not Change.");
                            targetGraphWidget.refreshRootState(context.rootGlobalIDs);
                        }
                    }));
                }));
            } else {
                deferred.resolve("Graph Already in Layout.");
            }
            return deferred.promise;
        }
    });

    var GraphViewHistory = declare("GraphViewHistory", null, {
        sourceGraphWidget: null,
        history: null,
        index: null,

        constructor: function (sourceGraphWidget) {
            this.sourceGraphWidget = sourceGraphWidget;
            this.historicPos = 0;
            this.history = [];
            this.index = {};
        },

        clear: function () {
            this.history = [];
            this.index = {};
            this.sourceGraphWidget.refreshActionState();
        },

        //  Index  ----
        has: function(id) {
            return this.index[id] != null;
        },

        set: function(id, graphView) {
            return this.index[id] = graphView;
        },

        get: function(id) {
            return this.index[id];
        },

        //  History  ----
        push: function (graphView) {
            this.set(graphView.id, graphView);
            if (this.hasNext()) {
                this.history.splice(this.historicPos + 1, this.history.length);
            }
            if (this.history[this.history.length - 1] !== graphView) {
                this.history.push(graphView);
            }
            this.historicPos = this.history.length - 1;
            this.sourceGraphWidget.refreshActionState();
        },

        getCurrent: function () {
            return this.history[this.historicPos];
        },

        getLatest: function () {
            return this.history[this.history.length - 1];
        },

        hasPrevious: function () {
            return this.historicPos > 0;
        },

        hasNext: function () {
            return this.historicPos < this.history.length - 1;
        },

        isRootSubgraph: function () {
            arrayUtil.forEach(this.history[this.historicPos].rootGlobalIDs, function (item, idx) {

            }, this);
        },

        navigatePrevious: function () {
            if (this.hasPrevious()) {
                this.historicPos -= 1;
                this.history[this.historicPos].navigateTo(this.sourceGraphWidget, true).then(lang.hitch(this, function(response) {
                    this.sourceGraphWidget.refreshActionState();
                }));
            }
        },

        navigateNext: function () {
            if (this.hasNext()) {
                this.historicPos += 1;
                this.history[this.historicPos].navigateTo(this.sourceGraphWidget, true).then(lang.hitch(this, function (response) {
                    this.sourceGraphWidget.refreshActionState();
                }));
            }
        }
    });

    return declare("GraphWidget", [_Widget], {
            templateString: template,
            baseClass: "GraphWidget",
            i18n: nlsHPCC,

            KeyState_None: 0,
            KeyState_Shift: 1,
            KeyState_Control: 2,
            KeyState_Menu: 4,

            borderContainer: null,
            graphContentPane: null,
            _isPluginInstalled: false,
            _plugin: null,
            eventsRegistered: false,
            xgmml: null,
            dot: "",
            svg: "",

            isIE11: false,
            isIE: false,

            //  Known control properties  ---
            DOT_META_ATTR: "DOT_META_ATTR",

            constructor: function() {
                if (has("ie")) {
                    this.isIE = true;
                } else if (has("trident")) {
                    this.isIE11 = true;
                }
                this.graphViewHistory = new GraphViewHistory(this);
            },

            _onClickRefresh: function () {
                var graphView = this.getCurrentGraphView();
                graphView.refreshLayout(this);
                this.refreshRootState(graphView.rootGlobalIDs);
            },

            _onClickPrevious: function () {
                this.graphViewHistory.navigatePrevious();
            },

            _onClickNext: function () {
                this.graphViewHistory.navigateNext();
            },

            _onChangeZoom: function (args) {
                var selection = this.zoomDropCombo.get("value");
                switch (selection) {
                    case this.i18n.All:
                        this.centerOnItem(0, true);
                        break;
                    case this.i18n.Width:
                        this.centerOnItem(0, true, true);
                        break;
                    default:
                        var scale = parseFloat(selection);
                        if (!isNaN(scale)) {
                            this.setScale(scale);
                        }
                        break;
                }
            },

            _onDepthChange: function (value) {
                this._onRefreshScope();
            },

            _onDistanceChange: function (value) {
                this._onRefreshScope();
            },

            _onRefreshScope: function () {
                var graphView = this.getCurrentGraphView();
                if (graphView) {
                    var depth = this.depth.get("value");
                    var distance = this.distance.get("value");
                    graphView = graphView.changeScope(depth, distance);
                    graphView.navigateTo(this, true);
                }
            },

            _onSyncSelection: function () {
                var graphView = this.getCurrentGraphView();
                if (graphView) {
                    var rootItems = this.getSelectionAsGlobalID();
                    var depth = this.depth.get("value");
                    var distance = this.distance.get("value");
                    graphView = graphView.changeRootItems(rootItems, depth, distance);
                    graphView.navigateTo(this);
                }
            },

            _onOptionsApply: function () {
            },

            _onOptionsReset: function () {
            },

            onSelectionChanged: function (items) {
            },

            onDoubleClick: function (globalID, keyState) {
            },

            onLayoutFinished: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.graphContentPane = registry.byId(this.id + "GraphContentPane");
                this.next = registry.byId(this.id + "Next");
                this.previous = registry.byId(this.id + "Previous");
                this.zoomDropCombo = registry.byId(this.id + "ZoomDropCombo");
                this.depthLabel = registry.byId(this.id + "DepthLabel");
                this.depth = registry.byId(this.id + "Depth");
                this.distance = registry.byId(this.id + "Distance");
                this.syncSelectionSplitter = registry.byId(this.id + "SyncSelectionSplitter");
                this.syncSelection = registry.byId(this.id + "SyncSelection");
                this.optionsDropDown = registry.byId(this.id + "OptionsDropDown");
                this.optionsForm = registry.byId(this.id + "OptionsForm");
            },

            startup: function (args) {
                this.inherited(arguments);
                this._isPluginInstalled = dojoConfig.isPluginInstalled();
                this.createPlugin();
                this.watchStyleChange();
                this.watchSelect(this.zoomDropCombo);
            },

            resize: function (args) {
                this.inherited(arguments);
                this.borderContainer.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            //  Plugin wrapper  ---
            hasOptions: function () {
                return false;
            },

            createTreeStore: function () {
                var store = new GraphTreeStore();
                return Observable(store);
            },

            createStore: function () {
                var store = new GraphStore();
                return Observable(store);
            },

            showToolbar: function (show) {
                if (show) {
                    domClass.remove(this.id + "Toolbar", "hidden");
                } else {
                    domClass.add(this.id + "Toolbar", "hidden");
                }
                this.resize();
            },

            showNextPrevious: function (show) {
                if (show) {
                    domStyle.set(this.previous.domNode, 'display', 'block');
                    domStyle.set(this.next.domNode, 'display', 'block');
                } else {
                    domStyle.set(this.previous.domNode, 'display', 'none');
                    domStyle.set(this.next.domNode, 'display', 'none');
                }
                this.resize();
            },

            showDistance: function (show) {
                if (show) {
                    domClass.remove(this.id + "DistanceLabel", "hidden");
                    domStyle.set(this.distance.domNode, 'display', 'block');
                } else {
                    domClass.add(this.id + "DistanceLabel", "hidden");
                    domStyle.set(this.distance.domNode, 'display', 'none');
                }
                this.resize();
            },

            showSyncSelection: function (show) {
                if (show) {
                    domStyle.set(this.syncSelectionSplitter.domNode, 'display', 'block');
                    domStyle.set(this.syncSelection.domNode, 'display', 'block');
                } else {
                    domStyle.set(this.syncSelectionSplitter.domNode, 'display', 'none');
                    domStyle.set(this.syncSelection.domNode, 'display', 'none');
                }
                this.resize();
            },

            hasPlugin: function () {
                return this._plugin !== null;
            },

            clear: function () {
                if (this.hasPlugin()) {
                    this.xgmml = "";
                    this.dot = "";
                    this._plugin.clear();
                    this.graphViewHistory.clear();
                }
            },

            loadXGMML: function (xgmml, merge, timers, skipRender) {
                if (this.hasPlugin() && this.xgmml !== xgmml) {
                    this.xgmml = xgmml;
                    this._plugin._skipRender = skipRender;
                    if (merge) {
                        this._plugin.mergeXGMML(xgmml);
                    } else {
                        this._plugin.loadXGMML(xgmml);
                    }
                    if (timers) {
                        var totalTime = 0;
                        arrayUtil.forEach(timers, function (timer, idx) {
                            var item = this.getItem(timer.SubGraphId);
                            if (item) {
                                this.setProperty(item, this.i18n.TimeSeconds, timer.Seconds);
                                this.setProperty(item, "Label", timer.SubGraphId + " (" + timer.Seconds + "s)");
                                totalTime += timer.Seconds;
                            }
                        }, this);
                        this.setProperty(0, this.i18n.TimeSeconds, totalTime);
                    }
                    this.refreshActionState();
                    return true;
                }
                return false;
            },

            mergeXGMML: function (xgmml) {
                return this.loadXGMML(xgmml, true);
            },

            getTypeSummary: function (gloablIDs) {
                var retVal = {
                    Graph: 0,
                    Cluster: 0,
                    Vertex: 0,
                    Edge: 0,
                    Unknown: 0
                };
                arrayUtil.forEach(gloablIDs, function (item) {
                    retVal[this.getGlobalType(item)]++;
                }, this);
                return retVal;
            },

            getGlobalType: function(globalID) {
                if (this.hasPlugin()) {
                    return this._plugin.getGlobalType(this._plugin.getItem(globalID));
                }
                return "Unknown";
            },

            getComplexityInfo: function () {
                return {
                    threshold: 200,
                    activityCount: this.getVertices().length,
                    isComplex: function () { return this.activityCount > this.threshold; }
                };
            },

            centerOn: function (globalID) {
                if (this.hasPlugin()) {
                    var item = this.getItem(globalID);
                    if (item) {
                        this.centerOnItem(item, true);
                        var items = [item];
                        this._plugin.setSelected(items, true);
                    }
                }
            },

            getVersion: function () {
                if (this.hasPlugin()) {
                    return this._plugin.version;
                }
                return "";
            },

            getSVG: function () {
                return this._plugin.getSVG();
            },

            getXGMML: function () {
                return this.xgmml;
            },

            localLayout: function(callback) {
                var context = this;
                require(
                  ["hpcc/viz"],
                  function (viz) {
                      callback(Viz(context.dot, "svg"));
                  }
                );
            },

            displayProperties: function (globalID, place) {
                if (this.hasPlugin()) {
                    var item = this.getItem(globalID);
                    if (item) {
                        var props = this._plugin.getProperties(item);
                        if (props.id) {
                            var table = domConstruct.create("h3", {
                                innerHTML: props.id,
                                align: "center"
                            }, place);
                            delete props.id;
                        }
                        if (props.count) {
                            var table = domConstruct.create("table", { border: 1, cellspacing: 0, width: "100%" }, place);
                            var tr = domConstruct.create("tr", null, table);
                            var td = domConstruct.create("td", { innerHTML: this.i18n.Count }, tr);
                            var td = domConstruct.create("td", {
                                align: "right",
                                innerHTML: props.count
                            }, tr);
                            delete props.count;
                            domConstruct.create("br", null, place);
                        }
                        if (props.max) {
                            var table = domConstruct.create("table", { border: 1, cellspacing: 0, width: "100%" }, place);
                            var tr = domConstruct.create("tr", null, table);
                            domConstruct.create("th", { innerHTML: "    " }, tr);
                            domConstruct.create("th", { innerHTML: this.i18n.Skew }, tr);
                            domConstruct.create("th", { innerHTML: this.i18n.Node }, tr);
                            domConstruct.create("th", { innerHTML: this.i18n.Rows }, tr);
                            tr = domConstruct.create("tr", null, table);
                            domConstruct.create("td", { innerHTML: this.i18n.Max }, tr);
                            domConstruct.create("td", { innerHTML: props.maxskew }, tr);
                            domConstruct.create("td", { innerHTML: props.maxEndpoint }, tr);
                            domConstruct.create("td", { innerHTML: props.max }, tr);
                            tr = domConstruct.create("tr", null, table);
                            domConstruct.create("td", { innerHTML: this.i18n.Min }, tr);
                            domConstruct.create("td", { innerHTML: props.minskew }, tr);
                            domConstruct.create("td", { innerHTML: props.minEndpoint }, tr);
                            domConstruct.create("td", { innerHTML: props.min }, tr);
                            delete props.maxskew;
                            delete props.maxEndpoint;
                            delete props.max;
                            delete props.minskew;
                            delete props.minEndpoint;
                            delete props.min;
                            domConstruct.create("br", null, place);
                        }
                        if (props.slaves) {
                            var table = domConstruct.create("table", { border: 1, cellspacing: 0, width: "100%" }, place);
                            var tr = domConstruct.create("tr", null, table);
                            domConstruct.create("th", { innerHTML: this.i18n.Slaves }, tr);
                            domConstruct.create("th", { innerHTML: this.i18n.Started }, tr);
                            domConstruct.create("th", { innerHTML: this.i18n.Stopped }, tr);
                            tr = domConstruct.create("tr", null, table);
                            domConstruct.create("td", { innerHTML: props.slaves }, tr);
                            domConstruct.create("td", { innerHTML: props.started }, tr);
                            domConstruct.create("td", { innerHTML: props.stopped }, tr);
                            delete props.slaves;
                            delete props.started;
                            delete props.stopped;
                            domConstruct.create("br", null, place);
                        }
                        var first = true;
                        var table = {};
                        var tr = {};
                        for (var key in props) {
                            if (key[0] == "_")
                                continue;

                            if (first) {
                                first = false;
                                table = domConstruct.create("table", { border: 1, cellspacing: 0, width: "100%" }, place);
                                tr = domConstruct.create("tr", null, table);
                                domConstruct.create("th", { innerHTML: this.i18n.Property }, tr);
                                domConstruct.create("th", { innerHTML: this.i18n.Value }, tr);
                            }
                            tr = domConstruct.create("tr", null, table);
                            domConstruct.create("td", { innerHTML: key }, tr);
                            domConstruct.create("td", { innerHTML: props[key] }, tr);
                        }
                        if (first == false) {
                            domConstruct.create("br", null, place);
                        }
                    }
                }
            },

            createPlugin: function () {
                if (!this.hasPlugin()) {
                    if (this._isPluginInstalled) {
                        this.pluginID = this.id + "Plugin";
                        if (this.isIE || this.isIE11) {
                            this.graphContentPane.domNode.innerHTML = '<object type="application/x-hpccsystemsgraphviewcontrol" '
                                                    + 'id="' + this.pluginID + '" '
                                                    + 'name="' + this.pluginID + '" '
                                                    + 'width="100%" '
                                                    + 'height="100%">'
                                                    + '</object>';
                        } else {
                            this.graphContentPane.domNode.innerHTML = '<embed type="application/x-hpccsystemsgraphviewcontrol" '
                                                    + 'id="' + this.pluginID + '" '
                                                    + 'name="' + this.pluginID + '" '
                                                    + 'width="100%" '
                                                    + 'height="100%">'
                                                    + '</embed>';
                        }
                        var context = this;
                        this.checkPluginLoaded().then(lang.hitch(this, function (response) {
                            this.version = response;
                            this._plugin = dom.byId(context.pluginID);
                            this.registerEvents();
                            this.emit("ready");
                        }));
                    } else {
                        domConstruct.create("div", {
                            innerHTML: "<h4>" + this.i18n.GraphView + "</h4>" +
                                        "<p>" + this.i18n.Toenablegraphviews + ":</p>" +
                                        this.getResourceLinks()
                        }, this.graphContentPane.domNode);
                    }
                }
            },

            checkPluginLoaded: function () {
                var deferred = new Deferred();
                var context = this;
                var doCheck = function () {
                    domNode = dom.byId(context.pluginID);
                    if (domNode && domNode.version) {
                        return {
                            version: domNode.version,
                            major: domNode.version_major,
                            minor: domNode.version_minor,
                            point: domNode.version_point,
                            sequence: domNode.version_sequence
                        };
                    }
                    return null;
                };
                var doBackGroundCheck = function () {
                    setTimeout(function () {
                        var version = doCheck();
                        if (version) {
                            deferred.resolve(version);
                        } else {
                            doBackGroundCheck();
                        }
                    }, 20);
                };
                doBackGroundCheck();
                return deferred.promise;
            },

            getResourceLinks: function () {
                return "<a href=\"http://hpccsystems.com/download/free-community-edition/graph-control\" target=\"_blank\">" + this.i18n.BinaryInstalls + "</a><br/>" +
                "<a href=\"https://github.com/hpcc-systems/GraphControl\" target=\"_blank\">" + this.i18n.SourceCode + "</a><br/><br/>" +
                "<a href=\"http://hpccsystems.com\" target=\"_blank\">" + this.i18n.HPCCSystems + "</a>"
            },

            setMessage: function (message) {
                var deferred = new Deferred();
                var retVal = this._plugin ? this._plugin.setMessage(message) : null;
                setTimeout(function () {
                    deferred.resolve(retVal);
                }, 20);
                return deferred.promise;
            },

            getLocalisedXGMML: function (selectedItems, depth, distance) {
                if (this.hasPlugin()) {
                    return this._plugin.getLocalisedXGMML(selectedItems, depth, distance);
                }
                return null;
            },

            getCurrentGraphView: function () {
                return this.graphViewHistory.getCurrent();
            },

            getGraphView: function (rootGlobalIDs, depth, distance, selectedGlobalIDs) {
                var retVal = new GraphView(this, rootGlobalIDs, depth, distance, selectedGlobalIDs);
                if (this.graphViewHistory.has(retVal.id)) {
                    retVal = this.graphViewHistory.get(retVal.id);
                    retVal.selectedGlobalIDs = selectedGlobalIDs ? selectedGlobalIDs : rootGlobalIDs;
                } else {
                    this.graphViewHistory.set(retVal.id, retVal);
                }
                return retVal;
            },

            mergeSVG: function (svg) {
                if (this.hasPlugin()) {
                    return this._plugin.mergeSVG(svg);
                }
                return null;
            },

            startCachedLayout: function (svg) {
                if (this.hasPlugin()) {
                    var context = this;
                    this.setMessage(this.i18n.LoadingCachedLayout).then(function (response) {
                        context._plugin.mergeSVG(svg);
                        context._onLayoutFinished();
                    });
                }
            },

            startLayout: function (layout) {
                if (this.hasPlugin()) {
                    this.setMessage(this.i18n.PerformingLayout);
                    this._plugin.startLayout(layout);
                }
            },

            _onLayoutFinished: function() {
                this.setMessage('');
                this.centerOnItem(0, true);
                this.dot = this._plugin.getDOT();
                if (this.onLayoutFinished) {
                    this.onLayoutFinished();
                }
            },

            find: function (findText) {
                if (this.hasPlugin()) {
                    return this._plugin.find(findText);
                }
                return [];
            },

            findAsGlobalID: function (findText) {
                if (this.hasPlugin()) {
                    var items = this.find(findText);
                    var foundItem = this.getItem(findText);
                    if (foundItem) {
                        items.unshift(foundItem);
                    }
                    var globalIDs = [];
                    for (var i = 0; i < items.length; ++i) {
                        globalIDs.push(this._plugin.getGlobalID(items[i]));
                    }
                    return globalIDs;
                }
                return [];
            },

            setScale: function(percent) {
                if (this.hasPlugin()) {
                    return this._plugin.setScale(percent);
                }
                return 100;
            },

            centerOnItem: function (item, scaleToFit, widthOnly) {
                if (this.hasPlugin()) {
                    return this._plugin.centerOnItem(item, scaleToFit, widthOnly);
                }
                return null;
            },

            centerOnGlobalID: function (globalID, scaleToFit, widthOnly) {
                if (this.hasPlugin()) {
                    var item = this.getItem(globalID);
                    if (item) {
                        return this.centerOnItem(item, scaleToFit, widthOnly);
                    }
                }
                return null;
            },

            setSelected: function (items) {
                if (this.hasPlugin()) {
                    return this._plugin.setSelected(items);
                }
                return null;
            },

            setSelectedAsGlobalID: function (items) {
                if (this.hasPlugin()) {
                    var retVal = this._plugin.setSelectedAsGlobalID(items);
                    this.refreshActionState();
                    return retVal;
                }
                return null;
            },

            getSelection: function () {
                if (this.hasPlugin()) {
                    return this._plugin.getSelection();
                }
                return [];
            },

            getSelectionAsGlobalID: function () {
                if (this.hasPlugin()) {
                    return this._plugin.getSelectionAsGlobalID();
                }
                return [];
            },

            getItem: function (globalID) {
                if (this.hasPlugin()) {
                    var retVal = this._plugin.getItem(globalID);
                    if (retVal === -1) {
                        retVal = null;
                    }
                    return retVal;
                }
                return null;
            },

            getItems: function (globalIDs) {
                var retVal = [];
                if (this.hasPlugin()) {
                    arrayUtil.forEach(globalIDs, function (globalID, idx) {
                        var item = this.getItem(globalID);
                        if (item !== null) {
                            retVal.push(item);
                        }
                    }, this);
                }
                return retVal;
            },

            hide: function () {
                if (this.hasPlugin()) {
                    dojo.style(this._plugin, "width", "1px");
                    dojo.style(this._plugin, "height", "1px");
                }
            },

            show: function () {
                if (this.hasPlugin()) {
                    dojo.style(this._plugin, "width", "100%");
                    dojo.style(this._plugin, "height", "100%");
                }
            },

            watchSplitter: function (splitter) {
                if (has("chrome")) {
                    //  Chrome can ignore splitter events
                    return;
                }
                var context = this;
                dojo.connect(splitter, "_startDrag", function () {
                    context.hide();
                });
                dojo.connect(splitter, "_stopDrag", function (evt) {
                    context.show();
                });
            },

            watchSelect: function (select) {
                if (select) {
                    //  Only chrome needs to monitor select drop downs.
                    var context = this;
                    select.watch("_opened", function () {
                        if (select._opened) {
                            context.hide();
                        } else {
                            context.show();
                        }
                    });
                }
            },

            watchStyleChange: function () {
                ESPUtil.MonitorVisibility(this, function (visibility, node) {
                    if (visibility) {
                        dojo.style(node, "width", "100%");
                        dojo.style(node, "height", "100%");
                        dojo.style(node.firstChild, "width", "100%");
                        dojo.style(node.firstChild, "height", "100%");
                        return true;
                    } else {
                        dojo.style(node, "width", "1px");
                        dojo.style(node, "height", "1px");
                        dojo.style(node.firstChild, "width", "1px");
                        dojo.style(node.firstChild, "height", "1px");
                        return true;
                    }
                });
            },

            getDotMetaAttributes: function () {
                if (this._plugin && this._plugin.getControlProperty) {
                    return this._plugin.getControlProperty(this.DOT_META_ATTR);
                }
                return "";
            },

            setDotMetaAttributes: function (dotMetaAttr) {
                if (this._plugin && this._plugin.setControlProperty) {
                    this._plugin.setControlProperty(this.DOT_META_ATTR, dotMetaAttr);
                }
            },

            getProperty: function (item, key) {
                if (this._plugin && this._plugin.getProperty) {
                    return this._plugin.getProperty(item, key);
                }
                return "";
            },

            setProperty: function (item, key, value) {
                if (this._plugin && this._plugin.setProperty) {
                    this._plugin.setProperty(item, key, value);
                }
            },

            getProperties: function (item) {
                if (this.hasPlugin()) {
                    return this._plugin.getProperties(item);
                }
                return [];
            },

            getTreeWithProperties: function () {
                if (this.hasPlugin()) {
                    return this._plugin.getTreeWithProperties();
                }
                return [];
            },

            getSubgraphsWithProperties: function () {
                if (this.hasPlugin()) {
                    return this._plugin.getSubgraphsWithProperties();
                }
                return [];
            },

            getVertices: function () {
                if (this.hasPlugin()) {
                    return this._plugin.getVertices();
                }
                return [];
            },

            getVerticesWithProperties: function () {
                if (this.hasPlugin()) {
                    return this._plugin.getVerticesWithProperties();
                }
                return [];
            },

            getEdgesWithProperties: function () {
                if (this.hasPlugin()) {
                    return this._plugin.getEdgesWithProperties();
                }
                return [];
            },

            registerEvents: function () {
                if (!this.eventsRegistered) {
                    this.eventsRegistered = true;
                    var context = this;
                    this.registerEvent("MouseDoubleClick", function (item, keyState) {
                        context.onDoubleClick(context._plugin.getGlobalID(item), keyState);
                    });
                    this.registerEvent("LayoutFinished", function () {
                        context._onLayoutFinished();
                    });
                    this.registerEvent("SelectionChanged", function (items) {
                        context.refreshActionState();
                        context.onSelectionChanged(items);
                    });
                }
            },

            registerEvent: function (evt, func) {
                if (this.hasPlugin()) {
                    if (this._plugin instanceof Evented) {
                        this._plugin.on(evt, func);
                    } else if (this.isIE11) {
                        this._plugin["on" + evt] = func;
                    } else if (this._plugin.attachEvent !== undefined) {
                        return this._plugin.attachEvent("on" + evt, func);
                    } else {
                        return this._plugin.addEventListener(evt, func, false);
                    }
                }
                return false;
            },

            refreshActionState: function () {
                this.setDisabled(this.id + "Previous", !this.graphViewHistory.hasPrevious(), "iconLeft", "iconLeftDisabled");
                this.setDisabled(this.id + "Next", !this.graphViewHistory.hasNext(), "iconRight", "iconRightDisabled");
                this.setDisabled(this.id + "SyncSelection", !this.getSelection().length, "iconSync", "iconSyncDisabled");
            },

            refreshRootState: function (selectedGlobalIDs) {
                var depthDisabled = false;
                var distanceDisabled = false;
                if (selectedGlobalIDs) {
                    var typeSummary = this.getTypeSummary(selectedGlobalIDs);
                    depthDisabled = !selectedGlobalIDs.length || !(typeSummary.Graph || typeSummary.Cluster);
                    distanceDisabled = !(typeSummary.Vertex || typeSummary.Edge);
                }
                depthDisabled = depthDisabled || (this.hasOptions() && !this.option("subgraph"))

                this.setDisabled(this.id + "Depth", depthDisabled);
                this.setDisabled(this.id + "Distance", distanceDisabled);
                this.setDisabled(this.id + "OptionsDropDown", !this.hasOptions());
            }
        });
    });
