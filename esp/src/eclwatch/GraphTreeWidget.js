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
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-style",
    "dojo/on",
    "dojo/html",
    "dojo/topic",

    "dijit/registry",
    "dijit/Dialog",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/CheckedMenuItem",

    "dojox/html/entities",

    "dgrid/tree",

    "hpcc/_Widget",
    "hpcc/GraphWidget",
    "hpcc/JSGraphWidget",
    "hpcc/ESPUtil",
    "hpcc/ESPWorkunit",
    "hpcc/TimingTreeMapWidget",
    "hpcc/WsWorkunits",

    "dojo/text!../templates/GraphTreeWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/StackContainer",
    "dijit/layout/StackController",
    "dijit/layout/ContentPane",
    "dijit/Dialog",
    "dijit/form/TextBox",
    "dijit/form/SimpleTextarea",
    "dijit/form/NumberSpinner",
    "dijit/form/DropDownButton",
    "dijit/form/Select"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, Deferred, dom, domConstruct, domStyle, on, html, topic,
            registry, Dialog, Menu, MenuItem, MenuSeparator, CheckedMenuItem,
            entities,
            tree,
            _Widget, GraphWidget, JSGraphWidget, ESPUtil, ESPWorkunit, TimingTreeMapWidget, WsWorkunits,
            template) {

    return declare("GraphTreeWidget", [_Widget], {
        templateString: template,
        baseClass: "GraphTreeWidget",
        i18n: nlsHPCC,

        graphType: dojoConfig.isPluginInstalled() ? "GraphWidget" : "JSGraphWidget",
        graphName: "",
        wu: null,
        global: null,
        main: null,
        subgraphsGrid: null,
        verticesGrid: null,
        edgesGrid: null,
        xgmmlDialog: null,
        infoDialog: null,
        findText: "",
        found: [],
        foundIndex: 0,

        constructor: function (args) {
            if (args.forceNative) {
                this.graphType = "GraphWidget";
            } else {
                this.graphType = "JSGraphWidget";
            }
        },

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this._initGraphControls();
            this._initTimings();
            this._initActivitiesMap();
            this._initDialogs();
            var context = this;
            topic.subscribe(this.id + "OverviewTabContainer-selectChild", function (topic) {
                context.refreshActionState();
            });
        },

        startup: function (args) {
            this.inherited(arguments);

            this._initTree();
            this._initSubgraphs();
            this._initVertices();
            this._initEdges();

            var splitter = this.widget.BorderContainer.getSplitter("left");
            this.main.watchSplitter(splitter);

            splitter = this.widget.SideBorderContainer.getSplitter("bottom");
            this.main.watchSplitter(splitter);

            this.main.watchSelect(registry.byId(this.id + "AdvancedMenu"));

            this.refreshActionState();
        },

        resize: function (args) {
            this.inherited(arguments);
            this.widget.BorderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.xgmmlDialog.destroyRecursive();
            this.infoDialog.destroyRecursive();
            this.inherited(arguments);
        },

        //  Implementation  ---
        _initGraphControls: function () {
            var context = this;
            this.global = registry.byId(this.id + "GlobalGraphWidget");

            this.main = registry.byId(this.id + "MainGraphWidget");
            this.main.onSelectionChanged = function (items) {
                context.syncSelectionFrom(context.main);
            };
            this.main.onDoubleClick = function (globalID, keyState) {
                if (keyState && context.main.KeyState_Shift) {
                    context.main._onSyncSelection();
                } else {
                    context.main.centerOn(globalID);
                }
                context.syncSelectionFrom(context.main);
            };
        },

        _initTimings: function () {
            var context = this;
            this.widget.TimingsTreeMap.onClick = function (value) {
                context.syncSelectionFrom(context.widget.TimingsTreeMap);
            }
        },

        _initActivitiesMap: function () {
            var context = this;
            this.widget.ActivitiesTreeMap.onClick = function (value) {
                context.syncSelectionFrom(context.widget.ActivitiesTreeMap);
            }
        },

        _initDialogs: function () {
            var context = this;

            this.infoDialog = registry.byId(this.id + "InfoDialog");
            on(dom.byId(this.id + "InfoDialogCancel"), "click", function (event) {
                context.infoDialog.hide();
            });

            this.xgmmlDialog = registry.byId(this.id + "XGMMLDialog");
            this.xgmmlTextArea = registry.byId(this.id + "XGMMLTextArea");
            on(dom.byId(this.id + "XGMMLDialogApply"), "click", function (event) {
                context.xgmmlDialog.hide();
                if (context.xgmmlDialog.get("hpccMode") === "XGMML") {
                    var xgmml = context.xgmmlTextArea.get("value");
                    context.loadGraphFromXGMML(xgmml);
                } else if (context.xgmmlDialog.get("hpccMode") === "DOT") {
                    var dot = context.xgmmlTextArea.get("value");
                    context.loadGraphFromDOT(dot);
                } else if (context.xgmmlDialog.get("hpccMode") === "DOTATTRS") {
                    var dotAttrs = context.xgmmlTextArea.get("value");
                    context.global.setDotMetaAttributes(dotAttrs);
                    context.main.setDotMetaAttributes(dotAttrs);
                    context._onMainSync();
                }
            });
            on(dom.byId(this.id + "XGMMLDialogCancel"), "click", function (event) {
                context.xgmmlDialog.hide();
            });
        },

        _initItemGrid: function (grid) {
            var context = this;
            grid.on("dgrid-select, dgrid-deselect", function (event) {
                context.syncSelectionFrom(grid);
            });
            grid.on(".dgrid-row:dblclick", function (evt) {
                var item = grid.row(evt).data;
                if (item._globalID) {
                    var mainItem = context.main.getItem(item._globalID);
                    context.main.centerOnItem(mainItem, true);
                }
            });
        },

        _initTree: function () {
            this.treeStore = this.global.createTreeStore();
            this.treeGrid = new declare([ESPUtil.Grid(false, true)])({
                treeDepth: this.main.getDepth(),
                store: this.treeStore
            }, this.id + "TreeGrid");
            this._initItemGrid(this.treeGrid);
            this.initContextMenu();
        },

        initContextMenu: function () {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "TreeGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: this.i18n.ExpandAll,
                onClick: function (evt) {
                    context.treeGrid.set("treeDepth", 9999);
                    context.treeGrid.refresh();
                }
            }));
            pMenu.addChild(new MenuItem({
                label: this.i18n.CollapseAll,
                onClick: function (evt) {
                    context.treeGrid.set("treeDepth", 1);
                    context.treeGrid.refresh();
                }
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new CheckedMenuItem({
                label: this.i18n.Activities,
                checked: false,
                onClick: function (evt) {
                    if (this.checked) {
                        context.treeGrid.set("query", {
                            id: "0"
                        });
                    } else {
                        context.treeGrid.set("query", {
                            id: "0",
                            __hpcc_notActivity: true
                        });
                    }
                }
            }));
        },

        _initSubgraphs: function () {
            this.subgraphsStore = this.global.createStore();
            this.subgraphsGrid = new declare([ESPUtil.Grid(false, true)])({
                store: this.subgraphsStore
            }, this.id + "SubgraphsGrid");

            this._initItemGrid(this.subgraphsGrid);
        },

        _initVertices: function () {
            this.verticesStore =  this.global.createStore();
            this.verticesGrid = new declare([ESPUtil.Grid(false, true)])({
                store: this.verticesStore
            }, this.id + "VerticesGrid");

            this._initItemGrid(this.verticesGrid);
        },

        _initEdges: function () {
            this.edgesStore =  this.global.createStore();
            this.edgesGrid = new declare([ESPUtil.Grid(false, true)])({
                store: this.edgesStore
            }, this.id + "EdgesGrid");

            this._initItemGrid(this.edgesGrid);
        },

        _onRefresh: function () {
            this.refreshData();
        },

        _onTreeRefresh: function () {
            this.treeGrid.set("treeDepth", this.main.getDepth());
            this.treeGrid.refresh();
        },

        _onChangeActivityMetric: function () {
            var metric = this.widget.ActivityMetric.get("value");
            this.widget.ActivitiesTreeMap.setActivityMetric(metric);
        },

        _doFind: function (prev) {
            if (this.findText !== this.widget.FindField.value) {
                this.findText = this.widget.FindField.value;
                this.found = this.global.findAsGlobalID(this.findText);
                this.syncSelectionFrom(this.found);
                this.foundIndex = -1;
            }
            this.foundIndex += prev ? -1 : +1;
            if (this.foundIndex < 0) {
                this.foundIndex = this.found.length - 1;
            } else if (this.foundIndex >= this.found.length) {
                this.foundIndex = 0;
            }
            if (this.found.length) {
                this.main.centerOnGlobalID(this.found[this.foundIndex], true);
            }
            this.refreshActionState();
        },

        _onFind: function (prev) {
            this.findText = "";
            this._doFind(false);
        },

        _onFindNext: function () {
            this._doFind(false);
        },

        _onFindPrevious: function () {
            this._doFind(true);
        },

        _onAbout: function () {
            html.set(dom.byId(this.id + "InfoDialogContent"), "<div style='width: 320px; height: 120px; text-align: center;'><p>" + this.i18n.Version + ":  " + this.main.getVersion() + "</p><p>" + this.main.getResourceLinks() + "</p>");
            this.infoDialog.set("title", this.i18n.AboutHPCCSystemsGraphControl);
            this.infoDialog.show();
        },

        _onGetSVG: function () {
            html.set(dom.byId(this.id + "InfoDialogContent"), "<textarea rows='25' cols='80'>" + entities.encode(this.main.getSVG()) + "</textarea>");
            this.infoDialog.set("title", this.i18n.SVGSource);
            this.infoDialog.show();
        },

        _onRenderSVG: function () {
            var context = this
            this.main.localLayout(function (svg) {
                html.set(dom.byId(context.id + "InfoDialogContent"), "<div style='border: 1px inset grey; width: 640px; height: 480px; overflow : auto; '>" + svg + "</div>");
                context.infoDialog.set("title", this.i18n.RenderedSVG);
                context.infoDialog.show();
            });
        },

        _onGetXGMML: function () {
            this.xgmmlDialog.set("title", this.i18n.XGMML);
            this.xgmmlDialog.set("hpccMode", "XGMML");
            this.xgmmlTextArea.set("value", this.main.getXGMML());
            this.xgmmlDialog.show();
        },

        _onEditDOT: function () {
            this.xgmmlDialog.set("title", this.i18n.DOT);
            this.xgmmlDialog.set("hpccMode", "DOT");
            this.xgmmlTextArea.set("value", this.main.getDOT());
            this.xgmmlDialog.show();
        },

        _onGetGraphAttributes: function () {
            this.xgmmlDialog.set("title", this.i18n.DOTAttributes);
            this.xgmmlDialog.set("hpccMode", "DOTATTRS");
            this.xgmmlTextArea.set("value", this.global.getDotMetaAttributes());
            this.xgmmlDialog.show();
        },

        isWorkunit: function () {
            return lang.exists("params.Wuid", this);
        },

        isQuery: function () {
            return lang.exists("params.QueryId", this);
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (this.global._plugin) {
                this.doInit(params);
            } else {
                this.global.on("ready", lang.hitch(this, function (evt) {
                    this.doInit(params);
                }));
            }
        },

        refresh: function (params) {
            if (params.SubGraphId) {
                this.syncSelectionFrom([params.SubGraphId]);
            }
        },

        doInit: function (params) {
            if (this.global.version.major < 5) {
                dom.byId(this.id + "Warning").innerHTML = this.i18n.WarnOldGraphControl + " (" + this.global.version.version + ")";
            }

            if (params.SafeMode && params.SafeMode !== "false") {
                this.main.depth.set("value", 1);
                var dotAttrs = this.global.getDotMetaAttributes();
                dotAttrs = dotAttrs.replace("\n//graph[splines=\"line\"];", "\ngraph[splines=\"line\"];");
                this.global.setDotMetaAttributes(dotAttrs);
            } else {
                var dotAttrs = this.global.getDotMetaAttributes();
                dotAttrs = dotAttrs.replace("\ngraph[splines=\"line\"];", "\n//graph[splines=\"line\"];");
                this.global.setDotMetaAttributes(dotAttrs);
            }

            this.graphName = params.GraphName;
            this.subGraphId = params.SubGraphId;
            this.widget.TimingsTreeMap.init(lang.mixin({
                query: {
                    graphsOnly: true,
                    graphName: this.graphName,
                    subGraphId: "*"
                },
                hideHelp: true
            }, params));

            this.widget.ActivitiesTreeMap.init(lang.mixin({
                query: {
                    activitiesOnly: true,
                    graphName: this.graphName,
                    subGraphId: "*"
                },
                hideHelp: true
            }, params));


            if (this.isWorkunit()) {
                this.wu = ESPWorkunit.Get(params.Wuid);

                var firstLoad = true;
                var context = this;
                this.wu.monitor(function () {
                    context.wu.getInfo({
                        onGetApplicationValues: function (applicationValues) {
                        },
                        onGetGraphs: function (graphs) {
                            if (firstLoad === true) {
                                firstLoad = false;
                                context.loadGraphFromWu(context.wu, context.graphName, context.subGraphId);
                            } else {
                                context.refreshGraphFromWU(context.wu, context.graphName, context.subGraphId);
                            }
                        },
                        onGetTimers: function (timers) {
                            context.graphTimers = context.wu.getGraphTimers(context.GraphName);
                        }
                    });
                });
            } else if (this.isQuery()) {
                this.targetQuery = params.Target;
                this.queryId = params.QueryId;

                this.loadGraphFromQuery(this.targetQuery, this.queryId, this.graphName);
            }
        },

        refreshData: function () {
            if (this.isWorkunit()) {
                this.loadGraphFromWu(this.wu, this.graphName, this.subGraphId, true);
            } else if (this.isQuery()) {
                this.loadGraphFromQuery(this.targetQuery, this.queryId, this.graphName);
            }
        },

        loadGraphFromXGMML: function (xgmml) {
            if (this.global.loadXGMML(xgmml, false, this.graphTimers, true)) {
                this.global.setMessage("...");  //  Just in case it decides to render  ---
                var mainRoot = [0];
                var complexityInfo = this.global.getComplexityInfo();
                if (this.params.SubGraphId) {
                    mainRoot = [this.params.SubGraphId];
                } else if (complexityInfo.isComplex()) {
                    if (confirm(lang.replace(this.i18n.ComplexityWarning, complexityInfo) + "\n" + this.i18n.ManualTreeSelection)) {
                        mainRoot = [];
                    }
                }
                this.loadTree();
                this.loadSubgraphs();
                this.loadVertices();
                this.loadEdges();
                this.syncSelectionFrom(mainRoot);
            }
        },

        mergeGraphFromXGMML: function (xgmml) {
            if (this.global.loadXGMML(xgmml, true, this.graphTimers, true)) {
                this.global.setMessage("...");  //  Just in case it decides to render  ---
                this.refreshMainXGMML();
                this.loadSubgraphs();
                this.loadVertices();
                this.loadEdges();
            }
        },

        loadGraphFromDOT: function (dot) {
            this.global.loadDOT(dot);
            this.global.setMessage("...");  //  Just in case it decides to render  ---
            this.setMainRootItems([]);
            this.loadSubgraphs();
            this.loadVertices();
            this.loadEdges();
        },

        loadGraphFromWu: function (wu, graphName, subGraphId, refresh) {
            var deferred = new Deferred();
            this.main.setMessage(this.i18n.FetchingData);
            var context = this;
            wu.fetchGraphXgmmlByName(graphName, subGraphId, function (xgmml, svg) {
                context.main.setMessage("");
                context.loadGraphFromXGMML(xgmml, svg);
                deferred.resolve();
            }, refresh);
            return deferred.promise;
        },

        refreshGraphFromWU: function (wu, graphName, subGraphId) {
            var context = this;
            wu.fetchGraphXgmmlByName(graphName, subGraphId, function (xgmml) {
                context.mergeGraphFromXGMML(xgmml);
            }, true);
        },

        loadGraphFromQuery: function (targetQuery, queryId, graphName) {
            this.main.setMessage(this.i18n.FetchingData);
            var context = this;
            WsWorkunits.WUQueryGetGraph({
                request: {
                    Target: targetQuery,
                    QueryId: queryId,
                    GraphName: graphName
                }
            }).then(function(response){
                context.main.setMessage("");
                if(lang.exists("WUQueryGetGraphResponse.Graphs.ECLGraphEx", response)){
                    if(response.WUQueryGetGraphResponse.Graphs.ECLGraphEx.length > 0){
                        context.loadGraphFromXGMML(response.WUQueryGetGraphResponse.Graphs.ECLGraphEx[0].Graph, "");
                    }
                }
            });
        },

        refreshGraphFromQuery: function (targetQuery, queryId, graphName) {
            var context = this;
            WsWorkunits.WUQueryGetGraph({
                request: {
                    Target: targetQuery,
                    QueryId: queryId,
                    GraphName: graphName
                }
            }).then(function (response) {
                if (lang.exists("WUQueryGetGraphResponse.Graphs.ECLGraphEx", response)) {
                    if (response.WUQueryGetGraphResponse.Graphs.ECLGraphEx.length > 0) {
                        context.mergeGraphFromXGMML(response.WUQueryGetGraphResponse.Graphs.ECLGraphEx[0].Graph);
                    }
                }
            });
        },

        loadTree: function () {
            var treeData = this.global.getTreeWithProperties();
            this.treeStore.setTree(treeData);
            var context = this;
            var columns = [
                tree({
                    field: "id",
                    label: this.i18n.ID, width: 150,
                    collapseOnRefresh: true,
                    shouldExpand: function (row, level, previouslyExpanded) {
                        if (previouslyExpanded !== undefined) {
                            return previouslyExpanded;
                        } else if (level < context.treeGrid.get("treeDepth")) {
                            return true;
                        }
                        return false;
                    },
                    formatter: function (_id, row) {
                        var img = dojoConfig.getImageURL("file.png");
                        var label = _id + " - ";
                        switch (row._globalType) {
                            case "Graph":
                                img = dojoConfig.getImageURL("server.png");
                                label = context.params.GraphName + " (" + row._children.length + ")";
                                break;
                            case "Cluster":
                                img = dojoConfig.getImageURL("folder.png");
                                label += context.i18n.Subgraph + " (" + row._children.length + ")";
                                break;
                            case "Vertex":
                                label += row.label;
                                break;
                        }
                        return "<img src='" + img + "'/>&nbsp;" + label;
                    }
                })
            ];
            if (this.isWorkunit()) {
                this.treeStore.appendColumns(columns, ["name"], ["DescendantCount", "ecl", "definition", "SubgraphCount", "ActivityCount", "ChildCount", "Depth"]);
            } else if (this.isQuery()) {
                this.treeStore.appendColumns(columns, ["localTime", "totalTime", "label", "ecl"], ["DescendantCount", "definition", "SubgraphCount", "ActivityCount", "ChildCount", "Depth"]);
            }
            this.treeGrid.set("query", {
                id: "0",
                __hpcc_notActivity: true
            });
            this.treeGrid.set("columns", columns);
            this.treeGrid.refresh();
        },

        loadSubgraphs: function () {
            var subgraphs = this.global.getSubgraphsWithProperties();
            this.subgraphsStore.setData(subgraphs);
            var columns = [
                {
                    label: this.i18n.ID, field: "id", width: 54,
                    formatter: function (_id, row) {
                        var img = dojoConfig.getImageURL("folder.png");
                        return "<img src='" + img + "'/>&nbsp;" + _id;
                    }
                }
            ];
            this.subgraphsStore.appendColumns(columns, [this.i18n.TimeSeconds, "DescendantCount", "SubgraphCount", "ActivityCount"], ["ChildCount", "Depth"]);
            this.subgraphsGrid.set("columns", columns);
            this.subgraphsGrid.refresh();
        },

        loadVertices: function () {
            var vertices = this.global.getVerticesWithProperties();
            this.verticesStore.setData(vertices);
            var columns = [
                {
                    label: this.i18n.ID, field: "id", width: 54,
                    formatter: function (_id, row) {
                        var img = dojoConfig.getImageURL("file.png");
                        return "<img src='" + img + "'/>&nbsp;" + _id;
                    }
                },
                { label: this.i18n.Label, field: "label", width: 150 }
            ];
            this.verticesStore.appendColumns(columns, ["name"], ["ecl", "definition"], null, true);
            this.verticesGrid.set("columns", columns);
            this.verticesGrid.refresh();
            this.widget.ActivityMetric.set("options", arrayUtil.map(arrayUtil.filter(columns, function (col, idx) {
                return col.label.indexOf("Time") === 0 ||
                        col.label.indexOf("Size") === 0 ||
                        col.label.indexOf("Skew") === 0 ||
                        col.label.indexOf("Num") === 0;
            }), function (col, idx) {
                return {
                    label: col.label,
                    value: col.label,
                    selected: col.label === "TimeMaxLocalExecute"
                };
            }).sort(function (l, r) {
                if (l.label < r.label) {
                    return -1;
                } else if (l.label > r.label) {
                    return 1;
                }
                return 0;
            }));
            this.widget.ActivitiesTreeMap.setActivities(vertices, true);
            this.widget.ActivityMetric.set("value", "TimeMaxLocalExecute");
        },

        loadEdges: function () {
            var edges = this.global.getEdgesWithProperties();
            this.edgesStore.setData(edges);
            var columns = [
                { label: this.i18n.ID, field: "id", width: 50 }
            ];
            this.edgesStore.appendColumns(columns, ["label", "count"], ["source", "target"]);
            this.edgesGrid.set("columns", columns);
            this.edgesGrid.refresh();
        },

        inSyncSelectionFrom: false,
        syncSelectionFrom: function (sourceControl) {
            if (!this.inSyncSelectionFrom) {
                this._syncSelectionFrom(sourceControl);
            }
        },

        _syncSelectionFrom: dojoConfig.debounce(function (sourceControlOrGlobalIDs) {
            this.inSyncSelectionFrom = true;
            var sourceControl = sourceControlOrGlobalIDs instanceof Array ? null : sourceControlOrGlobalIDs;
            var selectedGlobalIDs = sourceControlOrGlobalIDs instanceof Array ? sourceControlOrGlobalIDs : [];
            if (sourceControl) {
                //  Get Selected Items  ---
                if (sourceControl === this.widget.TimingsTreeMap) {
                    var items = sourceControl.getSelected();
                    for (var i = 0; i < items.length; ++i) {
                        if (items[i].SubGraphId) {
                            selectedGlobalIDs.push(items[i].SubGraphId);
                        }
                    }
                } else if (sourceControl === this.widget.ActivitiesTreeMap) {
                    var items = sourceControl.getSelected();
                    for (var i = 0; i < items.length; ++i) {
                        if (items[i].ActivityID) {
                            selectedGlobalIDs.push(items[i].ActivityID);
                        }
                    }
                } else if (sourceControl === this.verticesGrid || sourceControl === this.edgesGrid || sourceControl === this.subgraphsGrid || sourceControl === this.treeGrid) {
                    var items = sourceControl.getSelected();
                    for (var i = 0; i < items.length; ++i) {
                        if (lang.exists("_globalID", items[i])) {
                            selectedGlobalIDs.push(items[i]._globalID);
                        }
                    }
                } else if (sourceControl === this.found) {
                    selectedGlobalIDs = this.found;
                } else {
                    selectedGlobalIDs = sourceControl.getSelectionAsGlobalID();
                }
            }

            //  Set Selected Items  ---
            if (sourceControl !== this.treeGrid) {
                this.treeGrid.setSelection(selectedGlobalIDs);
            }
            if (sourceControl !== this.widget.TimingsTreeMap) {
                this.widget.TimingsTreeMap.setSelectedAsGlobalID(selectedGlobalIDs);
            }
            if (sourceControl !== this.widget.ActivitiesTreeMap) {
                this.widget.ActivitiesTreeMap.setSelectedAsGlobalID(selectedGlobalIDs);
            }
            if (sourceControl !== this.subgraphsGrid && this.subgraphsGrid.store) {
                this.subgraphsGrid.setSelection(selectedGlobalIDs);
            }
            if (sourceControl !== this.verticesGrid && this.verticesGrid.store) {
                this.verticesGrid.setSelection(selectedGlobalIDs);
            }
            if (sourceControl !== this.edgesGrid && this.edgesGrid.store) {
                this.edgesGrid.setSelection(selectedGlobalIDs);
            }

            //  Refresh Graph Controls  ---
            if (sourceControl !== this.main) {
                this.setMainRootItems(selectedGlobalIDs);
            }

            var propertiesDom = dom.byId(this.id + "Properties");
            propertiesDom.innerHTML = "";
            for (var i = 0; i < selectedGlobalIDs.length; ++i) {
                this.global.displayProperties(this.wu, selectedGlobalIDs[i], propertiesDom);
            }
            var context = this;
            if (selectedGlobalIDs.length) {
                var edges = arrayUtil.filter(selectedGlobalIDs, function (id) {
                    return id && id.indexOf && id.indexOf("_") >= 0;
                });
                if (edges.length === 1) {
                    WsWorkunits.WUCDebug(context.params.Wuid, "<debug:print edgeId='" + edges[0] + "'/>").then(function (response) {
                        if (lang.exists("WUDebugResponse.Result", response)) {
                            context.global.displayTrace(response.WUDebugResponse.Result, propertiesDom);
                        }
                    });
                }
            }
            this.inSyncSelectionFrom = false;
        }, 500, false),

        resetPage: function () {
            this.main.clear();
        },

        setMainRootItems: function (globalIDs) {
            var graphView = this.global.getGraphView(globalIDs, this.main.getDepth(), this.main.distance.get("value"), this.main.option("subgraph"), this.main.option("vhidespills"));
            return graphView.navigateTo(this.main);
        },

        refreshMainXGMML: function () {
            var graphView = this.main.getCurrentGraphView();
            graphView.refreshXGMML(this.main);
        },

        displayGraphs: function (graphs) {
            for (var i = 0; i < graphs.length; ++i) {
                this.wu.fetchGraphXgmml(i, null, function (xgmml) {
                    this.main.loadXGMML(xgmml, true);
                });
            }
        },

        refreshActionState: function (selection) {
            var tab = this.widget.OverviewTabContainer.get("selectedChildWidget");
            this.setDisabled(this.id + "FindPrevious", this.foundIndex <= 0, "iconLeft", "iconLeftDisabled");
            this.setDisabled(this.id + "FindNext", this.foundIndex >= this.found.length - 1, "iconRight", "iconRightDisabled");
            this.setDisabled(this.id + "ActivityMetric", tab.id !== this.id + "ActivitiesTreeMap");
        }
    });
});
