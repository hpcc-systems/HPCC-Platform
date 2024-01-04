/* eslint-disable @typescript-eslint/no-unsafe-declaration-merging */
import * as arrayUtil from "dojo/_base/array";
import * as declare from "dojo/_base/declare";
import * as Deferred from "dojo/_base/Deferred";
import * as lang from "dojo/_base/lang";
import * as dom from "dojo/dom";
import * as html from "dojo/html";
import * as on from "dojo/on";
import * as topic from "dojo/topic";

import * as CheckedMenuItem from "dijit/CheckedMenuItem";
import * as Menu from "dijit/Menu";
import * as MenuItem from "dijit/MenuItem";
import * as MenuSeparator from "dijit/MenuSeparator";
import * as registry from "dijit/registry";

import * as entities from "dojox/html/entities";

// @ts-ignore
import * as tree from "../dgrid/tree";

// @ts-ignore
import * as _Widget from "hpcc/_Widget";
import * as ESPUtil from "./ESPUtil";
import * as ESPWorkunit from "./ESPWorkunit";
import nlsHPCC from "./nlsHPCC";
import * as Utility from "./Utility";
import * as WsWorkunits from "./WsWorkunits";

// @ts-ignore
import * as template from "dojo/text!hpcc/templates/GraphTreeWidget.html";

import "dijit/Dialog";
import "dijit/form/Button";
import "dijit/form/DropDownButton";
import "dijit/form/NumberSpinner";
import "dijit/form/Select";
import "dijit/form/SimpleTextarea";
import "dijit/form/TextBox";
import "dijit/form/ToggleButton";
import "dijit/layout/BorderContainer";
import "dijit/layout/ContentPane";
import "dijit/layout/StackContainer";
import "dijit/layout/StackController";
import "dijit/layout/TabContainer";
import "dijit/Toolbar";
import "dijit/ToolbarSeparator";
import "hpcc/JSGraphWidget";
import "hpcc/TimingTreeMapWidget";

import { declareDecorator } from "./DeclareDecorator";

type _Widget = any;
export interface GraphTreeWidget extends _Widget { }

@declareDecorator("GraphTreeWidget", _Widget)
export class GraphTreeWidget {
    templateString = template;
    baseClass = "GraphTreeWidget";
    i18n = nlsHPCC;

    graphType = "JSGraphWidget";
    graphName = "";
    wu = null;
    global = null;
    main = null;
    subgraphsGrid = null;
    verticesGrid = null;
    edgesGrid = null;
    xgmmlDialog = null;
    infoDialog = null;
    findText = "";
    found = [];
    foundIndex = 0;

    constructor() {
    }

    buildRendering(args) {
        this.inherited(arguments);
    }

    postCreate(args) {
        this.inherited(arguments);
        this._initGraphControls();
        this._initTimings();
        this._initActivitiesMap();
        this._initDialogs();
        const context = this;
        topic.subscribe(this.id + "OverviewTabContainer-selectChild", function (topic) {
            context.refreshActionState();
        });
    }

    startup(args) {
        this.inherited(arguments);

        this._initTree();
        this._initSubgraphs();
        this._initVertices();
        this._initEdges();

        let splitter = this.widget.BorderContainer.getSplitter("left");
        this.main.watchSplitter(splitter);

        splitter = this.widget.SideBorderContainer.getSplitter("bottom");
        this.main.watchSplitter(splitter);

        this.main.watchSelect(registry.byId(this.id + "AdvancedMenu"));

        this.refreshActionState();
    }

    resize(args) {
        this.inherited(arguments);
        this.widget.BorderContainer.resize();
    }

    layout(args) {
        this.inherited(arguments);
    }

    destroy(args) {
        this.xgmmlDialog.destroyRecursive();
        this.infoDialog.destroyRecursive();
        this.inherited(arguments);
    }

    //  Implementation  ---
    _initGraphControls() {
        const context = this;
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
    }

    _initTimings() {
        const context = this;
        this.widget.TimingsTreeMap.onClick = function (value) {
            context.syncSelectionFrom(context.widget.TimingsTreeMap);
        };
    }

    _initActivitiesMap() {
        const context = this;
        this.widget.ActivitiesTreeMap.onClick = function (value) {
            context.syncSelectionFrom(context.widget.ActivitiesTreeMap);
        };
    }

    _initDialogs() {
        const context = this;

        this.infoDialog = registry.byId(this.id + "InfoDialog");
        on(dom.byId(this.id + "InfoDialogCancel"), "click", function (event) {
            context.infoDialog.hide();
        });

        this.xgmmlDialog = registry.byId(this.id + "XGMMLDialog");
        this.xgmmlTextArea = registry.byId(this.id + "XGMMLTextArea");
        on(dom.byId(this.id + "XGMMLDialogApply"), "click", function (event) {
            context.xgmmlDialog.hide();
            if (context.xgmmlDialog.get("hpccMode") === "XGMML") {
                const xgmml = context.xgmmlTextArea.get("value");
                context.loadGraphFromXGMML(xgmml);
            } else if (context.xgmmlDialog.get("hpccMode") === "DOT") {
                const dot = context.xgmmlTextArea.get("value");
                context.loadGraphFromDOT(dot);
            } else if (context.xgmmlDialog.get("hpccMode") === "DOTATTRS") {
                const dotAttrs = context.xgmmlTextArea.get("value");
                context.global.setDotMetaAttributes(dotAttrs);
                context.main.setDotMetaAttributes(dotAttrs);
                context._onMainSync();
            }
        });
        on(dom.byId(this.id + "XGMMLDialogCancel"), "click", function (event) {
            context.xgmmlDialog.hide();
        });
    }

    _initItemGrid(grid) {
        const context = this;
        grid.on("dgrid-select, dgrid-deselect", function (event) {
            context.syncSelectionFrom(grid);
        });
        grid.on(".dgrid-row:dblclick", function (evt) {
            const item = grid.row(evt).data;
            if (item._globalID) {
                const mainItem = context.main.getItem(item._globalID);
                context.main.centerOnItem(mainItem, true);
            }
        });
    }

    _initTree() {
        this.treeStore = this.global.createTreeStore();
        this.treeGrid = new declare([ESPUtil.Grid(false, true)])({
            treeDepth: this.main.getDepth(),
            store: this.treeStore
        }, this.id + "TreeGrid");
        this._initItemGrid(this.treeGrid);
        this.initContextMenu();
    }

    initContextMenu() {
        const context = this;
        const pMenu = new Menu({
            targetNodeIds: [this.id + "TreeGrid"]
        });
        pMenu.addChild(new MenuItem({
            label: this.i18n.ExpandAll,
            onClick(evt) {
                context.treeGrid.set("treeDepth", 9999);
                context.treeGrid.refresh();
            }
        }));
        pMenu.addChild(new MenuItem({
            label: this.i18n.CollapseAll,
            onClick(evt) {
                context.treeGrid.set("treeDepth", 1);
                context.treeGrid.refresh();
            }
        }));
        pMenu.addChild(new MenuSeparator());
        pMenu.addChild(new CheckedMenuItem({
            label: this.i18n.Activities,
            checked: false,
            onClick(evt) {
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
    }

    _initSubgraphs() {
        this.subgraphsStore = this.global.createStore();
        this.subgraphsGrid = new declare([ESPUtil.Grid(false, true)])({
            store: this.subgraphsStore
        }, this.id + "SubgraphsGrid");

        this._initItemGrid(this.subgraphsGrid);
    }

    _initVertices() {
        this.verticesStore = this.global.createStore();
        this.verticesGrid = new declare([ESPUtil.Grid(false, true)])({
            store: this.verticesStore
        }, this.id + "VerticesGrid");

        this._initItemGrid(this.verticesGrid);
    }

    _initEdges() {
        this.edgesStore = this.global.createStore();
        this.edgesGrid = new declare([ESPUtil.Grid(false, true)])({
            store: this.edgesStore
        }, this.id + "EdgesGrid");

        this._initItemGrid(this.edgesGrid);
    }

    _onRefresh() {
        this.refreshData();
    }

    _onTreeRefresh() {
        this.treeGrid.set("treeDepth", this.main.getDepth());
        this.treeGrid.refresh();
    }

    _onChangeActivityMetric() {
        const metric = this.widget.ActivityMetric.get("value");
        this.widget.ActivitiesTreeMap.setActivityMetric(metric);
    }

    _doFind(prev) {
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
    }

    _onFind(prev) {
        this.findText = "";
        this._doFind(false);
    }

    _onFindNext() {
        this._doFind(false);
    }

    _onFindPrevious() {
        this._doFind(true);
    }

    _onAbout() {
        html.set(dom.byId(this.id + "InfoDialogContent"), "<div style='width: 320px; height: 120px; text-align: center;'><p>" + this.i18n.Version + ":  " + this.main.getVersion() + "</p><p>" + this.main.getResourceLinks() + "</p>");
        this.infoDialog.set("title", this.i18n.AboutHPCCSystemsGraphControl);
        this.infoDialog.show();
    }

    _onGetSVG() {
        html.set(dom.byId(this.id + "InfoDialogContent"), "<textarea rows='25' cols='80'>" + entities.encode(this.main.getSVG()) + "</textarea>");
        this.infoDialog.set("title", this.i18n.SVGSource);
        this.infoDialog.show();
    }

    _onRenderSVG() {
        const context = this;
        this.main.localLayout(function (svg) {
            html.set(dom.byId(context.id + "InfoDialogContent"), "<div style='border: 1px inset grey; width: 640px; height: 480px; overflow : auto; '>" + svg + "</div>");
            context.infoDialog.set("title", this.i18n.RenderedSVG);
            context.infoDialog.show();
        });
    }

    _onGetXGMML() {
        this.xgmmlDialog.set("title", this.i18n.XGMML);
        this.xgmmlDialog.set("hpccMode", "XGMML");
        this.xgmmlTextArea.set("value", this.main.getXGMML());
        this.xgmmlDialog.show();
    }

    _onEditDOT() {
        this.xgmmlDialog.set("title", this.i18n.DOT);
        this.xgmmlDialog.set("hpccMode", "DOT");
        this.xgmmlTextArea.set("value", this.main.getDOT());
        this.xgmmlDialog.show();
    }

    _onGetGraphAttributes() {
        this.xgmmlDialog.set("title", this.i18n.DOTAttributes);
        this.xgmmlDialog.set("hpccMode", "DOTATTRS");
        this.xgmmlTextArea.set("value", this.global.getDotMetaAttributes());
        this.xgmmlDialog.show();
    }

    isWorkunit() {
        return lang.exists("params.Wuid", this);
    }

    isQuery() {
        return lang.exists("params.QueryId", this);
    }

    init(params) {
        if (this.inherited(arguments))
            return;

        if (this.global._plugin) {
            this.doInit(params);
        } else {
            this.global.on("ready", lang.hitch(this, function (evt) {
                this.doInit(params);
            }));
        }
    }

    refresh(params) {
        if (params.SubGraphId) {
            this.syncSelectionFrom([params.SubGraphId]);
        }
    }

    doInit(params) {
        if (this.global.version.major < 5) {
            dom.byId(this.id + "Warning").innerHTML = this.i18n.WarnOldGraphControl + " (" + this.global.version.version + ")";
        }

        if (params.SafeMode && params.SafeMode !== "false") {
            this.main.depth.set("value", 1);
            let dotAttrs = this.global.getDotMetaAttributes();
            dotAttrs = dotAttrs.replace("\n//graph[splines=\"line\"];", "\ngraph[splines=\"line\"];");
            this.global.setDotMetaAttributes(dotAttrs);
        } else {
            let dotAttrs = this.global.getDotMetaAttributes();
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

            let firstLoad = true;
            const context = this;
            this.wu.monitor(function () {
                context.wu.getInfo({
                    onGetApplicationValues(applicationValues) {
                    },
                    onGetGraphs(graphs) {
                        if (firstLoad === true) {
                            firstLoad = false;
                            context.loadGraphFromWu(context.wu, context.graphName, context.subGraphId);
                        } else {
                            context.refreshGraphFromWU(context.wu, context.graphName, context.subGraphId);
                        }
                    },
                    onGetTimers(timers) {
                        context.graphTimers = context.wu.getGraphTimers(context.GraphName);
                    }
                });
            });
        } else if (this.isQuery()) {
            this.targetQuery = params.Target;
            this.queryId = params.QueryId;

            this.loadGraphFromQuery(this.targetQuery, this.queryId, this.graphName);
        }
    }

    refreshData() {
        if (this.isWorkunit()) {
            this.loadGraphFromWu(this.wu, this.graphName, this.subGraphId, true);
        } else if (this.isQuery()) {
            this.loadGraphFromQuery(this.targetQuery, this.queryId, this.graphName);
        }
    }

    loadGraphFromXGMML(xgmml) {
        if (this.global.loadXGMML(xgmml, false, this.graphTimers, true)) {
            this.global.setMessage("...");  //  Just in case it decides to render  ---
            let mainRoot = [0];
            const complexityInfo = this.global.getComplexityInfo();
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
    }

    mergeGraphFromXGMML(xgmml) {
        if (this.global.loadXGMML(xgmml, true, this.graphTimers, true)) {
            this.global.setMessage("...");  //  Just in case it decides to render  ---
            this.refreshMainXGMML();
            this.loadSubgraphs();
            this.loadVertices();
            this.loadEdges();
        }
    }

    loadGraphFromDOT(dot) {
        this.global.loadDOT(dot);
        this.global.setMessage("...");  //  Just in case it decides to render  ---
        this.setMainRootItems([]);
        this.loadSubgraphs();
        this.loadVertices();
        this.loadEdges();
    }

    loadGraphFromWu(wu, graphName, subGraphId, refresh: boolean = false) {
        const deferred = new Deferred();
        this.main.setMessage(this.i18n.FetchingData);
        const context = this;
        wu.fetchGraphXgmmlByName(graphName, subGraphId, function (xgmml, svg) {
            context.main.setMessage("");
            context.loadGraphFromXGMML(xgmml);
            deferred.resolve();
        }, refresh);
        return deferred.promise;
    }

    refreshGraphFromWU(wu, graphName, subGraphId) {
        const context = this;
        wu.fetchGraphXgmmlByName(graphName, subGraphId, function (xgmml) {
            context.mergeGraphFromXGMML(xgmml);
        }, true);
    }

    loadGraphFromQuery(targetQuery, queryId, graphName) {
        this.main.setMessage(this.i18n.FetchingData);
        const context = this;
        WsWorkunits.WUQueryGetGraph({
            request: {
                Target: targetQuery,
                QueryId: queryId,
                GraphName: graphName
            }
        }).then(function (response) {
            context.main.setMessage("");
            if (lang.exists("WUQueryGetGraphResponse.Graphs.ECLGraphEx", response)) {
                if (response.WUQueryGetGraphResponse.Graphs.ECLGraphEx.length > 0) {
                    context.loadGraphFromXGMML(response.WUQueryGetGraphResponse.Graphs.ECLGraphEx[0].Graph);
                }
            }
        });
    }

    refreshGraphFromQuery(targetQuery, queryId, graphName) {
        const context = this;
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
    }

    loadTree() {
        const treeData = this.global.getTreeWithProperties();
        this.treeStore.setTree(treeData);
        const context = this;
        const columns = [
            tree({
                field: "id",
                label: this.i18n.ID, width: 150,
                collapseOnRefresh: true,
                shouldExpand(row, level, previouslyExpanded) {
                    if (previouslyExpanded !== undefined) {
                        return previouslyExpanded;
                    } else if (level < context.treeGrid.get("treeDepth")) {
                        return true;
                    }
                    return false;
                },
                formatter(_id, row) {
                    let img = Utility.getImageURL("file.png");
                    let label = _id + " - ";
                    switch (row._globalType) {
                        case "Graph":
                            img = Utility.getImageURL("server.png");
                            label = context.params.GraphName + " (" + row._children.length + ")";
                            break;
                        case "Cluster":
                            img = Utility.getImageURL("folder.png");
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
    }

    loadSubgraphs() {
        const subgraphs = this.global.getSubgraphsWithProperties();
        this.subgraphsStore.setData(subgraphs);
        const columns = [
            {
                label: this.i18n.ID, field: "id", width: 54,
                formatter(_id, row) {
                    const img = Utility.getImageURL("folder.png");
                    return "<img src='" + img + "'/>&nbsp;" + _id;
                }
            }
        ];
        this.subgraphsStore.appendColumns(columns, [this.i18n.TimeSeconds, "DescendantCount", "SubgraphCount", "ActivityCount"], ["ChildCount", "Depth"]);
        this.subgraphsGrid.set("columns", columns);
        this.subgraphsGrid.refresh();
    }

    loadVertices() {
        const vertices = this.global.getVerticesWithProperties();
        this.verticesStore.setData(vertices);
        const columns = [
            {
                label: this.i18n.ID, field: "id", width: 54,
                formatter(_id, row) {
                    const img = Utility.getImageURL("file.png");
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
    }

    loadEdges() {
        const edges = this.global.getEdgesWithProperties();
        this.edgesStore.setData(edges);
        const columns = [
            { label: this.i18n.ID, field: "id", width: 50 }
        ];
        this.edgesStore.appendColumns(columns, ["label", "count"], ["source", "target"]);
        this.edgesGrid.set("columns", columns);
        this.edgesGrid.refresh();
    }

    inSyncSelectionFrom = false;
    syncSelectionFrom(sourceControl) {
        if (!this.inSyncSelectionFrom) {
            this._syncSelectionFrom(sourceControl);
        }
    }

    // _syncSelectionFrom: Utility.debounce(function (sourceControlOrGlobalIDs) {
    _syncSelectionFrom(sourceControlOrGlobalIDs) {
        this.inSyncSelectionFrom = true;
        const sourceControl = sourceControlOrGlobalIDs instanceof Array ? null : sourceControlOrGlobalIDs;
        let selectedGlobalIDs = sourceControlOrGlobalIDs instanceof Array ? sourceControlOrGlobalIDs : [];
        if (sourceControl) {
            //  Get Selected Items  ---
            if (sourceControl === this.widget.TimingsTreeMap) {
                const items = sourceControl.getSelected();
                for (let i = 0; i < items.length; ++i) {
                    if (items[i].SubGraphId) {
                        selectedGlobalIDs.push(items[i].SubGraphId);
                    }
                }
            } else if (sourceControl === this.widget.ActivitiesTreeMap) {
                const items = sourceControl.getSelected();
                for (let i = 0; i < items.length; ++i) {
                    if (items[i].ActivityID) {
                        selectedGlobalIDs.push(items[i].ActivityID);
                    }
                }
            } else if (sourceControl === this.verticesGrid || sourceControl === this.edgesGrid || sourceControl === this.subgraphsGrid || sourceControl === this.treeGrid) {
                const items = sourceControl.getSelected();
                for (let i = 0; i < items.length; ++i) {
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

        const propertiesDom = dom.byId(this.id + "Properties");
        propertiesDom.innerHTML = "";
        for (let i = 0; i < selectedGlobalIDs.length; ++i) {
            this.global.displayProperties(this.wu, selectedGlobalIDs[i], propertiesDom);
        }
        const context = this;
        if (selectedGlobalIDs.length) {
            const edges = arrayUtil.filter(selectedGlobalIDs, function (id) {
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
        // }, 500, false)
    }

    resetPage() {
        this.main.clear();
    }

    setMainRootItems(globalIDs) {
        const graphView = this.global.getGraphView(globalIDs, this.main.getDepth(), this.main.distance.get("value"), this.main.option("subgraph"), this.main.option("vhidespills"));
        return graphView.navigateTo(this.main);
    }

    refreshMainXGMML() {
        const graphView = this.main.getCurrentGraphView();
        graphView.refreshXGMML(this.main);
    }

    displayGraphs(graphs) {
        for (let i = 0; i < graphs.length; ++i) {
            this.wu.fetchGraphXgmml(i, null, function (xgmml) {
                this.main.loadXGMML(xgmml, true);
            });
        }
    }

    refreshActionState() {
        const tab = this.widget.OverviewTabContainer.get("selectedChildWidget");
        this.setDisabled(this.id + "FindPrevious", this.foundIndex <= 0, "iconLeft", "iconLeftDisabled");
        this.setDisabled(this.id + "FindNext", this.foundIndex >= this.found.length - 1, "iconRight", "iconRightDisabled");
        this.setDisabled(this.id + "ActivityMetric", tab.id !== this.id + "ActivitiesTreeMap");
    }
}
