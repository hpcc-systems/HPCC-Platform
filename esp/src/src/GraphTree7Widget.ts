/* eslint-disable @typescript-eslint/no-unsafe-declaration-merging */
import * as arrayUtil from "dojo/_base/array";
import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";
import * as aspect from "dojo/aspect";
import * as dom from "dojo/dom";
import * as has from "dojo/has";
import * as topic from "dojo/topic";

import * as registry from "dijit/registry";

import { ECLGraph, ScopeGraph, Workunit } from "@hpcc-js/comms";
import { Graph as GraphWidget, Graph2 as Graph2Widget, Subgraph, Vertex } from "@hpcc-js/graph";
import { hashSum } from "@hpcc-js/util";

// @ts-ignore
import * as _Widget from "hpcc/_Widget";
import { declareDecorator } from "./DeclareDecorator";
import { Grid, maximizeWidget } from "./ESPUtil";
import { GraphStore, GraphTreeStore } from "./GraphStore";
import nlsHPCC from "./nlsHPCC";
import { debounce, getImageURL, Persist } from "./Utility";
import * as WsWorkunits from "./WsWorkunits";
import { WUGraphLegend } from "./WUGraphLegend";
import { WUScopeController, WUScopeController8 } from "./WUScopeController";

// @ts-ignore
import * as template from "dojo/text!hpcc/templates/GraphTree7Widget.html";

import "dijit/Fieldset";
import "dijit/form/Button";
import "dijit/form/CheckBox";
import "dijit/form/DropDownButton";
import "dijit/form/Form";
import "dijit/form/TextBox";
import "dijit/form/ToggleButton";
import "dijit/layout/BorderContainer";
import "dijit/layout/ContentPane";
import "dijit/layout/StackContainer";
import "dijit/layout/StackController";
import "dijit/layout/TabContainer";
import "dijit/Toolbar";
import "dijit/ToolbarSeparator";
import "dijit/TooltipDialog";
import "hpcc/TableContainer";

declare const dojoConfig;

class DataGraph {
    private _controller: WUScopeController = new WUScopeController();
    private _widget: GraphWidget = new GraphWidget();

    constructor() {
    }

    widget(): GraphWidget {
        return this._widget;
    }

    controller(): WUScopeController {
        return this._controller;
    }
}

class DataGraph2 {
    private _controller: WUScopeController8 = new WUScopeController8();
    private _widget: Graph2Widget = new Graph2Widget()
        .wasmFolder(dojoConfig.urlInfo.fullPath + "/dist")
        ;

    constructor() {
    }

    widget(): Graph2Widget {
        return this._widget;
    }

    controller(): WUScopeController8 {
        return this._controller;
    }
}

type _Widget = {
    id: string;
    widget: any;
    params: { [key: string]: any };
    inherited(args: any);
    setDisabled(id: string, disabled: boolean, icon?: string, disabledIcon?: string);
};

export interface GraphTree7Widget extends _Widget {
}

@declareDecorator("GraphTree7Widget", _Widget)
export class GraphTree7Widget {
    templateString = template;
    static baseClass = "GraphTree7Widget";
    i18n = nlsHPCC;

    GraphName: any;

    subGraphId: string;
    activityId: string;
    edgeId: string;
    graphTimers: any;
    targetQuery: any;
    queryId: any;

    _hostPage;
    wuid = "";
    graphName = "";
    optionsDropDown = null;
    optionsForm = null;
    OptionsFormLegacyGraph = null;
    optionsFormLegacyLayout = null;

    _optionsDefault = null;
    subgraphsGrid = null;
    verticesGrid = null;
    edgesGrid = null;
    graphStatus = null;

    findText = "";
    found = [];
    foundIndex = 0;

    _graph: DataGraph | DataGraph2;
    protected _legend: WUGraphLegend;
    protected treeStore = new GraphTreeStore();
    protected subgraphsStore = new GraphStore("Id");
    protected verticesStore = new GraphStore("Id");
    protected edgesStore = new GraphStore("Id");
    private persist = new Persist("GraphTree7Widget");

    private isIE = has("ie") || has("trident");

    constructor() {
    }

    //  Options ---

    optionsFormValues() {
        const retVal = this.optionsForm.getValues();
        if (this.isIE) {
            retVal.LegacyGraph = ["on"];
            retVal.LegacyLayout = ["on"];
        }
        if (retVal.LegacyGraph?.length) {
            retVal.LegacyLayout = ["on"];
        }
        return retVal;
    }

    _onOptionsApply() {
        const optionsValues = this.optionsFormValues();
        this.persist.setObj("options", optionsValues);
        this.optionsDropDown.closeDropDown();
        this.initGraph();
        this.refreshData();
        this.refreshActionState();
    }

    _onOptionsReset() {
        this.optionsForm.setValues(this._optionsDefault);
        this.initGraph();
        this.refreshData();
        this.refreshActionState();
    }

    //  Data ---
    private _prevHashSum;
    private _prevScopeGraph: Promise<ECLGraph[]>;
    fetchScopeGraph(wuid: string, graphID: string, subgraphID: string = "", refresh: boolean = false): Promise<ScopeGraph> {
        const hash = hashSum({
            wuid,
            graphID,
            subgraphID
        });
        if (!this._prevScopeGraph || refresh || this._prevHashSum !== hash) {
            this._prevHashSum = hash;
            this.graphStatus.innerText = this.i18n.FetchingData;
            this._graph.controller().clear();
            const wu = Workunit.attach({ baseUrl: "" }, wuid);
            this._prevScopeGraph = wu.fetchGraphs();
        }
        return this._prevScopeGraph.then(graphs => {
            for (const graph of graphs) {
                if (graph.Name === graphID) {
                    return graph.fetchScopeGraph(subgraphID).then(scopedGraph => {
                        this.graphStatus.innerText = this.i18n.Loading;
                        return new Promise<ScopeGraph>((resolve, reject) => {
                            this.graphStatus.innerText = "";
                            setTimeout(() => {
                                this._graph.controller().set(scopedGraph);
                                this._legend.data(this._graph.controller().calcLegend());
                                resolve(scopedGraph);
                            }, 0);
                        });
                    });
                }
            }
        });
    }
    //  --- ---

    buildRendering(args) {
        this.inherited(arguments);
    }

    postCreate(args) {
        this.inherited(arguments);
        this._initGraphControls();
        const context = this;
        topic.subscribe(this.id + "OverviewTabContainer-selectChild", function (topic) {
            context.refreshActionState();
        });
        this.optionsDropDown = registry.byId(this.id + "OptionsDropDown");
        this.optionsForm = registry.byId(this.id + "OptionsForm");
        this.OptionsFormLegacyGraph = registry.byId(this.id + "OptionsFormLegacyGraph");
        this.OptionsFormLegacyGraph.on("click", () => this.refreshActionState());
        this.optionsFormLegacyLayout = registry.byId(this.id + "OptionsFormLegacyLayout");
        this._optionsDefault = this.optionsFormValues();
        const options = this.persist.getObj("options", this._optionsDefault);
        this.optionsForm.setValues(options);
    }

    startup(args) {
        this.inherited(arguments);

        this.refreshActionState();
    }

    resize(s) {
        this.inherited(arguments);
        this.widget.BorderContainer.resize();
    }

    layout(args) {
        this.inherited(arguments);
    }

    destroy(args) {
        this.inherited(arguments);
    }

    //  Implementation  ---
    _initGraphControls() {
        aspect.after(registry.byId(this.id + "MainBorderContainer"), "resize", () => {
            if (this._graph) {
                this._graph.widget()
                    .resize()
                    .render()
                    ;
            }
        });
    }

    _initItemGrid(grid) {
        const context = this;
        grid.on("dgrid-select, dgrid-deselect", function (event) {
            context.syncSelectionFrom(grid);
        });
        grid.on(".dgrid-row:dblclick", function (evt) {
            const item = grid.row(evt).data;
            context.centerOn(item.Id);
        });
    }

    _onRefresh() {
        this.refreshData(true);
    }

    _onGraphRefresh() {
        if (this._graph instanceof DataGraph) {
            this._graph.widget().data().subgraphs.forEach((sg: Subgraph) => {
                sg.minState("normal");
            });
            delete this._graph.widget()["_prevLayout"];
        } else {
            this._graph.widget().resetLayout();
        }
        this.loadGraph(w => {
            this._graph.widget().zoomToFit();
        });
    }

    _onPartial(args) {
        if (this._graph instanceof DataGraph) {
            this._graph.widget().data().subgraphs.forEach((sg: Subgraph) => {
                sg.minState("partial");
            });
            this.loadGraph(w => {
                this._graph.widget().zoomToFit();
            });
        }
    }

    _onMax(args) {
        if (this._graph instanceof DataGraph) {
            this._graph.widget().data().subgraphs.forEach((sg: Subgraph) => {
                sg.minState("normal");
            });
            this.loadGraph(w => {
                this._graph.widget().zoomToFit();
            });
        }
    }

    _onZoomToFit(args) {
        this._graph.widget().zoomToFit();
    }

    _onZoomToWidth(args) {
        this._graph.widget().zoomToWidth();
    }

    _onZoomToPlus(args) {
        this._graph.widget().zoomPlus();
    }

    _onZoomToMinus(args) {
        this._graph.widget().zoomMinus();
    }

    _doFind(prev) {
        if (this.findText !== this.widget.FindField.value) {
            this.findText = this.widget.FindField.value;
            this.found = this._graph.controller().find(this.findText);
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
            if (this._graph instanceof DataGraph) {
                const item = this._graph.controller().item(this.found[this.foundIndex]);
                this._graph.widget().centerOnItem(item);
            } else {
                const item = this._graph.controller().item(this.found[this.foundIndex]);
                this._graph.widget().centerOnItem("" + item.id);
            }
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

    _prevMaxGraph;
    _onMaximizeGraph(max) {
        this._prevMaxGraph = maximizeWidget(registry.byId(this.id + "MainBorderContainer"), max, this._prevMaxGraph);
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

        this.initWidgets();
        this.initSubgraphs();
        this.initVertices();
        this.initEdges();

        this.doInit(params);

        this.refreshActionState();
    }

    refresh(params) {
        if (params.SubGraphId) {
            this.syncSelectionFrom(this);
        }
    }

    doInit(params) {
        this.wuid = params.Wuid;
        this.graphName = params.GraphName;
        this.subGraphId = params.SubGraphId;
        this.activityId = params.ActivityId;
        this.edgeId = params.EdgeId;
        this.targetQuery = params.Target;
        this.queryId = params.QueryId;

        this.refreshData().then(() => {
            this.syncSelectionFrom(this);
            if (this.edgeId) {
                this.centerOn(this.edgeId);
            } else if (this.activityId) {
                this.centerOn(this.activityId);
            } else if (this.subGraphId) {
                this.centerOn(this.subGraphId);
            }
        });
    }

    refreshData(refresh: boolean = false) {
        if (this.isWorkunit()) {
            return this.loadGraphFromWu(this.wuid, this.graphName, this.subGraphId, refresh);
        } else if (this.isQuery()) {
        }
        return Promise.resolve();
    }

    loadGraphFromWu(wuid, graphName, subGraphId, refresh: boolean = false) {
        this.initGraphController();
        return this.fetchScopeGraph(wuid, graphName, subGraphId, refresh).then(() => {
            this.loadGraph();
            this.loadSubgraphs();
            this.loadVertices();
            this.loadEdges();
        });
    }

    initGraph() {
        const options = this.optionsFormValues();
        const prevGraph = this._graph;
        if (options.LegacyGraph?.length) {
            if (!(prevGraph instanceof DataGraph)) {
                this._graph = new DataGraph();
                this._graph.controller().minClick = (sg: Subgraph) => {
                    this.loadGraph(w => {
                        (this._graph as DataGraph).widget()
                            .selection([sg])
                            .centerOnItem(sg)
                            ;
                        this.syncSelectionFrom(this._graph);
                    });
                };
            }
        } else {
            if (!(prevGraph instanceof DataGraph2)) {
                this._graph = new DataGraph2();
            }
        }
        if (this._graph !== prevGraph) {
            if (prevGraph) {
                prevGraph.widget().target(null);
            }
            if (this._graph instanceof DataGraph) {
                this._graph.widget()
                    .target(this.id + "MainGraphWidget")
                    .layout("Hierarchy")
                    .applyScaleOnLayout(true)
                    .showToolbar(false)
                    .allowDragging(false)
                    .on("vertex_click", sel => {
                        this.syncSelectionFrom(this._graph);
                    })
                    .on("edge_click", sel => {
                        this.syncSelectionFrom(this._graph);
                    })
                    .on("progress", what => {
                        switch (what) {
                            case "start":
                            case "layout-start":
                            case "layout-tick":
                                this.graphStatus.innerText = this.i18n.PerformingLayout;
                                break;
                            case "layout-end":
                            case "end":
                            default:
                                this.graphStatus.innerText = "";
                                break;
                        }
                    })
                    ;
            } else {
                this._graph.widget()
                    .target(this.id + "MainGraphWidget")
                    .layout("DOT")
                    .applyScaleOnLayout(true)
                    .showToolbar(false)
                    .allowDragging(false)
                    .minScale(0)
                    .maxScale(999999999)
                    .on("subgraph_click", sel => {
                        this.syncSelectionFrom(this._graph);
                    })
                    .on("vertex_click", sel => {
                        this.syncSelectionFrom(this._graph);
                    })
                    .on("edge_click", sel => {
                        this.syncSelectionFrom(this._graph);
                    })
                    .on("progress", what => {
                        switch (what) {
                            case "start":
                            case "stop":
                                break;
                            case "layout-start":
                            case "layout-tick":
                                // render(Busy, { open: true }, this.id + "MainGraphWidgetStatus");
                                this.graphStatus.innerText = this.i18n.PerformingLayout;
                                break;
                            case "layout-stop":
                                this.graphStatus.innerText = "";
                                // render(Busy, { open: false }, this.id + "MainGraphWidgetStatus");
                                break;
                        }
                    })
                    ;
            }
        }
        if (this._graph instanceof DataGraph2) {
            this._graph.widget().layout(options.LegacyLayout?.length ? "Hierarchy" : "DOT");
        }
    }

    initWidgets() {
        this.graphStatus = dom.byId(this.id + "GraphStatus");
        this.initGraph();

        if (this._graph instanceof DataGraph) {
            this._graph.widget().tooltipHTML((v: Vertex) => {
                return (this._graph.controller() as WUScopeController).calcGraphTooltip2(v);
            });
        }

        this._legend = new WUGraphLegend(this as any)
            .target(this.id + "LegendGrid")
            .on("click", kind => {
                this.loadGraph();
            })
            .on("mouseover", kind => {
                const verticesMap: { [id: string]: boolean } = {};

                if (this._graph instanceof DataGraph) {
                    for (const vertex of this._graph.controller().vertices(kind)) {
                        verticesMap[vertex.id()] = true;
                    }
                } else {
                    for (const vertex of this._graph.controller().vertices(kind)) {
                        verticesMap[vertex.id] = true;
                    }
                }
                this._graph.widget().highlightVerticies(verticesMap);
            })
            .on("mouseout", kind => {
                this._graph.widget().highlightVerticies();
            })
            ;
    }

    initGraphController() {
        const options = this.optionsFormValues();
        if (this._graph instanceof DataGraph) {
            this._graph.controller()
                .showSubgraphs(options.subgraph?.length)
                .showIcon(options.vicon?.length)
                .vertexLabelTpl(options.vlabel)
                .edgeLabelTpl(options.elabel)
                .disabled(this._legend.disabled())
                ;
        } else {
            this._graph.controller()
                .showSubgraphs(options.subgraph?.length)
                .showIcon(options.vicon?.length)
                .vertexLabelTpl(options.vlabel)
                .edgeLabelTpl(options.elabel)
                .disabled(this._legend.disabled())
                ;
        }
    }

    loadGraph(callback?) {
        this.initGraphController();
        if (this._graph instanceof DataGraph) {
            this._graph.widget()
                .data(this._graph.controller().graphData(), true)
                .render(callback)
                ;
        } else {
            this._graph.widget()
                .data(this._graph.controller().graphData())
                .render(callback)
                ;
        }
        this._legend
            .render()
            ;
    }

    formatColumns(columns) {
        columns.forEach((column: any) => {
            if (column.formatter === undefined) {
                column.formatter = function (cell, row) {
                    const retVal = (row.__formatted && row.__formatted[`${column.field}`]) ? row.__formatted[`${column.field}`] : cell;
                    return retVal !== undefined ? retVal : "";
                };
            }
        });
    }

    initSubgraphs() {
        this.subgraphsGrid = new declare([Grid(true, true)])({
            store: this.subgraphsStore
        }, this.id + "SubgraphsGrid");
        const context = this;
        this.subgraphsGrid.on(".dgrid-row-url:click", function (evt) {
            const row = context.subgraphsGrid.row(evt).data;
            context._hostPage.openGraph(context.graphName, row.Id);
        });

        this._initItemGrid(this.subgraphsGrid);
    }

    loadSubgraphs() {
        const subgraphs = this._graph.controller().subgraphStoreData();
        this.subgraphsStore.setData(subgraphs);
        const context = this;
        const img = getImageURL("folder.png");
        const columns = [
            {
                label: this.i18n.ID, field: "Id", width: 54,
                formatter(_id, row) {
                    return "<img src='" + img + "'/>&nbsp;" + (context._hostPage ? "<a href='#" + _id + "' class='dgrid-row-url'>" + _id + "</a>" : _id);
                }
            }
        ];
        this.subgraphsStore.appendColumns(columns, [this.i18n.TimeSeconds, "DescendantCount", "SubgraphCount", "ActivityCount"], ["ChildCount", "Depth"]);
        this.formatColumns(columns);
        this.subgraphsGrid.set("columns", columns);
        this.subgraphsGrid.refresh();
    }

    initVertices() {
        this.verticesGrid = new declare([Grid(true, true)])({
            store: this.verticesStore
        }, this.id + "VerticesGrid");

        this._initItemGrid(this.verticesGrid);
    }

    loadVertices() {
        const vertices = this._graph.controller().activityStoreData();
        this.verticesStore.setData(vertices);
        const columns = [
            {
                label: this.i18n.ID, field: "Id", width: 54,
                formatter(_id, row) {
                    const img = getImageURL("file.png");
                    return "<img src='" + img + "'/>&nbsp;" + _id;
                }
            },
            { label: this.i18n.Label, field: "Label", width: 150 }
        ];
        this.verticesStore.appendColumns(columns, [], ["Kind", "EclNameList", "EclText", "DefinitionList"]);
        this.formatColumns(columns);
        this.verticesGrid.set("columns", columns);
        this.verticesGrid.refresh();
    }

    initEdges() {
        this.edgesGrid = new declare([Grid(true, true)])({
            store: this.edgesStore
        }, this.id + "EdgesGrid");

        this._initItemGrid(this.edgesGrid);
    }

    loadEdges() {
        const edges = this._graph.controller().edgeStoreData();
        this.edgesStore.setData(edges);
        const columns = [
            { label: this.i18n.ID, field: "Id", width: 50 }
        ];
        this.edgesStore.appendColumns(columns, ["Label", "NumRowsProcessed"], ["IdSource", "IdTarget", "SourceIndex", "TargetIndex"]);
        this.formatColumns(columns);
        this.edgesGrid.set("columns", columns);
        this.edgesGrid.refresh();
    }

    centerOn(itemID?: string) {
        if (itemID) {
            if (this._graph instanceof DataGraph) {
                let refresh = false;
                let scopeItem = this._graph.controller().scopeItem(itemID);
                while (scopeItem) {
                    const w = this._graph.controller().item(scopeItem._.Id);
                    if (w && w instanceof Subgraph && w.minState() !== "normal") {
                        w.minState("normal");
                        refresh = true;
                    }
                    scopeItem = scopeItem.parent;
                }
                const w = this._graph.controller().item(itemID);
                if (w) {
                    if (refresh) {
                        this._graph.widget()
                            .data(this._graph.controller().graphData(), true)   //  Force re-render
                            .render(() => {
                                setTimeout(() => {
                                    if (this._graph instanceof DataGraph) {
                                        this._graph.widget().centerOnItem(w);
                                    }
                                }, 1000);
                            });
                    } else {
                        this._graph.widget().centerOnItem(w);
                    }
                }
            } else {
                this._graph.widget().centerOnItem(itemID);
            }
        }
    }

    inSyncSelectionFrom = false;
    syncSelectionFrom(sourceControl) {
        if (!this.inSyncSelectionFrom) {
            this._syncSelectionFrom(sourceControl, this._graph);
        }
    }

    _syncSelectionFrom(sourceControl, graphRef) {
    }

    resetPage() {
    }

    setMainRootItems(globalIDs) {
    }

    refreshMainXGMML() {
    }

    displayGraphs(graphs) {
    }

    refreshActionState() {
        const options = this.optionsFormValues();
        const tab = this.widget.OverviewTabContainer.get("selectedChildWidget");
        this.setDisabled(this.id + "FindPrevious", this.foundIndex <= 0, "iconLeft", "iconLeftDisabled");
        this.setDisabled(this.id + "FindNext", this.foundIndex >= this.found.length - 1, "iconRight", "iconRightDisabled");
        this.setDisabled(this.id + "ActivityMetric", tab && tab.id !== this.id + "ActivitiesTreeMap");
        this.setDisabled(this.id + "Partial", this._graph instanceof DataGraph2, "fa fa-window-restore", "disabled fa fa-window-restore");
        this.setDisabled(this.id + "Max", this._graph instanceof DataGraph2, "fa fa-window-maximize", "disabled fa fa-window-maximize");
        this.setDisabled(this.id + "OptionsFormLegacyGraph", this.isIE);
        this.setDisabled(this.id + "OptionsFormLegacyLayout", this.isIE || options.LegacyGraph?.length);
    }
}

GraphTree7Widget.prototype._syncSelectionFrom = debounce(function (this: GraphTree7Widget, sourceControlOrGlobalIDs) {
    this.inSyncSelectionFrom = true;
    const sourceControl = sourceControlOrGlobalIDs instanceof Array ? null : sourceControlOrGlobalIDs;
    let selectedGlobalIDs = sourceControlOrGlobalIDs instanceof Array ? sourceControlOrGlobalIDs : [];
    if (sourceControl) {
        //  Get Selected Items  ---
        if (sourceControl === this) {
            if (this.edgeId) {
                selectedGlobalIDs = [this.edgeId];
            } else if (this.activityId) {
                selectedGlobalIDs = [this.activityId];
            } else if (this.subGraphId) {
                selectedGlobalIDs = [this.subGraphId];
            }
        } else if (sourceControl === this._graph) {
            if (this._graph instanceof DataGraph) {
                selectedGlobalIDs = this._graph.widget().selection()
                    .map(w => (this._graph.controller() as WUScopeController).rItem(w as any))
                    .filter(item => !!item)
                    .map(item => item._.Id);
            } else {
                selectedGlobalIDs = this._graph.widget().selection()
                    .map(w => (this._graph.controller() as WUScopeController8).rItem("" + w.id))
                    .filter(item => !!item)
                    .map(item => item._.Id)
                    ;
            }
        } else if (sourceControl === this.verticesGrid || sourceControl === this.edgesGrid || sourceControl === this.subgraphsGrid) {
            const items = sourceControl.getSelected();
            for (let i = 0; i < items.length; ++i) {
                if (lang.exists("Id", items[i])) {
                    selectedGlobalIDs.push(items[i].Id);
                }
            }
        } else if (sourceControl === this.found) {
            selectedGlobalIDs = this.found;
        } else {
            selectedGlobalIDs = sourceControl.getSelectionAsGlobalID();
        }
    }

    //  Set Selected Items  ---
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
    if (sourceControl !== this._graph) {
        if (this._graph instanceof DataGraph) {
            const items = this._graph.controller().items(selectedGlobalIDs);
            this._graph.widget().selection(items);
        } else {
            const items = this._graph.controller().items(selectedGlobalIDs);
            this._graph.widget().selection(items);
        }
    }

    const propertiesDom = dom.byId(this.id + "Properties");
    propertiesDom.innerHTML = "";
    let html = "";
    for (const id of selectedGlobalIDs) {
        html += this._graph.controller().calcGraphTooltip(id, this.findText);
    }
    propertiesDom.innerHTML = html;
    const context = this;
    if (selectedGlobalIDs.length) {
        const edges = arrayUtil.filter(selectedGlobalIDs, function (id) {
            return id && id.indexOf && id.indexOf("_") >= 0;
        });
        if (edges.length === 1) {
            WsWorkunits.WUCDebug(context.params.Wuid, "<debug:print edgeId='" + edges[0] + "'/>").then(function (response) {
                if (lang.exists("WUDebugResponse.Result", response)) {
                    // context.global.displayTrace(response.WUDebugResponse.Result, propertiesDom);
                }
            });
        }
    }
    this.inSyncSelectionFrom = false;
}, 500, false);
