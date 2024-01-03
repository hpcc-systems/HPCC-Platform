/* eslint-disable @typescript-eslint/no-unsafe-declaration-merging */
import * as lang from "dojo/_base/lang";
import * as aspect from "dojo/aspect";
import * as dom from "dojo/dom";

import * as registry from "dijit/registry";

import { ScopeGraph, Workunit } from "@hpcc-js/comms";
import { Graph as GraphWidget, Subgraph, Vertex } from "@hpcc-js/graph";
import { hashSum } from "@hpcc-js/util";

// @ts-ignore
import * as _Widget from "hpcc/_Widget";

// @ts-ignore
import * as template from "dojo/text!hpcc/templates/Graph7Widget.html";

import "dijit/form/Button";
import "dijit/layout/BorderContainer";
import "dijit/layout/ContentPane";
import "dijit/Toolbar";
import "dijit/ToolbarSeparator";

import { declareDecorator } from "./DeclareDecorator";
import nlsHPCC from "./nlsHPCC";
import { WUScopeController } from "./WUScopeController";

type _Widget = {
    id: string;
    widget: any;
    params: { [key: string]: any };
    inherited(args: any);
    setDisabled(id: string, disabled: boolean, icon?: string, disabledIcon?: string);
};

export interface Graph7Widget extends _Widget {
}

@declareDecorator("Graph7Widget", _Widget)
export class Graph7Widget {
    templateString = template;
    static baseClass = "Graph7Widget";
    i18n = nlsHPCC;

    wuid = "";

    graphStatus = null;

    _graph: GraphWidget;
    _gc = new WUScopeController();

    constructor() {
        this._gc.minClick = (sg: Subgraph) => {
            this.loadGraph(w => {
                this._graph
                    .selection([sg])
                    .centerOnItem(sg)
                    ;
            });
        };
    }

    //  Data ---
    private _prevHashSum;
    private _prevScopeGraph: Promise<ScopeGraph>;
    fetchScopeGraph(wuid: string, graphID: string, refresh: boolean = false): Promise<ScopeGraph> {
        this.graphStatus.innerText = this.i18n.FetchingData;
        const hash = hashSum({
            wuid,
            graphID
        });
        if (!this._prevScopeGraph || refresh || this._prevHashSum !== hash) {
            this._prevHashSum = hash;
            this._gc.clear();
            const wu = Workunit.attach({ baseUrl: "" }, wuid);
            this._prevScopeGraph = wu.fetchScopeGraphs(graphID ? [graphID] : []).then(scopedGraph => {
                this.graphStatus.innerText = this.i18n.Loading;
                return new Promise<ScopeGraph>((resolve, reject) => {
                    setTimeout(() => {
                        this._gc.set(scopedGraph);
                        resolve(scopedGraph);
                    }, 0);
                });
            });
        }
        return this._prevScopeGraph;
    }
    //  --- ---

    buildRendering(args) {
        this.inherited(arguments);
    }

    postCreate(args) {
        this.inherited(arguments);
        this._initGraphControls();
    }

    startup(args) {
        this.inherited(arguments);
    }

    resize(s) {
        this.inherited(arguments);
        this.widget.MainBorderContainer.resize();
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
                this._graph
                    .resize()
                    .render()
                    ;
            }
        });
    }

    _onRefresh() {
        this.refreshData();
    }

    _onGraphRefresh() {
        this._graph.data().subgraphs.forEach((sg: Subgraph) => {
            sg.minState("normal");
        });
        delete this._graph["_prevLayout"];
        this.loadGraph(w => {
            this._graph.zoomToFit();
        });
    }

    _onPartial(args) {
        this._graph.data().subgraphs.forEach((sg: Subgraph) => {
            sg.minState("partial");
        });
        this.loadGraph(w => {
            this._graph.zoomToFit();
        });
    }

    _onMax(args) {
        this._graph.data().subgraphs.forEach((sg: Subgraph) => {
            sg.minState("normal");
        });
        this.loadGraph(w => {
            this._graph.zoomToFit();
        });
    }

    _onZoomToFit(args) {
        this._graph.zoomToFit();
    }

    _onZoomToWidth(args) {
        this._graph.zoomToWidth();
    }

    _onZoomToPlus(args) {
        this._graph.zoomPlus();
    }

    _onZoomToMinus(args) {
        this._graph.zoomMinus();
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

        this.initGraph();

        this.doInit(params.Wuid);
    }

    clear() {
        this._graph
            .data({ vertices: [], edges: [] })
            .render()
            ;
    }

    doInit(wuid: string) {
        this.wuid = this.params.Wuid = wuid;

        this.refreshData();
    }

    refreshData() {
        if (this.isWorkunit()) {
            return this.loadGraphFromWu(this.wuid, "", true);
        } else if (this.isQuery()) {
        }
        return Promise.resolve();
    }

    loadGraphFromWu(wuid, graphName, refresh: boolean = false) {
        return this.fetchScopeGraph(wuid, graphName, refresh).then(() => {
            this.loadGraph();
        });
    }

    initGraph() {
        this.graphStatus = dom.byId(this.id + "GraphStatus");
        this._graph = new GraphWidget()
            .target(this.id + "MainGraphWidget")
            .layout("Hierarchy")
            .applyScaleOnLayout(true)
            .showToolbar(false)
            .allowDragging(false)
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
            });

        this._graph.tooltipHTML((v: Vertex) => {
            return this._gc.calcGraphTooltip2(v);
        });
    }

    loadGraph(callback?) {
        this._graph
            .data(this._gc.graphData(), true)
            .render(callback)
            ;
    }
}
