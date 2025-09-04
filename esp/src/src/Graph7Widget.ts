import { declare, lang, aspect, dom, registry } from "src-dojo/index";

import { ScopeGraph, Workunit } from "@hpcc-js/comms";
import { Graph as GraphWidget, Subgraph, Vertex } from "@hpcc-js/graph";
import { hashSum } from "@hpcc-js/util";

// @ts-expect-error
import * as _Widget from "hpcc/_Widget";

// @ts-expect-error
import * as template from "dojo/text!hpcc/templates/Graph7Widget.html";

import nlsHPCC from "./nlsHPCC";
import { WUScopeController } from "./WUScopeController";

type _Widget = {
    id: string;
    widget: any;
    params: { [key: string]: any };
    inherited(args: any);
    setDisabled(id: string, disabled: boolean, icon?: string, disabledIcon?: string);
};

export const Graph7Widget = declare("Graph7Widget", [_Widget], {
    templateString: template,
    i18n: nlsHPCC,

    wuid: "",

    graphStatus: null,

    _graph: undefined as GraphWidget | undefined,
    _gc: undefined as WUScopeController | undefined,

    constructor() {
        this._gc = new WUScopeController();
        this._gc.minClick = (sg: Subgraph) => {
            this.loadGraph(w => {
                this._graph
                    .selection([sg])
                    .centerOnItem(sg)
                    ;
            });
        };
    },

    //  Data ---
    _prevHashSum: undefined as string | undefined,
    _prevScopeGraph: undefined as Promise<ScopeGraph> | undefined,
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
    },
    //  --- ---

    buildRendering: function buildRendering(args) {
        this.inherited(buildRendering, arguments);
    },

    postCreate: function postCreate(args) {
        this.inherited(postCreate, arguments);
        this._initGraphControls();
    },

    startup: function startup(args) {
        this.inherited(startup, arguments);
    },

    resize: function resize(s) {
        this.inherited(resize, arguments);
        this.widget.MainBorderContainer.resize();
    },

    layout: function layout(args) {
        this.inherited(layout, arguments);
    },

    destroy: function destroy(args) {
        this.inherited(destroy, arguments);
    },

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
    },

    _onRefresh() {
        this.refreshData();
    },

    _onGraphRefresh() {
        this._graph.data().subgraphs.forEach((sg: Subgraph) => {
            sg.minState("normal");
        });
        delete this._graph["_prevLayout"];
        this.loadGraph(w => {
            this._graph.zoomToFit();
        });
    },

    _onPartial(args) {
        this._graph.data().subgraphs.forEach((sg: Subgraph) => {
            sg.minState("partial");
        });
        this.loadGraph(w => {
            this._graph.zoomToFit();
        });
    },

    _onMax(args) {
        this._graph.data().subgraphs.forEach((sg: Subgraph) => {
            sg.minState("normal");
        });
        this.loadGraph(w => {
            this._graph.zoomToFit();
        });
    },

    _onZoomToFit(args) {
        this._graph.zoomToFit();
    },

    _onZoomToWidth(args) {
        this._graph.zoomToWidth();
    },

    _onZoomToPlus(args) {
        this._graph.zoomPlus();
    },

    _onZoomToMinus(args) {
        this._graph.zoomMinus();
    },

    isWorkunit() {
        return lang.exists("params.Wuid", this);
    },

    isQuery() {
        return lang.exists("params.QueryId", this);
    },

    init: function init(params) {
        if (this.inherited(init, arguments))
            return;

        this.initGraph();

        this.doInit(params.Wuid);
    },

    clear() {
        this._graph
            .data({ vertices: [], edges: [] })
            .render()
            ;
    },

    doInit(wuid: string) {
        this.wuid = this.params.Wuid = wuid;

        this.refreshData();
    },

    refreshData() {
        if (this.isWorkunit()) {
            return this.loadGraphFromWu(this.wuid, "", true);
        } else if (this.isQuery()) {
        }
        return Promise.resolve();
    },

    loadGraphFromWu(wuid, graphName, refresh: boolean = false) {
        return this.fetchScopeGraph(wuid, graphName, refresh).then(() => {
            this.loadGraph();
        });
    },

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
    },

    loadGraph(callback?) {
        this._graph
            .data(this._gc.graphData(), true)
            .render(callback)
            ;
    }
});
