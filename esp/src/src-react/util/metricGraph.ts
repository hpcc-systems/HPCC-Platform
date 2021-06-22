import { Button, d3Event, select as d3Select, Spacer, SVGZoomWidget } from "@hpcc-js/common";
import { graphviz } from "@hpcc-js/graph";
import { Graph2 } from "@hpcc-js/util";
import { decodeHTML } from "src/Utility";
import { MetricsOptions } from "../hooks/metrics";

declare const dojoConfig;

const KindShape = {
    2: "cylinder",          //  Disk Write
    3: "tripleoctagon",     //  Local Sort
    5: "invtrapezium",      //  Filter
    6: "diamond",           //  Split
    7: "trapezium",         //  Project
    16: "cylinder",         //  Output
    17: "invtrapezium",     //  Funnel
    19: "doubleoctagon",    //  Skew Distribute
    22: "cylinder",         //  Store Internal
    28: "diamond",          //  If
    71: "cylinder",         //  Disk Read
    73: "cylinder",         //  Disk Aggregate Spill
    74: "cylinder",         //  Disk Exists
    94: "cylinder",         //  Local Result
    125: "circle",          //  Count
    133: "cylinder",        //  Inline Dataset
    146: "doubleoctagon",   //  Distribute Merge
    148: "cylinder",        //  Inline Dataset
    155: "invhouse",        //  Join
    161: "invhouse",        //  Smart Join
    185: "invhouse",        //  Smart Denormalize Group
    195: "cylinder",        //  Spill Read
    196: "cylinder",        //  Spill Write
};

function shape(kind: string) {
    return KindShape[kind] || "rectangle";
}

export interface IScope {
    __parentID: string;
    __children: IScope[];
    __functions: IScope[];
    id: string;
    name: string;
    type: string;
    [key: string]: any;
}

interface IScopeEdge extends IScope {
    IdSource: string;
    IdTarget: string;
}

export class MetricGraph extends Graph2<IScope, IScopeEdge, IScope> {

    constructor() {
        super();
        this.idFunc(scope => scope.id);
        this.sourceFunc(scope => scope.IdSource);
        this.targetFunc(scope => scope.IdTarget);
        this.load([]);
    }

    load(data: any[]): this {
        this.clear();

        const index: { [id: string]: IScope } = {};
        data.forEach((scope: IScope, idx) => {
            index[scope.id] = scope;
        });

        for (let i = 0; i < data.length; ++i) {
            const scope = data[i];
            const parents = scope.name.split(":");
            parents.pop();
            let parentID = parents.pop();
            while (parentID && (parentID[0] === "a" || parentID[0] === "c")) {
                parentID = parents.pop();
            }
            scope.__children = [];
            scope.__functions = [];
            scope.__parentID = parentID;
            if (parentID && !index[parentID]) {
                index[parentID] = {
                    id: parentID,
                    type: "unknown",
                    name: parents.length ? parents.join(":") + ":" + parentID : parentID,
                } as IScope;
                data.splice(i, 0, index[parentID]);
                --i;
            }
        }

        data.forEach((scope: IScope) => {
            const parent = index[scope.__parentID];
            if (parent) {
                if (scope.type === "function") {
                    parent.__functions.push(scope);
                } else {
                    parent.__children.push(scope);
                }
            }
        });

        data.forEach((scope: IScope) => {
            const parentScope = index[scope.__parentID];
            switch (scope.type) {
                case "function":
                    break;
                case "activity":
                    this.addVertex(scope, parentScope);
                    break;
                case "edge":
                    break;
                default:
                    if (scope.__children.length) {
                        if (!this.subgraphExists(scope.id)) {
                            this.addSubgraph(scope, parentScope);
                        }
                    } else {
                        this.addVertex(scope, parentScope);
                    }
            }
        });

        data.forEach((scope: IScope) => {
            if (scope.type === "edge") {
                if (!this.vertexExists((scope as IScopeEdge).IdSource))
                    console.warn(`Missing vertex:  ${(scope as IScopeEdge).IdSource}`);
                else if (!this.vertexExists((scope as IScopeEdge).IdTarget)) {
                    console.warn(`Missing vertex:  ${(scope as IScopeEdge).IdTarget}`);
                } else {
                    if (scope.__parentID && !this.subgraphExists(scope.__parentID)) {
                        console.warn(`Edge missing subgraph:  ${scope.__parentID}`);
                    }
                    if (this.subgraphExists(scope.__parentID)) {
                        this.addEdge(scope as IScopeEdge, this.subgraph(scope.__parentID));
                    } else {
                        this.addEdge(scope as IScopeEdge);
                    }
                }
            }
        });

        return this;
    }

    safeID(id: string) {
        return id.replace(/\s/, "_");
    }

    vertexTpl(v: IScope, options: MetricsOptions): string {
        return `"${v.id}" [id="${v.id}" label="[${decodeHTML(v.Kind)}]\n${decodeHTML(v.Label) || v.id}" shape="${shape(v.Kind)}"]`;
    }

    protected _dedupEdges: { [id: string]: boolean } = {};

    edgeTpl(e: IScopeEdge, options: MetricsOptions) {
        if (this._dedupEdges[e.id] === true) return "";
        this._dedupEdges[e.id] = true;
        if (options.ignoreGlobalStoreOutEdges && this.vertex(e.IdSource).Kind === "22") {
            return "";
        }
        return `\"${e.IdSource}" -> "${e.IdTarget}" [id="${e.id}" label="" style="${this.vertexParent(e.IdSource) === this.vertexParent(e.IdTarget) ? "solid" : "dashed"}"]`;
    }

    subgraphTpl(sg: IScope, options: MetricsOptions): string {
        const childTpls: string[] = [];
        this.subgraphSubgraphs(sg.id).forEach(child => {
            childTpls.push(this.subgraphTpl(child, options));
        });
        this.subgraphVertices(sg.id).forEach(child => {
            childTpls.push(this.vertexTpl(child, options));
        });
        this.subgraphEdges(sg.id).forEach(child => {
            childTpls.push(this.edgeTpl(child, options));
        });
        return `\
subgraph cluster_${sg.id} {
    color="darkgrey";
    fillcolor="white";
    style="filled";
    id="${sg.id}";
    label="${sg.id}";

    ${childTpls.join("\n")}

}`;
    }

    graphTpl(items: IScope[] = [], options: MetricsOptions) {
        // subgraphs.sort();
        this._dedupEdges = {};
        const childTpls: string[] = [];
        if (items?.length) {
            items.forEach(item => {
                if (this.subgraphExists(item.id)) {
                    childTpls.push(this.subgraphTpl(item, options));
                } else {
                    if (item?.__parentID && this.subgraphExists(item?.__parentID)) {
                        childTpls.push(this.subgraphTpl(this.subgraph(item.__parentID), options));
                    }
                }
            });
            this.allEdges().filter(e => {
                const sV = this.vertex(e.IdSource);
                const tV = this.vertex(e.IdTarget);
                return sV.__parentID !== tV.__parentID && items.indexOf(this.subgraph(sV.__parentID)) >= 0 && items.indexOf(this.subgraph(tV.__parentID)) >= 0;
            }).forEach(e => {
                childTpls.push(this.edgeTpl(e, options));
            });
        } else {
            this.subgraphs().forEach(child => {
                childTpls.push(this.subgraphTpl(child, options));
            });
            this.vertices().forEach(child => {
                childTpls.push(this.vertexTpl(child, options));
            });
            this.edges().forEach(child => {
                childTpls.push(this.edgeTpl(child, options));
            });
        }
        return `\
digraph G {
    graph [fontname="arial"];// fontsize=11.0];
    // graph [rankdir=TB];
    // node [shape=rect fontname=arial fontsize=11.0 fixedsize=true];
    node [color="darkgrey" fontname="arial" fillcolor="whitesmoke" style="filled" margin=0.2]
    edge [color="darkgrey"]
    // edge [fontname=arial fontsize=11.0];
    
    ${childTpls.join("\n")}

}`;
    }
}

export class MetricGraphWidget extends SVGZoomWidget {

    protected _selection: { [id: string]: boolean } = {};

    constructor() {
        super();
        this._drawStartPos = "origin";
        this._iconBar
            .buttons([
                new Button().faChar("fa-arrows-alt").tooltip("Zoom to fit")
                    .on("click", () => {
                        this.zoomToFit();
                    }),
                new Spacer().vline(false),
                new Button().faChar("fa-plus").tooltip("Zoom in")
                    .on("click", () => {
                        this.zoomPlus();
                    }),
                new Button().faChar("fa-minus").tooltip("Zoom out")
                    .on("click", () => {
                        this.zoomMinus();
                    })
            ])
            ;

    }

    clearSelection(broadcast: boolean = false) {
        Object.keys(this._selection).forEach(id => {
            d3Select(`#${id}`).classed("selected", false);
        });
        this._selection = {};
        this._selectionChanged(broadcast);
    }

    toggleSelection(id: string, broadcast: boolean = false) {
        if (this._selection[id]) {
            delete this._selection[id];
        } else {
            this._selection[id] = true;
        }
        this._selectionChanged(broadcast);
    }

    selection(): string[];
    selection(_: string[]): this;
    selection(_: string[], broadcast: boolean): this;
    selection(_?: string[], broadcast: boolean = false): string[] | this {
        if (!arguments.length) return Object.keys(this._selection);
        this.clearSelection();
        _.forEach(id => this._selection[id] = true);
        this._selectionChanged(broadcast);
        return this;
    }

    protected _dot = "";
    dot(_: string): this {
        this._dot = _;
        return this;
    }

    protected _prevDot;
    _prevGV;
    update(domNode, element) {
        super.update(domNode, element);
    }

    _selectionChanged(broadcast = false) {
        const context = this;
        this._renderElement.selectAll(".node,.edge,.cluster")
            .each(function () {
                d3Select(this).selectAll("path,polygon")
                    .style("stroke", () => {
                        return context._selection[this.id] ? "red" : "darkgrey";
                    })
                    ;
            })
            ;
        if (broadcast) {
            this.selectionChanged();
        }
    }

    render(callback) {
        return super.render(w => {
            if (this._prevDot !== this._dot) {
                this._prevDot = this._dot;
                this?._prevGV?.terminate();
                const dot = this._dot;
                this._prevGV = graphviz(dot, "dot", dojoConfig.urlInfo.fullPath + "/dist");
                this._prevGV.response.then(svg => {
                    //  Check for race condition  ---
                    if (dot === this._prevDot) {
                        const startPos = svg.indexOf("<g id=");
                        const endPos = svg.indexOf("</svg>");
                        this._renderElement.html(svg.substring(startPos, endPos));
                        const context = this;
                        setTimeout(() => {
                            this.zoomToFit(0);
                            this._renderElement.selectAll(".node,.edge,.cluster")
                                .on("click", function () {
                                    const event = d3Event();
                                    if (!event.ctrlKey) {
                                        context.clearSelection();
                                    }
                                    context.toggleSelection(this.id, true);
                                });
                            if (callback) {
                                callback(this);
                            }
                        }, 0);
                    }
                }).catch(e => {
                    if (callback) {
                        callback(this);
                    }
                });
            } else {
                if (callback) {
                    callback(this);
                }
            }
        });
    }

    //  Events  ---
    selectionChanged() {
    }
}
