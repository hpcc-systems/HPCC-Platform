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

function encodeID(id: string): string {
    return id.split(":").join("-");
}

function decodeID(id: string): string {
    return id.split("-").join(":");
}

export interface IScope {
    __parentName?: string;
    __children?: IScope[];
    id: string;
    name: string;
    type: string;
    Kind: string;
    Label: string;
    [key: string]: any;
}

interface IScopeEdge extends IScope {
    IdSource: string;
    IdTarget: string;
}

export class MetricGraph extends Graph2<IScope, IScopeEdge, IScope> {

    protected _index: { [name: string]: IScope } = {};
    protected _activityIndex: { [id: string]: string } = {};

    constructor() {
        super();
        this.idFunc(scope => scope.name);
        this.sourceFunc(scope => this._activityIndex[scope.IdSource]);
        this.targetFunc(scope => this._activityIndex[scope.IdTarget]);
        this.load([]);
    }

    clear(): this {
        super.clear();
        this._index = {};
        this._activityIndex = {};
        return this;
    }

    protected parentName(scopeName: string): string {
        const lastIdx = scopeName.lastIndexOf(":");
        if (lastIdx >= 0) {
            return scopeName.substring(0, lastIdx);
        }
        return !scopeName ? undefined : "";
    }

    protected scopeID(scopeName: string): string {
        const lastIdx = scopeName.lastIndexOf(":");
        if (lastIdx >= 0) {
            return scopeName.substring(lastIdx + 1);
        }
        return scopeName;
    }

    protected ensureLineage(_scope: IScope): IScope {
        let scope = this._index[_scope.name];
        if (!scope) {
            scope = _scope;
            scope.__children = scope.__children || [];
            scope.__parentName = scope.__parentName || this.parentName(scope.name);
            this._index[scope.name] = scope;
        }
        if (scope.__parentName !== undefined) {
            let parent = this._index[scope.__parentName];
            if (!parent) {
                parent = this.ensureLineage({
                    id: this.scopeID(scope.__parentName),
                    name: scope.__parentName,
                    type: "unknown",
                    Kind: "-1",
                    Label: "unknown"
                });
            }
            parent.__children.push(scope);
        }
        return scope;
    }

    protected ensureGraphLineage(scope: IScope) {
        let parent = this._index[scope.__parentName];
        if (parent === scope) {
            parent = undefined;
        }
        if (parent && !this.subgraphExists(parent.name)) {
            this.ensureGraphLineage(parent);
        }
        if (!this.subgraphExists(scope.name)) {
            this.addSubgraph(scope, parent);
        }
    }

    load(data: any[]): this {
        this.clear();

        //  Index all items  ---
        data.forEach((scope: IScope) => {
            this.ensureLineage(scope);
        });

        data.forEach((scope: IScope) => {
            const parentScope = this._index[scope.__parentName];
            this.ensureGraphLineage(scope);
            switch (scope.type) {
                case "activity":
                    this._activityIndex[scope.id] = scope.name;
                    this.addVertex(scope, parentScope);
                    break;
                case "edge":
                    break;
                default:
                    if (!scope.__children.length) {
                        this._activityIndex[scope.id] = scope.name;
                        this.addVertex(scope, parentScope);
                    }
            }
        });

        data.forEach((scope: IScope) => {
            if (scope.type === "edge") {
                if (!this.vertexExists(this._activityIndex[(scope as IScopeEdge).IdSource]))
                    console.warn(`Missing vertex:  ${(scope as IScopeEdge).IdSource}`);
                else if (!this.vertexExists(this._activityIndex[(scope as IScopeEdge).IdTarget])) {
                    console.warn(`Missing vertex:  ${(scope as IScopeEdge).IdTarget}`);
                } else {
                    if (scope.__parentName && !this.subgraphExists(scope.__parentName)) {
                        console.warn(`Edge missing subgraph:  ${scope.__parentName}`);
                    }
                    if (this.subgraphExists(scope.__parentName)) {
                        this.addEdge(scope as IScopeEdge, this.subgraph(scope.__parentName));
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
        return `"${v.id}" [id="${encodeID(v.name)}" label="[${decodeHTML(v.Kind)}]\n${decodeHTML(v.Label) || v.id}" shape="${shape(v.Kind)}"]`;
    }

    protected _dedupEdges: { [id: string]: boolean } = {};

    findFirstVertex(scopeName: string) {
        if (this.vertexExists(scopeName)) {
            return this.vertex(scopeName).id;
        }
        for (const child of this.item(scopeName).__children) {
            const childID = this.findFirstVertex(child.name);
            if (childID) {
                return childID;
            }
        }
    }

    edgeTpl(e: IScopeEdge, options: MetricsOptions) {
        if (this._dedupEdges[e.id] === true) return "";
        this._dedupEdges[e.id] = true;
        if (options.ignoreGlobalStoreOutEdges && this.vertex(this._activityIndex[e.IdSource]).Kind === "22") {
            return "";
        }
        return `"${e.IdSource}" -> "${e.IdTarget}" [id="${encodeID(e.name)}" label="" style="${this.vertexParent(this._activityIndex[e.IdSource]) === this.vertexParent(this._activityIndex[e.IdTarget]) ? "solid" : "dashed"}"]`;
    }

    subgraphTpl(sg: IScope, options: MetricsOptions): string {
        const childTpls: string[] = [];
        this.subgraphSubgraphs(sg.name).forEach(child => {
            childTpls.push(this.subgraphTpl(child, options));
        });
        this.subgraphVertices(sg.name).forEach(child => {
            childTpls.push(this.vertexTpl(child, options));
        });
        this.subgraphEdges(sg.name).forEach(child => {
            childTpls.push(this.edgeTpl(child, options));
        });
        return `\
subgraph cluster_${sg.id} {
    color="darkgrey";
    fillcolor="white";
    style="filled";
    id="${encodeID(sg.name)}";
    label="${sg.id}";

    ${childTpls.join("\n")}

}`;
    }

    graphTpl(items: IScope[] = [], options: MetricsOptions) {
        // subgraphs.sort();
        this._dedupEdges = {};
        const childTpls: string[] = [];
        if (items?.length) {
            items.map(item => {
                if (this.subgraphExists(item.id)) {
                    return item;
                } else {
                    if (item?.__parentName && this.subgraphExists(item?.__parentName)) {
                        return this.subgraph(item.__parentName);
                    }
                }
            }).filter(subgraph => !!subgraph).map(subgraph => {
                childTpls.push(this.subgraphTpl(subgraph, options));
                return subgraph;
            });
            this.allEdges().filter(e => {
                const sV = this.vertex(this._activityIndex[e.IdSource]);
                const tV = this.vertex(this._activityIndex[e.IdTarget]);
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
            d3Select(`#${encodeID(id)}`).classed("selected", false);
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
                        return context._selection[decodeID(this.id)] ? "red" : "darkgrey";
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
                                    context.toggleSelection(decodeID(this.id), true);
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
