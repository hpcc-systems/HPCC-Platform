import { d3Event, select as d3Select, SVGZoomWidget } from "@hpcc-js/common";
import { graphviz } from "@hpcc-js/graph";
import { Graph2, hashSum, scopedLogger } from "@hpcc-js/util";
import { format } from "src/Utility";
import { MetricsOptions } from "../hooks/metrics";

import "/src-react/util/metricGraph.css";

const logger = scopedLogger("src-react/util/metricGraph.ts");

declare const dojoConfig;

const TypeShape = {
    "function": 'plain" fillcolor="" style="'
};

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

function shape(v: IScope) {
    return TypeShape[v.type] ?? KindShape[v.Kind] ?? "rectangle";
}

const CHARS = new Set("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
function encodeID(id: string): string {
    let retVal = "";
    for (let i = 0; i < id.length; ++i) {
        if (CHARS.has(id.charAt(i))) {
            retVal += id.charAt(i);
        } else {
            retVal += `__${id.charCodeAt(i)}__`;
        }
    }
    return retVal;
}

function decodeID(id: string): string {
    return id.replace(/__(\d+)__/gm, (_match, p1) => String.fromCharCode(+p1));
}

function encodeLabel(label: string) {
    return label
        .split('"')
        .join('\\"')
        .split("\n")
        .join("\\n")
        ;
}

export interface IScope {
    __parentName?: string;
    __children?: IScope[];
    __formattedProps: { [key: string]: any };
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

type ScopeStatus = "unknown" | "started" | "completed";

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
                    __formattedProps: {},
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
        if (scope.__children?.length > 0 && !this.subgraphExists(scope.name)) {
            this.addSubgraph(scope, parent);
        }
    }

    lineage(scope: IScope): IScope[] {
        const retVal: IScope[] = [];
        while (scope) {
            retVal.push(scope);
            scope = this._index[scope.__parentName];
        }
        return retVal.reverse();
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
            if (scope.type === "edge" && scope.IdSource !== undefined && scope.IdTarget !== undefined) {
                if (!this.vertexExists(this._activityIndex[(scope as IScopeEdge).IdSource]))
                    logger.warning(`Missing vertex:  ${(scope as IScopeEdge).IdSource}`);
                else if (!this.vertexExists(this._activityIndex[(scope as IScopeEdge).IdTarget])) {
                    logger.warning(`Missing vertex:  ${(scope as IScopeEdge).IdTarget}`);
                } else {
                    if (scope.__parentName && !this.subgraphExists(scope.__parentName)) {
                        logger.warning(`Edge missing subgraph:  ${scope.__parentName}`);
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

    vertexLabel(v: IScope, options: MetricsOptions): string {
        return v.type === "activity" ? format(options.activityTpl, v) : v.Label || v.id;
    }

    vertexStatus(v: IScope): ScopeStatus {
        const tally: { [id: string]: number } = { "unknown": 0, "started": 0, "completed": 0 };
        let outEdges = this.vertexInternalOutEdges(v);
        if (outEdges.length === 0) {
            outEdges = this.inEdges(v.name);
        }
        outEdges.forEach(e => ++tally[this.edgeStatus(e)]);
        if (outEdges.length === tally["completed"]) {
            return "completed";
        } else if (tally["started"] || tally["completed"]) {
            return "started";
        }
        return "unknown";
    }

    vertexInternalOutEdges(v: IScope): IScopeEdge[] {
        return this.outEdges(v.name).filter(e => e.__parentName === v.__parentName);
    }

    protected _dedupVertices: { [scopeName: string]: boolean } = {};
    vertexTpl(v: IScope, options: MetricsOptions): string {
        if (this._dedupVertices[v.id] === true) return "";
        this._dedupVertices[v.id] = true;
        return `"${v.id}" [id="${encodeID(v.name)}" label="${encodeLabel(this.vertexLabel(v, options))}" shape="${shape(v)}" class="${this.vertexStatus(v)}"]`;
    }

    hiddenTpl(v: IScope, options: MetricsOptions): string {
        if (this._dedupVertices[v.id] === true) return "";
        this._dedupVertices[v.id] = true;
        return `"${v.id}" [id="${encodeID(v.name)}" label="${encodeLabel(this.vertexLabel(v, options))}" shape="${shape(v)}" class="${this.vertexStatus(v)}" rank="min"]`;
    }

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

    edgeStatus(e: IScopeEdge): ScopeStatus {
        const starts = Number(e.NumStarts ?? 0);
        const stops = Number(e.NumStops ?? 0);
        if (!isNaN(starts) && !isNaN(stops)) {
            if (starts > 0) {
                if (starts === stops) {
                    return "completed";
                }
                return "started";
            }
        }
        return "unknown";
    }

    protected _dedupEdges: { [scopeName: string]: boolean } = {};
    edgeTpl(e: IScopeEdge, options: MetricsOptions) {
        if (this._dedupEdges[e.id] === true) return "";
        this._dedupEdges[e.id] = true;
        if (options.ignoreGlobalStoreOutEdges && this.vertex(this._activityIndex[e.IdSource]).Kind === "22") {
            return "";
        }
        const ltail = this.subgraphExists(this._sourceFunc(e)) ? `ltail=cluster_${e.IdSource}` : "";
        const lhead = this.subgraphExists(this._targetFunc(e)) ? `lhead=cluster_${e.IdTarget}` : "";
        return `"${e.IdSource}" -> "${e.IdTarget}" [id="${encodeID(e.name)}" label="${encodeLabel(format(options.edgeTpl, { ...e, ...e.__formattedProps }))}" style="${this.vertexParent(this._activityIndex[e.IdSource]) === this.vertexParent(this._activityIndex[e.IdTarget]) ? "solid" : "dashed"}" class="${this.edgeStatus(e)}" ${ltail} ${lhead}]`;
    }

    subgraphStatus(sg: IScope): ScopeStatus {
        const tally: { [id: string]: number } = { "unknown": 0, "started": 0, "completed": 0 };
        const finalVertices = this.subgraphVertices(sg.name).filter(v => this.vertexInternalOutEdges(v).length === 0);
        finalVertices.forEach(v => ++tally[this.vertexStatus(v)]);
        if (finalVertices.length && finalVertices.length === tally["completed"]) {
            return "completed";
        } else if (tally["started"] || tally["completed"]) {
            return "started";
        }
        return "unknown";
    }

    protected _dedupSubgraphs: { [scopeName: string]: boolean } = {};
    subgraphTpl(sg: IScope, options: MetricsOptions): string {
        if (this._dedupSubgraphs[sg.id] === true) return "";
        this._dedupSubgraphs[sg.id] = true;
        const childTpls: string[] = [];
        this.subgraphSubgraphs(sg.name).forEach(child => {
            childTpls.push(this.subgraphTpl(child, options));
        });
        if (this.vertexExists(this.id(sg))) {
            childTpls.push(this.hiddenTpl(this.vertex(this.id(sg)), options));
        }
        this.subgraphVertices(sg.name).forEach(child => {
            childTpls.push(this.vertexTpl(child, options));
        });
        this.subgraphEdges(sg.name).forEach(child => {
            childTpls.push(this.edgeTpl(child, options));
        });
        return `\
subgraph cluster_${encodeID(sg.id)} {
    fillcolor="white";
    style="${sg.type === "child" ? "dashed" : "filled"}";
    id="${encodeID(sg.name)}";
    label="${sg.type === "child" ? "" : encodeLabel(format(sg.type === "activity" ? options.activityTpl : options.subgraphTpl, sg))}";
    class="${this.subgraphStatus(sg)}";

    ${childTpls.join("\n")}

}`;
    }

    graphTpl(items: IScope[] = [], options: MetricsOptions) {
        this._dedupSubgraphs = {};
        this._dedupVertices = {};
        this._dedupEdges = {};
        const childTpls: string[] = [];
        if (items?.length) {
            items.map(item => {
                if (this.subgraphExists(this.id(item))) {
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
    compound=true;
    oredering=in;
    graph [fontname="arial"];// fontsize=11.0];
    // graph [rankdir=TB];
    // node [shape=rect fontname=arial fontsize=11.0 fixedsize=true];
    node [color="" fontname="arial" fillcolor="whitesmoke" style="filled" margin=0.2]
    edge []
    // edge [fontname=arial fontsize=11.0];

    ${childTpls.join("\n")}

}`;
    }
}

export class Rect {

    left: number;
    top: number;
    right: number;
    bottom: number;

    toStruct() {
        return { x: this.left, y: this.top, width: this.right - this.left, height: this.bottom - this.top };
    }

    extend(rect: SVGRect) {
        if (this.left === undefined || this.left > rect.x) {
            this.left = rect.x;
        }
        if (this.top === undefined || this.top > rect.y + rect.height) {
            this.top = rect.y + rect.height;
        }
        if (this.right === undefined || this.right < rect.x + rect.width) {
            this.right = rect.x + rect.width;
        }
        if (this.bottom === undefined || this.bottom < rect.y) {
            this.bottom = rect.y;
        }
    }
}

interface GraphvizWorkerResponse {
    svg: string;
}

interface GraphvizWorkerError {
    error: string;
    errorDot: string;
}

export function isGraphvizWorkerResponse(response: GraphvizWorkerResponse | GraphvizWorkerError): response is GraphvizWorkerResponse {
    return (response as GraphvizWorkerResponse).svg !== undefined;
}

interface GraphvizWorker {
    terminate: () => void;
    response: Promise<GraphvizWorkerResponse | GraphvizWorkerError>;
    svg?: string;
    error?: string;
}

export enum LayoutStatus {
    UNKNOWN,
    STARTED,
    COMPLETED,
    FAILED
}

class LayoutCache {

    protected _cache: { [key: string]: GraphvizWorker } = {};

    calcSVG(dot: string): Promise<GraphvizWorkerResponse | GraphvizWorkerError> {
        const hashDot = hashSum(dot);
        if (!(hashDot in this._cache)) {
            this._cache[hashDot] = graphviz(dot, "dot", dojoConfig.urlInfo.fullPath + "/dist") as unknown as GraphvizWorker;
            this._cache[hashDot].response.then(response => {
                if (isGraphvizWorkerResponse(response)) {
                    this._cache[hashDot].svg = response.svg as string;
                } else {
                    logger.error(`Invalid DOT:  ${response.error}`);
                    this._cache[hashDot].error = response.error;
                }
            }).catch(e => {
                logger.error(`Invalid DOT:  ${e}`);
                this._cache[hashDot].error = e;
            });
        }
        return this._cache[hashDot].response;
    }

    status(dot: string): LayoutStatus {
        const hashDot = hashSum(dot);
        if (!(hashDot in this._cache)) {
            return LayoutStatus.UNKNOWN;
        } else if (this._cache[hashDot].svg) {
            return LayoutStatus.COMPLETED;
        } else if (this._cache[hashDot].error) {
            return LayoutStatus.FAILED;
        }
        return LayoutStatus.STARTED;
    }

    isComplete(dot: string): boolean {
        switch (this.status(dot)) {
            case LayoutStatus.COMPLETED:
            case LayoutStatus.FAILED:
                return true;
        }
        return false;
    }
}

export const layoutCache = new LayoutCache();

export class MetricGraphWidget extends SVGZoomWidget {

    protected _selection: { [id: string]: boolean } = {};

    constructor() {
        super();
        this._drawStartPos = "origin";
        this.showToolbar(false);

        this._iconBar
            .buttons([])
            ;
    }

    exists(id: string) {
        return id && !this._renderElement.select(`#${encodeID(id)}`).empty();
    }

    clearSelection(broadcast: boolean = false) {
        Object.keys(this._selection).filter(name => !!name).forEach(name => {
            d3Select(`#${encodeID(name)}`).classed("selected", false);
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

    selectionCompare(_: string[]): boolean {
        const currSelection = this.selection();
        return currSelection.length !== _.length || _.some(id => currSelection.indexOf(id) < 0);
    }

    selection(): string[];
    selection(_: string[]): this;
    selection(_: string[], broadcast: boolean): this;
    selection(_?: string[], broadcast: boolean = false): string[] | this {
        if (!arguments.length) return Object.keys(this._selection);
        if (this.selectionCompare(_)) {
            this.clearSelection();
            _.forEach(id => this._selection[id] = true);
            this._selectionChanged(broadcast);
        }
        return this;
    }

    itemBBox(scopeID: string) {
        const rect = new Rect();
        const elem = this._renderElement.select(`#${encodeID(scopeID)}`);
        const node = elem.node() as SVGGraphicsElement;
        if (node) {
            rect.extend(node.getBBox());
        }

        const bbox = rect.toStruct();
        const renderBBox = this._renderElement.node().getBBox();
        bbox.y += renderBBox.height;
        return bbox;
    }

    selectionBBox() {
        const rect = new Rect();
        this.selection().filter(sel => !!sel).forEach(sel => {
            const elem = this._renderElement.select(`#${encodeID(sel)}`);
            if (elem?.node()) {
                rect.extend((elem.node() as SVGGraphicsElement).getBBox());
            }
        });
        const bbox = rect.toStruct();
        const renderBBox = this._renderElement.node().getBBox();
        bbox.y += renderBBox.height;
        return bbox;
    }

    _selectionChanged(broadcast = false) {
        const context = this;
        this._renderElement.selectAll(".node,.edge,.cluster")
            .each(function () {
                d3Select(this).selectAll("path,polygon")
                    .style("stroke", () => {
                        return context._selection[decodeID(this.id)] ? context.selectionGlowColor() : undefined;
                    })
                    ;
                d3Select(this).selectAll("polygon")
                    .style("fill", () => {
                        return context._selection[decodeID(this.id)] ? "LightCyan" : undefined;
                    })
                    ;
            })
            ;
        if (broadcast) {
            this.selectionChanged();
        }
    }

    protected _prevSVG;
    protected _svg = "";
    reset() {
        this._prevSVG = "";
        return this;
    }

    svg(): string;
    svg(_: string): this;
    svg(_?: string): this | string {
        if (arguments.length === 0) return this._svg;
        this._svg = _;
        return this;
    }

    centerOnItem(scopeID: string) {
        this.centerOnBBox(this.itemBBox(scopeID));
        return this;
    }

    centerOnSelection(transitionDuration?: number) {
        this.centerOnBBox(this.selectionBBox(), transitionDuration);
        return this;
    }

    zoomToItem(scopeID: string) {
        this.zoomToBBox(this.itemBBox(scopeID));
        return this;
    }

    zoomToSelection(transitionDuration?: number) {
        this.zoomToBBox(this.selectionBBox(), transitionDuration);
        return this;
    }

    update(domNode, element) {
        super.update(domNode, element);
    }

    async renderSVG(svg: string): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this._selection = {};
            const startPos = svg.indexOf("<g id=");
            const endPos = svg.indexOf("</svg>");
            this._renderElement.html(svg.substring(startPos, endPos));
            setTimeout(() => {
                this
                    .zoomToFit(0)
                    ;
                const context = this;
                this._renderElement.selectAll(".node,.edge,.cluster")
                    .on("click", function () {
                        const event = d3Event();
                        if (!event.ctrlKey) {
                            context.clearSelection();
                        }
                        context.toggleSelection(decodeID(this.id), true);
                    });
                resolve();
            }, 0);
        });
    }

    render(callback?: (w: MetricGraphWidget) => void) {

        return super.render(async w => {
            if (this._prevSVG !== this._svg) {
                this._prevSVG = this._svg;
                await this.renderSVG(this._svg);
            }
            if (callback) {
                callback(this);
            }
        });
    }

    //  Events  ---
    selectionChanged() {
    }
}
MetricGraphWidget.prototype._class += " eclwatch_MetricGraphWidget";
