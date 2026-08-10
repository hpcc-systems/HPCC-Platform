import { graphvizDot, Graphviz as GraphvizViz } from "@hpcc-js/graph";
import { Graphviz, type Engine, type Format, type Graph, type Subgraph } from "@hpcc-js/wasm-graphviz";
import { Graph2, hashSum, scopedLogger } from "@hpcc-js/util";
import { format } from "src/Utility";
import { IScopeEx, MetricsView } from "../hooks/metrics";

import "src-react-css/util/metricGraph.css";

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

function shape(v: IScopeEx) {
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

interface IScopeEdge extends IScopeEx {
    IdSource: string;
    IdTarget: string;
}

type ScopeStatus = "unknown" | "running" | "completed" | "failed";
type ExceptionStatus = "warning" | "error";

interface IExceptionSummary {
    Severity?: string;
}

export class MetricGraph extends Graph2<IScopeEx, IScopeEdge, IScopeEx> {

    protected _graphviz: Graphviz;
    protected _index: { [name: string]: IScopeEx } = {};
    protected _activityIndex: { [id: string]: string } = {};

    private constructor(graphviz: Graphviz) {
        super();
        this._graphviz = graphviz;
        this.idFunc(scope => scope.name);
        this.sourceFunc(scope => this._activityIndex[scope.IdSource]);
        this.targetFunc(scope => this._activityIndex[scope.IdTarget]);
        this.load([]);
    }

    static async create(): Promise<MetricGraph> {
        return new MetricGraph(await Graphviz.load());
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

    childCount(scopeName: string) {
        return this.allVertices().filter(v => v.name.startsWith(scopeName)).length;
    }

    protected ensureLineage(_scope: IScopeEx): IScopeEx {
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
                    __groupedProps: {},
                    __StdDevs: 0,
                    __StdDevsSource: "",
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

    protected ensureGraphLineage(scope: IScopeEx) {
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

    lineage(scope: IScopeEx): IScopeEx[] {
        const retVal: IScopeEx[] = [];
        while (scope) {
            retVal.push(scope);
            scope = this._index[scope.__parentName];
        }
        return retVal.reverse();
    }

    load(data: IScopeEx[]): this {
        this.clear();

        data.forEach((scope: IScopeEx) => {
            this.ensureLineage(scope);
        });

        data.forEach((scope: IScopeEx) => {
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

        data.forEach((scope: IScopeEx) => {
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

    vertexLabel(v: IScopeEx, options: MetricsView): string {
        return v.type === "activity" ? format(options.activityTpl, v) :
            v.type === "function" ? v.id + "()" :
                v.type === "operation" && v.id.charAt(0) === ">" ? v.id.substring(1) :
                    v.Label || v.id;
    }

    vertexStatus(v: IScopeEx): ScopeStatus {
        const tally: { [id: string]: number } = { "unknown": 0, "started": 0, "completed": 0 };
        let outEdges = this.vertexInternalOutEdges(v);
        if (outEdges.length === 0) {
            outEdges = this.inEdges(v.name);
        }
        outEdges.forEach(e => ++tally[this.edgeStatus(e)]);
        if (outEdges.length === tally["completed"]) {
            return "completed";
        } else if (tally["started"] || tally["completed"]) {
            return "running";
        }
        return "unknown";
    }

    vertexClass(v: IScopeEx): string {
        const retVal: Array<ScopeStatus | ExceptionStatus> = [this.vertexStatus(v)];
        const exceptionClass = this.exceptionClass(v);
        if (exceptionClass) {
            retVal.push(exceptionClass);
        }
        return retVal.join(" ");
    }

    exceptionClass(item: { __exceptions?: IExceptionSummary[] }): ExceptionStatus | undefined {
        if (!item.__exceptions?.length) {
            return undefined;
        }
        const severity: { [id: string]: number } = {};
        item.__exceptions.forEach(ex => {
            if (!severity[ex.Severity]) {
                severity[ex.Severity] = 0;
            }
            severity[ex.Severity]++;
        });
        if (severity["Error"]) {
            return "error";
        }
        if (severity["Warning"]) {
            return "warning";
        }
        return undefined;
    }

    subgraphClass(sg: IScopeEx): string {
        const classes: Array<ScopeStatus | ExceptionStatus> = [this.subgraphStatus(sg)];
        const exceptionClass = this.exceptionClass(sg);
        if (exceptionClass) {
            classes.push(exceptionClass);
        }
        return classes.join(" ");
    }

    vertexInternalOutEdges(v: IScopeEx): IScopeEdge[] {
        return this.outEdges(v.name).filter(e => e.__parentName === v.__parentName);
    }

    activityByID(id: string): IScopeEx | undefined {
        const name = this._activityIndex[id];
        return name && this.vertexExists(name) ? this.vertex(name) : undefined;
    }

    inActivities(scope: IScopeEx): IScopeEx[] {
        if (!this.vertexExists(scope.name)) return [];
        return this.inEdges(scope.name)
            .map(e => this.activityByID(e.IdSource))
            .filter(v => !!v);
    }

    outActivities(scope: IScopeEx): IScopeEx[] {
        if (!this.vertexExists(scope.name)) return [];
        return this.outEdges(scope.name)
            .map(e => this.activityByID(e.IdTarget))
            .filter(v => !!v);
    }

    allSubgraphVertices(scopeName: string): IScopeEx[] {
        const result: IScopeEx[] = [...this.subgraphVertices(scopeName)];
        for (const child of this.subgraphSubgraphs(scopeName)) {
            result.push(...this.allSubgraphVertices(child.name));
        }
        return result;
    }

    inSubgraphActivities(scope: IScopeEx): IScopeEx[] {
        if (!this.subgraphExists(scope.name)) return [];
        const allVertices = this.allSubgraphVertices(scope.name);
        const innerNames = new Set(allVertices.map(v => v.name));
        const sources: IScopeEx[] = [];
        const seen = new Set<string>();
        for (const v of allVertices) {
            for (const e of this.inEdges(v.name)) {
                const src = this.activityByID(e.IdSource);
                if (src && !innerNames.has(src.name) && !seen.has(src.name)) {
                    seen.add(src.name);
                    sources.push(src);
                }
            }
        }
        return sources;
    }

    outSubgraphActivities(scope: IScopeEx): IScopeEx[] {
        if (!this.subgraphExists(scope.name)) return [];
        const allVertices = this.allSubgraphVertices(scope.name);
        const innerNames = new Set(allVertices.map(v => v.name));
        const targets: IScopeEx[] = [];
        const seen = new Set<string>();
        for (const v of allVertices) {
            for (const e of this.outEdges(v.name)) {
                const tgt = this.activityByID(e.IdTarget);
                if (tgt && !innerNames.has(tgt.name) && !seen.has(tgt.name)) {
                    seen.add(tgt.name);
                    targets.push(tgt);
                }
            }
        }
        return targets;
    }

    protected _dedupVertices: { [scopeName: string]: boolean } = {};

    private _buildVertexTemplate(target: Graph | Subgraph, v: IScopeEx, options: MetricsView, isHidden: boolean = false): void {
        if (this._dedupVertices[v.id] === true) return;
        this._dedupVertices[v.id] = true;

        target.addNode(v.id, { id: encodeID(v.name), label: encodeLabel(this.vertexLabel(v, options)), shape: shape(v), class: this.vertexClass(v) });

        if (isHidden) {
            target.setNodeAttr(v.id, "rank", "min");
        }
    }

    private _buildBoundaryPointDot(target: Graph | Subgraph, id: string): void {
        if (this._dedupVertices[id] === true) return;
        this._dedupVertices[id] = true;

        const activity = this.activityByID(id);
        if (!activity) return;
        target.addNode(id, { id: encodeID(activity.name), label: "", shape: "point", width: 0.1, fixedsize: true });
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
                return "running";
            }
        }
        return "unknown";
    }

    protected _dedupEdges: { [scopeName: string]: boolean } = {};
    private edgeTpl(target: Graph | Subgraph, e: IScopeEdge, options: MetricsView, renderedSubgraphs?: ReadonlySet<string>): void {
        if (this._dedupEdges[e.id] === true) return;
        this._dedupEdges[e.id] = true;

        const sourceVertexName = this._activityIndex[e.IdSource];
        const targetVertexName = this._activityIndex[e.IdTarget];

        if (options.ignoreGlobalStoreOutEdges && sourceVertexName) {
            const sourceVertex = this.vertex(sourceVertexName);
            if (sourceVertex.Kind === "22") {
                return;
            }
        }

        let edgeStyle = "solid";
        if (sourceVertexName && targetVertexName) {
            const sourceParent = this.vertexParent(sourceVertexName);
            const targetParent = this.vertexParent(targetVertexName);
            edgeStyle = sourceParent === targetParent ? "solid" : "dashed";
        }

        const formatData = e.__formattedProps ?
            Object.assign({}, e, e.__formattedProps) :
            e;

        const encodedName = encodeID(e.name);

        target.addEdge(e.IdSource, e.IdTarget, encodedName, {
            id: encodedName,
            label: encodeLabel(format(options.edgeTpl, formatData)),
            style: edgeStyle,
            class: this.edgeStatus(e)
        });

        const sourceName = `${this._sourceFunc(e)}`;
        const targetName = `${this._targetFunc(e)}`;
        if (this.subgraphExists(sourceName) && (!renderedSubgraphs || renderedSubgraphs.has(sourceName))) {
            target.setEdgeAttr(e.IdSource, e.IdTarget, encodedName, "ltail", `cluster_${e.IdSource}`);
        }
        if (this.subgraphExists(targetName) && (!renderedSubgraphs || renderedSubgraphs.has(targetName))) {
            target.setEdgeAttr(e.IdSource, e.IdTarget, encodedName, "lhead", `cluster_${e.IdTarget}`);
        }
    }

    subgraphStatus(sg: IScopeEx): ScopeStatus {
        const sgId = this.id(sg);
        const visibleVertices = this.subgraphVertices(sg.name).filter(v => v.id !== sgId);
        const finalVertices = visibleVertices.filter(v => this.vertexInternalOutEdges(v).filter(e => e.IdTarget !== sgId).length === 0);

        if (!visibleVertices.length && this.vertexExists(sgId)) {
            return this.vertexStatus(this.vertex(sgId));
        }

        const tally: { [id: string]: number } = { "unknown": 0, "running": 0, "completed": 0 };
        finalVertices.forEach(v => ++tally[this.vertexStatus(v)]);
        if (finalVertices.length && finalVertices.length === tally["completed"]) {
            return "completed";
        } else if (tally["running"] || tally["completed"]) {
            return "running";
        }
        return "unknown";
    }

    itemStatus(item: IScopeEx): ScopeStatus {
        if (this.isVertex(item)) {
            return this.vertexStatus(item);
        } else if (this.isEdge(item)) {
            return this.edgeStatus(item);
        } else if (this.isSubgraph(item)) {
            return this.subgraphStatus(item);
        }
        return "unknown";
    }

    protected _dedupSubgraphs: { [scopeName: string]: boolean } = {};
    private subgraphTpl(target: Graph | Subgraph, sg: IScopeEx, options: MetricsView): void {
        if (this._dedupSubgraphs[sg.id]) return;
        this._dedupSubgraphs[sg.id] = true;

        const encodedId = encodeID(sg.id);
        const encodedName = encodeID(sg.name);
        const sgType = sg.type;
        const isChild = sgType === "child";

        const subgraph = target.addSubgraph(encodedId, {
            color: "black",
            fillcolor: "white",
            style: isChild ? "dashed" : "filled",
            id: encodedName,
            label: isChild ? "" : encodeLabel(format(sgType === "activity" ? options.activityTpl : options.subgraphTpl, sg)),
            class: this.subgraphClass(sg)
        });

        for (const child of this.subgraphSubgraphs(sg.name)) {
            this.subgraphTpl(subgraph, child, options);
        }

        const sgId = this.id(sg);
        if (this.vertexExists(sgId)) {
            this._buildVertexTemplate(subgraph, this.vertex(sgId), options, true);
        }

        for (const child of this.subgraphVertices(sg.name)) {
            this._buildVertexTemplate(subgraph, child, options, false);
        }

        for (const child of this.subgraphEdges(sg.name)) {
            this.edgeTpl(subgraph, child, options);
        }
    }

    graphTpl(ids: string[] = [], options: MetricsView) {
        this._dedupSubgraphs = {};
        this._dedupVertices = {};
        this._dedupEdges = {};

        const g = this._graphviz.createGraph("G", "directed");
        try {
            g.setGraphAttr("compound", true);
            g.setGraphAttr("ordering", "in");
            g.setDefaultGraphAttr("fontname", "arial");
            g.setDefaultGraphAttr("style", "filled");
            g.setDefaultNodeAttr("fontname", "arial");
            g.setDefaultNodeAttr("margin", 0.2);

            if (ids?.length) {
                const selectedSubgraphs = new Map<string, IScopeEx>();

                for (const id of ids) {
                    let subgraph: IScopeEx | undefined;

                    if (this.subgraphExists(id)) {
                        subgraph = this.subgraph(id);
                    } else {
                        const item = this.item(id);
                        if (item?.__parentName && this.subgraphExists(item.__parentName)) {
                            subgraph = this.subgraph(item.__parentName);
                        }
                    }

                    if (subgraph) {
                        selectedSubgraphs.set(subgraph.name, subgraph);
                    }
                }

                const renderedSubgraphs = new Set<string>();
                const renderedVertexIds = new Set<string>();
                for (const subgraph of selectedSubgraphs.values()) {
                    const pendingSubgraphs = [subgraph];
                    while (pendingSubgraphs.length) {
                        const renderedSubgraph = pendingSubgraphs.pop();
                        if (renderedSubgraph && !renderedSubgraphs.has(renderedSubgraph.name)) {
                            renderedSubgraphs.add(renderedSubgraph.name);
                            pendingSubgraphs.push(...this.subgraphSubgraphs(renderedSubgraph.name));
                        }
                    }
                    for (const vertex of this.allSubgraphVertices(subgraph.name)) {
                        renderedVertexIds.add(vertex.id);
                    }
                    this.subgraphTpl(g, subgraph, options);
                }

                for (const edge of this.allEdges()) {
                    const sourceVertexName = this._activityIndex[edge.IdSource];
                    if (options.ignoreGlobalStoreOutEdges && sourceVertexName && this.vertex(sourceVertexName).Kind === "22") {
                        continue;
                    }
                    const sourceIsRendered = renderedVertexIds.has(edge.IdSource);
                    const targetIsRendered = renderedVertexIds.has(edge.IdTarget);

                    if (sourceIsRendered !== targetIsRendered) {
                        this._buildBoundaryPointDot(g, sourceIsRendered ? edge.IdTarget : edge.IdSource);
                        this.edgeTpl(g, edge, options, renderedSubgraphs);
                    } else if (sourceIsRendered && targetIsRendered) {
                        this.edgeTpl(g, edge, options, renderedSubgraphs);
                    }
                }
            } else {
                for (const child of this.subgraphs()) {
                    this.subgraphTpl(g, child, options);
                }
                for (const child of this.vertices()) {
                    this._buildVertexTemplate(g, child, options, false);
                }
                for (const child of this.edges()) {
                    this.edgeTpl(g, child, options);
                }
            }
            return g.toDot();
        } finally {
            g.delete();
        }
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
    LONG_RUNNING,
    COMPLETED,
    FAILED
}
export function isLayoutComplete(status: LayoutStatus) {
    return status === LayoutStatus.COMPLETED || status === LayoutStatus.FAILED;
}

export const METRIC_GRAPH_HOVER_EVENT = "eclwatch-metric-graph-hover";

export interface MetricGraphHoverDetail {
    widgetID: string;
    id?: string;
    encodedId?: string;
    relatedTarget?: EventTarget | null;
    clientX?: number;
    clientY?: number;
    pageX?: number;
    pageY?: number;
    anchorLeft?: number;
    anchorTop?: number;
    anchorWidth?: number;
    anchorHeight?: number;
}

function graphvizDotAdapter(dot: string, engine: Engine, format: Format): GraphvizWorker {
    const worker = graphvizDot(dot, engine, format);
    const retVal: GraphvizWorker = {
        terminate: worker.terminate,
        response: undefined
    };
    retVal.response = new Promise((resolve, reject) => {
        worker.response.then(response => {
            retVal.svg = response;
            resolve({ svg: response });
        }).catch(e => {
            retVal.error = e;
            reject({ error: e, errorDot: dot });
        });
    });
    return retVal;
}

class LayoutCache {

    protected _cache: { [key: string]: GraphvizWorker } = {};

    calcSVG(dot: string): Promise<GraphvizWorkerResponse | GraphvizWorkerError> {
        const hashDot = hashSum(dot);
        if (!(hashDot in this._cache)) {
            this._cache[hashDot] = graphvizDotAdapter(dot, "dot", "svg");
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

    svg(dot: string) {
        const hashDot = hashSum(dot);
        if (hashDot in this._cache) {
            return this._cache[hashDot].svg;
        }
        return "";
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
        return isLayoutComplete(this.status(dot));
    }
}
export const layoutCache = new LayoutCache();

export class MetricGraphWidget extends GraphvizViz.SVGWidget {
    protected _metricGraph?: MetricGraph;

    metricGraph(metricGraph: MetricGraph): this {
        this._metricGraph = metricGraph;
        return this;
    }

    private selectionBBoxWithEdgeEndpoints() {
        const ids = new Set(Object.keys(this._selection));
        for (const id of ids) {
            const item = this._metricGraph?.item(decodeID(id));
            if (item && this._metricGraph.isEdge(item)) {
                const source = this._metricGraph.activityByID(item.IdSource);
                const target = this._metricGraph.activityByID(item.IdTarget);
                if (source) ids.add(encodeID(source.name));
                if (target) ids.add(encodeID(target.name));
            }
        }

        let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
        for (const id of ids) {
            const bbox = this.itemBBox(id);
            minX = Math.min(minX, bbox.x);
            minY = Math.min(minY, bbox.y);
            maxX = Math.max(maxX, bbox.x + bbox.width);
            maxY = Math.max(maxY, bbox.y + bbox.height);
        }
        return isFinite(minX) && isFinite(minY) && isFinite(maxX) && isFinite(maxY) ?
            { x: minX, y: minY, width: maxX - minX, height: maxY - minY } : undefined;
    }

    zoomToSelection(transitionDuration?: number): this {
        const bbox = this.selectionBBoxWithEdgeEndpoints();
        if (bbox) this.zoomToBBox(bbox, transitionDuration);
        return this;
    }

    centerOnSelection(transitionDuration?: number): this {
        const bbox = this.selectionBBoxWithEdgeEndpoints();
        if (bbox) this.centerOnBBox(bbox, transitionDuration);
        return this;
    }

    selection(): string[];
    selection(_: string[]): this;
    selection(_: string[], broadcast: boolean): this;
    selection(_?: string[], broadcast: boolean = false): string[] | this {
        if (!arguments.length) return Object.keys(this._selection).map(decodeID);
        if (this.selectionCompare(_)) {
            this.clearSelection();
            _.forEach(id => this._selection[encodeID(id)] = true);
            this._selectionChanged(broadcast);
        }
        return this;
    }

    //  Events ---
    onHover(id?: string, element?: SVGGElement, event?: MouseEvent): string {
        const rect = element?.getBoundingClientRect();
        const detail: MetricGraphHoverDetail = {
            widgetID: this.id(),
            id: id ? decodeID(id) : undefined,
            encodedId: id,
            clientX: event?.clientX,
            clientY: event?.clientY,
            pageX: event?.pageX,
            pageY: event?.pageY,
            anchorLeft: rect?.left,
            anchorTop: rect?.top,
            anchorWidth: rect?.width,
            anchorHeight: rect?.height
        };
        document.dispatchEvent(new CustomEvent<MetricGraphHoverDetail>(METRIC_GRAPH_HOVER_EVENT, { detail }));
        return detail.id ?? "";
    }

    onHoverLeave(id?: string, _element?: SVGGElement, _event?: MouseEvent): string {
        const detail: MetricGraphHoverDetail = {
            widgetID: this.id(),
            id: undefined,
            encodedId: id,
            relatedTarget: _event?.relatedTarget,
            clientX: undefined,
            clientY: undefined,
            pageX: undefined,
            pageY: undefined,
            anchorLeft: undefined,
            anchorTop: undefined,
            anchorWidth: undefined,
            anchorHeight: undefined
        };
        document.dispatchEvent(new CustomEvent<MetricGraphHoverDetail>(METRIC_GRAPH_HOVER_EVENT, { detail }));
        return "";
    }
}
MetricGraphWidget.prototype._class += " eclwatch_MetricGraphWidget";

