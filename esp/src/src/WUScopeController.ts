import { format as d3Format, Icon, Palette } from "@hpcc-js/common";
import { BaseScope, ScopeEdge, ScopeGraph, ScopeSubgraph, ScopeVertex } from "@hpcc-js/comms";
import { Edge, IEdge, IGraphData, IGraphData2, IHierarchy, ISubgraph, IVertex, Lineage, Subgraph, Vertex } from "@hpcc-js/graph";
import { Edge as UtilEdge, Subgraph as UtilSubgraph, Vertex as UtilVertex } from "@hpcc-js/util";
import { decodeHtml } from "./Utility";

const formatNum = d3Format(",");

export type VertexType = Vertex | Icon;

export interface WUGraphLegendData {
    kind: number;
    faChar: string;
    label: string;
    count: number;
}

export interface MyGraphData {
    subgraphs: Subgraph[];
    vertices: VertexType[];
    edges: Edge[];
    hierarchy: Lineage[];
}

const UNKNOWN_STROKE = "darkgray";
const UNKNOWN_FILL = "lightgray";
const ACTIVE_STROKE = "#fea201";
const ACTIVE_FILL = "#fed080";
const FINISHED_STROKE = "darkgreen";
const FINISHED_FILL = "lightgreen";

const faCharType = {
    2: "\uf0c7",      //  Disk Write
    3: "\uf15d",      //  sort
    5: "\uf0b0",      //  Filter
    6: "\uf1e0",      //  Split
    12: "\uf039",     //  First N
    15: "\uf126",     //  Lightweight Join
    17: "\uf126",     //  Lookup Join
    22: "\uf1e6",     //  Pipe Output
    23: "\uf078",     //  Funnel
    25: "\uf0ce",     //  Inline Dataset
    26: "\uf074",     //  distribute
    29: "\uf005",     //  Store Internal Result
    36: "\uf128",     //  If
    44: "\uf0c7",     //  write csv
    47: "\uf0c7",     //  write
    54: "\uf013",     //  Workunit Read
    56: "\uf0c7",     //  Spill
    59: "\uf126",     //  Merge
    61: "\uf0c7",     //  write xml
    82: "\uf1c0",     //  Projected Disk Read Spill
    88: "\uf1c0",     //  Projected Disk Read Spill
    92: "\uf129",     //  Limted Index Read
    93: "\uf129",     //  Limted Index Read
    99: "\uf1c0",     //  CSV Read
    105: "\uf1c0",    //  CSV Read

    7: "\uf090",      //  Project
    9: "\uf0e2",      //  Local Iterate
    16: "\uf005",     //  Output Internal
    19: "\uf074",     //  Hash Distribute
    21: "\uf275",     //  Normalize
    35: "\uf0c7",     //  CSV Write
    37: "\uf0c7",     //  Index Write
    71: "\uf1c0",     //  Disk Read Spill
    133: "\uf0ce",    //  Inline Dataset
    148: "\uf0ce",    //  Inline Dataset
    168: "\uf275"    //  Local Denormalize
};

function faCharFactory(kind: number): string {
    return faCharType[kind] || "\uf063";
}

export abstract class WUScopeControllerBase<ISubgraph, IVertex, IEdge, IGraphData> {
    protected graphDB: ScopeGraph;
    protected subgraphsMap: { [id: string]: ISubgraph } = {};
    protected scopeSubgraphsMap: { [id: string]: ScopeSubgraph } = {};
    protected verticesMap: { [id: string]: IVertex } = {};
    protected scopeVerticesMap: { [id: string]: ScopeVertex } = {};
    protected edgesMap: { [id: string]: IEdge } = {};
    protected scopeEdgesMap: { [id: string]: ScopeEdge } = {};
    protected kindMap: { [id: string]: ScopeVertex[] } = {};

    protected _disabled: { [kind: number]: boolean } = {};

    clear() {
        this.subgraphsMap = {};
        this.scopeSubgraphsMap = {};
        this.verticesMap = {};
        this.scopeVerticesMap = {};
        this.edgesMap = {};
        this.scopeEdgesMap = {};
    }

    set(masterGraph: ScopeGraph) {
        this.graphDB = masterGraph || new ScopeGraph();
        this.graphGui(this.graphDB);

        this.kindMap = {};
        this.graphDB.walk(item => {
            if (item instanceof UtilSubgraph) {
            } else if (item instanceof UtilVertex) {
                const kind = item._.attr("Kind").RawValue;
                if (!this.kindMap[kind]) {
                    this.kindMap[kind] = [];
                }
                this.kindMap[kind].push(item);
            } else if (item instanceof UtilEdge) {
            }
        });
    }

    protected _showSubgraphs = true;
    showSubgraphs(): boolean;
    showSubgraphs(_: boolean): this;
    showSubgraphs(_?: boolean): boolean | this {
        if (!arguments.length) return this._showSubgraphs;
        this._showSubgraphs = _;
        return this;
    }

    protected _showIcon = true;
    showIcon(): boolean;
    showIcon(_: boolean): this;
    showIcon(_?: boolean): boolean | this {
        if (!arguments.length) return this._showIcon;
        this._showIcon = _;
        return this;
    }

    protected _vertexLabelTpl = "%Label%";
    vertexLabelTpl(): string;
    vertexLabelTpl(_: string): this;
    vertexLabelTpl(_?: string): string | this {
        if (!arguments.length) return this._vertexLabelTpl;
        this._vertexLabelTpl = _;
        return this;
    }

    protected _edgeLabelTpl = "%Label%\n%NumRowsProcessed%\n%SkewMinRowsProcessed% / %SkewMaxRowsProcessed%";
    edgeLabelTpl(): string;
    edgeLabelTpl(_: string): this;
    edgeLabelTpl(_?: string): string | this {
        if (!arguments.length) return this._edgeLabelTpl;
        this._edgeLabelTpl = _;
        return this;
    }

    disabled(): number[];
    disabled(_: number[]): this;
    disabled(_?: number[]): number[] | this {
        if (!arguments.length) {
            const retVal = [];
            for (const key in this._disabled) {
                if (this._disabled[key]) {
                    retVal.push(key);
                }
            }
            return retVal;
        }
        this._disabled = {};
        _.forEach(kind => this._disabled[kind] = true);
        return this;
    }

    protected splitTerm(term: string): [string, string] {
        const termParts = term.toLowerCase().split(":");
        return [
            termParts.length > 1 ? termParts[0].trim() : undefined,
            termParts.length > 1 ? termParts[1].trim() : termParts[0].trim()
        ];
    }

    find(term: string) {
        const [findScope, findTerm] = this.splitTerm(term);

        function compare(attr: any): boolean {
            if (typeof attr === "string") {
                return attr.toLowerCase().indexOf(findTerm) >= 0;
            }
            // tslint:disable-next-line: triple-equals
            return attr == findTerm;
        }

        function test(scopeItem: ScopeSubgraph | ScopeVertex | ScopeEdge): boolean {
            const attrs = scopeItem._.rawAttrs();
            attrs["ID"] = scopeItem._.Id;
            attrs["Parent ID"] = scopeItem.parent._.Id;
            attrs["Scope"] = scopeItem._.ScopeName;
            for (const key in attrs) {
                if (findScope === undefined || findScope === key.toLowerCase()) {
                    if (compare(attrs[key])) {
                        return true;
                    }
                }
            }
            return false;
        }

        return [...this.graphDB.subgraphs.filter(test), ...this.graphDB.vertices.filter(test), ...this.graphDB.edges.filter(test)].map(scopeItem => scopeItem._.Id);
    }

    abstract graphGui(graphDB: ScopeGraph): IGraphData;

    formatNum(str): string {
        if (isNaN(str)) {
            return str;
        }
        return formatNum(str);
    }

    formatNums(obj) {
        for (const key in obj) {
            obj[key] = this.formatNum(obj[key]);
        }
        return obj;
    }

    formatLine(labelTpl, obj): string {
        let retVal = "";
        let lpos = labelTpl.indexOf("%");
        let rpos = -1;
        let replacementFound = lpos >= 0 ? false : true;  //  If a line has no symbols always include it, otherwise only include that line IF a replacement was found  ---
        while (lpos >= 0) {
            retVal += labelTpl.substring(rpos + 1, lpos);
            rpos = labelTpl.indexOf("%", lpos + 1);
            if (rpos < 0) {
                console.log("Invalid Label Template");
                break;
            }
            const key = labelTpl.substring(lpos + 1, rpos);
            replacementFound = replacementFound || !!obj[labelTpl.substring(lpos + 1, rpos)];
            retVal += !key ? "%" : (obj[labelTpl.substring(lpos + 1, rpos)] || "");
            lpos = labelTpl.indexOf("%", rpos + 1);
        }
        retVal += labelTpl.substring(rpos + 1, labelTpl.length);
        return replacementFound ? retVal : "";
    }

    format(labelTpl, obj) {
        labelTpl = labelTpl.split("\\n").join("\n");
        return labelTpl
            .split("\n")
            .map(line => this.formatLine(line, obj))
            .filter(d => d.trim().length > 0)
            .map(decodeHtml)
            .join("\n")
            ;
    }

    isSpill(edge: ScopeEdge): boolean {
        const sourceKind = edge.source._.attr("Kind").RawValue;
        const targetKind = edge.target._.attr("Kind").RawValue;
        return sourceKind === "2" || targetKind === "71";
    }

    spansSubgraph(edge: ScopeEdge): boolean {
        return edge.source.parent._.Id !== edge.target.parent._.Id;
    }

    filterLegend(graphDB: ScopeGraph) {
        for (let i = graphDB.vertices.length - 1; i >= 0; --i) {
            const vertex = graphDB.vertices[i];
            const kind = vertex._.attr("Kind").RawValue;
            if (this._disabled[kind]) {
                vertex.remove(false, (source: BaseScope, target: BaseScope) => {
                    return new BaseScope({
                        ScopeName: vertex._.ScopeName + ":in",
                        Id: source.Id + "->" + target.Id,
                        ScopeType: "dummy-edge",
                        Properties: {
                            Property: [vertex._.attr("Label")]
                        }
                    });
                });
            }
        }
    }

    abstract filterPartial(graphDB: ScopeGraph);

    filterEmptySubgraphs(graphDB: ScopeGraph) {
        while (true) {
            const emptySubgraphs = graphDB.subgraphs.filter(subgraph => subgraph.subgraphs.length === 0 && subgraph.vertices.length === 0);
            if (emptySubgraphs.length === 0) break;
            emptySubgraphs.forEach(subgraph => subgraph.remove(true));
        }
    }

    removeObsoleteSubgraphs(graphDB: ScopeGraph) {
        for (const subgraph of [...graphDB.subgraphs]) {
            if (subgraph.vertices.length === 0) {
                subgraph.remove(false);
            }
        }
    }

    graphData(): IGraphData {
        const graphDB = this.graphDB.clone();
        this.filterLegend(graphDB);
        this.filterPartial(graphDB);
        this.filterEmptySubgraphs(graphDB);
        this.removeObsoleteSubgraphs(graphDB);
        return this.graphGui(graphDB);
    }

    calcLegend(): WUGraphLegendData[] {
        const retVal: WUGraphLegendData[] = [];
        for (const kind in this.kindMap) {
            retVal.push({
                kind: parseInt(kind),
                faChar: faCharFactory(+kind),
                label: this.kindMap[kind][0]._.attr("Label").RawValue.split("\n")[0],
                count: this.kindMap[kind].length
            });
        }
        return retVal;
    }

    formatStoreRow(item: ScopeEdge | ScopeSubgraph | ScopeVertex) {
        const retVal = item._.rawAttrs();
        retVal["Id"] = item._.Id;
        for (const key in retVal) {
            //  TODO Move into BaseScope  ---
            retVal[key] = decodeHtml(retVal[key]);
        }
        retVal.__formatted = this.formatNums(item._.formattedAttrs());
        return retVal;
    }

    edges(_: string[]): IEdge[] {
        const retVal: IEdge[] = [];
        for (const id of _) {
            if (this.edgesMap[id]) {
                retVal.push(this.edgesMap[id]);
            }
        }
        return retVal;
    }

    subgraphStoreData() {
        return this.graphDB.subgraphs.map(sg => {
            return this.formatStoreRow(sg);
        });
    }

    activityStoreData() {
        return this.graphDB.vertices.map(v => {
            return this.formatStoreRow(v);
        });
    }

    edgeStoreData() {
        return this.graphDB.edges.map(e => {
            return this.formatStoreRow(e);
        });
    }

    calcTooltipTable(scope: BaseScope, parentScope?: BaseScope, term = "") {
        const [findScope, findTerm] = this.splitTerm(term);

        function highlightText(key: string, _text: any) {
            if (!findTerm) return _text;
            const text = "" + _text;
            if (findScope && findScope !== key.toLowerCase()) return _text;
            const found = text.toLowerCase().indexOf(findTerm.toLowerCase());
            if (found >= 0) {
                return text.substring(0, found) + "<span style='background:#fff2a8'>" + text.substring(found, found + findTerm.length) + "</span>" + text.substring(found + findTerm.length);
            }
            return _text;
        }

        let label = "";
        const rows: string[] = [];
        label = scope.Id;
        rows.push(`<tr><td class="key">ID:</td><td class="value">${highlightText("ID", scope.Id)}</td></tr>`);
        if (parentScope) {
            rows.push(`<tr><td class="key">Parent ID:</td><td class="value">${highlightText("Parent ID", parentScope.Id)}</td></tr>`);
        }
        rows.push(`<tr><td class="key">Scope:</td><td class="value">${highlightText("Scope", scope.ScopeName)}</td></tr>`);
        const attrs = this.formatNums(scope.formattedAttrs());
        for (const key in attrs) {
            if (key === "Label") {
                label = attrs[key];
            } else {
                rows.push(`<tr><td class="key">${key}</td><td class="value">${highlightText(key, attrs[key])}</td></tr>`);
            }
        }

        const funcTooltips: string[] = [];
        scope.children().forEach(row => {
            funcTooltips.push(this.calcTooltipTable(row));
        });

        return `<h4 align="center">${highlightText("Label", label)}</h4>
            <table>
                ${rows.join("")}
            </table>${funcTooltips.length ? `<br>${funcTooltips.join("<br>")}` : ""}`;
    }

    calcTooltip(scope: BaseScope, parentScope?: BaseScope, term = "") {

        return `<div class="eclwatch_WUGraph_Tooltip" style="max-width:480px">
            ${this.calcTooltipTable(scope, parentScope, term)}
        </div>`;
    }

    abstract scopeItem(_: string): ScopeSubgraph | ScopeVertex | ScopeEdge | undefined;

    calcGraphTooltip(id: string, findText?: string) {
        const item = this.scopeItem(id);
        if (item) {
            const scope = item._;
            const parentScope = item.parent._;
            if (scope) {
                return this.calcTooltip(scope, parentScope, findText);
            }
        }
        return "";
    }
}

export class WUScopeController extends WUScopeControllerBase<Subgraph, VertexType, Edge, IGraphData> {

    constructor() {
        super();
    }

    collapsedOnce = false;
    graphGui(graphDB: ScopeGraph): IGraphData {
        const retVal: MyGraphData = {
            subgraphs: [],
            vertices: [],
            edges: [],
            hierarchy: []
        };

        graphDB.walk((item) => {
            if (item instanceof UtilSubgraph) {
                const subgraph = this.appendSubgraph(item, retVal.hierarchy, retVal.subgraphs);
                subgraph.showMinMax(item.vertices.length > 3 || subgraph.minState() !== "normal");
            } else if (item instanceof UtilVertex) {
                this.appendVertex(item, retVal.hierarchy, retVal.vertices);
            } else if (item instanceof UtilEdge) {
                this.appendEdge(item, retVal.edges);
            }
        });

        const sgColors: {
            [key: string]: {
                sg: Subgraph;
                total: number;
                started: number;
                finished: number;
            }
        } = {};
        retVal.hierarchy.forEach(h => {
            let sgColor = sgColors[h.parent.id()];
            if (!sgColor) {
                sgColor = sgColors[h.parent.id()] = {
                    sg: h.parent as Subgraph,
                    total: 0,
                    started: 0,
                    finished: 0
                };
            }
            if (h.child instanceof Vertex) {
                sgColor.total++;
                sgColor.started += (h.child as any).__started ? 1 : 0;
                sgColor.finished += (h.child as any).__finished ? 1 : 0;
            }
        });
        for (const key in sgColors) {
            const sgColor = sgColors[key];
            if (sgColor.total === sgColor.finished) {
                sgColor.sg.border_colorStroke(FINISHED_STROKE);
            } else if (sgColor.finished > 0) {
                sgColor.sg.border_colorStroke(ACTIVE_STROKE);
            } else {
                sgColor.sg.border_colorStroke(UNKNOWN_STROKE);
            }
        }

        if (!this.showSubgraphs()) {
            retVal.subgraphs = [];
        }

        if (!this.collapsedOnce && retVal.vertices.length >= 100) {
            this.collapsedOnce = true;
            retVal.subgraphs.forEach((sg: Subgraph) => {
                sg.minState("partial");
            });
        }

        return retVal;
    }

    createSubgraph(subgraph: ScopeSubgraph): Subgraph {
        let sg = this.subgraphsMap[subgraph._.Id];
        if (!sg) {
            sg = new Subgraph()
                .title(subgraph._.Id)
                .on("minClick", () => {
                    this.minClick(sg);
                })
                ;
            this.subgraphsMap[subgraph._.Id] = sg;
        }
        this.scopeSubgraphsMap[sg.id()] = subgraph;
        return sg;
    }

    createVertex(vertex: ScopeVertex): VertexType {
        const rawAttrs = vertex._.rawAttrs();
        const formattedAttrs = this.formatNums(vertex._.formattedAttrs());
        formattedAttrs["ID"] = vertex._.Id;
        formattedAttrs["Parent ID"] = vertex.parent && vertex.parent._.Id;
        formattedAttrs["Scope"] = vertex._.ScopeName;
        let v = this.verticesMap[vertex._.Id];
        if (!v) {
            if (vertex._.ScopeType === "dummy") {
                const parent = this.subgraphsMap[vertex.parent._.Id];
                v = new Icon()
                    .shape_colorFill("darkred")
                    .shape_colorStroke("darkred")
                    .image_colorFill("white")
                    .faChar("\uf067")
                    .on("click", () => {
                        parent.minState("normal");
                        this.minClick(parent);
                    })
                    ;
            } else {
                v = new Vertex()
                    .icon_shape_colorStroke(UNKNOWN_STROKE)
                    .icon_shape_colorFill(UNKNOWN_STROKE)
                    .icon_image_colorFill(Palette.textColor(UNKNOWN_STROKE))
                    .faChar(faCharFactory(rawAttrs["Kind"]))
                    .textbox_shape_colorStroke(UNKNOWN_STROKE)
                    .textbox_shape_colorFill(UNKNOWN_FILL)
                    .textbox_text_colorFill(Palette.textColor(UNKNOWN_FILL))
                    ;
                const annotations = [];
                if (vertex._.hasAttr("Definition")) {
                    annotations.push({
                        faChar: "\uf036",
                        tooltip: "Definition",
                        shape_colorFill: UNKNOWN_FILL,
                        shape_colorStroke: UNKNOWN_FILL,
                        image_colorFill: Palette.textColor(UNKNOWN_FILL)
                    });
                }
                if (vertex._.hasAttr("IsInternal")) {
                    annotations.push({
                        faChar: "\uf085",
                        tooltip: "IsInternal",
                        shape_colorFill: "red",
                        shape_colorStroke: "red",
                        image_colorFill: Palette.textColor("red")
                    });
                }
                v.annotationIcons(annotations);
            }
            this.verticesMap[vertex._.Id] = v;
        }
        this.scopeVerticesMap[v.id()] = vertex;
        if (v instanceof Vertex) {
            const label = this.format(this.vertexLabelTpl(), formattedAttrs);
            v
                .icon_diameter(this.showIcon() ? 24 : 0)
                .text(label)
                ;
        }
        return v;
    }

    createEdge(edge: ScopeEdge): Edge | undefined {
        const sourceV = this.verticesMap[edge.source._.Id];
        const targetV = this.verticesMap[edge.target._.Id];
        const rawAttrs = edge._.rawAttrs();
        const formattedAttrs = this.formatNums(edge._.formattedAttrs());
        formattedAttrs["ID"] = edge._.Id;
        formattedAttrs["Parent ID"] = edge.parent && edge.parent._.Id;
        formattedAttrs["Scope"] = edge._.ScopeName;
        let e = this.edgesMap[edge._.Id];
        if (!e) {
            if (sourceV && targetV) {
                const isSpill = this.isSpill(edge);
                const spansSubgraph = this.spansSubgraph(edge);

                let strokeDasharray = null;
                let weight = 100;
                if (rawAttrs["IsDependency"]) {
                    weight = 10;
                    strokeDasharray = "1,2";
                } else if (rawAttrs["_childGraph"]) {
                    strokeDasharray = "5,5";
                } else if (isSpill) {
                    weight = 25;
                    strokeDasharray = "5,5,10,5";
                } else if (spansSubgraph) {
                    weight = 5;
                    strokeDasharray = "5,5";
                }
                e = new Edge()
                    .sourceVertex(sourceV)
                    .targetVertex(targetV)
                    .sourceMarker("circle")
                    .targetMarker("arrow")
                    .weight(weight)
                    .strokeDasharray(strokeDasharray)
                    ;
                this.edgesMap[edge._.Id] = e;
            }
        }
        if (e instanceof Edge) {
            this.scopeEdgesMap[e.id()] = edge;
            const label = this.format(this.edgeLabelTpl(), formattedAttrs);
            e.text(label);
            e.sourceVertex(sourceV);
            e.targetVertex(targetV);
        }
        return e;
    }

    appendSubgraph(subgraph: ScopeSubgraph, hierarchy: Lineage[], subgraphs: Subgraph[]): Subgraph {
        const sg = this.createSubgraph(subgraph);
        subgraphs.push(sg);
        const parent = this.subgraphsMap[subgraph.parent._.Id];
        if (parent) {
            hierarchy.push({ parent, child: sg });
        }
        return sg;
    }

    appendVertex(vertex: ScopeVertex, hierarchy: Lineage[], vertices: VertexType[]): VertexType {
        const v = this.createVertex(vertex);
        vertices.push(v);
        const parent = this.subgraphsMap[vertex.parent._.Id];
        if (parent) {
            hierarchy.push({ parent, child: v });
        }
        return v;
    }

    appendEdge(edge: ScopeEdge, edges: Edge[]): Edge {
        const e = this.createEdge(edge);
        if (e) {
            const attrs = edge._.rawAttrs();
            const numSlaves = edge.parent._.hasAttr("NumSlaves") ? parseInt(edge.parent._.attr("NumSlaves").RawValue) : Number.MAX_SAFE_INTEGER;
            const numStarts = parseInt(attrs["NumStarts"]);
            const numStops = parseInt(attrs["NumStops"]);
            if (!isNaN(numSlaves) && !isNaN(numStarts) && !isNaN(numStops)) {
                const started = numStarts > 0;
                const finished = numStops === numSlaves;
                const active = started && !finished;
                const strokeColor = active ? ACTIVE_STROKE : finished ? FINISHED_STROKE : UNKNOWN_STROKE;
                const lightColor = active ? ACTIVE_FILL : finished ? FINISHED_FILL : UNKNOWN_FILL;
                e.strokeColor(strokeColor);

                const vInOut = [e.sourceVertex(), e.targetVertex()];
                vInOut.forEach(v => {
                    if (v instanceof Vertex) {
                        (v as any)["__started"] = started;
                        (v as any)["__finished"] = finished;
                        (v as any)["__active"] = active;
                        v
                            .icon_shape_colorStroke(strokeColor)
                            .icon_shape_colorFill(strokeColor)
                            .icon_image_colorFill(Palette.textColor(strokeColor))
                            .textbox_shape_colorStroke(strokeColor)
                            .textbox_shape_colorFill(lightColor)
                            .textbox_text_colorFill(Palette.textColor(lightColor))
                            ;
                    }
                });
            }
            edges.push(e);
        }
        return e;
    }

    filterPartial(graphDB: ScopeGraph) {
        for (const subgraph of graphDB.subgraphs) {
            const sg = this.subgraphsMap[subgraph._.Id];
            switch (sg.minState()) {
                case "partial":
                    const childVertices: ReadonlyArray<ScopeVertex> = subgraph.vertices;
                    const vShow: ScopeVertex[] = [];
                    const vHide: ScopeVertex[] = [];

                    for (const vertex of childVertices) {
                        if (vertex.inEdges.length === 0 || vertex.inEdges.some(edge => edge.source.parent !== edge.target.parent) ||
                            vertex.outEdges.length === 0 || vertex.outEdges.some(edge => edge.source.parent !== edge.target.parent)) {
                            vShow.push(vertex);
                        } else {
                            vHide.push(vertex);
                        }
                    }

                    if (vHide.length > 1) {
                        const dummyDetails = {
                            ScopeName: subgraph._.ScopeName,
                            Id: subgraph._.Id + ":dummy",
                            ScopeType: "dummy",
                            Properties: {
                                Property: [{
                                    Name: "Activities",
                                    RawValue: "" + vHide.length,
                                    Formatted: "" + vHide.length,
                                    Measure: "count",
                                    Creator: "",
                                    CreatorType: ""
                                }]
                            }
                        };
                        const dummyScope = new BaseScope(dummyDetails);
                        const dummyVertex = subgraph.createVertex(dummyScope);

                        for (const vertex of vHide) {
                            for (const edge of vertex.inEdges) {
                                if (vShow.indexOf(edge.source) >= 0) {
                                    const dummyEdgeScope = new BaseScope({
                                        ScopeName: edge.source._.ScopeName,
                                        Id: edge.source._.Id + "->" + dummyVertex._.Id,
                                        ScopeType: "dummy-in",
                                        Properties: {
                                            Property: []
                                        }
                                    });
                                    console.log(dummyEdgeScope.Id);
                                    subgraph.createEdge(edge.source, dummyVertex, dummyEdgeScope);
                                }
                            }
                            for (const edge of vertex.outEdges) {
                                if (vShow.indexOf(edge.target) >= 0) {
                                    const dummyEdgeScope = new BaseScope({
                                        ScopeName: edge.target._.ScopeName,
                                        Id: dummyVertex._.Id + "->" + edge.target._.Id,
                                        ScopeType: "dummy-out",
                                        Properties: {
                                            Property: []
                                        }
                                    });
                                    console.log(dummyEdgeScope.Id);
                                    subgraph.createEdge(dummyVertex, edge.target, dummyEdgeScope);
                                }
                            }
                        }
                        vHide.forEach(vertex => vertex.remove(true));
                    }
                    break;
            }
        }
    }

    private subgraphs(_: string[]): Subgraph[] {
        const retVal: Subgraph[] = [];
        for (const id of _) {
            if (this.subgraphsMap[id]) {
                retVal.push(this.subgraphsMap[id]);
            }
        }
        return retVal;
    }

    vertices(_: number | string[]): VertexType[] {
        const retVal: VertexType[] = [];
        if (typeof _ === "number") {
            for (const v of this.kindMap[_]) {
                retVal.push(this.verticesMap[v._.Id]);
            }
        } else {
            for (const id of _) {
                if (this.verticesMap[id]) {
                    retVal.push(this.verticesMap[id]);
                }
            }
        }
        return retVal;
    }

    scopeItem(_: string): ScopeSubgraph | ScopeVertex | ScopeEdge | undefined {
        const widget = this.item(_);
        return widget ? this.rItem(widget) : undefined;
    }

    item(_: string): Subgraph | VertexType | Edge {
        return this.subgraphsMap[_] || this.verticesMap[_] || this.edgesMap[_];
    }

    rItem(_: Subgraph | VertexType | Edge): ScopeSubgraph | ScopeVertex | ScopeEdge {
        return this.scopeSubgraphsMap[_.id()] || this.scopeVerticesMap[_.id()] || this.scopeEdgesMap[_.id()];
    }

    items(_: string[]): (Subgraph | VertexType | Edge)[] {
        return [...this.subgraphs(_), ...this.vertices(_), ...this.edges(_)];
    }

    calcGraphTooltip2(item: VertexType | Edge) {
        let scope;
        let parentScope;
        if (item instanceof Subgraph) {
            const subgraph = this.scopeSubgraphsMap[item.id()];
            scope = subgraph._;
            parentScope = subgraph.parent._;
        } else if (item instanceof Vertex || item instanceof Icon) {
            const vertex = this.scopeVerticesMap[item.id()];
            scope = vertex._;
            parentScope = vertex.parent._;
        } else if (item instanceof Edge) {
            const edge = this.scopeEdgesMap[item.id()];
            scope = edge._;
            parentScope = edge.parent._;
        }
        if (scope) {
            return this.calcTooltip(scope, parentScope);
        }
        return "";
    }

    //  Events  ---
    minClick(sg: Subgraph) {
    }
}

export class WUScopeController8 extends WUScopeControllerBase<ISubgraph, IVertex, IEdge, IGraphData2> {

    constructor() {
        super();
    }

    private collapsedOnce = false;
    graphGui(graphDB: ScopeGraph): IGraphData2 {
        const retVal: IGraphData2 = {
            subgraphs: [],
            vertices: [],
            edges: [],
            hierarchy: []
        };

        graphDB.walk((item) => {
            if (item instanceof UtilSubgraph) {
                this.appendSubgraph(item, retVal.hierarchy, retVal.subgraphs);
            } else if (item instanceof UtilVertex) {
                this.appendVertex(item, retVal.hierarchy, retVal.vertices);
            } else if (item instanceof UtilEdge) {
                this.appendEdge(item, retVal.edges);
            }
        });

        const sgColors: {
            [key: string]: {
                sg: ISubgraph;
                total: number;
                started: number;
                finished: number;
            }
        } = {};
        retVal.hierarchy.forEach(h => {
            let sgColor = sgColors[h.parent.id];
            if (!sgColor) {
                sgColor = sgColors[h.parent.id] = {
                    sg: h.parent,
                    total: 0,
                    started: 0,
                    finished: 0
                };
            }
            if (h.child && this.verticesMap[h.child.id]) {
                sgColor.total++;
                sgColor.started += (h.child as any).__started ? 1 : 0;
                sgColor.finished += (h.child as any).__finished ? 1 : 0;
            }
        });
        for (const key in sgColors) {
            const sgColor = sgColors[key];
            if (sgColor.total === sgColor.finished) {
                sgColor.sg.stroke = FINISHED_STROKE;
            } else if (sgColor.finished > 0) {
                sgColor.sg.stroke = ACTIVE_STROKE;
            } else {
                sgColor.sg.stroke = UNKNOWN_STROKE;
            }
        }

        if (!this.showSubgraphs()) {
            retVal.subgraphs = [];
        }

        if (!this.collapsedOnce && retVal.vertices.length >= 100) {
            this.collapsedOnce = true;
            retVal.subgraphs.forEach(sg => {
            });
        }

        return retVal;
    }

    private createSubgraph(subgraph: ScopeSubgraph): ISubgraph {
        let sg = this.subgraphsMap[subgraph._.Id];
        if (!sg) {
            sg = {
                id: subgraph._.Id,
                text: subgraph._.Id
            };
            this.subgraphsMap[subgraph._.Id] = sg;
        }
        this.scopeSubgraphsMap[sg.id] = subgraph;
        return sg;
    }

    private createVertex(vertex: ScopeVertex): IVertex {
        const rawAttrs = vertex._.rawAttrs();
        const formattedAttrs = this.formatNums(vertex._.formattedAttrs());
        formattedAttrs["ID"] = vertex._.Id;
        formattedAttrs["Parent ID"] = vertex.parent && vertex.parent._.Id;
        formattedAttrs["Scope"] = vertex._.ScopeName;
        let v = this.verticesMap[vertex._.Id];
        if (!v) {
            v = {
                icon: {
                    imageChar: faCharFactory(rawAttrs["Kind"]),
                    fill: "white",
                    stroke: "lightgray",
                    imageCharFill: "black",
                    yOffset: -3,
                    padding: 12
                },
                id: vertex._.Id,
                text: "",
                origData: rawAttrs,
                textboxFill: UNKNOWN_FILL,
                textboxStroke: UNKNOWN_STROKE,
                textFill: Palette.textColor(UNKNOWN_FILL)
            };
            const annotations = [];
            if (vertex._.hasAttr("Definition")) {
                annotations.push({
                    faChar: "\uf036",
                    tooltip: "Definition",
                    shape_colorFill: UNKNOWN_FILL,
                    shape_colorStroke: UNKNOWN_FILL,
                    image_colorFill: Palette.textColor(UNKNOWN_FILL)
                });
            }
            if (vertex._.hasAttr("IsInternal")) {
                annotations.push({
                    faChar: "\uf085",
                    tooltip: "IsInternal",
                    shape_colorFill: "red",
                    shape_colorStroke: "red",
                    image_colorFill: Palette.textColor("red")
                });
            }
            // v.annotationIcons(annotations);
            this.verticesMap[vertex._.Id] = v;
        }
        this.scopeVerticesMap[v.id] = vertex;
        const label = this.format(this.vertexLabelTpl(), formattedAttrs);
        v.text = label;
        return v;
    }

    private createEdge(edge: ScopeEdge): IEdge | undefined {
        const sourceV = this.verticesMap[edge.source._.Id];
        const targetV = this.verticesMap[edge.target._.Id];
        const rawAttrs = edge._.rawAttrs();
        const formattedAttrs = this.formatNums(edge._.formattedAttrs());
        formattedAttrs["ID"] = edge._.Id;
        formattedAttrs["Parent ID"] = edge.parent && edge.parent._.Id;
        formattedAttrs["Scope"] = edge._.ScopeName;
        let e = this.edgesMap[edge._.Id];
        if (!e) {
            if (sourceV && targetV) {
                const isSpill = this.isSpill(edge);
                const spansSubgraph = this.spansSubgraph(edge);

                let strokeDasharray;
                let weight = 100;
                if (rawAttrs["IsDependency"]) {
                    weight = 10;
                    strokeDasharray = "1,2";
                } else if (rawAttrs["_childGraph"]) {
                    strokeDasharray = "5,5";
                } else if (isSpill) {
                    weight = 25;
                    strokeDasharray = "5,5,10,5";
                } else if (spansSubgraph) {
                    weight = 5;
                    strokeDasharray = "5,5";
                }
                e = {
                    id: edge._.Id,
                    source: sourceV,
                    target: targetV,
                    strokeDasharray,
                    weight
                };
                this.edgesMap[edge._.Id] = e;
            }
        }
        if (e) {
            this.scopeEdgesMap[e.id] = edge;
            const label = this.format(this.edgeLabelTpl(), formattedAttrs);
            e.label = label;
            e.source = sourceV;
            e.target = targetV;
        }
        return e;
    }

    private appendSubgraph(subgraph: ScopeSubgraph, hierarchy: IHierarchy[], subgraphs: ISubgraph[]): ISubgraph {
        const sg = this.createSubgraph(subgraph);
        subgraphs.push(sg);
        const parent = this.subgraphsMap[subgraph.parent._.Id];
        if (parent) {
            hierarchy.push({
                id: parent.id + "=>" + sg.id,
                parent,
                child: sg
            });
        }
        return sg;
    }

    private appendVertex(vertex: ScopeVertex, hierarchy: IHierarchy[], vertices: IVertex[]): IVertex {
        const v = this.createVertex(vertex);
        vertices.push(v);
        const parent = this.subgraphsMap[vertex.parent._.Id];
        if (parent) {
            hierarchy.push({
                id: parent.id + "=>" + v.id,
                parent,
                child: v
            });
        }
        return v;
    }

    private appendEdge(edge: ScopeEdge, edges: IEdge[]): IEdge {
        const e = this.createEdge(edge);
        if (e) {
            const attrs = edge._.rawAttrs();
            const numSlaves = edge.parent._.hasAttr("NumSlaves") ? parseInt(edge.parent._.attr("NumSlaves").RawValue) : Number.MAX_SAFE_INTEGER;
            const numStarts = parseInt(attrs["NumStarts"]);
            const numStops = parseInt(attrs["NumStops"]);
            if (!isNaN(numSlaves) && !isNaN(numStarts) && !isNaN(numStops)) {
                const started = numStarts > 0;
                const finished = numStops === numSlaves;
                const active = started && !finished;
                const strokeColor = active ? ACTIVE_STROKE : finished ? FINISHED_STROKE : UNKNOWN_STROKE;
                const lightColor = active ? ACTIVE_FILL : finished ? FINISHED_FILL : UNKNOWN_FILL;
                e.stroke = strokeColor;

                const vInOut = [e.source, e.target];
                vInOut.forEach(v => {
                    if (v) {
                        (v as any)["__started"] = started;
                        (v as any)["__finished"] = finished;
                        (v as any)["__active"] = active;
                        v.icon.stroke = strokeColor;
                        v.icon.fill = strokeColor;
                        v.icon.imageCharFill = Palette.textColor(strokeColor);
                        v.icon.stroke = strokeColor;
                        v.textboxFill = lightColor;
                        v.textboxStroke = strokeColor;
                        v.textFill = Palette.textColor(lightColor);
                    }
                });
            }
            edges.push(e);
        }
        return e;
    }

    filterPartial(graphDB: ScopeGraph) {
    }

    private subgraphs(_: string[]): ISubgraph[] {
        const retVal: ISubgraph[] = [];
        for (const id of _) {
            if (this.subgraphsMap[id]) {
                retVal.push(this.subgraphsMap[id]);
            }
        }
        return retVal;
    }

    vertices(_: number | string[]): IVertex[] {
        const retVal: IVertex[] = [];
        if (typeof _ === "number") {
            for (const v of this.kindMap[_]) {
                retVal.push(this.verticesMap[v._.Id]);
            }
        } else {
            for (const id of _) {
                if (this.verticesMap[id]) {
                    retVal.push(this.verticesMap[id]);
                }
            }
        }
        return retVal;
    }

    scopeItem(_: string): ScopeSubgraph | ScopeVertex | ScopeEdge | undefined {
        const widget = this.item(_);
        return widget ? this.rItem(widget.id) : undefined;
    }

    item(_: string): ISubgraph | IVertex | IEdge {
        return this.subgraphsMap[_] || this.verticesMap[_] || this.edgesMap[_];
    }

    rItem(id: string): ScopeSubgraph | ScopeVertex | ScopeEdge {
        return this.scopeSubgraphsMap[id] || this.scopeVerticesMap[id] || this.scopeEdgesMap[id];
    }

    items(_: string[]): (ISubgraph | IVertex | IEdge)[] {
        return [...this.subgraphs(_), ...this.vertices(_), ...this.edges(_)];
    }

    //  Events  ---
    minClick(sg: ISubgraph) {
    }
}
