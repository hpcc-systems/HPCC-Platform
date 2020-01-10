import { format as d3Format, Icon, Palette } from "@hpcc-js/common";
import { BaseScope, ScopeEdge, ScopeGraph, ScopeSubgraph, ScopeVertex } from "@hpcc-js/comms";
import { Edge, IGraphData, Lineage, Subgraph, Vertex } from "@hpcc-js/graph";
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

function faCharFactory(kind): string {
    switch (kind) {
        case "2": return "\uf0c7";      //  Disk Write
        case "3": return "\uf15d";      //  sort
        case "5": return "\uf0b0";      //  Filter
        case "6": return "\uf1e0";      //  Split
        case "12": return "\uf039";     //  First N
        case "15": return "\uf126";     //  Lightweight Join
        case "17": return "\uf126";     //  Lookup Join
        case "22": return "\uf1e6";     //  Pipe Output
        case "23": return "\uf078";     //  Funnel
        case "25": return "\uf0ce";     //  Inline Dataset
        case "26": return "\uf074";     //  distribute
        case "29": return "\uf005";     //  Store Internal Result
        case "36": return "\uf128";     //  If
        case "44": return "\uf0c7";     //  write csv
        case "47": return "\uf0c7";     //  write
        case "54": return "\uf013";     //  Workunit Read
        case "56": return "\uf0c7";     //  Spill
        case "59": return "\uf126";     //  Merge
        case "61": return "\uf0c7";     //  write xml
        case "82": return "\uf1c0";     //  Projected Disk Read Spill
        case "88": return "\uf1c0";     //  Projected Disk Read Spill
        case "92": return "\uf129";     //  Limted Index Read
        case "93": return "\uf129";     //  Limted Index Read
        case "99": return "\uf1c0";     //  CSV Read
        case "105": return "\uf1c0";    //  CSV Read

        case "7": return "\uf090";      //  Project
        case "9": return "\uf0e2";      //  Local Iterate
        case "16": return "\uf005";     //  Output Internal
        case "19": return "\uf074";     //  Hash Distribute
        case "21": return "\uf275";     //  Normalize
        case "35": return "\uf0c7";     //  CSV Write
        case "37": return "\uf0c7";     //  Index Write
        case "71": return "\uf1c0";     //  Disk Read Spill
        case "133": return "\uf0ce";    //  Inline Dataset
        case "148": return "\uf0ce";    //  Inline Dataset
        case "168": return "\uf275";    //  Local Denormalize
    }
    return "\uf063";
}

export class WUScopeController {
    private graphDB: ScopeGraph;
    private subgraphsMap: { [id: string]: Subgraph } = {};
    private rSubgraphsMap: { [id: string]: ScopeSubgraph } = {};
    private verticesMap: { [id: string]: VertexType } = {};
    private rVerticesMap: { [id: string]: ScopeVertex } = {};
    private edgesMap: { [id: string]: Edge } = {};
    private rEdgesMap: { [id: string]: ScopeEdge } = {};
    private kindMap: { [id: string]: ScopeVertex[] } = {};

    protected _disabled: { [kind: number]: boolean } = {};

    constructor() {
    }

    clear() {
        this.subgraphsMap = {};
        this.rSubgraphsMap = {};
        this.verticesMap = {};
        this.rVerticesMap = {};
        this.edgesMap = {};
        this.rEdgesMap = {};
    }

    set(masterGraph: ScopeGraph) {
        this.graphDB = masterGraph;
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

    _showSubgraphs = true;
    showSubgraphs(): boolean;
    showSubgraphs(_: boolean): this;
    showSubgraphs(_?: boolean): boolean | this {
        if (!arguments.length) return this._showSubgraphs;
        this._showSubgraphs = _;
        return this;
    }

    _showIcon = true;
    showIcon(): boolean;
    showIcon(_: boolean): this;
    showIcon(_?: boolean): boolean | this {
        if (!arguments.length) return this._showIcon;
        this._showIcon = _;
        return this;
    }

    _vertexLabelTpl = "%Label%";
    vertexLabelTpl(): string;
    vertexLabelTpl(_: string): this;
    vertexLabelTpl(_?: string): string | this {
        if (!arguments.length) return this._vertexLabelTpl;
        this._vertexLabelTpl = _;
        return this;
    }

    _edgeLabelTpl = "%Label%\n%NumRowsProcessed%\n%SkewMinRowsProcessed% / %SkewMaxRowsProcessed%";
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

    splitTerm(term: string): [string, string] {
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
            this.rSubgraphsMap[sg.id()] = subgraph;
        }
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
            this.rVerticesMap[v.id()] = vertex;
        }
        if (v instanceof Vertex) {
            const label = this.format(this.vertexLabelTpl(), formattedAttrs);
            v
                .icon_diameter(this.showIcon() ? 24 : 0)
                .text(label)
                ;
        }
        return v;
    }

    isSpill(edge: ScopeEdge): boolean {
        const sourceKind = edge.source._.attr("Kind").RawValue;
        const targetKind = edge.target._.attr("Kind").RawValue;
        return sourceKind === "2" || targetKind === "71";
    }

    spansSubgraph(edge: ScopeEdge): boolean {
        return edge.source.parent._.Id !== edge.target.parent._.Id;
    }

    createEdge(edge: ScopeEdge): Edge | undefined {
        const rawAttrs = edge._.rawAttrs();
        const formattedAttrs = this.formatNums(edge._.formattedAttrs());
        formattedAttrs["ID"] = edge._.Id;
        formattedAttrs["Parent ID"] = edge.parent && edge.parent._.Id;
        formattedAttrs["Scope"] = edge._.ScopeName;
        let e = this.edgesMap[edge._.Id];
        if (!e) {
            const sourceV = this.verticesMap[edge.source._.Id];
            const targetV = this.verticesMap[edge.target._.Id];
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
                this.rEdgesMap[e.id()] = edge;
            }
        }
        if (e instanceof Edge) {
            const label = this.format(this.edgeLabelTpl(), formattedAttrs);
            e.text(label);
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
            const numSlaves = parseInt(attrs["NumSlaves"]);
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
                faChar: faCharFactory(kind),
                label: this.kindMap[kind][0]._.attr("Label").RawValue.split("\n")[0],
                count: this.kindMap[kind].length
            });
        }
        return retVal;
    }

    subgraphs(_: string[]): Subgraph[] {
        const retVal: Subgraph[] = [];
        for (const id of _) {
            if (this.subgraphsMap[id]) {
                retVal.push(this.subgraphsMap[id]);
            }
        }
        return retVal;
    }

    rSubgraphs(_: Subgraph[]): ScopeSubgraph[] {
        const retVal: ScopeSubgraph[] = [];
        for (const sg of _) {
            if (this.rSubgraphsMap[sg.id()]) {
                retVal.push(this.rSubgraphsMap[sg.id()]);
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

    rVertices(_: VertexType[]): ScopeVertex[] {
        const retVal: ScopeVertex[] = [];
        for (const v of _) {
            if (this.rVerticesMap[v.id()]) {
                retVal.push(this.rVerticesMap[v.id()]);
            }
        }
        return retVal;
    }

    edges(_: string[]): Edge[] {
        const retVal: Edge[] = [];
        for (const id of _) {
            if (this.edgesMap[id]) {
                retVal.push(this.edgesMap[id]);
            }
        }
        return retVal;
    }

    rEdges(_: Edge[]): ScopeEdge[] {
        const retVal: ScopeEdge[] = [];
        for (const e of _) {
            if (this.rEdgesMap[e.id()]) {
                retVal.push(this.rEdgesMap[e.id()]);
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
        return this.rSubgraphsMap[_.id()] || this.rVerticesMap[_.id()] || this.rEdgesMap[_.id()];
    }

    items(_: string[]): Array<Subgraph | VertexType | Edge> {
        return [...this.subgraphs(_), ...this.vertices(_), ...this.edges(_)];
    }

    rItems(_: Array<Subgraph | VertexType | Edge>): Array<ScopeSubgraph | ScopeVertex | ScopeEdge> {
        return [...this.rSubgraphs(_ as Subgraph[]), ...this.rVertices(_ as VertexType[]), ...this.rEdges(_ as Edge[])];
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

    formatRow(item: ScopeEdge | ScopeSubgraph | ScopeVertex, columns, row) {
        const attrs = this.formatNums(item._.formattedAttrs());
        for (const key in attrs) {
            const idx = columns.indexOf(key);
            if (idx === -1) {
                columns.push(key);
                row.push(attrs[key]);
            } else {
                row[idx] = attrs[key];
            }
        }
        for (let i = 0; i < 100; ++i) {
            if (row[i] === undefined) {
                row[i] = "";
            }
        }
        return row;
    }

    subgraphStoreData() {
        return this.graphDB.subgraphs.map(sg => {
            return this.formatStoreRow(sg);
        });
    }

    subgraphData(): { columns: string[], data: any[][] } {
        const columns = ["Id", "Label"];
        const data = this.graphDB.subgraphs.map(sg => {
            const row = [sg._.Id];
            return this.formatRow(sg, columns, row);
        });
        return { columns, data };
    }

    activityStoreData() {
        return this.graphDB.vertices.map(v => {
            return this.formatStoreRow(v);
        });
    }

    activityData(): { columns: string[], data: any[][] } {
        const columns = ["Id", "Kind", "Label"];
        const data = this.graphDB.vertices.map(v => {
            const row = [parseInt(v._.Id.split("a")[1])];
            return this.formatRow(v, columns, row);
        });
        return { columns, data };
    }

    edgeStoreData() {
        return this.graphDB.edges.map(e => {
            return this.formatStoreRow(e);
        });
    }

    edgeData(): { columns: string[], data: any[][] } {
        const columns = ["Id", "Label"];
        const data = this.graphDB.edges.map(e => {
            const row = [e._.Id];
            return this.formatRow(e, columns, row);
        });
        return { columns, data };
    }

    treeData() {
        // TODO  ---
        return this.subgraphData();
    }

    calcTooltip(scope: BaseScope, parentScope?: BaseScope, term: string = "") {
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

        return `<div class="eclwatch_WUGraph_Tooltip" style="max-width:480px">
            <h4 align="center">${highlightText("Label", label)}</h4>
            <table>
                ${rows.join("")}
            </table>
        </div>`;
    }

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

    calcGraphTooltip2(item: VertexType | Edge) {
        let scope;
        let parentScope;
        if (item instanceof Subgraph) {
            const subgraph = this.rSubgraphsMap[item.id()];
            scope = subgraph._;
            parentScope = subgraph.parent._;
        } else if (item instanceof Vertex || item instanceof Icon) {
            const vertex = this.rVerticesMap[item.id()];
            scope = vertex._;
            parentScope = vertex.parent._;
        } else if (item instanceof Edge) {
            const edge = this.rEdgesMap[item.id()];
            scope = edge._;
            parentScope = edge.parent._;
        }
        if (scope) {
            return this.calcTooltip(scope, parentScope);
        }
        return "";
    }

    subgraph(id: string): Subgraph | undefined {
        return this.subgraphsMap[id];
    }

    vertex(id: string): VertexType | undefined {
        return this.verticesMap[id];
    }

    edge(id: string): Edge {
        return this.edgesMap[id];
    }

    //  Events  ---
    minClick(sg: Subgraph) {
    }
}
