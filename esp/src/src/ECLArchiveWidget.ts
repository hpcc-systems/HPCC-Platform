import { ECLEditor } from "@hpcc-js/codemirror";
import { extent, Palette } from "@hpcc-js/common";
import { Workunit } from "@hpcc-js/comms";
import { HTMLTooltip } from "@hpcc-js/html";
import { SplitPanel } from "@hpcc-js/phosphor";
import { DirectoryTree } from "@hpcc-js/tree";
import { xml2json } from "@hpcc-js/util";
import "dijit/form/Button";
import "dijit/layout/BorderContainer";
import "dijit/layout/ContentPane";
import * as registry from "dijit/registry";
import "dijit/Toolbar";
import "dijit/ToolbarSeparator";
import nlsHPCC from "./nlsHPCC";
// @ts-ignore
import * as template from "dojo/text!../templates/ECLArchiveWidget.html";
// @ts-ignore
import * as _Widget from "hpcc/_Widget";
import { declareDecorator } from "./DeclareDecorator";

class DirectoryTreeEx extends DirectoryTree {
    calcWidth() {
        return this.calcRequiredWidth();
    }
}

type _Widget = {
    id: string;
    widget: any;
    params: { [key: string]: any };
    inherited(args: any);
    setDisabled(id: string, disabled: boolean, icon?: string, disabledIcon?: string);
};

export interface ECLArchiveWidget extends _Widget {
}

@declareDecorator("ECLArchiveWidget", _Widget)
export class ECLArchiveWidget {
    protected templateString = template;
    static baseClass = "ECLArchiveWidget";
    protected i18n = nlsHPCC;

    private borderContainer = null;
    private editor: ECLEditor = null;
    private archiveViewer: SplitPanel;
    private directoryTree: DirectoryTreeEx;
    private tooltip: HTMLTooltip;

    buildRendering(args) {
        this.inherited(arguments);
    }

    postCreate(args) {
        this.inherited(arguments);
        this.borderContainer = registry.byId(this.id + "BorderContainer");
    }

    startup(args) {
        this.inherited(arguments);
    }

    resize(args) {
        this.inherited(arguments);
        this.borderContainer.resize();
        if (this.archiveViewer) {
            const rw = this.directoryTree.calcWidth() + 20;
            const pw = this.borderContainer._contentBox.w;
            const ratio = rw / pw;
            this.archiveViewer
                .relativeSizes([ratio, 1 - ratio])
                .resize()
                .render()
                ;
        }
    }

    //  Plugin wrapper  ---
    init(params) {
        if (this.inherited(arguments))
            return;
        const context = this;
        this.directoryTree = new DirectoryTreeEx()
            .textFileIcon("fa fa-file-code-o")
            .omitRoot(true)
            ;
        this.editor = new ECLEditor().readOnly(true);
        this.archiveViewer = new SplitPanel("horizontal");
        this.tooltip = new HTMLTooltip()
            .target(document.body)
            .direction("e")
            .visible(false)
            .render()
            ;
        
        const tableDataTransformer = d => {
            const ret = d.map((n: any) => {
                return [
                    n.Name,
                    n.Formatted
                ];
            });
            ret.sort((a,b)=>a[0].localeCompare(b[0]));
            return ret;
        };

        const wu = Workunit.attach({ baseUrl: "" }, params.Wuid);
        wu.refresh(true).then(function (wuInfo) {
            context.editor.text(wuInfo.Query.Text);
            if (!wuInfo.HasArchiveQuery) {
                context.archiveViewer
                    .target(context.id + "EclContent")
                    .addWidget(context.editor)
                    .render()
                    ;
            } else {
                context.directoryTree
                    .data({
                        label: "root",
                        children: [
                            {
                                label: ""
                            }
                        ]
                    })
                    .textFileIcon("fa fa-spinner fa-spin")
                    .iconSize(20)
                    .rowItemPadding(2)
                    ;
                context.archiveViewer
                    .target(context.id + "EclContent")
                    .addWidget(context.directoryTree)
                    .addWidget(context.editor)
                    .relativeSizes([0.1, 0.9])
                    .render()
                    ;
                const scopesOptions = {
                    ScopeFilter: {
                        MaxDepth: 999999,
                        ScopeTypes: ["graph"]
                    },
                    ScopeOptions: {
                        IncludeMatchedScopesInResults: true,
                        IncludeScope: true,
                        IncludeId: true,
                        IncludeScopeType: true
                    },
                    PropertyOptions: {
                        IncludeName: true,
                        IncludeRawValue: true,
                        IncludeFormatted: true,
                        IncludeMeasure: true,
                        IncludeCreator: true,
                        IncludeCreatorType: true
                    },
                    NestedFilter: {
                        Depth: 999999,
                        ScopeTypes: ["activity"]
                    },
                    PropertiesToReturn: {
                        AllStatistics: true,
                        AllAttributes: true,
                        AllHints: true,
                        AllProperties: true,
                        AllScopes: true
                    }
                };
                Promise.all([wu.fetchArchive(), wu.fetchDetailsRaw(scopesOptions)])
                    .then(([archiveXML, scopes])=>{
                        const markerData = buildMarkerData(scopes);
                        renderArchive(archiveXML, markerData);
                    });
            }
        });
        function renderArchive(archiveXmlStr, markerData) {
            const archive = xml2json(archiveXmlStr);

            const data = {
                label: ".",
                children: []
            };
            let queryPath = "";

            function walk(node, pathArr) {
                if (node && typeof node === "object") {
                    // 1 === Move empty string nodes to root
                    const hasRootIndicator = node["$"]
                        && node["$"].key === ""
                        && node["$"].name === ""
                        ;
                    // 2 === Move $.flags to './Libraries'
                    const hasFlagsIndicator = node["$"]
                        && typeof node["$"].flags !== "undefined"
                        ;
                    // 3 === "./AdditionalFiles" should be renamed to "./Resources"
                    const hasResourceIndicator = node.name === "AdditionalFiles"
                        ;
                    // 4 === node._children.length === 1 && node._children[0].name === Text
                    const hasRedundantTextNode = node._children.length === 1
                        && node._children[0].name === "Text"
                        ;
                    // 5 === Archive folder contents goes to root
                    const hasArchiveIndicator = node._children
                        && node._children.length > 0
                        && node.name === "Archive"
                        ;
                    // 6 === Has query path
                    const hasQueryPath = node.name === "Query";

                    // 7 === Check for base64 encoded
                    const hasABase64Indicator = node["$"]
                        && node["$"]["xsi:type"] === "SOAP-ENC:base64"
                        ;
                    let skipPathConcat = false;

                    if (hasRootIndicator || hasArchiveIndicator) { // 1 & 5
                        pathArr = [];
                        skipPathConcat = true;
                    } else if (hasFlagsIndicator) { // 2
                        pathArr = ["Libraries"];
                        if (hasRedundantTextNode) { // 4
                            node.content = node._children[0].content;
                            node._children = [];
                        }
                        skipPathConcat = true;
                    } else if (hasResourceIndicator) { // 3
                        pathArr = ["Resources"];
                        skipPathConcat = true;
                    }
                    if (node["$"]) {
                        let label = getNodeLabel(node);
                        if (hasQueryPath) { // 6
                            if (node["$"].attributePath) {
                                queryPath = node["$"].attributePath;
                            } else if (node.content && node.content.trim() && label.trim() === "") {
                                label = "<unnamed query>";
                                queryPath = label;
                            }
                        }
                        if (node.content && node.content.trim()) {
                            let _data = data;
                            pathArr.forEach(function (pathSegmentName, i) {
                                const found = _data.children.find(function (_n) {
                                    return _n.label === pathSegmentName;
                                });
                                if (found) {
                                    _data = found;
                                } else {
                                    const _temp = {
                                        label: pathSegmentName,
                                        path: pathArr.slice(0, i).join("."),
                                        children: []
                                    };
                                    _data.children.push(_temp);
                                    _data = _temp;
                                }
                            })
                                ;
                            let content = node.content.trim();
                            content = hasABase64Indicator ? atob(content) : content;
                            const path = pathArr.join(".");
                            const fullPath = path.length === 0 ? label : path + "." + label;
                            
                            const markers = fileMarkers(fullPath, markerData);
                            _data.children.push({
                                label,
                                path,
                                content,
                                markers,
                                fullPath,
                                selected: false
                            });
                        } else if (node._children) {
                            if (skipPathConcat) {
                                node._children.forEach(function (_node, i) {
                                    walk(_node, [...pathArr]);
                                });
                            } else {
                                if (label.indexOf(".") !== -1) {
                                    const name = getNodeName(node);
                                    if (name.indexOf(".") !== -1) {
                                        name.split(".").forEach(function (seg, i, arr) {
                                            if (i < arr.length - 1) {
                                                pathArr.push(seg);
                                            }
                                            label = seg;
                                        });
                                    }
                                }
                                node._children.forEach(function (_node, i) {
                                    walk(_node, [...pathArr].concat([label]));
                                });
                            }
                        }
                    }
                }
            }

            walk((archive as any)._children[0], []);

            recursiveSort(data);

            let firstTierLabels = data.children.map(function (n) {
                return n.label;
            });

            const librariesIdx = firstTierLabels.indexOf("Libraries");
            if (librariesIdx !== -1) {
                const lib = data.children[librariesIdx];
                data.children.splice(librariesIdx, 1);
                data.children.push(lib);
                firstTierLabels = data.children.map(function (n) {
                    return n.label;
                });
            }
            const resourcesIdx = firstTierLabels.indexOf("Resources");
            if (resourcesIdx !== -1) {
                const res = data.children[resourcesIdx];
                data.children.splice(resourcesIdx, 1);
                data.children.push(res);
            }
            context.directoryTree
                .data({})
                .render(function () {
                    context.directoryTree
                        .data(data)
                        .iconSize(16)
                        .rowItemPadding(2)
                        .textFileIcon("fa fa-file-code-o")
                        .render()
                        ;
                    context.directoryTree.rowClick = directoryTreeClick;
                    if (!context.archiveViewer.isDOMHidden()) {
                        const rw = context.directoryTree.calcWidth() + 20;
                        const pw = context.archiveViewer.width();
                        const ratio = rw / pw;
                        context.archiveViewer.relativeSizes([ratio, 1 - ratio]).resize().render();
                    }
                });

            context.directoryTree.rowClick = directoryTreeClick;

            function directoryTreeClick(contentStr, markers = []) {
                context.editor.text(contentStr);
            
                const fontFamily = "Verdana";
                const fontSize = 12;
            
                const maxLabelWidth = Math.max(
                    ...markers.map(marker=>{
                        return context.editor.textSize(marker.label, fontFamily, fontSize).width;
                    })
                );
            
                context.editor.gutterMarkerWidth(maxLabelWidth + 22);
                try {
                    addMarkers(markers);
                    context.editor.render();
                } catch(e) {
                    context.archiveViewer.render(()=>{
                        addMarkers(markers);
                    });
                }
            }

            function recursiveSort(n) {
                if (n.fullPath === queryPath) {
                    n.selected = true;
                    n.iconClass = "fa fa-code";
                    directoryTreeClick(n.content, n.markers);
                }
                if (n && n.children) {
                    n.children.sort(function (a, b) {
                        const aContent = a.content && a.content.trim();
                        const bContent = b.content && b.content.trim();
                        if (aContent && !bContent) {
                            return -1;
                        }
                        if (!aContent && bContent) {
                            return 1;
                        }
                        return a.label.toLowerCase() < b.label.toLowerCase() ? -1 : 1;
                    });
                    n.children.forEach(recursiveSort);
                }
            }
            function getNodeName(node) {
                return node["$"].name || node["$"].key;
            }
            function getNodeLabel(node) {
                let label = getNodeName(node);
                label = label ? label : "";
                if (!label && node["$"].resourcePath) {
                    label = node["$"].resourcePath.split("/").pop();
                } else if (!label && node["$"].originalFilename) {
                    label = node["$"].originalFilename.split("\\").pop();
                }
                return label;
            }
        }
        function buildMarkerData(scopesArr) {
            const markers = {};
        
            const timeName = "TimeMaxLocalExecute";
        
            scopesArr.forEach(scope => {
                const definitionList = scope.Properties.Property.find(n=>n.Name === "DefinitionList");
                
                const tableData = tableDataTransformer(scope.Properties.Property);
                
                const timeEntry = tableData.find(n=>n[0]===timeName);
                
                if(definitionList !== undefined && timeEntry !== undefined) {
                    const label = timeEntry[1];
                    const color = "orange";
                    const arr = definitionList.Formatted ? JSON.parse(definitionList.Formatted.split("\\").join("\\\\")) : [];
                    arr.forEach(path=>{
                        const sp = path.split("(");
                        const filePath = sp.slice(0, -1).join("(");
                        const [
                            lineNum,
                            charNum
                        ] = sp.slice(-1)[0].split(",").map(n=>parseInt(n));
                        if(!markers[filePath]){
                            markers[filePath] = [];
                        }
                        const rawTime = {};
                        scope.Properties.Property.forEach(n=>{
                            if(n.Name === timeName){
                                Object.assign(rawTime, n);
                            }
                        });
                        markers[filePath].push({
                            lineNum,
                            charNum,
                            label,
                            color,
                            definitionList,
                            tableData,
                            rawTime,
                            properties: scope.Properties.Property
                        });
                    });
                }
            });
            
            return markers;
        }
        function markerTooltipTable(marker) {
            const table = document.createElement("table");
            const thead = document.createElement("thead");
            const tbody = document.createElement("tbody");
            const labels = [];
            const tableDataArr = marker.tableData.map((_table, tableIdx)=>{
                const tableData = JSON.parse(_table);
                tableData.forEach(row=>{
                    if(labels.indexOf(row[0]) === -1){
                        labels.push(row[0]);
                    }
                });
                return tableData;
            });
            labels.sort();
            const _data = labels.map(label => {
                return [
                    label,
                    ...tableDataArr.map(tableData => {
                        let ret = "";
                        tableData.forEach(tableRow => {
                            if(tableRow[0] === label) {
                                ret = tableRow[1];
                            }
                        });
                        return ret;
                    })
                ];
            });
            
            _data
                .filter(row=>row[0] === "Label")
                .forEach(row=>{
                    appendRow(row, thead, () => true);
                });
            _data
                .filter(row=>row[0] !== "Label")
                .forEach(row=>{
                    appendRow(row, tbody, idx => idx === 0);
                });
            table.appendChild(thead);
            table.appendChild(tbody);
            table.style.maxWidth = "500px";
            return table;

            function appendRow(cellArr, parentNode, thCondition) {
                const tr = document.createElement("tr");
                tr.style.maxHeight = "200px";
                cellArr.forEach((cellText, i) => {
                    const td = document.createElement(thCondition(i) ? "th" : "td");
                    td.style.maxWidth = "180px";
                    td.style.textAlign = i === 0 ? "right" : "left";
                    td.style.overflow = "hidden";
                    td.style.textOverflow = "ellipsis";
                    td.textContent = cellText;
                    tr.appendChild(td);
                });
                parentNode.appendChild(tr);
            }
        }
        function addMarkers(markers) {
            const palette = Palette.rainbow("YlOrRd");
            const _markers = mergeCommonLines(markers);
        
            const [min, max] = extent(_markers, (n: any) => !n.timeSum ? 0 : parseInt(n.timeSum));
            if(min !== undefined && max !== undefined) {
                _markers.forEach(marker=>{
                    marker.color = palette(marker.timeSum, min, max);
                });
            }
            _markers.forEach(marker=>{
                context.editor.addGutterMarker(
                    marker.lineNum-1,
                    marker.label,
                    marker.color,
                    "Verdana",
                    "12px",
                    () => {
                        //onmouseenter
                        const _content = markerTooltipTable(marker);
                        context.tooltip._cursorLoc = [
                            (event as MouseEvent).clientX,
                            (event as MouseEvent).clientY
                        ];
                        context.tooltip
                            .followCursor(true)
                            .visible(true)
                            .fitContent(true)
                            .tooltipContent(_content)
                            .render()
                            ;
                    },
                    ()=>{
                        //onmouseleave
                        context.tooltip.visible(false);
                    }
                );
            });
        }
        function fileMarkers(fullPath, markerData) {
            const markerFilenameArr = Object.keys(markerData);
        
            const nameMatches = [];

            markerFilenameArr.forEach(name => {
                let formattedName = name;
                const dotSplit = name.split(".");
                if(dotSplit.length > 1){
                    formattedName = dotSplit[dotSplit.length - 2];
                }
                formattedName = formattedName.split("\\").join(".").split("/").join(".");
                const nameSegments = formattedName.split(".");
                if(fullPath.indexOf(nameSegments[nameSegments.length - 1]) !== -1) {
                    let _path = fullPath;
                    let score = fullPath.split(".").length;
                    while(_path.length > 0) {
                        if(formattedName.indexOf(fullPath) !== -1) {
                            nameMatches.push({
                                name,
                                score,
                                _path
                            });
                            break;
                        } else {
                            _path = _path.split(".").slice(1).join(".");
                            score = _path.split(".").length;
                        }
                    }
                }
            });
            nameMatches.sort((a, b) => b.score - a.score);
            let markers = [];
            if(nameMatches[0]) {
                markers = [...markerData[nameMatches[0].name]]; 
            }
            return markers;
        }
        function mergeCommonLines(markers) {
            const timeMapToMs = {
                "ns": 1000000,
                "us": 1000,
                "ms": 1,
            };
        
            const lineMap = {};
            markers.forEach(n=>{
                if(!lineMap[n.lineNum]){
                    lineMap[n.lineNum] = [];
                }
                lineMap[n.lineNum].push(n);
            });
            const ret = [];
            Object.keys(lineMap).forEach(key=>{
                let timeSum = 0;
                const units = "ns";
                const tableDataArr = [];
                lineMap[key].forEach(n=>{
                    if(n.rawTime){
                        const num = Number(n.rawTime.RawValue);
                        timeSum += num;
                    }
                    tableDataArr.push(JSON.stringify(n.tableData));
                });
                const lineMarker = {
                    lineNum: parseInt(key + ""),
                    label: (timeSum / timeMapToMs[units]).toFixed(3) + "ms",
                    timeSum,
                    tableData: tableDataArr
                };
                ret.push(lineMarker);
            });
            return ret;
        }
    }
}
