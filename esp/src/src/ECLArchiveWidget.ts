import { ECLEditor } from "@hpcc-js/codemirror";
import { extent, Palette } from "@hpcc-js/common";
import { Workunit } from "@hpcc-js/comms";
import { Table } from "@hpcc-js/dgrid";
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
import * as template from "dojo/text!hpcc/templates/ECLArchiveWidget.html";
// @ts-ignore
import * as _Widget from "hpcc/_Widget";
import { declareDecorator } from "./DeclareDecorator";

class DirectoryTreeEx extends DirectoryTree {
    
    public metricArr;

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
    private leftPanel: SplitPanel;
    private summaryTable: Table;
    private archiveViewer: SplitPanel;
    private directoryTree: DirectoryTreeEx;
    private selectedMarker: number;
    
    private scopes: any;
    private archiveXML: any;
    public metricArr: string[] = [];

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

    updateMetrics(params) {
        const scopes = this.scopes;
        const archiveXML = this.archiveXML;

        const metricTypeMap = {Time:{}, Size:{}, Num:{}, Other:{}};
        const metricArr = [];
        Object.keys(metricTypeMap).forEach((prefix)=>{
            
            const _prefix = params.metricKey.slice(0, prefix.length);
            let suffix = "";
            if(prefix === _prefix) {
                suffix = params.metricKey.slice(prefix.length);

                metricArr.push(`${prefix}Max${suffix}`);
                metricArr.push(`${prefix}Avg${suffix}`);
                metricArr.push(`${prefix}${suffix}`);
            }
        });
        this.metricArr = metricArr;
        
        const markerData = this.buildMarkerData(scopes);
        if(params.ondatachange){
            params.ondatachange(
                this.markerDataToOptionsData(markerData, metricTypeMap)
            );
        }
        this.renderArchive(archiveXML, markerData);
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
        this.summaryTable = new Table()
            .sortable(true)
            ;
        this.editor = new ECLEditor().readOnly(true);
        this.archiveViewer = new SplitPanel("horizontal");
        this.leftPanel = new SplitPanel("vertical");

        this.archiveViewer
            .target(context.id + "EclContent")
            ;

        const wu = Workunit.attach({ baseUrl: "" }, params.Wuid);
        wu.fetchQuery().then(function (query) {
            context.editor.text(query.Text);
            if (!wu.HasArchiveQuery) {
                context.archiveViewer
                    .addWidget(context.editor)
                    .lazyRender()
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
                context.leftPanel
                    .addWidget(context.directoryTree)
                    .addWidget(context.summaryTable)
                    .relativeSizes([0.38, 0.62])
                    .lazyRender()
                    ;
                context.archiveViewer
                    .addWidget(context.leftPanel)
                    .addWidget(context.editor)
                    .relativeSizes([0.2, 0.8])
                    .lazyRender()
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
                    .then(([archiveXML, scopes]) => {
                        
                        context.archiveXML = archiveXML;
                        context.scopes = scopes;

                        context.updateMetrics(params);
                    });
            }
        });
    }
    markerDataToOptionsData(data, metricTypeMap) {
        const metricOptionCountMap = Object.keys(data).reduce((r, k, i)=>{
            data[k].forEach(n=>{
                n.properties.filter(p=>p.Measure).forEach(p=>{
                    if(!r[p.Name])r[p.Name] = 0;
                    r[p.Name]++;
                });
            });
            return r;
        }, {});
        const keyArr = Object.keys(metricOptionCountMap);
        const metricSubTypes = {};
        keyArr.forEach(k=>{
            const typeStr = Object.keys(metricTypeMap).find(k2 => k.indexOf(k2) === 0);
            
            if(typeStr){
                let subType = k.slice(typeStr.length);
                let subsubType = "";
                if(subType.includes("Max"))subsubType = "Max";
                if(subType.includes("Avg"))subsubType = "Avg";
                subType = subType.split("Max").join("");
                subType = subType.split("Avg").join("");
                if(!metricTypeMap[typeStr][subType]){
                    metricTypeMap[typeStr][subType] = [];
                }
                metricSubTypes[subType] = typeStr;
                metricTypeMap[typeStr][subType].push({
                    type: typeStr,
                    subType,
                    subsubType,
                    text: k
                });
            }
        });
        let optionArr = [];
        Object.keys(metricTypeMap).forEach((type, typeIdx, typeArr)=>{
            if(Object.keys(metricTypeMap[type]).length === 0)return;

            optionArr.push({
                key: type, text: type, itemType: "Header"
            });
            const arr = Object.keys(metricTypeMap[type]);
            arr.forEach(subType=>{
                metricTypeMap[type][subType].sort((a, b) => {
                    let _a = 0;
                    let _b = 0;
                    if(a.subsubType === "Max")_a += 2; 
                    if(b.subsubType === "Max")_b += 2;
                    if(a.subsubType === "Avg")_a += 1; 
                    if(b.subsubType === "Avg")_b += 1;
                    return _a > _b ? -1 : 1;
                });
            });
            arr.forEach(subType=>{
                optionArr.push({
                    key: subType, text: subType
                });
            });
            if((typeIdx + 1) < typeArr.length) {
                optionArr.push({
                    key: `divider_${typeIdx+1}`, text: "-", itemType: "Divider"
                });
            }
        });
        if(optionArr.length > 0 && optionArr[optionArr.length-1].text === "-")optionArr = optionArr.slice(0, -1);
        return {
            metricTypeMap,
            metricSubTypes,
            optionArr
        };
    }
    renderArchive(archiveXmlStr, markerData) {
        const context = this;
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

                        const markers = context.fileMarkers(fullPath, markerData);
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
        this.directoryTree
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

            try {
                context.addMarkers(markers);
                context.editor.render();
            } catch (e) {
                context.archiveViewer.render(() => {
                    context.addMarkers(markers);
                });
            }
            context.updateSummary(markers);
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
    updateSummary(markers) {
        const context = this;
        const propCounts = {};
        const propFormats = {};
        const propSums = markers.reduce((ret, n)=>{
            n.properties.forEach(prop=>{
                if(prop.Measure !== undefined){
                    if(!propCounts[prop.Name]){
                        propCounts[prop.Name] = 0;
                        propFormats[prop.Name] = prop.Measure;
                        ret[prop.Name] = 0;
                    }
                    propCounts[prop.Name]++;
                    ret[prop.Name] += Number(prop.RawValue);
                }
            });
            return ret;
        }, {});
        const propAvgs = Object.keys(propSums).reduce((ret, k)=>{
            ret[k] = propSums[k] / propCounts[k];
            return ret;
        }, {});
        context.summaryTable
            .columns(["Name", "Cnt", "Avg", "Sum"])
            .data([
                ...Object.keys(propSums).map(k=>{
                    let avg = propAvgs[k];
                    let sum = propSums[k];

                    const isTime = propFormats[k] === "ns";
                    const isSize = propFormats[k] === "sz";

                    if(isTime) {
                        avg = _formatTime(avg);
                        sum = _formatTime(sum);
                    } else if (isSize) {
                        avg = _formatSize(avg);
                        sum = _formatSize(sum);
                    } else {
                        avg = avg.toFixed(3);
                        sum = sum.toFixed(3);
                    }
                    return [
                        k,
                        propCounts[k],
                        avg,
                        sum,
                    ];
                })
            ])
            .lazyRender()
            ;
    }
    markerTableData(marker) {
        const labels = [];
        const tableDataArr = marker.tableData.map((_table, tableIdx) => {
            const tableData = JSON.parse(_table);
            tableData.forEach(row => {
                if (labels.indexOf(row[0]) === -1) {
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
                        if (tableRow[0] === label) {
                            ret = tableRow[1];
                        }
                    });
                    return ret;
                })
            ];
        });
        return _data;
    }
    addMarkers(markers) {
        const palette = Palette.rainbow("YlOrRd");
        const _markers = this.mergeCommonLines(markers);
        const fontFamily = "Verdana";
        const fontSize = 12;

        const maxLabelWidth = Math.max(
            ..._markers.map(marker => {
                return this.editor.textSize(marker.label, fontFamily, fontSize).width;
            })
        );
        this.editor.gutterMarkerWidth(maxLabelWidth + 22);
        const [min, max] = extent(_markers, (n: any) => !n.valueSum ? 0 : parseInt(n.valueSum));
        if (min !== undefined && max !== undefined) {
            _markers.forEach(marker => {
                marker.color = palette(marker.valueSum, min, max);
            });
        }
        _markers.forEach(marker => {
            this.editor.addGutterMarker(
                marker.lineNum - 1,
                marker.label,
                marker.color,
                "Verdana",
                "12px",
                () => {},
                () => {},
                () => {
                    if(this.selectedMarker === marker.lineNum) {
                        this.updateSummary(markers);
                        this.selectedMarker = -1;
                        const columnArr = this.summaryTable.columns();
                        columnArr[0] = "Name";
                        this.summaryTable
                            .columns(columnArr)
                            .lazyRender()
                            ;
                    } else {

                        const _data = this.markerTableData(marker);
                        
                        this.summaryTable
                            .columns(["Line: "+marker.lineNum, ...Array(_data[0].length).fill("")])
                            .data(_data)
                            .lazyRender()
                            ;

                        this.selectedMarker = marker.lineNum;
                    }
                }
            );
        });
    }
    fileMarkers(fullPath, markerData) {
        const markerFilenameArr = Object.keys(markerData);

        const nameMatches = [];

        markerFilenameArr.forEach(name => {
            let formattedName = name;
            const dotSplit = name.split(".");
            if (dotSplit.length > 1) {
                formattedName = dotSplit[dotSplit.length - 2];
            }
            formattedName = formattedName.split("\\").join(".").split("/").join(".");
            const nameSegments = formattedName.split(".");
            if (fullPath.indexOf(nameSegments[nameSegments.length - 1]) !== -1) {
                let _path = fullPath;
                let score = fullPath.split(".").length;
                while (_path.length > 0) {
                    if (formattedName.indexOf(fullPath) !== -1) {
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
        if (nameMatches[0]) {
            markers = [...markerData[nameMatches[0].name]];
        }
        return markers;
    }
    mergeCommonLines(markers) {
        const lineMap = {};
        markers.forEach(n => {
            if (!lineMap[n.lineNum]) {
                lineMap[n.lineNum] = [];
            }
            lineMap[n.lineNum].push(n);
        });
        const ret = [];
        Object.keys(lineMap).forEach(key => {
            let valueSum = 0;
            const tableDataArr = [];
            lineMap[key].forEach(n => {
                const k = this.metricArr.find(k=>{
                    return n.propertyMap[k];
                });
                if (n.propertyMap[k]) {
                    const num = Number(n.propertyMap[k].RawValue);
                    valueSum += num;
                }
                tableDataArr.push(JSON.stringify(n.tableData));
            });
            let label = ""+ (valueSum);
            if(this.metricArr[0].slice(0, 4) === "Time") {
                label = nsToTime(valueSum);
            } else if (this.metricArr[0].slice(0, 4) === "Size") {
                label = _formatSize(valueSum);
            }
            const lineMarker = {
                lineNum: parseInt(key + ""),
                label,
                valueSum,
                tableData: tableDataArr
            };
            ret.push(lineMarker);
        });
        return ret;

        function nsToTime(nanoseconds) {
            const subSecond:string|number = Math.floor(nanoseconds % 100000000);
            let seconds:string|number = Math.floor((nanoseconds / 1000000000) % 60);
            let minutes:string|number = Math.floor((nanoseconds / (1000000000 * 60)) % 60);
            let hours:string|number = Math.floor((nanoseconds / (1000000000 * 60 * 60)) % 24);
            
            hours = (hours < 10) ? "0" + hours : hours;
            minutes = (minutes < 10) ? "0" + minutes : minutes;
            seconds = (seconds < 10) ? "0" + seconds : seconds;
            
            return String(hours).padStart(2, "0") + ":" + String(minutes) + ":" + String(seconds) + "." + String(subSecond).padStart(9, "0");
        }
    }
    tableDataTransformer(d) {
        const ret = d.map((n: any) => {
            return [
                n.Name,
                n.Formatted
            ];
        });
        ret.sort((a, b) => a[0].localeCompare(b[0]));
        return ret;
    }
    
    buildMarkerData(scopesArr) {
        const markers = {};
        const localExecuteArr = [
            "TimeMaxLocalExecute",
            "TimeAvgLocalExecute",
            "TimeLocalExecute",
        ];
        scopesArr.forEach(scope => {
            const definitionList = scope.Properties.Property.find(n => n.Name === "DefinitionList");
    
            const tableData = this.tableDataTransformer(scope.Properties.Property);
    
            let timeEntry;
            for (const timeName of localExecuteArr) {
                timeEntry = tableData.find(n => n[0] === timeName);
                if (timeEntry) {
                    break;
                }
            }
            if (definitionList !== undefined && timeEntry !== undefined) {
                let label = timeEntry[1];
                const color = "orange";
                const arr = definitionList.Formatted ? JSON.parse(definitionList.Formatted.split("\\").join("\\\\")) : [];
                arr.forEach(path => {
                    const sp = path.split("(");
                    const filePath = sp.slice(0, -1).join("(");
                    const [
                        lineNum,
                        charNum
                    ] = sp.slice(-1)[0].split(",").map(n => parseInt(n));
                    if (!markers[filePath]) {
                        markers[filePath] = [];
                    }
                    let rawValue;
                    const propertyMap = scope.Properties.Property.reduce((r, n)=>{
                        r[n.Name] = n;
                        return r;
                    }, {});
                    for (const metricName of this.metricArr) {
                        if(propertyMap[metricName]) {
                            rawValue = { ...propertyMap[metricName] };
                            break;
                        }
                    }
                    if(rawValue && rawValue.Formatted){
                        label = rawValue.Formatted;
                    } else {
                        label = "";
                    }
                    markers[filePath].push({
                        lineNum,
                        charNum,
                        label,
                        color,
                        definitionList,
                        tableData,
                        rawTime: rawValue ?? {},
                        properties: scope.Properties.Property,
                        propertyMap
                    });
                });
            }
        });
        return markers;
    }
}
function _formatTime(v){
    if(v > 1000000000) {
        return (v / 1000000000).toFixed(3) + "s";
    }
    return (v / 1000000).toFixed(3) + "ms";
}
function _formatSize(v){
    if(v > 1000000000) {
        return (v * 0.000000000931).toFixed(3) + "Gb";
    }
    else if(v > 1000000) {
        return (v * 0.0000009537).toFixed(3) + "Mb";
    }
    return (v * 0.000977).toFixed(3) + "Kb";
}