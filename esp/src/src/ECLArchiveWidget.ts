import { ECLEditor } from "@hpcc-js/codemirror";
import { Workunit } from "@hpcc-js/comms";
import { SplitPanel } from "@hpcc-js/phosphor";
import { DirectoryTree } from "@hpcc-js/tree";
import { xml2json, XMLNode } from "@hpcc-js/util";
import "dijit/form/Button";
import "dijit/layout/BorderContainer";
import "dijit/layout/ContentPane";
import * as registry from "dijit/registry";
import "dijit/Toolbar";
import "dijit/ToolbarSeparator";
import "dojo/i18n";
// @ts-ignore
import * as nlsHPCC from "dojo/i18n!hpcc/nls/hpcc";
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

interface TreeNode {
    label: string;
    content?: string;
    selected?: boolean;
    children?: TreeNode[];
}

interface FlatNode {
    path: string;
    data: TreeNode;
}

type TreeNodeMap = { [path: string]: TreeNode };

function flattenizeNode(node: XMLNode, pathStr: string, pathToQuery: string, flatMap: TreeNodeMap, flatNodes: FlatNode[]): TreeNode | undefined {
    if (typeof pathStr === "undefined") {
        pathStr = "";
    }
    const hasName = node.name;
    const hasMoneyName = node["$"] && node["$"].name;
    const childrenUndefined = typeof node.children() === "undefined";
    const ret: TreeNode = {
        label: hasMoneyName ? node["$"].name : ""
    };

    if (!hasMoneyName && hasName) {
        ret.label = node.name;
    }
    const pathToNode = (pathStr.length > 0 ? pathStr + "." : "") + ret.label;
    if (!childrenUndefined && (node.children().length > 0 || !node.content)) {
        if (node.children().length === 1 && node.children()[0].name === "Text") {
            ret.content = node.children()[0].content.trim();
            if (pathToNode === pathToQuery) {
                ret.selected = true;
            }
        } else {
            ret.children = node.children().map(function (n) {
                return flattenizeNode(n, pathToNode, pathToQuery, flatMap, flatNodes);
            }).filter(function (n) {
                return n;
            });
        }
    } else if (typeof node.content === "string" && node.content.trim()) {
        ret.content = node.content.trim();
        ret.selected = pathToNode === pathToQuery;
    } else {
        if (node.name && node.name !== "Query") {
            return {
                label: node.name,
                content: JSON.stringify(node, undefined, 4)
            };
        }
        return undefined;
    }
    let path = (pathStr.length > 0 ? pathStr + "." : "") + ret.label;
    if (node["$"] && node["$"].name === "" && node["$"].key === "") {
        // new root folder
        path = "";
    }
    if (ret.label === "Query" && (!node.children || node.children().length === 0)) {
        // skipping 'Query' metadata
    } else {
        if (typeof flatMap[path] === "undefined") {
            flatMap[path] = ret;
            flatNodes.push({
                data: ret,
                path
            });
        } else {
            if (flatMap[path].label !== ret.label || ret.content) {
                flatNodes.push({
                    data: ret,
                    path
                });
            }
        }

        let pathSegmentArr = path.split(".").slice(0, -1);
        while (pathSegmentArr.length > 0) {
            const ancestorPath = pathSegmentArr.join(".");
            if (!flatMap[ancestorPath]) {
                flatMap[ancestorPath] = { label: ancestorPath, children: [] };
                flatNodes.push({
                    data: flatMap[ancestorPath],
                    path: ancestorPath
                });
            }
            pathSegmentArr = pathSegmentArr.slice(0, -1);
        }
    }
    return undefined;
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
        this.editor = new ECLEditor();
        this.archiveViewer = new SplitPanel("horizontal");

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

                wu.fetchArchive().then(function (archiveResp) {
                    const json = xml2json(archiveResp);
                    let pathToQuery = "";
                    try {
                        pathToQuery = json.children()[0].children().find(function (n) {
                            return n.name && n.name.toLowerCase() === "query";
                        })["$"].attributePath;
                    } catch (e) { }
                    const data: { label: string, children: any[] } = {
                        label: "",
                        children: []
                    };
                    if (json.children() && json.children()[0]) {
                        const flatMap: TreeNodeMap = {};
                        const flatNodes: FlatNode[] = [];

                        // 1) Flatten and normalize the nodes
                        json.children()[0].children().forEach(function (n) {
                            flattenizeNode(n, "", pathToQuery, flatMap, flatNodes);
                        });

                        // 2) Sort the flattened nodes
                        flatNodes.sort(function (a, b) {
                            const aIsFolder = !!a.data.children;
                            const bIsFolder = !!b.data.children;
                            if ((aIsFolder && bIsFolder) || (!aIsFolder && !bIsFolder)) {
                                return a.path.toLowerCase() > b.path.toLowerCase() ? 1 : -1;
                            } else {
                                return aIsFolder && !bIsFolder ? -1 : 1;
                            }
                        });

                        // 3) Push sorted nodes into tree data shape
                        flatNodes.forEach(function (flatNode, i) {
                            const pathArr = flatNode.path.split(".").filter(function (n) {
                                return n !== "";
                            });
                            if (pathArr.length === 0) {
                                data.children = data.children.concat(flatNode.data.children);
                            } else {
                                const parentPath = pathArr.slice(0, -1).join(".");
                                if (!parentPath) {
                                    data.children.push(flatNode.data);
                                } else {
                                    if (!flatNode.data.content || !flatNode.data.content.trim()) {
                                        flatNode.data.label = pathArr.slice(-1).join("");
                                    }
                                    if (!flatMap[parentPath].children) {
                                        flatMap[parentPath].children = [];
                                    }
                                    flatMap[parentPath].children.push(flatNode.data);
                                }
                            }
                        });
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
                            context.directoryTree.rowClick = function (contentStr) {
                                context.editor.text(contentStr);
                            };
                            if (!context.archiveViewer.isDOMHidden()) {
                                const rw = context.directoryTree.calcWidth() + 20;
                                const pw = context.archiveViewer.width();
                                const ratio = rw / pw;
                                context.archiveViewer.relativeSizes([ratio, 1 - ratio]).resize().render();
                            }
                        });

                    context.directoryTree.rowClick = function (contentStr) {
                        context.editor.text(contentStr);
                    };
                });
            }

        });
    }
}
