import { ECLEditor } from "@hpcc-js/codemirror";
import { Workunit } from "@hpcc-js/comms";
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

                wu.fetchArchive().then(function (archiveXML) {
                    renderArchive(archiveXML);
                });
            }

        });

        function renderArchive(archiveXmlStr) {
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
                            _data.children.push({
                                label,
                                path,
                                content,
                                fullPath: path.length === 0 ? label : path + "." + label,
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

            function recursiveSort(n) {
                if (n.fullPath === queryPath) {
                    n.selected = true;
                    n.iconClass = "fa fa-code";
                    if (n.content.length > context.editor.text().length) {
                        context.editor.text(n.content);
                    }
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

    }
}
