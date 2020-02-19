
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/request/xhr",
    "dojo/topic",

    "dijit/registry",

    "hpcc/_Widget",
    "src/ESPWorkunit",

    "@hpcc-js/comms",
    "@hpcc-js/tree",
    "@hpcc-js/codemirror",
    "@hpcc-js/phosphor",
    "@hpcc-js/util",

    "dojo/text!../templates/ECLSourceWidgetNew.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/form/Button"

], function (declare, lang, i18n, nlsHPCC, dom, xhr, topic,
    registry,
    _Widget, ESPWorkunit,
    hpccjsComms,
    hpccjsTree,
    hpccjsCodemirror,
    hpccjsPhosphor,
    hpccjsUtil,
    template) {
    return declare("ECLSourceWidgetNew", [_Widget], {
        templateString: template,
        baseClass: "ECLSourceWidgetNew",
        i18n: nlsHPCC,

        borderContainer: null,
        eclSourceContentPane: null,
        wu: null,
        editor: null,
        markers: [],
        highlightLines: [],
        readOnly: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
            if (this.archiveViewer) {
                this.archiveViewer
                    .resize()
                    .render()
                ;
            }
        },

        //  Plugin wrapper  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            var flatMap = {};
            var flatNodes = [];
            function transformNode(node, pathStr, pathToQuery) {
                if(typeof pathStr === "undefined") {
                    pathStr = "";
                }
                var ret = {};
                var hasName = node.name;
                var hasMoneyName = node["$"] && node["$"].name;
                var childrenUndefined = typeof node._children === "undefined";
                ret.label = hasMoneyName ? node["$"].name : "";
    
                var pathToNode = (pathStr.length > 0 ? pathStr + "." : "") + ret.label;
                
                if (!hasMoneyName && hasName) {
                    ret.label = node.name;
                }
                if (!childrenUndefined && (node._children.length > 0 || !node.content)) {
                    if (node._children.length === 1 && node._children[0].name === "Text") {
                        ret.content = node._children[0].content.trim();
                        if (pathToNode === pathToQuery) {
                            ret.selected = true;
                        }
                    } else {
                        ret.children = node._children.map(function(n) {
                                return transformNode(n, pathToNode, pathToQuery);
                            })
                            .filter(function(n){
                                return n;
                            })
                            ;
                    }
                } else if (typeof node.content === "string" && node.content.trim()) {
                    ret.content = node.content.trim();
                    ret.selected = pathToNode === pathToQuery;
                } else {
                    if(node.name && node.name !== "Query"){
                        ret = {
                            label: node.name,
                            content: JSON.stringify(node, undefined, 4)
                        };
                    }
                    return false;
                }
                var path = (pathStr.length > 0 ? pathStr + "." : "") + ret.label;
                if(node["$"] && node["$"].name === "" && node["$"].key === "") {
                    // new root folder
                    path = "";
                }
                if(ret.label === "Query" && (!node.children || node._children.length === 0)){
                    // skipping 'Query' metadata and empty folders
                } else {
                    flatMap[path] = ret;
                    flatNodes.push({
                        data: ret,
                        path: path,
                        node: node
                    });
                }
            }

            var context = this;

            var directoryTree = new hpccjsTree.DirectoryTree()
                .textFileIcon("fa fa-file-code-o")
                .omitRoot(true)
                ;
            var editor = new hpccjsCodemirror.ECLEditor();
            var panels = new hpccjsPhosphor.SplitPanel("horizontal");
        
            var wu = hpccjsComms.Workunit.attach({ baseUrl: "" }, params.Wuid);
            wu.refresh(true).then(function(wuInfo){

                editor.text(wuInfo.Query.Text);
                
                if(!wuInfo.HasArchiveQuery){
                    editor
                        .target(context.id + "EclContent")
                        .render()
                        ;
                } else {
                    
                    directoryTree
                        .data({
                            label: "root",
                            children: [
                                {
                                    label:"loading...",
                                }
                            ]
                        })
                        .textFileIcon("fa fa-spinner fa-spin")
                        .iconSize(20)
                        .rowItemPadding(2)
                        ;
                    panels
                        .target(context.id + "EclContent")
                        .addWidget(directoryTree)
                        .addWidget(editor)
                        .relativeSizes([0.38,0.62])
                        .render()
                        ;

                    wu.fetchArchive().then(function(archiveResp) {
                        var json = hpccjsUtil.xml2json(archiveResp);
                        var pathToQuery = "";
                        try{
                            pathToQuery = json._children[0]._children.find(function(n){
                                return n.name && n.name.toLowerCase()==="query";
                            })["$"].attributePath;
                        }catch(e){}
                        var data = {
                            label: "",
                            children: []
                        };
                        if (json._children && json._children[0]) {
                            json._children[0]._children.forEach(function(n){
                                transformNode(n, "", pathToQuery);
                            });
                            flatNodes.sort(function(a,b){
                                var aIsFolder = !!a.data.children;
                                var bIsFolder = !!b.data.children;
                                if((aIsFolder && bIsFolder) || (!aIsFolder && !bIsFolder)) {
                                    return a.path.toLowerCase() > b.path.toLowerCase() ? 1 : -1;
                                } else {
                                    return aIsFolder && !bIsFolder ? -1 : 1;
                                }
                            })
                            flatNodes.forEach(function(flatNode,i){
                                var pathArr = flatNode.path.split(".").filter(function(n){
                                    return n !== "";
                                });
                                if (pathArr.length === 0) {
                                    data.children = data.children.concat(flatNode.data.children);
                                } else {
                                    var parentPath = pathArr.slice(0,-1).join('.');
                                    if(!parentPath) {
                                        data.children.push(flatNode.data);
                                    } else {
                                        if(!flatNode.data.content || !flatNode.data.content.trim()){
                                            flatNode.data.label = pathArr.slice(-1);
                                        }
                                        if(!flatMap[parentPath].children){
                                            flatMap[parentPath].children = [];
                                        }
                                        flatMap[parentPath].children.push(flatNode.data);
                                    }
                                }
                                
                            })
                        }
                        directoryTree
                            .data({})
                            .render(function(){
                                directoryTree
                                    .data(data)
                                    .iconSize(16)
                                    .rowItemPadding(2)
                                    .textFileIcon("fa fa-file-code-o")
                                    .render()
                                    ;
                                
                                directoryTree.rowClick = function(contentStr) {
                                    editor.text(contentStr);
                                };
                                
                                panels.render(function(){
                                    var rw = directoryTree.calcRequiredWidth() + 20;
                                    var pw = panels.width();
                                    var ratio = rw / pw;
                                    panels.relativeSizes([ratio, 1 - ratio]);
                                });
                            });
            
                        directoryTree.rowClick = function(contentStr) {
                            editor.text(contentStr);
                        }
                        
                        panels.render(function(){
                            var rw = directoryTree.calcRequiredWidth() + 20;
                            var pw = panels.width();
                            var ratio = rw / pw;
                            panels.relativeSizes([ratio, 1 - ratio]);
                        });
                    });
                }
                
            });

            
        }
    });
});

