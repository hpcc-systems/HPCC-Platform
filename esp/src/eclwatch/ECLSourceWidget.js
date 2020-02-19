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

    "dojo/text!../templates/ECLSourceWidget.html",

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
    return declare("ECLSourceWidget", [_Widget], {
        templateString: template,
        baseClass: "ECLSourceWidget",
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
                var rw = this.directoryTree.calcRequiredWidth() + 20;
                var pw = this.borderContainer._contentBox.w;
                var ratio = rw / pw;
                this.archiveViewer
                    .relativeSizes([ratio, 1 - ratio])
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
            function flattenizeNode(node, pathStr, pathToQuery) {
                if(typeof pathStr === "undefined") {
                    pathStr = "";
                }
                var ret = {};
                var hasName = node.name;
                var hasMoneyName = node["$"] && node["$"].name;
                var childrenUndefined = typeof node._children === "undefined";
                ret.label = hasMoneyName ? node["$"].name : "";
    
                if (!hasMoneyName && hasName) {
                    ret.label = node.name;
                }
                var pathToNode = (pathStr.length > 0 ? pathStr + "." : "") + ret.label;
                if (!childrenUndefined && (node._children.length > 0 || !node.content)) {
                    if (node._children.length === 1 && node._children[0].name === "Text") {
                        ret.content = node._children[0].content.trim();
                        if (pathToNode === pathToQuery) {
                            ret.selected = true;
                        }
                    } else {
                        ret.children = node._children.map(function(n) {
                                return flattenizeNode(n, pathToNode, pathToQuery);
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
                    if(node.name && node.name !== "Query") {
                        return {
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
                    // skipping 'Query' metadata
                } else {
                    if(typeof flatMap[path] === "undefined"){
                        flatMap[path] = ret;
                        flatNodes.push({
                            data: ret,
                            path: path,
                            node: node
                        });
                    } else {
                        if (flatMap[path].label !== ret.label || ret.content) {
                            flatNodes.push({
                                data: ret,
                                path: path,
                                node: node
                            });
                        }
                    }
                    
                    var pathSegmentArr = path.split(".").slice(0,-1);
                    while(pathSegmentArr.length > 0){
                        var ancestorPath = pathSegmentArr.join(".");
                        if(!flatMap[ancestorPath]){
                            flatMap[ancestorPath] = {label:ancestorPath,children:[]};
                            flatNodes.push({
                                data: flatMap[ancestorPath],
                                path: ancestorPath
                            });
                        }
                        pathSegmentArr = pathSegmentArr.slice(0,-1);
                    }
                }
            }

            var context = this;

            this.directoryTree = new hpccjsTree.DirectoryTree()
                .textFileIcon("fa fa-file-code-o")
                .omitRoot(true)
                ;
            this.editor = new hpccjsCodemirror.ECLEditor();
            this.archiveViewer = new hpccjsPhosphor.SplitPanel("horizontal");
        
            var wu = hpccjsComms.Workunit.attach({ baseUrl: "" }, params.Wuid);
            wu.refresh(true).then(function(wuInfo){

                context.editor.text(wuInfo.Query.Text);
                
                if(!wuInfo.HasArchiveQuery){
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
                                    label: "",
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
                        .relativeSizes([0.1,0.9])
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
                            //1) Flatten and normalize the nodes
                            json._children[0]._children.forEach(function(n){
                                flattenizeNode(n, "", pathToQuery);
                            });

                            //2) Sort the flattened nodes
                            flatNodes.sort(function(a,b){
                                var aIsFolder = !!a.data.children;
                                var bIsFolder = !!b.data.children;
                                if((aIsFolder && bIsFolder) || (!aIsFolder && !bIsFolder)) {
                                    return a.path.toLowerCase() > b.path.toLowerCase() ? 1 : -1;
                                } else {
                                    return aIsFolder && !bIsFolder ? -1 : 1;
                                }
                            })

                            //3) Push sorted nodes into tree data shape
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
                                            flatNode.data.label = pathArr.slice(-1).join("");
                                        }
                                        if(!flatMap[parentPath].children){
                                            flatMap[parentPath].children = [];
                                        }
                                        flatMap[parentPath].children.push(flatNode.data);
                                    }
                                }
                            })
                        }
                        context.directoryTree
                            .data({})
                            .render(function(){
                                context.directoryTree
                                    .data(data)
                                    .iconSize(16)
                                    .rowItemPadding(2)
                                    .textFileIcon("fa fa-file-code-o")
                                    .render()
                                    ;
                                context.directoryTree.rowClick = function(contentStr) {
                                    context.editor.text(contentStr);
                                };
                                if(!context.archiveViewer.isDOMHidden()){
                                    var rw = context.directoryTree.calcRequiredWidth() + 20;
                                    var pw = context.archiveViewer.width();
                                    var ratio = rw / pw;
                                    context.archiveViewer.relativeSizes([ratio, 1 - ratio]).resize().render();
                                }
                            });
            
                        context.directoryTree.rowClick = function(contentStr) {
                            context.editor.text(contentStr);
                        }
                    });
                }
                
            });

            
        }
    });
});
