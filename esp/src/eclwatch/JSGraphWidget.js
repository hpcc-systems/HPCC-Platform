define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/Evented",

    "@hpcc-js/common",
    "@hpcc-js/graph",
    "@hpcc-js/layout",

    "hpcc/GraphWidget",
    "src/ESPGraph",
    "src/Utility",

    "css!font-awesome/css/font-awesome.css"
], function (declare, lang, nlsHPCCMod, arrayUtil, Evented,
    hpccCommon, hpccGraph, hpccLayout,
    GraphWidget, ESPGraph, Utility) {

    var nlsHPCC = nlsHPCCMod.default;
    var faCharFactory = function (kind) {
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
    };

    var JSPlugin = declare([Evented], {
        KeyState_None: 0,
        KeyState_Shift: 1,
        KeyState_Control: 2,
        KeyState_Menu: 4,

        constructor: function (domNode) {
            this.graphData = new ESPGraph.Graph();
            this.graphWidget = new hpccGraph.Graph()
                .allowDragging(false)
                .showToolbar(false)
                ;
            var context = this;
            this.graphWidget.vertex_click = function (item, event) {
                context.emit("SelectionChanged", [item]);
            }
            this.graphWidget.edge_click = function (item, event) {
                context.emit("SelectionChanged", [item]);
            }
            this.graphWidget.vertex_dblclick = function (item, event) {
                context.emit("MouseDoubleClick", item, (event.shiftKey ? context.KeyState_Shift : 0) + (event.ctrlKey ? context.KeyState_Control : 0) + (event.altKey ? context.KeyState_Menu : 0));
            }
            this.messageWidget = new hpccCommon.TextBox()
                .shape_colorFill("#006CCC")
                .shape_colorStroke("#003666")
                .text_colorFill("#FFFFFF")
                ;
            this.layout = new hpccLayout.Layered()
                .target(domNode.id)
                .addLayer(this.messageWidget)
                .addLayer(this.graphWidget)
                .render()
                ;
            this._options = {};
        },

        option: function (key, _) {
            if (arguments.length < 1) throw Error("Invalid Call:  option");
            if (arguments.length === 1) return this._options[key];
            this._options[key] = _ instanceof Array ? _.length > 0 : _;
            return this;
        },

        optionsReset: function (options) {
            options = options || this._optionsDefault;
            for (var key in options) {
                this.option(key, options[key]);
            }
        },

        setMessage: function (msg) {
            if (msg !== this._prevMsg) {
                this.messageWidget
                    .text(msg)
                    .visible(msg ? true : false)
                    .render()
                    ;
                if ((msg && this.graphWidget.visible()) || (!msg && !this.graphWidget.visible())) {
                    this.graphWidget.visible(msg ? false : true).render();
                }
                this._prevMsg = msg;
            }
        },

        setScale: function (scale) {
            this.graphWidget.zoomTo(undefined, scale / 100);
        },

        centerOnItem: function (item, scaleToFit, widthOnly) {
            if (item) {
                if (scaleToFit) {
                    var bbox = item.__widget.getBBox();
                    this.graphWidget.zoomToBBox(bbox);
                } else {
                    var bounds = this.graphWidget.getBounds([item.__widget]);
                    this.graphWidget.centerOn(bounds);
                }
            } else {
                if (scaleToFit) {
                    this.graphWidget.zoomToFit();
                } else {
                    var bounds = this.graphWidget.getVertexBounds();
                    this.graphWidget.centerOn(bounds);
                }
            }
        },

        getSelectionAsGlobalID: function () {
            var selection = this.graphWidget.selection();
            return selection.map(function (item) {
                return item.__hpcc_globalID;
            });
        },

        setSelectedAsGlobalID: function (globalIDs) {
            var selection = [];
            globalIDs.forEach(function (globalID, idx) {
                var item = this.getItem(globalID);
                if (item && item.__widget) {
                    selection.push(item.__widget);
                }
            }, this);
            this.graphWidget.selection(selection);
        },

        getGlobalType: function (item) {
            return this.graphData.getGlobalTypeString(item);
        },

        getGlobalID: function (item) {
            return item.__hpcc_id;
        },

        getItem: function (globalID) {
            return this.graphData.idx[globalID];
        },

        setSelected: function (items) {
            this.graphWidget.selection(items);
        },

        getSelection: function () {
            return this.graphWidget.selection();
        },

        getSVG: function () {
            return "";  //TODO - Should be Serialized Layout to prevent re-calculation on prev/next  ---
        },

        getDOT: function () {
            return "";
        },

        getVertices: function () {
            return this.graphData.vertices;
        },

        find: function (findText) {
            var findProp = "";
            var findTerm = findText;
            var findTextParts = findText.split(":");
            if (findTextParts.length > 1) {
                findProp = findTextParts[0];
                findTextParts.splice(0, 1);
                findTerm = findTextParts.join(":");
            }
            return arrayUtil.filter(this.graphData.vertices, function (item) {
                if (findProp) {
                    if (item.hasOwnProperty(findProp)) {
                        return (item[findProp].toString().toLowerCase().indexOf(findTerm.toLowerCase()) >= 0);
                    }
                } else {
                    for (var key in item) {
                        if (item.hasOwnProperty(key) && item[key].toString().toLowerCase().indexOf(findTerm.toLowerCase()) >= 0) {
                            return true;
                        }
                    }
                }
                return false;
            });
        },

        cleanObject: function (object) {
            var retVal = {};
            for (var key in object) {
                if (object.hasOwnProperty(key) && typeof object[key] !== "function") {
                    retVal[key] = object[key];
                }
            }
            return retVal;
        },

        cleanObjects: function (objects) {
            return objects.map(function (object) {
                return this.cleanObject(object);
            }, this);
        },

        gatherTreeWithProperties: function (subgraph) {
            subgraph = subgraph || this.graphData.subgraphs[0];
            var retVal = subgraph.getProperties();
            retVal._children = [];
            arrayUtil.forEach(subgraph.__hpcc_subgraphs, function (subgraph, idx) {
                retVal._children.push(this.gatherTreeWithProperties(subgraph));
            }, this);
            arrayUtil.forEach(subgraph.__hpcc_vertices, function (vertex, idx) {
                retVal._children.push(vertex.getProperties());
            }, this);
            return retVal;
        },

        getProperties: function (item) {
            return item.getProperties();
        },

        getTreeWithProperties: function () {
            return [this.gatherTreeWithProperties()];
        },

        getSubgraphsWithProperties: function () {
            return this.cleanObjects(this.graphData.subgraphs);
        },

        getVerticesWithProperties: function () {
            return this.cleanObjects(this.graphData.vertices);
        },

        getEdgesWithProperties: function () {
            return this.cleanObjects(this.graphData.edges);
        },

        getLocalisedXGMML2: function (selectedItems, depth, distance, noSpills) {
            return this.graphData.getLocalisedXGMML(selectedItems, depth, distance, noSpills);
        },

        startLayout: function (layout) {
            var context = this;
            setTimeout(function (layout) {
                context.graphWidget
                    .layout("Hierarchy")
                    .render()
                    ;
                context.emit("LayoutFinished", {});
            }, 100);
        },

        clear: function () {
            this.graphData.clear();
            this.graphWidget.clear();
        },

        mergeXGMML: function (xgmml) {
            this._loadXGMML(xgmml, true);
        },

        loadXGMML: function (xgmml) {
            this._loadXGMML(xgmml, false);
        },

        _loadXGMML: function (xgmml, merge) {
            if (merge) {
                this.graphData.merge(xgmml, {});
            } else {
                this.graphData.load(xgmml, {});
            }
            if (!this._skipRender) {
                this.rebuild(merge);
            }
        },

        format: function (labelTpl, obj) {
            var retVal = "";
            var lpos = labelTpl.indexOf("%");
            var rpos = -1;
            while (lpos >= 0) {
                retVal += labelTpl.substring(rpos + 1, lpos);
                rpos = labelTpl.indexOf("%", lpos + 1);
                if (rpos < 0) {
                    console.log("Invalid Label Template");
                    break;
                }
                var key = labelTpl.substring(lpos + 1, rpos);
                retVal += !key ? "%" : (obj[labelTpl.substring(lpos + 1, rpos)] || "");
                lpos = labelTpl.indexOf("%", rpos + 1);
            }
            retVal += labelTpl.substring(rpos + 1, labelTpl.length);
            return retVal.split("\\n").filter(function (line) {
                return !!line;
            }).join("\n");
        },

        rebuild: function (merge) {
            merge = merge || false;
            var vertices = [];
            var edges = [];
            var hierarchy = [];

            if (this.option("subgraph")) {
                arrayUtil.forEach(this.graphData.subgraphs, function (subgraph, idx) {
                    if (!merge || !subgraph.__widget) {
                        subgraph.__widget = new hpccGraph.Subgraph()
                            .title(subgraph.__hpcc_id)
                            ;
                        subgraph.__widget.__hpcc_globalID = subgraph.__hpcc_id;
                    }
                    vertices.push(subgraph.__widget);
                }, this);
            }
            var labelTpl = this.option("vlabel");
            var tooltipTpl = this.option("vtooltip");
            arrayUtil.forEach(this.graphData.vertices, function (item, idx) {
                if (!this.option("vhidespills") || !item.isSpill()) {
                    if (!merge || !item.__widget) {
                        switch (item._kind) {
                            case "point":
                                item.__widget = new hpccCommon.Shape()
                                    .radius(7)
                                    ;
                                break;
                            default:
                                if (this.option("vicon") && this.option("vlabel")) {
                                    item.__widget = new hpccGraph.Vertex()
                                        .faChar(faCharFactory(item._kind))
                                        ;
                                } else if (this.option("vicon")) {
                                    item.__widget = new hpccCommon.Icon()
                                        .faChar(faCharFactory(item._kind))
                                        ;
                                } else if (this.option("vlabel")) {
                                    item.__widget = new hpccCommon.TextBox()
                                        ;
                                } else {
                                    item.__widget = new hpccCommon.Shape()
                                        .radius(7)
                                        ;
                                }
                                break;
                        }
                        item.__widget.__hpcc_globalID = item.__hpcc_id;
                    }
                    if (item.__widget.text) {
                        var label = this.format(labelTpl, item);
                        item.__widget.text(label);
                    }
                    if (item.__widget.tooltip) {
                        var tooltip = this.format(tooltipTpl, item);
                        item.__widget.tooltip(tooltip);
                    }
                    vertices.push(item.__widget);
                }
            }, this);
            labelTpl = this.option("elabel");
            tooltipTpl = this.option("etooltip");
            arrayUtil.forEach(this.graphData.edges, function (item, idx) {
                var source = item.getSource();
                var target = item.getTarget();
                if (source && target && (!this.option("vhidespills") || !target.isSpill())) {
                    var label = this.format(labelTpl, item);
                    var tooltip = this.format(tooltipTpl, item);
                    var numSlaves = parseInt(item.NumSlaves);
                    var numStarts = parseInt(item.NumStarts);
                    var numStops = parseInt(item.NumStops);
                    var started = numStarts > 0;
                    var finished = numStops === numSlaves;
                    var active = started && !finished;

                    var strokeDasharray = null;
                    var weight = 100;
                    if (item._dependsOn) {
                        weight = 10;
                        strokeDasharray = "1,5";
                    } else if (item._childGraph) {
                        strokeDasharray = "5,5";
                    } else if (item._isSpill) {
                        weight = 25;
                        strokeDasharray = "5,5,10,5";
                    }
                    if (this.option("vhidespills") && source.isSpill()) {
                        label += "\n(" + nlsHPCC.Spill + ")";
                        weight = 25;
                        strokeDasharray = "5,5,10,5";
                        while (source && source.isSpill()) {
                            var inputs = source.getInVertices();
                            source = inputs[0];
                        }
                    }
                    if (source) {
                        if (!merge || !item.__widget) {
                            item.__widget = new hpccGraph.Edge()
                                .sourceVertex(source.__widget)
                                .targetVertex(target.__widget)
                                .targetMarker("arrow")
                                .weight(weight)
                                .strokeDasharray(strokeDasharray)
                                ;
                            item.__widget.__hpcc_globalID = item.__hpcc_id;
                        }
                        item.__widget.text(label);
                        item.__widget.tooltip(tooltip);
                        item.__widget.classed({
                            started: started && !finished && !active,
                            finished: finished && !active,
                            active: active
                        });
                        edges.push(item.__widget);
                    }
                }
            }, this);
            if (this.option("subgraph")) {
                arrayUtil.forEach(this.graphData.subgraphs, function (subgraph, idx) {
                    arrayUtil.forEach(subgraph.__hpcc_subgraphs, function (item, idx) {
                        if (subgraph.__widget && item.__widget) {
                            hierarchy.push({ parent: subgraph.__widget, child: item.__widget });
                        }
                    }, this);
                    arrayUtil.forEach(subgraph.__hpcc_vertices, function (item, idx) {
                        if (subgraph.__widget && item.__widget) {
                            hierarchy.push({ parent: subgraph.__widget, child: item.__widget });
                        }
                    }, this);
                }, this);
            }
            this.graphWidget.data({ vertices: vertices, edges: edges, hierarchy: hierarchy, merge: merge });
        }
    });

    return declare("JSGraphWidget", [GraphWidget], {
        baseClass: "JSGraphWidget",
        constructor: function () {
            this.graphData = new ESPGraph.Graph();
        },

        hasOptions: function (key, val) {
            return this.hasPlugin();
        },

        _onOptionsApply: function () {
            var optionsValues = this.optionsForm.getValues();
            this.persist.setObj("options", optionsValues);
            this.optionsDropDown.closeDropDown();
            this._plugin.optionsReset(optionsValues);
            this.refreshRootState();
            delete this.xgmml;
            this._onRefreshScope();
        },

        _onOptionsReset: function () {
            this.optionsForm.setValues(this._plugin._optionsDefault);
            this._plugin.optionsReset(this._plugin._optionsDefault);
        },

        option: function (key, val) {
            return this._plugin.option.apply(this._plugin, arguments);
        },

        resize: function (size) {
            this.inherited(arguments);
            if (this.hasPlugin()) {
                this._plugin.layout
                    .resize()
                    .render()
                    ;
            }
        },

        createPlugin: function () {
            if (!this.hasPlugin()) {
                this.persist = new Utility.Persist(this._persistID || "");
                var context = this;
                context._plugin = new JSPlugin(context.graphContentPane.domNode);
                context._plugin._optionsDefault = context.optionsForm.getValues();
                switch (context._persistID) {
                    case "overview":
                        context._plugin._optionsDefault.subgraph = ["on"];
                        context._plugin._optionsDefault.vlabel = "";
                        break;
                    case "local":
                        context._plugin._optionsDefault.subgraph = ["on"];
                        context._plugin._optionsDefault.vhidespills = ["off"];
                        break;
                    default:
                        context._plugin._optionsDefault.vhidespills = ["on"];
                        break;
                }
                var optionsValues = lang.mixin({}, context._plugin._optionsDefault, context.persist.getObj("options"));
                context._plugin.optionsReset(optionsValues);
                context.optionsForm.setValues(optionsValues);
                context.version = {
                    major: 6,
                    minor: 0
                };
                context.registerEvents();
                context.refreshRootState();
                context.emit("ready");
            }
        },

        watchSplitter: function (splitter) {
        },

        watchSelect: function (select) {
        }
    });
});
