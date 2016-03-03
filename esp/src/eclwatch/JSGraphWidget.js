/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/Evented",

    "hpcc/GraphWidget",
    "hpcc/ESPGraph"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, Evented,
            GraphWidget, ESPGraph) {

    var persist = {
        remove: function (key) {
            if (typeof (Storage) !== "undefined") {
                localStorage.removeItem("JSGraphWidget_" + key);
            }
        },
        set: function (key, val) {
            if (typeof (Storage) !== "undefined") {
                localStorage.setItem("JSGraphWidget_" + key, val);
            }
        },
        setObj: function (key, val) {
            this.set(key, JSON.stringify(val));
        },
        get: function (key, defValue) {
            if (typeof (Storage) !== "undefined") {
                var retVal = localStorage.getItem("JSGraphWidget_" + key);
                return retVal === null ? defValue : retVal;
            }
            return "";
        },
        getObj: function (key, defVal) {
            try {
                return JSON.parse(this.get(key, defVal));
            } catch (e) {
                return {};
            }
        },
        exists: function (key) {
            if (typeof (Storage) !== "undefined") {
                var retVal = localStorage.getItem("JSGraphWidget_" + key);
                return retVal === null;
            }
            return false;
        }
    };

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

    var loadJSPlugin = function (callback) {
        require(["src/hpcc-viz", "src/hpcc-viz-common", "src/hpcc-viz-graph", "src/hpcc-viz-layout"], function () {
            require(["src/common/Shape", "src/common/Icon", "src/common/TextBox", "src/graph/Graph", "src/graph/Vertex", "src/graph/Edge", "src/layout/Layered"], function (Shape, Icon, TextBox, Graph, Vertex, Edge, Layered) {
                callback(declare([Evented], {
                    KeyState_None: 0,
                    KeyState_Shift: 1,
                    KeyState_Control: 2,
                    KeyState_Menu: 4,

                    constructor: function (domNode) {
                        this.graphData = new ESPGraph();
                        this.graphWidget = new Graph()
                            .allowDragging(false)
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
                        this.messageWidget = new TextBox()
                            .shape_colorFill("#006CCC")
                            .shape_colorStroke("#003666")
                            .text_colorFill("#FFFFFF")
                        ;
                        this.layout = new Layered()
                            .target(domNode.id)
                            .widgets([ this.messageWidget, this.graphWidget])
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
                            this.messageWidget.text(msg).render();
                            if ((msg && this.graphWidget.visible()) || (!msg && !this.graphWidget.visible())) {
                                this.graphWidget.visible(msg ? false : true).render();
                            }
                            this._prevMsg = msg;
                        }
                    },

                    setScale: function (scale) {
                        this.graphWidget.zoom.scale(scale / 100);
                        this.graphWidget.applyZoom(this.graphWidget._transitionDuration);
                    },

                    centerOnItem: function (item, scaleToFit, widthOnly) {
                        var bounds = item === 0 ? this.graphWidget.getVertexBounds() : this.graphWidget.getBounds([item.__widget]);
                        if (scaleToFit) {
                            if (widthOnly) {
                                bounds[0][1] = 0;
                                bounds[1][1] = 0;
                            }
                            this.graphWidget.shrinkToFit(bounds);
                        } else {
                            this.graphWidget.centerOn(bounds);
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
                        return this.graphData.vertices.filter(function (item) {
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
                        return this.graphData.subgraphs;
                    },

                    getVerticesWithProperties: function () {
                        return this.graphData.vertices;
                    },

                    getEdgesWithProperties: function () {
                        return this.graphData.edges;
                    },

                    getLocalisedXGMML: function (selectedItems, depth, distance) {
                        return this.graphData.getLocalisedXGMML(selectedItems, depth, distance);
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
                            this.graphData.merge(xgmml);
                        } else {
                            this.graphData.load(xgmml);
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
                        return retVal.split("\\n").join("\n");
                    },

                    rebuild: function (merge) {
                        merge = merge || false;
                        var vertices = [];
                        var edges = [];
                        var hierarchy = [];

                        if (this.option("subgraph")) {
                            arrayUtil.forEach(this.graphData.subgraphs, function (subgraph, idx) {
                                if (!merge || !subgraph.__widget) {
                                    subgraph.__widget = new Shape()
                                        .shape("rect")
                                        .width(0)
                                        .height(0)
                                    ;
                                    subgraph.__widget.__hpcc_globalID = subgraph.__hpcc_id;
                                }
                                vertices.push(subgraph.__widget);
                            }, this);
                        }
                        var labelTpl = this.option("vlabel");
                        var tooltipTpl = this.option("vtooltip");
                        arrayUtil.forEach(this.graphData.vertices, function (item, idx) {
                            if (!merge || !item.__widget) {
                                var label = this.format(labelTpl, item);
                                var tooltip = this.format(tooltipTpl, item);
                                switch (item._kind) {
                                    case "point":
                                        item.__widget = new Shape()
                                            .radius(7)
                                            .tooltip(label)
                                        ;
                                        break;
                                    default:
                                        if (this.option("vicon") && this.option("vlabel")) {
                                            item.__widget = new Vertex()
                                                .faChar(faCharFactory(item._kind))
                                                .text(label)
                                                .tooltip(tooltip)
                                            ;
                                        } else if (this.option("vicon")) {
                                            item.__widget = new Icon()
                                                .faChar(faCharFactory(item._kind))
                                                .tooltip(tooltip)
                                            ;
                                        } else if (this.option("vlabel")) {
                                            item.__widget = new TextBox()
                                                .text(label)
                                                .tooltip(tooltip)
                                            ;
                                        } else {
                                            item.__widget = new Shape()
                                                .radius(7)
                                                .tooltip(tooltip)
                                            ;
                                        }
                                        break;
                                }
                                item.__widget.__hpcc_globalID = item.__hpcc_id;
                            }
                            vertices.push(item.__widget);
                        }, this);
                        labelTpl = this.option("elabel");
                        tooltipTpl = this.option("etooltip");
                        arrayUtil.forEach(this.graphData.edges, function (item, idx) {
                            if (!merge || !item.__widget) {
                                var strokeDasharray = null;
                                var weight = 100;
                                if (item._dependsOn) {
                                    weight = 10;
                                    strokeDasharray = "1,5";
                                } else if (item._childGraph) {
                                    strokeDasharray = "5,5";
                                } else if (item._sourceActivity || item._targetActivity) {
                                    weight = 25;
                                    strokeDasharray = "5,5,10,5";
                                }

                                var label = this.format(labelTpl, item);
                                var tooltip = this.format(tooltipTpl, item);
                                item.__widget = new Edge()
                                    .sourceVertex(item.getSource().__widget)
                                    .targetVertex(item.getTarget().__widget)
                                    .targetMarker("arrowHead")
                                    .weight(weight)
                                    .strokeDasharray(strokeDasharray)
                                    .text(label)
                                    .tooltip(tooltip)
                                ;
                                item.__widget.__hpcc_globalID = item.__hpcc_id;
                            }
                            edges.push(item.__widget);
                        }, this);
                        if (this.option("subgraph")) {
                            arrayUtil.forEach(this.graphData.subgraphs, function (subgraph, idx) {
                                arrayUtil.forEach(subgraph.__hpcc_subgraphs, function (item, idx) {
                                    if (!subgraph.__widget || !item.__widget) {
                                        var d = 0;
                                    }
                                    hierarchy.push({ parent: subgraph.__widget, child: item.__widget });
                                }, this);
                                arrayUtil.forEach(subgraph.__hpcc_vertices, function (item, idx) {
                                    if (!subgraph.__widget || !item.__widget) {
                                        var d = 0;
                                    }
                                    hierarchy.push({ parent: subgraph.__widget, child: item.__widget });
                                }, this);
                            }, this);
                        }
                        this.graphWidget.data({ vertices: vertices, edges: edges, hierarchy: hierarchy, merge: merge });
                    }
                }));
            });
        });
    };

    return declare("JSGraphWidget", [GraphWidget], {
        baseClass: "JSGraphWidget",
        constructor: function () {
            this.graphData = new ESPGraph();
        },

        hasOptions: function(key, val) {
            return this.hasPlugin();
        },

        _onOptionsApply: function () {
            var optionsValues = this.optionsForm.getValues();
            persist.setObj("options", optionsValues);
            this.optionsDropDown.closeDropDown();
            this._plugin.optionsReset(optionsValues);
            this._plugin.rebuild();
            this._onClickRefresh();
        },

        _onOptionsReset: function () {
            this.optionsForm.setValues(this._plugin._optionsDefault);
            this._plugin.optionsReset(this._plugin._optionsDefault);
        },

        option: function(key, val) {
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
                var context = this;
                loadJSPlugin(function (JSPlugin) {
                    context._plugin = new JSPlugin(context.graphContentPane.domNode);
                    context._plugin._optionsDefault = context.optionsForm.getValues();
                    var optionsValues = lang.mixin({}, context._plugin._optionsDefault, persist.getObj("options"));
                    context._plugin.optionsReset(optionsValues);
                    context.optionsForm.setValues(optionsValues);
                    context.version = {
                        major: 6,
                        minor: 0
                    };
                    context.registerEvents();
                    context.emit("ready");
                });
            }
        },

        watchSplitter: function (splitter) {
        },

        watchSelect: function (select) {
        }
    });
});
