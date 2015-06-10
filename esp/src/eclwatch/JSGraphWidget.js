/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
    var loadJSPlugin = function (callback) {
        require(["src/hpcc-viz", "src/hpcc-viz-common", "src/hpcc-viz-graph"], function () {
            require(["src/common/Shape", "src/common/TextBox", "src/graph/Graph", "src/graph/Vertex", "src/graph/Edge"], function (Shape, TextBox, Graph, Vertex, Edge) {
                callback(declare([Evented], {
                    KeyState_None: 0,
                    KeyState_Shift: 1,
                    KeyState_Control: 2,
                    KeyState_Menu: 4,

                    constructor: function (domNode) {
                        this.graphData = new ESPGraph();
                        this.graphWidget = new Graph()
                            .target(domNode.id)
                            .allowDragging(false)
                            .render()
                        ;
                        var context = this;
                        this.graphWidget.vertex_click = function (item, event) {
                            context.emit("SelectionChanged", [item]);
                        }
                        this.graphWidget.vertex_dblclick = function (item, event) {
                            context.emit("MouseDoubleClick", item, (event.shiftKey ? context.KeyState_Shift : 0) + (event.ctrlKey ? context.KeyState_Control : 0) + (event.altKey ? context.KeyState_Menu : 0));
                        }
                    },

                    setMessage: function (msg) {
                    },

                    setScale: function (scale) {
                        this.graphWidget.zoom.scale(scale / 100);
                        this.graphWidget.applyZoom(this.graphWidget._transitionDuration);
                    },

                    centerOnItem: function (item, scaleToFit, widthOnly) {
                        if (item === 0) {
                            item = this.graphData.subgraphs[0];
                        }
                        var bounds = this.graphWidget.getBounds([item.__widget]);
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
                        return this.graphData.vertices.filter(function (item) {
                            return (item.label.toLowerCase().indexOf(findText.toLowerCase()) >= 0);
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

                    mergeXGMML: function (xgmml, timers) {
                        return this.loadXGMML(xgmml, true, timers)
                    },

                    loadXGMML: function (xgmml, merge, timers) {
                        var retVal = this.inherited(arguments);
                        if (merge) {
                            this.graphData.merge(xgmml);
                        } else {
                            this.graphData.load(xgmml);
                        }
                        var vertices = [];
                        var edges = [];
                        var hierarchy = [];

                        arrayUtil.forEach(this.graphData.subgraphs, function (subgraph, idx) {
                            if (!subgraph.__widget) {
                                subgraph.__widget = new Shape()
                                    .shape("rect")
                                    .width(0)
                                    .height(0)
                                ;
                                subgraph.__widget.__hpcc_globalID = subgraph.__hpcc_id;
                            }
                            vertices.push(subgraph.__widget);
                        }, this);
                        arrayUtil.forEach(this.graphData.vertices, function (item, idx) {
                            if (!item.__widget) {
                                switch (item._kind) {
                                    case "point":
                                        item.__widget = new Shape()
                                            .radius(3)
                                        ;
                                        break;
                                    default:
                                        item.__widget = new TextBox()
                                            .text(item.label)
                                        ;
                                        break;
                                }
                                item.__widget.__hpcc_globalID = item.__hpcc_id;
                            }
                            vertices.push(item.__widget);
                        }, this);
                        arrayUtil.forEach(this.graphData.edges, function (item, idx) {
                            if (!item.__widget) {
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

                                var label = item.label ? item.label : "";
                                if (item.count) {
                                    if (label) {
                                        label += "\n";
                                    }
                                    label += item.count.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                                }
                                if (item.inputProgress) {
                                    if (label) {
                                        label += "\n";
                                    }
                                    label += "[" + item.inputProgress.replace(/\B(?=(\d{3})+(?!\d))/g, ",") + "]";
                                }
                                if (item.maxskew && item.minskew) {
                                    if (label) {
                                        label += "\n";
                                    }
                                    label += "+" + item.maxskew + "%, -" + item.minskew + "%";
                                }
                                item.__widget = new Edge()
                                    .sourceVertex(item.getSource().__widget)
                                    .targetVertex(item.getTarget().__widget)
                                    .targetMarker("arrowHead")
                                    .weight(weight)
                                    .strokeDasharray(strokeDasharray)
                                    .text(label)
                                ;
                                item.__widget.__hpcc_globalID = item.__hpcc_id;
                            }
                            edges.push(item.__widget);
                        }, this);
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
                        this.graphWidget.data({ vertices: vertices, edges: edges, hierarchy: hierarchy, merge: merge });
                        return retVal;
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

        resize: function (size) {
            this.inherited(arguments);
            if (this.hasPlugin()) {
                this._plugin.graphWidget
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
