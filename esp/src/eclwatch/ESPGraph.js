﻿/*##############################################################################
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
    "dojo/_base/array",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",

    "dojox/xml/parser",

    "hpcc/Utility"

], function (declare, arrayUtil, i18n, nlsHPCC,
    parser,
    Utility) {

    var i18n = nlsHPCC;

    var GRAPH_TYPE = {
        UNKNOWN: 0,
        GRAPH: 1,
        SUBGRAPH: 2,
        VERTEX: 3,
        EDGE: 4,
        LAST: 5
    };
    var GRAPH_TYPE_STRING = {
        UNKNOWN: "Unknown",
        GRAPH: "Graph",
        SUBGRAPH: "Cluster",
        VERTEX: "Vertex",
        EDGE: "Edge",
        LAST: "Last"
    };

    var LocalisedXGMMLWriter = declare([], {
        constructor: function (graph) {
            this.graph = graph;

            this.m_xgmml = "";
            this.m_visibleSubgraphs = {};
            this.m_visibleVertices = {};
            this.m_semiVisibleVertices = {};
            this.m_visibleEdges = {};
        },

        calcVisibility: function (items, localisationDepth, localisationDistance, noSpills) {
            this.noSpills = noSpills;
            arrayUtil.forEach(items, function (item) {
                switch (this.graph.getGlobalType(item)) {
                    case GRAPH_TYPE.VERTEX:
                        this.calcInVertexVisibility(item, localisationDistance);
                        this.calcOutVertexVisibility(item, localisationDistance);
                        break;
                    case GRAPH_TYPE.EDGE:
                        this.calcInVertexVisibility(item.getSource(), localisationDistance - 1);
                        this.calcOutVertexVisibility(item.getTarget(), localisationDistance - 1);
                        break;
                    case GRAPH_TYPE.SUBGRAPH:
                        this.m_visibleSubgraphs[item.__hpcc_id] = item;
                        this.calcSubgraphVisibility(item, localisationDepth - 1);
                        break;
                }
            }, this);
            this.calcVisibility2();
        },

        calcInVertexVisibility: function (vertex, localisationDistance) {
            if (this.noSpills && vertex.isSpill()) {
                localisationDistance++;
            }
            this.m_visibleVertices[vertex.__hpcc_id] = vertex;
            if (localisationDistance > 0) {
                arrayUtil.forEach(vertex.getInEdges(), function (edge, idx) {
                    this.calcInVertexVisibility(edge.getSource(), localisationDistance - 1);
                }, this);
            }
        },

        calcOutVertexVisibility: function (vertex, localisationDistance) {
            if (this.noSpills && vertex.isSpill()) {
                localisationDistance++;
            }
            this.m_visibleVertices[vertex.__hpcc_id] = vertex;
            if (localisationDistance > 0) {
                arrayUtil.forEach(vertex.getOutEdges(), function (edge, idx) {
                    this.calcOutVertexVisibility(edge.getTarget(), localisationDistance - 1);
                }, this);
            }
        },

        calcSubgraphVisibility: function (subgraph, localisationDepth) {
            if (localisationDepth < 0) {
                return;
            }

            if (localisationDepth > 0) {
                arrayUtil.forEach(subgraph.__hpcc_subgraphs, function (subgraph, idx) {
                    this.calcSubgraphVisibility(subgraph, localisationDepth - 1);
                }, this);
            }

            arrayUtil.forEach(subgraph.__hpcc_subgraphs, function (subgraph, idx) {
                this.m_visibleSubgraphs[subgraph.__hpcc_id] = subgraph;
            }, this);
            arrayUtil.forEach(subgraph.__hpcc_vertices, function (vertex, idx) {
                this.m_visibleVertices[vertex.__hpcc_id] = vertex;
            }, this);

            //  Calculate edges that pass through the subgraph  ---
            var dedupEdges = {};
            arrayUtil.forEach(this.graph.edges, function (edge, idx) {
                if (edge.getSource().__hpcc_parent !== edge.getTarget().__hpcc_parent && subgraph === this.getCommonAncestor(edge)) {
                    //  Only include one unique edge between subgraphs  ---
                    if (!dedupEdges[edge.getSource().__hpcc_parent.__hpcc_id + "::" + edge.getTarget().__hpcc_parent.__hpcc_id]) {
                        dedupEdges[edge.getSource().__hpcc_parent.__hpcc_id + "::" + edge.getTarget().__hpcc_parent.__hpcc_id] = true;
                        this.m_visibleEdges[edge.__hpcc_id] = edge;
                    }
                }
            }, this);
        },

        buildVertexString: function (vertex, isPoint) {
            var attrStr = "";
            var propsStr = "";
            var props = vertex.getProperties();
            for (var key in props) {
                if (isPoint && key.indexOf("_kind") >= 0) {
                    propsStr += "<att name=\"_kind\" value=\"point\"/>";
                } else if (key === "id" || key === "label") {
                    attrStr += " " + key + "=\"" + Utility.xmlEncode(props[key]) + "\"";
                } else {
                    propsStr += "<att name=\"" + key + "\" value=\"" + Utility.xmlEncode(props[key]) + "\"/>";
                }
            }
            return "<node" + attrStr + ">" + propsStr + "</node>";
        },

        buildEdgeString: function (edge) {
            var attrStr = "";
            var propsStr = "";
            var props = edge.getProperties();
            for (var key in props) {
                if (key.toLowerCase() === "id" ||
                    key.toLowerCase() === "label" ||
                    key.toLowerCase() === "source" ||
                    key.toLowerCase() === "target") {
                    attrStr += " " + key + "=\"" + Utility.xmlEncode(props[key]) + "\"";
                } else {
                    propsStr += "<att name=\"" + key + "\" value=\"" + Utility.xmlEncode(props[key]) + "\"/>";
                }
            }
            return "<edge" + attrStr + ">" + propsStr + "</edge>";
        },

        getAncestors: function (v, ancestors) {
            var parent = v.__hpcc_parent;
            while (parent) {
                ancestors.push(parent);
                parent = parent.__hpcc_parent;
            }
        },

        getCommonAncestorV: function (v1, v2) {
            var v1_ancestors = [];
            var v2_ancestors = [];
            this.getAncestors(v1, v1_ancestors);
            this.getAncestors(v2, v2_ancestors);
            var finger1 = v1_ancestors.length - 1;
            var finger2 = v2_ancestors.length - 1;
            var retVal = null;
            while (finger1 >= 0 && finger2 >= 0 && v1_ancestors[finger1] === v2_ancestors[finger2]) {
                retVal = v1_ancestors[finger1];
                --finger1;
                --finger2;
            }
            return retVal;
        },

        getCommonAncestor: function (e) {
            return this.getCommonAncestorV(e.getSource(), e.getTarget());
        },

        calcAncestorVisibility: function (vertex) {
            var ancestors = [];
            this.getAncestors(vertex, ancestors);
            arrayUtil.forEach(ancestors, function (item, idx) {
                this.m_visibleSubgraphs[item.__hpcc_id] = item;
            }, this);
        },

        calcVisibility2: function () {
            for (var key in this.m_visibleVertices) {
                var vertex = this.m_visibleVertices[key];
                arrayUtil.forEach(vertex.getInEdges(), function (edge, idx) {
                    this.m_visibleEdges[edge.__hpcc_id] = edge;
                }, this);
                arrayUtil.forEach(vertex.getOutEdges(), function (edge, idx) {
                    this.m_visibleEdges[edge.__hpcc_id] = edge;
                }, this);
                this.calcAncestorVisibility(vertex);
            }
            this.calcSemiVisibleVertices();
        },

        addSemiVisibleEdge: function (edge) {
            if (!this.m_visibleEdges[edge.__hpcc_id]) {
                this.m_visibleEdges[edge.__hpcc_id] = edge;
            }
        },

        addSemiVisibleVertex: function (vertex) {
            if (!this.m_visibleVertices[vertex.__hpcc_id]) {
                this.m_semiVisibleVertices[vertex.__hpcc_id] = vertex;
                this.calcAncestorVisibility(vertex);
            }
        },

        calcSemiVisibleVertices: function () {
            for (var key in this.m_visibleEdges) {
                var edge = this.m_visibleEdges[key];
                var source = edge.getSource();
                this.addSemiVisibleVertex(source);
                while (this.noSpills && source.isSpill()) {
                    var inEdges = source.getInEdges();
                    this.addSemiVisibleEdge(inEdges[0]);
                    source = inEdges[0].getSource();
                    this.addSemiVisibleVertex(source);
                }
                var target = edge.getTarget();
                this.addSemiVisibleVertex(target);
                while (this.noSpills && target.isSpill()) {
                    var outEdges = target.getOutEdges();
                    this.addSemiVisibleEdge(outEdges[0]);
                    target = outEdges[0].getTarget();
                    this.addSemiVisibleVertex(target);
                }
            }
        },

        writeXgmml: function () {
            this.subgraphVisited(this.graph.subgraphs[0], true);
            arrayUtil.forEach(this.graph.edges, function (edge, idx) {
                this.edgeVisited(edge);
            }, this);
        },

        subgraphVisited: function (subgraph, root) {
            if (this.m_visibleSubgraphs[subgraph.__hpcc_id]) {
                var propsStr = "";
                this.m_xgmml += root ? "" : "<node id=\"" + subgraph.__hpcc_id + "\"><att><graph>";
                var xgmmlLen = this.m_xgmml.length;
                subgraph.walkSubgraphs(this);
                subgraph.walkVertices(this);
                if (xgmmlLen === this.m_xgmml.length) {
                    //  Add at least one child otherwise subgraphs will render as a vertex  ---
                    var vertex = subgraph.__hpcc_vertices[0];
                    if (vertex) {
                        this.m_xgmml += this.buildVertexString(vertex, true);
                    }
                }

                var props = subgraph.getProperties();
                for (var key in props) {
                    propsStr += "<att name=\"" + key + "\" value=\"" + Utility.xmlEncode(props[key]) + "\"/>";
                }
                this.m_xgmml += root ? "" : "</graph></att>" + propsStr + "</node>";
            }
            return false;
        },

        vertexVisited: function (vertex) {
            if (this.m_visibleVertices[vertex.__hpcc_id]) {
                this.m_xgmml += this.buildVertexString(vertex, false);
            }
            else if (this.m_semiVisibleVertices[vertex.__hpcc_id]) {
                this.m_xgmml += this.buildVertexString(vertex, true);
            }
        },

        edgeVisited: function (edge) {
            if (this.m_visibleEdges[edge.__hpcc_id]) {
                this.m_xgmml += this.buildEdgeString(edge);
            }
        }
    });

    var GraphItem = declare([], {
        constructor: function (graph, id) {
            this.__hpcc_graph = graph;
            this.__hpcc_id = id;
            this._globalID = id;
        },
        getProperties: function () {
            var retVal = {};
            for (var key in this) {
                if (key.indexOf("__") !== 0 && this.hasOwnProperty(key)) {
                    retVal[key] = this[key];
                }
            }
            return retVal;
        }
    });

    var Subgraph = declare([GraphItem], {
        constructor: function (graph, id) {
            this._globalType = id === "0" ? "Graph" : "Cluster";
            this.__hpcc_subgraphs = [];
            this.__hpcc_vertices = [];
            this.__hpcc_edges = [];
            this.id = id;
        },

        addSubgraph: function (subgraph) {
            subgraph.__hpcc_parent = this;
            if (!arrayUtil.some(this.__hpcc_subgraphs, function (subgraph2) {
                return subgraph === subgraph2;
            })) {
                this.__hpcc_subgraphs.push(subgraph);
            }
        },

        addVertex: function (vertex) {
            vertex.__hpcc_parent = this;
            if (!arrayUtil.some(this.__hpcc_vertices, function (vertex2) {
                return vertex === vertex2;
            })) {
                this.__hpcc_vertices.push(vertex);
            }
        },

        removeVertex: function (vertex) {
            this.__hpcc_vertices = arrayUtil.filter(this.__hpcc_vertices, function (vertex2) {
                return vertex !== vertex2;
            }, this);
        },

        addEdge: function (edge) {
            edge.__hpcc_parent = this;
            if (!arrayUtil.some(this.__hpcc_edges, function (edge2) {
                return edge === edge2;
            })) {
                this.__hpcc_edges.push(edge);
            }
        },

        removeEdge: function (edge) {
            this.__hpcc_edges = arrayUtil.filter(this.__hpcc_edges, function (edge2) {
                return edge !== edge2;
            }, this);
        },

        remove: function () {
            arrayUtil.forEach(this.__hpcc_subgraphs, function (subgraph) {
                subgraph.__hpcc_parent = this.__hpcc_parent;
            }, this);
            arrayUtil.forEach(this.__hpcc_vertices, function (vertex) {
                vertex.__hpcc_parent = this.__hpcc_parent;
            }, this);
            arrayUtil.forEach(this.__hpcc_edges, function (edge) {
                edge.__hpcc_parent = this.__hpcc_parent;
            }, this);
            delete this.__hpcc_parent;
            this.__hpcc_graph.removeItem(this);
        },

        walkSubgraphs: function (visitor) {
            arrayUtil.forEach(this.__hpcc_subgraphs, function (subgraph, idx) {
                if (visitor.subgraphVisited(subgraph)) {
                    subgraph.walkSubgraphs(visitor);
                }
            }, this);
        },

        walkVertices: function (visitor) {
            arrayUtil.forEach(this.__hpcc_vertices, function (vertex, idx) {
                visitor.vertexVisited(vertex);
            }, this);
        }
    });

    var Vertex = declare([GraphItem], {
        constructor: function () {
            this._globalType = "Vertex";
        },

        isSpill: function () {
            return this._isSpill;
        },

        remove: function () {
            var inVertices = this.getInVertices();
            if (inVertices.length <= 1) {
                console.log(this.__hpcc_id + ":  remove only supports single or zero inputs activities...");
            }
            arrayUtil.forEach(this.getInEdges(), function (edge) {
                edge.remove();
            }, this);
            arrayUtil.forEach(this.getOutEdges(), function (edge) {
                edge.setSource(inVertices[0]);
            }, this);
            arrayUtil.forEach(this.subgraphs, function (subgraph) {
                subgraph.removeVertex(item);
            }, this);
            this.__hpcc_graph.removeItem(this);
        },

        getInVertices: function () {
            return arrayUtil.map(this.getInEdges(), function (edge) {
                return edge.getSource();
            }, this);
        },

        getInEdges: function () {
            return arrayUtil.filter(this.__hpcc_graph.edges, function (edge) {
                return edge.getTarget() === this;
            }, this);
        },

        getOutVertices: function () {
            return arrayUtil.map(this.getOutEdges(), function (edge) {
                return edge.getTarget();
            }, this);
        },

        getOutEdges: function () {
            return arrayUtil.filter(this.__hpcc_graph.edges, function (edge) {
                return edge.getSource() === this;
            }, this);
        }
    });

    var Edge = declare([GraphItem], {
        constructor: function (graph, id) {
            this._globalType = "Edge";
        },

        remove: function () {
            arrayUtil.forEach(this.__hpcc_graph.subgraphs, function (subgraph) {
                subgraph.removeEdge(this);
            }, this);
            this.__hpcc_graph.removeItem(this);
        },

        getSource: function () {
            return this.__hpcc_graph.idx[this._sourceActivity || this.source];
        },

        setSource: function (source) {
            if (this._sourceActivity) {
                this._sourceActivity = source.__hpcc_id;
            } else if (this.source) {
                this.source = source.__hpcc_id;
            }
            if (this.__widget) {
                this.__widget.setSource(this.getSource().__widget);
            }
        },

        getTarget: function () {
            return this.__hpcc_graph.idx[this._targetActivity || this.target];
        }
    });

    var Graph = declare([], {
        constructor: function () {
            this.clear();
        },

        clear: function () {
            this.xgmml = "";

            this.idx = {};
            this.subgraphs = [];
            this.vertices = [];
            this.edges = [];
        },

        load: function (xgmml) {
            this.clear();
            this.merge(xgmml);
        },

        merge: function (xgmml) {
            this.xgmml = xgmml;
            var dom = parser.parse(xgmml);
            this.walkDocument(dom.documentElement, "0");
        },

        getGlobalType: function (item) {
            if (item instanceof Vertex) {
                return GRAPH_TYPE.VERTEX;
            } else if (item instanceof Edge) {
                return GRAPH_TYPE.EDGE;
            } else if (item instanceof Subgraph) {
                return GRAPH_TYPE.SUBGRAPH;
            } else if (item instanceof Graph) {
                return GRAPH_TYPE.GRAPH;
            }
            return GRAPH_TYPE.UNKNOWN;
        },

        getGlobalTypeString: function (item) {
            if (item instanceof Vertex) {
                return GRAPH_TYPE_STRING.VERTEX;
            } else if (item instanceof Edge) {
                return GRAPH_TYPE_STRING.EDGE;
            } else if (item instanceof Subgraph) {
                return GRAPH_TYPE_STRING.SUBGRAPH;
            } else if (item instanceof Graph) {
                return GRAPH_TYPE_STRING.GRAPH;
            }
            return GRAPH_TYPE_STRING.UNKNOWN;
        },

        getItem: function (docNode, id) {
            if (!this.idx[id]) {
                switch (docNode.tagName) {
                    case "graph":
                        var subgraph = new Subgraph(this, id);
                        this.subgraphs.push(subgraph);
                        this.idx[id] = subgraph;
                        break;
                    case "node":
                        var vertex = new Vertex(this, id);
                        this.vertices.push(vertex);
                        this.idx[id] = vertex;
                        break;
                    case "edge":
                        var edge = new Edge(this, id);
                        this.edges.push(edge);
                        this.idx[id] = edge;
                        break;
                    default:
                        console.log("Graph.getItem - Unknown Node Type!");
                        break;
                }
            }
            var retVal = this.idx[id];
            arrayUtil.forEach(docNode.attributes, function (attr, idx) {
                retVal[attr.name] = attr.value;
            }, this);
            return retVal;
        },

        removeItem: function (item) {
            delete this.idx[item.__hpcc_id];
            if (item instanceof Subgraph) {
                this.subgraphs = arrayUtil.filter(this.subgraphs, function (subgraph) {
                    return item !== subgraph;
                }, this);
            } else if (item instanceof Vertex) {
                this.vertices = arrayUtil.filter(this.vertices, function (vertex) {
                    return item !== vertex;
                }, this);
            } else if (item instanceof Edge) {
                this.edges = arrayUtil.filter(this.edges, function(edge) {
                    return item !== edge;
                }, this);
            }
        },

        getChildByTagName: function (docNode, tagName) {
            var retVal = null;
            arrayUtil.some(docNode.childNodes, function (childNode, idx) {
                if (childNode.tagName === tagName) {
                    retVal = childNode;
                    return true;
                }
            }, this);
            return retVal;
        },

        walkDocument: function (docNode, id) {
            var retVal = this.getItem(docNode, id);
            arrayUtil.forEach(docNode.childNodes, function (childNode, idx) {
                switch (childNode.nodeType) {
                    case 1:     //	ELEMENT_NODE
                        switch (childNode.tagName) {
                            case "graph":
                                break;
                            case "node":
                                var isSubgraph = false;
                                var attNode = this.getChildByTagName(childNode, "att");
                                if (attNode) {
                                    var graphNode = this.getChildByTagName(attNode, "graph");
                                    if (graphNode) {
                                        isSubgraph = true;
                                        var subgraph = this.walkDocument(graphNode, childNode.getAttribute("id"));
                                        retVal.addSubgraph(subgraph);
                                    }
                                }
                                if (!isSubgraph) {
                                    var vertex = this.walkDocument(childNode, childNode.getAttribute("id"));
                                    retVal.addVertex(vertex);
                                }
                                break;
                            case "att":
                                var name = childNode.getAttribute("name");
                                var value = childNode.getAttribute("value");
                                if (name.indexOf("Time") === 0) {
                                    retVal["_" + name] = value;
                                    retVal[name] = "" + Utility.espTime2Seconds(value);
                                } else if (name.indexOf("Size") === 0) {
                                    retVal["_" + name] = value;
                                    retVal[name] = "" + Utility.espSize2Bytes(value);
                                } else if (name.indexOf("Skew") === 0) {
                                    retVal["_" + name] = value;
                                    retVal[name] = "" + Utility.espSkew2Number(value);
                                } else {
                                    retVal[name] = value;
                                }
                                break;
                            case "edge":
                                var edge = this.walkDocument(childNode, childNode.getAttribute("id"));
                                if (edge.NumRowsProcessed) {
                                    edge._eclwatchCount = edge.NumRowsProcessed.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                                }
                                if (edge.inputProgress) {
                                    edge._eclwatchInputProgress = "[" + edge.inputProgress.replace(/\B(?=(\d{3})+(?!\d))/g, ",") + "]";
                                }
                                if (edge.SkewMaxRowsProcessed && edge.SkewMinRowsProcessed) {
                                    edge._eclwatchSkew = "+" + edge.SkewMaxRowsProcessed + ", " + edge.SkewMinRowsProcessed;
                                }
                                if (edge._dependsOn) {
                                } else if (edge._childGraph) {
                                } else if (edge._sourceActivity || edge._targetActivity) {
                                    edge._isSpill = true;
                                    var source = edge.getSource();
                                    source._isSpill = true;
                                    var target = edge.getTarget();
                                    target._isSpill = true;
                                }
                                retVal.addEdge(edge);
                                break;
                            default:
                                break;
                        }
                        break;
                    case 2:     //	ATTRIBUTE_NODE
                    case 3:     //	TEXT_NODE
                    case 4:     //	CDATA_SECTION_NODE
                    case 5:     //	ENTITY_REFERENCE_NODE
                    case 6:     //	ENTITY_NODE
                    case 7:     //	PROCESSING_INSTRUCTION_NODE
                    case 8:     //	COMMENT_NODE
                    case 9:     //	DOCUMENT_NODE
                    case 10:    //	DOCUMENT_TYPE_NODE
                    case 11:    //	DOCUMENT_FRAGMENT_NODE
                    case 12:    //	NOTATION_NODE
                        break;
                    default:
                        break;
                }
            }, this);
            return retVal;
        },

        removeSubgraphs: function () {
            var subgraphs = arrayUtil.map(this.subgraphs, function (subgraph) { return subgraph; });
            arrayUtil.forEach(subgraphs, function (subgraph) {
                if (subgraph.__hpcc_parent instanceof Subgraph) {
                    subgraph.remove();
                }
            }, this);
        },

        removeSpillVertices: function () {
            var vertices = arrayUtil.map(this.vertices, function (vertex) { return vertex; });
            arrayUtil.forEach(vertices, function (vertex) {
                if (vertex.isSpill()) {
                    vertex.remove();
                }
            }, this);
        },

        getLocalisedXGMML: function (items, localisationDepth, localisationDistance, noSpills) {
            var xgmmlWriter = new LocalisedXGMMLWriter(this);
            xgmmlWriter.calcVisibility(items, localisationDepth, localisationDistance, noSpills);
            xgmmlWriter.writeXgmml();
            return "<graph>" + xgmmlWriter.m_xgmml + "</graph>";
        }
    });

    return Graph;
});
