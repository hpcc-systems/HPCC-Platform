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
    "dojo/_base/sniff",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/on",
    "dojo/has",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/registry",
    "dijit/Dialog",

    "dojox/html/entities",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GraphWidget",
    "hpcc/ESPUtil",
    "hpcc/ESPWorkunit",
    "hpcc/TimingGridWidget",
    "hpcc/TimingTreeMapWidget",

    "dojo/text!../templates/GraphPageWidget.html",

    "dijit/PopupMenuItem",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/form/TextBox",
    "dijit/form/DropDownButton"
], function (declare, lang, sniff, arrayUtil, dom, domConstruct, on, has, Memory, Observable,
            _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, registry, Dialog,
            entities,
            OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
            GraphWidget, ESPUtil, ESPWorkunit, TimingGridWidget, TimingTreeMapWidget,
            template) {
    return declare("GraphPageWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "GraphPageWidget",
        borderContainer: null,
        rightBorderContainer: null,
        graphName: "",
        wu: null,
        editorControl: null,
        main: null,
        overview: null,
        local: null,
        timingGrid: null,
        timingTreeMap: null,
        subgraphsGrid: null,
        verticesGrid: null,
        edgesGrid: null,
        findField: null,
        findText: "",
        found: [],
        foundIndex: 0,
        initalized: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.rightBorderContainer = registry.byId(this.id + "RightBorderContainer");
            this.findField = registry.byId(this.id + "FindField");
            this._initGraphControls();
            this._initTimings();
        },

        startup: function (args) {
            this.inherited(arguments);

            this._initSubgraphs();
            this._initVertices();
            this._initEdges();

            var splitter = this.borderContainer.getSplitter("right");
            this.main.watchSplitter(splitter);
            this.overview.watchSplitter(splitter);
            this.local.watchSplitter(splitter);

            splitter = this.rightBorderContainer.getSplitter("bottom");
            this.main.watchSplitter(splitter);
            this.overview.watchSplitter(splitter);
            this.local.watchSplitter(splitter);

            this.main.watchSelect(registry.byId(this.id + "AdvancedMenu"));
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        //  Implementation  ---
        _initGraphControls: function () {
            var context = this;
            this.main = registry.byId(this.id + "MainGraphWidget");
            this.main.onSelectionChanged = function (items) {
                context.syncSelectionFrom(context.main);
            };
            this.main.onLayoutFinished = function () {
                //  TODO:  Could be too expensive  ---
                //context.wu.setGraphSvg(context.graphName, context.main.svg);
            };

            this.overview = registry.byId(this.id + "MiniGraphWidget");
            this.overview.onSelectionChanged = function (items) {
                context.syncSelectionFrom(context.overview);
            };
            this.overview.onDoubleClick = function (globalID) {
                var mainItem = context.main.getItem(globalID);
                context.main.centerOnItem(mainItem, true);
            };

            this.local = registry.byId(this.id + "LocalGraphWidget");
            this.local.onSelectionChanged = function (items) {
                context.syncSelectionFrom(context.local);
            };
            this.local.onDoubleClick = function (globalID) {
                var mainItem = context.main.getItem(globalID);
                context.main.centerOnItem(mainItem, true);
            };
        },

        _initTimings: function () {
            this.timingGrid = registry.byId(this.id + "TimingsGrid");

            var context = this;
            this.timingGrid.onClick = function (items) {
                context.syncSelectionFrom(context.timingGrid);
            };

            this.timingGrid.onDblClick = function (item) {
                var subgraphID = item.SubGraphId;
                var mainItem = context.main.getItem(subgraphID);
                context.main.centerOnItem(mainItem, true);
            };

            this.timingTreeMap = registry.byId(this.id + "TimingsTreeMap");
            this.timingTreeMap.onClick = function (value) {
                context.syncSelectionFrom(context.timingTreeMap);
            }
            this.timingTreeMap.onDblClick = function (value) {
                var mainItem = context.main.getItem(value.SubGraphId);
                context.main.centerOnItem(mainItem, true);
            }
        },

        _initItemGrid: function (grid) {
            var context = this;
            grid.on(".dgrid-row:click", function (evt) {
                context.syncSelectionFrom(grid);
            });
            grid.on(".dgrid-row:dblclick", function (evt) {
                var item = grid.row(evt).data;
                if (item._globalID) {
                    var mainItem = context.main.getItem(item._globalID);
                    context.main.centerOnItem(mainItem, true);
                }
            });
        },

        _initSubgraphs: function () {
            var store = new Memory({
                idProperty: "id",
                data: []
            });
            this.subgraphsStore = Observable(store);
            this.subgraphsGrid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                store: this.subgraphsStore
            }, this.id + "SubgraphsGrid");

            this._initItemGrid(this.subgraphsGrid);
        },

        _initVertices: function () {
            var store = new Memory({
                idProperty: "id",
                data: []
            });
            this.verticesStore = Observable(store);
            this.verticesGrid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                store: this.verticesStore
            }, this.id + "VerticesGrid");

            this._initItemGrid(this.verticesGrid);
        },

        _initEdges: function () {
            var store = new Memory({
                idProperty: "id",
                data: []
            });
            this.edgesStore = Observable(store);
            this.edgesGrid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                store: this.edgesStore
            }, this.id + "EdgesGrid");

            this._initItemGrid(this.edgesGrid);
        },

        _onLayout: function () {
            this.main.setMessage("Performing Layout...");
            this.main.startLayout("dot");
        },

        _onLocalSync: function () {
            this.syncSelectionFrom(this.main);
        },

        _doFind: function (prev) {
            if (this.findText != this.findField.value) {
                this.findText = this.findField.value;
                this.found = this.main.find(this.findText);
                this.main.setSelected(this.found);
                this.syncSelectionFrom(this.main);
                this.foundIndex = -1;
            }
            this.foundIndex += prev ? -1 : +1;
            if (this.foundIndex < 0) {
                this.foundIndex = this.found.length - 1;
            } else if (this.foundIndex >= this.found.length) {
                this.foundIndex = 0;
            }
            if (this.found.length) {
                this.main.centerOnItem(this.found[this.foundIndex], true);
            }
        },

        _onFind: function (prev) {
            this.findText = "";
            this._doFind(false);
        },

        _onFindNext: function () {
            this._doFind(false);
        },

        _onFindPrevious: function () {
            this._doFind(true);
        },

        _showDialog: function(params) {
            myDialog = new Dialog(params);
            if (has("chrome")) {
                this.main.hide();
                this.overview.hide();
                this.local.hide();

                var context = this;
                myDialog.connect(myDialog, "hide", function (e) {
                    context.main.show();
                    context.overview.show();
                    context.local.show();
                });
            }
            myDialog.show();
        },

        _onAbout: function () {
            this._showDialog({
                title: "About HPCC Systems Graph Control",
                content: "Version:  " + this.main.getVersion() + "<br/>" + this.main.getResourceLinks(),
                style: "width: 320px"
            });
        },

        _onGetSVG: function () {
            this._showDialog({
                title: "SVG Source",
                content: entities.encode(this.main.getSVG())
            });
        },

        _onRenderSVG: function () {
            var context = this
            this.main.localLayout(function (svg) {
                context._showDialog({
                    title: "Rendered SVG",
                    content: svg
                });
            });
        },

        _onGetXGMML: function () {
            this._showDialog({
                title: "XGMML",
                content: entities.encode(this.main.getXGMML())
            });
        },

        init: function (params) {
            if (this.initalized) {
                return;
            }
            this.initalized = true;
            this.graphName = params.GraphName;
            this.wu = ESPWorkunit.Get(params.Wuid);

            var firstLoad = true;
            var context = this;
            this.wu.monitor(function () {
                context.wu.getInfo({
                    onGetApplicationValues: function (applicationValues) {
                    },
                    onGetGraphs: function (graphs) {
                        if (firstLoad == true) {
                            firstLoad = false;
                            context.loadGraph(context.wu, context.graphName);
                        } else {
                            context.refreshGraph(context.wu, context.graphName);
                        }
                    }
                });
            });

            this.timingGrid.init(lang.mixin({
                query: this.graphName
            }, params));

            this.timingTreeMap.init(lang.mixin({
                query: this.graphName
            }, params));

        },

        loadGraph: function (wu, graphName) {
            var context = this;
            wu.fetchGraphXgmmlByName(graphName, function (xgmml, svg) {
                context.main.setMessage("Loading Data...");
                context.main.loadXGMML(xgmml);
                context.overview.loadXGMML(context.main.getLocalisedXGMML([0]));
                context.loadSubgraphs();
                context.loadVertices();
                context.loadEdges();
                if (svg) {
                    context.main.setMessage("Loading Layout...");
                    if (context.main.mergeSVG(svg)) {
                        context.main.centerOnItem(0, true);
                        context.main.setMessage("");
                        return;
                    }
                }
                context.main.setMessage("Performing Layout...");
                context.main.startLayout("dot");
            });
        },

        refreshGraph: function (wu, graphName) {
            var context = this;
            wu.fetchGraphXgmmlByName(graphName, function (xgmml) {
                context.main.mergeXGMML(xgmml);
                context.loadSubgraphs();
                context.loadVertices();
                context.loadEdges();
            });
        },

        loadSubgraphs: function () {
            var subgraphs = this.main.getSubgraphsWithProperties();

            var layoutMap = [];
            for (var i = 0; i < subgraphs.length; ++i) {
                for (var key in subgraphs[i]) {
                    if (key != "id" && key.substring(0, 1) != "_") {
                        layoutMap[key] = true;
                    }
                }
            }

            var layout = [
                { label: "ID", field: "id", width: 50 }
            ];

            for (var key in layoutMap) {
                layout.push({ label: key, field: key, width: 100 });
            }

            this.subgraphsStore.setData(subgraphs);
            this.subgraphsGrid.set("columns", layout);
            this.subgraphsGrid.refresh();
        },

        loadVertices: function () {
            var vertices = this.main.getVerticesWithProperties();

            var layoutMap = [];
            for (var i = 0; i < vertices.length; ++i) {
                for (var key in vertices[i]) {
                    if (key != "id" && key != "ecl" && key != "label" && key.substring(0, 1) != "_") {
                        layoutMap[key] = true;
                    }
                }
            }

            var layout = [
                { label: "ID", field: "id", width: 50 },
                { label: "Label", field: "label", width: 150 }
            ];

            for (var key in layoutMap) {
                layout.push({ label: key, field: key, width: 200 });
            }
            layout.push({ label: "ECL", field: "ecl", width: 1024 });

            this.verticesStore.setData(vertices);
            this.verticesGrid.set("columns", layout);
            this.verticesGrid.refresh();
        },

        loadEdges: function () {
            var edges = this.main.getEdgesWithProperties();

            var layoutMap = [];
            for (var i = 0; i < edges.length; ++i) {
                for (var key in edges[i]) {
                    if (key != "id" && key.substring(0, 1) != "_") {
                        layoutMap[key] = true;
                    }
                }
            }

            var layout = [
                { label: "ID", field: "id", width: 50 }
            ];

            for (var key in layoutMap) {
                layout.push({ label: key, field: key, width: 100 });
            }

            this.edgesStore.setData(edges);
            this.edgesGrid.set("columns", layout);
            this.edgesGrid.refresh();
        },

        syncSelectionFrom: function (sourceControl) {
            var selectedGlobalIDs = [];

            //  Get Selected Items  ---
            if (sourceControl == this.timingGrid || sourceControl == this.timingTreeMap) {
                var items = sourceControl.getSelected();
                for (var i = 0; i < items.length; ++i) {
                    if (items[i].SubGraphId) {
                        selectedGlobalIDs.push(items[i].SubGraphId);
                    }
                }
            } else if (sourceControl == this.verticesGrid || sourceControl == this.edgesGrid || sourceControl == this.subgraphsGrid) {
                var items = sourceControl.getSelected();
                for (var i = 0; i < items.length; ++i) {
                    if (items[i]._globalID) {
                        selectedGlobalIDs.push(items[i]._globalID);
                    }
                }
            } else {
                selectedGlobalIDs = sourceControl.getSelectionAsGlobalID();
            }

            //  Set Selected Items  ---
            if (sourceControl != this.timingGrid) {
                this.timingGrid.setSelectedAsGlobalID(selectedGlobalIDs);
            }
            if (sourceControl != this.timingTreeMap) {
                this.timingTreeMap.setSelectedAsGlobalID(selectedGlobalIDs);
            }
            if (sourceControl != this.subgraphsGrid && this.subgraphsGrid.store) {
                this.subgraphsGrid.setSelection(selectedGlobalIDs);
            }
            if (sourceControl != this.verticesGrid && this.verticesGrid.store) {
                this.verticesGrid.setSelection(selectedGlobalIDs);
            }
            if (sourceControl != this.edgesGrid && this.edgesGrid.store) {
                this.edgesGrid.setSelection(selectedGlobalIDs);
            }
            if (sourceControl != this.main) {
                this.main.setSelectedAsGlobalID(selectedGlobalIDs);
            }
            if (sourceControl != this.overview) {
                this.overview.setSelectedAsGlobalID(selectedGlobalIDs);
            }

            var mainItems = [];
            for (var i = 0; i < selectedGlobalIDs.length; ++i) {
                mainItems.push(this.main.getItem(selectedGlobalIDs[i]));
            }

            if (sourceControl != this.local) {
                var xgmml = this.main.getLocalisedXGMML(mainItems, 2);
                this.local.loadXGMML(xgmml);
                this.local.setSelectedAsGlobalID(selectedGlobalIDs);
            }

            var propertiesDom = dom.byId(this.id + "Properties");
            propertiesDom.innerHTML = "";
            for (var i = 0; i < mainItems.length; ++i) {
                this.main.displayProperties(mainItems[i], propertiesDom);
            }
        },

        resetPage: function () {
            this.main.clear();
        },

        displayGraphs: function (graphs) {
            for (var i = 0; i < graphs.length; ++i) {
                this.wu.fetchGraphXgmml(i, function (xgmml) {
                    this.main.loadXGMML(xgmml, true);
                });
            }
        }
    });
});
