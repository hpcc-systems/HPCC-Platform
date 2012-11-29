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
    "dojo/data/ObjectStore",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/registry",
    "dijit/Dialog",

    "dojox/grid/DataGrid",

    "hpcc/GraphWidget",
    "hpcc/ESPWorkunit",
    "hpcc/TimingGridWidget",
    "hpcc/TimingTreeMapWidget",

    "dojo/text!../templates/GraphPageWidget.html",

    "dijit/form/TextBox"
], function (declare, lang, sniff, array, dom, domConstruct, on, has, Memory, ObjectStore,
            _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, registry, Dialog,
            DataGrid, GraphWidget, ESPWorkunit, TimingGridWidget, TimingTreeMapWidget,
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
            this._initControls();
        },

        startup: function (args) {
            this.inherited(arguments);

            var splitter = this.borderContainer.getSplitter("right");
            this.main.watchSplitter(splitter);
            this.overview.watchSplitter(splitter);
            this.local.watchSplitter(splitter);

            splitter = this.rightBorderContainer.getSplitter("bottom");
            this.main.watchSplitter(splitter);
            this.overview.watchSplitter(splitter);
            this.local.watchSplitter(splitter);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        //  Implementation  ---
        _initControls: function () {
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.rightBorderContainer = registry.byId(this.id + "RightBorderContainer");
            this.findField = registry.byId(this.id + "FindField");
            this._initGraphControls();
            this._initTimings();
            this._initVertices();
            this._initEdges();
        },

        _initGraphControls: function () {
            var context = this;
            this.main = registry.byId(this.id + "MainGraphWidget");
            this.main.onSelectionChanged = function (items) {
                context.syncSelectionFrom(context.main);
            };
            this.main.onLayoutFinished = function () {
                context.wu.setGraphSvg(context.graphName, context.main.svg);
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
            grid.on("RowClick", function (evt) {
                context.syncSelectionFrom(grid);
            }, true);

            var context = this;
            grid.on("RowDblClick", function (evt) {
                var idx = evt.rowIndex;
                var item = this.getItem(idx);
                if (this.store.getValue(item, "_globalID")) {
                    var globalID = this.store.getValue(item, "_globalID");
                    var mainItem = context.main.getItem(globalID);
                    context.main.centerOnItem(mainItem, true);
                }

            }, true);
        },

        _initVertices: function () {
            this.verticesGrid = registry.byId(this.id + "VerticesGrid");
            this._initItemGrid(this.verticesGrid);
        },

        _initEdges: function () {
            this.edgesGrid = registry.byId(this.id + "EdgesGrid");
            this._initItemGrid(this.edgesGrid);
        },

        _initProperties: function () {
            this.propertyGrid = new DataGrid({
                //store: objStore,
                query: { id: "*" },
                structure: [
                { name: "Property", field: "key", width: "auto" },
                { name: "Value", field: "value", width: "auto" }
                ]
            });
            this.propertyGrid.placeAt(this.id + "Properties");
            this.propertyGrid.startup();
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

        _onAbout: function () {
            myDialog = new Dialog({
                title: "About HPCC Systems Graph Control",
                content: "Version:  " + this.main.getVersion() + "<br>"
                + this.main.getResourceLinks(),
                style: "width: 320px"
            });
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

        init: function (params) {
            if (this.initalized) {
                return;
            }
            this.initalized = true;
            this.graphName = params.GraphName;
            this.wu = new ESPWorkunit({
                Wuid: params.Wuid
            });

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
                context.loadVertices();
                context.loadEdges();
            });
        },

        loadVertices: function () {
            var vertices = this.main.plugin.getVerticesWithProperties();

            var layoutMap = [];
            for (var i = 0; i < vertices.length; ++i) {
                for (var key in vertices[i]) {
                    if (key != "id" && key != "ecl" && key != "label" && key.substring(0, 1) != "_") {
                        layoutMap[key] = true;
                    }
                }
            }

            var layout = [[
                { 'name': 'ID', 'field': 'id', 'width': '50px' },
                { 'name': 'Label', 'field': 'label', 'width': '150px' }
            ]];

            for (var key in layoutMap) {
                layout[0].push({ 'name': key, 'field': key, 'width': '200px' });
            }
            layout[0].push({ 'name': "ECL", 'field': "ecl", 'width': '1024px' });

            var store = new Memory({ data: vertices });
            var dataStore = new ObjectStore({ objectStore: store });
            this.verticesGrid.setStructure(layout);
            this.verticesGrid.setStore(dataStore);
            this.verticesGrid.setQuery({
                id: "*"
            });
        },

        loadEdges: function () {
            var edges = this.main.plugin.getEdgesWithProperties();

            var layoutMap = [];
            for (var i = 0; i < edges.length; ++i) {
                for (var key in edges[i]) {
                    if (key != "id" && key.substring(0, 1) != "_") {
                        layoutMap[key] = true;
                    }
                }
            }

            var layout = [[
                { 'name': 'ID', 'field': 'id', 'width': '50px' }
            ]];

            for (var key in layoutMap) {
                layout[0].push({ 'name': key, 'field': key, 'width': '100px' });
            }

            var store = new Memory({ data: edges });
            var dataStore = new ObjectStore({ objectStore: store });
            this.edgesGrid.setStructure(layout);
            this.edgesGrid.setStore(dataStore);
            this.edgesGrid.setQuery({
                id: "*"
            });
        },

        syncSelectionFrom: function (sourceControl) {
            var selItems = [];

            //  Get Selected Items  ---
            if (sourceControl == this.timingGrid || sourceControl == this.timingTreeMap) {
                var items = sourceControl.getSelected();
                for (var i = 0; i < items.length; ++i) {
                    if (items[i].SubGraphId) {
                        selItems.push(items[i].SubGraphId);
                    }
                }
            } else if (sourceControl == this.verticesGrid || sourceControl == this.edgesGrid) {
                var items = sourceControl.selection.getSelected();
                for (var i = 0; i < items.length; ++i) {
                    if (items[i]._globalID) {
                        selItems.push(items[i]._globalID);
                    }
                }
            } else {
                selItems = sourceControl.getSelectionAsGlobalID();
            }

            //  Set Selected Items  ---
            if (sourceControl != this.timingGrid) {
                this.timingGrid.setSelected(selItems);
            }
            if (sourceControl != this.timingTreeMap) {
                this.timingTreeMap.setSelected(selItems);
            }
            if (sourceControl != this.verticesGrid && this.verticesGrid.store) {
                for (var i = 0; i < this.verticesGrid.rowCount; ++i) {
                    var row = this.verticesGrid.getItem(i);
                    this.verticesGrid.selection.setSelected(i, (row._globalID && array.indexOf(selItems, row._globalID) != -1));
                }
            }
            if (sourceControl != this.edgesGrid && this.edgesGrid.store) {
                for (var i = 0; i < this.edgesGrid.rowCount; ++i) {
                    var row = this.edgesGrid.getItem(i);
                    this.edgesGrid.selection.setSelected(i, (row._globalID && array.indexOf(selItems, row._globalID) != -1));
                }
            }
            if (sourceControl != this.main) {
                this.main.setSelectedAsGlobalID(selItems);
            }
            if (sourceControl != this.overview) {
                this.overview.setSelectedAsGlobalID(selItems);
            }

            var mainItems = [];
            for (var i = 0; i < selItems.length; ++i) {
                mainItems.push(this.main.getItem(selItems[i]));
            }

            if (sourceControl != this.local) {
                var xgmml = this.main.getLocalisedXGMML(mainItems, 2);
                this.local.loadXGMML(xgmml);
                this.local.setSelectedAsGlobalID(selItems);
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
