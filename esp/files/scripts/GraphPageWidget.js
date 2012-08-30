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
require([
    "dojo/_base/declare",
    "dojo/_base/sniff",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/on",
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
    "dojo/text!./templates/GraphPageWidget.html",

    "dijit/form/Select",
    "dijit/form/TextBox"
], function (declare, sniff, array, dom, domConstruct, on, Memory, ObjectStore,
            _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, registry, Dialog,
            DataGrid, GraphWidget, ESPWorkunit, template) {
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
        graphSelect: null,
        timingGrid: null,
        findField: null,
        findText: "",
        found: [],
        foundIndex: 0,

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

            this.main.watchSelect(this.graphSelect);
            this.overview.watchSelect(this.graphSelect);
            this.local.watchSelect(this.graphSelect);

            this.overview.watchStyleChange();
            this.local.watchStyleChange();
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
            this._initGraphSelect()
            this._initTimings();
            //this._initProperties();
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

        _initGraphSelect: function () {
            this.graphSelect = registry.byId(this.id + "GraphSelect");
            var context = this;
            this.graphSelect.onChange = function () {
                context.graphName = this.getValue();
                context.loadGraph(context.wu, context.graphName);
                context.timingGrid.setQuery({
                    GraphName: context.graphName
                });
            }
        },

        _initTimings: function () {
            this.timingGrid = registry.byId(this.id + "TimingsGrid");

            var context = this;
            this.timingGrid.on("RowClick", function (evt) {
                context.syncSelectionFrom(context.timingGrid);
            }, true);

            var context = this;
            this.timingGrid.on("RowDblClick", function (evt) {
                var idx = evt.rowIndex;
                var item = this.getItem(idx);
                if (this.store.getValue(item, "SubGraphId")) {
                    var subgraphID = parseInt(this.store.getValue(item, "SubGraphId"), 10);
                    var mainItem = context.main.getItem(subgraphID);
                    context.main.centerOnItem(mainItem, true);
                }
            }, true);
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

        setWuid: function (wuid, graphName) {
            this.graphName = graphName;
            this.wu = new ESPWorkunit({
                wuid: wuid
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
                            context.loadGraphSelect(graphs);
                        } else {
                            context.refreshGraph(context.wu, context.graphName);
                        }
                    },
                    onGetTimers: function (timers) {
                        context.loadTimings(timers);
                    }
                });
            });
        },

        loadGraphSelect: function (graphs) {
            this.graphSelect.options = [];
            for (var i = 0; i < graphs.length; ++i) {
                if (!this.graphName) {
                    this.graphName = graphs[i].Name;
                }
                this.graphSelect.options.push({
                    label: graphs[i].Name + (graphs[i].Time ? " (" + graphs[i].Time + ")" : ""),
                    value: graphs[i].Name
                });
            }
            this.graphSelect.setValue(this.graphName);
        },

        loadGraph: function (wu, graphName) {
            var context = this;
            wu.fetchGraphXgmmlByName(graphName, function (xgmml, svg) {
                context.main.setMessage("Loading Data...");
                context.main.loadXGMML(xgmml);
                context.overview.loadXGMML(context.main.getLocalisedXGMML([0]));
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
            });
        },

        loadTimings: function (timers) {
            var store = new Memory({ data: timers });
            var dataStore = new ObjectStore({ objectStore: store });
            this.timingGrid.setStore(dataStore);
            this.timingGrid.setQuery({
                GraphName: this.graphName
            });
        },

        syncSelectionFrom: function (sourceControl) {
            var selItems = [];
            if (sourceControl == this.timingGrid) {
                var items = sourceControl.selection.getSelected();
                for (var i = 0; i < items.length; ++i) {
                    if (items[i].SubGraphId) {
                        selItems.push(parseInt(items[i].SubGraphId, 10));
                    }
                }
            } else {
                selItems = sourceControl.getSelectionAsGlobalID();
            }

            if (sourceControl != this.timingGrid && this.timingGrid.store) {
                for (var i = 0; i < this.timingGrid.store.objectStore.data.length; ++i) {
                    var row = this.timingGrid.store.objectStore.data[i];
                    this.timingGrid.selection.setSelected(i, (row.SubGraphId && array.indexOf(selItems, row.SubGraphId) != -1));
                }
            }
            if (sourceControl != this.main)
                this.main.setSelectedAsGlobalID(selItems);
            if (sourceControl != this.overview)
                this.overview.setSelectedAsGlobalID(selItems);

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

        showHelpAbout: function () {
            myDialog = new Dialog({
                title: "About Graph sourceControl",
                content: "Version:  " + main.getVersion(),
                style: "width: 300px"
            });
            myDialog.show();
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
