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
    "dojo/store/Memory",
    "dojo/data/ObjectStore",

    "dijit/registry",
    "dijit/layout/ContentPane",

    "dojox/grid/DataGrid",
    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/Pagination",
    "dojox/grid/enhanced/plugins/Filter",
    "dojox/grid/enhanced/plugins/NestedSorting"

], function (declare, Memory, ObjectStore,
    registry, ContentPane,
    DataGrid, EnhancedGrid, Pagination, Filter, NestedSorting) {
    return declare(null, {
        workunit: null,
        paneNum: 0,
        id: "",
        dataGridSheet: {},
        resultIdStoreMap: [],
        resultIdGridMap: [],
        delayLoad: [],
        selectedResult: null,

        //  Callbacks
        onErrorClick: function (line, col) {
        },

        // The constructor    
        constructor: function (args) {
            declare.safeMixin(this, args);

            this.dataGridSheet = registry.byId(this.id);
            var context = this;
            this.dataGridSheet.watch("selectedChildWidget", function (name, oval, nval) {
                if (nval.id in context.delayLoad) {
                    context.selectedResult = context.delayLoad[nval.id].result;
                    if (!context.selectedResult.isComplete()) {
                        context.delayLoad[nval.id].loadingMessage = context.getLoadingMessage();
                    }
                    context.delayLoad[nval.id].placeAt(nval.containerNode, "last");
                    context.delayLoad[nval.id].startup();
                    nval.resize();
                    delete context.delayLoad[nval.id];
                }
            });
        },

        clear: function () {
            this.delayLoad = [];
            this.resultIdStoreMap = [];
            this.resultIdGridMap = [];
            var tabs = this.dataGridSheet.getChildren();
            for (var i = 0; i < tabs.length; ++i) {
                this.dataGridSheet.removeChild(tabs[i]);
            }
        },

        getNextPaneID: function () {
            return this.id + "Pane_" + ++this.paneNum;
        },

        addTab: function (label, paneID) {
            if (paneID == null) {
                paneID = this.getNextPaneID();
            }
            var pane = new ContentPane({
                title: label,
                id: paneID,
                style: {
                    overflow: "hidden",
                    padding: 0
                }
            });
            this.dataGridSheet.addChild(pane);
            return pane;
        },

        addResultTab: function (result) {
            var paneID = this.getNextPaneID();

            var grid = EnhancedGrid({
                result: result,
                store: result.getObjectStore(),
                query: { id: "*" },
                structure: result.getStructure(),
                canSort: function (col) {
                    return false;
                },
                plugins: {
                    pagination: {
                        pageSizes: [25, 50, 100, "All"],
                        defaultPageSize: 50,
                        description: true,
                        sizeSwitch: true,
                        pageStepper: true,
                        gotoButton: true,
                        maxPageStep: 4,
                        position: "bottom"
                    }
                }
            });
            this.delayLoad[paneID] = grid;
            this.resultIdStoreMap[result.getID()] = result.store;
            this.resultIdGridMap[result.getID()] = grid;
            return this.addTab(result.getName(), paneID);
        },

        refresh: function (wu) {
            if (this.workunit != wu) {
                this.clear();
                this.workunit = wu;
            }
            this.addExceptionTab(this.workunit.exceptions);
            this.addResultsTab(this.workunit.results);
        },

        refreshSourceFiles: function (wu) {
            if (this.workunit != wu) {
                this.clear();
                this.workunit = wu;
            }
            this.addResultsTab(this.workunit.sourceFiles);
        },

        addResultsTab: function (results) {
            for (var i = 0; i < results.length; ++i) {
                var result = results[i];
                if (result.getID() in this.resultIdStoreMap) {
                    this.resultIdStoreMap[result.getID()].isComplete = result.isComplete();
                } else {
                    pane = this.addResultTab(result);
                    if (this.sequence != null && this.sequence == result.getID()) {
                        this.dataGridSheet.selectChild(pane);
                    } else if (this.name != null && this.name == result.getID()) {
                        this.dataGridSheet.selectChild(pane);
                    }
                }
                if (!result.isComplete()) {
                    this.resultIdGridMap[result.getID()].showMessage(this.getLoadingMessage());
                }
            }
        },

        getLoadingMessage: function () {
            return "<span class=\'dojoxGridWating\'>[" + this.workunit.state + "]</span>";
        },

        addExceptionTab: function (exceptions) {
            var hasErrorWarning = false;
            for (var i = 0; i < exceptions.length; ++i) {
                if (exceptions[i].Severity == "Error" || exceptions[i].Severity == "Warning") {
                    hasErrorWarning = true;
                    break;
                }
            }

            if (hasErrorWarning) {
                var resultNode = this.addTab("Error/Warning(s)");
                store = new Memory({ data: exceptions });
                dataStore = new ObjectStore({ objectStore: store });

                grid = new DataGrid({
                    store: dataStore,
                    query: { id: "*" },
                    structure: [
                        { name: "Severity", field: "Severity" },
                        { name: "Line", field: "LineNo" },
                        { name: "Column", field: "Column" },
                        { name: "Code", field: "Code" },
                        { name: "Message", field: "Message", width: "auto" }
                    ]
                });
                grid.placeAt(resultNode.containerNode, "last");
                grid.startup();

                var context = this;
                grid.on("RowClick", function (evt) {
                    var idx = evt.rowIndex;
                    var item = this.getItem(idx);
                    var line = parseInt(this.store.getValue(item, "LineNo"), 10);
                    var col = parseInt(this.store.getValue(item, "Column"), 10);
                    context.onErrorClick(line, col);
                }, true);
            }
        }
    });
});