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
	"dojox/grid/DataGrid",
	"dojox/grid/EnhancedGrid",
	"dojox/grid/enhanced/plugins/Pagination",
	"dojox/grid/enhanced/plugins/Filter",
	"dojox/grid/enhanced/plugins/NestedSorting",
	"dijit/registry",
	"dijit/layout/ContentPane"
], function (declare, Memory, ObjectStore, DataGrid, EnhancedGrid, Pagination, Filter, NestedSorting, registry, ContentPane) {
	return declare(null, {
		workunit: null,
		paneNum: 0,
		resultsSheetID: "",
		resultSheet: {},
		sequenceResultStoreMap: [],
		sequenceResultGridMap: [],
		delayLoad: [],

		//  Callbacks
		onErrorClick: function (line, col) {
		},

		// The constructor    
		constructor: function (args) {
			declare.safeMixin(this, args);

			this.resultSheet = registry.byId(this.resultsSheetID);
			var context = this;
			this.resultSheet.watch("selectedChildWidget", function (name, oval, nval) {
				if (nval.id in context.delayLoad) {
					var result = context.workunit.results[context.delayLoad[nval.id].resultIndex];
					if (!result.isComplete()) {
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
			this.sequenceResultStoreMap = [];
			this.sequenceResultGridMap = [];
			var tabs = this.resultSheet.getChildren();
			for (var i = 0; i < tabs.length; ++i) {
				this.resultSheet.removeChild(tabs[i]);
			}
		},

		getNextPaneID: function () {
			return "Pane_" + ++this.paneNum;
		},

		addTab: function (label, paneID) {
			if (paneID == null) {
				paneID = this.getNextPaneID();
			}
			var pane = new ContentPane({
				title: label,
				id: paneID,
				closable: true,
				style: {
					overflow: "hidden",
					padding: 0
				}
			});
			this.resultSheet.addChild(pane);
			return pane;
		},

		addResultTab: function (resultIndex) {
			var result = this.workunit.results[resultIndex];
			var paneID = this.getNextPaneID();
			var grid = EnhancedGrid({
				resultIndex: resultIndex,
				store: result.getObjectStore(),
				query: { id: "*" },
				structure: result.getStructure(),
				canSort: function (col) {
					return false;
				},
				plugins: {
					//					nestedSorting: true,
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
			this.sequenceResultStoreMap[result.Sequence] = result.store;
			this.sequenceResultGridMap[result.Sequence] = grid;
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

		addResultsTab: function (results) {
			for (var i = 0; i < results.length; ++i) {
				var result = results[i];
				if (result.Sequence in this.sequenceResultStoreMap) {
					this.sequenceResultStoreMap[result.Sequence].isComplete = result.isComplete();
				} else {
					pane = this.addResultTab(i);
					if (this.sequence && this.sequence == result.Sequence) {
						this.resultSheet.selectChild(pane);
					}
				}
				if (!result.isComplete()) {
					this.sequenceResultGridMap[result.Sequence].showMessage(this.getLoadingMessage());
				}
			}
		},

		getLoadingMessage: function () {
			return "<span class=\'dojoxGridWating\'>[" + this.workunit.state + "]</span>";
		},

		addExceptionTab: function (exceptions) {
			if (exceptions.length) {
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