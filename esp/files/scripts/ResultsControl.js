/*##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
], function (declare, Memory, ObjectStore,
			DataGrid, EnhancedGrid, Pagination, Filter, NestedSorting,
			registry, ContentPane) {
	return declare(null, {
		workunit: null,
		paneNum: 0,
		resultsSheetID: "",
		dataGridSheet: {},
		resultIdStoreMap: [],
		resultIdGridMap: [],
		delayLoad: [],

		//  Callbacks
		onErrorClick: function (line, col) {
		},

		// The constructor    
		constructor: function (args) {
			declare.safeMixin(this, args);

			this.dataGridSheet = registry.byId(this.resultsSheetID);
			var context = this;
			this.dataGridSheet.watch("selectedChildWidget", function (name, oval, nval) {
				if (nval.id in context.delayLoad) {
					var result = context.delayLoad[nval.id].result;
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
			this.resultIdStoreMap = [];
			this.resultIdGridMap = [];
			var tabs = this.dataGridSheet.getChildren();
			for (var i = 0; i < tabs.length; ++i) {
				this.dataGridSheet.removeChild(tabs[i]);
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