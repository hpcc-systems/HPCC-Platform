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
], function (declare, Memory, ObjectStore, DataGrid, EnhancedGrid, Pagination, Filter, NestedSorting, registry, ContentPane) {
	return declare(null, {
		workunit: null,
		paneNum: 0,
		resultsSheetID: "",
		resultSheet: {},
		sequenceResultStoreMap: [],
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

		addResultTab: function (result) {
			var paneID = this.getNextPaneID();
			var grid = EnhancedGrid({
				store: result.getObjectStore(),
				query: { id: "*" },
				structure: result.getStructure(),
				canSort: function (col) {
					return false;
				},
				loadingMessage: result.isComplete() ? "<span class=\'dojoxGridLoading\'>Loading...</span>" : "<span class=\'dojoxGridLoading\'>Calculating...</span>",
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
			return this.addTab(result.getName(), paneID);
		},

		refreshResults: function (wu) {
			if (this.workunit != wu) {
				this.clear();
				this.workunit = wu;
			}
			for (var i = 0; i < this.workunit.results.length; ++i) {
				var result = this.workunit.results[i];
				if (result.Sequence in this.sequenceResultStoreMap) {
					this.sequenceResultStoreMap[result.Sequence].isComplete = result.isComplete();
				} else {
					pane = this.addResultTab(result);
					if (this.sequence && this.sequence == result.Sequence) {
						this.resultSheet.selectChild(pane);
					}
				}
			}
		},

		addExceptionTab: function (exceptions) {
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
	});
});