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
	"dojo/dom",
	"dojo/store/Memory",
	"dojo/data/ObjectStore",
	"dojox/grid/DataGrid",
	"dijit/registry",
	"dijit/layout/ContentPane"
], function (declare, dom, Memory, ObjectStore, DataGrid, registry, ContentPane) {
	return declare(null, {
		paneNum: 0,
		resultsSheetID: "",

		//  Callbacks
		onErrorClick: function (line, col) {
		},

		// The constructor    
		constructor: function (args) {
			declare.safeMixin(this, args);
		},

		clear: function () {
			var resultSheet = registry.byId(this.resultsSheetID);
			var tabs = resultSheet.getChildren();
			for (var i = 0; i < tabs.length; ++i) {
				resultSheet.removeChild(tabs[i]);
			}
		},

		addTab: function (label) {
			var resultSheet = registry.byId(this.resultsSheetID);
			var paneID = "Pane_" + ++this.paneNum;
			var pane = new ContentPane({
				title: label,
				id: paneID,
				closable: "true",
				style: { padding: "0px" },
				content: "<div id=\"Div_" + paneID + "\"></div>"
			});
			resultSheet.addChild(pane);
			return dom.byId(pane.id);
		},

		addDatasetTab: function (dataset) {
			var resultNode = this.addTab(dataset.name);

			var gridLayout = [];
			for (var h = 0; h < dataset.header.length; ++h) {
				gridLayout.push({
					name: dataset.header[h],
					field: dataset.header[h],
					width: "auto"
				});
			}
			store = new Memory({ data: dataset.rows });
			dataStore = new ObjectStore({ objectStore: store });

			grid = new DataGrid({
				store: dataStore,
				query: {},
				structure: gridLayout
			}, "Div_" + resultNode.id);
			grid.startup();
		},

		addExceptionTab: function (errors) {
			var resultNode = this.addTab("Error(s)");
			store = new Memory({ data: errors });
			dataStore = new ObjectStore({ objectStore: store });

			grid = new DataGrid({
				store: dataStore,
				query: {},
				structure: [
					{ name: "Severity", field: "Severity" },
					{ name: "Line", field: "LineNo" },
					{ name: "Column", field: "Column" },
					{ name: "Code", field: "Code" },
					{ name: "Message", field: "Message", width: "auto" }
					]
			}, "Div_" + resultNode.id);
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