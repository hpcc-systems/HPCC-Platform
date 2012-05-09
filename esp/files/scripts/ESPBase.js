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
	"dojo/_base/config",
	"dojo/_base/declare"
], function (baseConfig, declare) {
	return declare(null, {

		constructor: function (args) {
			declare.safeMixin(this, args);
		},

		getParam: function (key) {
			var value = dojo.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search[0] === "?" ? 1 : 0)))[key];
			if (value)
				return value;
			return baseConfig[key];
		},

		getBaseURL: function () {
			var serverIP = this.getParam("serverIP");
			if (serverIP)
				return "http://" + serverIP + ":8010/WsWorkunits";
			return "/WsWorkunits";
		},

		parseKeyValue: function (xmlDom, nodeLabel) {
			var items = xmlDom.getElementsByTagName(nodeLabel);
			if (items.length && items[0].childNodes.length) {
				return items[0].childNodes[0].nodeValue;
			}
			return "";
		},

		parseKeyChildren: function (xmlDom, nodeLabel) {
			var items = xmlDom.getElementsByTagName(nodeLabel);
			if (items.length && items[0].childNodes.length) {
				return items[0].childNodes;
			}
			return null;
		},

		parseRows: function (xmlDom, nodeLabel) {
			var rows = [];
			var items = xmlDom.getElementsByTagName(nodeLabel);
			for (var i = 0; i < items.length; ++i) {
				var item = items[i];
				var cols = {};
				for (var c = 0; c < item.childNodes.length; ++c) {
					colNode = item.childNodes[c];
					if (colNode.childNodes.length && colNode.childNodes[0].nodeValue) {
						cols[colNode.nodeName] = colNode.childNodes[0].nodeValue;
					} else {
						cols[colNode.nodeName] = "";
					}
				}
				rows.push(cols);
			}
			return rows;
		},

		//  <XXX><YYY/><YYY/><YYY/><YYY/></XXX>
		parseDataset: function (xmlDom, _name, nodeLabel) {
			var retVal = {};
			var retValRows = this.parseRows(xmlDom, nodeLabel);
			var retValHeader = [];
			if (retValRows.length) {
				for (var key in retValRows[0]) {
					retValHeader.push(key)
				}
			}
			retVal = {
				name: _name,
				header: retValHeader,
				rows: retValRows
			};
			return retVal;

		},

		parseDatasets: function (xmlDom, nodeLabel, innerNodeLabel) {
			var retVal = [];
			var datasets = xmlDom.getElementsByTagName(nodeLabel);
			for (var d = 0; d < datasets.length; ++d) {
				var dataset = datasets[d];
				var retValRows = this.parseRows(dataset, innerNodeLabel);
				var retValHeader = [];
				if (retValRows.length) {
					for (var key in retValRows[0]) {
						retValHeader.push(key)
					}
				}

				retVal.push({
					name: dataset.getAttribute("name"),
					header: retValHeader,
					rows: retValRows
				});
			}
			return retVal;
		}
	});
});
