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
	"dojo/_base/lang",
	"dojo/_base/xhr",
	"dojo/_base/Deferred",
	"dojo/store/util/QueryResults",
	"hpcc/ESPBase"
], function (declare, lang, xhr, Deferred, QueryResults, ESPBase) {
	var WUQuery = declare(ESPBase, {
		idProperty: "Wuid",

		constructor: function (options) {
			declare.safeMixin(this, options);
		},

		getIdentity: function (object) {
			return object[this.idProperty];
		},

		query: function (query, options) {
			var request = {};
			lang.mixin(request, options.query);
			if (options.start)
				request['PageStartFrom'] = options.start;
			if (options.count)
				request['Count'] = options.count;
			if (options.sort) {
				request['Sortby'] = options.sort[0].attribute;
				request['Descending'] = options.sort[0].descending;
			}
			request['rawxml_'] = "1";

			var results = xhr.get({
				url: this.getBaseURL("WsWorkunits") + "/WUQuery",
				handleAs: "xml",
				content: request
			});

			var context = this;
			var parsedResults = results.then(function (domXml) {
				data = context.getValues(domXml, "ECLWorkunit");
				data.total = context.getValue(domXml, "NumWUs");
				return data;
			});

			lang.mixin(parsedResults, {
				total: Deferred.when(parsedResults, function (data) {
					return data.total;
				})
			});
	
			return QueryResults(parsedResults);
		}
	});

	var WUResult =  declare(ESPBase, {
		idProperty: "myInjectedRowNum",
		wuid: "",
		sequence: 0,
		isComplete: false,

		constructor: function (args) {
			declare.safeMixin(this, args);
		},

		getIdentity: function (object) {
			return object[this.idProperty];
		},

		queryWhenComplete: function (query, options, deferredResults) {
			var context = this;
			if (this.isComplete == true) {
				var request = {};
				request['Wuid'] = this.wuid;
				request['Sequence'] = this.sequence;
				request['Start'] = options.start;
				request['Count'] = options.count;
				request['rawxml_'] = "1";

				var results = xhr.post({
					url: this.getBaseURL("WsWorkunits") + "/WUResult",
					handleAs: "xml",
					content: request,
					load: function(domXml) {
						var rows = context.getValues(domXml, "Row");
						for (var i = 0; i < rows.length; ++i) {
							rows[i].myInjectedRowNum = options.start + i + 1;
						}
						rows.total = context.getValue(domXml, "Total");
						deferredResults.resolve(rows);
					}
				});
			} else {
				setTimeout(function() {
					context.queryWhenComplete(query, options, deferredResults);
				}, 100);
			}
		},

		query: function (query, options) {
			var deferredResults = new Deferred();

			this.queryWhenComplete(query, options, deferredResults);

			var retVal = lang.mixin({
				total: Deferred.when(deferredResults, function (rows) {
					return rows.total;
				})
			}, deferredResults);

			return QueryResults(retVal);
		}
	});

	return {
		WUQuery: WUQuery,
		WUResult: WUResult
	};
});

