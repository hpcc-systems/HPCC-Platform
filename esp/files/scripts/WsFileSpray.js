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

	var GetDFUWorkunits = declare(ESPBase, {
		idProperty: "ID",

		constructor: function (options) {
			declare.safeMixin(this, options);
		},

		getIdentity: function (object) {
			return object[this.idProperty];
		},

		query: function (query, options) {
			var request = {};
			lang.mixin(request, options.query);
			request['PageStartFrom'] = options.start;
			request['PageSize'] = options.count;
			if (options.sort) {
				request['Sortby'] = options.sort[0].attribute;
				request['Descending'] = options.sort[0].descending;
			}
			request['rawxml_'] = "1";

			var results = xhr.get({
				url: this.getBaseURL("FileSpray") + "/GetDFUWorkunits",
				handleAs: "xml",
				content: request
			});

			var context = this;
			var parsedResults = results.then(function (domXml) {
				data = context.getValues(domXml, "DFUWorkunit");
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

	var FileList = declare(ESPBase, {
		idProperty: "name",

		constructor: function (options) {
			declare.safeMixin(this, options);
		},

		getIdentity: function (object) {
			return object[this.idProperty];
		},

		query: function (query, options) {
			var request = {};
			lang.mixin(request, options.query);
			request['rawxml_'] = "1";

			var results = xhr.get({
				url: this.getBaseURL("FileSpray") + "/FileList",
				handleAs: "xml",
				content: request
			});

			var context = this;
			var parsedResults = results.then(function (domXml) {
				var debug = context.flattenXml(domXml);
				var data = context.getValues(domXml, "PhysicalFileStruct");
				return data;
			});

			lang.mixin(parsedResults, {
				total: Deferred.when(parsedResults, function (data) {
					return data.length;
				})
			});

			return QueryResults(parsedResults);
		}
	});

	return {
		GetDFUWorkunits: GetDFUWorkunits,
		FileList: FileList
	};
});
