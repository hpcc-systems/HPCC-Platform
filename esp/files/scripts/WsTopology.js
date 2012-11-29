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
	var TpServiceQuery =  declare(ESPBase, {
		idProperty: "Wuid",

		constructor: function (options) {
			declare.safeMixin(this, options);
		},

		getIdentity: function (object) {
			return object[this.idProperty];
		},

		query: function (query, options) {
			var request = {
					Type: "ALLSERVICES"
			};
			lang.mixin(request, options.query);
			request['rawxml_'] = "1";

			var results = xhr.get({
				url: this.getBaseURL("WsTopology") + "/TpServiceQuery",
				handleAs: "xml",
				content: request
			});

			var context = this;
			var parsedResults = results.then(function (domXml) {
				var data = context.getValues(domXml, "TpDropZone", ["TpMachine"]);
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
		TpServiceQuery: TpServiceQuery
	};
});
