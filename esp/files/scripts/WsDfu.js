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
    "dojo/_base/Deferred",
    "dojo/store/util/QueryResults",

    "hpcc/ESPRequest"
], function (declare, lang, Deferred, QueryResults,
    ESPRequest) {
    var DFUQuery = declare(null, {
        idProperty: "Name",

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

            var results = ESPRequest.send("WsDfu", "DFUQuery", {
                request: request
            });

            var deferredResults = new Deferred();
            deferredResults.total = results.then(function (response) {
                if (lang.exists("DFUQueryResponse.NumFiles", response)) {
                    return response.DFUQueryResponse.NumFiles;
                }
                return 0;
            });
            Deferred.when(results, function (response) {
                var workunits = [];
                if (lang.exists("DFUQueryResponse.DFULogicalFiles.DFULogicalFile", response)) {
                    workunits = response.DFUQueryResponse.DFULogicalFiles.DFULogicalFile;
                }
                deferredResults.resolve(workunits);
            });

            return QueryResults(deferredResults);
        }
    });

    return {
        DFUQuery: DFUQuery,

        DFUInfo: function (params) {
            ESPRequest.send("WsDfu", "DFUInfo", params);
        },

        DFUDefFile: function (params) {
            lang.mixin(params, {
                handleAs: "text"
            });
            ESPRequest.send("WsDfu", "DFUDefFile", params);
        }
    };
});

