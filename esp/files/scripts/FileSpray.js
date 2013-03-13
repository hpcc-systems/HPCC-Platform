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
    "dojo/_base/lang",
    "dojo/_base/Deferred",
    "dojo/store/util/QueryResults",
    "dojo/store/JsonRest", 
    "dojo/store/Memory", 
    "dojo/store/Cache", 
    "dojo/store/Observable",
    
    "dojox/xml/parser",    

    "hpcc/ESPBase",
    "hpcc/ESPRequest"
], function (declare, lang, Deferred, QueryResults, JsonRest, Memory, Cache, Observable,
    parser,
    ESPBase, ESPRequest) {
    var GetDFUWorkunits = declare(null, {
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
            if (options.start)
                request['PageStartFrom'] = options.start;
            if (options.count)
                request['PageSize'] = options.count;
            if (options.sort) {
                request['Sortby'] = options.sort[0].attribute;
                request['Descending'] = options.sort[0].descending;
            }

            var results = ESPRequest.send("FileSpray", "GetDFUWorkunits", {
                request: request
            });

            var deferredResults = new Deferred();
            deferredResults.total = results.then(function (response) {
                if (lang.exists("GetDFUWorkunitsResponse.NumWUs", response)) {
                    return response.GetDFUWorkunitsResponse.NumWUs;
                }
                return 0;
            });
            Deferred.when(results, function (response) {
                var workunits = [];
                if (lang.exists("GetDFUWorkunitsResponse.results.DFUWorkunit", response)) {
                    workunits = response.GetDFUWorkunitsResponse.results.DFUWorkunit;
                }
                deferredResults.resolve(workunits);
            });

            return QueryResults(deferredResults);
        }
    });

    return {
        GetDFUWorkunits: GetDFUWorkunits,

        Despray: function (params) {
            return ESPRequest.send("FileSpray", "Despray", params);
        },
        Copy: function (params) {
            return ESPRequest.send("FileSpray", "Copy", params);
        },
        Rename: function (params) {
            return ESPRequest.send("FileSpray", "Rename", params);
        },
        GetDFUWorkunit: function (params) {
            return ESPRequest.send("FileSpray", "GetDFUWorkunit", params);
        },

        DFUWUFile: function (params) {
            lang.mixin(params, {
                handleAs: "text"
            });
            return ESPRequest.send("FileSpray", "DFUWUFile", params);
        }
    };
});

