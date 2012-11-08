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

    var WUResult = declare(ESPBase, {
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
                if (this.sequence != null) {
                    request['Sequence'] = this.sequence;
                } else {
                    request['LogicalName'] = this.name;
                }
                request['Start'] = options.start;
                request['Count'] = options.count;
                request['rawxml_'] = "1";

                var results = xhr.post({
                    url: this.getBaseURL("WsWorkunits") + "/WUResult",
                    handleAs: "xml",
                    content: request,
                    sync: options.sync != null ? options.sync : false,
                    load: function (domXml) {
                        var rows = context.getValues(domXml, "Row");
                        for (var i = 0; i < rows.length; ++i) {
                            rows[i].myInjectedRowNum = options.start + i + 1;
                        }
                        rows.total = context.getValue(domXml, "Total");
                        //  TODO - Need to check why this happens only sometimes  (Suspect non XML from the server) ---
                        if (rows.total == null) {
                            var debug = context.flattenXml(domXml);
                            setTimeout(function () {
                                context.queryWhenComplete(query, options, deferredResults);
                            }, 100);
                        }
                        else {
                            deferredResults.resolve(rows);
                        }
                    }
                });
            } else {
                setTimeout(function () {
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

