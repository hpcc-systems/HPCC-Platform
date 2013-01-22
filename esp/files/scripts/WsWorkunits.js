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
    "dojo/store/JsonRest", 
    "dojo/store/Memory", 
    "dojo/store/Cache", 
    "dojo/store/Observable",
    
    "dojox/xml/parser",    

    "hpcc/ESPBase"
], function (declare, lang, xhr, Deferred, QueryResults, JsonRest, Memory, Cache, Observable,
    parser,
    ESPBase) {
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
                url: this.getBaseURL("WsWorkunits") + "/WUQuery.json",
                handleAs: "json",
                content: request
            });

            var deferredResults = new Deferred();
            deferredResults.total = results.then(function (response) {
                if (lang.exists("WUQueryResponse.NumWUs", response)) {
                    return response.WUQueryResponse.NumWUs;
                }
                return 0;
            });
            Deferred.when(results, function (response) {
                var workunits = [];
                if (lang.exists("WUQueryResponse.Workunits.ECLWorkunit", response)) {
                    workunits = response.WUQueryResponse.Workunits.ECLWorkunit;
                }
                deferredResults.resolve(workunits);
            });

            return QueryResults(deferredResults);
        }
    });

    var WUResultTest = declare(ESPBase, {
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

        query: function (query, options) {
            var request = {};
            request['Wuid'] = this.wuid;
            if (this.cluster && this.name) {
                request['Cluster'] = this.cluster;
                request['LogicalName'] = this.name;
            } else {
                request['Sequence'] = this.sequence;
            }
            request['Start'] = options.start;
            request['Count'] = options.count;
            request['rawxml_'] = "1";

            var results = xhr.get({
                url: this.getBaseURL("WsWorkunits") + "/WUResult.json",
                handleAs: "json",
                content: request
            });

            var deferredResults = new Deferred();
            deferredResults.total = results.then(function (response) {
                return response.WUResultResponse.Total;
            });
            var context = this;
            Deferred.when(results, function (response) {
                var resultXml = response.WUResultResponse.Result;
                var domXml = parser.parse("<WUResultResponse>" + resultXml + "</WUResultResponse>");
                var rows = context.getValues(domXml, "Row");
                for (var i = 0; i < rows.length; ++i) {
                    rows[i].myInjectedRowNum = options.start + i + 1;
                }
                deferredResults.resolve(rows);
            });

            return QueryResults(deferredResults);
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
                if (this.name) {
                    request['LogicalName'] = this.name;
                } else {
                    request['Wuid'] = this.wuid;
                    request['Sequence'] = this.sequence;
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
        WUResult: WUResult,

        WUAction: function (items, actionType, callback) {
            var request = {
                ActionType: actionType
            };

            for (var i = 0; i < items.length; ++i) {
                request["Wuids_i" + i] = items[i].Wuid;
            }

            var espBase = new ESPBase();
            var context = this;
            xhr.post({
                url: espBase.getBaseURL("WsWorkunits") + "/WUAction.json",
                handleAs: "json",
                content: request,
                load: function (response) {
                    if (callback && callback.load) {
                        callback.load(response);
                    }
                },
                error: function () {
                    if (callback && callback.error) {
                        callback.error(e);
                    }
                }
            });
        }
    };
});

