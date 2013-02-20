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
    "dojo/_base/array",
    "dojo/_base/Deferred",
    "dojo/data/ObjectStore",
    "dojo/store/util/QueryResults",
    "dojo/store/JsonRest", 
    "dojo/store/Memory", 
    "dojo/store/Cache", 
    "dojo/store/Observable",
    
    "dojox/xml/parser",    

    "hpcc/ESPBase",
    "hpcc/ESPRequest"
], function (declare, lang, arrayUtil, Deferred, ObjectStore, QueryResults, JsonRest, Memory, Cache, Observable,
    parser,
    ESPBase, ESPRequest) {
    var WUQueryStore = declare(ESPBase, {
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

            var results = ESPRequest.send("WsWorkunits", "WUQuery", {
                request: request
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

                var results = ESPRequest.send("WsWorkunits", "WUResult", {
                    request: request
                });
                results.then(function (response) {
                    if (lang.exists("WUResultResponse.Result", response)) {
                        var xml = "<Result>" + response.WUResultResponse.Result + "</Result>";
                        var domXml = parser.parse(xml);
                        var rows = context.getValues(domXml, "Row");
                        for (var i = 0; i < rows.length; ++i) {
                            rows[i].myInjectedRowNum = options.start + i + 1;
                        }
                        rows.total = response.WUResultResponse.Total;
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
                    return response;
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
        WUResult: WUResult,
        CreateWUResultObjectStore: function (options) {
            var store = new WUResultStore(options);
            var objStore = new ObjectStore({ objectStore: store });
            return objStore;
        },

        CreateWUQueryObjectStore: function (options) {
            var store = new WUQueryStore(options);
            var objStore = new ObjectStore({ objectStore: store });
            return objStore;
        },

        WUCreate: function (params) {
            ESPRequest.send("WsWorkunits", "WUCreate", params);
        },

        WUUpdate: function (params) {
            ESPRequest.flattenMap(params.request, "ApplicationValues")
            ESPRequest.send("WsWorkunits", "WUUpdate", params);
        },

        WUSubmit: function (params) {
            ESPRequest.send("WsWorkunits", "WUSubmit", params);
        },

        WUResubmit: function (params) {
            ESPRequest.send("WsWorkunits", "WUResubmit", params);
        },

        WUPublishWorkunit: function (params) {
            ESPRequest.send("WsWorkunits", "WUPublishWorkunit", params);
        },

        WUInfo: function (params) {
            ESPRequest.send("WsWorkunits", "WUInfo", params);
        },

        WUGetGraph: function (params) {
            ESPRequest.send("WsWorkunits", "WUGetGraph", params);
        },

        WUFile: function (params) {
            lang.mixin(params, {
                handleAs: "text"
            });
            ESPRequest.send("WsWorkunits", "WUFile", params);
        },

        WUAction: function (wuids, actionType, callback) {
            var request = {
                Wuids: wuids,
                ActionType: actionType
            };
            ESPRequest.flattenArray(request, "Wuids");

            ESPRequest.send("WsWorkunits", "WUAction", {
                request: request,
                load: function (response) {
                    if (lang.exists("WUActionResponse.ActionResults.WUActionResult", response)) {
                        arrayUtil.forEach(response.WUActionResponse.ActionResults.WUActionResult, function (item, index) {
                            dojo.publish("hpcc/brToaster", {
                                message: "<h4>" + item.Action + " "+ item.Wuid + "</h4>" + "<p>" + item.Result + "</p>",
                                type: item.Result.indexOf("Failed:") === 0 ? "error" : "message",
                                duration: -1
                            });
                        });
                    }

                    if (callback && callback.load) {
                        callback.load(response);
                    }
                },
                error: function (err) {
                    if (callback && callback.error) {
                        callback.error(err);
                    }
                }
            });
        }
    };
});

