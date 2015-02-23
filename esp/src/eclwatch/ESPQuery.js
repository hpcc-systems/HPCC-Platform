﻿/*##############################################################################
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
    "dojo/_base/array",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/Deferred",
    "dojo/store/Observable",
    "dojo/Stateful",
    "dojo/topic",

    "dojox/xml/parser",

    "hpcc/WsWorkunits",
    "hpcc/WsEcl",
    "hpcc/ESPRequest",
    "hpcc/ESPUtil",
    "hpcc/ESPWorkunit"
], function (declare, arrayUtil, lang, i18n, nlsHPCC, Deferred, Observable, Stateful, topic,
        parser,
        WsWorkunits, WsEcl, ESPRequest, ESPUtil, ESPWorkunit) {

    var _logicalFiles = {};

    var Store = declare([ESPRequest.Store], {
        i18n: nlsHPCC,
        service: "WsWorkunits",
        action: "WUListQueries",
        responseQualifier: "WUListQueriesResponse.QuerysetQueries.QuerySetQuery",
        responseTotalQualifier: "WUListQueriesResponse.NumberOfQueries",
        idProperty: "__hpcc_id",
        startProperty: "PageStartFrom",
        countProperty: "PageSize",

        _watched: [],

        create: function (__hpcc_id) {
            var tmp = __hpcc_id.split(":");
            return new Query({
                __hpcc_id: __hpcc_id,
                QuerySetId: tmp[0],
                Id: tmp[1]
            });
        },
        update: function (id, item) {
            var storeItem = this.get(id);
            storeItem.updateData(item);
            if (!this._watched[id]) {
                var context = this;
                this._watched[id] = storeItem.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                    if (oldValue !== newValue) {
                        context.notify(storeItem, id);
                    }
                });
            }
        },

        preProcessRow: function (item, request, query, options) {
            var context = this;
            var ErrorCount = 0;
            var StatusMessage;
            var MixedNodeStates;
            item[this.idProperty] = item.QuerySetId + ":" + item.Id;
            if (lang.exists("Clusters", item)) {
                arrayUtil.some(item.Clusters.ClusterQueryState, function(cqs, idx){
                    if (lang.exists("Errors", cqs) && cqs.Errors) {
                        ErrorCount++;
                        StatusMessage = context.i18n.SuspendedByCluster;
                        return false;
                    }
                    if (lang.exists("MixedNodeStates", cqs) && cqs.MixedNodeStates == true) {
                        StatusMessage =  context.i18n.MixedNodeStates;
                        MixedNodeStates = true;
                    }
                });
            }
            if (item.Suspended == true){
                StatusMessage = this.i18n.SuspendedByUser;
            }

            lang.mixin(item, {
                ErrorCount: ErrorCount,
                Status: StatusMessage,
                MixedNodeStates: MixedNodeStates
            });
        }
    });

    var Query = declare([ESPUtil.Singleton], {
        i18n: nlsHPCC,
        constructor: function (args) {
            this.inherited(arguments);
            if (args) {
                declare.safeMixin(this, args);
            }
            this.queries = {};
        },
        refresh: function (full) {
            return this.getDetails();
        },
        getDetails: function (args) {
            var context = this;
            return WsWorkunits.WUQueryDetails({
                request:{
                    QueryId: this.Id,
                    QuerySet: this.QuerySetId,
                    IncludeSuperFiles: 1
                }
            }).then(function (response) {
                if (lang.exists("WUQueryDetailsResponse", response)) {
                    context.updateData(response.WUQueryDetailsResponse);
                }
                return response;
            });
        },
        getWorkunit: function() {
            return ESPWorkunit.Get(this.Wuid);
        },
        SubmitXML: function (xml) {
            var deferred = new Deferred();
            if (this.queries[xml]) {
                deferred.resolve(this.queries[xml]);
            } else {
                var domXml = parser.parse(xml);
                var query = {};
                arrayUtil.forEach(domXml.firstChild.childNodes, function (item, idx) {
                    if (item.tagName) {
                        query[item.tagName] = item.textContent;
                    }
                });
                var context = this;
                WsEcl.Submit(this.QuerySetId, this.Id, query).then(function (response) {
                    context.queries[xml] = response;
                    deferred.resolve(response);
                    return response;
                });
            }
            return deferred.promise;
        },
        showResetQueryStatsResponse: function (responses) {
            var sv = "Error";
            var msg = "Invalid response";
            if (lang.exists("WUQuerySetQueryActionResponse.Results", responses[0])) {
                var result = responses[0].WUQuerySetQueryActionResponse.Results.Result[0];
                if (result.Success === 0) {
                    msg = this.i18n.Exception + ": code=" + result.Code + " message=" + result.Message;
                }
                else {
                    sv = "Message";
                    msg = result.Message;
                }
            }
            topic.publish("hpcc/brToaster", {
                Severity: sv,
                Source: "WsWorkunits.WUQuerysetQueryAction",
                Exceptions: [{ Source: "ResetQueryStats", Message: msg }]
            });
        },
        doAction: function (action) {
            var context = this;
            return WsWorkunits.WUQuerysetQueryAction([{
                QuerySetId: this.QuerySetId,
                Id: this.Id,
                Name: this.Name
            }], action).then(function (responses) {
                context.refresh();
                if (action === "ResetQueryStats")
                    context.showResetQueryStatsResponse(responses);
                return response;
            });
        },
        setSuspended: function (suspended) {
            return this.doAction(suspended ? "Suspend" : "Unsuspend");
        },
        setActivated: function (activated) {
            return this.doAction(activated ? "Activate" : "Deactivate");
        },
        doReset: function () {
            return this.doAction("ResetQueryStats");
        },
        doDelete: function () {
            return this.doAction("Delete");
        }
    });

    return {
        Get: function (QuerySetId, Id, data) {
            var store = new Store();
            var retVal = store.get(QuerySetId + ":" + Id);
            if (data) {
                retVal.updateData(data);
            }
            return retVal;
        },

        GetFromRequestXML: function (QuerySetId, requestXml) {
            try {
                var domXml = parser.parse(requestXml);
                //  Not all XML is a "Request"  ---
                if (lang.exists("firstChild.tagName", domXml) && domXml.firstChild.tagName.indexOf("Request") === domXml.firstChild.tagName.length - 7) {
                    return this.Get(QuerySetId, domXml.firstChild.tagName.slice(0, -7));
                }
            } catch (e) {
            }
            return null;
        },

        CreateQueryStore: function (options) {
            var store = new Store(options);
            return new Observable(store);
        }
    };
});
