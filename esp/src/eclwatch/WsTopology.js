/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/store/util/QueryResults",
    "dojo/Evented",

    "hpcc/ESPRequest"
], function (declare, lang, arrayUtil, Deferred, Memory, Observable, QueryResults, Evented,
    ESPRequest) {

    var TpLogFileStore = declare([Memory, Evented], {
        constructor: function () {
            this.idProperty = "__hpcc_id";
        },

        query: function (query, options) {
            var deferredResults = new Deferred();
            deferredResults.total = new Deferred();

            function nextItem(itemParts) {
                var part = "";
                while (itemParts.length && part.trim() === "") {
                    part = itemParts[0]; itemParts.shift();
                }
                return part;
            }

            if (!query.Name) {
                deferredResults.resolve([]);
                deferredResults.total.resolve(0);
            } else {
                var results = self.TpLogFile({
                    request: lang.mixin({}, query, {
                        PageNumber: options.start / options.count
                    })
                }).then(lang.hitch(this, function (response) {
                    var data = [];
                    if (lang.exists("TpLogFileResponse.LogData", response)) {
                        this.lastPage = response.TpLogFileResponse.LogData;
                        this.emit("pageLoaded", this.lastPage);
                        arrayUtil.forEach(response.TpLogFileResponse.LogData.split("\n"), function (item, idx) {
                            if (options.start === 0 || idx > 0) {
                                //  Throw away first line as it will probably only be a partial line  ---
                                var itemParts = item.split(" ");
                                var lineNo, date, time, pid, tid, details;
                                if (itemParts.length) lineNo = nextItem(itemParts);
                                if (itemParts.length) date = nextItem(itemParts);
                                if (itemParts.length) time = nextItem(itemParts);
                                if (itemParts.length) pid = nextItem(itemParts);
                                if (itemParts.length) tid = nextItem(itemParts);
                                if (itemParts.length) details = itemParts.join(" ");
                                data.push({
                                    __hpcc_id: response.TpLogFileResponse.PageNumber + "_" + idx,
                                    lineNo: lineNo,
                                    date: date,
                                    time: time,
                                    pid: pid,
                                    tid: tid,
                                    details: details
                                });
                            }
                        }, this);
                    }
                    this.setData(data);
                    if (lang.exists("TpLogFileResponse.TotalPages", response)) {
                        deferredResults.total.resolve(response.TpLogFileResponse.TotalPages * options.count);
                    } else {
                        deferredResults.total.resolve(data.length);
                    }
                    return deferredResults.resolve(this.data);
                }));
            }

            return QueryResults(deferredResults);
        }
    });

    var TpLogFileStoreXXX = declare([ESPRequest.Store], {
        service: "WsTopology",
        action: "TpLogFile",
        responseQualifier: "TpLogFileResponse.LogData",
        responseTotalQualifier: "TpLogFileResponse.TotalPages",
        idProperty: "Wuid",
        startProperty: "PageStartFrom",
        countProperty: "Count"
    });

    var self = {    // jshint ignore:line
        TpServiceQuery: function (params) {
            lang.mixin(params.request, {
                Type: "ALLSERVICES"
            });
            return ESPRequest.send("WsTopology", "TpServiceQuery", params);
        },

        TpClusterQuery: function (params) {
            lang.mixin(params.request, {
                Type: "ROOT"
            });
            return ESPRequest.send("WsTopology", "TpClusterQuery", params);
        },

        GetESPServiceBaseURL: function (type) {
            var deferred = new Deferred();
            var context = this;
            this.TpServiceQuery({}).then(function (response) {
                var retVal = ESPRequest.getURL({
                    port: window.location.protocol === "https:" ? 18002 : 8002,
                    pathname: ""
                });
                if (lang.exists("TpServiceQueryResponse.ServiceList.TpEspServers.TpEspServer", response)) {
                    arrayUtil.forEach(response.TpServiceQueryResponse.ServiceList.TpEspServers.TpEspServer, function (item, idx) {
                        if (lang.exists("TpBindings.TpBinding", item)) {
                            arrayUtil.forEach(item.TpBindings.TpBinding, function (binding, idx) {
                                if (binding.Service === type && binding.Protocol + ":" === location.protocol) {
                                    retVal = ESPRequest.getURL({
                                        port: binding.Port,
                                        pathname: ""
                                    });
                                    return true;
                                }
                            });
                        }
                        if (retVal !== "")
                            return true;
                    });
                }
                deferred.resolve(retVal);
            });
            return deferred.promise;
        },
        WsEclURL: "",
        GetWsEclURL: function (type) {
            var deferred = new Deferred();
            if (this.WsEclURL === "") {
                var context = this;
                this.GetESPServiceBaseURL("ws_ecl").then(function (response) {
                    context.WsEclURL = response + "/WsEcl/";
                    deferred.resolve(context.WsEclURL + type + "/query/");
                });
            } else {
                deferred.resolve(this.WsEclURL + type + "/query/");
            }
            return deferred.promise;
        },
        WsEclIFrameURL: "",
        GetWsEclIFrameURL: function (type) {
            var deferred = new Deferred();
            if (this.WsEclIFrameURL === "") {
                var context = this;
                this.GetESPServiceBaseURL("ws_ecl").then(function (response) {
                    context.WsEclIFrameURL = response + dojoConfig.urlInfo.basePath + "/stub.htm?Widget=IFrameWidget&src=" + encodeURIComponent("/WsEcl/");
                    deferred.resolve(context.WsEclIFrameURL + encodeURIComponent(type + "/query/"));
                });
            } else {
                deferred.resolve(this.WsEclIFrameURL + encodeURIComponent(type + "/query/"));
            }
            return deferred.promise;
        },
        TpTargetClusterQuery: function (params) {
            return ESPRequest.send("WsTopology", "TpTargetClusterQuery", params);
        },
        TpGroupQuery: function (params) {
            return ESPRequest.send("WsTopology", "TpGroupQuery", params);
        },
        TpLogicalClusterQuery: function (params) {
            return ESPRequest.send("WsTopology", "TpLogicalClusterQuery", params).then(function (response) {
                var best = null;
                var hthor = null;
                if (lang.exists("TpLogicalClusterQueryResponse.TpLogicalClusters.TpLogicalCluster", response)) {
                    arrayUtil.forEach(response.TpLogicalClusterQueryResponse.TpLogicalClusters.TpLogicalCluster, function (item, idx) {
                        if (!best) {
                            best = item;
                        }
                        if (item.Name.indexOf("hthor") !== -1) {
                            hthor = item;
                            return false;
                        } else if (item.Name.indexOf("thor") !== -1) {
                            best = item;
                        }
                    });
                }
                if (hthor) {
                    response.TpLogicalClusterQueryResponse["default"] = hthor;
                } else if (best) {
                    response.TpLogicalClusterQueryResponse["default"] = best;
                } else {
                    response.TpLogicalClusterQueryResponse["default"] = null;
                }
                return response;
            });
        },
        TpClusterInfo: function (params) {
            return ESPRequest.send("WsTopology", "TpClusterInfo", params);
        },
        TpThorStatus: function (params) {
            return ESPRequest.send("WsTopology", "TpThorStatus", params);
        },
        TpGetServicePlugins: function (params) {
            return ESPRequest.send("WsTopology", "TpGetServicePlugins", params);
        },
        TpDropZoneQuery: function (params) {
            return ESPRequest.send("WsTopology", "TpDropZoneQuery", params);
        },
        TpGetComponentFile: function (params) {
            params.handleAs = "text";
            return ESPRequest.send("WsTopology", "TpGetComponentFile", params);
        },
        TpLogFile: function (params) {
            return ESPRequest.send("WsTopology", "TpLogFile", params);
        },
        CreateTpLogFileStore: function () {
            var store = new TpLogFileStore();
            return Observable(store);
        }
    };
    return self;
});