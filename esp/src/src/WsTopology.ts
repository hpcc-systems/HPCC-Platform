import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";
import * as arrayUtil from "dojo/_base/array";
import * as Deferred from "dojo/_base/Deferred";
import * as Memory from "dojo/store/Memory";
import * as Observable from "dojo/store/Observable";
import * as QueryResults from "dojo/store/util/QueryResults";
import * as Evented from "dojo/Evented";

import * as ESPRequest from "./ESPRequest";

declare const dojoConfig;

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
            TpLogFile({
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

export function TpServiceQuery(params) {
    lang.mixin(params.request, {
        Type: "ALLSERVICES"
    });
    return ESPRequest.send("WsTopology", "TpServiceQuery", params);
}

export function TpClusterQuery(params) {
    lang.mixin(params.request, {
        Type: "ROOT"
    });
    return ESPRequest.send("WsTopology", "TpClusterQuery", params);
}

export function GetESPServiceBaseURL(type) {
    var deferred = new Deferred();
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
}
export const WsEclURL = "";
export function GetWsEclURL(type) {
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
}
export const WsEclIFrameURL = "";
export function GetWsEclIFrameURL(type) {
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
}
export function TpTargetClusterQuery(params) {
    return ESPRequest.send("WsTopology", "TpTargetClusterQuery", params);
}
export function TpGroupQuery(params) {
    return ESPRequest.send("WsTopology", "TpGroupQuery", params);
}
export function TpLogicalClusterQuery(params?) {
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
}
export function TpClusterInfo(params) {
    return ESPRequest.send("WsTopology", "TpClusterInfo", params);
}
export function TpThorStatus(params) {
    return ESPRequest.send("WsTopology", "TpThorStatus", params);
}
export function TpGetServicePlugins(params) {
    return ESPRequest.send("WsTopology", "TpGetServicePlugins", params);
}
export function TpDropZoneQuery(params) {
    return ESPRequest.send("WsTopology", "TpDropZoneQuery", params);
}
export function TpGetComponentFile(params) {
    params.handleAs = "text";
    return ESPRequest.send("WsTopology", "TpGetComponentFile", params);
}
export function TpLogFile(params) {
    return ESPRequest.send("WsTopology", "TpLogFile", params);
}
export function CreateTpLogFileStore() {
    var store = new TpLogFileStore();
    return Observable(store);
}
