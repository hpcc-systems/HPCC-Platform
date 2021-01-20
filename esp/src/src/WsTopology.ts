import { Connection } from "@hpcc-js/comms";
import * as arrayUtil from "dojo/_base/array";
import * as Deferred from "dojo/_base/Deferred";
import * as lang from "dojo/_base/lang";
import * as Observable from "dojo/store/Observable";
import * as QueryResults from "dojo/store/util/QueryResults";
import * as on from "dojo/on";
import * as aspect from "dojo/aspect";

import * as ESPRequest from "./ESPRequest";
import * as Utility from "./Utility";
import { Memory } from "src/Memory";

declare const dojoConfig;

class TpLogFileStore extends Memory {

    constructor() {
        super();
        this.idProperty = "__hpcc_id";
    }

    //  Evented  ---
    on(type, listener) {
        return on.parse(this, type, listener, function (target, type) {
            return aspect.after(target, "on" + type, listener, true);
        });
    }

    emit(type, event?) {
        const args = [this];
        args.push.apply(args, arguments);
        return on.emit.apply(on, args);
    }
    //  --- --- ---

    query(query, options) {
        const deferredResults = new Deferred();
        deferredResults.total = new Deferred();

        function nextItem(itemParts) {
            let part = "";
            while (itemParts.length && part.trim() === "") {
                part = itemParts[0];
                itemParts.shift();
            }
            return part;
        }

        if (!query.Name) {
            deferredResults.resolve([]);
            deferredResults.total.resolve(0);
        } else {
            this.emit("preFetch");
            TpLogFile({
                request: lang.mixin({}, query, {
                    PageNumber: options.start / options.count,
                    IncludeLogFieldNames: 0
                })
            }).then(lang.hitch(this, function (response) {
                const data = [];
                if (lang.exists("TpLogFileResponse.LogData", response)) {
                    const columns = response.TpLogFileResponse.LogFieldNames.Item.map(col => Utility.removeSpecialCharacters(col));
                    this.lastPage = response.TpLogFileResponse.LogData;
                    this.emit("pageLoaded", this.lastPage);
                    arrayUtil.forEach(response.TpLogFileResponse.LogData.split("\n"), function (item, idx) {
                        if (options.start === 0 || idx > 0) {
                            //  Throw away first line as it will probably only be a partial line  ---
                            const itemParts = item.split(" ");
                            const tempObj = {
                                __hpcc_id: response.TpLogFileResponse.PageNumber + "_" + idx
                            };

                            for (let i = 0; i < columns.length; ++i) {
                                const cleanName = columns[i];
                                let value = "";

                                if ((i + 1) === columns.length) {
                                    value = itemParts.join("");
                                } else if (itemParts.length) {
                                    value = nextItem(itemParts);
                                }

                                tempObj[cleanName] = value;
                                data.push(tempObj);
                            }
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
            })).then(lang.hitch(this, function (response) {
                this.emit("postFetch");
                return response;
            })).catch(lang.hitch(this, function (e) {
                this.emit("postFetch");
            }));
        }

        return QueryResults(deferredResults);
    }
}

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
    const deferred = new Deferred();
    this.TpServiceQuery({}).then(function (response) {
        let retVal = ESPRequest.getURL({
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
    const deferred = new Deferred();
    if (this.WsEclURL === "") {
        const context = this;
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
    const deferred = new Deferred();
    if (this.WsEclIFrameURL === "") {
        const context = this;
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

export enum RoxieQueueFilter {
    All = "All",
    QueriesOnly = "QueriesOnly",
    WorkunitsOnly = "WorkunitsOnly"
}

export function TpLogicalClusterQuery(params?: { EclServerQueue?: string, RoxieQueueFilter?: RoxieQueueFilter }) {
    return ESPRequest.send("WsTopology", "TpLogicalClusterQuery", params).then(function (response) {
        let best = null;
        let hthor = null;
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
    const store = new TpLogFileStore();
    return new Observable(store);
}
export function TpGetServerVersion() {
    const connection = new Connection({ baseUrl: "/esp", type: "get" });
    return connection.send("titlebar", { rawxml_: undefined }, "text").then((response: string) => {
        if (typeof response === "string") {
            const BuildVersion = "BuildVersion";
            const idxStart = response.indexOf(`<${BuildVersion}>`);
            const idxEnd = response.indexOf(`</${BuildVersion}>`);
            if (idxStart >= 0 && idxEnd >= 0) {
                return response.substr(idxStart + BuildVersion.length + 2, idxEnd - (idxStart + BuildVersion.length + 2));
            }
        }
        return "";
    });
}
