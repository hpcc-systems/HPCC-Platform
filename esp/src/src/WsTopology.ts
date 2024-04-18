import { Connection, ResourcesService, Topology } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { containerized } from "src/BuildInfo";
import { Memory } from "src/store/Memory";
import * as arrayUtil from "dojo/_base/array";
import * as Deferred from "dojo/_base/Deferred";
import * as lang from "dojo/_base/lang";
import * as Observable from "dojo/store/Observable";
import * as QueryResults from "dojo/store/util/QueryResults";
import * as on from "dojo/on";
import * as aspect from "dojo/aspect";

import * as ESPRequest from "./ESPRequest";
import * as Utility from "./Utility";

const logger = scopedLogger("src/ESPRequest.ts");

declare const dojoConfig;

class TpLogFileStore extends Memory {

    constructor() {
        super("__hpcc_id");
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
                            }
                            data.push(tempObj);
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

const eclqueriesPromise: { [id: string]: Promise<string> } = {};
export function GetESPServiceBaseURL(type: string): Promise<string> {
    if (!eclqueriesPromise[type]) {
        let retVal = ESPRequest.getURL({
            port: window.location.protocol === "https:" ? 18002 : 8002,
            pathname: ""
        });
        if (containerized) {
            const resources = new ResourcesService({ baseUrl: "" });
            eclqueriesPromise[type] = resources.ServiceQuery({ Type: type }).then(response => {
                const service = response?.Services?.Service?.find(s => s.Type === type);
                if (service) {
                    retVal = ESPRequest.getURL({
                        protocol: service.TLSSecure ? "https:" : "http:",
                        port: service.Port,
                        pathname: ""
                    });
                }
                return retVal;
            }).catch(e => {
                logger.error(e);
                return retVal;
            });
        } else {
            const topology = Topology.attach({ baseUrl: "" });
            eclqueriesPromise[type] = topology.fetchServices({ Type: type }).then(response => {
                const service = response?.TpEspServers?.TpEspServer?.find(s => s.Type === type);
                if (service) {
                    const binding = service.TpBindings?.TpBinding?.find(b => b.Service === type && b.Protocol + ":" === location.protocol);
                    if (binding) {
                        retVal = ESPRequest.getURL({
                            port: binding.Port,
                            pathname: ""
                        });
                    }
                }
                return retVal;
            }).catch(e => {
                logger.error(e);
                return retVal;
            });
        }
    }
    return eclqueriesPromise[type];
}
let WsEclURL: Promise<string>;
export function GetWsEclURL(type): Promise<string> {
    if (!WsEclURL) {
        WsEclURL = GetESPServiceBaseURL(containerized ? "eclqueries" : "ws_ecl").then(response => {
            return response + "/WsEcl/";
        });
    }
    return this.WsEclURL.then(response => response + type + "/query/");
}
let WsEclIFrameURL: Promise<string>;
export function GetWsEclIFrameURL(type): Promise<string> {
    if (!WsEclIFrameURL) {
        WsEclIFrameURL = GetESPServiceBaseURL(containerized ? "eclqueries" : "ws_ecl").then(response => {
            return response + dojoConfig.urlInfo.basePath + "/stub.htm?Widget=IFrameWidget&src=" + encodeURIComponent("/WsEcl/");
        });
    }
    return WsEclIFrameURL.then(url => url + encodeURIComponent(type + "/query/"));
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
    const _params = { request: {}, ...params };
    _params.request = { ...{ ECLWatchVisibleOnly: true }, ...params.request };
    return ESPRequest.send("WsTopology", "TpDropZoneQuery", _params);
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
