﻿import { arrayUtil, declare, Deferred, lang, Observable, topic, dojoxXmlParser as parser } from "src-dojo/index";

import { WorkunitsService, WsWorkunits as WsWorkunitsNS } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";

import * as ESPRequest from "./ESPRequest";
import * as ESPUtil from "./ESPUtil";
import * as ESPWorkunit from "./ESPWorkunit";
import nlsHPCC from "./nlsHPCC";
import * as WsEcl from "./WsEcl";
import * as WsWorkunits from "./WsWorkunits";
import { Paged } from "./store/Paged";
import { BaseStore } from "./store/Store";

const logger = scopedLogger("src/ESPQuery.ts");

class Store extends ESPRequest.Store {

    service = "WsWorkunits";
    action = "WUListQueries";
    responseQualifier = "WUListQueriesResponse.QuerysetQueries.QuerySetQuery";
    responseTotalQualifier = "WUListQueriesResponse.NumberOfQueries";
    idProperty = "__hpcc_id";

    startProperty = "PageStartFrom";
    countProperty = "PageSize";

    _watched = [];

    create(__hpcc_id) {
        const tmp = __hpcc_id.split(":");
        return new Query({
            __hpcc_id,
            QuerySetId: tmp[0],
            Id: tmp[1]
        });
    }

    update(id, item) {
        const storeItem = this.get(id);
        storeItem.updateData(item);
        if (!this._watched[id]) {
            const context = this;
            this._watched[id] = storeItem.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                if (oldValue !== newValue) {
                    context.notify(storeItem, id);
                }
            });
        }
    }

    preProcessRow(item, request, query, options) {
        let ErrorCount = 0;
        let StatusMessage;
        let MixedNodeStates;
        item[this.idProperty] = item.QuerySetId + ":" + item.Id;
        if (lang.exists("Clusters", item)) {
            arrayUtil.some(item.Clusters.ClusterQueryState, function (cqs, idx) {
                if (lang.exists("Errors", cqs) && cqs.Errors || cqs.State !== "Available") {
                    ErrorCount++;
                    StatusMessage = nlsHPCC.SuspendedByCluster;
                    return false;
                }
                if (lang.exists("MixedNodeStates", cqs) && cqs.MixedNodeStates === true) {
                    StatusMessage = nlsHPCC.MixedNodeStates;
                    MixedNodeStates = true;
                }
            });
        }
        if (item.Suspended === true) {
            StatusMessage = nlsHPCC.SuspendedByUser;
        }

        lang.mixin(item, {
            ErrorCount,
            Status: StatusMessage,
            MixedNodeStates
        });
    }
}

const Query = declare([ESPUtil.Singleton], {  // jshint ignore:line
    i18n: nlsHPCC,
    constructor: ESPUtil.override(function (inherited, args) {
        inherited(arguments);
        if (args) {
            declare.safeMixin(this, args);
        }
        this.queries = {};
    }),
    refresh(full) {
        return this.getDetails();
    },
    getDetails(args) {
        const context = this;
        return WsWorkunits.WUQueryDetails({
            request: {
                QueryId: this.Id,
                QuerySet: this.QuerySetId,
                IncludeSuperFiles: 1,
                IncludeStateOnClusters: 1
            }
        }).then(function (response) {
            if (lang.exists("WUQueryDetailsResponse", response)) {
                context.updateData(response.WUQueryDetailsResponse);
            }
            return response;
        });
    },
    getWorkunit() {
        return ESPWorkunit.Get(this.Wuid);
    },
    SubmitXML(xml) {
        const deferred = new Deferred();
        if (this.queries[xml]) {
            deferred.resolve(this.queries[xml]);
        } else {
            const domXml = parser.parse(xml);
            const query = {};
            arrayUtil.forEach(domXml.firstChild.childNodes, function (item, idx) {
                if (item.tagName) {
                    query[item.tagName] = item.textContent;
                }
            });
            const context = this;
            WsEcl.Submit(this.QuerySetId, this.Id, query).then(function (response) {
                context.queries[xml] = response;
                deferred.resolve(response);
                return response;
            });
        }
        return deferred.promise;
    },
    showResetQueryStatsResponse(responses) {
        let sv = "Error";
        let msg = "Invalid response";
        if (lang.exists("WUQuerySetQueryActionResponse.Results", responses[0])) {
            const result = responses[0].WUQuerySetQueryActionResponse.Results.Result[0];
            if (result.Success === 0) {
                msg = this.i18n.Exception + ": code=" + result.Code + " message=" + result.Message;
            } else {
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
    doAction(action) {
        const context = this;
        return WsWorkunits.WUQuerysetQueryAction([{
            QuerySetId: this.QuerySetId,
            Id: this.Id,
            Name: this.Name
        }], action).then(function (responses) {
            context.refresh();
            if (action === "ResetQueryStats")
                context.showResetQueryStatsResponse(responses);
            return responses;
        });
    },
    setSuspended(suspended) {
        return this.doAction(suspended ? "Suspend" : "Unsuspend");
    },
    setActivated(activated) {
        return this.doAction(activated ? "Activate" : "Deactivate");
    },
    doReset() {
        return this.doAction("ResetQueryStats");
    },
    doDelete() {
        return this.doAction("Delete");
    }
});

export function Get(QuerySetId, Id, data?) {
    const store = new Store();
    const retVal = store.get(QuerySetId + ":" + Id);
    if (data) {
        retVal.updateData(data);
    }
    return retVal;
}

export function GetFromRequestXML(QuerySetId, requestXml) {
    try {
        const domXml = parser.parse(requestXml);
        //  Not all XML is a "Request"  ---
        if (lang.exists("firstChild.tagName", domXml) && domXml.firstChild.tagName.indexOf("Request") === domXml.firstChild.tagName.length - 7) {
            return this.Get(QuerySetId, domXml.firstChild.tagName.slice(0, -7));
        }
    } catch (e) {
    }
    return null;
}

export function CreateQueryStoreLegacy() {
    const store = new Store();
    return new Observable(store);
}

const service = new WorkunitsService({ baseUrl: "" });

export type WUQueryStore = BaseStore<WsWorkunitsNS.WUListQueries, WsWorkunitsNS.QuerySetQuery>;

export function CreateQueryStore(): BaseStore<WsWorkunitsNS.WUListQueries, WsWorkunitsNS.QuerySetQuery> {
    const store = new Paged<WsWorkunitsNS.WUListQueries, WsWorkunitsNS.QuerySetQuery>({
        start: "PageStartFrom",
        count: "PageSize",
        sortBy: "Sortby",
        descending: "Descending"
    }, "Id", (request) => {
        return service.WUListQueries(request).then(response => {
            return {
                data: response?.QuerysetQueries?.QuerySetQuery?.map(qry => Get(qry.QuerySetId, qry.Id, qry)) ?? [],
                total: response?.NumberOfQueries ?? 0
            };
        }).catch(err => {
            logger.error(err);
            return {
                data: [],
                total: 0
            };
        });
    });
    return new Observable(store);
}