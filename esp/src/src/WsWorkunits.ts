import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";
import * as arrayUtil from "dojo/_base/array";
import "dojo/i18n";
// @ts-ignore
import * as nlsHPCC from "dojo/i18n!hpcc/nls/hpcc";    
import * as Deferred from "dojo/_base/Deferred";
import * as all from "dojo/promise/all";
import * as Observable from "dojo/store/Observable";
import * as topic from "dojo/topic";
import * as ESPRequest from "./ESPRequest";

declare const dojo;

var EventScheduleStore = declare([ESPRequest.Store], {
    service: "WsWorkunits",
    action: "WUShowScheduled",
    responseQualifier: "WUShowScheduledResponse.Workunits.ScheduledWU",
    idProperty: "calculatedID",

    preProcessRow: function (row) {
        lang.mixin(row, {
            calculatedID: row.Wuid + row.EventText
        });
    }
});

export const States = {
    0: "unknown",
    1: "compiled",
    2: "running",
    3: "completed",
    4: "aborting",
    5: "aborted",
    6: "blocked",
    7: "submitted",
    8: "wait",
    9: "failed",
    10: "compiling",
    11: "uploading_files",
    12: "debugging",
    13: "debug_running",
    14: "paused",
    999: "not found"
};

export function WUCreate(params) {
    return ESPRequest.send("WsWorkunits", "WUCreate", params).then(function (response) {
        topic.publish("hpcc/ecl_wu_created", {
            wuid: response.WUCreateResponse.Workunit.Wuid
        });
        return response;
    });
}

export function WUUpdate(params) {
    ESPRequest.flattenMap(params.request, "ApplicationValues")
    return ESPRequest.send("WsWorkunits", "WUUpdate", params);
}

export function WUSubmit(params) {
    return ESPRequest.send("WsWorkunits", "WUSubmit", params);
}

export function WUResubmit(params) {
    return ESPRequest.send("WsWorkunits", "WUResubmit", params);
}

export function WUQueryDetails(params) {
    return ESPRequest.send("WsWorkunits", "WUQueryDetails", params);
}

export function WUGetZAPInfo(params) {
    return ESPRequest.send("WsWorkunits", "WUGetZAPInfo", params);
}

export function WUShowScheduled(params) {
    return ESPRequest.send("WsWorkunits", "WUShowScheduled", params);
}

export function WUPushEvent(params) {
    return ESPRequest.send("WsWorkunits", "WUPushEvent", params);
}

export function WUQuerysetAliasAction(selection, action) {
    var requests = [];
    arrayUtil.forEach(selection, function (item, idx) {
        var request = {
            QuerySetName: item.QuerySetId,
            Action: action,
            "Aliases.QuerySetAliasActionItem.0.Name": item.Name,
            "Aliases.QuerySetAliasActionItem.itemcount": 1
        };
        requests.push(ESPRequest.send("WsWorkunits", "WUQuerysetAliasAction", {
            request: request
        }));
    });
    return all(requests);
}

export function WUQuerysetQueryAction(selection, action) {
    if (action === "Deactivate") {
        return this.WUQuerysetAliasAction(selection, action);
    }
    var requests = [];
    arrayUtil.forEach(selection, function (item, idx) {
        var request = {
            QuerySetName: item.QuerySetId,
            Action: action,
            "Queries.QuerySetQueryActionItem.0.QueryId": item.Id,
            "Queries.QuerySetQueryActionItem.itemcount": 1
        };
        requests.push(ESPRequest.send("WsWorkunits", "WUQuerysetQueryAction", {
            request: request
        }));
    });
    return all(requests);
}

export function WUListQueries(params) {
    return ESPRequest.send("WsWorkunits", "WUListQueries", params);
}

export function WURecreateQuery(params) {
    return ESPRequest.send("WsWorkunits", "WURecreateQuery", params);
}

export function WUGetNumFileToCopy(params) {
    return ESPRequest.send("WsWorkunits", "WUGetNumFileToCopy", params);
}

export function WUPublishWorkunit(params) {
    return ESPRequest.send("WsWorkunits", "WUPublishWorkunit", params).then(function (response) {
        if (lang.exists("WUPublishWorkunitResponse", response)) {
            if (response.WUPublishWorkunitResponse.ErrorMesssage) {
                topic.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: "WsWorkunits.WUPublishWorkunit",
                    Exceptions: response.Exceptions
                });
            } else {
                dojo.publish("hpcc/brToaster", {
                    Severity: "Message",
                    Source: "WsWorkunits.WUPublishWorkunit",
                    Exceptions: [{ Source: params.request.Wuid, Message: nlsHPCC.Published + ":  " + response.WUPublishWorkunitResponse.QueryId }]
                });
                topic.publish("hpcc/ecl_wu_published", {
                    wuid: params.request.Wuid
                });
            }
        }
        return response;
    });
}

export function WUQuery(params) {
    return ESPRequest.send("WsWorkunits", "WUQuery", params).then(function (response) {
        if (lang.exists("Exceptions.Exception", response)) {
            arrayUtil.forEach(response.Exceptions.Exception, function (item, idx) {
                if (item.Code === 20081) {
                    lang.mixin(response, {
                        WUQueryResponse: {
                            Workunits: {
                                ECLWorkunit: [{
                                    Wuid: params.request.Wuid,
                                    StateID: 999,
                                    State: "not found"
                                }]
                            }
                        }
                    });
                }
            });
        }
        return response;
    });
}

export function WUInfo(params) {
    return ESPRequest.send("WsWorkunits", "WUInfo", params).then(function (response) {
        if (lang.exists("Exceptions.Exception", response)) {
            arrayUtil.forEach(response.Exceptions.Exception, function (item, idx) {
                if (item.Code === 20080) {
                    lang.mixin(response, {
                        WUInfoResponse: {
                            Workunit: {
                                Wuid: params.request.Wuid,
                                StateID: 999,
                                State: "not found"
                            }
                        }
                    });
                }
            });
        }
        return response;
    });
}

export function WUGetGraph(params) {
    return ESPRequest.send("WsWorkunits", "WUGetGraph", params);
}

export function WUResult(params) {
    return ESPRequest.send("WsWorkunits", "WUResult", params);
}

export function WUQueryGetGraph(params) {
    return ESPRequest.send("WsWorkunits", "WUQueryGetGraph", params);
}

export function WUFile(params) {
    lang.mixin(params, {
        handleAs: "text"
    });
    return ESPRequest.send("WsWorkunits", "WUFile", params);
}

export function WUAction(workunits, actionType, callback) {
    var request = {
        Wuids: workunits,
        WUActionType: actionType
    };
    ESPRequest.flattenArray(request, "Wuids", "Wuid");

    return ESPRequest.send("WsWorkunits", "WUAction", {
        request: request,
        load: function (response) {
            if (lang.exists("WUActionResponse.ActionResults.WUActionResult", response)) {
                var wuMap = {};
                arrayUtil.forEach(workunits, function (item, index) {
                    wuMap[item.Wuid] = item;
                });
                arrayUtil.forEach(response.WUActionResponse.ActionResults.WUActionResult, function (item, index) {
                    if (item.Result.indexOf("Failed:") === 0) {
                        topic.publish("hpcc/brToaster", {
                            Severity: "Error",
                            Source: "WsWorkunits.WUAction",
                            Exceptions: [{ Source: item.Action + " " + item.Wuid, Message: item.Result }]
                        });
                    } else {
                        var wu = wuMap[item.Wuid];
                        if (actionType === "delete" && item.Result === "Success") {
                            wu.set("StateID", 999);
                            wu.set("State", "not found");
                        } else if (wu.refresh) {
                            wu.refresh();
                        }
                    }
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

export function WUGetStats(params) {
    return ESPRequest.send("WsWorkunits", "WUGetStats", params);
}

export function WUCDebug(wuid, command) {
    return ESPRequest.send("WsWorkunits", "WUCDebug", {
        skipExceptions: true,
        request: {
            Wuid: wuid,
            Command: command
        }
    }).then(function (response) {
        console.log(JSON.stringify(response));
        return response;
    });
}

//  Stub waiting for HPCC-10308
export const visualisations = [
    { value: "DojoD3NDChart COLUMN", label: "Column Chart" },
    { value: "DojoD3NDChart BAR", label: "Bar Chart" },
    { value: "DojoD3NDChart LINE", label: "Line Chart" },
    { value: "DojoD3NDChart AREA", label: "Area Chart" },
    { value: "DojoD3NDChart STEP", label: "Step Chart" },
    { value: "DojoD3NDChart SCATTER", label: "Scatter Chart" },
    { value: "DojoD32DChart BUBBLE", label: "Bubble Chart" },
    { value: "DojoD32DChart PIE", label: "Pie Chart" },
    { value: "DojoD32DChart WORD_CLOUD", label: "Word Cloud" },
    { value: "DojoD3Choropleth COUNTRY", label: "Country Choropleth" },
    { value: "DojoD3Choropleth STATE", label: "US State Choropleth" },
    { value: "DojoD3Choropleth COUNTY", label: "US County Choropleth" }
];
export function GetVisualisations() {
    var deferred = new Deferred();
    if (this.visualisations) {
        deferred.resolve(this.visualisations);
    }
    return deferred.promise;
}

export function CreateEventScheduleStore(options) {
    var store = new EventScheduleStore(options);
    return Observable(store);
}

//  Helpers  ---
export function isComplete(stateID, actionEx, archived?) {
    if (archived) {
        return true;
    }
    switch (stateID) {
        case 1: //WUStateCompiled
            if (actionEx && actionEx === "compile") {
                return true;
            }
            break;
        case 3: //WUStateCompleted:
        case 4: //WUStateFailed:
        case 5: //WUStateArchived:
        case 7: //WUStateAborted:
        case 999: //WUStateDeleted:
            return true;
    }
    return false;
}
