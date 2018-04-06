import * as declare from "dojo/_base/declare";
import * as topic from "dojo/topic";
import * as Observable from "dojo/store/Observable";

import * as ESPRequest from "./ESPRequest";

var NagiosStore = declare([ESPRequest.Store], {
    service: "ws_machine",
    action: "GetComponentStatus",
    responseQualifier: "GetComponentStatusResponse.ComponentStatusList.ComponentStatus",
    idProperty: "__hpcc_id"
});

var monitorHandle;
export function GetComponentStatus(params) {
    return ESPRequest.send("ws_machine", "GetComponentStatus", params);
}

export function GetTargetClusterInfo(params) {
    return ESPRequest.send("ws_machine", "GetTargetClusterInfo", params);
}

export function GetMachineInfo(params) {
    return ESPRequest.send("ws_machine", "GetMachineInfo", params);
}

export function MonitorComponentStatus(params) {
    var prevResponse = null;
    if (!monitorHandle) {
        var context = this;
        monitorHandle = setInterval(function () {
            context.GetComponentStatus(params).then(function (response) {
                if (response && response.GetComponentStatusResponse.ComponentStatus) {
                    response.GetComponentStatusResponse.ComponentStatusList.ComponentStatus.forEach(function (row) {
                        topic.publish("hpcc/monitoring_component_update", {
                            response: response,
                            status: response.GetComponentStatusResponse.ComponentStatus
                        });
                    });
                }
                prevResponse = response;
            });
        }, 60000);
    }
    return prevResponse;
}

export function CreateNagiosStore(options) {
    var store = new NagiosStore(options);
    return Observable(store);
}
