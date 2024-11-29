import * as Observable from "dojo/store/Observable";
import * as topic from "dojo/topic";

import * as ESPRequest from "./ESPRequest";

class NagiosStore extends ESPRequest.Store {

    service = "ws_machine";
    action = "GetComponentStatus";
    responseQualifier = "GetComponentStatusResponse.ComponentStatusList.ComponentStatus";
    responseTotalQualifier = undefined;
    idProperty = "__hpcc_id";

}

let monitorHandle;
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
    let prevResponse = null;
    if (!monitorHandle) {
        const context = this;
        monitorHandle = setInterval(function () {
            context.GetComponentStatus(params).then(function (response) {
                if (response && response.GetComponentStatusResponse.ComponentStatus) {
                    response.GetComponentStatusResponse.ComponentStatusList.ComponentStatus.forEach(function (row) {
                        topic.publish("hpcc/monitoring_component_update", {
                            response,
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
    const store = new NagiosStore(options);
    return new Observable(store);
}
