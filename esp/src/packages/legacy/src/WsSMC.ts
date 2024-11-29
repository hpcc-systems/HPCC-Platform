import * as ESPRequest from "./ESPRequest";

export function Activity(params) {
    return ESPRequest.send("WsSMC", "Activity", params);
}
export function GetStatusServerInfo(params) {
    return ESPRequest.send("WsSMC", "GetStatusServerInfo", params);
}
export function PauseQueue(params) {
    return ESPRequest.send("WsSMC", "PauseQueue", params);
}
export function ResumeQueue(params) {
    return ESPRequest.send("WsSMC", "ResumeQueue", params);
}
export function ClearQueue(params) {
    return ESPRequest.send("WsSMC", "ClearQueue", params);
}
export function SetJobPriority(params) {
    return ESPRequest.send("WsSMC", "SetJobPriority", params);
}
export function MoveJobFront(params) {
    return ESPRequest.send("WsSMC", "MoveJobFront", params);
}
export function MoveJobUp(params) {
    return ESPRequest.send("WsSMC", "MoveJobUp", params);
}
export function MoveJobDown(params) {
    return ESPRequest.send("WsSMC", "MoveJobDown", params);
}
export function MoveJobBack(params) {
    return ESPRequest.send("WsSMC", "MoveJobBack", params);
}
export function parseBuildString(build) {
    const retVal = {
        orig: build,
        prefix: "",
        postfix: "",
        version: ""
    };
    if (!build) {
        return retVal;
    }
    retVal.orig = build;
    retVal.prefix = "";
    retVal.postfix = "";
    let verArray = build.split("[");
    if (verArray.length > 1) {
        retVal.postfix = verArray[1].split("]")[0];
    }
    verArray = verArray[0].split("_");
    if (verArray.length > 1) {
        retVal.prefix = verArray[0];
        verArray.splice(0, 1);
    }
    retVal.version = verArray.join("_");
    return retVal;
}
