import * as ESPRequest from "./ESPRequest";

export function GetConfigDetails(params) {
    return ESPRequest.send("ws_elk", "GetConfigDetails", params);
}
