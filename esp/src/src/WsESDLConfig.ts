import * as ESPRequest from "./ESPRequest";

export function ListDESDLEspBindings(params) {
    return ESPRequest.send("WsESDLConfig", "ListDESDLEspBindings", params);
}

export function DeleteESDLBinding(params) {
    return ESPRequest.send("WsESDLConfig", "DeleteESDLBinding", params);
}

export function GetESDLBinding(params) {
    return ESPRequest.send("WsESDLConfig", "GetESDLBinding", params);
}

export function ConfigureESDLBindingMethod(params) {
    return ESPRequest.send("WsESDLConfig", "ConfigureESDLBindingMethod", params);
}

export function ListESDLDefinitions(params) {
    return ESPRequest.send("WsESDLConfig", "ListESDLDefinitions", params);
}

export function GetESDLDefinition(params) {
    return ESPRequest.send("WsESDLConfig", "GetESDLDefinition", params);
}

export function DeleteESDLDefinition(params) {
    return ESPRequest.send("WsESDLConfig", "DeleteESDLDefinition", params);
}

export function PublishESDLBinding(params) {
    return ESPRequest.send("WsESDLConfig", "PublishESDLBinding", params);
}

// post 1.3 services

export function ListESDLBindings(params) {
    return ESPRequest.send("WsESDLConfig", "ListESDLBindings", params);
}
