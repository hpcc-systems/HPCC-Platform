import * as lang from "dojo/_base/lang";
import * as topic from "dojo/topic";

import * as ESPRequest from "./ESPRequest";

export function checkError(response, sourceMethod, showOkMsg) {
    const retCode = lang.getObject(sourceMethod + "Response.retcode", false, response);
    const retMsg = lang.getObject(sourceMethod + "Response.message", false, response);
    if (retCode) {
        topic.publish("hpcc/brToaster", {
            Severity: "Error",
            Source: "WsAccount." + sourceMethod,
            Exceptions: [{ Message: retMsg }]
        });
    } else if (showOkMsg && retMsg) {
        topic.publish("hpcc/brToaster", {
            Severity: "Message",
            Source: "WsAccount." + sourceMethod,
            Exceptions: [{ Message: retMsg }]
        });
    }
}

export function UpdateUser(params) {
    const context = this;
    return ESPRequest.send("ws_account", "UpdateUser", params).then(function (response) {
        context.checkError(response, "UpdateUser", params ? params.showOkMsg : false);
        return response;
    });
}

export function UpdateUserInput(params) {
    return ESPRequest.send("ws_account", "UpdateUserInput", params);
}

export function MyAccount(params) {
    return ESPRequest.send("ws_account", "MyAccount", params);
}

export function Unlock(params) {
    lang.mixin(params, {
        handleAs: "json"
    });
    return ESPRequest.send("esp", "unlock", params);
}

export function Lock(params) {
    lang.mixin(params, {
        handleAs: "json"
    });
    return ESPRequest.send("esp", "lock", params);
}
