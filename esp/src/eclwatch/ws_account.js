/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
    "dojo/_base/lang",
    "dojo/topic",

    "hpcc/ESPRequest"
], function (lang, topic,
    ESPRequest) {
    return {
        checkError: function (response, sourceMethod, showOkMsg) {
            var retCode = lang.getObject(sourceMethod + "Response.retcode", false, response);
            var retMsg = lang.getObject(sourceMethod + "Response.message", false, response);
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
        },

        UpdateUser: function (params) {
            var context = this;
            return ESPRequest.send("ws_account", "UpdateUser", params).then(function (response) {
                context.checkError(response, "UpdateUser", params ? params.showOkMsg : false);
                return response;
            });
        },

        UpdateUserInput: function (params) {
            return ESPRequest.send("ws_account", "UpdateUserInput", params);
        },

        MyAccount: function (params) {
            return ESPRequest.send("ws_account", "MyAccount", params);
        }
    };
});

