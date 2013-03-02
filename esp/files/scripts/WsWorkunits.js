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
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    
    "hpcc/ESPRequest"
], function (declare, lang, arrayUtil,
    ESPRequest) {
    return {
        WUCreate: function (params) {
            return ESPRequest.send("WsWorkunits", "WUCreate", params);
        },

        WUUpdate: function (params) {
            ESPRequest.flattenMap(params.request, "ApplicationValues")
            return ESPRequest.send("WsWorkunits", "WUUpdate", params);
        },

        WUSubmit: function (params) {
            return ESPRequest.send("WsWorkunits", "WUSubmit", params);
        },

        WUResubmit: function (params) {
            return ESPRequest.send("WsWorkunits", "WUResubmit", params);
        },

        WUPublishWorkunit: function (params) {
            return ESPRequest.send("WsWorkunits", "WUPublishWorkunit", params);
        },

        WUQuery: function (params) {
            return ESPRequest.send("WsWorkunits", "WUQuery", params);
        },

        WUInfo: function (params) {
            return ESPRequest.send("WsWorkunits", "WUInfo", params);
        },

        WUGetGraph: function (params) {
            return ESPRequest.send("WsWorkunits", "WUGetGraph", params);
        },

        WUResult: function (params) {
            return ESPRequest.send("WsWorkunits", "WUResult", params);
        },

        WUFile: function (params) {
            lang.mixin(params, {
                handleAs: "text"
            });
            return ESPRequest.send("WsWorkunits", "WUFile", params);
        },

        WUAction: function (wuids, actionType, callback) {
            var request = {
                Wuids: wuids,
                ActionType: actionType
            };
            ESPRequest.flattenArray(request, "Wuids", "Wuid");

            return ESPRequest.send("WsWorkunits", "WUAction", {
                request: request,
                load: function (response) {
                    if (lang.exists("WUActionResponse.ActionResults.WUActionResult", response)) {
                        arrayUtil.forEach(response.WUActionResponse.ActionResults.WUActionResult, function (item, index) {
                            if (item.Result.indexOf("Failed:") === 0) {
                                dojo.publish("hpcc/brToaster", {
                                    message: "<h4>" + item.Action + " " + item.Wuid + "</h4>" + "<p>" + item.Result + "</p>",
                                    type: "error",
                                    duration: -1
                                });
                            } else {
                                dojo.publish("hpcc/brToaster", {
                                    message: "<h4>" + item.Action + " " + item.Wuid + "</h4>" + "<p>" + item.Result + "</p>",
                                    type: "message"
                                });
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
        },

        //  Helpers  ---
        isComplete: function (stateID, actionEx) {
            switch (stateID) {
                case 1: //WUStateCompiled
                    if (actionEx && actionEx == "compile") {
                        return true;
                    }
                    break;
                case 3:	//WUStateCompleted:
                case 4:	//WUStateFailed:
                case 5:	//WUStateArchived:
                case 7:	//WUStateAborted:
                    return true;
            }
            return false;
        }
    };
});

