/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/topic",
    "dojo/store/JsonRest",
    "dojo/store/Memory",
    "dojo/store/Cache",
    "dojo/store/Observable",

    "hpcc/ESPRequest",
    "hpcc/ws_machine"
], function (declare, lang, arrayUtil, topic, JsonRest, Memory, Cache, Observable,
    ESPRequest, WsMachine) {

    var NagiosStore = declare([ESPRequest.Store], {
        service: "ws_machine",
        action: "GetComponentStatus",
        responseQualifier: "GetComponentStatusResponse.ComponentStatusList.ComponentStatus",
        idProperty: "__hpcc_id"
    });

    var monitorHandle;
    return {
        GetComponentStatus: function (params) {
            return ESPRequest.send("ws_machine", "GetComponentStatus", params);
        },

        GetTargetClusterInfo: function (params) {
            return ESPRequest.send("ws_machine", "GetTargetClusterInfo", params);
        },

        GetMachineInfo: function (params) {
            return ESPRequest.send("ws_machine", "GetMachineInfo", params);
        },

        MonitorComponentStatus: function (params) {
            var prevResponse = null;
            if (!monitorHandle) {
                var context = this;
                monitorHandle = setInterval(function() {
                    context.GetComponentStatus(params).then(function(response) {
                        if (response && response.GetComponentStatusResponse.ComponentStatus) {
                            response.GetComponentStatusResponse.ComponentStatusList.ComponentStatus.forEach(function(row) {
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
        },

        CreateNagiosStore: function (options) {
            var store = new NagiosStore(options);
            return Observable(store);
        }
    };
});