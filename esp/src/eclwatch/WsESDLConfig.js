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
    "dojo/_base/Deferred",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/store/util/QueryResults",
    "dojo/Evented",

    "hpcc/ESPRequest"
], function (declare, lang, arrayUtil, Deferred, Memory, Observable, QueryResults, Evented,
    ESPRequest) {

    return {
        ListDESDLEspBindings: function (params) {
            handleAs: "text"
            return ESPRequest.send("WsESDLConfig", "ListDESDLEspBindings", params);
        },

        DeleteESDLBinding: function (params) {
            handleAs: "text"
            return ESPRequest.send("WsESDLConfig", "DeleteESDLBinding", params);
        },

        GetESDLBinding: function (params) {
            handleAs: "text"
            return ESPRequest.send("WsESDLConfig", "GetESDLBinding", params);
        },

        ConfigureESDLBindingMethod: function (params) {
            handleAs: "text"
            return ESPRequest.send("WsESDLConfig", "ConfigureESDLBindingMethod", params);
        },

        ListESDLDefinitions: function (params) {
            handleAs: "text"
            return ESPRequest.send("WsESDLConfig", "ListESDLDefinitions", params);
        },

        GetESDLDefinition: function (params) {
            handleAs: "text"
            return ESPRequest.send("WsESDLConfig", "GetESDLDefinition", params);
        },

        PublishESDLBinding: function (params) {
            handleAs: "text"
            return ESPRequest.send("WsESDLConfig", "PublishESDLBinding", params);
        }
    };
});

