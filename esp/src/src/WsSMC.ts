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

    "hpcc/ESPRequest"

], function (declare,
    ESPRequest) {

    return {
        Activity: function (params) {
            return ESPRequest.send("WsSMC", "Activity", params);
        },
        GetStatusServerInfo: function (params) {
            return ESPRequest.send("WsSMC", "GetStatusServerInfo", params);
        },
        PauseQueue: function (params) {
            return ESPRequest.send("WsSMC", "PauseQueue", params);
        },
        ResumeQueue: function (params) {
            return ESPRequest.send("WsSMC", "ResumeQueue", params);
        },
        ClearQueue: function (params) {
            return ESPRequest.send("WsSMC", "ClearQueue", params);
        },
        SetJobPriority: function (params) {
            return ESPRequest.send("WsSMC", "SetJobPriority", params);
        },
        MoveJobFront: function (params) {
            return ESPRequest.send("WsSMC", "MoveJobFront", params);
        },
        MoveJobUp: function (params) {
            return ESPRequest.send("WsSMC", "MoveJobUp", params);
        },
        MoveJobDown: function (params) {
            return ESPRequest.send("WsSMC", "MoveJobDown", params);
        },
        MoveJobBack: function (params) {
            return ESPRequest.send("WsSMC", "MoveJobBack", params);
        },
        parseBuildString: function (build) {
            var retVal = {
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
            var verArray = build.split("[");
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
    };
});

