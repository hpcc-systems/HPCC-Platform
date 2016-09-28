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
    "hpcc/ESPRequest"
], function (ESPRequest) {

    return {
        WUGetXref: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefList", params);
        },
        DFUXRefBuild: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefBuild", params);
        },
        DFUXRefUnusedFiles: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefUnusedFiles", params);
        },
        DFUXRefFoundFiles: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefFoundFiles", params);
        },
        DFUXRefOrphanFiles: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefOrphanFiles", params);
        },
        DFUXRefMessages: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefMessages", params);
        },
        DFUXRefCleanDirectories: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefCleanDirectories", params);
        },
        DFUXRefLostFiles: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefLostFiles", params);
        },
        DFUXRefDirectories: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefDirectories", params);
        },
        DFUXRefBuildCancel: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefBuildCancel", params);
        }
    };
});