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
    "dojo/_base/Deferred",
    "dojo/store/util/QueryResults",
    "dojo/store/JsonRest", 
    "dojo/store/Memory", 
    "dojo/store/Cache", 
    "dojo/store/Observable",
    
    "dojox/xml/parser",    

    "hpcc/ESPBase",
    "hpcc/ESPRequest"
], function (declare, lang, Deferred, QueryResults, JsonRest, Memory, Cache, Observable,
    parser,
    ESPBase, ESPRequest) {
    return {
        States: {
            0: "unknown",
            1: "scheduled",
            2: "queued",
            3: "started",
            4: "aborted",
            5: "failed",
            6: "finished",
            7: "monitoring",
            8: "aborting"
        },

        CommandMessages: {
            1: "Copy",
            2: "Remove",
            3: "Move",
            4: "Rename",
            5: "Replicate",
            6: "Spray (Import)",
            7: "Despray (Export)",
            8: "Add",
            9: "Transfer",
            10: "Save Map",
            11: "Add Group",
            12: "Server",
            13: "Monitor",
            14: "Copy Merge",
            15: "Super Copy"
        },

        FormatMessages: {
            0: "fixed",
            1: "csv",
            2: "utf8",
            3: "utf8n",
            4: "utf16",
            5: "utf16le",
            6: "utf16be",
            7: "utf32",
            8: "utf32le",
            9: "utf32be",
            10: "variable",
            11: "recfmvb",
            12: "recfmv",
            13: "variablebigendian"
        },

        GetDFUWorkunits: function (params) {
            return ESPRequest.send("FileSpray", "GetDFUWorkunits", params);
        },

        DFUWorkunitsAction: function (wuids, actionType, callback) {
            var request = {
                wuids: wuids,
                Type: actionType
            };
            ESPRequest.flattenArray(request, "wuids", "ID");

            return ESPRequest.send("FileSpray", "DFUWorkunitsAction", {
                request: request,
                load: function (response) {
                    if (lang.exists("DFUWorkunitsActionResponse.ActionResults.WUActionResult", response)) {
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
        Despray: function (params) {
            return ESPRequest.send("FileSpray", "Despray", params);
        },
        Copy: function (params) {
            return ESPRequest.send("FileSpray", "Copy", params);
        },
        Rename: function (params) {
            return ESPRequest.send("FileSpray", "Rename", params);
        },
        GetDFUWorkunit: function (params) {
            return ESPRequest.send("FileSpray", "GetDFUWorkunit", params);
        },
        UpdateDFUWorkunit: function (params) {
            return ESPRequest.send("FileSpray", "UpdateDFUWorkunit", params);
        },
        DFUWUFile: function (params) {
            lang.mixin(params, {
                handleAs: "text"
            });
            return ESPRequest.send("FileSpray", "DFUWUFile", params);
        },
        isComplete: function (state) {
            switch (state) {
                case 4:	
                case 5:	
                case 6:	
                    return true;
            }
            return false;
        }
    };
});

