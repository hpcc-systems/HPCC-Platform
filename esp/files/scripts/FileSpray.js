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
    "dojo/_base/Deferred",
    "dojo/store/util/QueryResults",
    "dojo/store/JsonRest",
    "dojo/store/Memory",
    "dojo/store/Cache",
    "dojo/store/Observable",

    "dojox/xml/parser",

    "hpcc/ESPBase",
    "hpcc/ESPRequest"
], function (declare, lang, arrayUtil, Deferred, QueryResults, JsonRest, Memory, Cache, Observable,
    parser,
    ESPBase, ESPRequest) {
    var FileListStore = declare([ESPRequest.Store], {
        service: "FileSpray",
        action: "FileList",
        responseQualifier: "files.PhysicalFileStruct",
        idProperty: "calculatedID",
        create: function (id) {
            var retVal = {
                lfEncode: function(path) {
                    var retVal = "";
                    for (var i = 0; i < path.length; ++i) {
                        switch (path[i]) {
                            case "/":
                            case "\\":
                                retVal += "::";
                                break;
                            case "A":
                            case "B":
                            case "C":
                            case "D":
                            case "E":
                            case "F":
                            case "G":
                            case "H":
                            case "I":
                            case "J":
                            case "K":
                            case "L":
                            case "M":
                            case "N":
                            case "O":
                            case "P":
                            case "Q":
                            case "R":
                            case "S":
                            case "T":
                            case "U":
                            case "V":
                            case "W":
                            case "X":
                            case "Y":
                            case "Z":
                                retVal += "^" + path[i];
                                break;
                            default:
                                retVal += path[i];
                        }
                    }
                    return retVal;
                },
                getLogicalFile: function () {
                    //var filePath = this.DropZone.Path + "/" + 
                    return "~file::" + this.DropZone.NetAddress + this.lfEncode(this.fullPath);
                }
            };
            retVal[this.idProperty] = id;
            return retVal;
        },
        preProcessRow: function (row) {
            var fullPath = this.parent.fullPath + row.name + (row.isDir ? "/" : "");
            lang.mixin(row, {
                calculatedID: this.parent.DropZone.NetAddress + fullPath,
                fullPath: fullPath,
                DropZone: this.parent.DropZone,
                displayName: row.name,
                type: row.isDir ? "folder" : "file"
            });
        },
        postProcessResults: function (items) {
            items.sort(function (l, r) {
                if (l.isDir === r.isDir) {
                    if (l.displayName === r.displayName)
                        return 0;
                    else if (l.displayName < r.displayName)
                        return -1;
                    return 1;
                } else if (l.isDir) {
                    return -1;
                }
                return 1;
            });
        }
    });

    var LandingZonesStore = declare([ESPRequest.Store], {
        service: "FileSpray",
        action: "DropZoneFiles",
        responseQualifier: "DropZones.DropZone",
        idProperty: "calculatedID",
        constructor: function (options) {
            declare.safeMixin(this, options);
        },
        preProcessRow: function (row) {
            lang.mixin(row, {
                calculatedID: row.NetAddress,
                displayName: row.Name,
                type: "dropzone",
                fullPath: row.Path + "/",
                DropZone: row
            });
        },
        mayHaveChildren: function (item) {
            switch (item.type) {
                case "dropzone":
                case "folder":
                    return true;
            }
            return false;
        },
        getChildren: function (parent, options) {
            var store = Observable(new FileListStore({
                parent: parent
            }));
            return store.query({
                Netaddr: parent.DropZone.NetAddress,
                Path: parent.fullPath,
                Mask: "",
                OS: parent.DropZone.Linux === "true" ? 1 : 0
            });
        }
    });

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

        isComplete: function (state) {
            switch (state) {
                case 4:
                case 5:
                case 6:
                    return true;
            }
            return false;
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

        CreateLandingZonesStore: function (options) {
            var store = new LandingZonesStore(options);
            return Observable(store);
        },

        CreateFileListStore: function (options) {
            var store = new FileListStore(options);
            return Observable(store);
        },

        GetDFUWorkunits: function (params) {
            return ESPRequest.send("FileSpray", "GetDFUWorkunits", params);
        },

        DFUWorkunitsAction: function (workunits, actionType, callback) {
            var request = {
                wuids: workunits,
                Type: actionType
            };
            ESPRequest.flattenArray(request, "wuids", "ID");

            return ESPRequest.send("FileSpray", "DFUWorkunitsAction", {
                request: request,
                load: function (response) {
                    arrayUtil.forEach(workunits, function (item, index) {
                        item.refresh();
                    });
                    /*  TODO:  Revisit after HPCC-9241 is fixed
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
                    */
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
        FileList: function (params) {
            return ESPRequest.send("FileSpray", "FileList", params);
        },
        DropZoneFiles: function (params) {
            return ESPRequest.send("FileSpray", "DropZoneFiles", params);
        }
    };
});

