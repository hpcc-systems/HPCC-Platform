import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";
import * as arrayUtil from "dojo/_base/array";
import * as QueryResults from "dojo/store/util/QueryResults";
import * as Observable from "dojo/store/Observable";

import * as ESPRequest from "./ESPRequest";
import * as ESPUtil from "./ESPUtil";

declare const dojo;

var FileListStore = declare([ESPRequest.Store], {
    service: "FileSpray",
    action: "FileList",
    responseQualifier: "FileListResponse.files.PhysicalFileStruct",
    idProperty: "calculatedID",
    create: function (id) {
        var retVal = {
            lfEncode: function (path) {
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
                return "~file::" + this.NetAddress + this.lfEncode(this.fullPath);
            }
        };
        retVal[this.idProperty] = id;
        return retVal;
    },
    preProcessRow: function (row) {
        var fullPath = this.parent.fullPath + row.name + (row.isDir ? "/" : "");
        var fullFolderPathParts = fullPath.split("/");
        fullFolderPathParts.pop();
        lang.mixin(row, {
            calculatedID: this.parent.NetAddress + fullPath,
            NetAddress: this.parent.NetAddress,
            OS: this.parent.OS,
            fullPath: fullPath,
            fullFolderPath: fullFolderPathParts.join("/"),
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

var LandingZonesFilterStore = declare([ESPRequest.Store], {
    service: "FileSpray",
    action: "DropZoneFileSearch",
    responseQualifier: "DropZoneFileSearchResponse.Files.PhysicalFileStruct",
    idProperty: "calculatedID",
    constructor: function (options) {
        if (options) {
            declare.safeMixin(this, options);
        }
    },
    preProcessRow: function (row) {
        var fullPath = row.Path + "/" + row.name;
        lang.mixin(row, {
            DropZone: {
                NetAddress: this.dropZone.machine.Netaddress
            },
            calculatedID: this.dropZone.machine.Netaddress + fullPath,
            fullPath: fullPath,
            fullFolderPath: row.Path,
            displayName: row.name,
            type: row.isDir ? "folder" : "file"
        });
    }
});

var LandingZonesStore = declare([ESPRequest.Store], {
    service: "WsTopology",
    action: "TpDropZoneQuery",
    responseQualifier: "TpDropZoneQueryResponse.TpDropZones.TpDropZone",
    idProperty: "calculatedID",
    constructor: function (options) {
        if (options) {
            declare.safeMixin(this, options);
        }
        this.userAddedFiles = {};
    },
    query: ESPUtil.override(function (inherited, query, options) {
        if (!query.filter) {
            return inherited(query, options);
        }
        var landingZonesFilterStore = new LandingZonesFilterStore({ dropZone: query.filter.__dropZone });
        delete query.filter.__dropZone;
        return landingZonesFilterStore.query(query.filter, options);
    }),
    addUserFile: function (_file) {
        var fileListStore = new FileListStore({
            parent: null
        });
        var file = fileListStore.get(_file.calculatedID);
        fileListStore.update(_file.calculatedID, _file);
        this.userAddedFiles[file.calculatedID] = file;
    },
    postProcessResults: function (items) {
        for (var key in this.userAddedFiles) {
            items.push(this.userAddedFiles[key]);
        }
    },
    preRequest: function (request) {
        request.ECLWatchVisibleOnly = true
    },
    preProcessRow: function (row) {
        lang.mixin(row, {
            OS: row.Linux === "true" ? 2 : 0
        });
        lang.mixin(row, {
            calculatedID: row.Path + "_" + row.Name,
            displayName: row.Name,
            type: "dropzone",
            fullPath: row.Path + (row.Path && !this.endsWith(row.Path, "/") ? "/" : ""),
            DropZone: row
        });
    },
    mayHaveChildren: function (item) {
        switch (item.type) {
            case "dropzone":
            case "folder":
            case "machine":
                return true;
        }
        return false;
    },
    getChildren: function (parent, options) {
        var children = [];
        if (parent.TpMachines) {
            arrayUtil.forEach(parent.TpMachines.TpMachine, function (item, idx) {
                children.push({
                    calculatedID: item.Netaddress,
                    displayName: item.ConfigNetaddress !== item.Netaddress ? item.ConfigNetaddress + " [" + item.Netaddress + "]" : item.ConfigNetaddress,
                    NetAddress: item.Netaddress,
                    ConfigNetaddress: item.ConfigNetaddress,
                    type: "machine",
                    isMachine: true,
                    isDir: false,
                    OS: item.OS,
                    fullPath: parent.fullPath,
                    DropZone: parent.DropZone
                });
            });
            return QueryResults(children);
        } else if (parent.isMachine || parent.isDir) {
            var store = Observable(new FileListStore({
                parent: parent
            }));
            return store.query({
                Netaddr: parent.NetAddress,
                Path: parent.fullPath,
                Mask: "",
                OS: parent.OS
            });
        }
    }
});

export var LogFileStore = declare([ESPRequest.Store], {
    service: "FileSpray",
    action: "FileList",
    responseQualifier: "FileListResponse.files.PhysicalFileStruct",
    idProperty: ""
});


export const States = {
    0: "unknown",
    1: "scheduled",
    2: "queued",
    3: "started",
    4: "aborted",
    5: "failed",
    6: "finished",
    7: "monitoring",
    8: "aborting",
    999: "not found"
};

export function isComplete(state) {
    switch (state) {
        case 4:
        case 5:
        case 6:
        case 999:
            return true;
    }
    return false;
}

export const OS_TYPE = {
    OS_WINDOWS: 0,
    OS_SOLARIS: 1,
    OS_LINUX: 2
};

export const CommandMessages = {
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
};

export const FormatMessages = {
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
};

export function CreateLandingZonesStore(options) {
    var store = new LandingZonesStore(options);
    return Observable(store);
}

export function CreateFileListStore(options) {
    var store = new FileListStore(options);
    return Observable(store);
}

export function CreateLandingZonesFilterStore(options) {
    var store = new LandingZonesFilterStore(options);
    return Observable(store);
}

export function GetDFUWorkunits(params) {
    return ESPRequest.send("FileSpray", "GetDFUWorkunits", params);
}

export function DFUWorkunitsAction(workunits, actionType, callback) {
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
}
export function SprayFixed(params) {
    return ESPRequest.send("FileSpray", "SprayFixed", params);
}
export function SprayVariable(params) {
    return ESPRequest.send("FileSpray", "SprayVariable", params);
}
export function Despray(params) {
    return ESPRequest.send("FileSpray", "Despray", params);
}
export function Replicate(params) {
    return ESPRequest.send("FileSpray", "Replicate", params);
}
export function Copy(params) {
    return ESPRequest.send("FileSpray", "Copy", params);
}
export function Rename(params) {
    return ESPRequest.send("FileSpray", "Rename", params);
}
export function GetDFUWorkunit(params) {
    return ESPRequest.send("FileSpray", "GetDFUWorkunit", params).then(function (response) {
        if (lang.exists("Exceptions.Exception", response)) {
            arrayUtil.forEach(response.Exceptions.Exception, function (item, idx) {
                if (item.Code === 20080) {
                    lang.mixin(response, {
                        GetDFUWorkunitResponse: {
                            result: {
                                Wuid: params.request.Wuid,
                                State: 999,
                                StateMessage: "not found"
                            }
                        }
                    });
                }
            });
        }
        return response;
    });
}
export function UpdateDFUWorkunit(params) {
    return ESPRequest.send("FileSpray", "UpdateDFUWorkunit", params);
}
export function AbortDFUWorkunit(params) {
    return ESPRequest.send("FileSpray", "AbortDFUWorkunit", params);
}
export function DFUWUFile(params) {
    lang.mixin(params, {
        handleAs: "text"
    });
    return ESPRequest.send("FileSpray", "DFUWUFile", params);
}
export function FileList(params) {
    return ESPRequest.send("FileSpray", "FileList", params);
}
export function DropZoneFiles(params) {
    return ESPRequest.send("FileSpray", "DropZoneFiles", params);
}
export function DeleteDropZoneFile(params) {
    // Single File Only
    return ESPRequest.send("FileSpray", "DeleteDropZoneFiles", params).then(function (response) {
        if (lang.exists("DFUWorkunitsActionResponse.DFUActionResults.DFUActionResult", response)) {
            var resultID = response.DFUWorkunitsActionResponse.DFUActionResults.DFUActionResult[0].ID;
            var resultMessage = response.DFUWorkunitsActionResponse.DFUActionResults.DFUActionResult[0].Result;
            if (resultMessage.indexOf("Success") === 0) {
                dojo.publish("hpcc/brToaster", {
                    Severity: "Message",
                    Source: "FileSpray.DeleteDropZoneFiles",
                    Exceptions: [{ Source: "Delete " + resultID, Message: resultMessage }]
                });
            } else {
                dojo.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: "FileSpray.DeleteDropZoneFiles",
                    Exceptions: [{ Source: "Delete " + resultID, Message: resultMessage }]
                });
            }
        }
        return response;
    });
}
export function GetSprayTargets(params) {
    return ESPRequest.send("FileSpray", "GetSprayTargets", params);
}
