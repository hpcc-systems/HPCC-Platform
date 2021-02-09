import * as arrayUtil from "dojo/_base/array";
import * as lang from "dojo/_base/lang";
import * as Observable from "dojo/store/Observable";
import * as QueryResults from "dojo/store/util/QueryResults";

import * as ESPRequest from "./ESPRequest";

declare const dojo;

class FileListStore extends ESPRequest.Store {

    service = "FileSpray";
    action = "FileList";
    responseQualifier = "FileListResponse.files.PhysicalFileStruct";
    responseTotalQualifier = undefined;
    idProperty = "calculatedID";

    parent: any;

    create(id) {
        const retVal = {
            lfEncode(path) {
                let retVal = "";
                for (let i = 0; i < path.length; ++i) {
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
            getLogicalFile() {
                return "~file::" + this.NetAddress + this.lfEncode(this.fullPath);
            }
        };
        retVal[this.idProperty] = id;
        return retVal;
    }

    preProcessRow(row) {
        const fullPath = this.parent.fullPath + row.name + (row.isDir ? "/" : "");
        const fullFolderPathParts = fullPath.split("/");
        fullFolderPathParts.pop();
        lang.mixin(row, {
            calculatedID: this.parent.NetAddress + fullPath,
            NetAddress: this.parent.NetAddress,
            OS: this.parent.OS,
            fullPath,
            fullFolderPath: fullFolderPathParts.join("/"),
            DropZone: this.parent.DropZone,
            displayName: row.name,
            type: row.isDir ? "folder" : "file"
        });
    }

    postProcessResults(items) {
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
}

class LandingZonesFilterStore extends ESPRequest.Store {

    service = "FileSpray";
    action = "DropZoneFileSearch";
    responseQualifier = "DropZoneFileSearchResponse.Files.PhysicalFileStruct";
    responseTotalQualifier = undefined;
    idProperty = "calculatedID";
    dropZone: any;

    constructor(options?) {
        super(options);
    }

    preProcessRow(row) {
        const fullPath = this.dropZone.machine.Directory + "/" + (row.Path === null ? "" : (row.Path + "/"));
        lang.mixin(row, {
            NetAddress: this.dropZone.machine.Netaddress,
            Directory: this.dropZone.machine.Directory,
            calculatedID: this.dropZone.machine.Netaddress + fullPath,
            OS: this.dropZone.machine.OS,
            fullFolderPath: fullPath,
            displayName: row.Path ? (row.Path + "/" + row.name) : row.name,
            type: row.isDir ? "filteredFolder" : "file"
        });
    }
}

class LandingZonesStore extends ESPRequest.Store {

    service = "WsTopology";
    action = "TpDropZoneQuery";
    responseQualifier = "TpDropZoneQueryResponse.TpDropZones.TpDropZone";
    responseTotalQualifier = undefined;
    idProperty = "calculatedID";

    userAddedFiles: object = {};

    constructor(options?) {
        super(options);
    }

    query(query, options) {
        if (!query.filter) {
            return super.query(query, options);
        }
        const landingZonesFilterStore = new LandingZonesFilterStore({ dropZone: query.filter.__dropZone, server: query.filter.Server });
        delete query.filter.__dropZone;
        return landingZonesFilterStore.query(query.filter, options);
    }

    addUserFile(_file) {
        //  Just add a file "reference" so it can be remotely sprayed etc.
        const fileListStore = new FileListStore({
            parent: null
        });
        _file._isUserFile = true;
        const file = fileListStore.get(_file.calculatedID);
        fileListStore.update(_file.calculatedID, _file);
        this.userAddedFiles[file.calculatedID] = file;
    }

    removeUserFile(_file) {
        const fileListStore = new FileListStore({
            parent: null
        });
        fileListStore.remove(_file.calculatedID);
        delete this.userAddedFiles[_file.calculatedID];
    }

    postProcessResults(items) {
        for (const key in this.userAddedFiles) {
            items.push(this.userAddedFiles[key]);
        }
    }

    preRequest(request) {
        request.ECLWatchVisibleOnly = true;
    }

    preProcessRow(row) {
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
    }

    mayHaveChildren(item) {
        switch (item.type) {
            case "dropzone":
            case "folder":
            case "machine":
                return true;
        }
        return false;
    }

    getChildren(parent, options) {
        const children = [];
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
            const store = new Observable(new FileListStore({
                parent
            }));
            return store.query({
                Netaddr: parent.NetAddress,
                Path: parent.fullPath,
                Mask: "",
                OS: parent.OS
            });
        }
    }
}

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
    const store = new LandingZonesStore(options);
    return new Observable(store);
}

export function CreateFileListStore(options) {
    const store = new FileListStore(options);
    return new Observable(store);
}

export function CreateLandingZonesFilterStore(options) {
    const store = new LandingZonesFilterStore(options);
    return new Observable(store);
}

export function GetDFUWorkunits(params) {
    return ESPRequest.send("FileSpray", "GetDFUWorkunits", params);
}

export function DFUWorkunitsAction(workunits, actionType, callback?) {
    const request = {
        wuids: workunits,
        Type: actionType
    };
    ESPRequest.flattenArray(request, "wuids", "ID");

    return ESPRequest.send("FileSpray", "DFUWorkunitsAction", {
        request,
        load(response) {
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
        error(err) {
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
            const resultID = response.DFUWorkunitsActionResponse.DFUActionResults.DFUActionResult[0].ID;
            const resultMessage = response.DFUWorkunitsActionResponse.DFUActionResults.DFUActionResult[0].Result;
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
