import * as arrayUtil from "dojo/_base/array";
import * as lang from "dojo/_base/lang";

import * as ESPRequest from "./ESPRequest";

declare const dojo;
export function WUGetXref(params) {
    return ESPRequest.send("WsDFUXRef", "DFUXRefList", params);
}
export function DFUXRefBuild(params) {
    return ESPRequest.send("WsDFUXRef", "DFUXRefBuild", params);
}
export function DFUXRefUnusedFiles(params) {
    return ESPRequest.send("WsDFUXRef", "DFUXRefUnusedFiles", params);
}
export function DFUXRefFoundFiles(params) {
    const request = {
        Cluster: params
    };
    return ESPRequest.send("WsDFUXRef", "DFUXRefFoundFiles", {
        request
    }).then(function (response) {
        const newRows = [];
        if (lang.exists("DFUXRefFoundFilesQueryResponse.DFUXRefFoundFilesQueryResult.File", response)) {
            const results = response.DFUXRefFoundFilesQueryResponse.DFUXRefFoundFilesQueryResult.File;
            if (results.length) {
                arrayUtil.forEach(results, function (row, idx) {
                    newRows.push({
                        Name: row.Partmask,
                        Modified: row.Modified,
                        Parts: row.Numparts,
                        Size: row.Size
                    });
                });
            } else if (results.Partmask) {
                newRows.push({
                    Name: results.Partmask,
                    Modified: results.Modified,
                    Parts: results.Numparts,
                    Size: results.Size
                });
            }
        }
        return newRows;
    });
}
export function DFUXRefOrphanFiles(params) {
    const request = {
        Cluster: params
    };
    return ESPRequest.send("WsDFUXRef", "DFUXRefOrphanFiles", {
        request
    }).then(function (response) {
        const newRows = [];
        if (lang.exists("DFUXRefOrphanFilesQueryResponse.DFUXRefOrphanFilesQueryResult.File", response)) {
            const results = response.DFUXRefOrphanFilesQueryResponse.DFUXRefOrphanFilesQueryResult.File;
            if (results.length) {
                arrayUtil.forEach(results, function (row, idx) {
                    newRows.push({
                        Name: row.Partmask,
                        Modified: row.Modified,
                        PartsFound: row.Partsfound,
                        TotalParts: row.Numparts,
                        Size: row.Size
                    });
                });
            } else if (results.Partmask) {
                newRows.push({
                    Name: results.Partmask,
                    Modified: results.Modified,
                    PartsFound: results.Partsfound,
                    TotalParts: results.Numparts,
                    Size: results.Size
                });
            }
        }
        return newRows;
    });
}
export function DFUXRefMessages(params) {
    return ESPRequest.send("WsDFUXRef", "DFUXRefMessages", params);
}
export function DFUXRefCleanDirectories(params) {
    return ESPRequest.send("WsDFUXRef", "DFUXRefCleanDirectories", params);
}
export function DFUXRefLostFiles(params) {
    const request = {
        Cluster: params
    };
    return ESPRequest.send("WsDFUXRef", "DFUXRefLostFiles", {
        request
    }).then(function (response) {
        const newRows = [];
        if (lang.exists("DFUXRefLostFilesQueryResponse.DFUXRefLostFilesQueryResult.File", response)) {
            const results = response.DFUXRefLostFilesQueryResponse.DFUXRefLostFilesQueryResult.File;
            if (results.length) {
                arrayUtil.forEach(results, function (row, idx) {
                    newRows.push({
                        Name: row.Name,
                        Modified: row.Modified,
                        Numparts: row.Numparts,
                        Size: row.Size,
                        Partslost: row.Partslost,
                        Primarylost: row.Primarylost,
                        Replicatedlost: row.Replicatedlost
                    });
                });
            } else if (results.Name) {
                newRows.push({
                    Name: results.Name,
                    Modified: results.Modified,
                    Numparts: results.Numparts,
                    Size: results.Size,
                    Partslost: results.Partslost,
                    Primarylost: results.Primarylost,
                    Replicatedlost: results.Replicatedlost
                });
            }
        }
        return newRows;
    });

}
export function DFUXRefDirectories(params) {
    return ESPRequest.send("WsDFUXRef", "DFUXRefDirectories", params);
}
export function DFUXRefBuildCancel(params) {
    return ESPRequest.send("WsDFUXRef", "DFUXRefBuildCancel", params);
}
export function DFUXRefArrayAction(xrefFiles, actionType, cluster, type) {
    arrayUtil.forEach(xrefFiles, function (item, idx) {
        item.qualifiedName = item.Name;
    });
    const request = {
        XRefFiles: xrefFiles,
        Action: actionType,
        Cluster: cluster,
        Type: type
    };
    ESPRequest.flattenArray(request, "XRefFiles", "qualifiedName");
    return ESPRequest.send("WsDFUXRef", "DFUXRefArrayAction", {
        request
    }).then(function (response) {
        if (lang.exists("DFUXRefArrayActionResponse.DFUXRefArrayActionResult", response)) {
            if (response.DFUXRefArrayActionResponse.DFUXRefArrayActionResult.Value) {
                dojo.publish("hpcc/brToaster", {
                    Severity: "Message",
                    Source: "WsDfu.DFUXRefArrayAction",
                    Exceptions: [{ Message: response.DFUXRefArrayActionResponse.DFUXRefArrayActionResult.Value }]
                });
            }
        }
        return response;
    });
}
