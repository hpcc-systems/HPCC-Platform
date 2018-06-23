import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";
import * as arrayUtil from "dojo/_base/array";
import * as Memory from "dojo/store/Memory";
import * as Observable from "dojo/store/Observable";
import * as QueryResults from "dojo/store/util/QueryResults";
import * as topic from "dojo/topic";

import * as parser from "dojox/xml/parser";

import ESPBase from "./ESPBase";

import * as ESPRequest from "./ESPRequest";

var DiskUsageStore = declare([Memory], {

    constructor: function () {
        this.idProperty = "__hpcc_id";
    },

    query: function (query, options) {
        switch (query.CountBy) {
            case "Year":
            case "Quarter":
            case "Month":
            case "Day":
                query.Interval = query.CountBy;
                query.CountBy = "Date";
                break;
        }
        var results = DFUSpace({
            request: query
        }).then(lang.hitch(this, function (response) {
            var data = [];
            if (lang.exists("DFUSpaceResponse.DFUSpaceItems.DFUSpaceItem", response)) {
                arrayUtil.forEach(response.DFUSpaceResponse.DFUSpaceItems.DFUSpaceItem, function (item, idx) {
                    data.push(lang.mixin(item, {
                        __hpcc_id: item.Name
                    }));
                }, this);
            }
            if (options.sort && options.sort.length) {
                data.sort(function (_l, _r) {
                    var l = _l[options.sort[0].attribute];
                    var r = _r[options.sort[0].attribute];
                    if (l === r) {
                        return 0;
                    }
                    switch (options.sort[0].attribute) {
                        case "TotalSize":
                        case "LargestSize":
                        case "SmallestSize":
                        case "NumOfFiles":
                        case "NumOfFilesUnknown":
                            l = parseInt(l.split(",").join(""));
                            r = parseInt(r.split(",").join(""));
                    }
                    if (options.sort[0].descending) {
                        return r < l ? -1 : 1;
                    }
                    return l < r ? -1 : 1;
                })
            }
            this.setData(data);
            return this.data;
        }));
        return QueryResults(results);
    }
});

export function CreateDiskUsageStore() {
    var store = new DiskUsageStore();
    return Observable(store);
}

export function DFUArrayAction(logicalFiles, actionType) {
    arrayUtil.forEach(logicalFiles, function (item, idx) {
        if (item.isSuperfile) {
            item.qualifiedName = item.Name;
        } else {
            item.qualifiedName = item.Name + "@" + item.NodeGroup;
        }
    });
    var request = {
        LogicalFiles: logicalFiles,
        Type: actionType
    };
    ESPRequest.flattenArray(request, "LogicalFiles", "qualifiedName");

    return ESPRequest.send("WsDfu", "DFUArrayAction", {
        request: request
    }).then(function (response) {
        if (lang.exists("DFUArrayActionResponse.ActionResults.DFUActionInfo", response)) {
            var exceptions = [];
            arrayUtil.forEach(response.DFUArrayActionResponse.ActionResults.DFUActionInfo, function (item, idx) {
                if (item.Failed) {
                    exceptions.push({
                        Source: item.FileName,
                        Message: item.ActionResult
                    });
                }
            });
            if (exceptions.length) {
                topic.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: "WsDfu.DFUArrayAction",
                    Exceptions: exceptions
                });
            }
        }
        return response;
    });
}

export function SuperfileAction(action, superfile, subfiles, removeSuperfile) {
    var request = {
        action: action,
        superfile: superfile,
        subfiles: subfiles,
        removeSuperfile: removeSuperfile
    };
    ESPRequest.flattenArray(request, "subfiles", "Name");

    return ESPRequest.send("WsDfu", "SuperfileAction", {
        request: request
    });
}

export function AddtoSuperfile(logicalFiles, superfile, existingFile) {
    var request = {
        names: logicalFiles,
        Superfile: superfile,
        ExistingFile: existingFile ? 1 : 0
    };
    ESPRequest.flattenArray(request, "names", "Name");

    return ESPRequest.send("WsDfu", "AddtoSuperfile", {
        request: request
    });
}

export function DFUQuery(params) {
    return ESPRequest.send("WsDfu", "DFUQuery", params);
}

export function DFUFileView(params) {
    return ESPRequest.send("WsDfu", "DFUFileView", params);
}

export function DFUSpace(params) {
    return ESPRequest.send("WsDfu", "DFUSpace", params);
}

export function ListHistory(params) {
    return ESPRequest.send("WsDfu", "ListHistory", params);
}

export function EraseHistory(params) {
    return ESPRequest.send("WsDfu", "EraseHistory", params);
}

export function DFUInfo(params) {
    return ESPRequest.send("WsDfu", "DFUInfo", params).then(function (response) {
        if (lang.exists("Exceptions.Exception", response)) {
            arrayUtil.forEach(response.Exceptions.Exception, function (item, idx) {
                if (item.Code === 20038) {
                    lang.mixin(response, {
                        DFUInfoResponse: {
                            FileDetail: {
                                Name: params.request.Name,
                                StateID: 999,
                                State: "not found"
                            }
                        }
                    });
                }
            });
        } else if (lang.exists("DFUInfoResponse.FileDetail", response)) {
            response.DFUInfoResponse.FileDetail.StateID = 0;
            response.DFUInfoResponse.FileDetail.State = "";
        }
        return response;
    });
}

export function DFUDefFile(params) {
    lang.mixin(params, {
        handleAs: "text"
    });
    return ESPRequest.send("WsDfu", "DFUDefFile", params).then(function (response) {
        try {
            var domXml = parser.parse(response);
            var espBase = new ESPBase();
            var exceptions = espBase.getValues(domXml, "Exception", ["Exception"]);
            if (exceptions.length) {
                response = "";
                arrayUtil.forEach(exceptions, function (item, idx) {
                    response += item.Message + "\n";
                });
            }
        } catch (e) {
            //  No errors  ---
        }
        return response;
    });
}
