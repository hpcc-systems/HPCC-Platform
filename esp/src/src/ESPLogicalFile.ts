import * as declare from "dojo/_base/declare";
import * as arrayUtil from "dojo/_base/array";
import * as lang from "dojo/_base/lang";
import * as Observable from "dojo/store/Observable";
import * as QueryResults from "dojo/store/util/QueryResults";

import * as WsDfu from "./WsDfu";
import * as FileSpray from "./FileSpray";
import * as ESPRequest from "./ESPRequest";
import * as ESPUtil from "./ESPUtil";
import * as ESPResult from "./ESPResult";
import * as Utility from "./Utility";

var _logicalFiles = {};

var createID = function (Cluster, Name) {
    return (Cluster ? Cluster : "") + "--" + Name;
};

var create = function (id) {
    if (!lang.exists(id, _logicalFiles)) {
        var idParts = id.split("--");
        _logicalFiles[id] = new LogicalFile({
            Cluster: idParts[0] ? idParts[0] : "",
            NodeGroup: idParts[0] ? idParts[0] : "",
            Name: idParts[1]
        });
    }
    return _logicalFiles[id];
};

var Store = declare([ESPRequest.Store], {
    service: "WsDfu",
    action: "DFUQuery",
    responseQualifier: "DFUQueryResponse.DFULogicalFiles.DFULogicalFile",
    responseTotalQualifier: "DFUQueryResponse.NumFiles",
    idProperty: "__hpcc_id",
    startProperty: "PageStartFrom",
    countProperty: "PageSize",

    _watched: [],
    create: function (id) {
        return create(id);
    },
    preRequest: function (request) {
        switch (request.Sortby) {
            case "RecordCount":
                request.Sortby = "Records";
                break;
            case "IntSize":
                request.Sortby = "FileSize";
                break;
        }

        lang.mixin(request, {
            IncludeSuperOwner: 1
        });
    },
    update: function (id, item) {
        var storeItem = this.get(id);
        storeItem.updateData(item);
        if (!this._watched[id]) {
            var context = this;
            this._watched[id] = storeItem.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                if (oldValue !== newValue) {
                    context.notify(storeItem, id);
                }
            });
        }
    },
    preProcessRow: function (item, request, query, options) {
        lang.mixin(item, {
            __hpcc_id: createID(item.NodeGroup, item.Name),
            __hpcc_isDir: false,
            __hpcc_displayName: item.Name,
            StateID: 0,
            State: ""
        });
    },
    mayHaveChildren: function (object) {
        return object.__hpcc_isDir;
    }
});

var TreeStore = declare(null, {
    idProperty: "__hpcc_id",
    cache: null,
    _watched: [],

    constructor: function (options) {
        this.cache = {};
    },

    _fetchFiles: function (scope) {
        var context = this;
        var results = WsDfu.DFUFileView({
            request: {
                Scope: scope
            }
        }).then(function (response) {
            var retVal = [];
            if (lang.exists("DFUFileViewResponse.DFULogicalFiles.DFULogicalFile", response)) {
                arrayUtil.forEach(response.DFUFileViewResponse.DFULogicalFiles.DFULogicalFile, function (item, idx) {
                    var isDir = !(item.Name);

                    var childScope = "";
                    var leafName = "";
                    if (isDir) {
                        childScope = scope;
                        if (childScope)
                            childScope += "::";
                        childScope += item.Directory;
                    } else {
                        var parts = item.Name.split("::");
                        if (parts.length) {
                            leafName = parts[parts.length - 1];
                        }
                    }

                    lang.mixin(item, {
                        __hpcc_id: isDir ? childScope : createID(item.NodeGroup, item.Name),
                        __hpcc_isDir: isDir,
                        __hpcc_childScope: childScope,
                        __hpcc_displayName: isDir ? item.Directory : leafName
                    });

                    var storeItem = null;
                    if (isDir) {
                        storeItem = item;
                    } else {
                        storeItem = create(item.__hpcc_id);
                        if (!context._watched[item.__hpcc_id]) {
                            context._watched[item.__hpcc_id] = storeItem.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                                if (oldValue !== newValue) {
                                    context.notify(storeItem, storeItem.__hpcc_id);
                                }
                            });
                        }
                        storeItem.updateData(item);
                    }
                    retVal.push(storeItem);
                    context.cache[context.getIdentity(storeItem)] = storeItem;
                });
            }
            function boolToNumber(b:boolean): number{
                return b ? 1: 0;
            }
            retVal.sort(function (l, r) {
                if (l.__hpcc_isDir === r.__hpcc_isDir) {
                    return (boolToNumber(l.__hpcc_displayName > r.__hpcc_displayName) - boolToNumber(l.__hpcc_displayName < r.__hpcc_displayName));
                }
                return (boolToNumber(l.__hpcc_isDir < r.__hpcc_isDir) - boolToNumber(l.__hpcc_isDir > r.__hpcc_isDir));
            });
            return retVal;
        });
        results.total = results.then(function (response) {
            return response.length;
        });
        return results;
    },

    //  Store API ---
    get: function (id) {
        return this.cache[id];
    },
    getIdentity: function (object) {
        return object[this.idProperty];
    },
    put: function (object, directives) {
    },
    add: function (object, directives) {
    },
    remove: function (id) {
    },
    query: function (query, options) {
        return QueryResults(this._fetchFiles(""));
    },
    transaction: function () {
    },
    mayHaveChildren: function (object) {
        return object.__hpcc_isDir;
    },
    getChildren: function (parent, options) {
        return QueryResults(this._fetchFiles(parent.__hpcc_childScope));
    },
    getMetadata: function (object) {
    }
});

var LogicalFile = declare([ESPUtil.Singleton], {    // jshint ignore:line
    _FileDetailSetter: function (FileDetail) {
        this.FileDetail = FileDetail;
        this.result = ESPResult.Get(FileDetail);
    },
    _DirSetter: function (Dir) {
        this.set("Directory", Dir);
    },
    _DFUFilePartsOnClustersSetter: function (DFUFilePartsOnClusters) {
        var DFUFileParts = {
            DFUPart: []
        };
        if (lang.exists("DFUFilePartsOnCluster", DFUFilePartsOnClusters)) {
            arrayUtil.forEach(DFUFilePartsOnClusters.DFUFilePartsOnCluster, function (DFUFilePartsOnCluster, idx) {
                if (lang.exists("DFUFileParts.DFUPart", DFUFilePartsOnCluster)) {
                    arrayUtil.forEach(DFUFilePartsOnCluster.DFUFileParts.DFUPart, function (DFUPart, idx) {
                        DFUFileParts.DFUPart.push(lang.mixin({
                            __hpcc_id: DFUPart.Id + "--" + DFUPart.Copy,
                            Cluster: DFUFilePartsOnCluster.Cluster,
                            PartsizeInt: parseInt(DFUPart.Partsize.split(",").join(""))
                        }, DFUPart));
                    }, this);
                }
                if (idx === 0) {
                    this.set("CanReplicateFlag", DFUFilePartsOnCluster.CanReplicate);
                    this.set("ReplicateFlag", DFUFilePartsOnCluster.Replicate);
                }
            }, this);
        }
        this.set("DFUFileParts", DFUFileParts);
    },
    _CompressedFileSizeSetter: function (CompressedFileSize) {
        this.CompressedFileSize = "";
        if (CompressedFileSize) {
            this.CompressedFileSize = CompressedFileSize.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        }
    },
    constructor: function (args) {
        if (args) {
            declare.safeMixin(this, args);
        }
        this.logicalFile = this;
    },
    save: function (description, args) {
        //WsDfu/DFUInfo?FileName=progguide%3A%3Aexampledata%3A%3Akeys%3A%3Apeople.lastname.firstname&UpdateDescription=true&FileDesc=%C2%A0123&Save+Description=Save+Description
        var context = this;
        WsDfu.DFUInfo({
            request: {
                FileName: this.Name,
                Cluster: this.Cluster,
                UpdateDescription: true,
                FileDesc: description
            }
        }).then(function (response) {
            if (lang.exists("DFUInfoResponse.FileDetail", response)) {
                context.updateData(response.DFUInfoResponse.FileDetail);
                if (args && args.onAfterSend) {
                    args.onAfterSend(response.DFUInfoResponse.FileDetail);
                }
            }
        });
    },
    doDelete: function (params) {
        var context = this;
        WsDfu.DFUArrayAction([this], "Delete").then(function (response) {
            if (lang.exists("DFUArrayActionResponse.ActionResults.DFUActionInfo", response) &&
                response.DFUArrayActionResponse.ActionResults.DFUActionInfo.length &&
                !response.DFUArrayActionResponse.ActionResults.DFUActionInfo[0].Failed) {
                context.updateData({ StateID: 999, State: "deleted" })
            } else {
                context.refresh();
            }
        });
    },
    despray: function (params) {
        lang.mixin(params.request, {
            sourceLogicalName: this.Name
        });
        return FileSpray.Despray(params);
    },
    copy: function (params) {
        lang.mixin(params.request, {
            sourceLogicalName: this.Name
        });
        return FileSpray.Copy(params);
    },
    rename: function (params) {
        var context = this;
        lang.mixin(params.request, {
            srcname: this.Name
        });
        return FileSpray.Rename(params).then(function (response) {
            context.set("Name", params.request.dstname);  //TODO - need to monitor DFUWorkunit for success (After ESPDFUWorkunit has been updated to proper singleton).
            context.refresh();
            return response;
        });
    },
    removeSubfiles: function (subfiles, removeSuperfile) {
        var context = this;
        return WsDfu.SuperfileAction("remove", this.Name, subfiles, removeSuperfile).then(function (response) {
            context.refresh();
            return response;
        });
    },
    refresh: function (full) {
        return this.getInfo();
    },
    getInfo: function (args) {
        //WsDfu/DFUInfo?Name=progguide::exampledata::keys::people.state.city.zip.lastname.firstname.payload&Cluster=hthor__myeclagent HTTP/1.1
        var context = this;
        return WsDfu.DFUInfo({
            request: {
                Name: this.Name,
                Cluster: this.Cluster
            }
        }).then(function (response) {
            if (lang.exists("DFUInfoResponse.FileDetail", response)) {
                context.updateData(response.DFUInfoResponse.FileDetail);
                if (args && args.onAfterSend) {
                    args.onAfterSend(response.DFUInfoResponse.FileDetail);
                }
            }
        });
    },
    getInfo2: function (args) {
        var context = this;
        return WsDfu.DFUQuery({
            request: {
                LogicalName: this.Name
            }
        }).then(function (response) {
            if (lang.exists("DFUQueryResponse.DFULogicalFiles.DFULogicalFile", response) && response.DFUQueryResponse.DFULogicalFiles.DFULogicalFile.length) {
                context.updateData(response.DFUQueryResponse.DFULogicalFiles.DFULogicalFile[0]);
                if (args && args.onAfterSend) {
                    args.onAfterSend(response.DFUQueryResponse.DFULogicalFiles.DFULogicalFile[0]);
                }
            }
        });
    },
    getLeaf: function () {
        var nameParts = this.Name.split("::");
        return nameParts.length ? nameParts[nameParts.length - 1] : "";
    },
    updateData: ESPUtil.override(function (inherited, data) {
        inherited(data);
        if (!this.result) {
            this.result = ESPResult.Get(data);
        }
    }),
    fetchStructure: function (format, onFetchStructure) {
        WsDfu.DFUDefFile({
            request: {
                Name: this.Name,
                Format: format
            }
        }).then(function (response) {
            onFetchStructure(response);
        });
    },
    fetchDEF: function (onFetchXML) {
        this.fetchStructure("def", onFetchXML);
    },
    fetchXML: function (onFetchXML) {
        this.fetchStructure("xml", onFetchXML);
    },
    getStateIconClass: function () {
        if (this.isSuperfile) {
            switch (this.StateID) {
                case 999:
                    return "iconSuperFileDeleted";
            }
            return "iconSuperFile";
        } else {
            switch (this.StateID) {
                case 999:
                    return "iconLogicalFileDeleted";
            }
            return "iconLogicalFile";
        }
    },
    getStateImageName: function () {
        if (this.isSuperfile) {
            switch (this.StateID) {
                case 999:
                    return "superfile_deleted.png"
            }
            return "superfile.png";
        } else {
            switch (this.StateID) {
                case 999:
                    return "logicalfile_deleted.png";
            }
            return "logicalfile.png";
        }
    },
    getStateImageHTML: function () {
        return Utility.getImageHTML(this.getStateImageName());
    },
    getProtectedImage: function () {
        if (this.IsProtected) {
            return Utility.getImageURL("locked.png");
        }
        return Utility.getImageURL("unlocked.png");
    },
    getCompressedImage: function () {
        if (this.IsCompressed) {
            return Utility.getImageURL("compressed.png");
        }
        return Utility.getImageURL("");
    },
    isDeleted: function () {
        return this.StateID === 999;
    }
});

export function Get(Cluster, Name, data) {
    if (!Name) {
        throw new Error("Invalid Logical File ID");
    }
    var store = new Store();
    var retVal = store.get(createID(Cluster, Name));
    if (data) {
        lang.mixin(data, {
            __hpcc_id: createID(data.NodeGroup, data.Name),
            __hpcc_isDir: false,
            __hpcc_displayName: data.Name
        });
        retVal.updateData(data);
    }
    return retVal;
}

export function CreateLFQueryStore(options) {
    var store = new Store(options);
    return Observable(store);
}

export function CreateLFQueryTreeStore(options) {
    var store = new TreeStore(options);
    return Observable(store);
}
