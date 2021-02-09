import * as arrayUtil from "dojo/_base/array";
import * as declare from "dojo/_base/declare";
import * as Deferred from "dojo/_base/Deferred";
import * as lang from "dojo/_base/lang";
import * as Observable from "dojo/store/Observable";
import * as QueryResults from "dojo/store/util/QueryResults";
import { DPWorkunit } from "./DataPatterns/DPWorkunit";
import * as ESPRequest from "./ESPRequest";
import * as ESPResult from "./ESPResult";
import * as ESPUtil from "./ESPUtil";
import * as FileSpray from "./FileSpray";
import * as Utility from "./Utility";
import * as WsDfu from "./WsDfu";

const _logicalFiles = {};

const createID = function (Cluster, Name) {
    return (Cluster ? Cluster : "") + "--" + Name;
};

const create = function (id) {
    if (!lang.exists(id, _logicalFiles)) {
        const idParts = id.split("--");
        _logicalFiles[id] = new LogicalFile({
            Cluster: idParts[0] ? idParts[0] : "",
            NodeGroup: idParts[0] ? idParts[0] : "",
            Name: idParts[1]
        });
    }
    return _logicalFiles[id];
};

class Store extends ESPRequest.Store {

    service = "WsDfu";
    action = "DFUQuery";
    responseQualifier = "DFUQueryResponse.DFULogicalFiles.DFULogicalFile";
    responseTotalQualifier = "DFUQueryResponse.NumFiles";
    idProperty = "__hpcc_id";

    startProperty = "PageStartFrom";
    countProperty = "PageSize";

    _watched: { [id: string]: any } = {};

    create(id) {
        return create(id);
    }

    preRequest(request) {
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
    }

    update(id, item) {
        const storeItem = this.get(id);
        storeItem.updateData(item);
        if (!this._watched[id]) {
            const context = this;
            this._watched[id] = storeItem.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                if (oldValue !== newValue) {
                    context.notify(storeItem, id);
                }
            });
        }
    }

    preProcessRow(item, request, query, options) {
        lang.mixin(item, {
            __hpcc_id: createID(item.NodeGroup, item.Name),
            __hpcc_isDir: false,
            __hpcc_displayName: item.Name,
            StateID: 0,
            State: ""
        });
    }

    mayHaveChildren(object) {
        return object.__hpcc_isDir;
    }
}

const TreeStore = declare(null, {
    idProperty: "__hpcc_id",
    cache: null,
    _watched: [],

    constructor(options) {
        this.cache = {};
    },

    _fetchFiles(scope) {
        const deferredResults = new Deferred();
        deferredResults.total = new Deferred();

        const context = this;
        WsDfu.DFUFileView({
            request: {
                Scope: scope
            }
        }).then(function (response) {
            const retVal = [];
            if (lang.exists("DFUFileViewResponse.DFULogicalFiles.DFULogicalFile", response)) {
                arrayUtil.forEach(response.DFUFileViewResponse.DFULogicalFiles.DFULogicalFile, function (item, idx) {
                    const isDir = !(item.Name);

                    let childScope = "";
                    let leafName = "";
                    if (isDir) {
                        childScope = scope;
                        if (childScope)
                            childScope += "::";
                        childScope += item.Directory;
                    } else {
                        const parts = item.Name.split("::");
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

                    let storeItem = null;
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
            function boolToNumber(b: boolean): number {
                return b ? 1 : 0;
            }
            retVal.sort(function (l, r) {
                if (l.__hpcc_isDir === r.__hpcc_isDir) {
                    return (boolToNumber(l.__hpcc_displayName > r.__hpcc_displayName) - boolToNumber(l.__hpcc_displayName < r.__hpcc_displayName));
                }
                return (boolToNumber(l.__hpcc_isDir < r.__hpcc_isDir) - boolToNumber(l.__hpcc_isDir > r.__hpcc_isDir));
            });
            return retVal;
        }).then(function (response) {
            deferredResults.resolve(response);
            deferredResults.total.resolve(response.length);
        });
        return deferredResults;
    },

    //  Store API ---
    get(id) {
        return this.cache[id];
    },
    getIdentity(object) {
        return object[this.idProperty];
    },
    put(object, directives) {
    },
    add(object, directives) {
    },
    remove(id) {
    },
    query(query, options) {
        return QueryResults(this._fetchFiles(""));
    },
    transaction() {
    },
    mayHaveChildren(object) {
        return object.__hpcc_isDir;
    },
    getChildren(parent, options) {
        return QueryResults(this._fetchFiles(parent.__hpcc_childScope));
    },
    getMetadata(object) {
    }
});

const LogicalFile = declare([ESPUtil.Singleton], {    // jshint ignore:line
    _dpWU: DPWorkunit,

    _FileDetailSetter(FileDetail) {
        this.FileDetail = FileDetail;
        this.result = ESPResult.Get(FileDetail);
    },
    _DirSetter(Dir) {
        this.set("Directory", Dir);
    },
    _DFUFilePartsOnClustersSetter(DFUFilePartsOnClusters) {
        const DFUFileParts = {
            DFUPart: []
        };
        if (lang.exists("DFUFilePartsOnCluster", DFUFilePartsOnClusters)) {
            arrayUtil.forEach(DFUFilePartsOnClusters.DFUFilePartsOnCluster, function (DFUFilePartsOnCluster, idx) {
                if (lang.exists("DFUFileParts.DFUPart", DFUFilePartsOnCluster)) {
                    arrayUtil.forEach(DFUFilePartsOnCluster.DFUFileParts.DFUPart, function (DFUPart, idx) {
                        DFUFileParts.DFUPart.push(lang.mixin({
                            __hpcc_id: DFUPart.Id + "--" + DFUPart.Copy,
                            Cluster: DFUFilePartsOnCluster.Cluster
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
    _CompressedFileSizeSetter(CompressedFileSize) {
        this.CompressedFileSize = undefined;
        if (CompressedFileSize) {
            this.CompressedFileSize = CompressedFileSize;
            this.set("CompressedFileSizeString", CompressedFileSize.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ","));
        }
    },
    _StatSetter(Stat) {
        this.set("MinSkew", Stat.MinSkew);
        this.set("MaxSkew", Stat.MaxSkew);
        this.set("MinSkewPart", Stat.MinSkewPart);
        this.set("MaxSkewPart", Stat.MaxSkewPart);
    },
    constructor(args) {
        if (args) {
            declare.safeMixin(this, args);
        }
        this.logicalFile = this;
        this._dpWU = new DPWorkunit(this.Cluster, this.Name);
    },
    save(request, args) {
        // WsDfu/DFUInfo?FileName=progguide%3A%3Aexampledata%3A%3Akeys%3A%3Apeople.lastname.firstname&UpdateDescription=true&FileDesc=%C2%A0123&Save+Description=Save+Description
        const context = this;
        WsDfu.DFUInfo({
            request: {
                Name: this.Name,
                Cluster: this.Cluster,
                UpdateDescription: true,
                FileDesc: request.Description,
                Protect: request.isProtected === true ? 1 : 2,
                Restrict: request.isRestricted === true ? 1 : 2
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
    doDelete(params) {
        const context = this;
        WsDfu.DFUArrayAction([this], "Delete").then(function (response) {
            if (lang.exists("DFUArrayActionResponse.ActionResults.DFUActionInfo", response) &&
                response.DFUArrayActionResponse.ActionResults.DFUActionInfo.length &&
                !response.DFUArrayActionResponse.ActionResults.DFUActionInfo[0].Failed) {
                context.updateData({ StateID: 999, State: "deleted" });
            } else {
                context.refresh();
            }
        });
    },
    despray(params) {
        lang.mixin(params.request, {
            sourceLogicalName: this.Name
        });
        return FileSpray.Despray(params);
    },
    copy(params) {
        lang.mixin(params.request, {
            sourceLogicalName: this.Name
        });
        return FileSpray.Copy(params);
    },
    rename(params) {
        const context = this;
        lang.mixin(params.request, {
            srcname: this.Name
        });
        return FileSpray.Rename(params).then(function (response) {
            context.set("Name", params.request.dstname);  // TODO - need to monitor DFUWorkunit for success (After ESPDFUWorkunit has been updated to proper singleton).
            context.refresh();
            return response;
        });
    },
    removeSubfiles(subfiles, removeSuperfile) {
        const context = this;
        return WsDfu.SuperfileAction("remove", this.Name, subfiles, removeSuperfile).then(function (response) {
            context.refresh();
            return response;
        });
    },
    refresh(full) {
        return this.getInfo();
    },
    getInfo(args) {
        // WsDfu/DFUInfo?Name=progguide::exampledata::keys::people.state.city.zip.lastname.firstname.payload&Cluster=hthor__myeclagent HTTP/1.1
        const context = this;
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
    getInfo2(args) {
        const context = this;
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
    getLeaf() {
        const nameParts = this.Name.split("::");
        return nameParts.length ? nameParts[nameParts.length - 1] : "";
    },
    updateData: ESPUtil.override(function (inherited, data) {
        inherited(data);
        if (!this.result) {
            this.result = ESPResult.Get(data);
        }
    }),
    fetchStructure(format, onFetchStructure) {
        WsDfu.DFUDefFile({
            request: {
                Name: this.Name,
                Format: format
            }
        }).then(function (response) {
            onFetchStructure(response);
        });
    },
    fetchDEF(onFetchXML) {
        this.fetchStructure("def", onFetchXML);
    },
    fetchXML(onFetchXML) {
        this.fetchStructure("xml", onFetchXML);
    },
    getStateIconClass() {
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
    getStateImageName() {
        if (this.isSuperfile) {
            switch (this.StateID) {
                case 999:
                    return "superfile_deleted.png";
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
    getStateImageHTML() {
        return Utility.getImageHTML(this.getStateImageName());
    },
    getProtectedImage() {
        if (this.ProtectList.DFUFileProtect.length > 0) {
            return Utility.getImageURL("locked.png");
        }
        return Utility.getImageURL("unlocked.png");
    },
    getCompressedImage() {
        if (this.IsCompressed) {
            return Utility.getImageURL("compressed.png");
        }
        return "";
    },
    isDeleted() {
        return this.StateID === 999;
    },
    fetchDataPatternsWU() {
        return this._dpWU.resolveWU();
    }
});

export function Get(Cluster, Name, data?) {
    if (!Name) {
        throw new Error("Invalid Logical File ID");
    }
    const store = new Store();
    const retVal = store.get(createID(Cluster, Name));
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
    const store = new Store(options);
    return new Observable(store);
}

export function CreateLFQueryTreeStore(options) {
    const store = new TreeStore(options);
    return new Observable(store);
}
