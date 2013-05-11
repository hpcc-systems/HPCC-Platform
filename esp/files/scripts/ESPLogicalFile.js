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
    "dojo/_base/array",
    "dojo/_base/lang",
    "dojo/_base/Deferred",
    "dojo/data/ObjectStore",
    "dojo/store/util/QueryResults",
    "dojo/store/Observable",
    "dojo/Stateful",

    "hpcc/WsDfu",
    "hpcc/FileSpray",
    "hpcc/ESPRequest",
    "hpcc/ESPUtil",
    "hpcc/ESPResult"
], function (declare, arrayUtil, lang, Deferred, ObjectStore, QueryResults, Observable, Stateful,
        WsDfu, FileSpray, ESPRequest, ESPUtil, ESPResult) {

    var _logicalFiles = {};

    var Store = declare([ESPRequest.Store], {
        service: "WsDfu",
        action: "DFUQuery",
        responseQualifier: "DFULogicalFiles.DFULogicalFile",
        responseTotalQualifier: "NumFiles",
        idProperty: "Name",
        startProperty: "PageStartFrom",
        countProperty: "Count",

        _watched: [],
        create: function (id) {
            return new LogicalFile({
                Name: id
            });
        },
        update: function (id, item) {
            var storeItem = this.get(id);
            storeItem.updateData(item);
            if (!this._watched[id]) {
                var context = this;
                this._watched[id] = storeItem.watch("changedCount", function (name, oldValue, newValue) {
                    if (oldValue !== newValue) {
                        context.notify(storeItem, id);
                    }
                });
            }
        }
    });

    var LogicalFile = declare([ESPUtil.Singleton], {
        _FileDetailSetter: function(FileDetail) {
            this.FileDetail = FileDetail;
            this.result = ESPResult.Get(FileDetail);
        },
        _DirSetter: function (Dir) {
            this.set("Directory", Dir);
        },
        constructor: function (args) {
            this.inherited(arguments);
            declare.safeMixin(this, args);
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
                },
                load: function (response) {
                    if (lang.exists("DFUInfoResponse.FileDetail", response)) {
                        context.updateData(response.DFUInfoResponse.FileDetail);
                        if (args && args.onAfterSend) {
                            args.onAfterSend(response.DFUInfoResponse.FileDetail);
                        }
                    }
                }
            });
        },
        doDelete: function (params) {
            var context = this;
            WsDfu.DFUArrayAction([this], "Delete", {
                load: function (response) {
                    context.refresh();
                }
            });
        },
        despray: function (params) {
            var context = this;
            lang.mixin(params.request, {
                sourceLogicalName: this.Name
            });
            return FileSpray.Despray(params);
        },
        copy: function (params) {
            var context = this;
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
            this.getInfo();
        },
        getInfo: function (args) {
            //WsDfu/DFUInfo?Name=progguide::exampledata::keys::people.state.city.zip.lastname.firstname.payload&Cluster=hthor__myeclagent HTTP/1.1
            var context = this;
            WsDfu.DFUInfo({
                request:{
                    Name: this.Name,
                    Cluster: this.Cluster
                },
                load: function (response) {
                    if (lang.exists("DFUInfoResponse.FileDetail", response)) {
                        context.updateData(response.DFUInfoResponse.FileDetail);
                        if (args && args.onAfterSend) {
                            args.onAfterSend(response.DFUInfoResponse.FileDetail);
                        }
                    }
                }
            });
        },
        updateData: function (data) {
            this.inherited(arguments);
            if (!this.result) {
                this.result = ESPResult.Get(data);
            }
        },
        fetchStructure: function (format, onFetchStructure) {
            var context = this;
            WsDfu.DFUDefFile({
                request: {
                    Name: this.Name,
                    Format: format
                },
                load: function (response) {
                    onFetchStructure(response);
                }
            });
        },
        fetchDEF: function (onFetchXML) {
            this.fetchStructure("def", onFetchXML);
        },
        fetchXML: function (onFetchXML) {
            this.fetchStructure("xml", onFetchXML);
        }
    });

    return {
        Create: function (params) {
            retVal = new LogicalFile(params);
            retVal.create();
            return retVal;
        },

        Get: function (name) {
            var store = new Store();
            return store.get(name);
        },

        CreateLFQueryStore: function (options) {
            var store = new Store(options);
            return Observable(store);
        },

        CreateLFQueryObjectStore: function (options) {
            var objStore = new ObjectStore({
                objectStore: this.CreateLFQueryStore()
            });
            return objStore;
        }
    };
});
