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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/_base/xhr",
    "dojo/_base/Deferred",
    "dojo/data/ItemFileReadStore",
    "dojo/promise/all",
    "dojo/store/Memory",
    "dojo/on",

    "dijit/registry",

    "hpcc/WsTopology",
    "hpcc/WsWorkunits",
    "hpcc/FileSpray"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, xhr, Deferred, ItemFileReadStore, all, Memory, on,
    registry,
    WsTopology, WsWorkunits, FileSpray) {

    return {
        i18n: nlsHPCC,

        loading: false,
        defaultValue: "",

        //  Implementation  ---
        reset: function () {
            this.initalized = false;
            this.loading = false;
            this.defaultValue = "";
            this.options = [];
        },

        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;
            this.loading = true;
            this.options = [];

            if (params.Target) {
                this.defaultValue = params.Target;
                this.set("value", params.Target);
            }

            if (params.includeBlank) {
                this.includeBlank = params.includeBlank;
                this.options.push({
                    label: "&nbsp;",
                    value: ""
                });
            }
            if (params.Groups === true) {
                this.loadClusterGroups();
            } else if (params.SprayTargets === true) {
                this.loadSprayTargets();
            } else if (params.DropZones === true) {
                this.loadDropZones();
            } else if (params.DropZoneFolders === true) {
                this.defaultValue = ".";
                this.set("value", ".");
                this.loadDropZoneFolders();
            } else if (params.WUState === true) {
                this.loadWUState();
            } else if (params.DFUState === true) {
                this.loadDFUState();
            } else if (params.ECLSamples === true) {
                this.loadECLSamples();
            } else if (params.Logs === true) {
                this.loadLogs(params);
            } else {
                this.loadTargets();
            }
            if (params.callback) {
                this.callback = params.callback;
            }
        },

        _setValueAttr: function (target) {
            if (target === null)
                target = "";
            this.inherited(arguments);
            if (this.callback) {
                this.callback(this.value, this._getRowAttr());
            }
        },

        _getValueAttr: function () {
            if (this.loading)
                return this.defaultValue;

            if (this.textbox)
                return this.textbox.value;

            return this.value;
        },

        _getRowAttr: function () {
            var context = this;
            var retVal = null;
            arrayUtil.forEach(this.options, function (item, idx) {
                if (context.value === item.value) {
                    retVal = item;
                    return false;
                }
            });
            return retVal;
        },

        _postLoad: function () {
            if (this.defaultValue === "" && this.options.length) {
                this.defaultValue = this.options[0].value;
            }
            this.set("value", this.defaultValue);
            this.loading = false;
        },

        loadDropZones: function () {
            var context = this;
            WsTopology.TpServiceQuery({
                load: function (response) {
                    if (lang.exists("TpServiceQueryResponse.ServiceList.TpDropZones.TpDropZone", response)) {
                        var targetData = response.TpServiceQueryResponse.ServiceList.TpDropZones.TpDropZone;
                        for (var i = 0; i < targetData.length; ++i) {
                            context.options.push({
                                label: targetData[i].Name,
                                value: targetData[i].Name,
                                machine: targetData[i].TpMachines.TpMachine[0]
                            });
                        }
                        context._postLoad();
                    }
                }
            });
        },

        _loadDropZoneFolders: function (Netaddr, Path, OS, depth) {
            depth = depth || 0;
            var retVal = [];
            retVal.push(Path);
            var deferred = new Deferred();
            if (depth > 2) {
                setTimeout(function () {
                    deferred.resolve(retVal);
                }, 20);
            } else {
                var context = this;
                FileSpray.FileList({
                    request: {
                        Netaddr: Netaddr,
                        Path: Path,
                        OS: OS
                    },
                    suppressExceptionToaster: true
                }).then(function (response) {
                    var requests = [];
                    if (lang.exists("FileListResponse.files.PhysicalFileStruct", response)) {
                        var files = response.FileListResponse.files.PhysicalFileStruct;
                        for (var i = 0; i < files.length; ++i) {
                            if (files[i].isDir) {
                                requests.push(context._loadDropZoneFolders(Netaddr, Path + "/" + files[i].name, OS, ++depth));
                            }
                        }
                    }
                    all(requests).then(function (responses) {
                        arrayUtil.forEach(responses, function (response) {
                            retVal = retVal.concat(response);
                        });
                        deferred.resolve(retVal);
                    });
                });
            }
            return deferred.promise;
        },

        endsWith: function (str, suffix) {
            return str.indexOf(suffix, str.length - suffix.length) !== -1;
        },

        loadDropZoneFolders: function () {
            var context = this;
            this.getDropZoneFolder = function () {
                var baseFolder = this._dropZoneTarget.machine.Directory + (this.endsWith(this._dropZoneTarget.machine.Directory, "/") ? "" : "/");
                var selectedFolder = this.get("value");
                return baseFolder + selectedFolder;
            }
            if (this._dropZoneTarget) {
                this._loadDropZoneFolders(this._dropZoneTarget.machine.Netaddress, this._dropZoneTarget.machine.Directory, this._dropZoneTarget.machine.OS).then(function (results) {
                    results.sort();
                    var store = new Memory({
                        data: arrayUtil.map(results, function (_path) {
                            var path = _path.substring(context._dropZoneTarget.machine.Directory.length);
                            return {
                                name: "." + path,
                                id: _path
                            };
                        })
                    });
                    context.set("store", store);
                    context._postLoad();
                });
            }
        },

        loadClusterGroups: function () {
            var context = this;
            WsTopology.TpGroupQuery({
                load: function (response) {
                    if (lang.exists("TpGroupQueryResponse.TpGroups.TpGroup", response)) {
                        var targetData = response.TpGroupQueryResponse.TpGroups.TpGroup;
                        for (var i = 0; i < targetData.length; ++i) {
                            switch(targetData[i].Kind) {
                                case "Thor":
                                case "hthor":
                                case "Roxie":
                                    context.options.push({
                                        label: targetData[i].Name,
                                        value: targetData[i].Name
                                    });
                                    break;
                            }
                        }
                        context._postLoad();
                    }
                }
            });
        },

        loadSprayTargets: function () {
            var context = this;
            FileSpray.GetSprayTargets({
                load: function (response) {
                    if (lang.exists("GetSprayTargetsResponse.GroupNodes.GroupNode", response)) {
                        var targetData = response.GetSprayTargetsResponse.GroupNodes.GroupNode;
                        for (var i = 0; i < targetData.length; ++i) {
                            context.options.push({
                                label: targetData[i].Name,
                                value: targetData[i].Name
                            });
                        }
                        context._postLoad();
                    }
                }
            });
        },

        loadWUState: function() {
            for (var key in WsWorkunits.States) {
                this.options.push({
                    label: WsWorkunits.States[key],
                    value: WsWorkunits.States[key]
                });
            }
            this._postLoad();
        },

        loadDFUState: function () {
            for (var key in FileSpray.States) {
                this.options.push({
                    label: FileSpray.States[key],
                    value: FileSpray.States[key]
                });
            }
            this._postLoad();
        },

        LogicalFileSearchType: function() {
            this.options.push({
                label: "Created",
                value: "Created"
            });
            this.options.push({
                label: "Used",
                value: "Referenced"
            });
            this._postLoad();
        },

        loadTargets: function () {
            var context = this;
            WsTopology.TpLogicalClusterQuery({
            }).then(function (response) {
                if (lang.exists("TpLogicalClusterQueryResponse.TpLogicalClusters.TpLogicalCluster", response)) {
                    var targetData = response.TpLogicalClusterQueryResponse.TpLogicalClusters.TpLogicalCluster;
                    for (var i = 0; i < targetData.length; ++i) {
                        context.options.push({
                            label: targetData[i].Name,
                            value: targetData[i].Name
                        });
                    }

                    if (!context.includeBlank && context._value == "") {
                        if (response.TpLogicalClusterQueryResponse["default"]) {
                            context._value = response.TpLogicalClusterQueryResponse["default"].Name;
                        } else {
                            context._value = context.options[0].value;
                        }
                    }
                }
                context._postLoad();
            });
        },

        loadECLSamples: function () {
            var sampleStore = new ItemFileReadStore({
                url: dojoConfig.getURL("ecl/ECLPlaygroundSamples.json")
            });
            this.setStore(sampleStore);
            var context = this;
            this.on("change", function (evt) {
                var filename = this.get("value");
                xhr.get({
                    url: dojoConfig.getURL("ecl/" + filename),
                    handleAs: "text",
                    load: function (eclText) {
                        context.onNewSelection(eclText);
                    },
                    error: function () {
                    }
                });
            });
            context._postLoad();
        },

        loadLogs: function (params) {
            var context = this;
            this.set("options", []);
            FileSpray.FileList({
                request: {
                    Mask: "*.log",
                    Netaddr: params.treeNode.getNetaddress(),
                    OS: params.treeNode.getOS(),
                    Path: params.treeNode.getLogDirectory()
                }
            }).then(function (response) {
                if (lang.exists("FileListResponse.files.PhysicalFileStruct", response)) {
                    var options = [];
                    var targetData = response.FileListResponse.files.PhysicalFileStruct;
                    var shortestLabelLen = 9999;
                    var shortestLabel = "";
                    for (var i = 0; i < targetData.length; ++i) {
                        options.push({
                            label: targetData[i].name,// + " " + targetData[i].filesize + " " + targetData[i].modifiedtime,
                            value: targetData[i].name
                        });
                        if (shortestLabelLen > targetData[i].name.length) {
                            shortestLabelLen = targetData[i].name.length;
                            shortestLabel = targetData[i].name;
                        }
                    }
                    options.sort(function (l, r) {
                        return -l.label.localeCompare(r.label);
                    });
                    context.set("options", options);
                    context.defaultValue = shortestLabel;
                    context._value = shortestLabel;
                }
                context._postLoad();
            });
        }
    };
});
