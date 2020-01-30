define([
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/_base/xhr",
    "dojo/_base/Deferred",
    "dojo/data/ItemFileReadStore",
    "dojo/promise/all",
    "dojo/store/Memory",

    "src/WsTopology",
    "src/WsWorkunits",
    "src/FileSpray",
    "src/ws_access",
    "src/WsESDLConfig",
    "src/WsPackageMaps",
    "src/Utility"

], function (lang, i18n, nlsHPCC, arrayUtil, xhr, Deferred, ItemFileReadStore, all, Memory,
    WsTopology, WsWorkunits, FileSpray, WsAccess, WsESDLConfig, WsPackageMaps, Utility) {

    return {
        i18n: nlsHPCC,

        loading: true,
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
            } else if (params.LogicalFileSearchType === true) {
                this.logicalFileSearchType();
            } else if (params.DropZones === true) {
                this.loadDropZones();
            } else if (params.Users === true) {
                this.loadUsers();
            } else if (params.loadUsersNotInGroup === true) {
                this.loadUsersNotInGroup(params.groupname);
            } else if (params.loadUsersNotAMemberOfAGroup === true) {
                this.loadUsersNotAMemberOfAGroup(params.username);
            } else if (params.UserGroups === true) {
                this.loadUserGroups();
            } else if (params.DropZoneFolders === true) {
                this.defaultValue = "";
                this.set("value", "");
                this.set("placeholder", "/");
                this.loadDropZoneFolders();
            } else if (params.WUState === true) {
                this.loadWUState();
            } else if (params.DFUState === true) {
                this.loadDFUState();
            } else if (params.ECLSamples === true) {
                this.loadECLSamples();
            } else if (params.LoadDESDLDefinitions === true) {
                this.loadESDLDefinitions(params);
            } else if (params.LoadESDLESPProcesses === true) {
                this.loadESDLESPProcesses();
            } else if (params.Logs === true) {
                this.loadLogs(params);
            } else if (params.DFUSprayQueues === true) {
                this.loadSprayQueues();
            } else if (params.GetPackageMapTargets === true) {
                this.defaultValue = "*";
                this.set("value", "*");
                this.loadGetPackageMapTargets();
            } else if (params.GetPackageMapProcesses === true) {
                this.defaultValue = "*";
                this.set("value", "*");
                this.loadGetPackageMapProcesses();
            } else if (params.GetPackageMapProcessFilter === true) {
                this.defaultValue = "*";
                this.set("value", "*");
                this.loadGetPackageMapProcessFilter();
            } else if (params.DropZoneMachines === true) {
                this.defaultValue = "";
                this.set("value", "");
                this.set("placeholder", "");
                this.loadDropZoneMachines();
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

        loadGetPackageMapTargets: function () {
            var context = this;
            WsPackageMaps.GetPackageMapSelectTargets({
                request: {
                    IncludeTargets: true
                }
            }).then(function (response) {
                if (lang.exists("GetPackageMapSelectOptionsResponse.Targets.TargetData", response)) {
                    var targetData = response.GetPackageMapSelectOptionsResponse.Targets.TargetData;
                    context.options.push({
                        label: "ANY",
                        value: "*"
                    });
                    for (var i = 0; i < targetData.length; ++i) {
                        context.options.push({
                            label: targetData[i].Name,
                            value: targetData[i].Name
                        });
                    }
                    context._postLoad();
                }
            });
        },

        loadGetPackageMapProcesses: function () {
            var context = this;
            WsPackageMaps.GetPackageMapSelectProcesses({
                request: {
                    IncludeProcesses: true
                }
            }).then(function (response) {
                if (lang.exists("GetPackageMapSelectOptionsResponse.Targets.TargetData", response)) {
                    var targetData = response.GetPackageMapSelectOptionsResponse.Targets.TargetData;
                    context.options.push({
                        label: "ANY",
                        value: "*"
                    });
                    for (var i = 0; i < targetData.length; ++i) {
                        if (lang.exists("Processes.Item.length", targetData[i])) {
                            for (var j = 0; j < targetData[i].Processes.Item.length; ++j) {
                                context.options.push({
                                    label: targetData[i].Processes.Item[j],
                                    value: targetData[i].Processes.Item[j]
                                });
                            }
                        }
                    }
                    context._postLoad();
                }
            });
        },

        loadGetPackageMapProcessFilter: function () {
            var context = this;
            WsPackageMaps.GetPackageMapSelectProcessFilter({
                request: {
                    IncludeProcessFilters: true
                }
            }).then(function (response) {
                if (lang.exists("GetPackageMapSelectOptionsResponse.ProcessFilters.Item", response)) {
                    var targetData = response.GetPackageMapSelectOptionsResponse.ProcessFilters.Item;
                    for (var i = 0; i < targetData.length; ++i) {
                        context.options.push({
                            label: targetData[i],
                            value: targetData[i]
                        });
                    }
                    context._postLoad();
                }
            });
        },

        loadUserGroups: function () {
            var context = this;
            WsAccess.FilePermission({
                load: function (response) {
                    if (lang.exists("FilePermissionResponse.Groups.Group", response)) {
                        var targetData = response.FilePermissionResponse.Groups.Group;
                        Utility.stringLowerSort(targetData, "name");
                        for (var i = 0; i < targetData.length; ++i) {
                            context.options.push({
                                label: targetData[i].name,
                                value: targetData[i].name
                            });
                        }
                        context._postLoad();
                    }
                }
            });
        },

        loadUsers: function () {
            var context = this;
            WsAccess.FilePermission({
                load: function (response) {
                    if (lang.exists("FilePermissionResponse.Users.User", response)) {
                        var targetData = response.FilePermissionResponse.Users.User;
                        Utility.stringLowerSort(targetData, "username");
                        for (var i = 0; i < targetData.length; ++i) {
                            context.options.push({
                                label: targetData[i].username,
                                value: targetData[i].username
                            });
                        }
                        context._postLoad();
                    }
                }
            });
        },

        loadUsersNotAMemberOfAGroup: function (username) {
            var context = this;
            WsAccess.UserGroupEditInput({
                request: {
                    username: username
                }
            }).then(function (response) {
                if (lang.exists("UserGroupEditInputResponse.Groups.Group", response)) {
                    var targetData = response.UserGroupEditInputResponse.Groups.Group;
                    Utility.stringLowerSort(targetData, "name");
                    for (var i = 0; i < targetData.length; ++i) {
                        context.options.push({
                            label: targetData[i].name,
                            value: targetData[i].name
                        });
                    }
                    context._postLoad();
                }
            });
        },

        loadUsersNotInGroup: function (groupname) {
            var context = this;
            WsAccess.GroupMemberEditInput({
                request: {
                    groupname: groupname
                }
            }).then(function (response) {
                if (lang.exists("GroupMemberEditInputResponse.Users.User", response)) {
                    var targetData = response.GroupMemberEditInputResponse.Users.User;
                    Utility.stringLowerSort(targetData, "username");
                    for (var i = 0; i < targetData.length; ++i) {
                        context.options.push({
                            label: targetData[i].username,
                            value: targetData[i].username
                        });
                    }
                    context._postLoad();
                }
            });
        },

        loadDropZones: function () {
            var context = this;
            this.set("disabled", true);
            WsTopology.TpDropZoneQuery({
            }).then(function (response) {
                context.set("disabled", false);
                if (lang.exists("TpDropZoneQueryResponse.TpDropZones.TpDropZone", response)) {
                    var targetData = response.TpDropZoneQueryResponse.TpDropZones.TpDropZone;
                    for (var i = 0; i < targetData.length; ++i) {
                        if (lang.exists("TpMachines.TpMachine", targetData[i])) {
                            context.options.push({
                                label: targetData[i].Name,
                                value: targetData[i].Name,
                                machine: targetData[i].TpMachines.TpMachine[0]
                            });
                        }
                    }
                    context._postLoad();
                }
            });
        },

        loadDropZoneMachines: function (Name) {
            var context = this;
            this.set("disabled", true);
            if (Name) {
                WsTopology.TpDropZoneQuery({
                    request: {
                        Name: Name
                    }
                }).then(function (response) {
                    if (lang.exists("TpDropZoneQueryResponse.TpDropZones.TpDropZone", response)) {
                        context.set("disabled", false);
                        context.set("options", []);
                        context.options.push({
                            label: "&nbsp;",
                            value: ""
                        });
                        arrayUtil.forEach(response.TpDropZoneQueryResponse.TpDropZones.TpDropZone, function (item, idx) {
                            var targetData = item.TpMachines.TpMachine;
                            for (var i = 0; i < targetData.length; ++i) {
                                context.options.push({
                                    label: targetData[i].Netaddress,
                                    value: targetData[i].Netaddress
                                });
                            }
                            context._postLoad();
                        });
                    }
                });
            }
        },

        _loadDropZoneFolders: function (pathSepChar, Netaddr, Path, OS, depth) {
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
                                if (Path + pathSepChar === "//") {
                                    requests.push(context._loadDropZoneFolders(pathSepChar, Netaddr, Path + files[i].name, OS, ++depth));
                                } else {
                                    requests.push(context._loadDropZoneFolders(pathSepChar, Netaddr, Path + pathSepChar + files[i].name, OS, ++depth));
                                }
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

        loadDropZoneFolders: function (pathSepChar, defaultPath) {
            var context = this;
            this.getDropZoneFolder = function () {
                var baseFolder = this._dropZoneTarget.machine.Directory;
                var selectedFolder = this.get("value");
                return baseFolder + selectedFolder;
            }
            if (this._dropZoneTarget) {
                this._loadDropZoneFolders(pathSepChar, this._dropZoneTarget.machine.Netaddress, this._dropZoneTarget.machine.Directory, this._dropZoneTarget.machine.OS).then(function (results) {
                    results.sort();
                    var store = new Memory({
                        data: arrayUtil.map(results, function (_path) {
                            var path = _path.substring(context._dropZoneTarget.machine.Directory.length);
                            return {
                                name: path,
                                id: _path
                            };
                        })
                    });
                    context.set("store", store);
                    context.set("placeholder", defaultPath)
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
                            switch (targetData[i].Kind) {
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

        loadSprayQueues: function () {
            var context = this;
            WsTopology.TpServiceQuery({
                load: function (response) {
                    if (lang.exists("TpServiceQueryResponse.ServiceList.TpDfuServers", response)) {
                        var targetData = response.TpServiceQueryResponse.ServiceList.TpDfuServers.TpDfuServer;
                        for (var i = 0; i < targetData.length; ++i) {
                            context.options.push({
                                label: targetData[i].Queue,
                                value: targetData[i].Queue
                            })
                        }
                        context._postLoad();
                    }
                }
            })
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

        loadWUState: function () {
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

        logicalFileSearchType: function () {
            this.options.push({
                label: this.i18n.CreatedByWorkunit,
                value: "Created"
            });
            this.options.push({
                label: this.i18n.UsedByWorkunit,
                value: "Used"
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

                    if (!context.includeBlank && context._value === "") {
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
                url: Utility.getURL("ecl/ECLPlaygroundSamples.json")
            });
            this.setStore(sampleStore);
            var context = this;
            this.on("change", function (evt) {
                var filename = this.get("value");
                xhr.get({
                    url: Utility.getURL("ecl/" + filename),
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
                    Netaddr: params.treeNode.newPreflight ? params.treeNode.NetAddress : params.treeNode.getNetaddress(),
                    OS: params.treeNode.newPreflight ? params.treeNode.OS : params.treeNode.getOS(),
                    Path: params.treeNode.newPreflight ? params.treeNode.LogDirectory : params.treeNode.getLogDirectory()
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
        },

        loadESDLDefinitions: function () {
            var context = this;
            WsESDLConfig.ListESDLDefinitions({
                load: function (response) {
                    if (lang.exists("ListESDLDefinitionsResponse.Definitions.Definition", response)) {
                        var targetData = response.ListESDLDefinitionsResponse.Definitions.Definition;
                        Utility.alphanumSort(targetData, "Id");
                        for (var i = 0; i < targetData.length; ++i) {
                            context.options.push({
                                label: targetData[i].Id,
                                value: targetData[i].Id
                            });
                        }
                        context._postLoad();
                    }
                }
            });
        },

        loadESDLESPProcesses: function () {
            var context = this;
            WsESDLConfig.ListESDLBindings({
                request: {}
            }).then(function (response) {
                if (lang.exists("ListESDLBindingsResponse.EspProcesses.EspProcess", response)) {
                    arrayUtil.forEach(response.ListESDLBindingsResponse.EspProcesses.EspProcess, function (item, idx) {
                        context.options.push({
                            label: item.Name,
                            value: item.Name
                        });
                    });
                    context._postLoad();
                }
            });
        }
    };
});
