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
    "dojo/promise/all",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/store/util/QueryResults",

    "hpcc/WsTopology",
    "hpcc/WsSMC",
    "hpcc/ESPUtil",
    "hpcc/ESPTree"
], function (declare, lang, arrayUtil, Deferred, all, Memory, Observable, QueryResults,
    WsTopology, WsSMC, ESPUtil, ESPTree) {

    var ThorCache = {
    };
    var Thor = declare([ESPUtil.Singleton], {
        constructor: function (args) {
            if (args) {
                declare.safeMixin(this, args);
            }
        },

        refresh: function () {
            var context = this;
            return WsTopology.TpThorStatus({
                request: {
                    Name: this.Name
                }
            }).then(function (response) {
                if (lang.exists("TpThorStatusResponse", response)) {
                    context.updateData(response.TpThorStatusResponse);
                    if (response.TpThorStatusResponse.Graph && response.TpThorStatusResponse.SubGraph) {
                        context.updateData({
                            GraphSummary:  response.TpThorStatusResponse.Graph + "-" + response.TpThorStatusResponse.SubGraph
                        });
                    }
                }
                return response;
            })
        }
    });

    var createTreeItem = function (Type, id, espParent, data) {
        if (!(espParent instanceof TopologyItem)) {
            if (!espParent && id !== "root") {
                var d = 0;
            }
        }
        var retVal = new Type({ __hpcc_id: id, __hpcc_parent: espParent });
        if (data) {
            retVal.updateData(data);
        }
        return retVal;
    };


    var TopologyItem = declare([ESPTree.Item], {
        constructor: function (args) {
            this.__hpcc_children = [];
        },

        appendChild: function (child) {
            this.__hpcc_children.push(child);
        },

        appendChildren: function (children) {
            arrayUtil.forEach(children, function (child) {
                this.appendChild(child);
            }, this);
        },

        getLabel: function () {
            return this.__hpcc_displayName;
        },

        //  Helpers  ---
        getCompType: function () {
            if (this.__hpcc_parent && this.__hpcc_parent.Type) {
                return this.__hpcc_parent.Type;
            } else {
                return this.Type;
            }
        },

        getCompName: function () {
            if (this.__hpcc_parent && this.__hpcc_parent.Name) {
                return this.__hpcc_parent.Name;
            } else {
                return this.Name;
            }
        },

        getNetaddress: function () {
            if (this.Netaddress) {
                return this.Netaddress;
            } else if (this.__hpcc_parent) {
                if (this.__hpcc_parent.Netaddress) {
                    return this.__hpcc_parent.Netaddress;
                }
            }
            return "";
        },

        getLogDirectory: function () {
            if (this.LogDirectory) {
                return this.LogDirectory;
            } else if (this.__hpcc_parent) {
                if (this.__hpcc_parent.LogDir) {
                    return this.__hpcc_parent.LogDir;
                } else if (this.__hpcc_parent.LogDirectory) {
                    return this.__hpcc_parent.LogDirectory;
                }
            }
            return "";
        }
    });

    var TpMachine = declare([TopologyItem], {
        __hpcc_type: "TpMachine",
        constructor: function (args) {
        },
        getIcon: function () {
            return "machine.png";
        },
        updateData: function () {
            this.inherited(arguments);
            this.__hpcc_displayName = "[" + this.Netaddress + "] " + this.Name;
        }
    });

    var TpCommon = declare([TopologyItem], {
        _TpMachinesSetter: function (TpMachines) {
            if (lang.exists("TpMachine", TpMachines)) {
                arrayUtil.forEach(TpMachines.TpMachine, function (item, idx) {
                    var newMachine = createTreeItem(TpMachine, item.Type + "_" + item.Netaddress + "_" + item.ProcessNumber + "_" + item.Directory, this, item)
                    this.appendChild(newMachine);
                }, this);
            }
        },
        updateData: function () {
            this.inherited(arguments);
            this.__hpcc_displayName = "[" + (this.Type ? this.Type : this.__hpcc_type) + "] " + this.Name;
        }
    });

    var TpService = declare([TpCommon], {
        __hpcc_type: "TpService",
        constructor: function (args) {
            var d = 0;
        },
        getLabel: function () {
            return "[" + this.Type + "] " + this.Name;
        }
    });

    var TpEclAgent = declare([TpService], {
        __hpcc_type: "TpEclAgent",
        constructor: function (args) {
            var d = 0;
        }
    });

    var TpEclServer = declare([TpService], {
        __hpcc_type: "TpEclServer",
        constructor: function (args) {
            var d = 0;
        }
    });

    var TpEclCCServer = declare([TpService], {
        __hpcc_type: "TpEclCCServer",
        constructor: function (args) {
            var d = 0;
        }
    });

    var TpEclScheduler = declare([TpService], {
        __hpcc_type: "TpEclScheduler",
        constructor: function (args) {
            var d = 0;
        }
    });

    var TpBinding = declare([TpCommon], {
        __hpcc_type: "TpBinding",
        constructor: function (args) {
        },

        getLabel: function () {
            return this.Service;
        }
    });

    var Cluster = declare([TpCommon], {
        __hpcc_type: "Cluster",
        constructor: function (args) {
        },
        getIcon: function () {
            return "cluster.png";
        },
        getLabel: function () {
            return this.Name;
        }
    });

    var Service = declare([TpCommon], {
        __hpcc_type: "Service",
        constructor: function (args) {
        },
        _TpBindingsSetter: function (TpBindings) {
            if (lang.exists("TpBinding", TpBindings)) {
                arrayUtil.forEach(TpBindings.TpBinding, function (item, idx) {
                    this.appendChild(createTreeItem(TpBinding, item.Service + "::" + item.Port, this, item));
                }, this);
            }
        }
    });

    var ServiceType = declare([TpCommon], {
        __hpcc_type: "ServiceType",
        constructor: function (args) {
            var d= 0;
        },
        getIcon: function () {
            return "folder.png";
        },
        getLabel: function () {
            switch (this.__hpcc_id) {
                case "ServiceType::TpDali":
                    return "Dali";
                case "ServiceType::TpDfuServer":
                    return "DFU Server";
                case "ServiceType::TpDropZone":
                    return "Drop Zone";
                case "ServiceType::TpEclAgent":
                    return "ECL Agent";
                case "ServiceType::TpEclCCServer":
                    return "ECLCC Server";
                case "ServiceType::TpEclServer":
                    return "ECL Server";
                case "ServiceType::TpEclScheduler":
                    return "ECL Scheduler";
                case "ServiceType::TpEspServer":
                    return "ESP Server";
                case "ServiceType::TpFTSlave":
                    return "FT Slave";
                case "ServiceType::TpSashaServer":
                    return "Sasha";
            }
            return "Unknown";
        },

        addServices: function (items) {
            arrayUtil.forEach(items, function (item) {
                this.appendChild(createTreeItem(Service, item.Name, this, item));
            }, this);
            return this;
        }
    });

    var Services = declare([TpCommon], {
        __hpcc_type: "Services",
        constructor: function (args) {
            args.__hpcc_displayName = "Services";
        },

        getIcon: function () {
            return "folder.png";
        },

        getLabel: function () {
            return "Services";
        },

        appendServiceType: function (property, data) {
            if (lang.exists(property, data)) {
                var newServiceType = createTreeItem(ServiceType, property, this);
                newServiceType.addServices(data[property]);
                this.appendChild(newServiceType);
            } else {
                throw "GJS";
            }
        },

        _TpDalisSetter: function (TpDalis) {
            this.appendServiceType("TpDali", TpDalis);
        },
        _TpDfuServersSetter: function (TpDfuServers) {
            this.appendServiceType("TpDfuServer", TpDfuServers);
        },
        _TpDropZonesSetter: function (TpDropZones) {
            this.appendServiceType("TpDropZone", TpDropZones);
        },
        _TpEclAgentsSetter: function (TpEclAgents) {
            this.appendServiceType("TpEclAgent", TpEclAgents);
        },
        _TpEclServersSetter: function (TpEclServers) {
            this.appendServiceType("TpEclServer", TpEclServers);
        },
        _TpEclCCServersSetter: function (TpEclCCServers) {
            this.appendServiceType("TpEclServer", TpEclCCServers);
        },
        _TpEclSchedulersSetter: function (TpEclSchedulers) {
            this.appendServiceType("TpEclScheduler", TpEclSchedulers);
        },
        _TpEspServersSetter: function (TpEspServers) {
            this.appendServiceType("TpEspServer", TpEspServers);
        },
        _TpFTSlavesSetter: function (TpFTSlaves) {
            this.appendServiceType("TpFTSlave", TpFTSlaves);
        },
        _TpSashaServersSetter: function (TpSashaServers) {
            this.appendServiceType("TpSashaServer", TpSashaServers);
        }
    });

    var TargetCluster = declare([TpCommon], {
        __hpcc_type: "TargetCluster",
        constructor: function (args) {
        },
        getIcon: function () {
            return "server.png";
        },
        getLabel: function () {
            return this.Name;
        },
        _TpEclAgentsSetter: function (TpEclAgents) {
            var context = this;
            if (lang.exists("TpEclAgent", TpEclAgents)) {
                arrayUtil.forEach(TpEclAgents.TpEclAgent, function (item, idx) {
                    this.appendChild(createTreeItem(TpEclAgent, item.Name, this, item));
                }, this);
            }
        },

        _TpEclCCServersSetter: function (TpEclCCServers) {
            if (lang.exists("TpEclServer", TpEclCCServers)) {
                arrayUtil.forEach(TpEclCCServers.TpEclServer, function (item, idx) {
                    this.appendChild(createTreeItem(TpEclCCServer, item.Name, this, item));
                }, this);
            }
        },

        _TpEclServersSetter: function (TpEclServers) {
            if (lang.exists("TpEclServer", TpEclServers)) {
                arrayUtil.forEach(TpEclServers.TpEclServer, function (item, idx) {
                    this.appendChild(createTreeItem(TpEclServer, item.Name, this, item));
                }, this);
            }
        },

        _TpEclSchedulersSetter: function (TpEclSchedulers) {
            if (lang.exists("TpEclScheduler", TpEclSchedulers)) {
                arrayUtil.forEach(TpEclSchedulers.TpEclScheduler, function (item, idx) {
                    this.appendChild(createTreeItem(TpEclScheduler, item.Name, this, item));
                }, this);
            }
        },

        _TpClustersSetter: function (TpClusters) {
            if (lang.exists("TpCluster", TpClusters)) {
                arrayUtil.forEach(TpClusters.TpCluster, function (item, idx) {
                    this.appendChild(createTreeItem(Cluster, item.Name, this, item));
                }, this);
            }
        }
    });

    var TargetClusterType = declare([TpCommon], {
        __hpcc_type: "TargetClusterType",
        constructor: function (args) {
            args.__hpcc_displayName = "TargetClusterType";
        },

        getIcon: function () {
            return "folder.png";
        },

        getLabel: function () {
            return this.Name;
        }
    });

    var TopologyRoot = declare([TopologyItem], {
        __hpcc_type: "TopologyRoot",
        getIcon: function () {
            return "workunit.png";
        },
        getLabel: function () {
            return "Topology";
        }
    });

    var TopologyTreeStore = declare([ESPTree.Store], {
        constructor: function() {
            this.viewMode("Debug");
            this.cachedTreeItems = {};
            this.cachedRelations = {};
            this.cachedRelationsPC = {};
        },
        createTreeNode: function (parentNode, treeItem) {
            var retVal = this.inherited(arguments);
            retVal.hasConfig = function () {
                return this.__hpcc_treeItem.Netaddress && this.__hpcc_treeItem.Directory;
            };
            retVal.getConfig = function () {
                return WsTopology.TpGetComponentFile({
                    request: {
                        CompType: this.__hpcc_treeItem.getCompType(),
                        CompName: this.__hpcc_treeItem.getCompName(),
                        NetAddress: this.__hpcc_treeItem.Netaddress,
                        Directory: this.__hpcc_treeItem.Directory,
                        FileType: "cfg",
                        OsType: this.__hpcc_treeItem.OS
                    }
                });
            };
            retVal.hasLogs = function () {
                return this.getNetaddress() && this.getLogDirectory();
            };
            retVal.getOS = function () {
                return this.__hpcc_treeItem.OS;
            };
            retVal.getNetaddress = function () {
                var retVal = null;
                if (this.__hpcc_treeItem.getNetaddress) {
                    retVal = this.__hpcc_treeItem.getNetaddress();
                }
                if (!retVal && parentNode && parentNode.__hpcc_treeItem.getNetaddress) {
                    retVal = parentNode.__hpcc_treeItem.getNetaddress();
                }
                return retVal;
            };
            retVal.getLogDirectory = function () {
                var retVal = null;
                if (this.__hpcc_treeItem.getLogDirectory) {
                    retVal = this.__hpcc_treeItem.getLogDirectory();
                }
                return retVal;
            };
            return retVal;
        },
        clear: function () {
            this.inherited(arguments);
            this.cachedTreeItems = {};
            this.cachedRelations = {};
            this.cachedRelationsPC = {};
        },
        viewMode: function (mode) {
            this._viewMode = mode;
        },
        createTreeItemXXX: function (Type, id, data) {
            var newItem = new Type({ __hpcc_store: this, __hpcc_id: id });
            var retVal = this.cachedTreeItems[newItem.getUniqueID()];
            if (!retVal) {
                retVal = newItem;
                this.cachedTreeItems[newItem.getUniqueID()] = retVal;
                this.cachedRelationsPC[newItem.getUniqueID()] = [];
            } else {
                //  Sanity Checking  ---
                for (key in data) {
                    if (!(data[key] instanceof Object)) {
                        if (retVal.get(key) != data[key] && key !== "HasThorSpareProcess") {
                            var d = 0;//throw "Duplicate ID";
                        }
                    }
                }
            }
            if (data) {
                retVal.updateData(data);
            }
            return retVal;
        },
        query: function (query, options) {
            var data = [];
            if (this.rootItem) {
                switch (this._viewMode) {
                    case "Debug":
                        data.push(this.createTreeNode(null, this.rootItem));
                        break;
                    case "Targets":
                        arrayUtil.forEach(this.rootItem.__hpcc_children, function (item) {
                            if (item.__hpcc_type === "TargetClusterType") {
                                data.push(this.createTreeNode(null, item));
                            }
                        }, this);
                        break;
                    case "Services":
                        arrayUtil.forEach(this.rootItem.__hpcc_children, function (item) {
                            if (item.__hpcc_type === "Services") {
                                arrayUtil.forEach(item.__hpcc_children, function (item2) {
                                    if (item2.__hpcc_type === "ServiceType") {
                                        data.push(this.createTreeNode(null, item2));
                                    }
                                }, this);
                            }
                        }, this);
                        break;
                    case "Machines":
                        var instance = {};
                        var machines = {};
                        var context = this;
                        function getMachines(treeItem, parentTreeItem) {
                            if (treeItem instanceof TpMachine) {
                                if (!machines[treeItem.Netaddress]) {
                                    var machineNode = context.createTreeNode(null, treeItem);
                                    machines[treeItem.Netaddress] = machineNode;
                                    data.push(machineNode);
                                }
                                if (parentTreeItem) {
                                    if (!instance[treeItem.getUniqueID()]) {
                                        instance[treeItem.getUniqueID()] = true;
                                        context.createTreeNode(machines[treeItem.Netaddress], parentTreeItem);
                                    }
                                }
                            }
                            arrayUtil.forEach(treeItem.__hpcc_children, function (child) {
                                getMachines(child, treeItem);
                            }, this);
                        }
                        getMachines(this.rootItem);
                        data.sort(function (a, b) {
                            aa = a.__hpcc_treeItem.Netaddress.split(".");
                            bb = b.__hpcc_treeItem.Netaddress.split(".");
                            var resulta = aa[0] * 0x1000000 + aa[1] * 0x10000 + aa[2] * 0x100 + aa[3] * 1;
                            var resultb = bb[0] * 0x1000000 + bb[1] * 0x10000 + bb[2] * 0x100 + bb[3] * 1;
                            return resulta - resultb;
                        });
                        break;
                }
            }
            return QueryResults(this.queryEngine({}, {})(data));
        },

        mayHaveChildren: function (treeNode) {
            return this.getChildren(treeNode, {}).length > 0;
        },

        getChildren: function (treeNode, options) {
            var data = [];
            if (treeNode.__hpcc_children.length) {
                data = treeNode.__hpcc_children;
            } else {
                switch (this._viewMode) {
                    case "Targets":
                        data = arrayUtil.map(treeNode.__hpcc_treeItem.__hpcc_children, function (item) {
                            return this.createTreeNode(treeNode, item);
                        }, this);
                        break;
                    case "Services":
                        if (!treeNode.__hpcc_parentNode) {
                            arrayUtil.forEach(treeNode.__hpcc_treeItem.__hpcc_children, function (child) {
                                var serviceNode = this.createTreeNode(treeNode, child);
                                var machines = [];
                                var bindings = [];
                                arrayUtil.forEach(child.__hpcc_children, function (gchild) {
                                    if (gchild instanceof TpMachine) {
                                        machines.push(gchild);
                                    } else if (gchild instanceof TpBinding) {
                                        bindings.push(gchild);
                                    }
                                }, this);
                                arrayUtil.forEach(bindings, function (binding) {
                                    var bindingNode = this.createTreeNode(serviceNode, binding);
                                    arrayUtil.forEach(machines, function (machine) {
                                        this.createTreeNode(bindingNode, machine);
                                    }, this);
                                }, this);
                                arrayUtil.forEach(machines, function (machine) {
                                    var machineNode = this.createTreeNode(serviceNode, machine);
                                    arrayUtil.forEach(bindings, function (binding) {
                                        this.createTreeNode(machineNode, binding);
                                    }, this);
                                }, this);
                                data.push(serviceNode);
                            }, this);
                        }
                        break;
                    case "Debug":
                        data = arrayUtil.map(treeNode.__hpcc_treeItem.__hpcc_children, function (item) {
                            return this.createTreeNode(treeNode, item);
                        }, this);
                        break;
                    default:
                        break;
                }
            }
            return QueryResults(this.queryEngine({}, {})(data));
        },

        refresh: function (callback) {
            this.clear();
            this.rootItem = createTreeItem(TopologyRoot, "root");

            var calls = [];
            var context = this;
            return all({
                targetClusterQuery: WsTopology.TpTargetClusterQuery({
                    request: {
                        Type: "ROOT"
                    }
                }).then(function (response) {
                    var clusterTypes = {};
                    var retVal = [];
                    if (lang.exists("TpTargetClusterQueryResponse.TpTargetClusters", response)) {
                        arrayUtil.forEach(response.TpTargetClusterQueryResponse.TpTargetClusters.TpTargetCluster, function (item, idx) {
                            if (!clusterTypes[item.Type]) {
                                clusterTypes[item.Type] = createTreeItem(TargetClusterType, item.Type, context.rootItem, { Name: item.Type })
                                retVal.push(clusterTypes[item.Type]);
                            }
                            clusterTypes[item.Type].appendChild(createTreeItem(TargetCluster, item.Name, context.rootItem, item));
                        }, this);
                    }
                    return retVal;
                }),
                serviceQuery: WsTopology.TpServiceQuery({
                    request: {
                        Type: "ALLSERVICES"
                    }
                }).then(function (response) {
                    var retVal = [];
                    if (lang.exists("TpServiceQueryResponse.ServiceList", response)) {
                        retVal.push(createTreeItem(Services, "Services", context.rootItem, response.TpServiceQueryResponse.ServiceList));
                    }
                    return retVal;
                })
            }).then(function (responses) {
                context.rootItem.appendChildren(responses.targetClusterQuery);
                context.rootItem.appendChildren(responses.serviceQuery);
                callback();
            });
        }
    });

    return {
        GetThor: function (thorName) {
            if (!ThorCache[thorName]) {
                ThorCache[thorName] = new Thor({
                    Name: thorName
                });
            }
            return ThorCache[thorName];
        },
        Store: TopologyTreeStore
    };
});
