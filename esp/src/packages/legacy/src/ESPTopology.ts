import * as arrayUtil from "dojo/_base/array";
import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";
import * as all from "dojo/promise/all";
import * as QueryResults from "dojo/store/util/QueryResults";

import * as ESPTree from "./ESPTree";
import * as ESPUtil from "./ESPUtil";
import * as WsTopology from "./WsTopology";

const ThorCache = {
};
const Thor = declare([ESPUtil.Singleton], {
    constructor(args) {
        if (args) {
            declare.safeMixin(this, args);
        }
    },

    refresh() {
        const context = this;
        return WsTopology.TpThorStatus({
            request: {
                Name: this.Name
            }
        }).then(function (response) {
            if (lang.exists("TpThorStatusResponse", response)) {
                context.updateData(response.TpThorStatusResponse);
                if (response.TpThorStatusResponse.Graph && response.TpThorStatusResponse.SubGraph) {
                    context.updateData({
                        GraphSummary: response.TpThorStatusResponse.Graph + "-" + response.TpThorStatusResponse.SubGraph
                    });
                }
            }
            return response;
        });
    }
});

const createTreeItem = function (Type, id, espParent?, data?) {
    if (!(espParent instanceof TopologyItem)) {
        if (!espParent && id !== "root") {
            // var d = 0;
        }
    }
    const retVal = new Type({ __hpcc_id: id, __hpcc_parent: espParent });
    if (data) {
        retVal.updateData(data);
    }
    return retVal;
};

const TopologyItem = declare([ESPTree.Item], {    // jshint ignore:line
    constructor(args) {
        this.__hpcc_children = [];
    },

    appendChild(child) {
        this.__hpcc_children.push(child);
    },

    appendChildren(children) {
        arrayUtil.forEach(children, function (child) {
            this.appendChild(child);
        }, this);
    },

    getLabel() {
        return this.__hpcc_displayName;
    },

    //  Helpers  ---
    getCompType() {
        if (this.__hpcc_parent && this.__hpcc_parent.Type) {
            return this.__hpcc_parent.Type;
        } else {
            return this.Type;
        }
    },

    getCompName() {
        if (this.__hpcc_parent && this.__hpcc_parent.Name) {
            return this.__hpcc_parent.Name;
        } else {
            return this.Name;
        }
    },

    getNetaddress() {
        if (this.Netaddress) {
            return this.Netaddress;
        } else if (this.__hpcc_parent) {
            if (this.__hpcc_parent.Netaddress) {
                return this.__hpcc_parent.Netaddress;
            }
        }
        return "";
    },

    getLogDirectory() {
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

const TpMachine = declare([TopologyItem], {
    __hpcc_type: "TpMachine",
    constructor(args) {
    },
    getIcon() {
        return "machine.png";
    },
    updateData: ESPUtil.override(function (inherited, data) {
        inherited(data);
        this.__hpcc_displayName = "[" + this.Netaddress + "] " + this.Name;
    })
});

const TpCommon = declare([TopologyItem], {
    _TpMachinesSetter(TpMachines) {
        if (lang.exists("TpMachine", TpMachines)) {
            arrayUtil.forEach(TpMachines.TpMachine, function (item, idx) {
                const newMachine = createTreeItem(TpMachine, item.Type + "_" + item.Netaddress + "_" + item.ProcessNumber + "_" + item.Directory, this, item);
                this.appendChild(newMachine);
            }, this);
        }
    },
    updateData: ESPUtil.override(function (inherited, data) {
        inherited(data);
        this.__hpcc_displayName = "[" + (this.Type ? this.Type : this.__hpcc_type) + "] " + this.Name;
    })
});

const TpService = declare([TpCommon], {
    __hpcc_type: "TpService",
    constructor(args) {
    },
    getLabel() {
        return "[" + this.Type + "] " + this.Name;
    }
});

const TpEclAgent = declare([TpService], {
    __hpcc_type: "TpEclAgent",
    constructor(args) {
    }
});

const TpEclServer = declare([TpService], {
    __hpcc_type: "TpEclServer",
    constructor(args) {
    }
});

const TpEclCCServer = declare([TpService], {
    __hpcc_type: "TpEclCCServer",
    constructor(args) {
    }
});

const TpEclScheduler = declare([TpService], {
    __hpcc_type: "TpEclScheduler",
    constructor(args) {
    }
});

const TpBinding = declare([TpCommon], {
    __hpcc_type: "TpBinding",
    constructor(args) {
    },

    getLabel() {
        return this.Service;
    }
});

const Cluster = declare([TpCommon], {
    __hpcc_type: "Cluster",
    constructor(args) {
    },
    getIcon() {
        return "cluster.png";
    },
    getLabel() {
        return this.Name;
    }
});

const Service = declare([TpCommon], {
    __hpcc_type: "Service",
    constructor(args) {
    },
    _TpBindingsSetter(TpBindings) {
        if (lang.exists("TpBinding", TpBindings)) {
            arrayUtil.forEach(TpBindings.TpBinding, function (item, idx) {
                this.appendChild(createTreeItem(TpBinding, item.Service + "::" + item.Port, this, item));
            }, this);
        }
    }
});

const ServiceType = declare([TpCommon], {
    __hpcc_type: "ServiceType",
    constructor(args) {
    },
    getIcon() {
        return "folder.png";
    },
    getLabel() {
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

    addServices(items) {
        arrayUtil.forEach(items, function (item) {
            this.appendChild(createTreeItem(Service, item.Name, this, item));
        }, this);
        return this;
    }
});

const Services = declare([TpCommon], {
    __hpcc_type: "Services",
    constructor(args) {
        args.__hpcc_displayName = "Services";
    },

    getIcon() {
        return "folder.png";
    },

    getLabel() {
        return "Services";
    },

    appendServiceType(property, data) {
        if (lang.exists(property, data)) {
            const newServiceType = createTreeItem(ServiceType, property, this);
            newServiceType.addServices(data[property]);
            this.appendChild(newServiceType);
        } else {
            throw new Error("GJS");
        }
    },

    _TpDalisSetter(TpDalis) {
        this.appendServiceType("TpDali", TpDalis);
    },
    _TpDfuServersSetter(TpDfuServers) {
        this.appendServiceType("TpDfuServer", TpDfuServers);
    },
    _TpDropZonesSetter(TpDropZones) {
        this.appendServiceType("TpDropZone", TpDropZones);
    },
    _TpEclAgentsSetter(TpEclAgents) {
        this.appendServiceType("TpEclAgent", TpEclAgents);
    },
    _TpEclServersSetter(TpEclServers) {
        this.appendServiceType("TpEclServer", TpEclServers);
    },
    _TpEclCCServersSetter(TpEclCCServers) {
        this.appendServiceType("TpEclServer", TpEclCCServers);
    },
    _TpEclSchedulersSetter(TpEclSchedulers) {
        this.appendServiceType("TpEclScheduler", TpEclSchedulers);
    },
    _TpEspServersSetter(TpEspServers) {
        this.appendServiceType("TpEspServer", TpEspServers);
    },
    _TpFTSlavesSetter(TpFTSlaves) {
        this.appendServiceType("TpFTSlave", TpFTSlaves);
    },
    _TpSashaServersSetter(TpSashaServers) {
        this.appendServiceType("TpSashaServer", TpSashaServers);
    }
});

const TargetCluster = declare([TpCommon], {
    __hpcc_type: "TargetCluster",
    constructor(args) {
    },
    getIcon() {
        return "server.png";
    },
    getLabel() {
        return this.Name;
    },
    _TpEclAgentsSetter(TpEclAgents) {
        if (lang.exists("TpEclAgent", TpEclAgents)) {
            arrayUtil.forEach(TpEclAgents.TpEclAgent, function (item, idx) {
                this.appendChild(createTreeItem(TpEclAgent, item.Name, this, item));
            }, this);
        }
    },

    _TpEclCCServersSetter(TpEclCCServers) {
        if (lang.exists("TpEclServer", TpEclCCServers)) {
            arrayUtil.forEach(TpEclCCServers.TpEclServer, function (item, idx) {
                this.appendChild(createTreeItem(TpEclCCServer, item.Name, this, item));
            }, this);
        }
    },

    _TpEclServersSetter(TpEclServers) {
        if (lang.exists("TpEclServer", TpEclServers)) {
            arrayUtil.forEach(TpEclServers.TpEclServer, function (item, idx) {
                this.appendChild(createTreeItem(TpEclServer, item.Name, this, item));
            }, this);
        }
    },

    _TpEclSchedulersSetter(TpEclSchedulers) {
        if (lang.exists("TpEclScheduler", TpEclSchedulers)) {
            arrayUtil.forEach(TpEclSchedulers.TpEclScheduler, function (item, idx) {
                this.appendChild(createTreeItem(TpEclScheduler, item.Name, this, item));
            }, this);
        }
    },

    _TpClustersSetter(TpClusters) {
        if (lang.exists("TpCluster", TpClusters)) {
            arrayUtil.forEach(TpClusters.TpCluster, function (item, idx) {
                this.appendChild(createTreeItem(Cluster, item.Name, this, item));
            }, this);
        }
    }
});

const TargetClusterType = declare([TpCommon], {
    __hpcc_type: "TargetClusterType",
    constructor(args) {
        args.__hpcc_displayName = "TargetClusterType";
    },

    getIcon() {
        return "folder.png";
    },

    getLabel() {
        return this.Name;
    }
});

const TopologyRoot = declare([TopologyItem], {
    __hpcc_type: "TopologyRoot",
    getIcon() {
        return "workunit.png";
    },
    getLabel() {
        return "Topology";
    }
});

const TopologyTreeStore = declare([ESPTree.Store], {
    constructor() {
        this.viewMode("Debug");
        this.cachedTreeItems = {};
        this.cachedRelations = {};
        this.cachedRelationsPC = {};
    },
    createTreeNode: ESPUtil.override(function (inherited, parentNode, treeItem) {
        const retVal = inherited(parentNode, treeItem);
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
            let retVal = null;
            if (this.__hpcc_treeItem.getNetaddress) {
                retVal = this.__hpcc_treeItem.getNetaddress();
            }
            if (!retVal && parentNode && parentNode.__hpcc_treeItem.getNetaddress) {
                retVal = parentNode.__hpcc_treeItem.getNetaddress();
            }
            return retVal;
        };
        retVal.getLogDirectory = function () {
            let retVal = null;
            if (this.__hpcc_treeItem.getLogDirectory) {
                retVal = this.__hpcc_treeItem.getLogDirectory();
            }
            return retVal;
        };
        return retVal;
    }),
    clear: ESPUtil.override(function (inherited) {
        inherited(arguments);
        this.cachedTreeItems = {};
        this.cachedRelations = {};
        this.cachedRelationsPC = {};
    }),
    viewMode(mode) {
        this._viewMode = mode;
    },
    createTreeItemXXX(Type, id, data) {
        const newItem = new Type({ __hpcc_store: this, __hpcc_id: id });
        let retVal = this.cachedTreeItems[newItem.getUniqueID()];
        if (!retVal) {
            retVal = newItem;
            this.cachedTreeItems[newItem.getUniqueID()] = retVal;
            this.cachedRelationsPC[newItem.getUniqueID()] = [];
        } else {
            //  Sanity Checking  ---
            for (const key in data) {
                if (!(data[key] instanceof Object)) {
                    if (retVal.get(key) !== data[key] && key !== "HasThorSpareProcess") {
                        // var d = 0;//throw "Duplicate ID";
                    }
                }
            }
        }
        if (data) {
            retVal.updateData(data);
        }
        return retVal;
    },
    query(query, options) {
        const data = [];
        let instance = {};
        let machines = {};
        const context = this;

        function getMachines(treeItem, parentTreeItem?) {
            if (treeItem instanceof TpMachine) {
                if (!machines[treeItem.Netaddress]) {
                    const machineNode = context.createTreeNode(null, treeItem);
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
                    instance = {};
                    machines = {};
                    getMachines(this.rootItem);
                    data.sort(function (a, b) {
                        const aa = a.__hpcc_treeItem.Netaddress.split(".");
                        const bb = b.__hpcc_treeItem.Netaddress.split(".");
                        const resulta = aa[0] * 0x1000000 + aa[1] * 0x10000 + aa[2] * 0x100 + aa[3] * 1;
                        const resultb = bb[0] * 0x1000000 + bb[1] * 0x10000 + bb[2] * 0x100 + bb[3] * 1;
                        return resulta - resultb;
                    });
                    break;
            }
        }
        return QueryResults(this.queryEngine({}, {})(data));
    },

    mayHaveChildren(treeNode) {
        return this.getChildren(treeNode, {}).length > 0;
    },

    getChildren(treeNode, options) {
        let data = [];
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
                            const serviceNode = this.createTreeNode(treeNode, child);
                            const machines = [];
                            const bindings = [];
                            arrayUtil.forEach(child.__hpcc_children, function (gchild) {
                                if (gchild instanceof TpMachine) {
                                    machines.push(gchild);
                                } else if (gchild instanceof TpBinding) {
                                    bindings.push(gchild);
                                }
                            }, this);
                            arrayUtil.forEach(bindings, function (binding) {
                                const bindingNode = this.createTreeNode(serviceNode, binding);
                                arrayUtil.forEach(machines, function (machine) {
                                    this.createTreeNode(bindingNode, machine);
                                }, this);
                            }, this);
                            arrayUtil.forEach(machines, function (machine) {
                                const machineNode = this.createTreeNode(serviceNode, machine);
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

    refresh(callback) {
        this.clear();
        this.rootItem = createTreeItem(TopologyRoot, "root");

        const context = this;
        return all({
            targetClusterQuery: WsTopology.TpTargetClusterQuery({
                request: {
                    Type: "ROOT"
                }
            }).then(function (response) {
                const clusterTypes = {};
                const retVal = [];
                if (lang.exists("TpTargetClusterQueryResponse.TpTargetClusters", response)) {
                    arrayUtil.forEach(response.TpTargetClusterQueryResponse.TpTargetClusters.TpTargetCluster, function (item, idx) {
                        if (!clusterTypes[item.Type]) {
                            clusterTypes[item.Type] = createTreeItem(TargetClusterType, item.Type, context.rootItem, { Name: item.Type });
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
                const retVal = [];
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

export function GetThor(thorName) {
    if (!ThorCache[thorName]) {
        ThorCache[thorName] = new Thor({
            Name: thorName
        });
    }
    return ThorCache[thorName];
}

export const Store = TopologyTreeStore;
