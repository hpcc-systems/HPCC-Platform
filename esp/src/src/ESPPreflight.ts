import * as arrayUtil from "dojo/_base/array";
import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";
import * as Observable from "dojo/store/Observable";
import * as QueryResults from "dojo/store/util/QueryResults";
import * as ESPRequest from "./ESPRequest";
import nlsHPCC from "./nlsHPCC";

const i18n = nlsHPCC;

const SystemServersStore = declare([ESPRequest.Store], {
    service: "WsTopology",
    action: "TpServiceQuery",
    responseQualifier: "TpServiceQueryResponse.ServiceList",
    idProperty: "hpcc_id",
    constructor(options) {
        if (options) {
            declare.safeMixin(this, options);
        }
    },
    preProcessRow(row) {
        lang.mixin(row, {
            Name: row.parent,
            childName: row.Name,
            hpcc_id: row.parent + "_" + row.Name,
            Configuration: false
        });
    },

    preProcessResponse(response, request) {
        const results = [];
        for (const key in response.ServiceList) {
            for (const i in response.ServiceList[key]) {
                if (key !== "TpEclServers") {
                    response.ServiceList[key][i].map(function (item) {
                        const cleanKey = key.replace("Tp", "");
                        results.push(item);
                        lang.mixin(item, {
                            parent: cleanKey
                        });
                    });
                }
            }
        }
        response.ServiceList = results;
    },

    mayHaveChildren(item) {
        return item.TpMachines;
    },

    getMachineIP(item) {
        if (item.TpMachines) {
            return item.TpMachines.TpMachine[0].Netaddress;
        } else {
            return "";
        }
    },

    getMachinePort(port) {
        if (port > 0) {
            return ":" + port;
        }
        return "";
    },

    getChildren(parent, options) {
        const children = [];
        const context = this;

        arrayUtil.forEach(parent.TpMachines.TpMachine, function (item, idx) {
            children.push({
                hpcc_id: parent.childName + "_" + item.Name + "_" + idx,
                Parent: parent,
                Name: parent.childName,
                Computer: item.Name,
                Netaddress: item.Netaddress,
                NetaddressWithPort: item.Netaddress + context.getMachinePort(item.Port),
                OS: item.OS,
                Directory: item.Directory,
                Type: item.Type,
                AuditLog: parent.AuditLogDirectory,
                Log: parent.LogDirectory,
                ChildQueue: parent.Queue,
                Informational: item.Type === "SparkThorProcess" || item.Type === "EspProcess",
                Component: "SystemServers",
                Configuration: true
            });
        });
        return QueryResults(children);
    }
});

const ClusterTargetStore = declare([ESPRequest.Store], {
    service: "WsTopology",
    action: "TpTargetClusterQuery",
    responseQualifier: "TpTargetClusterQueryResponse.TpTargetClusters.TpTargetCluster",
    idProperty: "hpcc_id",
    constructor(options) {
        if (options) {
            declare.safeMixin(this, options);
        }
    },
    preProcessRow(row) {
        lang.mixin(row, {
            hpcc_id: row.Name,
            displayName: row.Name,
            type: "targetClusterProcess",
            Component: "",
            Configuration: false
        });
    },
    mayHaveChildren(item) {
        return item.type === "targetClusterProcess";
    },
    getChildren(parent, options) {
        const context = this;
        const children = [];
        const tempArr = [];
        for (const key in parent) {
            if (typeof parent[key] === "object") {
                for (const i in parent[key]) {
                    if (key !== "TpEclServers") {
                        parent[key][i].map(function (item) {
                            tempArr.push(item);
                        });
                    }
                }
            }
        }

        arrayUtil.forEach(tempArr, function (item, idx) {
            children.push({
                hpcc_id: parent.Name + "_" + item.Name,
                Name: item.Type + " - " + item.Name,
                Type: item.Type,
                DaliServer: item.DaliServer ? true : false,
                Directory: item.TpMachines ? item.TpMachines.TpMachine[0].Directory : "",
                LogDir: item.LogDir,
                LogDirectory: item.LogDirectory,
                OS: item.TpMachines.TpMachine[0].OS,
                Platform: item.TpMachines ? context.getOS(item.TpMachines.TpMachine[0].OS) : "",
                Configuration: item.TpMachines ? true : false,
                Node: item.TpMachines ? item.TpMachines.TpMachine[0].Name : "",
                Netaddress: item.TpMachines ? item.TpMachines.TpMachine[0].Netaddress : "",
                Parent: parent,
                type: "targetClusterComponent"
            });
        });
        return QueryResults(children);
    },

    getMachineType(type) {
        switch (type) {
            case "RoxieCluster":
                return "ROXIEMACHINES";
            case "ThorCluster":
                return "THORMACHINES";
        }
    },

    getOS(int) {
        switch (int) {
            case 0:
                return "Windows";
            case 1:
                return "Solaris";
            case 2:
                return "Linux";
            default:
                return "Linux";
        }
    }
});

const ClusterProcessStore = declare([ESPRequest.Store], {
    service: "WsTopology",
    action: "TpClusterQuery",
    responseQualifier: "TpClusterQueryResponse.TpClusters.TpCluster",
    idProperty: "hpcc_id",
    constructor(options) {
        if (options) {
            declare.safeMixin(this, options);
        }
    },
    preProcessRow(row) {
        lang.mixin(row, {
            Platform: this.getOS(row.OS),
            hpcc_id: row.Name,
            displayName: row.Name,
            type: "clusterProcess",
            Component: row.Type,
            Configuration: true
        });
    },
    mayHaveChildren(item) {
        return item.type === "clusterProcess";
    },
    getChildren(parent, options) {
        const store = Observable(new ClusterProcessesList({
            parent
        }));
        return store.query({
            Type: this.getMachineType(parent.Type),
            Parent: parent,
            Cluster: parent.Name,
            LogDirectory: parent.LogDirectory,
            Path: parent.Path,
            Directory: parent.Directory
        });
    },

    getMachineType(type) {
        switch (type) {
            case "RoxieCluster":
                return "ROXIEMACHINES";
            case "ThorCluster":
                return "THORMACHINES";
        }
    },

    getOS(int) {
        switch (int) {
            case 0:
                return "Windows";
            case 1:
                return "Solaris";
            case 2:
            default:
                return "Linux";
        }
    }
});

const ClusterProcessesList = declare([ESPRequest.Store], {
    service: "WsTopology",
    action: "TpMachineQuery",
    responseQualifier: "TpMachineQueryResponse.TpMachines.TpMachine",
    idProperty: "hpcc_id",

    preProcessRow(row) {
        lang.mixin(row, {
            Platform: this.getOS(row.OS),
            hpcc_id: row.Name + "_" + row.Netaddress + "_" + row.Directory,
            displayName: row.Name,
            type: "machine",
            Component: row.Type,
            Channel: row.Channels,
            Domain: row.Domain,
            Directory: "",
            Parent: this.parent
        });
    },

    getOS(int) {
        switch (int) {
            case 0:
                return "Windows";
            case 1:
                return "Solaris";
            case 2:
            default:
                return "Linux";
        }
    }
});

export function getCondition(int) {
    switch (int) {
        case 1:
            return i18n.Normal;
        case 2:
            return i18n.Warning;
        case 3:
            return i18n.Minor;
        case 4:
            return i18n.Major;
        case 5:
            return i18n.Critical;
        case 6:
            return i18n.Fatal;
        default:
            return i18n.Unknown;
    }
}

export function getState(int) {
    switch (int) {
        case 0:
            return i18n.Unknown;
        case 1:
            return i18n.Starting;
        case 2:
            return i18n.Stopping;
        case 3:
            return i18n.Suspended;
        case 4:
            return i18n.Recycling;
        case 5:
            return i18n.Ready;
        case 6:
            return i18n.Busy;
        default:
            return i18n.Unknown;
    }
}

export function CreateTargetClusterStore(options) {
    const store = new ClusterTargetStore(options);
    return Observable(store);
}

export function CreateClusterProcessStore(options) {
    const store = new ClusterProcessStore(options);
    return Observable(store);
}

export function CreateSystemServersStore(options) {
    const store = new SystemServersStore(options);
    return Observable(store);
}

export function MachineQuery(params) {
    return ESPRequest.send("WsTopology", "TpMachineQuery", params);
}

export function GetConfiguration(params) {
    return ESPRequest.send("WsTopology", "TpGetComponentFile", params);
}
