import * as arrayUtil from "dojo/_base/array";
import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";

import * as ESPUtil from "./ESPUtil";
import * as Utility from "./Utility";
import * as WsSMC from "./WsSMC";
import { Memory } from "./Memory";

class Store extends Memory {
    idProperty: "__hpcc_id"
}

class QueueMemory extends Memory {

    idProperty = "__hpcc_id";
    data = [];

    constructor(protected parent) {
        super();
    }

}

const Queue = declare([ESPUtil.Singleton, ESPUtil.Monitor], {

    constructor(id) {
        this.__hpcc_id = id;

        this._watched = [];
        this.children = new QueueMemory(this);
    },

    pause() {
        const context = this;
        return WsSMC.PauseQueue({
            request: {
                ClusterType: this.ServerType,
                QueueName: this.QueueName,
                Cluster: this.ClusterName,
                ServerType: this.ServerType,
                NetworkAddress: this.NetworkAddress
            }
        }).then(function (response) {
            context.refresh();
        });
    },

    resume() {
        const context = this;
        return WsSMC.ResumeQueue({
            request: {
                ClusterType: this.ServerType,
                QueueName: this.QueueName,
                Cluster: this.ClusterName,
                ServerType: this.ServerType,
                NetworkAddress: this.NetworkAddress
            }
        }).then(function (response) {
            context.refresh();
        });
    },

    clear() {
        const context = this;
        return WsSMC.ClearQueue({
            request: {
                QueueName: this.QueueName,
                ServerType: this.ServerType,
                NetworkAddress: this.NetworkAddress,
                Port: this.Port
            }
        }).then(function (response) {
            context.clearChildren();
            return response;
        });
    },

    setPriority(wuid, priority) {    //  high, normal, low
        return WsSMC.SetJobPriority({
            request: {
                QueueName: this.QueueName,
                Wuid: wuid,
                Priority: priority
            }
        });
    },

    moveTop(wuid) {
        return WsSMC.MoveJobFront({
            request: {
                QueueName: this.QueueName,
                Wuid: wuid
            }
        });
    },

    moveUp(wuid) {
        return WsSMC.MoveJobUp({
            request: {
                QueueName: this.QueueName,
                Wuid: wuid
            }
        });
    },

    moveDown(wuid) {
        return WsSMC.MoveJobDown({
            request: {
                QueueName: this.QueueName,
                Wuid: wuid
            }
        });
    },

    moveBottom(wuid) {
        return WsSMC.MoveJobBack({
            request: {
                QueueName: this.QueueName,
                Wuid: wuid
            }
        });
    },

    canChildMoveUp(id) {
        return (this.getChildIndex(id) > 0);
    },

    canChildMoveDown(id) {
        return (this.getChildIndex(id) < this.getChildCount() - 1);
    },

    clearChildren() {
        this.children.setData([]);
        this.set("DisplaySize", "");
    },

    tmp: 0,
    addChild(wu) {
        wu.set("ESPQueue", this);
        if (!this.children.get(wu.__hpcc_id)) {
            this.children.add(wu);
        }
        if (!this._watched[wu.__hpcc_id]) {
            const context = this;
            this._watched[wu.__hpcc_id] = wu.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                if (oldValue !== newValue) {
                    //  If child changes force the parent to refresh...
                    context.updateData({
                        childChangedCount: ++context.tmp
                    });
                }
            });
        }
        this.set("DisplaySize", this.getChildCount());
    },

    getChild(id) {
        return this.children.get(id);
    },

    getChildIndex(id) {
        return this.children.index[id];
    },

    getChildCount() {
        return this.children.data.length;
    },

    queryChildren() {
        return this.children.query();
    }
});

const TargetCluster = declare([Queue], {
    _QueueNameSetter(QueueName) {
        this.QueueName = QueueName;
        this.ServerName = QueueName;
    },
    _ClusterTypeSetter(ClusterType) {
        this.ClusterType = ClusterType;
        switch (this.ClusterType) {
            case 1:
                this.ServerType = "HThorServer";
                break;
            case 2:
                this.ServerType = "RoxieServer";
                break;
            case 3:
                this.ServerType = "ThorMaster";
                break;
            default:
                this.ServerType = "";
        }
    },

    refresh() {
        const context = this;
        return WsSMC.GetStatusServerInfo({
            request: {
                ServerName: this.ClusterName,
                ServerType: this.ServerType,
                NetworkAddress: this.NetworkAddress
            }
        }).then(function (response) {
            if (lang.exists("GetStatusServerInfoResponse.StatusServerInfo.TargetClusterInfo", response)) {
                context.updateData(response.GetStatusServerInfoResponse.StatusServerInfo.TargetClusterInfo);
            }
            return response;
        });
    },

    getDisplayName() {
        return this.ServerType + (this.ClusterName ? " - " + this.ClusterName : "");
    },

    isNormal() {
        return this.ClusterStatus === 0;
    },

    isPaused() {
        switch (this.ClusterStatus) {
            case 1:
            case 2:
                return true;
        }
        return false;
    },

    pause: ESPUtil.override(function (inherited) {
        const context = this;
        return inherited(arguments).then(function (response) {
            context.updateData({
                ClusterStatus: 2
            });
            return response;
        });
    }),

    resume: ESPUtil.override(function (inherited) {
        const context = this;
        return inherited(arguments).then(function (response) {
            context.updateData({
                ClusterStatus: 0
            });
            return response;
        });
    }),

    getStateImageName() {
        switch (this.ClusterStatus) {
            case 1:
            case 2:
                return "server_paused.png";
            case 3:
            case 4:
                return "server_notfound.png";
        }
        return "server.png";
    },

    getStateImage() {
        return Utility.getImageURL(this.getStateImageName());
    }
});

const ServerJobQueue = declare([Queue], {
    _ServerNameSetter(ServerName) {
        this.ServerName = ServerName;
        this.ClusterName = ServerName;
    },

    refresh() {
        const context = this;
        return WsSMC.GetStatusServerInfo({
            request: {
                ServerName: this.ServerName,
                ServerType: this.ServerType,
                NetworkAddress: this.NetworkAddress
            }
        }).then(function (response) {
            if (lang.exists("GetStatusServerInfoResponse.StatusServerInfo.ServerInfo.Queues.ServerJobQueue", response)) {
                arrayUtil.forEach(response.GetStatusServerInfoResponse.StatusServerInfo.ServerInfo.Queues.ServerJobQueue, function (queueItem) {
                    if (queueItem.QueueName === context.QueueName) {
                        context.updateData(response.GetStatusServerInfoResponse.StatusServerInfo.ServerInfo);
                        context.updateData(queueItem);
                    }
                });
            }
            return response;
        });
    },

    getDisplayName() {
        return this.ServerName + (this.QueueName ? " - " + this.QueueName : "");
    },

    isNormal() {
        return this.QueueStatus === "running";
    },

    isPaused() {
        if (this.QueueStatus === "paused") {
            return true;
        }
        return false;
    },

    pause: ESPUtil.override(function (inherited) {
        const context = this;
        return inherited(arguments).then(function (response) {
            context.updateData({
                QueueStatus: "paused"
            });
            return response;
        });
    }),

    resume: ESPUtil.override(function (inherited) {
        const context = this;
        return inherited(arguments).then(function (response) {
            context.updateData({
                QueueStatus: null
            });
            return response;
        });
    }),

    getStateImageName() {
        switch (this.QueueStatus) {
            case "running":
                return "server.png";
            case "paused":
                return "server_paused.png";
            default:
                console.log("ESPQueue:  New State - " + this.QueueStatus);
        }
        return "server.png";
    },

    getStateImage() {
        return Utility.getImageURL(this.getStateImageName());
    }

});

let globalQueueStore = null;
const GetGlobalQueueStore = function () {
    if (!globalQueueStore) {
        globalQueueStore = new Store();
    }
    return globalQueueStore;
};

export function isInstanceOfQueue(obj) {
    return obj && obj.isInstanceOf && obj.isInstanceOf(Queue);
}

export function GetTargetCluster(name, createIfMissing = false) {
    const store = GetGlobalQueueStore();
    const id = "TargetCluster::" + name;
    let retVal = store.get(id);
    if (!retVal && createIfMissing) {
        retVal = new TargetCluster(id);
        store.put(retVal);
    }
    return retVal;
}

export function GetServerJobQueue(name, createIfMissing = false) {
    const store = GetGlobalQueueStore();
    const id = "ServerJobQueue::" + name;
    let retVal = store.get(id);
    if (!retVal && createIfMissing) {
        retVal = new ServerJobQueue(id);
        store.put(retVal);
    }
    return retVal;
}
