﻿import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";
import * as Memory from "dojo/store/Memory";

import * as WsSMC from "./WsSMC";
import * as ESPUtil from "./ESPUtil";
import * as Utility from "./Utility";

var Store = declare([Memory], {
    idProperty: "__hpcc_id"
});

var Queue = declare([ESPUtil.Singleton, ESPUtil.Monitor], {

    constructor: function (id) {
        this.__hpcc_id = id;

        this._watched = [];
        this.children = new Memory({
            idProperty: "__hpcc_id",
            parent: this,
            data: []
        });
    },

    pause: function () {
        var context = this;
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

    resume: function () {
        var context = this;
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

    clear: function () {
        var context = this;
        return WsSMC.ClearQueue({
            request: {
                QueueName: this.QueueName
            }
        }).then(function (response) {
            context.clearChildren();
            return response;
        });
    },

    setPriority: function (wuid, priority) {    //  high, normal, low
        return WsSMC.SetJobPriority({
            request: {
                QueueName: this.QueueName,
                Wuid: wuid,
                Priority: priority
            }
        });
    },

    moveTop: function (wuid) {
        return WsSMC.MoveJobFront({
            request: {
                QueueName: this.QueueName,
                Wuid: wuid
            }
        });
    },

    moveUp: function (wuid) {
        return WsSMC.MoveJobUp({
            request: {
                QueueName: this.QueueName,
                Wuid: wuid
            }
        });
    },

    moveDown: function (wuid) {
        return WsSMC.MoveJobDown({
            request: {
                QueueName: this.QueueName,
                Wuid: wuid
            }
        });
    },

    moveBottom: function (wuid) {
        return WsSMC.MoveJobBack({
            request: {
                QueueName: this.QueueName,
                Wuid: wuid
            }
        });
    },

    canChildMoveUp: function (id) {
        return (this.getChildIndex(id) > 0);
    },

    canChildMoveDown: function (id) {
        return (this.getChildIndex(id) < this.getChildCount() - 1);
    },

    clearChildren: function () {
        this.children.setData([]);
        this.set("DisplaySize", "");
    },

    tmp: 0,
    addChild: function (wu) {
        wu.set("ESPQueue", this);
        if (!this.children.get(wu.__hpcc_id)) {
            this.children.add(wu);
        }
        if (!this._watched[wu.__hpcc_id]) {
            var context = this;
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

    getChild: function (id) {
        return this.children.get(id);
    },

    getChildIndex: function (id) {
        return this.children.index[id];
    },

    getChildCount: function () {
        return this.children.data.length;
    },

    queryChildren: function () {
        return this.children.query();
    }
});

var TargetCluster = declare([Queue], {
    _QueueNameSetter: function (QueueName) {
        this.QueueName = QueueName;
        this.ServerName = QueueName;
    },
    _ClusterTypeSetter: function (ClusterType) {
        this.ClusterType = ClusterType;
        switch (this.ClusterType) {
            case 1:
                this.ServerType = "HThorServer";
                break;
            case 2:
                this.ServerType = "RoxieServer";
                break;
            case 3:
                this.ServerType = "ThorMaster"
                break;
            default:
                this.ServerType = ""
        }
    },

    refresh: function () {
        var context = this;
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

    getDisplayName: function () {
        return this.ClusterName;
    },

    isNormal: function () {
        return this.ClusterStatus === 0;
    },

    isPaused: function () {
        switch (this.ClusterStatus) {
            case 1:
            case 2:
                return true;
        }
        return false;
    },

    pause: ESPUtil.override(function (inherited) {
        var context = this;
        return inherited().then(function (response) {
            context.updateData({
                ClusterStatus: 2
            });
            return response;
        });
    }),

    resume: ESPUtil.override(function (inherited) {
        var context = this;
        return inherited().then(function (response) {
            context.updateData({
                ClusterStatus: 0
            });
            return response;
        });
    }),

    getStateImageName: function () {
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

    getStateImage: function () {
        return Utility.getImageURL(this.getStateImageName());
    }
});

var ServerJobQueue = declare([Queue], {
    _ServerNameSetter: function (ServerName) {
        this.ServerName = ServerName;
        this.ClusterName = ServerName;
    },

    refresh: function () {
        var context = this;
        return WsSMC.GetStatusServerInfo({
            request: {
                ServerName: this.ServerName,
                ServerType: this.ServerType,
                NetworkAddress: this.NetworkAddress
            }
        }).then(function (response) {
            if (lang.exists("GetStatusServerInfoResponse.StatusServerInfo.ServerInfo", response)) {
                context.updateData(response.GetStatusServerInfoResponse.StatusServerInfo.ServerInfo);
            }
            return response;
        });
    },

    getDisplayName: function () {
        return this.ServerName;
    },

    isNormal: function () {
        return this.QueueStatus === "running";
    },

    isPaused: function () {
        if (this.QueueStatus === "paused") {
            return true;
        }
        return false;
    },

    pause: ESPUtil.override(function (inherited) {
        var context = this;
        return inherited().then(function (response) {
            context.updateData({
                QueueStatus: "paused"
            });
            return response;
        });
    }),

    resume: ESPUtil.override(function (inherited) {
        var context = this;
        return inherited().then(function (response) {
            context.updateData({
                QueueStatus: null
            });
            return response;
        });
    }),

    getStateImageName: function () {
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

    getStateImage: function () {
        return Utility.getImageURL(this.getStateImageName());
    }

});

var globalQueueStore = null;
var GetGlobalQueueStore = function () {
    if (!globalQueueStore) {
        globalQueueStore = new Store();
    }
    return globalQueueStore;
}

export function isInstanceOfQueue(obj) {
    return obj && obj.isInstanceOf && obj.isInstanceOf(Queue);
}

export function GetTargetCluster(name) {
    var store = GetGlobalQueueStore();
    var id = "TargetCluster::" + name;
    var retVal = store.get(id);
    if (!retVal) {
        retVal = new TargetCluster(id);
        store.put(retVal);
    }
    return retVal;
}

export function GetServerJobQueue(name) {
    var store = GetGlobalQueueStore();
    var id = "ServerJobQueue::" + name;
    var retVal = store.get(id);
    if (!retVal) {
        retVal = new ServerJobQueue(id);
        store.put(retVal);
    }
    return retVal;
}
