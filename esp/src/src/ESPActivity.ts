import * as declare from "dojo/_base/declare";
import * as arrayUtil from "dojo/_base/array";
import * as lang from "dojo/_base/lang";
import * as Memory from "dojo/store/Memory";
import * as Observable from "dojo/store/Observable";

import * as WsSMC from "./WsSMC";
import * as ESPUtil from "./ESPUtil";
import * as ESPQueue from "./ESPQueue";
import * as ESPWorkunit from "./ESPWorkunit";
import * as ESPDFUWorkunit from "./ESPDFUWorkunit";
import * as WsWorkunits from "./WsWorkunits";

var Store = declare([Memory], {
    idProperty: "__hpcc_id",
    mayHaveChildren: function (item) {
        return (item.getChildCount && item.getChildCount());
    },
    getChildren: function (parent, options) {
        return parent.queryChildren();
    }
});

var Activity = declare([ESPUtil.Singleton, ESPUtil.Monitor], {
    //  Asserts  ---
    _assertHasWuid: function () {
        if (!this.Wuid) {
            throw new Error("Wuid cannot be empty.");
        }
    },
    //  Attributes  ---
    _StateIDSetter: function (StateID) {
        this.StateID = StateID;
        var actionEx = lang.exists("ActionEx", this) ? this.ActionEx : null;
        this.set("hasCompleted", WsWorkunits.isComplete(this.StateID, actionEx));
    },

    //  ---  ---  ---
    constructor: function (args) {
        this._watched = [];
        this.store = new Store();
        this.observableStore = new Observable(this.store)
    },

    isInstanceOfQueue: function (obj) {
        return ESPQueue.isInstanceOfQueue(obj);
    },

    isInstanceOfWorkunit: function (obj) {
        return ESPWorkunit.isInstanceOfWorkunit(obj) || ESPDFUWorkunit.isInstanceOfWorkunit(obj);
    },

    setBanner: function (request) {
        lang.mixin(request, {
            FromSubmitBtn: true,
            BannerAction: request.BannerAction ? 1 : 0,
            EnableChatURL: 0
        });
        this.getActivity(request);
    },

    resolve: function (id) {
        var queue = this.observableStore.get(id);
        if (queue) {
            return queue;
        }

        var wu = id[0] === "D" ? ESPDFUWorkunit.Get(id) : ESPWorkunit.Get(id);
        if (wu) {
            //  is wu still in a queue?
            queue = wu.get("ESPQueue");
            if (queue) {
                return queue.getChild(id);
            }
        }
        return null;
    },

    monitor: function (callback) {
        if (callback && this.__hpcc_changedCount) {
            callback(this);
        }
        if (!this.hasCompleted) {
            var context = this;
            this.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                if (oldValue !== newValue && newValue) {
                    if (callback) {
                        callback(context);
                    }
                }
            });
        }
    },

    getActivity: function (request) {
        var context = this;
        return WsSMC.Activity({
            request: request
        }).then(function (response) {
            if (lang.exists("ActivityResponse", response)) {
                var targetClusters = [];
                var targetClusterMap = {};
                context.refreshTargetClusters(lang.getObject("ActivityResponse.HThorClusterList.TargetCluster", false, response), targetClusters, targetClusterMap);
                context.refreshTargetClusters(lang.getObject("ActivityResponse.ThorClusterList.TargetCluster", false, response), targetClusters, targetClusterMap);
                context.refreshTargetClusters(lang.getObject("ActivityResponse.RoxieClusterList.TargetCluster", false, response), targetClusters, targetClusterMap);
                context.refreshTargetClusters(lang.getObject("ActivityResponse.ServerJobQueues.ServerJobQueue", false, response), targetClusters, targetClusterMap);
                context.refreshActiveWorkunits(lang.getObject("ActivityResponse.Running.ActiveWorkunit", false, response), targetClusters, targetClusterMap);
                context.store.setData(targetClusters);
                context.updateData(response.ActivityResponse);
            }
            return response;
        });
    },

    refreshTargetClusters: function (responseTargetClusters, targetClusters, targetClusterMap) {
        var context = this;
        if (responseTargetClusters) {
            arrayUtil.forEach(responseTargetClusters, function (item, idx) {
                var queue = null;
                if (item.ClusterName) {
                    queue = ESPQueue.GetTargetCluster(item.ClusterName);
                } else {
                    queue = ESPQueue.GetServerJobQueue(item.ServerName);
                }
                queue.updateData(item);
                queue.set("DisplayName", queue.getDisplayName());
                queue.clearChildren();
                targetClusters.push(queue);
                targetClusterMap[queue.__hpcc_id] = queue;
                if (!context._watched[queue.__hpcc_id]) {
                    context._watched[queue.__hpcc_id] = queue.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                        if (oldValue !== newValue) {
                            if (context.observableStore.get(queue.__hpcc_id)) {
                                context.observableStore.notify(queue, queue.__hpcc_id);
                            }
                        }
                    });
                }
            });
        }
    },

    refreshActiveWorkunits: function (responseActiveWorkunits, targetClusters, targetClusterMap) {
        if (responseActiveWorkunits) {
            arrayUtil.forEach(responseActiveWorkunits, function (item, idx) {
                item["__hpcc_id"] = item.Wuid;
                var queue = null;
                if (item.ClusterName) {
                    queue = ESPQueue.GetTargetCluster(item.ClusterName);
                } else {
                    queue = ESPQueue.GetServerJobQueue(item.ServerName);
                }
                var wu = item.Server === "DFUserver" ? ESPDFUWorkunit.Get(item.Wuid) : ESPWorkunit.Get(item.Wuid);
                wu.updateData(lang.mixin({
                    __hpcc_id: item.Wuid
                }, item));
                queue.addChild(wu);
            });
        }
    },

    inRefresh: false,
    refresh: function (full) {
        var context = this;
        if (this.inRefresh) {
            return;
        }
        this.inRefresh = true;
        this.getActivity({
        }).then(function (response) {
            context.inRefresh = false;
        }, function (err) {
            context.inRefresh = false;
        });
    },

    getStore: function () {
        return this.observableStore;
    }
});
var globalActivity = null;

export function Get() {
    if (!globalActivity) {
        globalActivity = new Activity;
        globalActivity.startMonitor();
        globalActivity.refresh();
    }
    return globalActivity;
}

export function CreateActivityStore(options) {
    var store = new Store(options);
    return Observable(store);
}
