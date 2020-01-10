import * as arrayUtil from "dojo/_base/array";
import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";
import * as Memory from "dojo/store/Memory";
import * as Observable from "dojo/store/Observable";

import * as ESPDFUWorkunit from "./ESPDFUWorkunit";
import * as ESPQueue from "./ESPQueue";
import * as ESPUtil from "./ESPUtil";
import * as ESPWorkunit from "./ESPWorkunit";
import * as WsSMC from "./WsSMC";
import * as WsWorkunits from "./WsWorkunits";

const Store = declare([Memory], {
    idProperty: "__hpcc_id",
    mayHaveChildren(item) {
        return (item.getChildCount && item.getChildCount());
    },
    getChildren(parent, options) {
        return parent.queryChildren();
    }
});

const Activity = declare([ESPUtil.Singleton, ESPUtil.Monitor], {
    //  Asserts  ---
    _assertHasWuid() {
        if (!this.Wuid) {
            throw new Error("Wuid cannot be empty.");
        }
    },
    //  Attributes  ---
    _StateIDSetter(StateID) {
        this.StateID = StateID;
        const actionEx = lang.exists("ActionEx", this) ? this.ActionEx : null;
        this.set("hasCompleted", WsWorkunits.isComplete(this.StateID, actionEx));
    },

    //  ---  ---  ---
    constructor(args) {
        this._watched = [];
        this.store = new Store();
        this.observableStore = new Observable(this.store);
    },

    isInstanceOfQueue(obj) {
        return ESPQueue.isInstanceOfQueue(obj);
    },

    isInstanceOfWorkunit(obj) {
        return ESPWorkunit.isInstanceOfWorkunit(obj) || ESPDFUWorkunit.isInstanceOfWorkunit(obj);
    },

    setBanner(request) {
        lang.mixin(request, {
            FromSubmitBtn: true,
            BannerAction: request.BannerAction ? 1 : 0,
            EnableChatURL: 0
        });
        this.getActivity(request);
    },

    resolve(id) {
        let queue = this.observableStore.get(id);
        if (queue) {
            return queue;
        }

        const wu = id[0] === "D" ? ESPDFUWorkunit.Get(id) : ESPWorkunit.Get(id);
        if (wu) {
            //  is wu still in a queue?
            queue = wu.get("ESPQueue");
            if (queue) {
                return queue.getChild(id);
            }
        }
        return null;
    },

    monitor(callback) {
        if (callback && this.__hpcc_changedCount) {
            callback(this);
        }
        if (!this.hasCompleted) {
            const context = this;
            this.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                if (oldValue !== newValue && newValue) {
                    if (callback) {
                        callback(context);
                    }
                }
            });
        }
    },

    getActivity(request) {
        const context = this;
        return WsSMC.Activity({
            request
        }).then(function (response) {
            if (lang.exists("ActivityResponse", response)) {
                const targetClusters = [];
                const targetClusterMap = {};
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

    refreshTargetClusters(responseTargetClusters, targetClusters, targetClusterMap) {
        const context = this;
        if (responseTargetClusters) {
            arrayUtil.forEach(responseTargetClusters, function (item, idx) {
                if (lang.exists("Queues.ServerJobQueue", item)) {
                    arrayUtil.forEach(item.Queues.ServerJobQueue, function (queueItem) {
                        context.refreshTargetCluster(item, queueItem, targetClusters, targetClusterMap);
                    });
                } else {
                    context.refreshTargetCluster(item, undefined, targetClusters, targetClusterMap);
                }
            });
        }
    },

    refreshTargetCluster(item, queueItem, targetClusters, targetClusterMap) {
        let queue = null;
        if (item.ClusterName) {
            queue = ESPQueue.GetTargetCluster(item.ClusterName, true);
        } else {
            queue = ESPQueue.GetServerJobQueue(queueItem ? queueItem.QueueName : item.ServerName, true);
        }
        queue.updateData(item);
        if (queueItem) {
            queue.updateData(queueItem);
        }
        queue.set("DisplayName", queue.getDisplayName());
        queue.clearChildren();
        targetClusters.push(queue);
        targetClusterMap[queue.__hpcc_id] = queue;
        const context = this;
        if (!this._watched[queue.__hpcc_id]) {
            this._watched[queue.__hpcc_id] = queue.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                if (oldValue !== newValue) {
                    if (context.observableStore.get(queue.__hpcc_id)) {
                        context.observableStore.notify(queue, queue.__hpcc_id);
                    }
                }
            });
        }
    },

    refreshActiveWorkunits(responseActiveWorkunits, targetClusters, targetClusterMap) {
        if (responseActiveWorkunits) {
            arrayUtil.forEach(responseActiveWorkunits, function (item, idx) {
                item["__hpcc_id"] = item.Wuid;
                let queue = null;
                if (item.QueueName) {
                    queue = ESPQueue.GetServerJobQueue(item.QueueName);
                }
                if (!queue) {
                    if (item.ClusterName) {
                        queue = ESPQueue.GetTargetCluster(item.ClusterName);
                    } else {
                        queue = ESPQueue.GetServerJobQueue(item.ServerName);
                    }
                }
                const wu = item.Server === "DFUserver" ? ESPDFUWorkunit.Get(item.Wuid) : ESPWorkunit.Get(item.Wuid);
                wu.updateData(lang.mixin({
                    __hpcc_id: item.Wuid,
                    component: "ActivityWidget"
                }, item));
                if (!wu.isComplete || !wu.isComplete()) {
                    queue.addChild(wu);
                }
            });
        }
    },

    inRefresh: false,
    refresh(full) {
        const context = this;
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

    getStore() {
        return this.observableStore;
    }
});
let globalActivity = null;

export function Get() {
    if (!globalActivity) {
        globalActivity = new Activity();
        globalActivity.startMonitor();
        globalActivity.refresh();
    }
    return globalActivity;
}

export function CreateActivityStore(options) {
    const store = new Store(options);
    return Observable(store);
}
