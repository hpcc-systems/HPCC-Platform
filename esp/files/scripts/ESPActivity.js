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
    "dojo/store/Memory",
    "dojo/store/Observable",

    "hpcc/WsSMC",
    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/ESPQueue",
    "hpcc/ESPWorkunit",
    "hpcc/ESPDFUWorkunit"

], function (declare, arrayUtil, lang, Memory, Observable,
    WsSMC, ESPUtil, ESPRequest, ESPQueue, ESPWorkunit, ESPDFUWorkunit) {

    var _workunits = {};

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
            this.store = new Store();
            this._watched = [];
            this.observableStore = new Observable(this.store)
        },

        isInstanceOfQueue: function (obj) {
            return ESPQueue.isInstanceOfQueue(obj);
        },

        isInstanceOfWorkunit: function (obj) {
            return ESPWorkunit.isInstanceOfWorkunit(obj) || ESPDFUWorkunit.isInstanceOfWorkunit(obj);
        },

        setBanner: function (bannerText) {
            this.getActivity({
                FromSubmitBtn: true,
                BannerAction: bannerText != "",
                EnableChatURL: 0,
                BannerContent: bannerText,
                BannerColor: "red",
                BannerSize: 4,
                BannerScroll: 2
            });
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
            if (callback && this.changedCount) {
                callback(this);
            }
            if (!this.hasCompleted) {
                var context = this;
                this.watch("changedCount", function (name, oldValue, newValue) {
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
                        queue = ESPQueue.GetServerJobQueue(item.QueueName);
                    }
                    queue.updateData(item);
                    queue.set("DisplayName", queue.getDisplayName());
                    queue.clearChildren();
                    targetClusters.push(queue);
                    targetClusterMap[queue.__hpcc_id] = queue;
                    if (!context._watched[queue.__hpcc_id]) {
                        context._watched[queue.__hpcc_id] = queue.watch("changedCount", function (name, oldValue, newValue) {
                            if (oldValue !== newValue) {
                                context.observableStore.notify(queue, queue.__hpcc_id);
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
                        queue = ESPQueue.GetServerJobQueue(item.QueueName);
                    }
                    var wu = item.Server === "DFUserver" ? ESPDFUWorkunit.Get(item.Wuid) : ESPWorkunit.Get(item.Wuid);
                    wu.updateData(lang.mixin({
                        __hpcc_id: item.Wuid,
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

    return {
        Get: function () {
            if (!globalActivity) {
                globalActivity = new Activity;
                globalActivity.startMonitor();
                globalActivity.refresh();
            }
            return globalActivity;
        },

        CreateActivityStore: function (options) {
            var store = new Store(options);
            return Observable(store);
        }
    };
});
