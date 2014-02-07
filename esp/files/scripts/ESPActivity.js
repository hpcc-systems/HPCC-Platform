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
    "hpcc/ESPWorkunit",
    "hpcc/ESPDFUWorkunit"

], function (declare, arrayUtil, lang, Memory, Observable,
    WsSMC, ESPUtil, ESPRequest, ESPWorkunit, ESPDFUWorkunit) {

    var _workunits = {};

    var Store = declare([Memory], {
        idProperty: "__hpcc_id",
        mayHaveChildren: function (item) {
            return (item.children && item.children.length)
        },
        getChildren: function (parent, options) {
            var store = Observable(new Memory({
                idProperty: "__hpcc_id",
                parent: parent,
                _watched: [],
                data: []
            }));
            arrayUtil.forEach(parent.children, function (item, idx) {
                var wu = item.Server === "DFUserver" ? ESPDFUWorkunit.Get(item.Wuid) : ESPWorkunit.Get(item.Wuid);
                wu.updateData(item);
                try {
                    store.add(wu);
                    if (!store._watched[item.Wuid]) {
                        store._watched[item.Wuid] = wu.watch("changedCount", function (name, oldValue, newValue) {
                            if (oldValue !== newValue) {
                                store.notify(wu, item.__hpcc_id);
                            }
                        });
                    }
                } catch (e) {
                }
            });
            return store.query();
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
            this.inherited(arguments);
            this.store = new Store();
            this.observableStore = new Observable(this.store)
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
                    context.updateData(response.ActivityResponse);

                    var targetClusters = [];
                    var targetClusterMap = {};
                    context.refreshTargetClusters("HThorClusterList.TargetCluster", targetClusters, targetClusterMap);
                    context.refreshTargetClusters("ThorClusterList.TargetCluster", targetClusters, targetClusterMap);
                    context.refreshTargetClusters("RoxieClusterList.TargetCluster", targetClusters, targetClusterMap);
                    context.refreshServerJobQueue("ServerJobQueues.ServerJobQueue", targetClusters, targetClusterMap);
                    context.refreshActiveWorkunits("Running.ActiveWorkunit", targetClusters, targetClusterMap);
                    context.store.setData(targetClusters);
                    context.updateData({
                        targetClusters: targetClusters
                    });
                }
                return response;
            });
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

        refreshTargetClusters: function (targetClusterStr, targetClusters, targetClusterMap) {
            if (lang.exists(targetClusterStr, this)) {
                arrayUtil.forEach(lang.getObject(targetClusterStr, false, this), function (item, idx) {
                    item["__hpcc_type"] = "TargetCluster";
                    item["__hpcc_id"] = item.ClusterName;
                    item.children = [];
                    targetClusters.push(item);
                    targetClusterMap[item.ClusterName] = item;
                });
            }
        },

        refreshServerJobQueue: function (serverJobQueueStr, targetClusters, targetClusterMap) {
            if (lang.exists(serverJobQueueStr, this)) {
                arrayUtil.forEach(lang.getObject(serverJobQueueStr, false, this), function (item, idx) {
                    item["__hpcc_type"] = "TargetCluster";
                    item["__hpcc_id"] = item.QueueName;
                    item.children = [];
                    targetClusters.push(item);
                    targetClusterMap[item.QueueName] = item;
                });
            }
        },

        refreshActiveWorkunits: function (activeWorkunitsStr, targetClusters, targetClusterMap) {
            if (lang.exists(activeWorkunitsStr, this)) {
                arrayUtil.forEach(lang.getObject(activeWorkunitsStr, false, this), function (item, idx) {
                    item["__hpcc_id"] = item.Wuid;
                    targetClusterMap[item.ClusterName ? item.ClusterName : item.QueueName].children.push(item);
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
