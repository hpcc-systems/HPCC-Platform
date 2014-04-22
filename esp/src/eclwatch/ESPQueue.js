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

    var Store = declare([Memory], {
        idProperty: "__hpcc_id"
    });

    var Queue = declare([ESPUtil.Singleton, ESPUtil.Monitor], {

        constructor: function (id) {
            this.__hpcc_id = id;

            this._watched = [];
            this.children = Observable(new Memory({
                idProperty: "__hpcc_id",
                parent: this,
                data: []
            }));
        },

        pause: function () {
            return WsSMC.PauseQueue({
                request: {
                    QueueName: this.QueueName
                }
            });
        },

        resume: function () {
            return WsSMC.ResumeQueue({
                request: {
                    QueueName: this.QueueName
                }
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

        addChild: function (wu) {
            wu.set("ESPQueue", this);
            this.children.put(wu, {
                overwrite: true
            });
            if (!this._watched[wu.__hpcc_id]) {
                var context = this;
                this._watched[wu.__hpcc_id] = wu.watch("changedCount", function (name, oldValue, newValue) {
                    if (oldValue !== newValue) {
                        context.children.notify(wu, wu.__hpcc_id);
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

        pause: function () {
            var context = this;
            return this.inherited(arguments).then(function (response) {
                context.updateData({
                    ClusterStatus: 2
                });
                return response;
            });
        },

        resume: function () {
            var context = this;
            return this.inherited(arguments).then(function (response) {
                context.updateData({
                    ClusterStatus: 0
                });
                return response;
            });
        },

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
            return dojoConfig.getImageURL(this.getStateImageName());
        }
    });

    var ServerJobQueue = declare([Queue], {
        getDisplayName: function () {
            return this.QueueName;
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

        pause: function () {
            var context = this;
            return this.inherited(arguments).then(function (response) {
                context.updateData({
                    QueueStatus: "paused"
                });
                return response;
            });
        },

        resume: function () {
            var context = this;
            return this.inherited(arguments).then(function (response) {
                context.updateData({
                    QueueStatus: null
                });
                return response;
            });
        },

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
            return dojoConfig.getImageURL(this.getStateImageName());
        }

    });

    var globalQueueStore = null;
    GetGlobalQueueStore = function () {
        if (!globalQueueStore) {
            globalQueueStore = new Store();
        }
        return globalQueueStore;
    }
    
    return {
        isInstanceOfQueue: function (obj) {
            return obj.isInstanceOf(Queue);
        },

        GetTargetCluster: function (name) {
            var store = GetGlobalQueueStore();
            var id = "TargetCluster::" + name;
            var retVal = store.get(id);
            if (!retVal) {
                retVal = new TargetCluster(id);
                store.put(retVal);
            }
            return retVal;
        },

        GetServerJobQueue: function (name) {
            var store = GetGlobalQueueStore();
            var id = "ServerJobQueue::" + name;
            var retVal = store.get(id);
            if (!retVal) {
                retVal = new ServerJobQueue(id);
                store.put(retVal);
            }
            return retVal;
        }
    };
});
