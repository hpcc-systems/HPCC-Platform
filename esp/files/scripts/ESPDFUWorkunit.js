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
    "dojo/_base/Deferred",
    "dojo/data/ObjectStore",
    "dojo/store/util/QueryResults",
    "dojo/store/Observable",

    "hpcc/FileSpray",
    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/ESPResult"
], function (declare, arrayUtil, lang, Deferred, ObjectStore, QueryResults, Observable,
    FileSpray, ESPUtil, ESPRequest, ESPResult) {

    var Store = declare([ESPRequest.Store], {
        service: "FileSpray",
        action: "GetDFUWorkunits",
        responseQualifier: "results.DFUWorkunit",
        responseTotalQualifier: "NumWUs",
        idProperty: "ID",
        startProperty: "PageStartFrom",
        countProperty: "PageSize",

        _watched: [],
        preRequest: function (request) {
            switch (request.Sortby) {
                case "ClusterName":
                    request.Sortby = "Cluster";
                    break;
                case "JobName":
                    request.Sortby = "Jobname";
                    break;
                case "Command":
                    request.Sortby = "Type";
                    break;
                case "StateMessage":
                    request.Sortby = "State";
                    break;
            }
        },
        create: function (id) {
            return new Workunit({
                ID: id,
                Wuid: id
            });
        },
        update: function (id, item) {
            var storeItem = this.get(id);
            storeItem.updateData(item);
            if (!this._watched[id]) {
                var context = this;
                this._watched[id] = storeItem.watch("changedCount", function (name, oldValue, newValue) {
                    if (oldValue !== newValue) {
                        context.notify(storeItem, id);
                    }
                });
            }
        }
    });

    var Workunit = declare([ESPUtil.Singleton, ESPUtil.Monitor], {
        //  Asserts  ---
        _assertHasWuid: function () {
            if (!this.Wuid) {
                throw new Error("Wuid cannot be empty.");
            }
        },
        //  Attributes  ---
        Wuid: "",

        text: "",

        resultCount: 0,
        results: [],

        graphs: [],

        exceptions: [],
        timers: [],

        _StateSetter: function (state) {
            this.State = state;
            this.set("hasCompleted", FileSpray.isComplete(this.State));
        },

        _CommandSetter: function (command) {
            this.Command = command;
            if (command in FileSpray.CommandMessages) {
                this.set("CommandMessage", FileSpray.CommandMessages[command]);
            } else {
                this.set("CommandMessage", "Unknown (" + command + ")");
            }
        },

        _SourceFormatSetter: function (format) {
            this.SourceFormat = format;
            if (format in FileSpray.FormatMessages) {
                this.set("SourceFormatMessage", FileSpray.FormatMessages[format]);
            } else {
                this.set("SourceFormatMessage", "Unknown (" + format + ")");
            }
        },

        _DestFormatSetter: function (format) {
            this.DestFormat = format;
            if (format in FileSpray.FormatMessages) {
                this.set("DestFormatMessage", FileSpray.FormatMessages[format]);
            } else {
                this.set("DestFormatMessage", "Unknown (" + format + ")");
            }
        },

        onCreate: function () {
        },
        onUpdate: function () {
        },
        onSubmit: function () {
        },
        constructor: function (args) {
            this.inherited(arguments);
            declare.safeMixin(this, args);
            this.wu = this;
        },
        isComplete: function () {
            return this.hasCompleted;
        },
        monitor: function (callback) {
            if (callback) {
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
        create: function (ecl) {
        },
        update: function (request) {
            this._assertHasWuid();
            lang.mixin(request, {
                ID: this.Wuid
            });

            var outerRequest = {
                "wu.ID": request.ID,
                "wu.isProtected": request.isProtected,
                "wu.JobName": request.JobName,
                isProtectedOrig: this.isProtected,
                JobNameOrig: this.JobName
            };

            var context = this;
            FileSpray.UpdateDFUWorkunit({
                request: outerRequest,
                load: function (response) {
                    context.refresh();
                }
            });
        },
        submit: function (target) {
        },
        fetchXML: function (onFetchXML) {
            FileSpray.DFUWUFile({
                request: {
                    Wuid: this.Wuid
                },
                load: function (response) {
                    onFetchXML(response);
                }
            });
        },
        _resubmit: function (clone, resetWorkflow, callback) {
        },
        resubmit: function (callback) {
        },
        _action: function (action, callback) {
        },
        abort: function (callback) {
        },
        doDelete: function (callback) {
        },
        refresh: function (full) {
            this.getInfo({
                onAfterSend: function () {
                }
            });
        },
        getInfo: function (args) {
            this._assertHasWuid();
            var context = this;
            FileSpray.GetDFUWorkunit({
                request: {
                    wuid: this.Wuid
                },
                load: function (response) {
                    if (lang.exists("GetDFUWorkunitResponse.result", response)) {
                        context.updateData(response.GetDFUWorkunitResponse.result);

                        if (args.onAfterSend) {
                            args.onAfterSend(context);
                        }
                    }
                }
            });
        },
        getState: function () {
            return this.State;
        },
        getProtectedImage: function () {
            if (this.isProtected) {
                return "img/locked.png"
            }
            return "img/unlocked.png"
        },
        getStateIconClass: function () {
            switch (this.StateID) {
                case 1:
                    return "iconWarning";
                case 2:
                    return "iconSubmitted";
                case 3:
                    return "iconRunning";
                case 4:
                    return "iconFailed";
                case 5:
                    return "iconFailed";
                case 6:
                    return "iconCompleted";
                case 7:
                    return "iconRunning";
                case 8:
                    return "iconAborting";
                case 999:
                    return "iconDeleted";
            }
            return "iconWorkunit";
        },
        getStateImage: function () {
            switch (this.State) {
                case 1:
                    return "img/workunit_warning.png";
                case 2:
                    return "img/workunit_submitted.png";
                case 3:
                    return "img/workunit_running.png";
                case 4:
                    return "img/workunit_failed.png";
                case 5:
                    return "img/workunit_failed.png";
                case 6:
                    return "img/workunit_completed.png";
                case 7:
                    return "img/workunit_running.png";
                case 8:
                    return "img/workunit_aborting.png";
            }
            return "img/workunit.png";
        }
    });

    return {
        Get: function (wuid) {
            var store = new Store();
            return store.get(wuid);
        },

        CreateWUQueryStore: function (options) {
            var store = new Store(options);
            store = Observable(store);
            return store;
        },

        CreateWUQueryObjectStore: function (options) {
            var objStore = new ObjectStore({
                objectStore: this.CreateWUQueryStore()
            });
            return objStore;
        }
    };
});
