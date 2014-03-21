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
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/_base/Deferred",
    "dojo/store/Observable",
    "dojo/topic",

    "hpcc/FileSpray",
    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/ESPResult"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, Deferred, Observable, topic,
    FileSpray, ESPUtil, ESPRequest, ESPResult) {

    var i18n = nlsHPCC;

    var Store = declare([ESPRequest.Store], {
        service: "FileSpray",
        action: "GetDFUWorkunits",
        responseQualifier: "GetDFUWorkunitsResponse.results.DFUWorkunit",
        responseTotalQualifier: "GetDFUWorkunitsResponse.NumWUs",
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
                throw new Error(i18n.Wuidcannotbeempty);
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

        _hasCompletedSetter: function(completed) {
            var justCompleted = !this.hasCompleted && completed;
            this.hasCompleted = completed;
            if (justCompleted) {
                topic.publish("hpcc/dfu_wu_completed", this);
            }
        },

        _CommandSetter: function (command) {
            this.Command = command;
            if (command in FileSpray.CommandMessages) {
                this.set("CommandMessage", FileSpray.CommandMessages[command]);
            } else {
                this.set("CommandMessage", i18n.Unknown + " (" + command + ")");
            }
        },

        _SourceFormatSetter: function (format) {
            this.SourceFormat = format;
            if (format in FileSpray.FormatMessages) {
                this.set("SourceFormatMessage", FileSpray.FormatMessages[format]);
            } else {
                this.set("SourceFormatMessage", i18n.Unknown + " (" + format + ")");
            }
        },

        _DestFormatSetter: function (format) {
            this.DestFormat = format;
            if (format in FileSpray.FormatMessages) {
                this.set("DestFormatMessage", FileSpray.FormatMessages[format]);
            } else {
                this.set("DestFormatMessage", i18n.Unknown + " (" + format + ")");
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
            if (args) {
                declare.safeMixin(this, args);
            }
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
                request: outerRequest
            }).then(function (response) {
                context.refresh();
            });
        },
        submit: function (target) {
        },
        fetchXML: function (onFetchXML) {
            FileSpray.DFUWUFile({
                request: {
                    Wuid: this.Wuid
                }
            }).then(function (response) {
                onFetchXML(response);
            });
        },
        _resubmit: function (clone, resetWorkflow, callback) {
        },
        resubmit: function (callback) {
        },
        _action: function (action) {
            var context = this;
            return FileSpray.DFUWorkunitsAction([this], action, {
            }).then(function (response) {
                context.refresh();
            });
        },
        abort: function () {
            return FileSpray.AbortDFUWorkunit({
                request: {
                    wuid: this.Wuid
                }
            });
        },
        doDelete: function (callback) {
            return this._action("Delete");
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
                }
            }).then(function (response) {
                if (lang.exists("GetDFUWorkunitResponse.result", response)) {
                    context.updateData(response.GetDFUWorkunitResponse.result);

                    if (args.onAfterSend) {
                        args.onAfterSend(context);
                    }
                }
            });
        },
        getState: function () {
            return this.State;
        },
        getProtectedImage: function () {
            if (this.isProtected) {
                return "/esp/files/img/locked.png"
            }
            return "/esp/files/img/unlocked.png"
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
                    return "/esp/files/img/workunit_warning.png";
                case 2:
                    return "/esp/files/img/workunit_submitted.png";
                case 3:
                    return "/esp/files/img/workunit_running.png";
                case 4:
                    return "/esp/files/img/workunit_failed.png";
                case 5:
                    return "/esp/files/img/workunit_failed.png";
                case 6:
                    return "/esp/files/img/workunit_completed.png";
                case 7:
                    return "/esp/files/img/workunit_running.png";
                case 8:
                    return "/esp/files/img/workunit_aborting.png";
                case 999:
                    return "/esp/files/img/workunit_deleted.png";                    
            }
            return "/esp/files/img/workunit.png";
        }
    });

    return {
        isInstanceOfWorkunit: function (obj) {
            return obj.isInstanceOf(Workunit);
        },

        Get: function (wuid) {
            var store = new Store();
            return store.get(wuid);
        },

        CreateWUQueryStore: function (options) {
            var store = new Store(options);
            store = Observable(store);
            return store;
        }
    };
});
