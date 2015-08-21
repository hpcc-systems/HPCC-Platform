/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/Deferred",
    "dojo/promise/all",
    "dojo/store/Observable",
    "dojo/topic",

    "hpcc/WsWorkunits",
    "hpcc/WsTopology",
    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/ESPResult"
], function (declare, arrayUtil, lang, i18n, nlsHPCC, Deferred, all, Observable, topic,
    WsWorkunits, WsTopology, ESPUtil, ESPRequest, ESPResult) {

    var _workunits = {};

    var Store = declare([ESPRequest.Store], {
        service: "WsWorkunits",
        action: "WUQuery",
        responseQualifier: "WUQueryResponse.Workunits.ECLWorkunit",
        responseTotalQualifier: "WUQueryResponse.NumWUs",
        idProperty: "Wuid",
        startProperty: "PageStartFrom",
        countProperty: "Count",

        _watched: [],
        preRequest: function (request) {
            if (request.Sortby && request.Sortby === "TotalClusterTime") {
                request.Sortby = "ClusterTime";
            }
            this.busy = true;
        },
        preProcessFullResponse: function (response, request, query, options) {
            this.busy = false;
        },
        create: function (id) {
            return new Workunit({
                Wuid: id
            });
        },
        update: function (id, item) {
            var storeItem = this.get(id);
            storeItem.updateData(item);
            if (!this._watched[id]) {
                var context = this;
                this._watched[id] = storeItem.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                    if (oldValue !== newValue) {
                        context.notify(storeItem, id);
                    }
                });
            }
        }
    });

    var Workunit = declare([ESPUtil.Singleton, ESPUtil.Monitor], {
        i18n: nlsHPCC,

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
        _ActionExSetter: function (ActionEx) {
            if (this.StateID) {
                this.ActionEx = ActionEx;
                this.set("hasCompleted", WsWorkunits.isComplete(this.StateID, this.ActionEx));
            }
        },
        _hasCompletedSetter: function (completed) {
            var justCompleted = !this.hasCompleted && completed;
            this.hasCompleted = completed;
            if (justCompleted) {
                topic.publish("hpcc/ecl_wu_completed", this);
            }
        },
        _VariablesSetter: function (Variables) {
            this.set("variables", Variables.ECLResult);
        },
        _ResultsSetter: function (Results) {
            var results = [];
            var sequenceResults = [];
            var namedResults = {};
            for (var i = 0; i < Results.ECLResult.length; ++i) {
                var espResult = ESPResult.Get(lang.mixin({
                    wu: this.wu,
                    Wuid: this.Wuid,
                    ResultViews: lang.exists("ResultViews.View", Results) ? Results.ResultViews.View : []
                }, Results.ECLResult[i]));
                results.push(espResult);
                sequenceResults[Results.ECLResult[i].Sequence] = espResult;
                if (Results.ECLResult[i].Name) {
                    namedResults[Results.ECLResult[i].Name] = espResult;
                }
            }
            this.set("results", results);
            this.set("sequenceResults", sequenceResults);
            this.set("namedResults", namedResults);
        },
        _SourceFilesSetter: function (SourceFiles) {
            var sourceFiles = [];
            for (var i = 0; i < SourceFiles.ECLSourceFile.length; ++i) {
                sourceFiles.push(ESPResult.Get(lang.mixin({ wu: this.wu, Wuid: this.Wuid, __hpcc_parentName: "" }, SourceFiles.ECLSourceFile[i])));
                if (lang.exists("ECLSourceFiles.ECLSourceFile", SourceFiles.ECLSourceFile[i])) {
                    for (var j = 0; j < SourceFiles.ECLSourceFile[i].ECLSourceFiles.ECLSourceFile.length; ++j) {
                        sourceFiles.push(ESPResult.Get(lang.mixin({ wu: this.wu, Wuid: this.Wuid, __hpcc_parentName: SourceFiles.ECLSourceFile[i].Name }, SourceFiles.ECLSourceFile[i].ECLSourceFiles.ECLSourceFile[j])));
                    }
                }
            }
            this.set("sourceFiles", sourceFiles);
        },
        ExtractTime: function (Timer) {
            //  GH:  <n>ns or <m>ms or <s>s or [<d> days ][<h>:][<m>:]<s>[.<ms>]
            var nsIndex = Timer.indexOf("ns");
            if (nsIndex !== -1) {
                return parseFloat(Timer.substr(0, nsIndex)) / 1000000000;
            }
            var msIndex = Timer.indexOf("ms");
            if (msIndex !== -1) {
                return parseFloat(Timer.substr(0, msIndex)) / 1000;
            }
            var sIndex = Timer.indexOf("s");
            if (sIndex !== -1 && Timer.indexOf("days") === -1) {
                return parseFloat(Timer.substr(0, sIndex));
            }

            var dayTimeParts = Timer.split(" days ");
            var days = parseFloat(dayTimeParts.length > 1 ? dayTimeParts[0] : 0.0);
            var time = dayTimeParts.length > 1 ? dayTimeParts[1] : dayTimeParts[0];
            var secs = 0.0;
            var timeParts = time.split(":").reverse();
            for (var j = 0; j < timeParts.length; ++j) {
                secs += parseFloat(timeParts[j]) * Math.pow(60, j);
            }
            return (days * 24 * 60 * 60) + secs;
        },
        ExtractTimeTests: function () {
            var tests = [
                { str: "1.1s", expected: 1.1 },
                { str: "2.2ms", expected: 0.0022 },
                { str: "3.3ns", expected: 0.0000000033 },
                { str: "4.4", expected: 4.4 },
                { str: "5:55.5", expected: 355.5 },
                { str: "6:06:06.6", expected: 21966.6 },
                { str: "6:06:6.6", expected: 21966.6 },
                { str: "6:6:6.6", expected: 21966.6 },
                { str: "7 days 7:07:7.7", expected: 630427.7 }
            ];
            tests.forEach(function (test, idx) {
                if (this.ExtractTime(test.str) !== test.expected) {
                    console.log("ExtractTimeTests failed with " + this.ExtractTime(test.str) + " !== " +  test.expected);
                }
            }, this);
        },
        _TimersSetter: function (Timers) {
            var timers = [];
            for (var i = 0; i < Timers.ECLTimer.length; ++i) {
                var secs = this.ExtractTime(Timers.ECLTimer[i].Value);
                timers.push(lang.mixin(Timers.ECLTimer[i], {
                    __hpcc_id: i + 1,
                    Seconds: Math.round(secs * 1000) / 1000,
                    HasSubGraphId: Timers.ECLTimer[i].SubGraphId && Timers.ECLTimer[i].SubGraphId != "" ? true : false
                }));
            }
            this.set("timers", timers);
        },
        _ResourceURLCountSetter: function (ResourceURLCount) {
            //  All WU's have 1 resource URL, which we are not interested in  ---
            this.set("resourceURLCount", ResourceURLCount - 1);
        },
        _ResourceURLsSetter: function (resourceURLs) {
            var data = [];
            arrayUtil.forEach(resourceURLs.URL, function (url, idx) {
                var cleanedURL = url.split("\\").join("/");
                var urlParts = cleanedURL.split("/");
                var matchStr = "res/" + this.wu.Wuid + "/";
                if (cleanedURL.indexOf(matchStr) === 0) {
                    var displayPath = cleanedURL.substr(matchStr.length);
                    var displayName = urlParts[urlParts.length - 1];
                    var row = {
                        __hpcc_id: idx,
                        DisplayName: displayName,
                        DisplayPath: displayPath,
                        URL: cleanedURL
                    };
                    data.push(row);
                }
            }, this);
            this.set("resourceURLs", data);
            this.set("resourceURLCount", data.length);
        },
        _GraphsSetter: function (Graphs) {
            this.set("graphs", Graphs.ECLGraph);
        },

        //  Calculated "Helpers"  ---
        _HelpersSetter: function (Helpers) {
            this.set("helpers", Helpers.ECLHelpFile);
            this.refreshHelpersCount();
        },
        _ThorLogListSetter: function (ThorLogList) {
            this.set("thorLogInfo", ThorLogList.ThorLogInfo);
            this.refreshHelpersCount();
        },
        _HasArchiveQuerySetter: function (HasArchiveQuery) {
            this.set("hasArchiveQuery", HasArchiveQuery);
            this.refreshHelpersCount();
        },
        refreshHelpersCount: function () {
            var helpersCount = 2;   //  ECL + Workunit XML are also helpers...
            if (this.helpers) {
                helpersCount += this.helpers.length;
            }
            if (this.thorLogList) {
                helpersCount += this.thorLogList.length;
            }
            if (this.hasArchiveQuery) {
                helpersCount += 1;
            }
            this.set("helpersCount", helpersCount);
        },

        //  ---  ---  ---
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
        isFailed: function () {
            return this.StateID === 4;
        },
        isDeleted: function () {
            return this.StateID === 999;
        },
        monitor: function (callback) {
            if (callback) {
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
        doDeschedule: function () {
            return this._action("Deschedule").then(function(response) {
            });
        },
        doReschedule: function () {
            return this._action("Reschedule").then(function(response) {
            });
        },
        create: function (ecl) {
            var context = this;
            WsWorkunits.WUCreate({
                load: function (response) {
                    _workunits[response.WUCreateResponse.Workunit.Wuid] = context;
                    context.Wuid = response.WUCreateResponse.Workunit.Wuid;
                    context.startMonitor(true);
                    context.updateData(response.WUCreateResponse.Workunit);
                    context.onCreate();
                }
            });
        },
        update: function (request, appData) {
            this._assertHasWuid();
            lang.mixin(request, {
                Wuid: this.Wuid
            });
            lang.mixin(request, {
                StateOrig: this.State,
                JobnameOrig: this.Jobname,
                DescriptionOrig: this.Description,
                ProtectedOrig: this.Protected,
                ScopeOrig: this.Scope,
                ClusterOrig: this.Cluster,
                ApplicationValues: appData
            });

            var context = this;
            WsWorkunits.WUUpdate({
                request: request,
                load: function (response) {
                    if (lang.exists("Exceptions.Exception", response)) {
                        dojo.publish("hpcc/brToaster", {
                            message: "<h4>" + response.Exceptions.Source + "</h4>" + "<p>" + response.Exceptions.Exception[0].Message + "</p>",
                            type: "error",
                            duration: -1
                        });
                    } else {
                        context.updateData(response.WUUpdateResponse.Workunit);
                    }
                    context.onUpdate();
                }
            });
        },
        submit: function (target) {
            this._assertHasWuid();
            var context = this;
            var deferred = new Deferred()
            deferred.promise.then(function (target) {
                WsWorkunits.WUSubmit({
                    request: {
                        Wuid: context.Wuid,
                        Cluster: target
                    },
                    load: function (response) {
                        context.onSubmit();
                    }
                });
            });

            if (target) {
                deferred.resolve(target);
            } else {
                WsTopology.TpLogicalClusterQuery().then(function (response) {
                    if (lang.exists("TpLogicalClusterQueryResponse.default", response)) {
                        deferred.resolve(response.TpLogicalClusterQueryResponse["default"].Name);
                    }
                });
            }
        },
        _resubmit: function (clone, resetWorkflow) {
            this._assertHasWuid();
            var context = this;
            return WsWorkunits.WUResubmit({
                request: {
                    Wuids: this.Wuid,
                    CloneWorkunit: clone,
                    ResetWorkflow: resetWorkflow
                }
            }).then(function (response) {
                context.refresh();
                return response;
            });
        },
        clone: function () {
            var context = this;
            this._resubmit(true, false).then(function (response) {
                if (!lang.exists("Exceptions.Source", response)) {
                    var msg = "";
                    if (lang.exists("WUResubmitResponse.WUs.WU", response) && response.WUResubmitResponse.WUs.WU.length) {
                        msg = context.i18n.ClonedWUID + ":  " + response.WUResubmitResponse.WUs.WU[0].WUID;
                        topic.publish("hpcc/ecl_wu_created", {
                            wuid: response.WUResubmitResponse.WUs.WU[0].WUID
                        });
                    }
                    dojo.publish("hpcc/brToaster", {
                        Severity: "Message",
                        Source: "ESPWorkunit.clone",
                        Exceptions: [{ Source: context.Wuid, Message: msg }]
                    });
                }
                return response;
            });
        },
        resubmit: function () {
            var context = this;
            this._resubmit(false, true).then(function (response) {
                if (!lang.exists("Exceptions.Source", response)) {
                    dojo.publish("hpcc/brToaster", {
                        Severity: "Message",
                        Source: "ESPWorkunit.resubmit",
                        Exceptions: [{ Source: context.Wuid, Message: context.i18n.Resubmitted }]
                    });
                    context.hasCompleted = false;
                    context.startMonitor(true);
                }
                return response;
            });
        },
        recover: function () {
            var context = this;
            this._resubmit(false, false).then(function (response) {
                if (!lang.exists("Exceptions.Source", response)) {
                    dojo.publish("hpcc/brToaster", {
                        Severity: "Message",
                        Source: "ESPWorkunit.recover",
                        Exceptions: [{ Source: context.Wuid, Message: context.i18n.Restarted }]
                    });
                    context.hasCompleted = false;
                    context.startMonitor(true);
                }
                return response;
            });
        },
        _action: function (action) {
            this._assertHasWuid();
            var context = this;
            return WsWorkunits.WUAction([{ Wuid: this.Wuid }], action, {
                load: function (response) {
                    context.refresh();
                }
            });
        },
        setToFailed: function () {
            return this._action("setToFailed");
        },
        pause: function () {
            return this._action("Pause");
        },
        pauseNow: function () {
            return this._action("PauseNow");
        },
        resume: function () {
            return this._action("Resume");
        },
        abort: function () {
            return this._action("Abort");
        },
        doDelete: function () {
            return this._action("Delete").then(function(response) {
            });
        },

        restore: function () {
            return this._action("Restore");
        },

        publish: function (jobName, remoteDali, sourceProcess, priority, comment, allowForeign) {
            this._assertHasWuid();
            var context = this;
            WsWorkunits.WUPublishWorkunit({
                request: {
                    Wuid: this.Wuid,
                    JobName: jobName,
                    RemoteDali: remoteDali,
                    SourceProcess: sourceProcess,
                    Priority: priority,
                    Comment: comment,
                    AllowForeignFiles: allowForeign,
                    Activate: 1,
                    UpdateWorkUnitName: 1,
                    Wait: 5000
                },
                load: function (response) {
                    context.updateData(response.WUPublishWorkunitResponse);
                }
            });
        },
        refresh: function (full) {
            if (full || this.Archived || this.__hpcc_changedCount === 0) {
                return this.getInfo({
                    onGetText: function () {
                    },
                    onGetWUExceptions: function () {
                    }
                });
            } else {
                return this.getQuery();
            }
        },
        getQuery: function () {
            this._assertHasWuid();
            var context = this;
            return WsWorkunits.WUQuery({
                request: {
                    Wuid: this.Wuid
                }
            }).then(function (response) {
                if (lang.exists("WUQueryResponse.Workunits.ECLWorkunit", response)) {
                    arrayUtil.forEach(response.WUQueryResponse.Workunits.ECLWorkunit, function (item, index) {
                        context.updateData(item);
                    });
                }
                return response;
            });
        },
        getInfo: function (args) {
            this._assertHasWuid();
            var context = this;
            return WsWorkunits.WUInfo({
                request: {
                    Wuid: this.Wuid,
                    TruncateEclTo64k: args.onGetText ? false : true,
                    IncludeExceptions: args.onGetWUExceptions ? true : false,
                    IncludeGraphs: args.onGetGraphs ? true : false,
                    IncludeSourceFiles: args.onGetSourceFiles ? true : false,
                    IncludeResults: (args.onGetResults || args.onGetSequenceResults) ? true : false,
                    IncludeResultsViewNames: (args.onGetResults || args.onGetSequenceResults) ? true : false,
                    IncludeVariables: args.onGetVariables ? true : false,
                    IncludeTimers: args.onGetTimers ? true : false,
                    IncludeResourceURLs: args.onGetResourceURLs ? true : false,
                    IncludeDebugValues: args.onGetDebugValues ? true : false,
                    IncludeApplicationValues: args.onGetApplicationValues ? true : false,
                    IncludeWorkflows: args.onGetWorkflows ? true : false,
                    IncludeXmlSchemas: false,
                    SuppressResultSchemas: true
                }
            }).then(function(response) {
                if (lang.exists("WUInfoResponse.Workunit", response)) {
                    if (!args.onGetText && lang.exists("WUInfoResponse.Workunit.Query", response)) {
                        //  A truncated version of ECL just causes issues  ---
                        delete response.WUInfoResponse.Workunit.Query;
                    }
                    if (lang.exists("WUInfoResponse.ResultViews", response) && lang.exists("WUInfoResponse.Workunit.Results", response)) {
                        lang.mixin(response.WUInfoResponse.Workunit.Results, {
                            ResultViews: response.WUInfoResponse.ResultViews
                        });
                    }
                    if (args.onGetWUExceptions && !lang.exists("WUInfoResponse.Workunit.Exceptions.ECLException", response)) {
                        lang.mixin(response.WUInfoResponse.Workunit, {
                            Exceptions: {
                                ECLException: []
                            }
                        })
                    }
                    context.updateData(response.WUInfoResponse.Workunit);

                    if (args.onGetText) {
                        args.onGetText(lang.exists("Query.Text", context) ? context.Query.Text : "");
                    }
                    if (args.onGetWUExceptions) {
                        args.onGetWUExceptions(lang.exists("Exceptions.ECLException", context) ? context.Exceptions.ECLException : []);
                    }
                    if (args.onGetApplicationValues) {
                        args.onGetApplicationValues(lang.exists("ApplicationValues.ApplicationValue", context) ? context.ApplicationValues.ApplicationValue : []);
                    }
                    if (args.onGetDebugValues) {
                        args.onGetDebugValues(lang.exists("DebugValues.DebugValue", context) ? context.DebugValues.DebugValue : []);
                    }
                    if (args.onGetVariables) {
                        args.onGetVariables(lang.exists("variables", context) ? context.variables: []);
                    }
                    if (args.onGetResults) {
                        args.onGetResults(lang.exists("results", context) ? context.results : []);
                    }
                    if (args.onGetSequenceResults) {
                        args.onGetSequenceResults(lang.exists("sequenceResults", context) ? context.sequenceResults : []);
                    }
                    if (args.onGetSourceFiles) {
                        args.onGetSourceFiles(lang.exists("sourceFiles", context) ? context.sourceFiles : []);
                    }
                    if (args.onGetTimers) {
                        args.onGetTimers(lang.exists("timers", context) ? context.timers : []);
                    }
                    if (args.onGetResourceURLs && lang.exists("resourceURLs", context)) {
                        args.onGetResourceURLs(context.resourceURLs);
                    }
                    if (args.onGetGraphs && lang.exists("graphs", context)) {
                        if (context.timers || lang.exists("ApplicationValues.ApplicationValue", context)) {
                            for (var i = 0; i < context.graphs.length; ++i) {
                                if (context.timers) {
                                    context.graphs[i].Time = 0;
                                    for (var j = 0; j < context.timers.length; ++j) {
                                        if (context.timers[j].GraphName === context.graphs[i].Name && !context.timers[j].HasSubGraphId) {
                                            context.graphs[i].Time = context.timers[j].Seconds;
                                            break;
                                        }
                                    }
                                    context.graphs[i].Time = Math.round(context.graphs[i].Time * 1000) / 1000;
                                }
                                if (lang.exists("ApplicationValues.ApplicationValue", context)) {
                                    var idx = context.getApplicationValueIndex("ESPWorkunit.js", context.graphs[i].Name + "_SVG");
                                    if (idx >= 0) {
                                        context.graphs[i].svg = context.ApplicationValues.ApplicationValue[idx].Value;
                                    }
                                }
                            }
                        }
                        args.onGetGraphs(context.graphs);
                    } else if (args.onGetGraphs) {
                        args.onGetGraphs([]);
                    }
                    if (args.onGetWorkflows && lang.exists("Workflows.ECLWorkflow", context)) {
                        args.onGetWorkflows(context.Workflows.ECLWorkflow);
                    }
                    if (args.onAfterSend) {
                        args.onAfterSend(context);
                    }
                }
                return response;
            });
        },
        getGraphIndex: function (name) {
            for (var i = 0; i < this.graphs.length; ++i) {
                if (this.graphs[i].Name == name) {
                    return i;
                }
            }
            return -1;
        },
        getGraphTimers: function (name) {
            var retVal = [];
            arrayUtil.forEach(this.timers, function (timer, idx) {
                if (timer.HasSubGraphId && timer.GraphName === name) {
                    retVal.push(timer);
                }
            }, this);
            return retVal;
        },
        getApplicationValueIndex: function (application, name) {
            if (lang.exists("ApplicationValues.ApplicationValue", this)) {
                for (var i = 0; i < this.ApplicationValues.ApplicationValue.length; ++i) {
                    if (this.ApplicationValues.ApplicationValue[i].Application == application && this.ApplicationValues.ApplicationValue[i].Name == name) {
                        return i;
                    }
                }
            }
            return -1;
        },
        getState: function () {
            return this.State;
        },
        getStateIconClass: function () {
            if (this.Archived) {
                return "iconArchived";
            }
            switch (this.StateID) {
                case 1:
                case 3:
                    return "iconCompleted";
                case 2:
                case 11:
                case 15:
                    return "iconRunning";
                case 4:
                case 7:
                    return "iconFailed";
                case 5:
                case 8:
                case 10:
                case 12:
                case 13:
                case 14:
                case 16:
                    return "iconArchived";
                case 6:
                    return "iconAborting";
                case 9:
                    return "iconSubmitted";
                case 999:
                    return "iconDeleted";
            }
            return "iconWorkunit";
        },
        getStateImageName: function () {
            if (this.Archived) {
                return "workunit_archived.png";
            }
            switch (this.StateID) {
                case 1:
                    return "workunit_completed.png";
                case 2:
                    return "workunit_running.png";
                case 3:
                    return "workunit_completed.png";
                case 4:
                    return "workunit_failed.png";
                case 5:
                    return "workunit_warning.png";
                case 6:
                    return "workunit_aborting.png";
                case 7:
                    return "workunit_failed.png";
                case 8:
                    return "workunit_warning.png";
                case 9:
                    return "workunit_submitted.png";
                case 10:
                    return "workunit_warning.png";
                case 11:
                    return "workunit_running.png";
                case 12:
                    return "workunit_warning.png";
                case 13:
                    return "workunit_warning.png";
                case 14:
                    return "workunit_warning.png";
                case 15:
                    return "workunit_running.png";
                case 16:
                    return "workunit_warning.png";
                case 999:
                    return "workunit_deleted.png";
            }
            return "workunit.png";
        },
        getStateImage: function () {
            return dojoConfig.getImageURL(this.getStateImageName());
        },
        getStateImageHTML: function () {
            return dojoConfig.getImageHTML(this.getStateImageName());
        },
        getProtectedImageName: function () {
            if (this.Protected) {
                return "locked.png";
            }
            return "unlocked.png";
        },
        getProtectedImage: function () {
            return dojoConfig.getImageURL(this.getProtectedImageName());
        },
        getProtectedHTML: function () {
            return dojoConfig.getImageHTML(this.getProtectedImageName());
        },
        fetchText: function (onFetchText) {
            var context = this;
            if (lang.exists("Query.Text", context)) {
                onFetchText(this.Query.Text);
                return;
            }

            this.getInfo({
                onGetText: onFetchText
            });
        },
        fetchXML: function (onFetchXML) {
            if (this.xml) {
                onFetchXML(this.xml);
                return;
            }

            this._assertHasWuid();
            var context = this;
            WsWorkunits.WUFile({
                request: {
                    Wuid: this.Wuid,
                    Type: "XML"
                },
                load: function (response) {
                    context.xml = response;
                    onFetchXML(response);
                }
            });
        },
        fetchResults: function (onFetchResults) {
            if (this.results && this.results.length) {
                onFetchResults(this.results);
                return;
            }

            this.getInfo({
                onGetResults: onFetchResults
            });
        },
        fetchNamedResults: function (resultNames, row, count) {
            var deferred = new Deferred();
            var context = this;
            this.fetchResults(function (results) {
                var resultContents = [];
                arrayUtil.forEach(resultNames, function (item, idx) {
                    resultContents.push(context.namedResults[item].fetchContent(row, count));
                });
                all(resultContents).then(function (resultContents) {
                    var results = [];
                    arrayUtil.forEach(resultContents, function (item, idx) {
                        results[resultNames[idx]] = item;
                    });
                    deferred.resolve(results);
                });
            });
            return deferred.promise;
        },
        fetchAllNamedResults: function (row, count) {
            var deferred = new Deferred();
            var context = this;
            this.fetchResults(function (results) {
                var resultNames = [];
                arrayUtil.forEach(results, function (item, idx) {
                    resultNames.push(item.Name);
                });
                context.fetchNamedResults(resultNames, row, count).then(function(response) {
                    deferred.resolve(response);
                });
            });
            return deferred.promise;
        },
        fetchSequenceResults: function (onFetchSequenceResults) {
            if (this.sequenceResults && this.sequenceResults.length) {
                onFetchSequenceResults(this.sequenceResults);
                return;
            }

            this.getInfo({
                onGetSequenceResults: onFetchSequenceResults
            });
        },
        fetchSourceFiles: function (onFetchSourceFiles) {
            if (this.sourceFiles && this.sourceFiles.length) {
                onFetchSourceFiles(this.sourceFiles);
                return;
            }

            this.getInfo({
                onGetSourceFiles: onFetchSourceFiles
            });
        },
        fetchTimers: function (onFetchTimers) {
            if (this.timers && this.timers.length) {
                onFetchTimers(this.timers);
                return;
            }

            this.getInfo({
                onGetTimers: onFetchTimers
            });
        },
        fetchGraphs: function (onFetchGraphs) {
            if (this.graphs && this.graphs.length) {
                onFetchGraphs(this.graphs);
                return;
            }

            this.getInfo({
                onGetGraphs: onFetchGraphs
            });
        },
        fetchGraphXgmmlByName: function (name, onFetchGraphXgmml, force) {
            var idx = this.getGraphIndex(name);
            if (idx >= 0) {
                this.fetchGraphXgmml(idx, onFetchGraphXgmml, force);
            }
        },
        fetchGraphXgmml: function (idx, onFetchGraphXgmml, force) {
            if (!force && this.graphs && this.graphs[idx] && this.graphs[idx].xgmml) {
                onFetchGraphXgmml(this.graphs[idx].xgmml, this.graphs[idx].svg);
                return;
            }

            this._assertHasWuid();
            var context = this;
            WsWorkunits.WUGetGraph({
                request: {
                    Wuid: this.Wuid,
                    GraphName: this.graphs[idx].Name
                }
            }).then(function (response) {
                if (lang.exists("WUGetGraphResponse.Graphs.ECLGraphEx", response) && response.WUGetGraphResponse.Graphs.ECLGraphEx.length) {
                    context.graphs[idx].xgmml = response.WUGetGraphResponse.Graphs.ECLGraphEx[0].Graph;
                    onFetchGraphXgmml(context.graphs[idx].xgmml, context.graphs[idx].svg);
                }
            });
        },
        setGraphSvg: function (graphName, svg) {
            var idx = this.getGraphIndex(graphName);
            if (idx >= 0) {
                this.graphs[idx].svg = svg;
                var appData = [];
                appData[graphName + "_SVG"] = svg;
                this.update({}, appData);
            }
        }
    });

    return {
        isInstanceOfWorkunit: function (obj) {
            return obj && obj.isInstanceOf && obj.isInstanceOf(Workunit);
        },

        Create: function (params) {
            retVal = new Workunit(params);
            retVal.create();
            return retVal;
        },

        Get: function (wuid, data) {
            var store = new Store();
            var retVal = store.get(wuid);
            if (data) {
                retVal.updateData(data);
            }
            return retVal;
        },

        CreateWUQueryStore: function (options) {
            var store = new Store(options);
            return Observable(store);
        }
    };
});
