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
    "dojo/promise/all",
    "dojo/store/Observable",
    "dojo/topic",

    "hpcc/WsWorkunits",
    "hpcc/WsTopology",
    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/ESPResult"
], function (declare, arrayUtil, lang, Deferred, all, Observable, topic,
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
            if (request.Sortby && request.Sortby === "TotalThorTime") {
                request.Sortby = "ThorTime";
            }
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
                sourceFiles.push(ESPResult.Get(lang.mixin({ wu: this.wu, Wuid: this.Wuid }, SourceFiles.ECLSourceFile[i])));
            }
            this.set("sourceFiles", sourceFiles);
        },
        _TimersSetter: function (Timers) {
            var timers = [];
            for (var i = 0; i < Timers.ECLTimer.length; ++i) {
                var timeParts = Timers.ECLTimer[i].Value.split(":");
                var secs = 0;
                for (var j = 0; j < timeParts.length; ++j) {
                    secs = secs * 60 + timeParts[j] * 1;
                }

                timers.push(lang.mixin(Timers.ECLTimer[i], {
                    Seconds: Math.round(secs * 1000) / 1000,
                    HasSubGraphId: Timers.ECLTimer[i].SubGraphId && Timers.ECLTimer[i].SubGraphId != "" ? true : false
                }));
            }
            this.set("timers", timers);
        },
        _GraphsSetter: function (Graphs) {
            this.set("graphs", Graphs.ECLGraph);
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
                    context.updateData(response.WUUpdateResponse.Workunit);
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
            WsWorkunits.WUResubmit({
                request: {
                    Wuids: this.Wuid,
                    CloneWorkunit: clone,
                    ResetWorkflow: resetWorkflow
                },
                load: function (response) {
                    context.refresh();
                }
            });
        },
        clone: function () {
            this._resubmit(true, false);
        },
        resubmit: function () {
            this._resubmit(false, false);
        },
        restart: function () {
            this._resubmit(false, true);
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
        abort: function () {
            return this._action("Abort");
        },
        doDelete: function () {
            return this._action("Delete").then(function(response) {
                var d= 0;
            });
        },
        publish: function (jobName, remoteDali, priority, comment) {
            this._assertHasWuid();
            var context = this;
            WsWorkunits.WUPublishWorkunit({
                request: {
                    Wuid: this.Wuid,
                    JobName: jobName,
                    RemoteDali: remoteDali,
                    Priority: priority,
                    Comment: comment,
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
            if (full || this.changedCount === 0) {
                this.getInfo({
                    onGetText: function () {
                    },
                    onGetWUExceptions: function () {
                    },
                    onGetVariables: function () {
                    },
                    onGetTimers: function () {
                    },
                    onGetApplicationValues: function () {
                    }
                });
            } else {
                this.getQuery();
            }
        },
        getQuery: function () {
            this._assertHasWuid();
            var context = this;
            WsWorkunits.WUQuery({
                request: {
                    Wuid: this.Wuid
                }
            }).then(function (response) {
                if (lang.exists("WUQueryResponse.Workunits.ECLWorkunit", response)) {
                    arrayUtil.forEach(response.WUQueryResponse.Workunits.ECLWorkunit, function (item, index) {
                        context.updateData(item);
                    });
                }
            });
        },
        getInfo: function (args) {
            this._assertHasWuid();
            var context = this;
            WsWorkunits.WUInfo({
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
                    IncludeDebugValues: false,
                    IncludeApplicationValues: args.onGetApplicationValues ? true : false,
                    IncludeWorkflows: false,
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
                    context.updateData(response.WUInfoResponse.Workunit);

                    if (args.onGetText && lang.exists("Query.Text", context)) {
                        args.onGetText(context.Query.Text);
                    }
                    if (args.onGetWUExceptions && lang.exists("Exceptions.ECLException", context)) {
                        args.onGetWUExceptions(context.Exceptions.ECLException);
                    }
                    if (args.onGetApplicationValues && lang.exists("ApplicationValues.ApplicationValue", context)) {
                        args.onGetApplicationValues(context.ApplicationValues.ApplicationValue)
                    }
                    if (args.onGetVariables && lang.exists("variables", context)) {
                        args.onGetVariables(context.variables);
                    }
                    if (args.onGetResults && lang.exists("results", context)) {
                        args.onGetResults(context.results);
                    }
                    if (args.onGetSequenceResults && lang.exists("sequenceResults", context)) {
                        args.onGetSequenceResults(context.sequenceResults);
                    }
                    if (args.onGetSourceFiles && lang.exists("sourceFiles", context)) {
                        args.onGetSourceFiles(context.sourceFiles);
                    }
                    if (args.onGetTimers && lang.exists("timers", context)) {
                        args.onGetTimers(context.timers);
                    }
                    if (args.onGetGraphs && lang.exists("graphs", context)) {
                        if (context.timers || lang.exists("ApplicationValues.ApplicationValue", context)) {
                            for (var i = 0; i < context.graphs.length; ++i) {
                                if (context.timers) {
                                    context.graphs[i].Time = 0;
                                    for (var j = 0; j < context.timers.length; ++j) {
                                        if (context.timers[j].GraphName == context.graphs[i].Name) {
                                            context.graphs[i].Time += context.timers[j].Seconds;
                                        }
                                        context.graphs[i].Time = Math.round(context.graphs[i].Time * 1000) / 1000;
                                    }
                                }
                                if (lang.exists("ApplicationValues.ApplicationValue", context)) {
                                    var idx = context.getApplicationValueIndex("ESPWorkunit.js", context.graphs[i].Name + "_SVG");
                                    if (idx >= 0) {
                                        context.graphs[i].svg = context.ApplicationValues.ApplicationValue[idx].Value;
                                    }
                                }
                            }
                        }
                        args.onGetGraphs(context.graphs)
                    }
                    if (args.onAfterSend) {
                        args.onAfterSend(context);
                    }
                }
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
        fetchNamedResults: function (resultNames) {
            var deferred = new Deferred()
            var context = this;
            this.fetchResults(function (results) {
                var resultContents = [];
                arrayUtil.forEach(resultNames, function (item, idx) {
                    resultContents.push(context.namedResults[item].fetchContent());
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
                },
                load: function (response) {
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
            return obj.isInstanceOf(Workunit);
        },

        Create: function (params) {
            retVal = new Workunit(params);
            retVal.create();
            return retVal;
        },

        Get: function (wuid) {
            var store = new Store();
            return store.get(wuid);
        },

        CreateWUQueryStore: function (options) {
            var store = new Store(options);
            return Observable(store);
        }
    };
});
