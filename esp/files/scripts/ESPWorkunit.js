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

    "hpcc/WsWorkunits",
    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/ESPResult"
], function (declare, arrayUtil, lang, Deferred, ObjectStore, QueryResults, Observable,
    WsWorkunits, ESPUtil, ESPRequest, ESPResult) {

    var _workunits = {};

    var Store = declare([ESPRequest.Store], {
        service: "WsWorkunits",
        action: "WUQuery",
        responseQualifier: "Workunits.ECLWorkunit",
        responseTotalQualifier: "NumWUs",
        idProperty: "Wuid",
        startProperty: "PageStartFrom",
        countProperty: "Count",

        _watched: [],
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
        _VariablesSetter: function (Variables) {
            var variables = [];
            for (var i = 0; i < Variables.ECLResult.length; ++i) {
                variables.push(lang.mixin({
                    ColumnType: Variables.ECLResult[i].ECLSchemas && Variables.ECLResult[i].ECLSchemas.ECLSchemaItem.length ? Variables.ECLResult[i].ECLSchemas.ECLSchemaItem[0].ColumnType : "unknown"
                }, variables[i]));
            }
            this.set("variables", variables);
        },
        _ResultsSetter: function (Results) {
            var results = [];
            for (var i = 0; i < Results.ECLResult.length; ++i) {
                results.push(ESPResult.Get(lang.mixin({ wu: this.wu, Wuid: this.Wuid }, Results.ECLResult[i])));
            }
            this.set("results", results);
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
            WsWorkunits.WUSubmit({
                request: {
                    Wuid: this.Wuid,
                    Cluster: target
                },
                load: function (response) {
                    context.onSubmit();
                }
            });
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
            WsWorkunits.WUAction([{ Wuid: this.Wuid }], action, {
                load: function (response) {
                    context.refresh();
                }
            });
        },
        setToFailed: function () {
            this._action("setToFailed");
        },
        abort: function () {
            this._action("Abort");
        },
        doDelete: function () {
            this._action("Delete");
        },
        publish: function (jobName) {
            this._assertHasWuid();
            var context = this;
            WsWorkunits.WUPublishWorkunit({
                request: {
                    Wuid: this.Wuid,
                    JobName: jobName,
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
            if (full) {
                this.getInfo({
                    onGetText: function () {
                    },
                    onGetWUExceptions: function () {
                    },
                    onGetGraphs: function () {
                    },
                    onGetSourceFiles: function () {
                    },
                    onGetResults: function () {
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
                    Wuid: this.Wuid,
                },
                load: function (response) {
                    if (lang.exists("WUQueryResponse.Workunits.ECLWorkunit", response)) {
                        arrayUtil.forEach(response.WUQueryResponse.Workunits.ECLWorkunit, function (item, index) {
                            context.updateData(item);
                        });
                    }
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
                    IncludeResults: args.onGetResults ? true : false,
                    IncludeResultsViewNames: false,
                    IncludeVariables: args.onGetVariables ? true : false,
                    IncludeTimers: args.onGetTimers ? true : false,
                    IncludeDebugValues: false,
                    IncludeApplicationValues: args.onGetApplicationValues ? true : false,
                    IncludeWorkflows: false,
                    IncludeXmlSchemas: args.onGetResults ? true : false,
                    SuppressResultSchemas: args.onGetResults ? false : true
                },
                load: function (response) {
                    if (lang.exists("WUInfoResponse.Workunit", response)) {
                        if (!args.onGetText && lang.exists("WUInfoResponse.Workunit.Query", response)) {
                            //  A truncated version of ECL just causes issues  ---
                            delete response.WUInfoResponse.Workunit.Query;
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
        getStateImage: function () {
            switch (this.StateID) {
                case 1:
                    return "img/workunit_completed.png";
                case 2:
                    return "img/workunit_running.png";
                case 3:
                    return "img/workunit_completed.png";
                case 4:
                    return "img/workunit_failed.png";
                case 5:
                    return "img/workunit_warning.png";
                case 6:
                    return "img/workunit_aborting.png";
                case 7:
                    return "img/workunit_failed.png";
                case 8:
                    return "img/workunit_warning.png";
                case 9:
                    return "img/workunit_submitted.png";
                case 10:
                    return "img/workunit_warning.png";
                case 11:
                    return "img/workunit_running.png";
                case 12:
                    return "img/workunit_warning.png";
                case 13:
                    return "img/workunit_warning.png";
                case 14:
                    return "img/workunit_warning.png";
                case 15:
                    return "img/workunit_running.png";
                case 16:
                    return "img/workunit_warning.png";
                case 999:
                    return "img/workunit_deleted.png";
            }
            return "img/workunit.png";
        },
        getProtectedImage: function () {
            if (this.Protected) {
                return "img/locked.png"
            }
            return "img/unlocked.png"
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
        fetchGraphXgmmlByName: function (name, onFetchGraphXgmml) {
            var idx = this.getGraphIndex(name);
            if (idx >= 0) {
                this.fetchGraphXgmml(idx, onFetchGraphXgmml);
            }
        },
        fetchGraphXgmml: function (idx, onFetchGraphXgmml) {
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
        },

        CreateWUQueryObjectStore: function (options) {
            return new ObjectStore({ objectStore: this.CreateWUQueryStore(options) });
        }
    };
});
