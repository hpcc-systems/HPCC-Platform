﻿/*##############################################################################
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

    "hpcc/ESPResult",
    "hpcc/WsWorkunits"
], function (declare, lang,
    ESPResult, WsWorkunits) {
    return declare(null, {
        Wuid: "",

        stateID: 0,
        state: "",

        text: "",

        resultCount: 0,
        results: [],

        graphs: [],

        exceptions: [],
        timers: [],

        WUInfoResponse: {},

        onCreate: function () {
        },
        onUpdate: function () {
        },
        onSubmit: function () {
        },
        constructor: function (args) {
            declare.safeMixin(this, args);
            if (this.Wuid) {
                this.WUInfoResponse = {
                    Wuid: this.Wuid
                }
            }
        },
        isComplete: function () {
            switch (this.stateID) {
                case 1: //WUStateCompiled
                    if (lang.exists("WUInfoResponse.ActionEx", this) && this.WUInfoResponse.ActionEx == "compile") {
                        return true;
                    }
                    break;
                case 3:	//WUStateCompleted:
                case 4:	//WUStateFailed:
                case 5:	//WUStateArchived:
                case 7:	//WUStateAborted:
                    return true;
            }
            return false;
        },
        monitor: function (callback, monitorDuration) {
            if (!monitorDuration)
                monitorDuration = 0;

            var context = this;
            WsWorkunits.WUInfo({
                request: {
                    Wuid: this.Wuid,
                    TruncateEclTo64k: true,
                    IncludeExceptions: false,
                    IncludeGraphs: false,
                    IncludeSourceFiles: false,
                    IncludeResults: false,
                    IncludeResultsViewNames: false,
                    IncludeVariables: false,
                    IncludeTimers: false,
                    IncludeDebugValues: false,
                    IncludeApplicationValues: false,
                    IncludeWorkflows: false,
                    IncludeXmlSchemas: false,
                    SuppressResultSchemas: true
                },
                load: function (response) {
                    if (lang.exists("WUInfoResponse.Workunit", response)) {
                        context.WUInfoResponse = response.WUInfoResponse.Workunit;
                        context.stateID = context.WUInfoResponse.StateID;
                        context.state = context.WUInfoResponse.State;
                        context.protected = context.WUInfoResponse.Protected;
                        if (callback) {
                            callback(context.WUInfoResponse);
                        }

                        if (!context.isComplete()) {
                            var timeout = 30;	// Seconds

                            if (monitorDuration < 5) {
                                timeout = 1;
                            } else if (monitorDuration < 10) {
                                timeout = 2;
                            } else if (monitorDuration < 30) {
                                timeout = 5;
                            } else if (monitorDuration < 60) {
                                timeout = 10;
                            } else if (monitorDuration < 120) {
                                timeout = 20;
                            }
                            setTimeout(function () {
                                context.monitor(callback, monitorDuration + timeout);
                            }, timeout * 1000);
                        }
                    }
                },
                error: function (repsonse) {
                    done = true;
                }
            });
        },
        create: function (ecl) {
            var context = this;
            WsWorkunits.WUCreate({
                load: function (response) {
                    context.Wuid = response.WUCreateResponse.Workunit.Wuid;
                    context.onCreate();
                }
            });
        },
        update: function (request, appData, callback) {
            lang.mixin(request, {
                Wuid: this.Wuid
            });
            if (this.WUInfoResponse) {
                lang.mixin(request, {
                    StateOrig: this.WUInfoResponse.State,
                    JobnameOrig: this.WUInfoResponse.Jobname,
                    DescriptionOrig: this.WUInfoResponse.Description,
                    ProtectedOrig: this.WUInfoResponse.Protected,
                    ScopeOrig: this.WUInfoResponse.Scope,
                    ClusterOrig: this.WUInfoResponse.Cluster,
                    ApplicationValues: appData
                });
            }

            var context = this;
            WsWorkunits.WUUpdate({
                request: request,
                load: function (response) {
                    context.WUInfoResponse = lang.mixin(context.WUInfoResponse, response.WUUpdateResponse.Workunit);
                    context.onUpdate();
                    if (callback && callback.load) {
                        callback.load(response);
                    }
                },
                error: function (error) {
                    if (callback && callback.error) {
                        callback.error(e);
                    }
                }
            });
        },
        submit: function (target) {
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
        _resubmit: function (clone, resetWorkflow, callback) {
            var context = this;
            WsWorkunits.WUResubmit({
                request: {
                    Wuids: this.Wuid,
                    CloneWorkunit: clone,
                    ResetWorkflow: resetWorkflow
                },
                load: function (response) {
                    if (callback && callback.load) {
                        callback.load(response);
                    }
                },
                error: function (e) {
                    if (callback && callback.error) {
                        callback.error(e);
                    }
                }
            });
        },
        clone: function (callback) {
            this._resubmit(true, false, callback);
        },
        resubmit: function (callback) {
            this._resubmit(false, false, callback);
        },
        restart: function (callback) {
            this._resubmit(false, true, callback);
        },
        _action: function (action, callback) {
            var context = this;
            WsWorkunits.WUAction([{ Wuid: this.Wuid }], action, {
                load: function (response) {
                    if (callback && callback.load) {
                        callback.load(response);
                    }
                },
                error: function (e) {
                    if (callback && callback.error) {
                        callback.error(e);
                    }
                }
            });
        },
        abort: function (callback) {
            this._action("Abort", callback);
        },
        doDelete: function (callback) {
            this._action("Delete", callback);
        },
        publish: function (jobName) {
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
                }
            });
        },
        getInfo: function (args) {
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
                        context.WUInfoResponse = response.WUInfoResponse.Workunit;

                        if (args.onGetText && lang.exists("Query.Text", context.WUInfoResponse)) {
                            context.text = context.WUInfoResponse.Query.Text;
                            args.onGetText(context.text);
                        }
                        if (args.onGetWUExceptions && lang.exists("Exceptions.ECLException", context.WUInfoResponse)) {
                            context.exceptions = [];
                            for (var i = 0; i < context.WUInfoResponse.Exceptions.ECLException.length; ++i) {
                                context.exceptions.push(context.WUInfoResponse.Exceptions.ECLException[i]);
                            }
                            args.onGetWUExceptions(context.exceptions);
                        }
                        if (args.onGetApplicationValues && lang.exists("ApplicationValues.ApplicationValue", context.WUInfoResponse)) {
                            context.applicationValues = context.WUInfoResponse.ApplicationValues.ApplicationValue;
                            args.onGetApplicationValues(context.applicationValues)
                        }
                        if (args.onGetVariables && lang.exists("Variables.ECLResult", context.WUInfoResponse)) {
                            context.variables = [];
                            var variables = context.WUInfoResponse.Variables.ECLResult;
                            for (var i = 0; i < variables.length; ++i) {
                                context.variables.push(lang.mixin({
                                    ColumnType: variables[i].ECLSchemas && variables[i].ECLSchemas.ECLSchemaItem.length ? variables[i].ECLSchemas.ECLSchemaItem[0].ColumnType : "unknown"
                                }, variables[i]));
                            }
                            args.onGetVariables(context.variables);
                        }
                        if (args.onGetResults && lang.exists("Results.ECLResult", context.WUInfoResponse)) {
                            context.results = [];
                            var results = context.WUInfoResponse.Results.ECLResult;
                            for (var i = 0; i < results.length; ++i) {
                                context.results.push(new ESPResult(lang.mixin({ wu: context, Wuid: context.Wuid }, results[i])));
                            }
                            args.onGetResults(context.results);
                        }
                        if (args.onGetSourceFiles && lang.exists("SourceFiles.ECLSourceFile", context.WUInfoResponse)) {
                            context.sourceFiles = [];
                            var sourceFiles = context.WUInfoResponse.SourceFiles.ECLSourceFile;
                            for (var i = 0; i < sourceFiles.length; ++i) {
                                context.sourceFiles.push(new ESPResult(lang.mixin({ wu: context, Wuid: context.Wuid }, sourceFiles[i])));
                            }
                            args.onGetSourceFiles(context.sourceFiles);
                        }
                        if (args.onGetTimers && lang.exists("Timers.ECLTimer", context.WUInfoResponse)) {
                            context.timers = [];
                            for (var i = 0; i < context.WUInfoResponse.Timers.ECLTimer.length; ++i) {
                                var timeParts = context.WUInfoResponse.Timers.ECLTimer[i].Value.split(":");
                                var secs = 0;
                                for (var j = 0; j < timeParts.length; ++j) {
                                    secs = secs * 60 + timeParts[j] * 1;
                                }

                                context.timers.push(lang.mixin(context.WUInfoResponse.Timers.ECLTimer[i], {
                                    Seconds: Math.round(secs * 1000) / 1000,
                                    HasSubGraphId: context.WUInfoResponse.Timers.ECLTimer[i].SubGraphId && context.WUInfoResponse.Timers.ECLTimer[i].SubGraphId != "" ? true : false
                                }));
                            }
                            args.onGetTimers(context.timers);
                        }
                        if (args.onGetGraphs && lang.exists("Graphs.ECLGraph", context.WUInfoResponse)) {
                            context.graphs = context.WUInfoResponse.Graphs.ECLGraph;
                            if (context.timers || context.applicationValues) {
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
                                    if (context.applicationValues) {
                                        var idx = context.getApplicationValueIndex("ESPWorkunit.js", context.graphs[i].Name + "_SVG");
                                        if (idx >= 0) {
                                            context.graphs[i].svg = context.applicationValues[idx].Value;
                                        }
                                    }
                                }
                            }
                            args.onGetGraphs(context.graphs)
                        }
                        if (args.onGetAll) {
                            args.onGetAll(context.WUInfoResponse);
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
            for (var i = 0; i < this.applicationValues.length; ++i) {
                if (this.applicationValues[i].Application == application && this.applicationValues[i].Name == name) {
                    return i;
                }
            }
            return -1;
        },
        getState: function () {
            return this.state;
        },
        getStateIconClass: function () {
            switch (this.stateID) {
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
            switch (this.stateID) {
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
            if (this.protected) {
                return "img/locked.png"
            }
            return "img/unlocked.png"
        },
        fetchText: function (onFetchText) {
            if (this.text) {
                onFetchText(this.text);
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
});
