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
    "dojo/_base/xhr",
    "hpcc/ESPResult",
    "hpcc/ESPBase"
], function (declare, lang, xhr, ESPResult, ESPBase) {
    return declare(ESPBase, {
        wuid: "",

        stateID: 0,
        state: "",

        text: "",

        resultCount: 0,
        results: [],

        graphs: [],

        exceptions: [],
        timers: [],

        onCreate: function () {
        },
        onUpdate: function () {
        },
        onSubmit: function () {
        },
        constructor: function (args) {
            declare.safeMixin(this, args);

            if (!this.wuid) {
                this.create();
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
            var request = {};
            request['Wuid'] = this.wuid;
            request['rawxml_'] = "1";

            var context = this;
            xhr.post({
                url: this.getBaseURL() + "/WUQuery.json",
                handleAs: "json",
                content: request,
                load: function (response) {
                    var workunit = response.WUQueryResponse.Workunits.ECLWorkunit[0];
                    context.stateID = workunit.StateID;
                    context.state = workunit.State;
                    context.protected = workunit.Protected;
                    if (callback) {
                        callback(workunit);
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
                },
                error: function () {
                    done = true;
                }
            });
        },
        create: function (ecl) {
            var request = {};
            request['rawxml_'] = "1";

            var context = this;
            xhr.post({
                url: this.getBaseURL() + "/WUCreate.json",
                handleAs: "json",
                content: request,
                load: function (response) {
                    context.wuid = response.WUCreateResponse.Workunit.Wuid;
                    context.onCreate();
                },
                error: function () {
                }
            });
        },
        update: function (request, appData, callback) {
            lang.mixin(request, {
                Wuid: this.wuid,
                rawxml_: true
            });
            if (this.WUInfoResponse) {
                lang.mixin(request, {
                    StateOrig: this.WUInfoResponse.State,
                    JobnameOrig: this.WUInfoResponse.Jobname,
                    DescriptionOrig: this.WUInfoResponse.Description,
                    ProtectedOrig: this.WUInfoResponse.Protected,
                    ScopeOrig: this.WUInfoResponse.Scope,
                    ClusterOrig: this.WUInfoResponse.Cluster
                });
            }
            if (appData) {
                request['ApplicationValues.ApplicationValue.itemcount'] = appData.length;
                var i = 0;
                for (key in appData) {
                    request['ApplicationValues.ApplicationValue.' + i + '.Application'] = "ESPWorkunit.js";
                    request['ApplicationValues.ApplicationValue.' + i + '.Name'] = key;
                    request['ApplicationValues.ApplicationValue.' + i + '.Value'] = appData[key];
                    ++i;
                }
            }

            var context = this;
            xhr.post({
                url: this.getBaseURL() + "/WUUpdate.json",
                handleAs: "json",
                content: request,
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
            var request = {
                Wuid: this.wuid,
                Cluster: target
            };
            request['rawxml_'] = "1";

            var context = this;
            xhr.post({
                url: this.getBaseURL() + "/WUSubmit.json",
                handleAs: "json",
                content: request,
                load: function (response) {
                    context.onSubmit();
                },
                error: function (error) {
                }
            });
        },
        _resubmit: function (clone, resetWorkflow, callback) {
            var request = {
                Wuids: this.wuid,
                CloneWorkunit: clone,
                ResetWorkflow: resetWorkflow,
                rawxml_: true
            };

            var context = this;
            xhr.post({
                url: this.getBaseURL() + "/WUResubmit.json",
                handleAs: "json",
                content: request,
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
            var request = {
                Wuids: this.wuid,
                ActionType: action,
                rawxml_: true
            };

            var context = this;
            xhr.post({
                url: this.getBaseURL() + "/WUAction.json",
                handleAs: "json",
                content: request,
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
            var request = {
                Wuid: this.wuid,
                JobName: jobName,
                Activate: 1,
                UpdateWorkUnitName: 1,
                Wait: 5000,
                rawxml_: true
            };

            var context = this;
            xhr.post({
                url: this.getBaseURL() + "/WUPublishWorkunit.json",
                handleAs: "json",
                content: request,
                load: function (response) {
                },
                error: function (e) {
                }
            });
        },
        getInfo: function (args) {
            var request = {
                Wuid: this.wuid,
                TruncateEclTo64k: args.onGetText ? false : true,
                IncludeExceptions: args.onGetExceptions ? true : false,
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
                SuppressResultSchemas: args.onGetResults ? false : true,
                rawxml_: true
            };

            var context = this;
            xhr.post({
                url: this.getBaseURL() + "/WUInfo.json",
                handleAs: "json",
                content: request,
                load: function (response) {
                    //var workunit = context.getValue(xmlDom, "Workunit", ["ECLException", "ECLResult", "ECLGraph", "ECLTimer", "ECLSchemaItem", "ApplicationValue"]);
                    var workunit = response.WUInfoResponse.Workunit;
                    context.WUInfoResponse = workunit;

                    if (args.onGetText && workunit.Query.Text) {
                        context.text = workunit.Query.Text;
                        args.onGetText(context.text);
                    }
                    if (args.onGetExceptions && workunit.Exceptions && workunit.Exceptions.ECLException) {
                        context.exceptions = [];
                        for (var i = 0; i < workunit.Exceptions.ECLException.length; ++i) {
                            context.exceptions.push(workunit.Exceptions.ECLException[i]);
                        }
                        args.onGetExceptions(context.exceptions);
                    }
                    if (args.onGetApplicationValues && workunit.ApplicationValues && workunit.ApplicationValues.ApplicationValue) {
                        context.applicationValues = workunit.ApplicationValues.ApplicationValue;
                        args.onGetApplicationValues(context.applicationValues)
                    }
                    if (args.onGetVariables && workunit.Variables && workunit.Variables.ECLResult) {
                        context.variables = [];
                        var variables = workunit.Variables.ECLResult;
                        for (var i = 0; i < variables.length; ++i) {
                            context.variables.push(lang.mixin({
                                ColumnType: variables[i].ECLSchemas && variables[i].ECLSchemas.ECLSchemaItem.length ? variables[i].ECLSchemas.ECLSchemaItem[0].ColumnType : "unknown"
                            }, variables[i]));
                        }
                        args.onGetVariables(context.variables);
                    }
                    if (args.onGetResults && workunit.Results && workunit.Results.ECLResult) {
                        context.results = [];
                        var results = workunit.Results.ECLResult;
                        for (var i = 0; i < results.length; ++i) {
                            context.results.push(new ESPResult(lang.mixin({ wuid: context.wuid }, results[i])));
                        }
                        args.onGetResults(context.results);
                    }
                    if (args.onGetSourceFiles && workunit.SourceFiles && workunit.SourceFiles.ECLSourceFile) {
                        context.sourceFiles = [];
                        var sourceFiles = workunit.SourceFiles.ECLSourceFile;
                        for (var i = 0; i < sourceFiles.length; ++i) {
                            context.sourceFiles.push(new ESPResult(lang.mixin({ wuid: context.wuid }, sourceFiles[i])));
                        }
                        args.onGetSourceFiles(context.sourceFiles);
                    }
                    if (args.onGetTimers && workunit.Timers && workunit.Timers.ECLTimer) {
                        context.timers = [];
                        for (var i = 0; i < workunit.Timers.ECLTimer.length; ++i) {
                            var timeParts = workunit.Timers.ECLTimer[i].Value.split(":");
                            var secs = 0;
                            for (var j = 0; j < timeParts.length; ++j) {
                                secs = secs * 60 + timeParts[j] * 1;
                            }

                            context.timers.push(lang.mixin(workunit.Timers.ECLTimer[i], {
                                Seconds: Math.round(secs * 1000) / 1000,
                                HasSubGraphId: workunit.Timers.ECLTimer[i].SubGraphId && workunit.Timers.ECLTimer[i].SubGraphId != "" ? true : false
                            }));
                        }
                        args.onGetTimers(context.timers);
                    }
                    if (args.onGetGraphs && workunit.Graphs && workunit.Graphs.ECLGraph) {
                        context.graphs = workunit.Graphs.ECLGraph;
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
                        args.onGetAll(workunit);
                    }
                },
                error: function (e) {
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

            var request = {
                Wuid: this.wuid,
                Type: "XML"
            };

            var context = this;
            xhr.post({
                url: this.getBaseURL() + "/WUFile.json",
                handleAs: "text",
                content: request,
                load: function (response) {
                    context.xml = response;
                    onFetchXML(response);
                },
                error: function (e) {
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
            var request = {};
            request['Wuid'] = this.wuid;
            request['GraphName'] = this.graphs[idx].Name;
            request['rawxml_'] = "1";

            var context = this;
            xhr.post({
                url: this.getBaseURL() + "/WUGetGraph.json",
                handleAs: "json",
                content: request,
                load: function (response) {
                    context.graphs[idx].xgmml = response.WUGetGraphResponse.Graphs.ECLGraphEx[0].Graph;
                    onFetchGraphXgmml(context.graphs[idx].xgmml, context.graphs[idx].svg);
                },
                error: function () {
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
