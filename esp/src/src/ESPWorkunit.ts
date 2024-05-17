import * as arrayUtil from "dojo/_base/array";
import * as declare from "dojo/_base/declare";
import * as Deferred from "dojo/_base/Deferred";
import * as lang from "dojo/_base/lang";
import * as all from "dojo/promise/all";
import * as Observable from "dojo/store/Observable";
import * as topic from "dojo/topic";

import { Workunit as HPCCWorkunit, WorkunitsService, WsWorkunits as WsWorkunitsNS, WUUpdate } from "@hpcc-js/comms";
import { IEvent } from "@hpcc-js/util";

import * as ESPRequest from "./ESPRequest";
import * as ESPResult from "./ESPResult";
import * as ESPUtil from "./ESPUtil";
import nlsHPCC from "./nlsHPCC";
import * as Utility from "./Utility";
import * as WsTopology from "./WsTopology";
import * as WsWorkunits from "./WsWorkunits";
import { Paged } from "./store/Paged";
import { BaseStore } from "./store/Store";

declare const dojo;

const _workunits = {};

export function getStateIconClass(stateID: number, complete: boolean, archived: boolean): string {
    if (archived) {
        return "iconArchived";
    }
    switch (stateID) {
        case 1:
            if (complete) {
                return "iconCompleted";
            }
            return "iconSubmitted";
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
}

export function getStateImageName(stateID: number, complete: boolean, archived: boolean): string {
    if (archived) {
        return "workunit_archived.png";
    }
    switch (stateID) {
        case 1:
            if (complete) {
                return "workunit_completed.png";
            }
            return "workunit_submitted.png";
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
}
export function getStateImage(stateID: number, complete: boolean, archived: boolean): string {
    return Utility.getImageURL(getStateImageName(stateID, complete, archived));
}

export function getStateImageHTML(stateID: number, complete: boolean, archived: boolean): string {
    return Utility.getImageHTML(getStateImageName(stateID, complete, archived));
}

export function formatQuery(_filter): { [id: string]: any } {
    const filter = { ..._filter };
    if (filter.LastNDays) {
        const end = new Date();
        const start = new Date();
        start.setDate(end.getDate() - filter.LastNDays);
        filter.StartDate = start.toISOString();
        filter.EndDate = end.toISOString();
        delete filter.LastNDays;
    } else {
        if (filter.StartDate) {
            filter.StartDate = new Date(filter.StartDate).toISOString();
        }
        if (filter.EndDate) {
            filter.EndDate = new Date(filter.EndDate).toISOString();
        }
    }
    if (filter.Type === true) {
        filter.Type = "archived workunits";
    }
    if (filter.Protected === true) {
        filter.Protected = "Protected";
    }
    return filter;
}

export const emptyFilter: { [id: string]: any } = {};
export const defaultSort = { attribute: "Wuid", descending: true };

class Store extends ESPRequest.Store {

    service = "WsWorkunits";
    action = "WUQuery";
    responseQualifier = "WUQueryResponse.Workunits.ECLWorkunit";
    responseTotalQualifier = "WUQueryResponse.NumWUs";
    idProperty = "Wuid";

    startProperty = "PageStartFrom";
    countProperty = "Count";

    _watched: object;
    busy: boolean;
    _toUnwatch: any;

    constructor(options?) {
        super(options);
        this._watched = {};
    }

    preRequest(request) {
        if (request.Sortby && request.Sortby === "TotalClusterTime") {
            request.Sortby = "ClusterTime";
        }
        this.busy = true;
    }

    preProcessFullResponse(response, request, query, options) {
        this.busy = false;
        this._toUnwatch = lang.mixin({}, this._watched);
    }

    create(id) {
        return new Workunit({
            Wuid: id
        });
    }

    update(id, item) {
        const storeItem = this.get(id);
        storeItem.updateData(item);
        if (!this._watched[id]) {
            const context = this;
            this._watched[id] = storeItem.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                if (oldValue !== newValue) {
                    context.notify(storeItem, id);
                }
            });
        } else {
            delete this._toUnwatch[id];
        }
    }

    notify(storeItem: any, id: any) {
        throw new Error("Method not implemented.");
    }

    postProcessResults() {
        for (const key in this._toUnwatch) {
            this._toUnwatch[key].unwatch();
            delete this._watched[key];
        }
        delete this._toUnwatch;
    }
}

const Workunit = declare([ESPUtil.Singleton], {  // jshint ignore:line
    i18n: nlsHPCC,

    //  Asserts  ---
    _assertHasWuid() {
        if (!this.Wuid) {
            throw new Error("Wuid cannot be empty.");
        }
    },
    //  Attributes  ---
    _StateIDSetter(StateID) {
        this.StateID = StateID;
        const actionEx = lang.exists("ActionEx", this) ? this.ActionEx : null;
        this.set("hasCompleted", WsWorkunits.isComplete(this.StateID, actionEx));
    },
    _ActionExSetter(ActionEx) {
        if (this.StateID !== undefined) {
            this.ActionEx = ActionEx;
            this.set("hasCompleted", WsWorkunits.isComplete(this.StateID, this.ActionEx));
        }
    },
    _hasCompletedSetter(completed) {
        const justCompleted = !this.hasCompleted && completed;
        this.hasCompleted = completed;
        if (justCompleted) {
            topic.publish("hpcc/ecl_wu_completed", this);
        }
        if (!this.hasCompleted && this.component !== "ActivityWidget") {
            this.startMonitor();
        }
    },
    _VariablesSetter(Variables) {
        this.set("variables", Variables.ECLResult);
    },
    _ResultsSetter(Results) {
        const results = [];
        const sequenceResults = [];
        const namedResults = {};
        for (let i = 0; i < Results.ECLResult.length; ++i) {
            const espResult = ESPResult.Get(lang.mixin({
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
    _SourceFilesSetter(SourceFiles) {
        const sourceFiles = [];
        for (let i = 0; i < SourceFiles.ECLSourceFile.length; ++i) {
            sourceFiles.push(ESPResult.Get(lang.mixin({ wu: this.wu, Wuid: this.Wuid, __hpcc_parentName: "" }, SourceFiles.ECLSourceFile[i])));
            if (lang.exists("ECLSourceFiles.ECLSourceFile", SourceFiles.ECLSourceFile[i])) {
                for (let j = 0; j < SourceFiles.ECLSourceFile[i].ECLSourceFiles.ECLSourceFile.length; ++j) {
                    sourceFiles.push(ESPResult.Get(lang.mixin({ wu: this.wu, Wuid: this.Wuid, __hpcc_parentName: SourceFiles.ECLSourceFile[i].Name }, SourceFiles.ECLSourceFile[i].ECLSourceFiles.ECLSourceFile[j])));
                }
            }
        }
        this.set("sourceFiles", sourceFiles);
    },
    _TimersSetter(Timers) {
        const timers = [];
        for (let i = 0; i < Timers.ECLTimer.length; ++i) {
            const secs = Utility.espTime2Seconds(Timers.ECLTimer[i].Value);
            timers.push(lang.mixin(Timers.ECLTimer[i], {
                __hpcc_id: i + 1,
                Seconds: Math.round(secs * 1000) / 1000,
                HasSubGraphId: Timers.ECLTimer[i].SubGraphId && Timers.ECLTimer[i].SubGraphId !== "" ? true : false
            }));
        }
        this.set("timers", timers);
    },
    _ResourceURLsSetter(resourceURLs) {
        const data = [];
        arrayUtil.forEach(resourceURLs.URL, function (url, idx) {
            const cleanedURL = url.split("\\").join("/");
            const urlParts = cleanedURL.split("/");
            const matchStr = "res/" + this.wu.Wuid + "/";
            if (cleanedURL.indexOf(matchStr) === 0) {
                const displayPath = cleanedURL.substr(matchStr.length);
                const displayName = urlParts[urlParts.length - 1];
                const row = {
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
    _GraphsSetter(Graphs) {
        this.set("graphs", Graphs.ECLGraph);
    },

    //  Calculated "Helpers"  ---
    _HelpersSetter(Helpers) {
        this.set("helpers", Helpers.ECLHelpFile);
        this.refreshHelpersCount();
    },
    _ThorLogListSetter(ThorLogList) {
        this.set("thorLogInfo", ThorLogList.ThorLogInfo);
        this.getThorLogStatus(ThorLogList);
        this.refreshHelpersCount();
    },
    _HasArchiveQuerySetter(HasArchiveQuery) {
        this.set("hasArchiveQuery", HasArchiveQuery);
        this.refreshHelpersCount();
    },
    refreshHelpersCount() {
        let eclwatchHelpersCount = 2;   //  ECL + Workunit XML are also helpers...
        if (this.helpers) {
            eclwatchHelpersCount += this.helpers.length;
        }
        if (this.thorLogList) {
            eclwatchHelpersCount += this.thorLogList.length;
        }
        if (this.hasArchiveQuery) {
            eclwatchHelpersCount += 1;
        }
        this.set("eclwatchHelpersCount", eclwatchHelpersCount);
    },

    //  ---  ---  ---
    onCreate() {
    },
    onUpdate() {
    },
    onSubmit() {
    },
    constructor: ESPUtil.override(function (inherited, args) {
        inherited(arguments);
        if (args) {
            declare.safeMixin(this, args);
        }
        this.wu = this;
        this._hpccWU = HPCCWorkunit.attach({ baseUrl: "" }, this.Wuid);
    }),
    isComplete() {
        return this.hasCompleted;
    },
    isFailed() {
        return this.StateID === 4;
    },
    isDeleted() {
        return this.StateID === 999;
    },
    isBlocked() {
        return this.StateID === 8;
    },
    isAbleToDeschedule() {
        return this.EventSchedule === 2;
    },
    isAbleToReschedule() {
        return this.EventSchedule === 1;
    },
    isMonitoring(): boolean {
        return !!this._hpccWatchHandle;
    },
    disableMonitor(disableMonitor: boolean) {
        if (disableMonitor) {
            this.stopMonitor();
        } else {
            this.startMonitor();
        }
    },
    startMonitor() {
        if (this.isMonitoring())
            return;
        this._hpccWatchHandle = this._hpccWU.watch((changes: IEvent[]) => {
            this.updateData(this._hpccWU.properties);
        }, true);
    },
    stopMonitor() {
        if (this._hpccWatchHandle) {
            this._hpccWatchHandle.release();
            delete this._hpccWatchHandle;
        }
    },
    monitor(callback) {
        if (callback) {
            callback(this);
        }
        if (!this.hasCompleted) {
            const context = this;
            if (this._watchHandle) {
                this._watchHandle.unwatch();
            }
            this._watchHandle = this.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                if (oldValue !== newValue && newValue) {
                    if (callback) {
                        callback(context);
                    }
                }
            });
        }
    },
    doDeschedule() {
        return this._action("Deschedule").then(function (response) {
        });
    },
    doReschedule() {
        return this._action("Reschedule").then(function (response) {
        });
    },
    create(ecl) {
        const context = this;
        WsWorkunits.WUCreate({
            load(response) {
                if (lang.exists("Exceptions.Exception", response)) {
                    dojo.publish("hpcc/brToaster", {
                        message: "<h4>" + response.Exceptions.Source + "</h4>" + "<p>" + response.Exceptions.Exception[0].Message + "</p>",
                        type: "error",
                        duration: -1
                    });
                } else {
                    _workunits[response.WUCreateResponse.Workunit.Wuid] = context;
                    context.Wuid = response.WUCreateResponse.Workunit.Wuid;
                    context._hpccWU = HPCCWorkunit.attach({ baseUrl: "" }, context.Wuid);
                    context.startMonitor(true);
                    context.updateData(response.WUCreateResponse.Workunit);
                    context.onCreate();
                }
            }
        });
    },
    update(request, appData) {
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

        const context = this;
        WsWorkunits.WUUpdate({
            request,
            load(response) {
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
    submit(target) {
        this._assertHasWuid();
        const context = this;
        const deferred = new Deferred();
        deferred.promise.then(function (target) {
            WsWorkunits.WUSubmit({
                request: {
                    Wuid: context.Wuid,
                    Cluster: target
                },
                load(response) {
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
    _resubmit(clone, resetWorkflow) {
        this._assertHasWuid();
        const context = this;
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
    clone() {
        const context = this;
        this._resubmit(true, false).then(function (response) {
            if (!lang.exists("Exceptions.Source", response)) {
                let msg = "";
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
    resubmit() {
        const context = this;
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
    recover() {
        const context = this;
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
    _action(action) {
        this._assertHasWuid();
        const context = this;
        return WsWorkunits.WUAction([{ Wuid: this.Wuid }], action, {
            load(response) {
                context.refresh();
            }
        });
    },
    setToFailed() {
        return this._action("setToFailed");
    },
    pause() {
        return this._action("Pause");
    },
    pauseNow() {
        return this._action("PauseNow");
    },
    resume() {
        return this._action("Resume");
    },
    abort() {
        return this._action("Abort");
    },
    doDelete() {
        return this._action("Delete").then(function (response) {
        });
    },

    restore() {
        return this._action("Restore");
    },

    publish(jobName, remoteDali, remoteStorage, sourceProcess, priority, comment, allowForeign, updateSupers) {
        this._assertHasWuid();
        const context = this;
        WsWorkunits.WUPublishWorkunit({
            request: {
                Wuid: this.Wuid,
                JobName: jobName,
                RemoteDali: remoteDali,
                RemoteStorage: remoteStorage,
                SourceProcess: sourceProcess,
                Priority: priority,
                Comment: comment,
                AllowForeignFiles: allowForeign,
                UpdateSuperFiles: updateSupers,
                Activate: 1,
                UpdateWorkUnitName: 1,
                Wait: 5000
            },
            load(response) {
                context.updateData(response.WUPublishWorkunitResponse);
            }
        });
    },
    refresh(full) {
        return this._hpccWU.refresh(full || this.Archived || this.__hpcc_changedCount === 0).then(wu => {
            this.updateData(wu.properties);
            return wu.properties;
        });
    },
    getQuery() {
        this._assertHasWuid();
        const context = this;
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
    getInfo(args) {
        this._assertHasWuid();
        const context = this;
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
                IncludeServiceNames: args.onGetServiceNames ? true : false,
                SuppressResultSchemas: true
            }
        }).then(function (response) {
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
                    });
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
                    args.onGetVariables(lang.exists("variables", context) ? context.variables : []);
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
                        for (let i = 0; i < context.graphs.length; ++i) {
                            if (context.timers) {
                                context.graphs[i].Time = 0;
                                for (let j = 0; j < context.timers.length; ++j) {
                                    if (context.timers[j].GraphName === context.graphs[i].Name && !context.timers[j].HasSubGraphId) {
                                        context.graphs[i].Time = context.timers[j].Seconds;
                                        break;
                                    }
                                }
                                context.graphs[i].Time = Math.round(context.graphs[i].Time * 1000) / 1000;
                            }
                            if (lang.exists("ApplicationValues.ApplicationValue", context)) {
                                const idx = context.getApplicationValueIndex("ESPWorkunit.js", context.graphs[i].Name + "_SVG");
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
                if (args.onGetServiceNames && lang.exists("ServiceNames.Item", context)) {
                    args.onGetServiceNames(context.ServiceNames.Item);
                }
                if (args.onAfterSend) {
                    args.onAfterSend(context);
                }
            }
            return response;
        });
    },
    getGraphIndex(name) {
        if (this.graphs) {
            for (let i = 0; i < this.graphs.length; ++i) {
                if (this.graphs[i].Name === name) {
                    return i;
                }
            }
        }
        return -1;
    },
    getGraphTimers(name) {
        const retVal = [];
        arrayUtil.forEach(this.timers, function (timer, idx) {
            if (timer.HasSubGraphId && timer.GraphName === name) {
                retVal.push(timer);
            }
        }, this);
        return retVal;
    },
    getApplicationValueIndex(application, name) {
        if (lang.exists("ApplicationValues.ApplicationValue", this)) {
            for (let i = 0; i < this.ApplicationValues.ApplicationValue.length; ++i) {
                if (this.ApplicationValues.ApplicationValue[i].Application === application && this.ApplicationValues.ApplicationValue[i].Name === name) {
                    return i;
                }
            }
        }
        return -1;
    },
    getThorLogStatus(ThorLogList) {
        return ThorLogList.ThorLogInfo.length > 0 ? true : false;
    },
    getState() {
        return this.State;
    },
    getStateIconClass() {
        return getStateIconClass(this.StateID, this.isComplete(), this.Archived);
    },
    getStateImageName() {
        return getStateImageName(this.StateID, this.isComplete(), this.Archived);
    },
    getStateImage() {
        return getStateImage(this.StateID, this.isComplete(), this.Archived);
    },
    getStateImageHTML() {
        return getStateImageHTML(this.StateID, this.isComplete(), this.Archived);
    },
    getProtectedImageName() {
        if (this.Protected) {
            return "locked.png";
        }
        return "unlocked.png";
    },
    getProtectedImage() {
        return Utility.getImageURL(this.getProtectedImageName());
    },
    getProtectedHTML() {
        return Utility.getImageHTML(this.getProtectedImageName());
    },
    fetchText(onFetchText) {
        const context = this;
        if (lang.exists("Query.Text", context)) {
            onFetchText(this.Query.Text);
            return;
        }

        this.getInfo({
            onGetText: onFetchText
        });
    },
    fetchXML(onFetchXML) {
        if (this.xml) {
            onFetchXML(this.xml);
            return;
        }

        this._assertHasWuid();
        const context = this;
        WsWorkunits.WUFile({
            request: {
                Wuid: this.Wuid,
                Type: "XML"
            },
            load(response) {
                context.xml = response;
                onFetchXML(response);
            }
        });
    },
    fetchResults(onFetchResults) {
        if (this.results && this.results.length) {
            onFetchResults(this.results);
            return;
        }

        this.getInfo({
            onGetResults: onFetchResults
        });
    },
    fetchNamedResults(resultNames, row, count) {
        const deferred = new Deferred();
        const context = this;
        this.fetchResults(function (results) {
            const resultContents = [];
            arrayUtil.forEach(resultNames, function (item, idx) {
                resultContents.push(context.namedResults[item].fetchContent(row, count));
            });
            all(resultContents).then(function (resultContents) {
                const results = [];
                arrayUtil.forEach(resultContents, function (item, idx) {
                    results[resultNames[idx]] = item;
                });
                deferred.resolve(results);
            });
        });
        return deferred.promise;
    },
    fetchAllNamedResults(row, count) {
        const deferred = new Deferred();
        const context = this;
        this.fetchResults(function (results) {
            const resultNames = [];
            arrayUtil.forEach(results, function (item, idx) {
                resultNames.push(item.Name);
            });
            context.fetchNamedResults(resultNames, row, count).then(function (response) {
                deferred.resolve(response);
            });
        });
        return deferred.promise;
    },
    fetchSequenceResults(onFetchSequenceResults) {
        if (this.sequenceResults && this.sequenceResults.length) {
            onFetchSequenceResults(this.sequenceResults);
            return;
        }

        this.getInfo({
            onGetSequenceResults: onFetchSequenceResults
        });
    },
    fetchSourceFiles(onFetchSourceFiles) {
        if (this.sourceFiles && this.sourceFiles.length) {
            onFetchSourceFiles(this.sourceFiles);
            return;
        }

        this.getInfo({
            onGetSourceFiles: onFetchSourceFiles
        });
    },
    fetchTimers(onFetchTimers) {
        if (this.timers && this.timers.length) {
            onFetchTimers(this.timers);
            return;
        }

        this.getInfo({
            onGetTimers: onFetchTimers
        });
    },
    fetchActivities() {
        return (this._hpccWU as HPCCWorkunit).fetchDetails({
            ScopeFilter: {
                MaxDepth: 999999,
                ScopeTypes: { ScopeType: ["graph"] }
            },
            ScopeOptions: {
                IncludeMatchedScopesInResults: true,
                IncludeScope: true,
                IncludeId: true,
                IncludeScopeType: true
            },
            PropertyOptions: {
                IncludeName: false,
                IncludeRawValue: false,
                IncludeFormatted: false,
                IncludeMeasure: false,
                IncludeCreator: false,
                IncludeCreatorType: false
            },
            NestedFilter: {
                Depth: 999999,
                ScopeTypes: { ScopeType: ["activity"] }
            },
            PropertiesToReturn: {
                AllStatistics: false,
                AllAttributes: false,
                AllHints: false,
                AllProperties: false,
                AllScopes: true
            }
        }).then(response => {
            const retVal = {};
            response.forEach(scope => {
                const graphID = scope.ScopeName.split(":")[1];
                if (!retVal[graphID]) {
                    retVal[graphID] = [];
                }
                retVal[graphID].push(scope.Id);
            });
            return retVal;
        });
    },
    fetchGraphs(onFetchGraphs) {
        if (this.graphs && this.graphs.length) {
            onFetchGraphs(this.graphs);
            return;
        }

        this.getInfo({
            onGetGraphs: onFetchGraphs
        });
    },
    fetchGraphXgmmlByName(name, subGraphId, onFetchGraphXgmml, force) {
        const idx = this.getGraphIndex(name);
        if (idx >= 0) {
            this.fetchGraphXgmml(idx, subGraphId, onFetchGraphXgmml, force);
        } else {
            topic.publish("hpcc/brToaster", {
                Severity: "Error",
                Source: "ESPWorkunit.fetchGraphXgmmlByName",
                Exceptions: [
                    { Message: this.i18n.FetchingXGMMLFailed }
                ]
            });
            onFetchGraphXgmml("", "");
        }
    },
    fetchGraphXgmml(idx, subGraphId, onFetchGraphXgmml, force) {
        if (!force && !subGraphId && this.graphs && this.graphs[idx] && this.graphs[idx].xgmml) {
            onFetchGraphXgmml(this.graphs[idx].xgmml, this.graphs[idx].svg);
            return;
        } else if (!force && subGraphId && this.subgraphs && this.subgraphs[idx + "." + subGraphId] && this.subgraphs[idx + "." + subGraphId].xgmml) {
            onFetchGraphXgmml(this.subgraphs[idx + "." + subGraphId].xgmml, this.subgraphs[idx + "." + subGraphId].svg);
            return;
        }

        this._assertHasWuid();
        const context = this;
        WsWorkunits.WUGetGraph({
            request: {
                Wuid: this.Wuid,
                GraphName: this.graphs[idx].Name,
                SubGraphId: subGraphId
            }
        }).then(function (response) {
            if (lang.exists("WUGetGraphResponse.Graphs.ECLGraphEx", response) && response.WUGetGraphResponse.Graphs.ECLGraphEx.length) {
                if (subGraphId) {
                    if (!context.subgraphs) {
                        context.subgraphs = {};
                    }
                    if (!context.subgraphs[idx + "." + subGraphId]) {
                        context.subgraphs[idx + "." + subGraphId] = {};
                    }
                    context.subgraphs[idx + "." + subGraphId].xgmml = "<graph>" + response.WUGetGraphResponse.Graphs.ECLGraphEx[0].Graph + "</graph>";
                    onFetchGraphXgmml(context.subgraphs[idx + "." + subGraphId].xgmml, context.subgraphs[idx + "." + subGraphId].svg);
                } else {
                    context.graphs[idx].xgmml = response.WUGetGraphResponse.Graphs.ECLGraphEx[0].Graph;
                    onFetchGraphXgmml(context.graphs[idx].xgmml, context.graphs[idx].svg);
                }
            } else {
                topic.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: "ESPWorkunit.fetchGraphXgmml",
                    Exceptions: [
                        { Message: context.i18n.FetchingXGMMLFailed }
                    ]
                });
                onFetchGraphXgmml("", "");
            }
        });
    },
    fetchServiceNames(onFetchServiceNames: (items: string[]) => void = items => { }) {
        if (this.serviceNames && this.serviceNames.length) {
            onFetchServiceNames(this.serviceNames);
            return;
        }

        this.getInfo({
            onGetServiceNames: onFetchServiceNames
        });
    },
    setGraphSvg(graphName, svg) {
        const idx = this.getGraphIndex(graphName);
        if (idx >= 0) {
            this.graphs[idx].svg = svg;
            const appData = [];
            appData[graphName + "_SVG"] = svg;
            this.update({}, appData);
        }
    }
});

export function isInstanceOfWorkunit(obj) {
    return obj && obj.isInstanceOf && obj.isInstanceOf(Workunit);
}

export function Create(params) {
    const retVal = new Workunit(params);
    retVal.create();
    return retVal;
}

export function Get(wuid, data?) {
    const store = new Store();
    const retVal = store.get(wuid);
    if (data) {
        retVal.updateData(data);
    }
    return retVal;
}

export function CreateWUQueryStoreLegacy(options) {
    const store = new Store(options);
    return new Observable(store);
}

const service = new WorkunitsService({ baseUrl: "" });

export type WUQueryStore = BaseStore<WsWorkunitsNS.WUQuery, typeof Workunit>;

export function CreateWUQueryStore(): BaseStore<WsWorkunitsNS.WUQuery, typeof Workunit> {
    const store = new Paged<WsWorkunitsNS.WUQuery, typeof Workunit>({
        start: "PageStartFrom",
        count: "PageSize",
        sortBy: "Sortby",
        descending: "Descending"
    }, "Wuid", request => {
        if (request.Sortby && request.Sortby === "TotalClusterTime") {
            request.Sortby = "ClusterTime";
        }
        return service.WUQuery(request).then(response => {
            return {
                data: response.Workunits.ECLWorkunit.map(wu => Get(wu.Wuid, wu)),
                total: response.NumWUs
            };
        });
    });
    return new Observable(store);
}

export const Action = WUUpdate.Action;
