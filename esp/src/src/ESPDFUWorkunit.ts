import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";
import * as Observable from "dojo/store/Observable";
import * as topic from "dojo/topic";
import * as ESPRequest from "./ESPRequest";
import * as ESPUtil from "./ESPUtil";
import * as FileSpray from "./FileSpray";
import nlsHPCC from "./nlsHPCC";
import * as Utility from "./Utility";

const i18n = nlsHPCC;

class Store extends ESPRequest.Store {

    service = "FileSpray";
    action = "GetDFUWorkunits";
    responseQualifier = "GetDFUWorkunitsResponse.results.DFUWorkunit";
    responseTotalQualifier = "GetDFUWorkunitsResponse.NumWUs";
    idProperty = "ID";

    startProperty = "PageStartFrom";
    countProperty = "PageSize";

    _watched = [];

    preRequest(request) {
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
    }

    create(id) {
        return new Workunit({
            ID: id,
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
        }
    }
}

const Workunit = declare([ESPUtil.Singleton, ESPUtil.Monitor], { // jshint ignore:line
    //  Asserts  ---
    _assertHasWuid() {
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

    _StateSetter(state) {
        this.State = state;
        this.set("hasCompleted", FileSpray.isComplete(this.State));
    },

    _hasCompletedSetter(completed) {
        const justCompleted = lang.exists("hasCompleted", this) && !this.hasCompleted && completed;
        this.hasCompleted = completed;
        if (justCompleted) {
            topic.publish("hpcc/dfu_wu_completed", this);
        }
    },

    _CommandSetter(command) {
        this.Command = command;
        if (command in FileSpray.CommandMessages) {
            this.set("CommandMessage", FileSpray.CommandMessages[command]);
        } else {
            this.set("CommandMessage", i18n.Unknown + " (" + command + ")");
        }
    },

    _SourceFormatSetter(format) {
        this.SourceFormat = format;
        if (format in FileSpray.FormatMessages) {
            this.set("SourceFormatMessage", FileSpray.FormatMessages[format]);
        } else {
            this.set("SourceFormatMessage", i18n.Unknown + " (" + format + ")");
        }
    },

    _DestFormatSetter(format) {
        this.DestFormat = format;
        if (format in FileSpray.FormatMessages) {
            this.set("DestFormatMessage", FileSpray.FormatMessages[format]);
        } else {
            this.set("DestFormatMessage", i18n.Unknown + " (" + format + ")");
        }
    },

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
    }),
    isComplete() {
        return this.hasCompleted;
    },
    isDeleted() {
        return this.State === 999;
    },
    monitor(callback) {
        if (callback) {
            callback(this);
        }
        if (!this.hasCompleted) {
            const context = this;
            this.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                if (oldValue !== newValue && newValue) {
                    if (callback) {
                        callback(context);
                    }
                }
            });
        }
    },
    create(ecl) {
    },
    update(request) {
        this._assertHasWuid();
        lang.mixin(request, {
            ID: this.Wuid
        });

        const outerRequest = {
            "wu.ID": request.ID,
            "wu.isProtected": request.isProtected,
            "wu.JobName": request.JobName,
            "isProtectedOrig": this.isProtected,
            "JobNameOrig": this.JobName
        };

        const context = this;
        FileSpray.UpdateDFUWorkunit({
            request: outerRequest
        }).then(function (response) {
            context.refresh();
        });
    },
    submit(target) {
    },
    fetchXML(onFetchXML) {
        FileSpray.DFUWUFile({
            request: {
                Wuid: this.Wuid
            }
        }).then(function (response) {
            onFetchXML(response);
        });
    },
    _resubmit(clone, resetWorkflow, callback) {
    },
    resubmit(callback) {
    },
    _action(action) {
        const context = this;
        return FileSpray.DFUWorkunitsAction([this], action, {
        }).then(function (response) {
            context.refresh();
        });
    },
    abort() {
        return FileSpray.AbortDFUWorkunit({
            request: {
                wuid: this.Wuid
            }
        });
    },
    doDelete(callback) {
        return this._action("Delete");
    },
    refresh(full) {
        this.getInfo({
            onAfterSend() {
            }
        });
    },
    getInfo(args) {
        this._assertHasWuid();
        const context = this;
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
    getState() {
        return this.State;
    },
    getProtectedImage() {
        if (this.isProtected) {
            return Utility.getImageURL("locked.png");
        }
        return Utility.getImageURL("unlocked.png");
    },
    getStateIconClass() {
        switch (this.State) {
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
    getStateImage() {
        switch (this.State) {
            case 1:
                return Utility.getImageURL("workunit_warning.png");
            case 2:
                return Utility.getImageURL("workunit_submitted.png");
            case 3:
                return Utility.getImageURL("workunit_running.png");
            case 4:
                return Utility.getImageURL("workunit_failed.png");
            case 5:
                return Utility.getImageURL("workunit_failed.png");
            case 6:
                return Utility.getImageURL("workunit_completed.png");
            case 7:
                return Utility.getImageURL("workunit_running.png");
            case 8:
                return Utility.getImageURL("workunit_aborting.png");
            case 999:
                return Utility.getImageURL("workunit_deleted.png");
        }
        return Utility.getImageURL("workunit.png");
    }
});

export function isInstanceOfWorkunit(obj) {
    return obj && obj.isInstanceOf && obj.isInstanceOf(Workunit);
}

export function Get(wuid, data?) {
    const store = new Store();
    const retVal = store.get(wuid);
    if (data) {
        retVal.updateData(data);
    }
    return retVal;
}

export function CreateWUQueryStore(options) {
    let store = new Store(options);
    store = new Observable(store);
    return store;
}
