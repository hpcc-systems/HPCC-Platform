define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/topic",

    "dijit/registry",

    "src/Clippy",
    "src/react/index",

    "hpcc/_TabContainerWidget",
    "src/ESPWorkunit",
    "src/ESPActivity",
    "src/ESPRequest",
    "src/WsWorkunits",
    "src/Session",

    "dojo/text!../templates/WUDetailsWidget.html",

    "hpcc/DelayLoadWidget",
    "hpcc/InfoGridWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Form",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/NumberTextBox",
    "dijit/form/ValidationTextBox",
    "dijit/form/Select",
    "dijit/form/ToggleButton",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/form/TextBox",
    "dijit/Dialog",
    "dijit/form/SimpleTextarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",

    "hpcc/TableContainer"
], function (declare, lang, nlsHPCCMod, dom, domAttr, domClass, topic,
    registry,
    Clippy,
    srcReact,
    _TabContainerWidget, ESPWorkunit, ESPActivity, ESPRequest, WsWorkunits, Session,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("WUDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "WUDetailsWidget",
        i18n: nlsHPCC,

        summaryWidget: null,
        resultsWidget: null,
        resultsWidgetLoaded: false,
        filesWidget: null,
        filesWidgetLoaded: false,
        timersWidget: null,
        timersWidgetLoaded: false,
        graphsWidget: null,
        graphsWidgetLoaded: false,
        logsWidget: null,
        logsWidgetLoaded: false,
        eclWidget: null,
        eclWidgetLoaded: false,
        xmlWidget: null,
        xmlWidgetLoaded: false,
        publishForm: null,

        wu: null,
        buildVersion: null,
        espIPAddress: null,
        thorIPAddress: null,
        zapDescription: null,
        warnHistory: null,
        warnTimings: null,
        logDate: null,
        clusterGroup: null,
        maxSlaves: null,

        logAccessorMessage: "",

        prevState: "",

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.resultsWidget = registry.byId(this.id + "_Results");
            this.filesWidget = registry.byId(this.id + "_Files");
            this.timersWidget = registry.byId(this.id + "_Timers");
            this.graphsWidget = registry.byId(this.id + "_Graphs");
            this.logsWidget = registry.byId(this.id + "_Logs");
            this.eclWidget = registry.byId(this.id + "_ECL");
            this.xmlWidget = registry.byId(this.id + "_XML");
            this.publishForm = registry.byId(this.id + "PublishForm");
            this.zapDescription = registry.byId(this.id + "ZapDescription");
            this.zapForm = registry.byId(this.id + "ZapForm");
            this.warnHistory = registry.byId(this.id + "WarnHistory");
            this.warnTimings = registry.byId(this.id + "WarnTimings");
            this.clusters = registry.byId(this.id + "Clusters");
            this.allowedClusters = registry.byId(this.id + "AllowedClusters");
            this.thorProcess = registry.byId(this.id + "ThorProcess");
            this.slaveNumber = registry.byId(this.id + "SlaveNumber");
            this.fileFormat = registry.byId(this.id + "FileFormat");
            this.slaveLogs = registry.byId(this.id + "SlaveLogs");
            this.includeSlaveLogsCheckbox = registry.byId(this.id + "IncludeSlaveLogsCheckbox");
            this.logsForm = registry.byId(this.id + "LogsForm");
            this.allowOnlyNumber = registry.byId(this.id + "AllowOnlyNumber");
            this.emailCheckbox = registry.byId(this.id + "EmailCheckbox");
            this.emailTo = registry.byId(this.id + "EmailTo");
            this.emailFrom = registry.byId(this.id + "EmailFrom");
            this.emailSubject = registry.byId(this.id + "EmailSubject");
            this.emailBody = registry.byId(this.id + "EmailBody");

            //Zap LogFilters
            this.logFilterStartDateTime = dom.byId(this.id + "StartDateTime");
            this.logFilterStartDate = registry.byId(this.id + "StartDate");
            this.logFilterStartTime = registry.byId(this.id + "StartTime");
            this.logFilterEndDateTime = dom.byId(this.id + "EndDateTime");
            this.logFilterEndDate = registry.byId(this.id + "EndDate");
            this.logFilterEndTime = registry.byId(this.id + "EndTime");
            this.logFilterRelativeTimeRangeBuffer = registry.byId(this.id + "RelativeTimeRangeBuffer");

            this.protected = registry.byId(this.id + "Protected");
            this.infoGridWidget = registry.byId(this.id + "InfoContainer");
            this.zapDialog = registry.byId(this.id + "ZapDialog");

            Clippy.attach(this.id + "ClippyButton");
            Clippy.attach(this.id + "ShareWUClippy");

        },

        startup: function (args) {
            this.inherited(arguments);
            this.__globalActivities = ESPActivity.Get();
        },

        destroy: function (args) {
            srcReact.unrender(this.statusNode);
            this.zapDialog.destroyRecursive();
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_WUDetails;
        },

        _onCancelDialog: function () {
            this.zapDialog.hide();
            this.checkThorLogStatus();
        },

        formatLogFilterDateTime: function (dateField, timeField, dateTimeField) {
            if (dateField.value.toString() !== "Invalid Date") {
                const d = new Date(dateField.value);
                const month = d.getMonth() + 1;
                const day = d.getDate();
                const date = `${d.getFullYear()}-${(month < 9 ? "0" : "") + month}-${(day < 9 ? "0" : "") + day}`;
                const time = timeField.value.toString().replace(/.*1970\s(\S+).*/, "$1");
                dateTimeField.value = `${date}T${time}.000Z`;
            }
        },

        _onSubmitDialog: function () {
            var context = this;
            var includeSlaveLogsCheckbox = this.includeSlaveLogsCheckbox.get("checked");
            if (this.logFilterRelativeTimeRangeBuffer.value !== "") {
                this.logFilterEndDate.required = "";
                this.logFilterStartDate.required = "";
            }
            if (this.zapForm.validate()) {
                //WUCreateAndDownloadZAPInfo is not a webservice so relying on form to submit.
                //Server treats "on" and '' as the same thing.
                this.includeSlaveLogsCheckbox.set("value", includeSlaveLogsCheckbox ? "on" : "off");

                // Log Filters
                this.formatLogFilterDateTime(this.logFilterStartDate, this.logFilterStartTime, this.logFilterStartDateTime);
                this.formatLogFilterDateTime(this.logFilterEndDate, this.logFilterEndTime, this.logFilterEndDateTime);

                this.zapForm.set("action", "/WsWorkunits/WUCreateAndDownloadZAPInfo");

                this.zapDialog.hide();
                this.checkThorLogStatus();
                if (this.logAccessorMessage !== "") {
                    topic.publish("hpcc/brToaster", {
                        Severity: "Warning",
                        Source: "WsWorkunits/WUCreateAndDownloadZAPInfo",
                        Exceptions: [{
                            Message: context.logAccessorMessage,
                        }]
                    });
                }
            }
        },

        //  Hitched actions  ---
        _onSave: function (event) {
            var protectedCheckbox = registry.byId(this.id + "Protected");
            var context = this;
            this.wu.update({
                State: dom.byId(this.id + "State").innerHTML,
                Jobname: dom.byId(context.id + "Jobname").value,
                Description: dom.byId(context.id + "Description").value,
                Protected: protectedCheckbox.get("value"),
                Scope: dom.byId(context.id + "Scope").value,
                ClusterSelection: this.allowedClusters.get("value")
            }, null);
        },
        _onRestore: function (event) {
            this.wu.restore();
            this.checkThorLogStatus();
        },
        _onAutoRefresh: function (event) {
            var autoRefresh = this.widget.AutoRefresh.get("checked");
            this.wu.disableMonitor(!autoRefresh);
        },
        _onRefresh: function (event) {
            this.wu.refresh(true);
            this.wu.fetchServiceNames();
        },
        _onClone: function (event) {
            this.wu.clone();
        },
        _onDelete: function (event) {
            if (confirm(this.i18n.YouAreAboutToDeleteThisWorkunit)) {
                this.wu.doDelete();
            }
        },
        _onResubmit: function (event) {
            this.wu.resubmit();
        },
        _onSetToFailed: function (event) {
            this.wu.setToFailed();
        },
        _onAbort: function (event) {
            this.wu.abort();
        },
        _onRecover: function (event) {
            this.wu.recover();
        },
        _onDeschedule: function (event) {
            this.wu.doDeschedule();
        },
        _onReschedule: function (event) {
            this.wu.doReschedule();
        },
        _onPublish: function (event) {
            var allowForeign = registry.byId(this.id + "AllowForeignFiles");
            if (allowForeign.checked === true) {
                allowForeign.value = 1;
            } else {
                allowForeign.value = 0;
            }
            var updateSupers = registry.byId(this.id + "UpdateSuperFiles");
            if (updateSupers.checked === true) {
                updateSupers.value = 1;
            } else {
                updateSupers.value = 0;
            }
            if (this.publishForm.validate()) {
                registry.byId(this.id + "Publish").closeDropDown();
                this.wu.publish(
                    dom.byId(this.id + "Jobname2").value,
                    dom.byId(this.id + "RemoteDali").value,
                    dom.byId(this.id + "RemoteStorage").value,
                    dom.byId(this.id + "SourceProcess").value,
                    registry.byId(this.id + "Priority").value,
                    dom.byId(this.id + "Comment").value,
                    allowForeign.value,
                    updateSupers.value
                );
            }
        },
        _onActiveGraph: function () {
            this.graphsWidgetLoaded = true;
            var context = this;
            this.graphsWidget.init({
                Wuid: this.wu.Wuid
            }).then(function (w) {
                w.openGraph(context.wu.GraphName, "sg" + context.wu.GID);
            });
            this.selectChild(this.graphsWidget.id);
        },

        _onZapReport: function (event) {
            var context = this;
            WsWorkunits.WUGetZAPInfo({
                request: {
                    WUID: this.wu.Wuid
                }
            }).then(function (response) {
                context.zapDialog.show();
                context.emailCheckbox.on("change", function (evt) {
                    if (context.emailCheckbox.get("checked")) {
                        context.emailSubject.set("required", true);
                    } else {
                        context.emailSubject.set("required", false);
                    }
                });
                if (lang.exists("WUGetZAPInfoResponse", response)) {
                    if (response.WUGetZAPInfoResponse.EmailTo) {
                        context.emailCheckbox.set("disabled", false);
                        context.emailTo.set("disabled", false);
                        context.emailFrom.set("disabled", false);
                        context.emailSubject.set("disabled", false);
                        context.emailBody.set("disabled", false);
                    } else {
                        context.emailCheckbox.set("disabled", true);
                        context.emailTo && context.emailTo.set("disabled", true);
                        context.emailFrom && context.emailFrom.set("disabled", true);
                        context.emailSubject && context.emailSubject.set("disabled", true);
                        context.emailBody && context.emailBody.set("disabled", true);
                    }
                    context.updateInput("ZapWUID", null, response.WUGetZAPInfoResponse.WUID);
                    context.updateInput("BuildVersion", null, response.WUGetZAPInfoResponse.BuildVersion);
                    context.updateInput("ESPIPAddress", null, response.WUGetZAPInfoResponse.ESPIPAddress);
                    context.updateInput("ThorIPAddress", null, response.WUGetZAPInfoResponse.ThorIPAddress);
                    context.updateInput("EmailTo", null, response.WUGetZAPInfoResponse.EmailTo);
                    context.updateInput("EmailFrom", null, response.WUGetZAPInfoResponse.EmailFrom);

                    context.buildVersion = response.WUGetZAPInfoResponse.BuildVersion;
                    context.espIPAddress = response.WUGetZAPInfoResponse.ESPIPAddress;
                    context.thorIPAddress = response.WUGetZAPInfoResponse.ThorIPAddress;
                    context.emailTo = response.WUGetZAPInfoResponse.EmailTo;
                    context.emailFrom = response.WUGetZAPInfoResponse.EmailFrom;

                    context.logAccessorMessage = response.WUGetZAPInfoResponse?.Message ?? "";
                }
            });
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.graphLink = dom.byId(this.id + "ActiveGraph");

            domAttr.set(this.id + "ShareWUClippy", "data-clipboard-text", this.getURL());

            if (params.Wuid) {
                this.summaryWidget.set("title", params.Wuid);

                dom.byId(this.id + "Wuid").textContent = params.Wuid;
                this.wu = ESPWorkunit.Get(params.Wuid);
                var data = this.wu.getData();
                for (var key in data) {
                    this.updateInput(key, null, data[key]);
                }
                var context = this;
                this.wu.watch(function (name, oldValue, newValue) {
                    context.updateInput(name, oldValue, newValue);
                });
                this.wu.refresh();
                this.wu.fetchServiceNames();
            }

            this.infoGridWidget.init(params);
            this.checkIfClustersAllowed();
            this.checkThorLogStatus();
            this.statusNode = dom.byId(this.id + "WUStatus");
            srcReact.render(srcReact.WUStatus, { wuid: params.Wuid }, this.statusNode);

            this.protected.on("click", function (evt) {
                context._onSave();
            });

            this.refreshActionState();
        },

        initTab: function () {
            if (!this.wu) {
                return;
            }
            var currSel = this.getSelectedChild();
            if (currSel.id === this.widget._Variables.id && !this.widget._Variables.__hpcc_initalized) {
                this.widget._Variables.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id === this.widget._Workflows.id && !this.widget._Workflows.__hpcc_initalized) {
                this.widget._Workflows.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id === this.resultsWidget.id && !this.resultsWidgetLoaded) {
                this.resultsWidgetLoaded = true;
                this.resultsWidget.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id === this.filesWidget.id && !this.filesWidgetLoaded) {
                this.filesWidgetLoaded = true;
                this.filesWidget.init({
                    Wuid: this.wu.Wuid,
                    SourceFiles: true
                });
            } else if (currSel.id === this.timersWidget.id && !this.timersWidgetLoaded) {
                this.timersWidgetLoaded = true;
                this.timersWidget.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id === this.graphsWidget.id && !this.graphsWidgetLoaded) {
                this.graphsWidgetLoaded = true;
                this.graphsWidget.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id === this.widget._Queries.id && !this.widget._Queries.__hpcc_initalized) {
                this.widget._Queries.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id === this.widget._Resources.id && !this.resourcesWidgetLoaded) {
                this.resourcesWidgetLoaded = true;
                this.widget._Resources.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id === this.logsWidget.id && !this.logsWidgetLoaded) {
                this.logsWidgetLoaded = true;
                this.logsWidget.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id === this.eclWidget.id && !this.eclWidgetLoaded) {
                this.eclWidgetLoaded = true;
                this.eclWidget.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id === this.xmlWidget.id && !this.xmlWidgetLoaded) {
                this.xmlWidgetLoaded = true;
                this.xmlWidget.init({
                    Wuid: this.wu.Wuid
                });
            }
        },

        resetPage: function () {
        },

        objectToText: function (obj) {
            var text = "";
            for (var key in obj) {
                text += "<tr><td>" + key + ":</td>";
                if (typeof obj[key] === "object") {
                    text += "[<br/>";
                    for (var i = 0; i < obj[key].length; ++i) {
                        text += this.objectToText(obj[key][i]);
                    }
                    text += "<br/>]<br/>";
                } else {
                    text += "<td>" + obj[key] + "</td></tr>";

                }
            }
            return text;
        },

        checkIfClustersAllowed: function () {
            var context = this;
            WsWorkunits.WUInfo({
                request: {
                    Wuid: this.wu.Wuid
                }
            }).then(function (response) {
                if (lang.exists("WUInfoResponse.Workunit.AllowedClusters.AllowedCluster", response)) {
                    var targetData = response.WUInfoResponse.Workunit.AllowedClusters.AllowedCluster;
                    if (targetData.length > 1) {
                        context.allowedClusters.options.push({
                            label: "&nbsp;",
                            value: ""
                        });
                        for (var i = 0; i < targetData.length; ++i) {
                            context.allowedClusters.options.push({
                                label: targetData[i],
                                value: targetData[i]
                            });
                        }
                        context.allowedClusters.set("value", "");
                        domClass.add(context.id + "Cluster", "hidden");
                    } else {
                        domClass.add(context.id + "AllowedClusters", "hidden");
                    }
                }
            });
        },

        checkThorLogStatus: function () {
            var context = this;
            WsWorkunits.WUInfo({
                request: {
                    Wuid: this.wu.Wuid
                }
            }).then(function (response) {
                if (lang.exists("WUInfoResponse.Workunit.ThorLogList.ThorLogInfo", response)) {
                    context.maxSlaves = response.WUInfoResponse.Workunit.ThorLogList.ThorLogInfo[0].NumberSlaves;
                    context.slaveNumber.set("maxLength", context.maxSlaves);
                    dom.byId("SlavesMaxNumber").innerHTML = context.i18n.NumberofSlaves + " " + response.WUInfoResponse.Workunit.ThorLogList.ThorLogInfo[0].NumberSlaves;
                    context.logDate = response.WUInfoResponse.Workunit.ThorLogList.ThorLogInfo[0].LogDate;
                    context.clusterGroup = response.WUInfoResponse.Workunit.ThorLogList.ThorLogInfo[0].ProcessName;
                    context.slaveLogs.set("disabled", false);
                    context.includeSlaveLogsCheckbox.set("disabled", false);
                    context.includeSlaveLogsCheckbox.set("checked", false);
                    context.emailCheckbox.set("checked", false);
                    var targetData = response.WUInfoResponse.Workunit.ThorLogList.ThorLogInfo;
                    for (var i = 0; i < targetData.length; ++i) {
                        context.thorProcess.options.push({
                            label: targetData[i].ProcessName,
                            value: targetData[i].ProcessName
                        });
                    }
                    context.thorProcess.set("value", targetData[0].ProcessName);
                } else {
                    context.slaveLogs.set("disabled", true);
                    context.includeSlaveLogsCheckbox.set("disabled", true);
                }
            });
        },

        _getURL: function (completeURL) {
            return ESPRequest.getBaseURL() + completeURL;
        },

        _getDownload: function () {
            var context = this;
            if (this.logsForm.validate() && context.slaveNumber.get("value") <= context.maxSlaves) {
                dom.byId("AllowOnlyNumber").innerHTML = "";
                var buildURL = "/WUFile?" + "Wuid=" + this.wu.Wuid + "&Type=ThorSlaveLog" + "&Process=" + this.thorProcess.get("value") + "&ClusterGroup=" + this.clusterGroup + "&LogDate=" + this.logDate + "&SlaveNumber=" + this.slaveNumber.get("value") + "&Option=" + this.fileFormat.get("value");
                window.open(this._getURL(buildURL));
            } else if (context.slaveNumber.get("value") > context.maxSlaves) {
                dom.byId("AllowOnlyNumber").innerHTML = context.i18n.PleaseEnterANumber + context.maxSlaves;
            }
        },

        updateInput: function (name, oldValue, newValue) {
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            } else {
                var domElem = dom.byId(this.id + name);
                if (domElem) {
                    switch (domElem.tagName) {
                        case "SPAN":
                        case "DIV":
                            dom.byId(this.id + name).textContent = newValue;
                            break;
                        case "INPUT":
                        case "TEXTAREA":
                            domAttr.set(this.id + name, "value", newValue);
                            break;
                        default:
                            alert(domElem.tagName);
                    }
                }
            }
            if (name === "Protected") {
                dom.byId(this.id + "ProtectedImage").src = this.wu.getProtectedImage();
            } else if (name === "Jobname") {
                this.updateInput("Jobname2", oldValue, newValue);
                this.summaryWidget.set("tooltip", newValue);
            } else if (name === "WorkflowCount" && newValue) {
                this.widget._Workflows.set("title", this.i18n.Workflows + " (" + newValue + ")");
                this.setDisabled(this.widget._Workflows.id, false);
            } else if (name === "variables") {
                var tooltip = "";
                for (var key in newValue) {
                    if (tooltip !== "")
                        tooltip += "\n";
                    tooltip += newValue[key].Name;
                    if (newValue[key].Value)
                        tooltip += " " + newValue[key].Value;
                }
                this.widget._Variables.set("tooltip", tooltip);
            } else if (name === "ResultCount" && newValue) {
                this.resultsWidget.set("title", this.i18n.Outputs + " (" + newValue + ")");
                this.setDisabled(this.resultsWidget.id, false);
            } else if (name === "results") {
                this.resultsWidget.set("title", this.i18n.Outputs + " (" + newValue.length + ")");
                var tooltip = "";
                for (var key in newValue) {
                    if (tooltip !== "")
                        tooltip += "\n";
                    tooltip += newValue[key].Name;
                    if (newValue[key].Value)
                        tooltip += " " + newValue[key].Value;
                }
                this.resultsWidget.set("tooltip", tooltip);
                this.setDisabled(this.resultsWidget.id, false);
            } else if (name === "SourceFileCount" && newValue) {
                this.filesWidget.set("title", this.i18n.Inputs + " (" + newValue + ")");
                this.setDisabled(this.filesWidget.id, false);
            } else if (name === "sourceFiles") {
                this.filesWidget.set("title", this.i18n.Inputs + " (" + newValue.length + ")");
                var tooltip = "";
                for (var i = 0; i < newValue.length; ++i) {
                    if (tooltip !== "")
                        tooltip += "\n";
                    tooltip += newValue[i].Name;
                }
                this.filesWidget.set("tooltip", tooltip);
                this.setDisabled(this.filesWidget.id, false);
            } else if (name === "TimerCount" && newValue) {
                this.timersWidget.set("title", this.i18n.Timers + " (" + newValue + ")");
                this.setDisabled(this.timersWidget.id, false);
            } else if (name === "timers") {
                this.timersWidget.set("title", this.i18n.Timers + " (" + newValue.length + ")");
                var tooltip = "";
                for (var i = 0; i < newValue.length; ++i) {
                    if (newValue[i].GraphName)
                        continue;
                    if (tooltip !== "")
                        tooltip += "\n";
                    tooltip += newValue[i].Name;
                    if (newValue[i].Value)
                        tooltip += " " + newValue[i].Value;
                }
                this.timersWidget.set("tooltip", tooltip);
                this.setDisabled(this.timersWidget.id, false);
            } else if (name === "GraphCount" && newValue) {
                this.graphsWidget.set("title", this.i18n.Graphs + " (" + newValue + ")");
                this.setDisabled(this.graphsWidget.id, false);
            } else if (name === "graphs") {
                this.graphsWidget.set("title", this.i18n.Graphs + " (" + newValue.length + ")");
                var tooltip = "";
                for (var i = 0; i < newValue.length; ++i) {
                    if (tooltip !== "")
                        tooltip += "\n";
                    tooltip += newValue[i].Name;
                    if (newValue[i].Time)
                        tooltip += " " + newValue[i].Time;
                }
                this.graphsWidget.set("tooltip", tooltip);
                this.setDisabled(this.graphsWidget.id, false);
            } else if (name === "ResourceURLCount" && newValue && newValue > 1) {
                this.widget._Resources.set("title", this.i18n.Resources + " (" + (newValue - 1) + ")");
                this.setDisabled(this.widget._Resources.id, false);
            } else if (name === "eclwatchHelpersCount" && newValue) {
                this.logsWidget.set("title", this.i18n.Helpers + " (" + newValue + ")");
                this.setDisabled(this.logsWidget.id, false);
            } else if (name === "Archived") {
                this.refreshActionState();
            } else if (name === "StateID") {
                this.refreshActionState();
            } else if (name === "GraphName" || name === "GID") {
                this.graphLink.innerText = this.wu.GraphName && this.wu.GID ? this.wu.GraphName + " - " + this.wu.GID : "";
            } else if (name === "ActionEx") {
                this.refreshActionState();
            } else if (name === "EventSchedule") {
                this.refreshActionState();
            } else if (name === "hasCompleted") {
                this.checkIfComplete();
            } else if (name === "Scope" && newValue) {
                domClass.remove("scopeOptional", "hidden");
                domClass.add("scopeOptional", "show");
            } else if (name === "ServiceNames" && newValue && newValue.Item) {
                var domElem = registry.byId(this.id + "ServiceNamesCustom");
                domElem.set("value", newValue.Item.join("\n"));
            } else if (name === "CompileCost") {
                this.updateInput("FormattedCompileCost", oldValue, Session.formatCost(newValue));
            } else if (name === "ExecuteCost") {
                this.updateInput("FormattedExecuteCost", oldValue, Session.formatCost(newValue));
            } else if (name === "FileAccessCost") {
                this.updateInput("FormattedFileAccessCost", oldValue, Session.formatCost(newValue));
            }
            if (name === "__hpcc_changedCount" && newValue > 0) {
                var getInt = function (item) {
                    if (item)
                        return item;
                    return 0;
                };
                this.widget._Variables.set("title", this.i18n.Variables + " (" + (getInt(this.wu.VariableCount) + getInt(this.wu.ApplicationValueCount) + getInt(this.wu.DebugValueCount)) + ")");
                this.setDisabled(this.widget._Variables.id, false);
            }
        },

        refreshActionState: function () {
            var isArchived = this.wu.get("Archived");
            this.setDisabled(this.id + "AutoRefresh", isArchived || this.wu.isComplete(), "iconAutoRefresh", "iconAutoRefreshDisabled");
            registry.byId(this.id + "Save").set("disabled", isArchived || (!this.wu.isComplete() && !this.wu.isBlocked()) || this.wu.isDeleted());
            registry.byId(this.id + "Delete").set("disabled", isArchived || !this.wu.isComplete() || this.wu.isDeleted());
            registry.byId(this.id + "Restore").set("disabled", !isArchived);
            registry.byId(this.id + "SetToFailed").set("disabled", isArchived || this.wu.isComplete() || this.wu.isDeleted());
            registry.byId(this.id + "Abort").set("disabled", isArchived || this.wu.isComplete() || this.wu.isDeleted());
            registry.byId(this.id + "Clone").set("disabled", isArchived || !this.wu.isComplete() || this.wu.isDeleted());
            registry.byId(this.id + "Resubmit").set("disabled", isArchived || !this.wu.isComplete() || this.wu.isDeleted());
            registry.byId(this.id + "Recover").set("disabled", isArchived || !this.wu.isComplete() || this.wu.isDeleted());
            registry.byId(this.id + "Publish").set("disabled", isArchived || !this.wu.isComplete() || this.wu.isDeleted());
            registry.byId(this.id + "ZapReport").set("disabled", isArchived || this.wu.isDeleted());
            registry.byId(this.id + "Reschedule").set("disabled", !this.wu.isAbleToReschedule());
            registry.byId(this.id + "Deschedule").set("disabled", !this.wu.isAbleToDeschedule());

            registry.byId(this.id + "Jobname").set("readOnly", !this.wu.isComplete() || this.wu.isDeleted());
            registry.byId(this.id + "Description").set("readOnly", !this.wu.isComplete() || this.wu.isDeleted());
            registry.byId(this.id + "Protected").set("readOnly", !this.wu.isComplete() || this.wu.isDeleted());

            this.summaryWidget.set("iconClass", this.wu.getStateIconClass());
            domClass.remove(this.id + "StateIdImage");
            domClass.add(this.id + "StateIdImage", this.wu.getStateIconClass());
        },

        checkIfComplete: function () {
            var context = this;
            if (this.wu.isComplete()) {
                this.wu.getInfo({
                    onGetVariables: function (response) {
                    }
                });
            }
        },

        monitorWorkunit: function (response) {
        }
    });
});
