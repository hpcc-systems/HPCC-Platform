/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/dom-form",
    "dojo/dom-attr",
    "dojo/request/iframe",
    "dojo/dom-class",
    "dojo/query",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPWorkunit",
    "hpcc/ESPRequest",
    "hpcc/TargetSelectWidget",
    "hpcc/DelayLoadWidget",
    "hpcc/InfoGridWidget",
    "hpcc/WsWorkunits",

    "dojo/text!../templates/WUDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Form",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/ValidationTextBox",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/form/TextBox",
    "dijit/Dialog",
    "dijit/form/SimpleTextarea",

    "hpcc/TableContainer"
], function (declare, lang, i18n, nlsHPCC, dom, domForm, domAttr, iframe, domClass, query, Memory, Observable,
                registry,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                _TabContainerWidget, ESPWorkunit, ESPRequest, TargetSelectWidget, DelayLoadWidget, InfoGridWidget, WsWorkunits,
                template) {
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
        playgroundWidget: null,
        playgroundWidgetLoaded: false,
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

        prevState: "",

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.resultsWidget = registry.byId(this.id + "_Results");
            this.filesWidget = registry.byId(this.id + "_Files");
            this.timersWidget = registry.byId(this.id + "_Timers");
            this.graphsWidget = registry.byId(this.id + "_Graphs");
            this.logsWidget = registry.byId(this.id + "_Logs");
            this.playgroundWidget = registry.byId(this.id + "_Playground");
            this.xmlWidget = registry.byId(this.id + "_XML");
            this.publishForm = registry.byId(this.id + "PublishForm");
            this.zapDescription = registry.byId(this.id + "ZapDescription");
            this.warnHistory = registry.byId(this.id + "WarnHistory");
            this.warnTimings = registry.byId(this.id + "WarnTimings");

            this.infoGridWidget = registry.byId(this.id + "InfoContainer");
            this.zapDialog = registry.byId(this.id + "ZapDialog");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.zapDialog.destroyRecursive();
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_WUDetails;
        },

        _onCancelDialog: function (){
            this.zapDialog.hide();
        },

        //  Hitched actions  ---
        _onSave: function (event) {
            var protectedCheckbox = registry.byId(this.id + "Protected");
            var context = this;
            this.wu.update({
                Description: dom.byId(context.id + "Description").value,
                Jobname: dom.byId(context.id + "Jobname").value,
                Protected: protectedCheckbox.get("value")
            }, null);
        },
        _onRestore: function (event) {
            this.wu.restore();
        },
        _onRefresh: function (event) {
            this.wu.refresh(true);
        },
        _onClone: function (event) {
            this.wu.clone();
        },
        _onDelete: function (event) {
            this.wu.doDelete();
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
        _onRestart: function (event) {
            this.wu.restart();
        },
        _onPublish: function (event) {
            var allowForeign = registry.byId(this.id + "AllowForeignFiles");
            if (allowForeign.checked == true) {
                allowForeign.value = 1;
            } else {
                allowForeign.value = 0;
            }
            if (this.publishForm.validate()) {
                registry.byId(this.id + "Publish").closeDropDown();
                this.wu.publish(
                    dom.byId(this.id + "Jobname2").value,
                    dom.byId(this.id + "RemoteDali").value,
                    dom.byId(this.id + "SourceProcess").value,
                    registry.byId(this.id + "Priority").value,
                    dom.byId(this.id + "Comment").value,
                    allowForeign.value
                );
            }
        },

        onZapReport: function (event) {
            var context = this;
            WsWorkunits.WUGetZAPInfo({
                request: {
                    WUID: this.wu.Wuid
                }
            }).then(function (response) {
                context.zapDialog.show();
                if (lang.exists("WUGetZAPInfoResponse", response)) {
                    context.updateInput("ZapWUID", null, response.WUGetZAPInfoResponse.WUID);
                    context.updateInput("BuildVersion", null, response.WUGetZAPInfoResponse.BuildVersion);
                    context.updateInput("ESPIPAddress", null, response.WUGetZAPInfoResponse.ESPIPAddress);
                    context.updateInput("ThorIPAddress", null, response.WUGetZAPInfoResponse.ThorIPAddress);

                    context.buildVersion = response.WUGetZAPInfoResponse.BuildVersion;
                    context.espIPAddress = response.WUGetZAPInfoResponse.ESPIPAddress;
                    context.thorIPAddress = response.WUGetZAPInfoResponse.ThorIPAddress;
                }
            });
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.Wuid) {
                this.summaryWidget.set("title", params.Wuid);

                dom.byId(this.id + "Wuid").innerHTML = params.Wuid;
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
            }
            this.infoGridWidget.init(params);
        },

        initTab: function () {
            if (!this.wu) {
                return
            }
            var currSel = this.getSelectedChild();
            if (currSel.id == this.widget._Variables.id && !this.widget._Variables.__hpcc_initalized) {
                this.widget._Variables.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id == this.widget._Workflows.id && !this.widget._Workflows.__hpcc_initalized) {
                this.widget._Workflows.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id == this.resultsWidget.id && !this.resultsWidgetLoaded) {
                this.resultsWidgetLoaded = true;
                this.resultsWidget.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id == this.filesWidget.id && !this.filesWidgetLoaded) {
                this.filesWidgetLoaded = true;
                this.filesWidget.init({
                    Wuid: this.wu.Wuid,
                    SourceFiles: true
                });
            } else if (currSel.id == this.timersWidget.id && !this.timersWidgetLoaded) {
                this.timersWidgetLoaded = true;
                this.timersWidget.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id == this.graphsWidget.id && !this.graphsWidgetLoaded) {
                this.graphsWidgetLoaded = true;
                this.graphsWidget.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id == this.widget._Queries.id && !this.widget._Queries.__hpcc_initalized) {
                this.widget._Queries.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id == this.widget._Resources.id && !this.resourcesWidgetLoaded) {
                this.resourcesWidgetLoaded = true;
                this.widget._Resources.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id == this.logsWidget.id && !this.logsWidgetLoaded) {
                this.logsWidgetLoaded = true;
                this.logsWidget.init({
                    Wuid: this.wu.Wuid
                });
            } else if (currSel.id == this.playgroundWidget.id && !this.playgroundWidgetLoaded) {
                this.playgroundWidgetLoaded = true;
                this.playgroundWidget.init({
                    Wuid: this.wu.Wuid,
                    Target: this.wu.Cluster
                });
            } else if (currSel.id == this.xmlWidget.id && !this.xmlWidgetLoaded) {
                this.xmlWidgetLoaded = true;
                this.xmlWidget.init({
                    Wuid: this.wu.Wuid
                });
            }
        },

        resetPage: function () {
        },

        objectToText: function (obj) {
            var text = ""
            for (var key in obj) {
                text += "<tr><td>" + key + ":</td>";
                if (typeof obj[key] == "object") {
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
                            domAttr.set(this.id + name, "innerHTML", newValue);
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
            } else if (name === "WorkflowCount" && newValue) {
                this.widget._Workflows.set("title", this.i18n.Workflows + " (" + newValue + ")");
                this.setDisabled(this.widget._Workflows.id, false);
            } else if (name === "variables") {
                var tooltip = "";
                for (var key in newValue) {
                    if (tooltip != "")
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
                    if (tooltip != "")
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
                    if (tooltip != "")
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
                    if (tooltip != "")
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
                    if (tooltip != "")
                        tooltip += "\n";
                    tooltip += newValue[i].Name;
                    if (newValue[i].Time)
                        tooltip += " " + newValue[i].Time;
                }
                this.graphsWidget.set("tooltip", tooltip);
                this.setDisabled(this.graphsWidget.id, false);
            } else if (name === "ResourceURLCount" && newValue) {
                this.widget._Resources.set("title", this.i18n.Resources + " (" + newValue + ")");
                this.setDisabled(this.widget._Resources.id, false);
            } else if (name === "Archived") {
                this.refreshActionState();
            } else if (name === "StateID") {
                this.refreshActionState();
            } else if (name === "ActionEx") {
                this.refreshActionState();
            } else if (name === "hasCompleted") {
                this.checkIfComplete();
            }
            if (name === "changedCount" && newValue > 0) {
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
            registry.byId(this.id + "Save").set("disabled", isArchived || !this.wu.isComplete());
            registry.byId(this.id + "Delete").set("disabled", isArchived || !this.wu.isComplete());
            registry.byId(this.id + "Restore").set("disabled", !isArchived);
            registry.byId(this.id + "SetToFailed").set("disabled", isArchived || this.wu.isComplete());
            registry.byId(this.id + "Abort").set("disabled", isArchived || this.wu.isComplete());
            registry.byId(this.id + "Clone").set("disabled", isArchived || !this.wu.isComplete());
            registry.byId(this.id + "Resubmit").set("disabled", isArchived || !this.wu.isComplete());
            registry.byId(this.id + "Restart").set("disabled", isArchived || !this.wu.isComplete());
            registry.byId(this.id + "Publish").set("disabled", isArchived || !this.wu.isComplete());

            registry.byId(this.id + "Jobname").set("readOnly", !this.wu.isComplete());
            registry.byId(this.id + "Description").set("readOnly", !this.wu.isComplete());
            registry.byId(this.id + "Protected").set("readOnly", !this.wu.isComplete());

            this.summaryWidget.set("iconClass", this.wu.getStateIconClass());
            domClass.remove(this.id + "StateIdImage");
            domClass.add(this.id + "StateIdImage", this.wu.getStateIconClass());
        },

        checkIfComplete: function() {
            var context = this;
            if (this.wu.isComplete()) {
                this.wu.getInfo({
                    onGetVariables: function (response) {
                    },

                    onAfterSend: function (response) {
                        var helpersCount = 0;
                        if (response.Helpers && response.Helpers.ECLHelpFile) {
                            helpersCount += response.Helpers.ECLHelpFile.length;
                        }
                        if (response.ThorLogList && response.ThorLogList.ThorLogInfo) {
                            helpersCount += response.ThorLogList.ThorLogInfo.length;
                        }
                        if (response.HasArchiveQuery) {
                            helpersCount += 1;
                        }

                        context.logsWidget.set("title", context.i18n.Helpers + " (" + helpersCount + ")");
                        context.setDisabled(context.logsWidget.id, false);
                    }
                });
            }
        },

        monitorWorkunit: function (response) {
        }
    });
});