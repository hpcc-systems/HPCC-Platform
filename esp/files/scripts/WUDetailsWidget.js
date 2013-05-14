/*##############################################################################
#	HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#	Licensed under the Apache License, Version 2.0 (the "License");
#	you may not use this file except in compliance with the License.
#	You may obtain a copy of the License at
#
#	   http://www.apache.org/licenses/LICENSE-2.0
#
#	Unless required by applicable law or agreed to in writing, software
#	distributed under the License is distributed on an "AS IS" BASIS,
#	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#	See the License for the specific language governing permissions and
#	limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/query",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPWorkunit",
    "hpcc/ECLSourceWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/SampleSelectWidget",
    "hpcc/GraphsWidget",
    "hpcc/ResultsWidget",
    "hpcc/InfoGridWidget",
    "hpcc/LogsWidget",
    "hpcc/TimingPageWidget",
    "hpcc/ECLPlaygroundWidget",

    "dojo/text!../templates/WUDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/TitlePane"
], function (declare, dom, domAttr, domClass, query, Memory, Observable,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                _TabContainerWidget, ESPWorkunit, EclSourceWidget, TargetSelectWidget, SampleSelectWidget, GraphsWidget, ResultsWidget, InfoGridWidget, LogsWidget, TimingPageWidget, ECLPlaygroundWidget,
                template) {
    return declare("WUDetailsWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "WUDetailsWidget",
        summaryWidget: null,
        resultsWidget: null,
        resultsWidgetLoaded: false,
        filesWidget: null,
        filesWidgetLoaded: false,
        timersWidget: null,
        timersWidgetLoaded: false,
        graphsWidget: null,
        graphsWidgetLoaded: false,
        sourceWidget: null,
        sourceWidgetLoaded: false,
        logsWidget: null,
        logsWidgetLoaded: false,
        playgroundWidget: null,
        playgroundWidgetLoaded: false,
        xmlWidget: null,
        xmlWidgetLoaded: false,

        initalized: false,
        wu: null,
        prevState: "",

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.resultsWidget = registry.byId(this.id + "_Results");
            this.filesWidget = registry.byId(this.id + "_Files");
            this.timersWidget = registry.byId(this.id + "_Timers");
            this.graphsWidget = registry.byId(this.id + "_Graphs");
            this.sourceWidget = registry.byId(this.id + "_Source");
            this.logsWidget = registry.byId(this.id + "_Logs");
            this.playgroundWidget = registry.byId(this.id + "_Playground");
            this.xmlWidget = registry.byId(this.id + "_XML");

            this.infoGridWidget = registry.byId(this.id + "InfoContainer");
        },

        startup: function (args) {
            this.inherited(arguments);
            var store = new Memory({
                idProperty: "Id",
                data: []
            });
            this.variablesStore = Observable(store);

            this.variablesGrid = new declare([OnDemandGrid, Keyboard, ColumnResizer, DijitRegistry])({
                allowSelectAll: true,
                columns: {
                    Name: { label: "Name", width: 160 },
                    ColumnType: { label: "Type", width: 100 },
                    Value: { label: "Default Value" }
                },
                store: this.variablesStore
            }, this.id + "VariablesGrid");
            this.variablesGrid.startup();
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
            registry.byId(this.id + "Publish").closeDropDown();
            this.wu.publish(dom.byId(this.id + "Jobname2").value);
        },

        //  Implementation  ---
        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;

            if (params.Wuid) {
                this.summaryWidget.set("title", params.Wuid);

                dom.byId(this.id + "Wuid").innerHTML = params.Wuid;
                this.wu = ESPWorkunit.Get(params.Wuid);
                var data = this.wu.getData();
                for (key in data) {
                    this.updateInput(key, null, data[key]);
                }
                var context = this;
                this.wu.watch(function (name, oldValue, newValue) {
                    context.updateInput(name, oldValue, newValue);
                });
                this.wu.refresh();
            }
            this.infoGridWidget.init(params);
            this.selectChild(this.summaryWidget, true);
        },

        initTab: function () {
            if (!this.wu) {
                return
            }
            var currSel = this.getSelectedChild();
            if (currSel.id == this.resultsWidget.id && !this.resultsWidgetLoaded) {
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
            } else if (currSel.id == this.sourceWidget.id && !this.sourceWidgetLoaded) {
                this.sourceWidgetLoaded = true;
                this.sourceWidget.init({
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
            } else if (name === "variable") {
                registry.byId(context.id + "Variables").set("title", "Variables " + "(" + newValue.length + ")");
                this.variablesStore.setData(newValue);
                this.variablesGrid.refresh();
            } else if (name === "results") {
                this.resultsWidget.set("title", "Outputs " + "(" + newValue.length + ")");
                var tooltip = "";
                for (var i = 0; i < newValue.length; ++i) {
                    if (tooltip != "")
                        tooltip += "\n";
                    tooltip += newValue[i].Name;
                    if (newValue[i].Value)
                        tooltip += " " + newValue[i].Value;
                }
                this.resultsWidget.set("tooltip", tooltip);
            } else if (name === "sourceFiles") {
                this.filesWidget.set("title", "Inputs " + "(" + newValue.length + ")");
                var tooltip = "";
                for (var i = 0; i < newValue.length; ++i) {
                    if (tooltip != "")
                        tooltip += "\n";
                    tooltip += newValue[i].Name;
                }
                this.filesWidget.set("tooltip", tooltip);
            } else if (name === "timers") {
                this.timersWidget.set("title", "Timers " + "(" + newValue.length + ")");
                var tooltip = "";
                for (var i = 0; i < newValue.length; ++i) {
                    if (newValue[i].GraphName)
                        continue;
                    if (newValue[i].Name == "Process")
                        dom.byId(this.id + "Time").innerHTML = newValue[i].Value;
                    if (tooltip != "")
                        tooltip += "\n";
                    tooltip += newValue[i].Name;
                    if (newValue[i].Value)
                        tooltip += " " + newValue[i].Value;
                }
                this.timersWidget.set("tooltip", tooltip);
            } else if (name === "graphs") {
                this.graphsWidget.set("title", "Graphs " + "(" + newValue.length + ")");
                var tooltip = "";
                for (var i = 0; i < newValue.length; ++i) {
                    if (tooltip != "")
                        tooltip += "\n";
                    tooltip += newValue[i].Name;
                    if (newValue[i].Time)
                        tooltip += " " + newValue[i].Time;
                }
                this.graphsWidget.set("tooltip", tooltip);
            } else if (name === "StateID") {
                this.refreshActionState();
            } else if (name === "hasCompleted") {
                this.checkIfComplete();
            }
        },

        refreshActionState: function () {
            registry.byId(this.id + "Save").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Clone").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Delete").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Abort").set("disabled", this.wu.isComplete());
            registry.byId(this.id + "Resubmit").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Restart").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Publish").set("disabled", !this.wu.isComplete());

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

                    onGetResults: function (response) {
                    },

                    onGetSourceFiles: function (response) {
                    },

                    onGetTimers: function (response) {
                    },

                    onGetGraphs: function (response) {
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

                        context.logsWidget.set("title", "Helpers " + "(" + helpersCount + ")");
                    }
                });
            }
        },

        monitorWorkunit: function (response) {
        }
    });
});
