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
    "dojo/dom-class",
    "dojo/store/Memory",
    "dojo/data/ObjectStore",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "hpcc/ECLSourceWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/SampleSelectWidget",
    "hpcc/GraphsWidget",
    "hpcc/ResultsWidget",
    "hpcc/InfoGridWidget",
    "hpcc/LogsWidget",
    "hpcc/ESPWorkunit",

    "dojo/text!../templates/WUDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/TitlePane"
], function (declare, dom, domClass, Memory, ObjectStore,
                _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                EclSourceWidget, TargetSelectWidget, SampleSelectWidget, GraphsWidget, ResultsWidget, InfoGridWidget, LogsWidget, Workunit,
                template) {
    return declare("WUDetailsWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "WUDetailsWidget",
        borderContainer: null,
        tabContainer: null,
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
        legacyPane: null,
        legacyPaneLoaded: false,

        wu: null,
        prevState: "",

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.tabContainer = registry.byId(this.id + "TabContainer");
            this.resultsWidget = registry.byId(this.id + "Results");
            this.filesWidget = registry.byId(this.id + "Files");
            this.timersWidget = registry.byId(this.id + "Timers");
            this.graphsWidget = registry.byId(this.id + "Graphs");
            this.sourceWidget = registry.byId(this.id + "Source");
            this.logsWidget = registry.byId(this.id + "Logs");
            this.playgroundWidget = registry.byId(this.id + "Playground");
            this.xmlWidget = registry.byId(this.id + "XML");
            this.infoGridWidget = registry.byId(this.id + "InfoContainer");
            this.legacyPane = registry.byId(this.id + "Legacy");

            var context = this;
            this.tabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                if (nval.id == context.id + "Results" && !context.resultsWidgetLoaded) {
                    context.resultsWidgetLoaded = true;
                    context.resultsWidget.init({
                        Wuid: context.wu.Wuid
                    });
                } else if (nval.id == context.id + "Files" && !context.filesWidgetLoaded) {
                    context.filesWidgetLoaded = true;
                    context.filesWidget.init({
                        Wuid: context.wu.Wuid,
                        SourceFiles: true
                    });
                } else if (nval.id == context.id + "Timers" && !context.timersWidgetLoaded) {
                    context.timersWidgetLoaded = true;
                    context.timersWidget.init({
                        Wuid: context.wu.Wuid
                    });
                } else if (nval.id == context.id + "Graphs" && !context.graphsWidgetLoaded) {
                    context.graphsWidgetLoaded = true;
                    context.graphsWidget.init({
                        Wuid: context.wu.Wuid
                    });
                } else if (nval.id == context.id + "Source" && !context.sourceWidgetLoaded) {
                    context.sourceWidgetLoaded = true;
                    context.sourceWidget.init({
                        Wuid: context.wu.Wuid
                    });
                } else if (nval.id == context.id + "Logs" && !context.logsWidgetLoaded) {
                    context.logsWidgetLoaded = true;
                    context.logsWidget.init({
                        Wuid: context.wu.Wuid
                    });
                } else if (nval.id == context.id + "Playground" && !context.playgroundWidgetLoaded) {
                    context.playgroundWidgetLoaded = true;
                    context.playgroundWidget.init({
                        Wuid: context.wu.Wuid,
                        Target: context.wu.WUInfoResponse.Cluster
                    });
                } else if (nval.id == context.id + "XML" && !context.xmlWidgetLoaded) {
                    context.xmlWidgetLoaded = true;
                    context.xmlWidget.init({
                        Wuid: context.wu.Wuid
                    });
                } else if (nval.id == context.id + "Legacy" && !context.legacyPaneLoaded) {
                    context.legacyPaneLoaded = true;
                    context.legacyPane.set("content", dojo.create("iframe", {
                        src: "/WsWorkunits/WUInfo?Wuid=" + context.wu.Wuid + "&IncludeExceptions=0&IncludeGraphs=0&IncludeSourceFiles=0&IncludeResults=0&IncludeVariables=0&IncludeTimers=0&IncludeDebugValues=0&IncludeApplicationValues=0&IncludeWorkflows&SuppressResultSchemas=1",
                        style: "border: 0; width: 100%; height: 100%"
                    }));
                }
            });
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        //  Hitched actions  ---
        _onSave: function (event) {
            var protectedCheckbox = registry.byId(this.id + "Protected");
            var context = this;
            this.wu.update({
                Description: dom.byId(context.id + "Description").value,
                Jobname: dom.byId(context.id + "JobName").value,
                Protected: protectedCheckbox.get("value")
            }, null, {
                load: function (response) {
                    context.monitor();
                }
            });
        },
        _onReload: function (event) {
            this.monitor();
        },
        _onClone: function (event) {
            this.wu.clone({
                load: function (response) {
                    //TODO
                }
            });
        },
        _onDelete: function (event) {
            this.wu.doDelete({
                load: function (response) {
                    //TODO
                }
            });
        },
        _onResubmit: function (event) {
            var context = this;
            this.wu.resubmit({
                load: function (response) {
                    context.monitor();
                }
            });
        },
        _onAbort: function (event) {
            var context = this;
            this.wu.abort({
                load: function (response) {
                    context.monitor();
                }
            });
        },
        _onRestart: function (event) {
            var context = this;
            this.wu.restart({
                load: function (response) {
                    context.monitor();
                }
            });
        },
        _onPublish: function (event) {
            this.wu.publish(dom.byId(this.id + "JobName2").value);
        },

        //  Implementation  ---
        init: function (params) {
            if (params.Wuid) {
                registry.byId(this.id + "Summary").set("title", params.Wuid);

                dom.byId(this.id + "Wuid").innerHTML = params.Wuid;
                this.wu = new Workunit({
                    Wuid: params.Wuid
                });
                this.monitor();
            }
            this.infoGridWidget.init(params);
        },

        monitor: function () {
            var prevState = "";
            var context = this;
            this.wu.monitor(function (workunit) {
                context.monitorWorkunit(workunit);
            });
        },

        resetPage: function () {
        },

        objectToText: function (obj) {
            var text = ""
            for (var key in obj) {
                text += "<tr><td>" + key + ":</td>";
                if (typeof obj[key] == "object") {
                    text += "[<br>";
                    for (var i = 0; i < obj[key].length; ++i) {
                        text += this.objectToText(obj[key][i]);
                    }
                    text += "<br>]<br>";
                } else {
                    text += "<td>" + obj[key] + "</td></tr>";

                }
            }
            return text;
        },

        monitorWorkunit: function (response) {
            registry.byId(this.id + "Save").set("disabled", !this.wu.isComplete());
            //registry.byId(this.id + "Reload").set("disabled", this.wu.isComplete());
            registry.byId(this.id + "Clone").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Delete").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Abort").set("disabled", this.wu.isComplete());
            registry.byId(this.id + "Resubmit").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Restart").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Publish").set("disabled", !this.wu.isComplete());
            
            registry.byId(this.id + "JobName").set("readOnly", !this.wu.isComplete());
            registry.byId(this.id + "Description").set("readOnly", !this.wu.isComplete());
            registry.byId(this.id + "Protected").set("readOnly", !this.wu.isComplete());

            registry.byId(this.id + "Summary").set("iconClass",this.wu.getStateIconClass());
            domClass.remove(this.id + "StateIdImage");
            domClass.add(this.id + "StateIdImage", this.wu.getStateIconClass());

            //dom.byId(this.id + "StateIdImage").title = response.State;
            dom.byId(this.id + "ProtectedImage").src = this.wu.getProtectedImage();
            dom.byId(this.id + "State").innerHTML = response.State;
            dom.byId(this.id + "Owner").innerHTML = response.Owner;
            dom.byId(this.id + "JobName").value = response.Jobname;
            dom.byId(this.id + "JobName2").value = response.Jobname;
            dom.byId(this.id + "Cluster").innerHTML = response.Cluster;

            var context = this;
            if (this.wu.isComplete() || this.prevState != response.State) {
                this.prevState = response.State;
                this.wu.getInfo({
                    onGetVariables: function (response) {
                        registry.byId(context.id + "Variables").set("title", "Variables " + "(" + response.length + ")");
                        context.variablesGrid = registry.byId(context.id + "VariablesGrid");
                        context.variablesGrid.setStructure([
                            { name: "Name", field: "Name", width: 16 },
                            { name: "Type", field: "ColumnType", width: 10 },
                            { name: "Default Value", field: "Value", width: 32 }
                        ]);
                        var memory = new Memory({ data: response });
                        var store = new ObjectStore({ objectStore: memory });
                        context.variablesGrid.setStore(store);
                        context.variablesGrid.setQuery({
                            Name: "*"
                        });
                    },

                    onGetResults: function (response) {
                        context.resultsWidget.set("title", "Outputs " + "(" + response.length + ")");
                        var tooltip = "";
                        for (var i = 0; i < response.length; ++i) {
                            if (tooltip != "")
                                tooltip += "\n";
                            tooltip += response[i].Name;
                            if (response[i].Value)
                                tooltip += " " + response[i].Value;
                        }
                        context.resultsWidget.set("tooltip", tooltip);
                    },

                    onGetSourceFiles: function (response) {
                        context.filesWidget.set("title", "Inputs " + "(" + response.length + ")");
                        var tooltip = "";
                        for (var i = 0; i < response.length; ++i) {
                            if (tooltip != "")
                                tooltip += "\n";
                            tooltip += response[i].Name;
                        }
                        context.filesWidget.set("tooltip", tooltip);
                    },

                    onGetTimers: function (response) {
                        context.timersWidget.set("title", "Timers " + "(" + response.length + ")");
                        var tooltip = "";
                        for (var i = 0; i < response.length; ++i) {
                            if (response[i].GraphName)
                                continue;
                            if (response[i].Name == "Process")
                                dom.byId(context.id + "Time").innerHTML = response[i].Value;
                            if (tooltip != "")
                                tooltip += "\n";
                            tooltip += response[i].Name;
                            if (response[i].Value)
                                tooltip += " " + response[i].Value;
                        }
                        context.timersWidget.set("tooltip", tooltip);
                    },

                    onGetGraphs: function (response) {
                        context.graphsWidget.set("title", "Graphs " + "(" + response.length + ")");
                        var tooltip = "";
                        for (var i = 0; i < response.length; ++i) {
                            if (tooltip != "")
                                tooltip += "\n";
                            tooltip += response[i].Name;
                            if (response[i].Time)
                                tooltip += " " + response[i].Time;
                        }
                        context.graphsWidget.set("tooltip", tooltip);
                    },

                    onGetAll: function (response) {
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
                        //dom.byId(context.id + "WUInfoResponse").innerHTML = context.objectToText(response);
                        dom.byId(context.id + "Description").value = response.Description;
                        dom.byId(context.id + "Action").innerHTML = response.ActionEx;
                        dom.byId(context.id + "Scope").innerHTML = response.Scope;
                        var protectedCheckbox = registry.byId(context.id + "Protected");
                        protectedCheckbox.set("value", response.Protected);
                    }
                });
            }
        }
    });
});
