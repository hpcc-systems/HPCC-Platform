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
    "dojo/_base/xhr",
    "dojo/dom",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Textarea",
    "dijit/TitlePane",
    "dijit/registry",

    "hpcc/ECLSourceWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/SampleSelectWidget",
    "hpcc/GraphWidget",
    "hpcc/ResultsWidget",
    "hpcc/InfoGridWidget",
    "hpcc/ESPWorkunit",

    "dojo/text!../templates/DFUSearchWidget.html"
], function (declare, xhr, dom,
                _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, Toolbar, Textarea, TitlePane, registry,
                EclSourceWidget, TargetSelectWidget, SampleSelectWidget, GraphWidget, ResultsWidget, InfoGridWidget, Workunit,
                template) {
    return declare("DFUSearchWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "DFUSearchWidget",
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
        playgroundWidget: null,
        playgroundWidgetLoaded: false,
        xmlWidget: null,
        xmlWidgetLoaded: false,

        wu: null,
        loaded: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            //this.tabContainer = registry.byId(this.id + "TabContainer");
            //this.resultsWidget = registry.byId(this.id + "Results");
            //this.filesWidget = registry.byId(this.id + "Files");
            //this.timersWidget = registry.byId(this.id + "Timers");
           // this.graphsWidget = registry.byId(this.id + "Graphs");
            //this.sourceWidget = registry.byId(this.id + "Source");
            //this.playgroundWidget = registry.byId(this.id + "Playground");
            //this.xmlWidget = registry.byId(this.id + "XML");
            //this.infoGridWidget = registry.byId(this.id + "InfoContainer");
            var context = this;
           /* this.tabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                if (nval.id == context.id + "Results" && !context.resultsWidgetLoaded) {
                    context.resultsWidgetLoaded = true;
                    context.resultsWidget.init({
                        Wuid: context.wu.wuid
                    });
                } else if (nval.id == context.id + "Files" && !context.filesWidgetLoaded) {
                    context.filesWidgetLoaded = true;
                    context.filesWidget.init({
                        Wuid: context.wu.wuid,
                        SourceFiles: true
                    });
                } else if (nval.id == context.id + "Timers" && !context.timersWidgetLoaded) {
                    context.timersWidgetLoaded = true;
                    context.timersWidget.init({
                        Wuid: context.wu.wuid
                    });
                } else if (nval.id == context.id + "Graphs" && !context.graphsWidgetLoaded) {
                    context.graphsWidgetLoaded = true;
                    context.graphsWidget.init({
                        Wuid: context.wu.wuid
                    });
                } else if (nval.id == context.id + "Source" && !context.sourceWidgetLoaded) {
                    context.sourceWidgetLoaded = true;
                    context.sourceWidget.init({
                        Wuid: context.wu.wuid
                    });
                } else if (nval.id == context.id + "Playground" && !context.playgroundWidgetLoaded) {
                    context.playgroundWidgetLoaded = true;
                    context.playgroundWidget.init({
                        Wuid: context.wu.wuid
                    });
                } else if (nval.id == context.id + "XML" && !context.xmlWidgetLoaded) {
                    context.xmlWidgetLoaded = true;
                    context.xmlWidget.init({
                        Wuid: context.wu.wuid
                    });
                }
            });*/

            
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
        },
        _onReset: function (event) {
        },
        _onClone: function (event) {
        },
        _onDelete: function (event) {
        },
        _onAbort: function (event) {
        },
        _onResubmit: function (event) {
        },
        _onRestart: function (event) {
        },
        _onPublish: function (event) {
        },

        //  Implementation  ---
        init: function (params) {
            //dom.byId("showWuid").innerHTML = params.Wuid;
            if (params.Wuid) {
                //dom.byId(this.id + "Wuid").innerHTML = params.Wuid;
                this.wu = new Workunit({
                    wuid: params.Wuid
                });
                var context = this;
                this.wu.monitor(function (workunit) {
                    context.monitorWorkunit(workunit);
                });
            }
           // this.infoGridWidget.init(params);
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
            if (!this.loaded) {
                //dom.byId(this.id + "WUInfoResponse").innerHTML = this.objectToText(response);             
                //dom.byId("showStateIdImage").src = this.wu.getStateImage();
                //dom.byId("showStateIdImage").title = response.State;
                //dom.byId("showStateReadOnly").innerHTML = response.State;
                //dom.byId("showAction").innerHTML = response.ActionId;
               // dom.byId("showOwner").innerHTML = response.Owner;
                //dom.byId("showScope").value = response.Scope;
                //dom.byId("showJobName").value = response.Jobname;
                //dom.byId("showCluster").innerHTML = response.Cluster;
                
                this.loaded = true;
            }

            var context = this;
            if (this.wu.isComplete()) {
                this.wu.getInfo({
                    onGetResults: function (response) {
                        
                    },

                    onGetSourceFiles: function (response) {
                        
                    },

                    onGetTimers: function (response) {
                       
                    },

                    onGetGraphs: function (response) {
                        
                    },

                    onGetAll: function (response) {
                        //dom.byId(context.id + "WUInfoResponse").innerHTML = context.objectToText(response);

                    }
                });
            }
        }
    });
});
