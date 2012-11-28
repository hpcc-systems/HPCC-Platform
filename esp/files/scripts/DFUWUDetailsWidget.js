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
    "dijit/ProgressBar",

    "hpcc/ECLSourceWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/SampleSelectWidget",
    "hpcc/GraphWidget",
    "hpcc/ResultsWidget",
    "hpcc/InfoGridWidget",
    "hpcc/ESPDFUWorkunit",

    "dojo/text!../templates/DFUWUDetailsWidget.html"
], function (declare, xhr, dom,
                _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, Toolbar, Textarea, TitlePane, registry, ProgressBar,
                EclSourceWidget, TargetSelectWidget, SampleSelectWidget, GraphWidget, ResultsWidget, InfoGridWidget, DFUWorkunit,
                template) {
    return declare("DFUWUDetailsWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "DFUWUDetailsWidget",
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
        legacyPane: null,
        legacyPaneLoaded: false,

        wu: null,
        loaded: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.tabContainer = registry.byId(this.id + "TabContainer");
            this.legacyPane = registry.byId(this.id + "Legacy");
            this.xmlWidget = registry.byId(this.id + "XML");


            var context = this;
            this.tabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                if (nval.id == context.id + "Content" && !context.resultWidgetLoaded) {
                    context.resultWidgetLoaded = true;
                    context.resultWidget.init({
                        result: context.logicalFile.result
                    });
                } else if (nval.id == context.id + "Source" && !context.sourceWidgetLoaded) {
                    context.sourceWidgetLoaded = true;
                    context.sourceWidget.init({
                        ECL: context.logicalFile.DFUInfoResponse.Ecl
                    });
                } else if (nval.id == context.id + "DEF" && !context.defWidgetLoaded) {
                    context.logicalFile.fetchDEF(function (response) {
                        context.defWidgetLoaded = true;
                        context.defWidget.init({
                            ECL: response
                        });
                    });
                } else if (nval.id == context.id + "XML" && !context.xmlWidgetLoaded) {
                    context.wu.fetchXML(function (response) {
                        context.xmlWidgetLoaded = true;
                        context.xmlWidget.init({
                            ECL: response
                        });
                    });
                } else if (nval.id == context.id + "FileParts" && !context.filePartsWidgetLoaded) {
                    context.filePartsWidgetLoaded = true;
                    context.filePartsWidget.init({
                        fileParts: lang.exists("logicalFile.DFUInfoResponse.DFUFileParts.DFUPart", context) ? context.logicalFile.DFUInfoResponse.DFUFileParts.DFUPart : []
                    });
                } else if (nval.id == context.id + "Workunit" && !context.workunitWidgetLoaded) {
                    context.workunitWidgetLoaded = true;
                    context.workunitWidget.init({
                        Wuid: context.logicalFile.DFUInfoResponse.Wuid
                    });
                } else if (nval.id == context.id + "DFUWorkunit" && !context.workunitWidgetLoaded) {
                    context.dfuWorkunitWidgetLoaded = true;
                    context.dfuWorkunitWidget.init({
                        Wuid: context.logicalFile.DFUInfoResponse.Wuid
                    });
                } else if (nval.id == context.id + "Legacy" && !context.legacyPaneLoaded) {
                    context.legacyPaneLoaded = true;
                    context.legacyPane.set("content", dojo.create("iframe", {
                        src: "/FileSpray/GetDFUWorkunit?wuid=" + context.wu.Wuid,
                        style: "border: 0; width: 100%; height: 100%"
                    }));
                }
            });       
        },

        startup: function (args) {
            this.inherited(arguments);

            
        },
    //TODO

        /*showProgress: function(event){
            alert(response.PercentDone);
            numParts = Math.floor(100/10);
            
            jsProgressBar.update({maximum:numParts, progress:response.PercentDone});

            for (var i = response.PercentDone; i <= numParts; i++){                
                timer = setTimeout(
                "jsProgressBar.update({progress: " + i + "})", (i+1)*1000 //1000ms of 1
                )
            }        
        },*/

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

        _onDelete: function (event) {
        },

        //  Implementation  ---
        init: function (params) {
            //dom.byId("showWuid").innerHTML = params.Wuid;
            if (params.Wuid) {
                registry.byId(this.id + "Summary").set("title", params.Wuid);
                dom.byId(this.id + "Wuid").innerHTML = params.Wuid;

                this.wu = new DFUWorkunit({
                    Wuid: params.Wuid
                });
                var context = this;
                this.wu.monitor(function (workunit) {
                    context.monitorDFUWorkunit(workunit);
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

        _onSave: function (event) { 
            var protectedCheckbox = registry.byId(this.id + "Protected");
            var context = this;
            this.wu.update({
                //Description: dom.byId(context.id + "Description").value,
                //obname: dom.byId(context.id + "JobName").value,
                Protected: protectedCheckbox.get("value")
            }, null, {
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
         _onResubmit: function (event) {
            var context = this;
            this.wu.resubmit({
                load: function (response) {
                    context.monitor();
                }
            });
        },
         _onModify: function (event) {
            var context = this;
            this.wu.resubmit({
                load: function (response) {
                    context.monitor();
                }
            });
        },

        monitorDFUWorkunit: function (response) {
            if (!this.loaded) {                
                 registry.byId(this.id + "Save").set("disabled", !this.wu.isComplete());            
                 registry.byId(this.id + "Delete").set("disabled", !this.wu.isComplete());
                 registry.byId(this.id + "Abort").set("disabled", this.wu.isComplete());            
                 registry.byId(this.id + "Resubmit").set("disabled", !this.wu.isComplete());
                 registry.byId(this.id + "Modify").set("disabled", !this.wu.isComplete());
                 registry.byId(this.id + "Protected").set("readOnly", !this.wu.isComplete());
                 
                 dom.byId(this.id + "ID").innerHTML = response.ID;
                 dom.byId(this.id + "JobName").value = response.JobName;
                 dom.byId(this.id + "Queue").innerHTML = response.Queue;
                 dom.byId(this.id + "Command").innerHTML = response.Command;
                 dom.byId(this.id + "TimeStarted").innerHTML = response.TimeStarted;
                 dom.byId(this.id + "TimeStopped").innerHTML = response.TimeStopped;                             
                 dom.byId(this.id + "ProgressBar").value = response.PercentDone;

                 dom.byId(this.id + "ProgressMessage").innerHTML = response.ProgressMessage;
                 dom.byId(this.id + "SummaryMessage").innerHTML = response.SummaryMessage;
                 dom.byId(this.id + "SourceLogicalName").innerHTML = response.SourceLogicalName;
                 dom.byId(this.id + "DestDirectory").innerHTML = response.DestDirectory;
                 dom.byId(this.id + "DestIP").innerHTML = response.DestIP;
                 dom.byId(this.id + "DestFilePath").innerHTML = response.DestFilePath;
                 dom.byId(this.id + "DestFormat").innerHTML = response.DestFormat;
                 dom.byId(this.id + "DestNumParts").innerHTML = response.DestNumParts;
                 dom.byId(this.id + "MonitorSub").innerHTML = response.MonitorSub;
                 dom.byId(this.id + "Overwrite").innerHTML = response.Overwrite;
                 dom.byId(this.id + "Replicate").innerHTML = response.Replicate;
                 dom.byId(this.id + "Compress").innerHTML = response.Compress;
                 dom.byId(this.id + "AutoRefresh").innerHTML = response.AutoRefresh;

                 
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
                        

                    }
                });
            }
        }
    });
});