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
    "dojo/_base/lang",
    "dojo/dom",
    "dojo/dom-class",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/form/SimpleTextarea",
    "dijit/form/Button",
    "dijit/TitlePane",
    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "hpcc/ResultWidget",
    "hpcc/ECLSourceWidget",
    "hpcc/FilePartsWidget",
    "hpcc/WUDetailsWidget",
    "hpcc/DFUWUDetailsWidget",

    "hpcc/ESPLogicalFile",

    "dojo/text!../templates/LFDetailsWidget.html"
], function (declare, lang, dom, domClass, 
                _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, Toolbar, TooltipDialog, SimpleTextarea, Button, TitlePane, registry,
                _TabContainerWidget, ResultWidget, EclSourceWidget, FilePartsWidget, WUDetailsWidget, DFUWUDetailsWidget,
                ESPLogicalFile,
                template) {
    return declare("LFDetailsWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "LFDetailsWidget",
        borderContainer: null,
        tabContainer: null,
        contentWidget: null,
        contentWidgetLoaded: false,
        sourceWidget: null,
        sourceWidgetLoaded: false,
        defWidget: null,
        defWidgetLoaded: false,
        xmlWidget: null,
        xmlWidgetLoaded: false,
        filePartsWidget: null,
        filePartsWidgetLoaded: false,
        workunitWidget: null,
        workunitWidgetLoaded: false,
        dfuWorkunitWidget: null,
        dfuWorkunitWidgetLoaded: false,
        legacyPane: null,
        legacyPaneLoaded: false,

        logicalFile: null,
        prevState: "",
        initalized: false,

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.contentWidget = registry.byId(this.id + "_Content");
            this.sourceWidget = registry.byId(this.id + "_Source");
            this.defWidget = registry.byId(this.id + "_DEF");
            this.xmlWidget = registry.byId(this.id + "_XML");
            this.filePartsWidget = registry.byId(this.id + "_FileParts");
            this.workunitWidget = registry.byId(this.id + "_Workunit");
            this.dfuWorkunitWidget = registry.byId(this.id + "_DFUWorkunit");
            this.legacyPane = registry.byId(this.id + "_Legacy");
        },

        //  Hitched actions  ---
        _onSave: function (event) {
            var context = this;
            this.logicalFile.save(dom.byId(context.id + "Description").value, {
                onGetAll: function (response) {
                    context.refreshFileDetails(response);
                }
            });
        },
        _onRename: function (event) {
            var context = this;
            this.logicalFile.rename(dom.byId(this.id + "LogicalFileName2").innerHTML, {
                onGetAll: function (response) {
                    context.refreshFileDetails(response);
                }
            });
        },
        _onDelete: function (event) {
            this.logicalFile.doDelete({
                load: function (response) {
                    //TODO
                }
            });
        },
        _onCopy: function (event) {
            this.logicalFile.copy({
                load: function (response) {
                    //TODO
                }
            });
        },
        _onDespray: function (event) {
            var context = this;
            this.logicalFile.despray({
                load: function (response) {
                }
            });
        },

        //  Implementation  ---
        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;

            if (params.Name) {
                dom.byId(this.id + "LogicalFileName").innerHTML = params.Name;
                //dom.byId(this.id + "LogicalFileName2").value = params.Name;
                this.logicalFile = new ESPLogicalFile({
                    cluster: params.Cluster,
                    logicalName: params.Name
                });
                this.refreshPage();
            }
            this.selectChild(this.summaryWidget, true);
        },

        initTab: function() {
            var currSel = this.getSelectedChild();
            if (currSel.id == this.contentWidget.id && !this.contentWidgetLoaded) {
                this.contentWidgetLoaded = true;
                this.contentWidget.init({
                    result: this.logicalFile.result
                });
            } else if (currSel.id == this.sourceWidget.id && !this.sourceWidgetLoaded) {
                this.sourceWidgetLoaded = true;
                this.sourceWidget.init({
                    ECL: this.logicalFile.DFUInfoResponse.Ecl
                });
            } else if (currSel.id == this.defWidget.id && !this.defWidgetLoaded) {
                var context = this;
                this.logicalFile.fetchDEF(function (response) {
                    context.defWidgetLoaded = true;
                    context.defWidget.init({
                        ECL: response
                    });
                });
            } else if (currSel.id == this.xmlWidget.id && !this.xmlWidgetLoaded) {
                var context = this;
                this.logicalFile.fetchXML(function (response) {
                    context.xmlWidgetLoaded = true;
                    context.xmlWidget.init({
                        ECL: response
                    });
                });
            } else if (currSel.id == this.filePartsWidget.id && !this.filePartsWidgetLoaded) {
                this.filePartsWidgetLoaded = true;
                this.filePartsWidget.init({
                    fileParts: lang.exists("logicalFile.DFUInfoResponse.DFUFileParts.DFUPart", this) ? this.logicalFile.DFUInfoResponse.DFUFileParts.DFUPart : []
                });
            } else if (this.workunitWidget && currSel.id == this.workunitWidget.id && !this.workunitWidgetLoaded) {
                this.workunitWidgetLoaded = true;
                this.workunitWidget.init({
                    Wuid: this.logicalFile.DFUInfoResponse.Wuid
                });
            } else if (this.dfuWorkunitWidget && currSel.id == this.dfuWorkunitWidget.id && !this.workunitWidgetLoaded) {
                this.dfuWorkunitWidgetLoaded = true;
                this.dfuWorkunitWidget.init({
                    Wuid: this.logicalFile.DFUInfoResponse.Wuid
                });
            } else if (currSel.id == this.legacyPane.id && !this.legacyPaneLoaded) {
                this.legacyPaneLoaded = true;
                this.legacyPane.set("content", dojo.create("iframe", {
                    src: "/WsDfu/DFUInfo?Name=" + this.logicalFile.logicalName,//+ "&Cluster=" + this.logicalFile.cluster,
                    style: "border: 0; width: 100%; height: 100%"
                }));
            }
        },

        showMessage: function (msg) {
        },

        /*isComplete: function () {
            return true;
        },*/

        refreshPage: function () {
            var context = this;
            this.logicalFile.getInfo({
                onGetAll: function (response) {
                    context.refreshFileDetails(response);
                }
            });
        },
        refreshFileDetails: function (fileDetails) {
            if (fileDetails.Wuid && fileDetails.Wuid[0] == "D" && this.workunitWidget) {
                this.removeChild(this.workunitWidget);
                this.workunitWidget = null;
            } else if (this.dfuWorkunitWidget) {
                this.removeChild(this.dfuWorkunitWidget);
                this.dfuWorkunitWidget = null;
            }
            registry.byId(this.id + "_Summary").set("title", fileDetails.Filename);
            //registry.byId(this.id + "Summary").set("iconClass", "iconRefresh");
            //domClass.remove(this.id + "Test");
            //domClass.add(this.id + "Test", "iconRefresh");
            dom.byId(this.id + "Owner").innerHTML = fileDetails.Owner;
            dom.byId(this.id + "Description").value = fileDetails.Description;
            dom.byId(this.id + "JobName").innerHTML = fileDetails.JobName;
            dom.byId(this.id + "Wuid").innerHTML = fileDetails.Wuid;
            dom.byId(this.id + "Modification").innerHTML = fileDetails.Modified + " (UTC/GMT)";
            dom.byId(this.id + "Directory").innerHTML = fileDetails.Dir;
            dom.byId(this.id + "RecordSize").innerHTML = fileDetails.RecordSize;
            dom.byId(this.id + "Count").innerHTML = fileDetails.RecordCount;
            this.contentWidget.set("title", "Content " + "(" + fileDetails.RecordCount + ")");
            dom.byId(this.id + "Filesize").innerHTML = fileDetails.Filesize;
            dom.byId(this.id + "Pathmask").innerHTML = fileDetails.PathMask;
        }

    });
});
