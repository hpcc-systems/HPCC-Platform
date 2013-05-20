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
    "exports",
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/query",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/form/Form",
    "dijit/form/SimpleTextarea",
    "dijit/form/TextBox",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/TitlePane",
    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "hpcc/ResultWidget",
    "hpcc/ECLSourceWidget",
    "hpcc/FilePartsWidget",
    "hpcc/WUDetailsWidget",
    "hpcc/DFUWUDetailsWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/ESPLogicalFile",
    "hpcc/ESPDFUWorkunit",

    "dojo/text!../templates/LFDetailsWidget.html",

    "dijit/TooltipDialog"
], function (exports, declare, lang, arrayUtil, dom, domAttr, domClass, domForm, query,
                _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, Toolbar, TooltipDialog, Form, SimpleTextarea, TextBox, Button, DropDownButton, TitlePane, registry,
                _TabContainerWidget, ResultWidget, EclSourceWidget, FilePartsWidget, WUDetailsWidget, DFUWUDetailsWidget, TargetSelectWidget, ESPLogicalFile, ESPDFUWorkunit,
                template) {
    exports.fixCircularDependency = declare("LFDetailsWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "LFDetailsWidget",
        borderContainer: null,
        tabContainer: null,
        summaryWidget: null,
        contentWidget: null,
        sourceWidget: null,
        defWidget: null,
        xmlWidget: null,
        filePartsWidget: null,
        workunitWidget: null,
        dfuWorkunitWidget: null,

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
            this.copyTargetSelect = registry.byId(this.id + "CopyTargetSelect");
            this.desprayTargetSelect = registry.byId(this.id + "DesprayTargetSelect");
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.logicalFile.refresh();
        },
        _onSave: function (event) {
            var context = this;
            this.logicalFile.save(dom.byId(context.id + "Description").value);
        },
        _onDelete: function (event) {
            this.logicalFile.doDelete({
                load: function (response) {
                }
            });
        },

        _handleResponse: function (wuidQualifier, response) {
            if (lang.exists(wuidQualifier, response)) {
                var wu = ESPDFUWorkunit.Get(lang.getObject(wuidQualifier, false, response));
                wu.startMonitor(true);
                var tab = this.ensurePane(this.id + "_" + wu.ID, {
                    Wuid: wu.ID
                });
                if (tab) {
                    this.selectChild(tab);
                }
            }
        },
        _onCopyOk: function (event) {
            var context = this;
            this.logicalFile.copy({
                request: domForm.toObject(this.id + "CopyDialog")
            }).then(function (response) {
                context._handleResponse("CopyResponse.result", response);
            });
            registry.byId(this.id + "CopyDropDown").closeDropDown();
        },
        _onCopyCancel: function (event) {
            registry.byId(this.id + "CopyDropDown").closeDropDown();
        },
        _onRenameOk: function (event) {
            var context = this;
            this.logicalFile.rename({
                request: domForm.toObject(this.id + "RenameDialog")
            }).then(function (response) {
                context._handleResponse("RenameResponse.wuid", response);
            });
            registry.byId(this.id + "RenameDropDown").closeDropDown();
        },
        _onRenameCancel: function (event) {
            registry.byId(this.id + "RenameDropDown").closeDropDown();
        },
        _onDesprayOk: function (event) {
            var context = this;
            this.logicalFile.despray({
                request: domForm.toObject(this.id + "DesprayDialog")
            }).then(function (response) {
                context._handleResponse("DesprayResponse.wuid", response);
            });
            registry.byId(this.id + "DesprayDropDown").closeDropDown();
        },
        _onDesprayCancel: function (event) {
            registry.byId(this.id + "DesprayDropDown").closeDropDown();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;

            var context = this;
            if (params.Name) {
                this.logicalFile = ESPLogicalFile.Get(params.Name);
                var data = this.logicalFile.getData();
                for (key in data) {
                    this.updateInput(key, null, data[key]);
                }
                this.logicalFile.watch(function (name, oldValue, newValue) {
                    context.updateInput(name, oldValue, newValue);
                });
                this.logicalFile.refresh();
            }
            this.selectChild(this.summaryWidget, true);
            this.copyTargetSelect.init({
                Groups: true
            });
            this.desprayTargetSelect.init({
                DropZones: true,
                callback: function (value, item) {
                    context.updateInput("DesprayTargetIPAddress", null, item.machine.Netaddress);
                    context.updateInput("DesprayTargetPath", null, item.machine.Directory + "/" + context.logicalFile.Filename);
                }
            });
        },

        initTab: function() {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id == this.summaryWidget.id) {
                } else if (currSel.id == this.contentWidget.id) {
                    this.contentWidget.init({
                        result: this.logicalFile.result
                    });
                } else if (currSel.id == this.sourceWidget.id) {
                    this.sourceWidget.init({
                        ECL: this.logicalFile.Ecl
                    });
                } else if (currSel.id == this.defWidget.id) {
                    var context = this;
                    this.logicalFile.fetchDEF(function (response) {
                        context.defWidget.init({
                            ECL: response
                        });
                    });
                } else if (currSel.id == this.xmlWidget.id) {
                    var context = this;
                    this.logicalFile.fetchXML(function (response) {
                        context.xmlWidget.init({
                            ECL: response
                        });
                    });
                } else if (currSel.id == this.filePartsWidget.id) {
                    this.filePartsWidget.init({
                        fileParts: lang.exists("logicalFile.DFUFileParts.DFUPart", this) ? this.logicalFile.DFUFileParts.DFUPart : []
                    });
                } else if (this.workunitWidget && currSel.id == this.workunitWidget.id) {
                    this.workunitWidget.init({
                        Wuid: this.logicalFile.Wuid
                    });
                } else if (this.dfuWorkunitWidget && currSel.id == this.dfuWorkunitWidget.id) {
                    this.dfuWorkunitWidget.init({
                        Wuid: this.logicalFile.Wuid
                    });
                } else {
                    currSel.init(currSel.params);
                }
            }
        },

        showMessage: function (msg) {
        },

        /*isComplete: function () {
            return true;
        },*/

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
            if (name === "Wuid") {
                if (!newValue) {
                    this.removeChild(this.workunitWidget);
                    this.workunitWidget = null;
                    this.removeChild(this.dfuWorkunitWidget);
                    this.dfuWorkunitWidget = null;
                } else if (this.workunitWidget && newValue[0] === "D") {
                    this.removeChild(this.workunitWidget);
                    this.workunitWidget = null;
                } else if (this.dfuWorkunitWidget) {
                    this.removeChild(this.dfuWorkunitWidget);
                    this.dfuWorkunitWidget = null;
                }
            }
            if (name === "Name") {
                this.updateInput("RenameSourceName", oldValue, newValue);
                this.updateInput("RenameTargetName", oldValue, newValue);
                this.updateInput("DespraySourceName", oldValue, newValue);
                this.updateInput("CopySourceName", oldValue, newValue);
                this.updateInput("CopyTargetName", oldValue, newValue);
            }

            /*
            var widget = registry.byId(this.id + name);
            if (widget) {
                if (widget.has("innerHTML")) {
                    widget.set("innerHTML", newValue);
                } else {
                    widget.set("value", newValue);
                }
            } else {
                var element = dom.byId(this.id + name);
                if (element) {
                    if (element.innerHTML) {
                        element.innerHTML = newValue;
                    } else {
                        element.value = newValue;
                    }
                }
            }
            if (name === "Filename") {
                registry.byId(this.id + "_Summary").set("title", newValue);
            }
            */
        },
        refreshFileDetails: function (fileDetails) {
            /*
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
            dom.byId(this.id + "Dir").innerHTML = fileDetails.Dir;
            dom.byId(this.id + "RecordSize").innerHTML = fileDetails.RecordSize;
            dom.byId(this.id + "Count").innerHTML = fileDetails.RecordCount;
            this.contentWidget.set("title", "Content " + "(" + fileDetails.RecordCount + ")");
            dom.byId(this.id + "Filesize").innerHTML = fileDetails.Filesize;
            dom.byId(this.id + "PathMask").innerHTML = fileDetails.PathMask;
            */
        },

        ensurePane: function (id, params) {
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new DFUWUDetailsWidget.fixCircularDependency({
                    id: id,
                    title: params.Wuid,
                    closable: true,
                    params: params
                });
                this.addChild(retVal);
            }
            return retVal;
        }

    });
});
