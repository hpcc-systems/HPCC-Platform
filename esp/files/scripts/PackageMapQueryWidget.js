/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.
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
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-form",
    "dojo/data/ObjectStore",
    "dojo/on",
    "dojo/topic",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",
    "dojox/form/Uploader",
    "dojox/form/uploader/FileList",
    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/Pagination",
    "dojox/grid/enhanced/plugins/IndirectSelection",
    "dojo/data/ItemFileWriteStore",

    "hpcc/PackageMapDetailsWidget",
    "hpcc/PackageMapValidateWidget",
    "hpcc/WsPackageMaps",
    "hpcc/ESPPackageProcess",
    "hpcc/SFDetailsWidget",

    "dojo/text!../templates/PackageMapQueryWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog"
], function (declare, lang, arrayUtil, dom, domConstruct, domForm, ObjectStore, on, topic,
    _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry,
    Uploader, FileUploader, EnhancedGrid, Pagination, IndirectSelection, ItemFileWriteStore,
    PackageMapDetailsWidget, PackageMapValidateWidget,
    WsPackageMaps, ESPPackageProcess, SFDetailsWidget,
    template) {
    return declare("PackageMapQueryWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "PackageMapQueryWidget",
        packagesTab: null,
        packagesGrid: null,
        tabMap: [],
        targets: null,
        processesToList: new Array(),
        processesToAdd: new Array(),
        targetSelected: '',
        processSelected: '',
        processFilters: null,
        addPackageMapDialog: null,
        validateTab: null,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.tabContainer = registry.byId(this.id + "TabContainer");
            this.packagesTab = registry.byId(this.id + "Packages");
            this.packagesGrid = registry.byId(this.id + "PackagesGrid");
            //this.packagesGrid.canSort = function(col){return false;};
            this.targetSelect = registry.byId(this.id + "TargetSelect");
            this.processSelect = registry.byId(this.id + "ProcessSelect");
            //this.processFilterSelect = registry.byId(this.id + "ProcessFilterSelect");
            this.addPackageMapDialog = registry.byId(this.id+"AddProcessMapDialog");
            this.addPackageMapTargetSelect = registry.byId(this.id + "AddProcessMapTargetSelect");
            this.addPackageMapProcessSelect = registry.byId(this.id + "AddProcessMapProcessSelect");

            var context = this;
            this.tabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                if ((nval.id != context.id + "Packages") && (!nval.initalized)) {
                    nval.init(nval.params);
                }
                context.selectedTab = nval;
            });
        },

        startup: function (args) {
            this.inherited(arguments);
            this.refreshActionState();
            this.getSelections();
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        onRowDblClick: function (item) {
            var tab = this.showPackageMapDetails(this.id + "_" + item.Id, {
                    target: item.Target,
                    process: item.Process,
                    active: item.Active,
                    packageMap: item.Id
                });
            this.tabContainer.selectChild(tab);
        },

        _onChangeTarget: function (event) {
            this.updateProcessSelections(this.processSelect, this.processesToList, this.targetSelect.getValue());
        },

        _onChangeAddProcessMapTarget: function (event) {
            this.updateProcessSelections(this.addPackageMapProcessSelect, this.processesToAdd, this.addPackageMapTargetSelect.getValue());
        },

        _onRefresh: function (event) {
            this.packagesGrid.rowSelectCell.toggleAllSelection(false);
            this.refreshGrid();
        },

        _onOpen: function (event) {
            var selections = this.packagesGrid.selection.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.showPackageMapDetails(this.id + "_" + selections[i].Id, {
                    target: selections[i].Target,
                    process: selections[i].Process,
                    active: selections[i].Active,
                    packageMap: selections[i].Id
                });
                if (i == 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.tabContainer.selectChild(firstTab, true);
            }
        },
        _onAdd: function (event) {
            this.initAddProcessMapInput();
            this.addPackageMapDialog.show();

            var context = this;
            var addPackageMapUploader = registry.byId(this.id+"AddProcessMapFileUploader");
            dojo.connect(addPackageMapUploader, "onComplete", this, function(e) {
                registry.byId(this.id+"AddProcessMapDialogSubmit").set('disabled', false);
                return context.addPackageMapCallback();
            });
            dojo.connect(addPackageMapUploader, "onBegin", this, function(e) {
                registry.byId(this.id+"AddProcessMapDialogSubmit").set('disabled', true);
                return;
            });
            var addPackageMapSubmitButton = registry.byId(this.id+"AddProcessMapDialogSubmit");
            dojo.connect(addPackageMapSubmitButton, "onClick", this, function(e) {
                return context._onAddPackageMapSubmit();
            });
            var addPackageMapCloseButton = registry.byId(this.id+"AddProcessMapDialogClose");
            dojo.connect(addPackageMapCloseButton, "onClick", this, function(e) {
                this.addPackageMapDialog.onCancel();
            });
        },
        initAddProcessMapInput: function () {
            var defaultTarget = null;
            for (var i = 0; i < this.targets.length; ++i) {
                var target = this.targets[i];
                if (target.Type == 'roxie') {//only roxie has package map for now.
                    this.addPackageMapTargetSelect.options.push({label: target.Name, value: target.Name});
                    if (defaultTarget == null)
                        defaultTarget = target;
                }
            }
            if (defaultTarget != null) {
                this.addPackageMapTargetSelect.set("value", defaultTarget.Name);
                if (defaultTarget.Processes != undefined)
                    this.addProcessSelections(this.addPackageMapProcessSelect, this.processesToAdd, defaultTarget.Processes.Item);
            }
            registry.byId(this.id+"AddProcessMapId").set('value', '');
            registry.byId(this.id+"AddProcessMapDaliIP").set('value', '');
            registry.byId(this.id+"AddProcessMapActivate").set('checked', 'checked');
            registry.byId(this.id+"AddProcessMapOverWrite").set('checked', '');
            registry.byId(this.id+"AddProcessMapFileUploader").reset();
            registry.byId(this.id+"AddProcessMapFileUploader").set('url', '');
            registry.byId(this.id+"AddProcessMapForm").set('action', '');
            registry.byId(this.id+"AddProcessMapDialogSubmit").set('disabled', true);
        },
        _onAddProcessMapIdKeyUp: function () {
            this._onCheckAddProcessMapInput();
        },
        _onCheckAddProcessMapInput: function () {
            var id = registry.byId(this.id+"AddProcessMapId").get('value');
            var files = registry.byId(this.id+"AddProcessMapFileUploader").getFileList();
            if (files.length > 1) {
                alert('Only one package file allowed');
                return;
            }
            var fileName = '';
            if (files.length > 0)
                fileName = files[0].name;
            if ((fileName != '') && (id == '')) {
                registry.byId(this.id+"AddProcessMapId").set('value', fileName);
                registry.byId(this.id+"AddProcessMapDialogSubmit").set('disabled', false);
            } else if ((id == '') || (files.length < 1))
                registry.byId(this.id+"AddProcessMapDialogSubmit").set('disabled', true);
            else
                registry.byId(this.id+"AddProcessMapDialogSubmit").set('disabled', false);
        },
        _onAddPackageMapSubmit: function () {
            var target = this.addPackageMapTargetSelect.getValue();
            var id = registry.byId(this.id+"AddProcessMapId").get('value');
            //var process = registry.byId(this.id+"AddProcessMapProcess").get('value');
            var process = this.addPackageMapProcessSelect.getValue();
            var daliIp = registry.byId(this.id+"AddProcessMapDaliIP").get('value');
            var activate = registry.byId(this.id+"AddProcessMapActivate").get('checked');
            var overwrite = registry.byId(this.id+"AddProcessMapOverWrite").get('checked');
            if ((id == '') || (target == ''))
                return false;
            if  ((process == '') || (process == 'ANY'))
                process = '*';

            var action = "/WsPackageProcess/AddPackage?upload_&PackageMap="+id+"&Target="+target;
            if (process != '')
                action += "&Process="+process;
            if (daliIp != '')
                action += "&DaliIp="+daliIp;
            if (activate)
                action += "&Activate=1";
            else
                action += "&Activate=0";
            if (overwrite)
                action += "&OverWrite=1";
            else
                action += "&OverWrite=0";
            var theForm = registry.byId(this.id+"AddProcessMapForm");
            if (theForm == undefined)
                return false;
            theForm.set('action', action);
            return true;
        },
        _onDelete: function (event) {
            if (confirm('Delete selected packages?')) {
                var context = this;
                WsPackageMaps.deletePackageMap(this.packagesGrid.selection.getSelected(), {
                    load: function (response) {
                        context.packagesGrid.rowSelectCell.toggleAllSelection(false);
                        context.refreshGrid(response);
                    },
                    error: function (errMsg, errStack) {
                        context.showErrors(errMsg, errStack);
                    }
                });
            }
        },
        _onActivate: function (event) {
            var context = this;
            WsPackageMaps.activatePackageMap(this.packagesGrid.selection.getSelected(), {
                load: function (response) {
                    context.packagesGrid.rowSelectCell.toggleAllSelection(false);
                    context.refreshGrid();
                },
                error: function (errMsg, errStack) {
                    context.showErrors(errMsg, errStack);
                }
            });
        },
        _onDeactivate: function (event) {
            var context = this;
            WsPackageMaps.deactivatePackageMap(this.packagesGrid.selection.getSelected(), {
                load: function (response) {
                    context.packagesGrid.rowSelectCell.toggleAllSelection(false);
                    context.refreshGrid();
                },
                error: function (errMsg, errStack) {
                    context.showErrors(errMsg, errStack);
                }
            });
        },

        showErrors: function (errMsg, errStack) {
            dojo.publish("hpcc/brToaster", {
                Severity: "Error",
                Source: errMsg,
                Exceptions: [{ Message: errStack }]
            });
        },

        getSelections: function () {
            this.targets = new Array();
            ///this.processes = new Array();
            this.processFilters = new Array();

            var context = this;
            WsPackageMaps.GetPackageMapSelectOptions({
                    includeTargets: true,
                    IncludeProcesses: true,
                    IncludeProcessFilters: true
                }, {
                load: function (response) {
                    context.targetSelect.options.push({label: 'ANY', value: '' });
                    context.processSelect.options.push({label: 'ANY', value: '' });
                    if (lang.exists("Targets.TargetData", response)) {
                        context.targets = response.Targets.TargetData;
                        context.initSelections();
                    }
                    context.targetSelect.set("value", '');
                    context.processSelect.set("value", '');
                    if (lang.exists("ProcessFilters.Item", response)) {
                        context.processFilters = response.ProcessFilters.Item;
                    //    context.setSelections(context.processFilterSelect, context.processFilters, '*');
                    }
                    context.initPackagesGrid();
                },
                error: function (errMsg, errStack) {
                    context.showErrors(errMsg, errStack);
                }
            });
        },

        addProcessSelections: function (processSelect, processes, processData) {
            for (var i = 0; i < processData.length; ++i) {
                var process = processData[i];
                if ((processes != null) && (processes.indexOf(process) != -1))
                    continue;
                processes.push(process);
                processSelect.options.push({label: process, value: process});
            }
        },

        updateProcessSelections: function (processSelect, processes, targetName) {
            var options = processSelect.getOptions();
            for (var ii = 0; ii < options.length; ++ii) {
                var value = options[ii].value;
                processSelect.removeOption(value);
            }
            ///processSelect.removeOption(processSelect.getOptions());
            processSelect.options.push({label: 'ANY', value: '' });
            processes.length = 0;
            for (var i = 0; i < this.targets.length; ++i) {
                var target = this.targets[i];
                if ((target.Processes != undefined) && ((targetName == '') || (targetName == target.Name)))
                    this.addProcessSelections(processSelect, processes, target.Processes.Item);
            }
            processSelect.set("value", '');
        },

        initSelections: function () {
            if (this.targets.length < 1)
                return;

            for (var i = 0; i < this.targets.length; ++i) {
                var target = this.targets[i];
                this.targetSelect.options.push({label: target.Name, value: target.Name});
                if (target.Processes != undefined)
                    this.addProcessSelections(this.processSelect, this.processesToList, target.Processes.Item);
            }
            if (this.validateTab != null)
                this.validateTab.initSelections(this.targets);
        },

        init: function (params) {
            if (this.initalized)
                return;

            this.initalized = true;

            this.validateTab = new PackageMapValidateWidget({
                id: this.id + "_ValidatePackageMap",
                title: 'Validate PackageMap',
                params: params
            });
            //this.tabMap[this.id + "_ValidatePackageMap"] = this.validateTab;
            this.tabContainer.addChild(this.validateTab, 1);

            this.tabContainer.selectChild(this.packagesTab);
        },

        initPackagesGrid: function() {
            this.packagesGrid.setStructure([
                { name: "Package Map", field: "Id", width: "40%" },
                { name: "Target", field: "Target", width: "15%" },
                { name: "Process Filter", field: "Process", width: "15%" },
                {
                    name: "Active",
                    field: "Active",
                    width: "10%",
                    formatter: function (active) {
                        if (active == true) {
                            return "A";
                        }
                        return "";
                    }
                },
                { name: "Description", field: "Description", width: "20%" }
            ]);
            var objStore = ESPPackageProcess.CreatePackageMapQueryObjectStore();
            this.packagesGrid.setStore(objStore);
            this.packagesGrid.setQuery(this.getFilter());

            var context = this;
            this.packagesGrid.on("RowDblClick", function (evt) {
                if (context.onRowDblClick) {
                    var idx = evt.rowIndex;
                    var item = this.getItem(idx);
                    context.onRowDblClick(item);
                }
            }, true);

            dojo.connect(this.packagesGrid.selection, 'onSelected', function (idx) {
                context.refreshActionState();
            });
            dojo.connect(this.packagesGrid.selection, 'onDeselected', function (idx) {
                context.refreshActionState();
            });

            this.packagesGrid.startup();
        },

        getFilter: function () {
            this.targetSelected  = this.targetSelect.getValue();
            this.processSelected  = this.processSelect.getValue();
            //var processFilterSelected  = this.processFilterSelect.getValue();
            var processFilterSelected  = "*";
            if (this.targetSelected == " ")
                this.targetSelected = "";
            if (this.processSelected == " ")
                this.processSelected = "";
            if (processFilterSelected == "")
                processFilterSelected = "*";
            return {Target: this.targetSelected, Process: this.processSelected, ProcessFilter: processFilterSelected};
        },

        refreshGrid: function (args) {
            this.packagesGrid.setQuery(this.getFilter());
            var context = this;
            setTimeout(function () {
                context.refreshActionState()
            }, 200);
        },

        refreshActionState: function () {
            var selection = this.packagesGrid.selection.getSelected();
            var hasSelection = (selection.length > 0);
            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "Activate").set("disabled", selection.length != 1);
            registry.byId(this.id + "Deactivate").set("disabled", selection.length != 1);
        },

        showPackageMapDetails: function (id, params) {
            var obj = id.split(".");
            id = obj.join("");
            params.tabId = id;

            var retVal = this.tabMap[id];
            if (retVal)
                return retVal;

            var context = this;
            retVal = new PackageMapDetailsWidget({
                id: id,
                title: params.packageMap,
                closable: true,
                onClose: function () {
                    delete context.tabMap[id];
                    return true;
                },
                params: params
            });

            this.tabMap[id] = retVal;
            this.tabContainer.addChild(retVal, 2);

            var handle = topic.subscribe("packageMapDeleted", function(tabId){
                context.packageMapDeleted(tabId);
                handle.remove();
            });

            return retVal;
        },

        packageMapDeleted: function (tabId) {
            if (this.tabMap[tabId] == null)
                return;
            this.tabContainer.removeChild(this.tabMap[tabId]);
            this.tabMap[tabId].destroyRecursive();
            delete this.tabMap[tabId];

            this.tabContainer.selectChild(this.packagesTab);
            this.packagesGrid.rowSelectCell.toggleAllSelection(false);
            this.refreshGrid();
        },

        addPackageMapCallback: function (event) {
            //var processFilter = this.addPackageMapProcessSelect.getValue();
            this.addPackageMapDialog.onCancel();
            this.packagesGrid.rowSelectCell.toggleAllSelection(false);
            this.refreshGrid();
        }
    });
});
