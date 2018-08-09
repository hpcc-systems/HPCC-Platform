define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
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

    "dgrid/selector",

    "dojox/form/Uploader",
    "dojox/form/uploader/FileList",

    "hpcc/_TabContainerWidget",
    "hpcc/DelayLoadWidget",
    "hpcc/PackageMapValidateWidget",
    "src/WsPackageMaps",
    "src/ESPPackageProcess",
    "hpcc/SFDetailsWidget",
    "src/ESPUtil",
    "hpcc/FilterDropDownWidget",

    "dojo/text!../templates/PackageMapQueryWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domConstruct, domForm, ObjectStore, on, topic,
    _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry,
    selector,
    Uploader, FileUploader,
    _TabContainerWidget, DelayLoadWidget, PackageMapValidateWidget, WsPackageMaps, ESPPackageProcess, SFDetailsWidget, ESPUtil, FilterDropDownWidget,
    template) {
        return declare("PackageMapQueryWidget", [_TabContainerWidget], {
            templateString: template,
            baseClass: "PackageMapQueryWidget",
            i18n: nlsHPCC,
            packagesTab: null,
            packagesGrid: null,
            tabMap: [],
            addPackageMapDialog: null,
            validateTab: null,
            validateTabInitialized: false,
            filter: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.tabContainer = registry.byId(this.id + "TabContainer");
                this.packagesTab = registry.byId(this.id + "_Packages");
                this.packagesGrid = registry.byId(this.id + "PackagesGrid");
                this.targetSelect = registry.byId(this.id + "TargetSelect");
                this.processSelect = registry.byId(this.id + "ProcessSelect");
                this.processSelectFilter = registry.byId(this.id + "ProcessFilterSelect");
                this.addPackageTargetSelect = registry.byId(this.id + "AddProcessMapTargetSelect");
                this.addPackageProcessSelect = registry.byId(this.id + "AddProcessMapProcessSelect");
                this.addPackageProcessFilter = registry.byId(this.id + "AddProcessMapProcessFilter");
                this.addPackageMapDialog = registry.byId(this.id + "AddProcessMapDialog");
                this.filter = registry.byId(this.id + "Filter");
            },

            onRowDblClick: function (item) {
                var tab = this.showPackageMapDetails(item.Id, {
                    target: item.Target,
                    process: item.Process,
                    active: item.Active,
                    packageMap: item.Id
                });
                this.tabContainer.selectChild(tab);
            },

            _onRefresh: function (event) {
                this.refreshGrid();
            },

            _onOpen: function (event) {
                var selections = this.packagesGrid.getSelected();
                var firstTab = null;
                for (var i = selections.length - 1; i >= 0; --i) {
                    var tab = this.ensurePane(selections[i].Id, {
                        target: selections[i].Target,
                        process: selections[i].Process,
                        active: selections[i].Active,
                        packageMap: selections[i].Id
                    });
                    if (i === 0) {
                        firstTab = tab;
                    }
                }
                if (firstTab) {
                    this.selectChild(firstTab);
                }
            },

            _onAdd: function (event) {
                this.addPackageMapDialog.show();

                var context = this;
                var addPackageMapUploader = registry.byId(this.id + "AddProcessMapFileUploader");
                dojo.connect(addPackageMapUploader, "onComplete", this, function (e) {
                    registry.byId(this.id + "AddProcessMapDialogSubmit").set('disabled', false);
                    return context.addPackageMapCallback();
                });
                dojo.connect(addPackageMapUploader, "onBegin", this, function (e) {
                    registry.byId(this.id + "AddProcessMapDialogSubmit").set('disabled', true);
                    return;
                });
                var addPackageMapSubmitButton = registry.byId(this.id + "AddProcessMapDialogSubmit");
                dojo.connect(addPackageMapSubmitButton, "onClick", this, function (e) {
                    return context._onAddPackageMapSubmit();
                });
                var addPackageMapCloseButton = registry.byId(this.id + "AddProcessMapDialogClose");
                dojo.connect(addPackageMapCloseButton, "onClick", this, function (e) {
                    this.addPackageMapDialog.onCancel();
                });
            },

            _onAddProcessMapIdKeyUp: function () {
                this._onCheckAddProcessMapInput();
            },

            _onCheckAddProcessMapInput: function () {
                var id = registry.byId(this.id + "AddProcessMapId").get('value');
                var files = registry.byId(this.id + "AddProcessMapFileUploader").getFileList();
                if (files.length > 1) {
                    alert(this.i18n.Only1PackageFileAllowed);
                    return;
                }
                var fileName = '';
                if (files.length > 0)
                    fileName = files[0].name;
                if ((fileName !== '') && (id === '')) {
                    registry.byId(this.id + "AddProcessMapId").set('value', fileName);
                    registry.byId(this.id + "AddProcessMapDialogSubmit").set('disabled', false);
                } else if ((id === '') || (files.length < 1))
                    registry.byId(this.id + "AddProcessMapDialogSubmit").set('disabled', true);
                else
                    registry.byId(this.id + "AddProcessMapDialogSubmit").set('disabled', false);
            },

            _onAddPackageMapSubmit: function () {
                var target = this.addPackageMapTargetSelect.getValue();
                var id = registry.byId(this.id + "AddProcessMapId").get('value');
                var process = this.addPackageMapProcessSelect.getValue();
                var daliIp = registry.byId(this.id + "AddProcessMapDaliIP").get('value');
                var activate = registry.byId(this.id + "AddProcessMapActivate").get('checked');
                var overwrite = registry.byId(this.id + "AddProcessMapOverWrite").get('checked');
                if ((id === '') || (target === ''))
                    return false;
                if ((process === '') || (process === this.i18n.ANY))
                    process = '*';

                var action = "/WsPackageProcess/AddPackage?upload_&PackageMap=" + id + "&Target=" + target;
                if (process !== '')
                    action += "&Process=" + process;
                if (daliIp !== '')
                    action += "&DaliIp=" + daliIp;
                if (activate)
                    action += "&Activate=1";
                else
                    action += "&Activate=0";
                if (overwrite)
                    action += "&OverWrite=1";
                else
                    action += "&OverWrite=0";
                var theForm = registry.byId(this.id + "AddProcessMapForm");
                if (theForm === undefined)
                    return false;
                theForm.set('action', action);
                return true;
            },
            _onDelete: function (event) {
                if (confirm('Delete selected packages?')) {
                    var context = this;
                    WsPackageMaps.deletePackageMap(this.packagesGrid.selection.getSelected()).then(function (response) {
                        context.packagesGrid.rowSelectCell.toggleAllSelection(false);
                        context.refreshGrid(response.DeletePackageResponse);
                        return response;
                    }, function (err) {
                        context.showErrors(err);
                        return err;
                    });
                }
            },
            _onActivate: function (event) {
                var context = this;
                WsPackageMaps.activatePackageMap(this.packagesGrid.selection.getSelected()).then(function (response) {
                    context.packagesGrid.rowSelectCell.toggleAllSelection(false);
                    context.refreshGrid();
                    return response;
                }, function (err) {
                    context.showErrors(err);
                    return err;
                });
            },
            _onDeactivate: function (event) {
                var context = this;
                WsPackageMaps.deactivatePackageMap(this.packagesGrid.selection.getSelected()).then(function (response) {
                    context.packagesGrid.rowSelectCell.toggleAllSelection(false);
                    context.refreshGrid();
                    return response;
                }, function (err) {
                    context.showErrors(err);
                    return err;
                });
            },

            showErrors: function (err) {
                topic.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: err.message,
                    Exceptions: [{ Message: err.stack }]
                });
            },

            addProcessSelections: function (processSelect, processes, processData) {
                for (var i = 0; i < processData.length; ++i) {
                    var process = processData[i];
                    if ((processes != null) && (processes.indexOf(process) !== -1))
                        continue;
                    processes.push(process);
                    processSelect.options.push({ label: process, value: process });
                }
            },

            init: function (params) {
                var context = this;
                if (this.inherited(arguments))
                    return;

                this.params = params;

                this.targetSelect.init({
                    GetPackageMapTargets: true
                });
                
                this.processSelect.init({
                    GetPackageMapProcesses: true
                });

                this.processSelectFilter.init({
                    GetPackageMapProcessFilter: true
                })

                this.addPackageTargetSelect.init({
                    GetPackageMapTargets: true
                });

                this.addPackageProcessSelect.init({
                    GetPackageMapProcesses: true
                });

                this.initPackagesGrid();

                this.filter.on("clear", function (evt) {
                    context._onFilterType();
                    context.refreshHRef();
                    context.refreshGrid();
                });
                this.filter.on("apply", function (evt) {
                    context.refreshHRef();
                    context.refreshGrid();
                });
            },

            initTab: function () {
                var currSel = this.getSelectedChild();
                if (currSel && !currSel.initalized) {
                    if (currSel.id === this.packagesTab.id) {
                    } else {
                        if (!currSel.initalized) {
                            currSel.init(currSel.params);
                        }
                    }
                }
            },

            initValidateTab: function (id, params) {
                id = this.createChildTabID(id);
                var retVal = new DelayLoadWidget({
                    id: id,
                    title: params.title,
                    closable: false,
                    delayWidget: "PackageMapValidateWidget",
                    params: params
                });
                this.tabContainer.addChild(retVal, 1);
                return retVal;
            },

            initPackagesGrid: function () {
                var context = this;
                this.store = ESPPackageProcess.CreatePackageMapQueryObjectStore();
                this.packagesGrid = new declare([ESPUtil.Grid(true, true)])({
                    store: this.store,
                    query: this.getFilter(),
                    columns: {
                        col1: selector({
                            width: 27,
                            selectorType: 'checkbox'
                        }),
                        Id: {
                            width: "40%",
                            sortable: false,
                            label: this.i18n.PackageMap,
                            formatter: function (Id, idx) {
                                return "<a href='#' class='dgrid-row-url'>" + Id + "</a>";
                            }
                        },
                        Target: {
                            width: "15%",
                            sortable: false,
                            label: this.i18n.Target
                        },
                        Process: {
                            width: "15%",
                            sortable: false,
                            label: this.i18n.ProcessFilter
                        },
                        Active: {
                            width: "10%",
                            sortable: false,
                            label: this.i18n.Active,
                            formatter: function (active) {
                                if (active === true) {
                                    return "A";
                                }
                                return "";
                            }
                        },
                        Description: {
                            width: "20%",
                            sortable: false,
                            label: this.i18n.Description,
                        }
                    }
                }, this.id + "PackagesGrid");

                this.packagesGrid.on(".dgrid-row-url:click", function (evt) {
                    if (context._onRowDblClick) {
                        var item = context.packagesGrid.row(evt).data;
                        context._onRowDblClick(item.Id, item.Target, item.Process, item.Active);
                    }
                });
                this.packagesGrid.on(".dgrid-row:dblclick", function (evt) {
                    if (context._onRowDblClick) {
                        var item = context.packagesGrid.row(evt).data;
                        context._onRowDblClick(item.Id, item.Target, item.Process, item.Active);
                    }
                });

                this.packagesGrid.onSelectionChanged(function (event) {
                    context.refreshActionState();
                });

                this.packagesGrid.startup();
            },

            _onRowDblClick: function (id, target, process, active) {
                var packageTab = this.ensurePane(id, {
                    target: target,
                    process: process,
                    active: active,
                    packageMap: id
                });
                this.selectChild(packageTab);
            },

            getFilter: function () {
                return {
                    Target: this.targetSelect.getValue(),
                    Process: this.processSelect.getValue(),
                    ProcessFilter: this.processSelectFilter.getValue()
                };
            },

            refreshGrid: function (clearSelection) {
                this.packagesGrid.set("query", this.getFilter());
                if (clearSelection) {
                    this.packagesGrid.clearSelection();
                }
            },

            refreshActionState: function () {
                var selection = this.packagesGrid.getSelected();
                var hasSelection = (selection.length > 0);
                registry.byId(this.id + "Open").set("disabled", !hasSelection);
                registry.byId(this.id + "Delete").set("disabled", !hasSelection);
                registry.byId(this.id + "Activate").set("disabled", selection.length !== 1);
                registry.byId(this.id + "Deactivate").set("disabled", selection.length !== 1);
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
                this.addPackageMapDialog.onCancel();
                this.refreshGrid();
            },

            ensurePane: function (id, params) {
                id = this.createChildTabID(id);
                var retVal = registry.byId(id);
                if (!retVal) {
                    var context = this;
                    retVal = new DelayLoadWidget({
                        id: id,
                        title: params.packageMap,
                        closable: true,
                        delayWidget: "PackageMapDetailsWidget",
                        params: params
                    });
                    this.addChild(retVal, 1);
                }
                return retVal;
            }
        });
    });
