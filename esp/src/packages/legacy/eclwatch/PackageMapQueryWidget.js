define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/topic",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "hpcc/DelayLoadWidget",
    "src/WsPackageMaps",
    "src/ESPPackageProcess",
    "src/ESPUtil",

    "dojo/text!../templates/PackageMapQueryWidget.html",

    "hpcc/TargetSelectWidget",
    "hpcc/FilterDropDownWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/DropDownButton",
    "dijit/form/Select",
    "dijit/form/Textarea",
    "dijit/Fieldset",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog"
], function (declare, lang, nlsHPCCMod, arrayUtil, dom, topic,
    _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry,
    selector,
    _TabContainerWidget, DelayLoadWidget, WsPackageMaps, ESPPackageProcess, ESPUtil,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
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
            this.validateTab = registry.byId(this.id + "_Validate");
            this.packagesGrid = registry.byId(this.id + "PackagesGrid");
            this.targetSelect = registry.byId(this.id + "TargetSelect");
            this.processSelect = registry.byId(this.id + "ProcessSelect");
            this.processSelectFilter = registry.byId(this.id + "ProcessFilterSelect");
            this.addPackageMapForm = registry.byId(this.id + "AddProcessMapForm");
            this.addProcessMapId = registry.byId(this.id + "AddProcessMapId");
            this.addPackageMapContent = registry.byId(this.id + "AddPackagemapContent");
            this.addPackageTargetSelect = registry.byId(this.id + "AddProcessMapTargetSelect");
            this.addPackageProcessSelect = registry.byId(this.id + "AddProcessMapProcessSelect");
            this.addPackageProcessFilter = registry.byId(this.id + "AddProcessMapProcessFilter");
            this.addPackageMapDialog = registry.byId(this.id + "AddProcessMapDialog");
            this.addPackageMapContent = dom.byId("AddPackagemapContent");
            this.addPackageMapSubmit = registry.byId(this.id + "AddProcessMapDialogSubmit");
            this.addPackageMapCloseButton = registry.byId(this.id + "AddProcessMapDialogClose");
            this.addPackageMapUploader = registry.byId(this.id + "AddProcessMapFileUploader");
            this.addProcessMapDaliIp = registry.byId(this.id + "AddProcessMapDaliIP");
            this.addProcessMapActivate = registry.byId(this.id + "AddProcessMapActivate");
            this.addProcessMapOverWrite = registry.byId(this.id + "AddProcessMapOverWrite");
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

        _onCancel: function (event) {
            this.addPackageMapDialog.hide();
            this.addPackageMapContent.value = "";
        },

        _onAdd: function (event) {
            this.addPackageMapDialog.show();
        },

        _onAddPackageMapSubmit: function () {
            var context = this;

            if (this.addPackageMapForm.validate()) {
                WsPackageMaps.AddPackage({
                    request: {
                        Info: this.addPackageMapContent.value,
                        PackageMap: this.addProcessMapId.get("value"),
                        Process: this.addPackageProcessSelect.get("value"),
                        Target: this.addPackageTargetSelect.get("value"),
                        Activate: this.addProcessMapActivate.get("checked"),
                        OverWrite: this.addProcessMapOverWrite.get("checked"),
                        DaliIp: this.addProcessMapDaliIp.get("value")
                    }
                }).then(function (response) {
                    if (lang.exists("AddPackageResponse.status", response)) {
                        if (response.AddPackageResponse.status.Code === 0) {
                            context.refreshGrid();
                            context._onCancel();
                        }
                    } else {
                        context._onCancel();
                        context.showErrors(response.Exceptions.Exception[0].Message);
                    }
                });
            }
        },

        _onDelete: function (event) {
            var context = this;
            var selection = this.packagesGrid.getSelected();

            if (confirm(this.i18n.DeleteSelectedPackages)) {
                arrayUtil.forEach(selection, function (item, idx) {
                    WsPackageMaps.deletePackageMap({
                        request: {
                            PackageMap: item.Id,
                            Target: item.Target,
                            Process: item.Process
                        }
                    }).then(function (response) {
                        if (lang.exists("DeletePackageResponse.status", response)) {
                            if (response.DeletePackageResponse.status.Code === 0) {
                                context.refreshGrid();
                            }
                        } else {
                            context.showErrors(response.Exceptions.Exception[0].Message);
                        }
                    });
                });
            }
        },

        _onActivate: function (event) {
            var context = this;
            var selection = this.packagesGrid.getSelected();

            WsPackageMaps.activatePackageMap({
                request: {
                    Target: selection[0].Target,
                    Process: selection[0].Process,
                    PackageMap: selection[0].Id
                }
            }).then(function (response) {
                if (lang.exists("ActivatePackageResponse.status", response)) {
                    if (response.ActivatePackageResponse.status.Code === 0) {
                        context.refreshGrid();
                    }
                }
            });
        },
        _onDeactivate: function (event) {
            var context = this;
            var selection = this.packagesGrid.getSelected();

            WsPackageMaps.deactivatePackageMap({
                request: {
                    Target: selection[0].Target,
                    Process: selection[0].Process,
                    PackageMap: selection[0].Id
                }
            }).then(function (response) {
                if (lang.exists("DeActivatePackageResponse.status", response)) {
                    if (response.DeActivatePackageResponse.status.Code === 0) {
                        context.refreshGrid();
                    }
                }
            });
        },

        showErrors: function (err) {
            topic.publish("hpcc/brToaster", {
                Severity: "Error",
                Source: err.message,
                Exceptions: [{ Message: err.stack }]
            });
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
            });

            this.addPackageTargetSelect.init({
                GetPackageMapTargets: true
            });

            this.addPackageProcessSelect.init({
                GetPackageMapProcesses: true
            });

            this.filter.on("clear", function (evt) {
                context._onFilterType();
                context.refreshHRef();
                context.refreshGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshHRef();
                context.packagesGrid._currentPage = 0;
                context.refreshGrid();
            });
            this.initPackagesGrid();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.packagesTab.id) {
                } else if (currSel.id === this.validateTab.id) {
                    currSel.init({
                        targets: this.targetSelect.options
                    });
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel.params);
                    }
                }
            }
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
                        selectorType: "checkbox"
                    }),
                    Id: {
                        width: "40%",
                        sortable: false,
                        label: this.i18n.PackageMap,
                        formatter: function (Id, idx) {
                            return "<a href='#' onClick='return false;' class='dgrid-row-url'>" + Id + "</a>";
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
            var retVal = this.filter.toObject();
            return retVal;
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
                this.addChild(retVal, 2);
            }
            return retVal;
        }
    });
});
