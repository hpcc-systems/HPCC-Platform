define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/_base/lang",

    "dijit/form/CheckBox",
    "dijit/form/ValidationTextBox",
    "dijit/registry",
    "dijit/form/ToggleButton",
    "dijit/form/Select",
    "dijit/ToolbarSeparator",

    "dgrid/tree",
    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "hpcc/PreflightDetailsWidget",
    "src/ESPTopology",
    "hpcc/TopologyDetailsWidget",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil",
    "hpcc/FilterDropDownWidget",
    "src/ws_machine",
    "src/Utility"

], function (declare, nlsHPCCMod, lang,
    CheckBox, ValidationTextBox, registry, ToggleButton, Select, ToolbarSeparator,
    tree, selector,
    GridDetailsWidget, PreflightDetailsWidget, ESPTopology, TopologyDetailsWidget, DelayLoadWidget, ESPUtil, FilterDropDownWidget, WsMachine, Utility) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("TopologyWidget", [GridDetailsWidget], {

        i18n: nlsHPCC,
        gridTitle: nlsHPCC.title_Topology,
        idProperty: "__hpcc_id",
        filter: null,
        filterLoaded: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.detailsWidget = new TopologyDetailsWidget({
                id: this.id + "Details",
                region: "right",
                splitter: true,
                style: "width: 80%",
                minSize: 240
            });
            this.detailsWidget.placeAt(this.gridTab, "last");
            this.filter = new FilterDropDownWidget({});
            this.filter.init({
                ownLabel: this.i18n.MachineInformation
            });
        },

        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;
            this.detailsWidget.requestInformationWidget.set("disabled", true);
            this.filter.disable(true);
            this.refreshGrid();

            this.filter.on("apply", function (evt) {
                context.refreshHRef();
                var selection = context.grid.getSelected();
                var filter = context.getFilter();

                var MachineInformationCount = 0;
                var TargetClusterCount = 0;
                for (var i = 0; i < selection.length; ++i) {
                    if (context.viewModeMachines.checked || context.viewModeServices.checked) {
                        var MachineInformationClean = "Addresses." + i;
                        MachineInformationCount++;

                        var request = {
                            Path: selection[i].__hpcc_treeItem.Path,
                            Cluster: selection[i].__hpcc_treeItem.Name,
                            AutoRefresh: filter.AutoRefresh,
                            MemThreshold: filter.MemThreshold,
                            CpuThreshold: filter.CpuThreshold,
                            MemThresholdType: filter.MemThreshold,
                            GetProcessorInfo: filter.GetProcessorInfo,
                            GetStorageInfo: filter.GetStorageInfo,
                            LocalFileSystemsOnly: filter.LocalFileSystemsOnly,
                            GetSoftwareInfo: filter.GetSoftwareInfo,
                            DiskThreshold: filter.DiskThreshold,
                            DiskThresholdType: filter.DiskThresholdType,
                            ApplyProcessFilter: filter.ApplyProcessFilter,
                            AddProcessesToFilter: filter.AddtionalProcessesToFilter,
                            cbAutoRefresh: filter.cbAutoRefresh
                        };

                        if (context.viewModeMachines.checked) {
                            filter[MachineInformationClean] = selection[i].getNetaddress() + "|:" + selection[i].__hpcc_treeItem.Type + ":" + selection[i].__hpcc_treeItem.Name + ":" + 2 + ":" + selection[i].__hpcc_treeItem.Directory + ":" + 0;
                        }
                        if (context.viewModeServices.checked) {
                            filter[MachineInformationClean] = selection[i].getNetaddress() + "|:" + selection[i].getNetaddress() + "|:" + selection[i].__hpcc_treeItem.Type + ":" + selection[i].__hpcc_treeItem.Name + ":" + 2 + ":" + selection[i].__hpcc_treeItem.Directory;
                        }

                        request[MachineInformationClean] = filter[MachineInformationClean];
                        request["Addresses.itemcount"] = MachineInformationCount;
                        WsMachine.GetMachineInfo({
                            request: request
                        }).then(function (response) {
                            var pfTab = context.ensureMIPane(response.GetMachineInfoResponse.Machines.MachineInfoEx[0].Address, {
                                params: response.GetMachineInfoResponse
                            });
                            pfTab.init(response.GetMachineInfoResponse, "machines");
                        });
                    } else {
                        var TargetClustersClean = "TargetClusters." + i;
                        TargetClusterCount++;
                        filter[TargetClustersClean] = selection[i].__hpcc_treeItem.Type + ":" + selection[i].__hpcc_treeItem.Name;
                        var request = {
                            AutoRefresh: filter.AutoRefresh,
                            MemThreshold: filter.MemThreshold,
                            CpuThreshold: filter.CpuThreshold,
                            MemThresholdType: filter.MemThreshold,
                            GetProcessorInfo: filter.GetProcessorInfo,
                            GetStorageInfo: filter.GetStorageInfo,
                            LocalFileSystemsOnly: filter.LocalFileSystemsOnly,
                            GetSoftwareInfo: filter.GetSoftwareInfo,
                            DiskThreshold: filter.DiskThreshold,
                            DiskThresholdType: filter.DiskThresholdType,
                            ApplyProcessFilter: filter.ApplyProcessFilter,
                            AddProcessesToFilter: filter.AddtionalProcessesToFilter,
                            cbAutoRefresh: filter.cbAutoRefresh
                        };
                        request[TargetClustersClean] = filter[TargetClustersClean];
                        request["TargetClusters.itemcount"] = TargetClusterCount;
                        WsMachine.GetTargetClusterInfo({
                            request: request
                        }).then(function (response) {
                            if (lang.exists("GetTargetClusterInfoResponse", response)) {
                                var pfTab = context.ensureTCPane(response.GetTargetClusterInfoResponse.TargetClusterInfoList.TargetClusterInfo[0].Name + response.GetTargetClusterInfoResponse.TimeStamp, {
                                    params: response.GetTargetClusterInfoResponse
                                });
                                context.detailsWidget.requestInformationWidget.set("disabled", false);
                                context.detailsWidget.requestInformationWidget.init(response.GetTargetClusterInfoResponse);
                                pfTab.init(response.GetTargetClusterInfoResponse, "cluster");
                            }
                        });
                    }
                }
            });
        },

        resetFilter: function () {
            this.filter.filterForm.reset();
            this.filter.iconFilter.src = Utility.getImageURL("noFilter1.png");
            this.filter.disable(true);
        },

        createGrid: function (domID) {
            var context = this;
            this.openButton = registry.byId(this.id + "Open");
            dojo.destroy(this.id + "Open");
            this.filter.placeAt(this.openButton.domNode, "after");
            this.filter.filterForm.set("style", "width:600px;");
            this.filter.filterDropDown.set("label", context.i18n.MachineInformation);
            this.filter.filterClear.set("disabled", true);

            this.viewModeDebug = new ToggleButton({
                showLabel: true,
                checked: false,
                style: { display: "none" },
                onChange: function (val) {
                    if (val) {
                        context.viewModeMachines.set("checked", false);
                        context.viewModeServices.set("checked", false);
                        context.viewModeTargets.set("checked", false);
                        context.refreshGrid("Debug");
                        context.resetFilter();
                    }
                },
                label: "Debug"
            }).placeAt(this.openButton.domNode, "after");
            this.viewModeMachines = new ToggleButton({
                showLabel: true,
                checked: false,
                onChange: function (val) {
                    if (val) {
                        context.viewModeDebug.set("checked", false);
                        context.viewModeServices.set("checked", false);
                        context.viewModeTargets.set("checked", false);
                        context.refreshGrid("Machines");
                        context.resetFilter();
                    }
                },
                label: this.i18n.ClusterProcesses
            }).placeAt(this.openButton.domNode, "after");
            this.viewModeServices = new ToggleButton({
                showLabel: true,
                checked: false,
                onChange: function (val) {
                    if (val) {
                        context.viewModeDebug.set("checked", false);
                        context.viewModeMachines.set("checked", false);
                        context.viewModeTargets.set("checked", false);
                        context.refreshGrid("Services");
                        context.resetFilter();
                    }
                },
                label: this.i18n.SystemServers
            }).placeAt(this.openButton.domNode, "after");
            this.viewModeTargets = new ToggleButton({
                showLabel: true,
                checked: true,
                onChange: function (val) {
                    if (val) {
                        context.viewModeDebug.set("checked", false);
                        context.viewModeMachines.set("checked", false);
                        context.viewModeServices.set("checked", false);
                        context.refreshGrid("Targets");
                        context.resetFilter();
                    }
                },
                label: this.i18n.TargetClusters
            }).placeAt(this.openButton.domNode, "after");

            new ToolbarSeparator().placeAt(this.viewModeMachines.domNode, "after");

            this.machineInformationDropDown = this.createLabelAndElement("machineinformation", "Machine Information", "Select", this.i18n.MachineInformation, [{ label: this.i18n.MachineInformation, value: "GetMachineInfo", selected: true }]);
            this.getProcessorInformation = this.createLabelAndElement("GetProcessorInfo", this.i18n.ProcessorInformation, "CheckBox");
            this.getStorageInformation = this.createLabelAndElement("GetStorageInfo", this.i18n.StorageInformation, "CheckBox");
            this.localFileSystemsOnly = this.createLabelAndElement("LocalFileSystemsOnly", this.i18n.LocalFileSystemsOnly, "CheckBox");
            this.getSoftwareInformation = this.createLabelAndElement("GetSoftwareInfo", this.i18n.GetSoftwareInformation, "CheckBox");
            this.showProcessesUsingFilter = this.createLabelAndElement("ApplyProcessFilter", this.i18n.ShowProcessesUsingFilter, "CheckBox");
            this.additionalProcessesFilter = this.createLabelAndElement("AddProcessesToFilter", this.i18n.AddtionalProcessesToFilter, "TextBox", this.i18n.AnyAdditionalProcessesToFilter);
            this.autoRefresh = this.createLabelAndElement("cbAutoRefresh", this.i18n.AutoRefresh, "CheckBox");
            this.autoRefreshEvery = this.createLabelAndElement("AutoRefresh", this.i18n.AutoRefreshIncrement, "TextBox", this.i18n.AutoRefreshEvery, 5);
            this.warnifcpuusageisover = this.createLabelAndElement("CpuThreshold", this.i18n.WarnIfCPUUsageIsOver, "TextBox", this.i18n.EnterAPercentage, 95);
            this.warnifavailablememoryisunder = this.createLabelAndElement("MemThreshold", this.i18n.WarnIfAvailableMemoryIsUnder, "TextBox", this.i18n.EnterAPercentageOrMB, 5);
            this.warnifavailablememoryisunderthreshold = this.createLabelAndElement("MemThresholdType", "", "SelectMini", "Threshold", [{ label: "%", value: 0, selected: true }, { label: "MB", value: 1 }]);
            this.warnifavailablediskisunder = this.createLabelAndElement("DiskThreshold", this.i18n.WarnIfAvailableDiskSpaceIsUnder, "TextBox", this.i18n.EnterAPercentageOrMB, 5);
            this.warnifdiskspaceisunder = this.createLabelAndElement("DiskThresholdType", "", "SelectMini", "Threshold", [{ label: "%", value: 0, selected: true }, { label: "MB", value: 1 }]);

            this.store = new ESPTopology.Store();
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                deselectOnRefresh: true,
                columns: [
                    selector({
                        width: 18,
                        selectorType: "checkbox",
                        sortable: false,
                        disabled: function (item) {
                            if (item.__hpcc_treeItem) {
                                if (context.viewModeTargets.checked) {
                                    if (item.__hpcc_treeItem.Type === "HoleCluster" || item.__hpcc_treeItem.Type === "ThorCluster" || item.__hpcc_treeItem.Type === "RoxieCluster" || item.hasLogs()) {
                                        return false;
                                    }
                                }
                                if (context.viewModeServices.checked) {
                                    if (item.__hpcc_treeItem.getNetaddress()) {
                                        return false;
                                    }
                                }
                                if (context.viewModeMachines.checked) {
                                    if (item.__hpcc_children.length > 0 || item.hasLogs()) {
                                        return false;
                                    }
                                }
                            }
                            return true;
                        }
                    }),
                    tree({
                        field: "__hpcc_displayName",
                        label: this.i18n.Topology,
                        width: 130,
                        collapseOnRefresh: false,
                        shouldExpand: function (row, level, previouslyExpanded) {
                            if (previouslyExpanded !== undefined) {
                                return previouslyExpanded;
                            } else if (level < -1) {
                                return true;
                            }
                            return false;
                        },
                        formatter: function (_id, row) {
                            return "<img src='" + Utility.getImageURL(row.getIcon()) + "'/>&nbsp;" + row.getLabel();
                        }
                    })
                ]
            }, domID);

            retVal.on("dgrid-select", function (event) {
                var selection = context.grid.getSelected();
                if (selection.length) {
                    context.detailsWidget.init(selection[0]);
                    if (context.viewModeTargets.checked === true && selection[0].__hpcc_parentNode && selection[0].hasLogs().length === 0) {
                        context.filter.disable(false);
                    } else if (context.viewModeServices.checked === true && selection[0].__hpcc_parentNode) {
                        context.filter.disable(false);
                    } else if (context.viewModeMachines.checked === true && selection) {
                        context.filter.disable(false);
                    }
                    else {
                        context.filter.disable(true);
                    }
                }
            });
            retVal.on("dgrid-deselect", function (event) {
                var selection = context.grid.getSelected();
                if (selection.length === 0) {
                    context.filter.disable(true);
                } else {
                    context.filter.disable(false);
                }
            });
            return retVal;
        },

        createDetail: function (id, row, params) {
            return new DelayLoadWidget({
                id: id,
                title: row.__hpcc_displayName,
                closable: true,

                delayWidget: "TopologyDetailsWidget",
                hpcc: {
                    params: row
                }
            });
        },

        listenAndDisable: function (state, id) {
            switch (id) {
                case "GetStorageInfo":
                    if (state === false) {
                        dijit.byId("LocalFileSystemsOnly").set("checked", false);
                        dijit.byId("LocalFileSystemsOnly").set("disabled", true);
                    } else if (state === "on") {
                        dijit.byId("GetStorageInfo").set("checked", true);
                        dijit.byId("LocalFileSystemsOnly").set("checked", true);
                        dijit.byId("LocalFileSystemsOnly").set("disabled", false);
                    }
                    break;
                case "GetSoftwareInfo":
                    if (state === false) {
                        dijit.byId("ApplyProcessFilter").set("disabled", true);
                        dijit.byId("ApplyProcessFilter").set("checked", false);
                        dijit.byId("AddProcessesToFilter").set("disabled", true);
                    } else {
                        dijit.byId("ApplyProcessFilter").set("disabled", false);
                        dijit.byId("ApplyProcessFilter").set("checked", true);
                        dijit.byId("AddProcessesToFilter").set("disabled", false);
                    }
                    break;
                case "ApplyProcessFilter":
                    if (state === false) {
                        dijit.byId("AddProcessesToFilter").set("disabled", true);
                    } else {
                        dijit.byId("ApplyProcessFilter").set("checked", true);
                        dijit.byId("AddProcessesToFilter").set("disabled", false);
                    }
                    break;
                case "cbAutoRefresh":
                    if (state === false) {
                        dijit.byId("AutoRefresh").set("disabled", true);
                    } else {
                        dijit.byId("cbAutoRefresh").set("checked", true);
                        dijit.byId("AutoRefresh").set("disabled", false);
                    }
            }
        },

        createLabelAndElement: function (id, label, element, placeholder, value) {
            var context = this;
            var control = null;
            switch (element) {
                case "CheckBox":
                    control = new CheckBox({
                        id: id,
                        name: id,
                        checked: true,
                        title: label,
                        onChange: function (b) {
                            var state = this.get("value");
                            context.listenAndDisable(state, id);
                        }
                    });
                    break;
                case "TextBox":
                    control = new ValidationTextBox({
                        id: id,
                        name: id,
                        placeholder: placeholder,
                        style: "width: 40%",
                        value: value
                    });
                    break;
                case "Select":
                    control = new Select({
                        id: id,
                        name: id,
                        placeholder: placeholder,
                        style: "width: 40%",
                        options: value
                    });
                    break;
                case "SelectMini":
                    control = new Select({
                        id: id,
                        name: id,
                        placeholder: placeholder,
                        "class": "miniSelect",
                        options: value
                    });
                    break;
            }
            if (control) {
                this.filter.tableContainer.domNode.appendChild(
                    dojo.create(label ? "div" : "span", {
                        id: this.id + id,
                        innerHTML: label ? "<label for=" + control + " style='float:left;width:40%'>" + label + ":</label>" : "",
                        style: "vertical-align:middle;padding:2px 0 2px 5px;"
                    })
                );
                control.placeAt(this.id + id);
            }
        },

        getFilter: function () {
            var retVal = this.filter.toObject();

            if (retVal.ApplyProcessFilter === "on") {
                lang.mixin(retVal, {
                    ApplyProcessFilter: 1
                });
            } if (retVal.GetProcessorInfo === "on") {
                lang.mixin(retVal, {
                    GetProcessorInfo: 1
                });
            } if (retVal.GetSoftwareInfo === "on") {
                lang.mixin(retVal, {
                    GetSoftwareInfo: 1
                });
            } if (retVal.GetStorageInfo === "on") {
                lang.mixin(retVal, {
                    GetStorageInfo: 1
                });
            } if (retVal.LocalFileSystemsOnly === "on") {
                lang.mixin(retVal, {
                    LocalFileSystemsOnly: 1
                });
            }
            return retVal;
        },

        ensureTCPane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                retVal = new PreflightDetailsWidget({
                    id: id,
                    title: this.i18n.Fetched + ": " + params.params.TimeStamp + " <b>(" + params.params.TargetClusterInfoList.TargetClusterInfo[0].Name + ")</b>",
                    closable: true,
                    params: params.params
                });
                this._tabContainer.addChild(retVal, "last");
            }
            return retVal;
        },

        ensureMIPane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                retVal = new PreflightDetailsWidget({
                    id: id,
                    style: "width: 100%",
                    params: params.params,
                    closable: true,
                    title: params.params.Machines.MachineInfoEx[0].Address
                });
                this._tabContainer.addChild(retVal, "last");
            }
            return retVal;
        },

        refreshGrid: function (mode) {
            var context = this;
            if (mode) {
                this.store.viewMode(mode);
                this.grid.refresh();
            } else if (this.store._viewMode === "Targets") {
                this.grid.refresh();
            } else if (this.store._viewMode === "Services") {
                this.grid.refresh();
            } else if (this.store._viewMode === "Machines") {
                this.grid.refresh();
            }
            else {
                this.store.viewMode("Targets");
                this.store.refresh(function () {
                    context.grid.refresh();
                });
            }
        },

        refreshActionState: function () {

        }
    });
});
