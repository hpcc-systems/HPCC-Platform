define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-style",
    "dojo/topic",

    "dijit/registry",

    "hpcc/_Widget",
    "src/ws_machine",

    "dojo/text!../templates/MachineInformationWidget.html",

    "dijit/form/Select",
    "dijit/form/CheckBox",
    "dijit/form/DropDownButton",
    "dijit/TooltipDialog",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/NumberTextBox",

    "hpcc/TableContainer"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domStyle, topic,
    registry,
    _Widget, WsMachine,
    template) {
    return declare("MachineInformationWidget", [_Widget], {
        templateString: template,
        baseClass: "MachineInformationWidget",
        i18n: nlsHPCC,

        _width: "100%",
        iconFilter: null,
        machineDropDown: null,
        machineForm: null,
        machineLabel: null,
        machineMessage: null,
        tableContainer: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.machineDropDown = registry.byId(this.id + "MachineDropDown");
            this.machineForm = registry.byId(this.id + "MachineForm");
            this.machineLabel = registry.byId(this.id + "MachineLabel");
            this.tableContainer = registry.byId(this.id + "TableContainer");
            this.machineApply = registry.byId(this.id + "MachineApply");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        //  Hitched actions  ---
        _onFilterClear: function (event) {
            this.emit("clear");
            this.clear();
        },

        _onMachineApply: function (event) {
            this.machineDropDown.closeDropDown();
            this.emit("apply");
            this.refreshState();
        },

        //  Implementation  ---

        setValue: function (id, value) {
            registry.byId(id).set("value", value);
            this.refreshState();
        },

        exists: function () {
            var filter = this.toObject();
            for (var key in filter) {
                if (filter[key] !== "") {
                    return true;
                }
            }
            return false;
        },

        toObject: function () {
            if (this.machineDropDown.get("disabled")) {
                return {};
            }
            var retVal = {};
            arrayUtil.forEach(this.machineForm.getDescendants(), function (item, idx) {
                var name = item.get("name");
                if (name) {
                    var value = item.get("value");
                    if (value) {
                        retVal[name] = value;
                    }
                }
            });
            return retVal;
        },

        fromObject: function (obj) {
            arrayUtil.forEach(this.machineForm.getDescendants(), function (item, idx) {
                var value = obj[item.get("name")];
                if (value) {
                    item.set("value", value);
                    if (item.defaultValue !== undefined) {
                        item.defaultValue = value;
                    }
                }
            });
            this.refreshState();
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;
        },

        open: function (event) {
            this.machineDropDown.focus();
            this.machineDropDown.openDropDown();
        },

        close: function (event) {
            this.machineDropDown.closeDropDown();
        },

        disable: function (disable) {
            this.machineDropDown.set("disabled", disable);
        },

        reset: function (disable) {
            this.filterForm.reset();
        },

        refreshState: function () {
            if (this.exists()) {
                dom.byId(this.id + "MachineDropDown_label").innerHTML = this.i18n.Preflight;
                domStyle.set(this.id + "MachineDropDown_label", {
                    "font-weight": "normal"
                });
            }
        },

        _onListenAndDisable: function (event) {
            switch (event.target.name) {
                case "GetStorageInfo":
                    if (event.target.checked === false) {
                        registry.byId(this.id + "LocalFileSystemsOnly").set("checked", false);
                        registry.byId(this.id + "LocalFileSystemsOnly").set("disabled", true);
                    } else if (event.target.checked === true) {
                        registry.byId(this.id + "GetStorageInfo").set("checked", true);
                        registry.byId(this.id + "LocalFileSystemsOnly").set("checked", true);
                        registry.byId(this.id + "LocalFileSystemsOnly").set("disabled", false);
                    }
                    break;
                case "GetSoftwareInfo":
                    if (event.target.checked === false) {
                        registry.byId(this.id + "ApplyProcessFilter").set("disabled", true);
                        registry.byId(this.id + "ApplyProcessFilter").set("checked", false);
                        registry.byId(this.id + "AddProcessesToFilter").set("disabled", true);
                    } else {
                        registry.byId(this.id + "ApplyProcessFilter").set("disabled", false);
                        registry.byId(this.id + "ApplyProcessFilter").set("checked", true);
                        registry.byId(this.id + "AddProcessesToFilter").set("disabled", false);
                    }
                    break;
                case "ApplyProcessFilter":
                    if (event.target.checked === false) {
                        registry.byId(this.id + "AddProcessesToFilter").set("disabled", true);
                    } else {
                        registry.byId(this.id + "ApplyProcessFilter").set("checked", true);
                        registry.byId(this.id + "AddProcessesToFilter").set("disabled", false);
                    }
                    break;
                case "cbAutoRefresh":
                    if (event.target.checked === false) {
                        registry.byId(this.id + "AutoRefresh").set("disabled", true);
                    } else {
                        registry.byId(this.id + "AutoRefreshCB").set("checked", true);
                        registry.byId(this.id + "AutoRefresh").set("disabled", false);
                    }
            }
        },

        getMachineInformation: function () {
            var retVal = this.toObject();

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

        _onSubmitRequest: function (type, selection) {
            var context = this;
            var machineInformation = this.getMachineInformation();

            if (type === "machine") {
                var MachineInformationCount = 0;
                var request = {
                    AutoRefresh: machineInformation.AutoRefresh,
                    MemThreshold: machineInformation.MemThreshold,
                    CpuThreshold: machineInformation.CpuThreshold,
                    MemThresholdType: machineInformation.MemThreshold,
                    GetProcessorInfo: machineInformation.GetProcessorInfo,
                    GetStorageInfo: machineInformation.GetStorageInfo,
                    LocalFileSystemsOnly: machineInformation.LocalFileSystemsOnly,
                    GetSoftwareInfo: machineInformation.GetSoftwareInfo,
                    DiskThreshold: machineInformation.DiskThreshold,
                    DiskThresholdType: machineInformation.DiskThresholdType,
                    ApplyProcessFilter: machineInformation.ApplyProcessFilter,
                    AddProcessesToFilter: machineInformation.AddtionalProcessesToFilter,
                    cbAutoRefresh: machineInformation.cbAutoRefresh
                };

                arrayUtil.forEach(selection, function (item, idx) {
                    MachineInformationCount++;
                    if (item.Component === "SystemServers") { //request params are unique for system servers vs cluster processes
                        request["SystemServers"] = true;
                        request["Addresses." + idx] = item.Netaddress + "|" + item.Netaddress + ":" + item.Type + ":" + item.Name + ":" + 2 + ":" + item.Directory;
                        request["Addresses.itemcount"] = MachineInformationCount;

                    } else {
                        request["ClusterProcesses"] = true;
                        request["Addresses." + idx] = item.Netaddress + "|:" + item.Type + ":" + item.Parent.Name + ":" + 2 + ":" + item.Parent.Directory + ":" + idx;
                        request["Addresses.itemcount"] = MachineInformationCount;
                    }
                });

                WsMachine.GetMachineInfo({
                    request: request
                }).then(function (response) {
                    if (request.ClusterProcesses) {
                        topic.publish("createClusterProcessPreflightTab", {
                            response: response.GetMachineInfoResponse
                        });
                    } else {
                        topic.publish("createSystemServersPreflightTab", {
                            response: response.GetMachineInfoResponse
                        });
                    }
                });
            } else if (type === "targets") {
                var TargetClusterCount = 0;
                var request = {
                    AutoRefresh: machineInformation.AutoRefresh,
                    MemThreshold: machineInformation.MemThreshold,
                    CpuThreshold: machineInformation.CpuThreshold,
                    MemThresholdType: machineInformation.MemThreshold,
                    GetProcessorInfo: machineInformation.GetProcessorInfo,
                    GetStorageInfo: machineInformation.GetStorageInfo,
                    LocalFileSystemsOnly: machineInformation.LocalFileSystemsOnly,
                    GetSoftwareInfo: machineInformation.GetSoftwareInfo,
                    DiskThreshold: machineInformation.DiskThreshold,
                    DiskThresholdType: machineInformation.DiskThresholdType,
                    ApplyProcessFilter: machineInformation.ApplyProcessFilter,
                    AddProcessesToFilter: machineInformation.AddtionalProcessesToFilter,
                    cbAutoRefresh: machineInformation.cbAutoRefresh
                };

                arrayUtil.forEach(selection, function (item, idx) {
                    TargetClusterCount++;
                    request["TargetClusters." + idx] = item.Type + ":" + item.Name;
                    request["TargetClusters.itemcount"] = TargetClusterCount;
                });

                WsMachine.GetTargetClusterInfo({
                    request: request
                }).then(function (response) {
                    if (lang.exists("GetTargetClusterInfoResponse", response)) {
                        topic.publish("createTargetClusterPreflightTab", {
                            response: response.GetTargetClusterInfoResponse
                        });
                    }
                });
            }
        }
    });
});
