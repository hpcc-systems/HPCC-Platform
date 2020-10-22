define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/dom-class",
    "dojo/topic",

    "dijit/registry",
    "dijit/Dialog",

    "dgrid/tree",
    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/ESPPreflight",
    "src/ESPRequest",
    "src/WsTopology",
    "src/Utility",
    "src/ESPUtil",
    "hpcc/DelayLoadWidget",
    "hpcc/PreflightDetailsWidget",
    "hpcc/MachineInformationWidget",
    "hpcc/IFrameWidget"
], function (declare, nlsHPCCMod, arrayUtil, domClass, topic,
    registry, Dialog,
    tree, selector,
    GridDetailsWidget, ESPPreflight, ESPRequest, WsTopology, Utility, ESPUtil, DelayLoadWidget, PreflightDetailsWidget, MachineInformationWidget, IFrameWidget) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("SystemServersQueryWidget", [GridDetailsWidget, ESPUtil.FormHelper], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_SystemServers,
        idProperty: "__hpcc_id",
        machineFilter: null,
        machineFilterLoaded: null,

        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;

            this._refreshActionState();
            this.refreshGrid();

            this.machineFilter.disable(true);

            this.machineFilter.on("apply", function (evt) {
                var selection = context.grid.getSelected();
                var selections = [];
                for (var i = 0; i < selection.length; ++i) {
                    var data = context.grid.row(selection[i].hpcc_id).data;
                    selections.push(data);
                }
                context.machineFilter._onSubmitRequest("machine", selections);
            });

            dojo.destroy(this.id + "Open");
            this.refreshActionState();

            topic.subscribe("createSystemServersPreflightTab", function (topic) {
                var pfTab = context.ensureMIPane(topic.response.Machines.MachineInfoEx[0].DisplayType + "- " + topic.response.TimeStamp, {
                    params: topic.response
                });
                pfTab.init(topic.response, "machines");
            });
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.id + "_Grid") {
                    this.refreshGrid()
                } else if (currSel.id === this.systemServersQueryWidgetIframeWidget.id && !this.systemServersQueryWidgetIframeWidget.initalized) {
                    this.systemServersQueryWidgetIframeWidget.init({
                        src: ESPRequest.getBaseURL("WsTopology") + "/TpServiceQuery?Type=ALLSERVICES"
                    });
                } else {
                    currSel.init(currSel.params);
                }
            }
        },

        _onRowDblClick: function (item) {
            var nodeTab = this.ensureLogsPane(item.Name, {
                params: item,
                ParentName: item.Parent.Name,
                LogDirectory: item.Parent.LogDirectory,
                NetAddress: item.Netaddress,
                OS: item.OS,
                newPreflight: true
            });
            this.selectChild(nodeTab);
        },

        _onAdditionalInformation: function (arr) {
            var context = this;
            var headings = [this.i18n.ServiceName, this.i18n.ServiceType, this.i18n.Protocol, this.i18n.Port];
            var rows = [];

            arrayUtil.forEach(arr, function (row) {
                rows.push({
                    "ServiceName": row.Name,
                    "ServiceType": row.ServiceType,
                    "Protocol": row.Protocol,
                    "Port": row.Port
                });
            });

            this.dialog = new Dialog({
                title: this.i18n.ESPBindings,
                style: "width: 600px; height: relative;"
            });

            var table = Utility.DynamicDialogTable(headings, rows);

            this.dialog.set("content", table);
            this.dialog.show();
            this.dialog.on("cancel", function () {
                context.dialog.destroyRecursive();
            });
            this.dialog.on("hide", function () {
                context.dialog.destroyRecursive();
            });
        },

        postCreate: function (args) {
            var context = this;
            this.inherited(arguments);
            this.openButton = registry.byId(this.id + "Open");
            this.refreshButton = registry.byId(this.id + "Refresh");
            this.machineFilter = new MachineInformationWidget({});
            this.machineFilter.placeAt(this.openButton.domNode, "after");
            this.machineFilter.machineForm.set("style", "width:500px;");
            dojo.destroy(this.id + "Open");

            this.systemServersQueryWidgetIframeWidget = new IFrameWidget({
                id: this.id + "_SystemServersQueryWidgetIframeWidget",
                title: this.i18n.title_SystemServersLegacy,
                style: "border: 0; width: 100%; height: 100%"
            });
            this.systemServersQueryWidgetIframeWidget.placeAt(this._tabContainer, "last");
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([ESPUtil.Grid(true, true, false, true, false)])({
                store: ESPPreflight.CreateSystemServersStore(),
                columns: {
                    col1: selector({
                        width: 20,
                        selectorType: 'checkbox',
                        disabled: function (item) {
                            if (!item.Configuration || item.Type === "LDAPServerProcess") {
                                return true;
                            } else {
                                return false;
                            }
                        },
                    }),
                    Configuration: {
                        label: this.i18n.Configuration,
                        renderHeaderCell: function (node) {
                            domClass.add(node, "centerInCell");
                            node.innerHTML = Utility.getImageHTML("configuration.png", context.i18n.Configuration);
                        },
                        width: 10,
                        sortable: false,
                        renderCell: function (object, value, node, options) {
                            if (object.Directory && object.Type && object.Type !== "FTSlaveProcess") {
                                domClass.add(node, "centerInCell");
                                node.innerHTML = "<a href='#' class='gridClick'/>" + Utility.getImageHTML("configuration.png", context.i18n.Configuration) + "</a>";
                            }
                        },
                    },
                    Informational: {
                        label: this.i18n.Informational,
                        width: 60,
                        renderCell: function (object, value, node, options) {
                            if (object.Informational) {
                                domClass.add(node, "centerInCell");
                                node.innerHTML = "<a href='#' class='additionalSystemServersDialog' />" + Utility.getImageHTML("information.png", context.i18n.Informational) + "</a>"
                            }
                        }
                    },
                    Logs: {
                        label: this.i18n.Logs,
                        width: 90,
                        children: [
                            {
                                label: this.i18n.AuditLogs,
                                width: 40,
                                id: "AuditLogs",
                                renderCell: function (object, value, node, options) {
                                    if (object.AuditLog) {
                                        domClass.add(node, "centerInCell");
                                        node.innerHTML = "<a href='#' class='gridClick'/>" + Utility.getImageHTML("base.gif", context.i18n.AuditLogs) + "</a>"
                                    }
                                },
                            },
                            {
                                label: this.i18n.ComponentLogs,
                                width: 60,
                                id: "Logs",
                                renderCell: function (object, value, node, options) {
                                    if (object.Log) {
                                        domClass.add(node, "centerInCell");
                                        node.innerHTML = "<a href='#' class='gridClick'/>" + Utility.getImageHTML("base.gif", context.i18n.ComponentLogs) + "</a>"
                                    }
                                }
                            }
                        ]
                    },
                    Name: tree({
                        formatter: function (_name, row) {
                            var img = "";
                            var name = _name;
                            if (row.parent) {
                                img = Utility.getImageHTML("folder.png");
                                name = row.parent;
                            }
                            return img + "&nbsp;" + name;
                        },
                        collapseOnRefresh: false,
                        label: this.i18n.Name,
                        sortable: true,
                        width: 150
                    }),
                    ChildQueue: {
                        label: this.i18n.Queue,
                        sortable: false,
                        width: 100
                    },
                    Computer: {
                        label: this.i18n.Node,
                        sortable: false,
                        width: 75
                    },
                    NetaddressWithPort: {
                        label: this.i18n.NetworkAddress,
                        sortable: false,
                        width: 100
                    },
                    Directory: {
                        label: this.i18n.Directory,
                        sortable: false,
                        width: 200
                    }
                }
            }, domID);

            retVal.on("dgrid-select", function (event) {
                var selection = context.grid.getSelected();
                for (var i = selection.length - 1; i >= 0; --i) {
                    if (selection[i].Component) {
                        context.machineFilter.disable(true);
                    } else {
                        context.machineFilter.disable(false);
                    }
                }
            });

            retVal.on("dgrid-deselect", function (event) {
                var selection = context.grid.getSelected();
                if (selection.length === 0) {
                    context.machineFilter.disable(true);
                } else {
                    context.machineFilter.disable(false);
                }
            });

            retVal.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = retVal.row(evt).data;
                    context._onRowDblClick(item);
                }
            });

            retVal.on(".dgrid-row:dblclick", function (evt) {
                event.preventDefault();
            });

            retVal.on(".dgrid-cell .gridClick:click", function (evt) {
                var item = retVal.row(evt).data;
                if (evt.target.title === "Audit Log" || evt.target.title === "Component Log") {
                    context._onOpenLog(item)
                } else {
                    context._onOpenConfiguration(item);
                }
            });

            retVal.on(".dgrid-cell .additionalSystemServersDialog:click", function (evt) {
                var item = retVal.row(evt).data;
                context._onAdditionalInformation(item.Parent.TpBindings.TpBinding);
            });

            retVal.on(".dgrid-cell:click", function (evt) {
                var cell = retVal.cell(evt)
            });

            retVal.onSelectionChanged(function (event) {
                context.refreshActionState();
            });

            return retVal;
        },

        _onOpen: function (evt) {
            var selections = this.grid.getSelected();
            var firstTab = null;
            for (var i = 0; i < selections.length; ++i) {
                var tab = this.ensureLogsPane(selections[i], {
                    Node: selections[i]
                });
                if (i === 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onOpenConfiguration: function (data) {
            var context = this;
            var selections = this.grid.getSelected();
            var firstTab = null;

            if (!data) {
                data = this.grid.row(selections[0].hpcc_id).data;
            }

            WsTopology.TpGetComponentFile({
                request: {
                    FileType: "cfg",
                    CompType: data.Type,
                    NetAddress: data.Netaddress,
                    Directory: data.Directory,
                    OsType: data.OS
                }
            }).then(function (response) {
                var tab = context.ensureConfigurationPane(data.Type + data.Name + "Configuration", {
                    Component: data.Type,
                    Name: data.Name,
                    Usergenerated: response
                });

                if (firstTab === null) {
                    firstTab = tab;
                }
                if (firstTab) {
                    context.selectChild(firstTab);
                }
            });
        },

        _onOpenLog: function (item) {
            var nodeTab = this.ensureLogsPane(item.Name + ": " + item.Parent.LogDirectory, {
                params: item,
                ParentName: item.Parent.Name,
                LogDirectory: item.Parent.LogDirectory,
                NetAddress: item.Netaddress,
                OS: item.OS,
                newPreflight: true
            });
            this.selectChild(nodeTab);
        },

        _onRefresh: function () {
            this.refreshGrid();
        },

        refreshGrid: function () {
            this.grid.set("query", {
                Type: "ALLSERVICES",
            });
        },

        refreshActionState: function () {
            var selection = this.grid.getSelected();
            var isCluster = false;
            var isNode = false;

            for (var i = 0; i < selection.length; ++i) {
                if (selection[i] && selection[i].type === "clusterProcess") {
                    isNode = false;
                } else {
                    isNode = true;
                }
            }

            this.openButton.set("disabled", !isNode);
        },

        ensureConfigurationPane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new DelayLoadWidget({
                    id: id,
                    title: "<b>" + params.Component + "</b>: " + params.Name + " " + context.i18n.Configuration,
                    closable: true,
                    delayWidget: "ECLSourceWidget",
                    params: params

                });
                this.addChild(retVal, "last");
            }
            return retVal;
        },

        ensureLogsPane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new DelayLoadWidget({
                    id: id,
                    title: "<b>" + params.NetAddress + "</b>: " + params.LogDirectory,
                    closable: true,
                    delayWidget: "LogWidget",
                    params: params

                });
                this.addChild(retVal, "last");
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
                    title: this.i18n.MachineInformation
                });
                this._tabContainer.addChild(retVal, "last");
            }
            return retVal;
        }
    });
});
