define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/topic",

    "dijit/registry",

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
], function (declare, nlsHPCCMod, topic,
    registry,
    tree, selector,
    GridDetailsWidget, ESPPreflight, ESPRequest, WsTopology, Utility, ESPUtil, DelayLoadWidget, PreflightDetailsWidget, MachineInformationWidget, IFrameWidget) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("CluserProcessesQueryWidget", [GridDetailsWidget, ESPUtil.FormHelper], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_Clusters,
        idProperty: "__hpcc_id",
        machineFilter: null,
        machineFilterLoaded: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.openButton = registry.byId(this.id + "Open");
            this.refreshButton = registry.byId(this.id + "Refresh");

            this.machineFilter = new MachineInformationWidget({});
            this.machineFilter.placeAt(this.openButton.domNode, "after");
            this.machineFilter.machineForm.set("style", "width:500px;");
            this.machineFilter.disable(true);

            this.legacyClustersProcessesIframeWidget = new IFrameWidget({
                id: this.id + "_LegacyClustersProcessesIframeWidget",
                title: "Clusters Processes (legacy)",
                style: "border: 0; width: 100%; height: 100%"
            });
            this.legacyClustersProcessesIframeWidget.placeAt(this._tabContainer, "last");
        },

        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;

            this.refreshActionState();
            this.refreshGrid();

            this.machineFilter.on("apply", function (evt) {
                var selection = context.grid.getSelected();
                var selections = [];
                for (var i = 0; i < selection.length; ++i) {
                    var data = context.grid.row(selection[i].hpcc_id).data;
                    selections.push(data);
                }
                context.machineFilter._onSubmitRequest("machine", selections);
            });

            topic.subscribe("createClusterProcessPreflightTab", function (topic) {
                var pfTab = context.ensureMIPane(topic.response.Machines.MachineInfoEx[0].DisplayType + "- " + topic.response.TimeStamp, {
                    params: topic.response
                });
                pfTab.init(topic.response, "machines");
            });

            dojo.destroy(this.id + "Open");
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.id + "_Grid") {
                    this.refreshGrid();
                } else if (currSel.id === this.legacyClustersProcessesIframeWidget.id && !this.legacyClustersProcessesIframeWidget.initalized) {
                    this.legacyClustersProcessesIframeWidget.init({
                        src: ESPRequest.getBaseURL("WsTopology") + "/TpClusterQuery?Type=ROOT"
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

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([ESPUtil.Grid(true, true, false, true)])({
                store: ESPPreflight.CreateClusterProcessStore(),
                columns: {
                    col1: selector({
                        width: 20,
                        selectorType: "checkbox",
                        disabled: function (item) {
                            return item.type === "clusterProcess";
                        }
                    }),
                    Configuration: {
                        label: this.i18n.Configuration,
                        renderHeaderCell: function (node) {
                            node.innerHTML = Utility.getImageHTML("configuration.png", context.i18n.Configuration);
                        },
                        width: 10,
                        sortable: false,
                        formatter: function (clusterType) {
                            if (clusterType === true) {
                                return "<a href='#' onClick='return false;' />" + Utility.getImageHTML("configuration.png", context.i18n.Configuration) + "</a>";
                            }
                            return "";
                        }
                    },
                    Name: tree({
                        formatter: function (_name, row) {
                            var img = "";
                            var name = _name;
                            if (row.type === "clusterProcess") {
                                img = Utility.getImageHTML("server.png");
                                name = row.Type + " - " + _name;
                            } if (row.type === "machine") {
                                img = Utility.getImageHTML("machine.png");
                                name = "<a href='#' onClick='return false;' class='dgrid-row-url'>" + row.Netaddress + " - " + _name + "</a>";
                            }
                            return img + "&nbsp;" + name;
                        },
                        collapseOnRefresh: false,
                        label: this.i18n.Name,
                        sortable: true,
                        width: 150
                    }),
                    Domain: {
                        label: this.i18n.Domain,
                        sortable: false,
                        width: 100
                    },
                    Platform: {
                        label: this.i18n.Platform,
                        sortable: false,
                        width: 75
                    },
                    ProcessNumber: {
                        label: this.i18n.SlaveNumber,
                        sortable: false,
                        width: 100
                    },
                    Channel: {
                        label: this.i18n.Channel,
                        sortable: false,
                        width: 100
                    },
                    Directory: {
                        label: this.i18n.Directory,
                        sortable: false,
                        width: 200
                    },
                    LogDirectory: {
                        label: this.i18n.LogDirectory,
                        sortable: false,
                        width: 200
                    }
                }
            }, domID);

            retVal.on(".dgrid-cell img:click", function (evt) {
                var item = retVal.row(evt).data;
                context._onOpenConfiguration(item);
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

            retVal.on(".dgrid-cell:click", function (evt) {
                var cell = retVal.cell(evt);
            });

            retVal.on("dgrid-select", function (event) {
                context.refreshActionState();
            });
            retVal.on("dgrid-deselect", function (event) {
                context.refreshActionState();
            });

            return retVal;
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
                    CompType: data.Component,
                    CompName: data.Name,
                    Directory: data.Directory,
                    OsType: data.OS
                }
            }).then(function (response) {
                var tab = context.ensureConfigurationPane(data.Component + "_" + data.Name, {
                    Component: data.Component,
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

        _onRefresh: function () {
            this.refreshGrid();
        },

        refreshGrid: function () {
            this.grid.set("query", {
                Type: "ROOT"
            });
        },

        refreshActionState: function () {
            var selection = this.grid.getSelected();
            var isTarget = false;

            for (var i = 0; i < selection.length; ++i) {
                if (selection) {
                    isTarget = true;
                }
            }
            this.machineFilter.disable(!isTarget);
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
                    title: "<b>" + params.ParentName + "</b>: " + params.NetAddress,
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
