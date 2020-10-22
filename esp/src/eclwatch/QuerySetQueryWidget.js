define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/topic",
    "dojo/_base/array",
    "dojo/dom-form",

    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "hpcc/DelayLoadWidget",
    "src/WsWorkunits",
    "src/ESPQuery",
    "src/ESPUtil",
    "src/Utility",

    "dojo/text!../templates/QuerySetQueryWidget.html",

    "hpcc/SelectionGridWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/CheckBox",
    "dijit/ToolbarSeparator",
    "dijit/form/TextBox",
    "dijit/Dialog",
    "dijit/form/DropDownButton",
    "dijit/Fieldset",
    "dijit/TooltipDialog",

    "hpcc/TargetSelectWidget",
    "hpcc/FilterDropDownWidget",
    "hpcc/TableContainer"
], function (declare, lang, nlsHPCCMod, topic, arrayUtil, domForm,
    registry, Menu, MenuItem, MenuSeparator, PopupMenuItem,
    selector,
    _TabContainerWidget, DelayLoadWidget, WsWorkunits, ESPQuery, ESPUtil, Utility,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("QuerySetQueryWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "QuerySetQueryWidget",
        i18n: nlsHPCC,

        borderContainer: null,
        queriesTab: null,
        querySetGrid: null,
        recreateQueriesGrid: null,
        clusterTargetSelect: null,
        recreateQueryTargetSelect: null,
        filter: null,

        initalized: false,
        loaded: false,
        userName: null,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        _onMine: function (event) {
            if (event) {
                this.filter.setValue(this.id + "PublishedBy", this.userName);
                this.filter._onFilterApply();
            } else {
                this.filter._onFilterClear();
                this.filter._onFilterApply();
            }
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.queriesTab = registry.byId(this.id + "_PublishedQueries");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
            this.recreateQueryTargetSelect = registry.byId(this.id + "RecreateTargetSelect");
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.filter = registry.byId(this.id + "Filter");
            this.downloadToList = registry.byId(this.id + "DownloadToList");
            this.downloadToListDialog = registry.byId(this.id + "DownloadToListDialog");
            this.downListForm = registry.byId(this.id + "DownListForm");
            this.fileName = registry.byId(this.id + "CSVFileName");
            this.recreateQueriesGrid = registry.byId(this.id + "RecreateQueriesGrid");
            this.recreateForm = registry.byId(this.id + "RecreateForm");
            this.mineControl = registry.byId(this.id + "Mine");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initContextMenu();
        },

        _onDownloadToListCancelDialog: function (event) {
            this.downloadToListDialog.hide();
        },

        _onDownloadToList: function (event) {
            this.downloadToListDialog.show();
        },

        _buildCSV: function (event) {
            var selections = this.querySetGrid.getSelected();
            var row = [];
            var fileName = this.fileName.get("value") + ".csv";

            arrayUtil.forEach(selections, function (cell, idx) {
                var rowData = [cell.Suspended, cell.ErrorCount, cell.MixedNodeStates, cell.Activated, cell.Id, cell.Name, cell.QuerySetId, cell.Wuid, cell.Dll, cell.PublishedBy, cell.Status];
                row.push(rowData);
            });

            Utility.downloadToCSV(this.querySetGrid, row, fileName);
            this._onDownloadToListCancelDialog();
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

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (this.params.searchResults) {
                this.filter.disable(true);
            }

            this.clusterTargetSelect.init({
                Targets: true,
                includeBlank: true,
                Target: params.Cluster
            });

            if (params.Wuid) {
                this.filter.setValue(this.id + "Wuid", params.Wuid);
            } else if (params.LogicalName) {
                this.filter.setValue(this.id + "FileName", params.LogicalName);
            }
            this.initQuerySetGrid();

            var context = this;
            this.filter.init({
                ws_key: "QuerySetQueryRecentFilter",
                widget: this.widget
            });
            this.filter.on("clear", function (evt) {
                context.refreshHRef();
                context.refreshGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshHRef();
                context.querySetGrid._currentPage = 0;
                context.refreshGrid();
            });
            topic.subscribe("hpcc/ecl_wu_published", function (topic) {
                context.refreshGrid();
            });

            this.userName = dojoConfig.username;
            if (this.userName === null) {
                this.mineControl.set("disabled", true);
            }
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.queriesTab.id) {
                    this.refreshGrid();
                } else {
                    currSel.init(currSel.hpcc.params);
                }
            }
        },

        addMenuItem: function (menu, details) {
            var menuItem = new MenuItem(details);
            menu.addChild(menuItem);
            return menuItem;
        },

        initContextMenu: function () {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "QuerySetGrid"]
            });
            this.menuOpen = this.addMenuItem(pMenu, {
                label: this.i18n.Open,
                onClick: function () { context._onOpen(); }
            });
            this.menuDelete = this.addMenuItem(pMenu, {
                label: this.i18n.Delete,
                onClick: function () { context._onDelete(); }
            });
            pMenu.addChild(new MenuSeparator());
            this.menuUnsuspend = this.addMenuItem(pMenu, {
                label: this.i18n.Unsuspend,
                onClick: function () { context._onUnsuspend(); }
            });
            this.menuSuspend = this.addMenuItem(pMenu, {
                label: this.i18n.Suspend,
                onClick: function () { context._onSuspend(); }
            });
            pMenu.addChild(new MenuSeparator());
            this.menuActivate = this.addMenuItem(pMenu, {
                label: this.i18n.Activate,
                onClick: function () { context._onActivate(); }
            });
            this.menuDeactivate = this.addMenuItem(pMenu, {
                label: this.i18n.Deactivate,
                onClick: function () { context._onDeactivate(); }
            });
            pMenu.addChild(new MenuSeparator());
            {
                var pSubMenu = new Menu();

                this.menuFilterCluster = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.filter.clear();
                        context.filter.setValue(context.id + "ClusterTargetSelect", context.menuFilterCluster.get("hpcc_value"));
                        context.refreshGrid();
                    }
                });
                this.menuFilterSuspended = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.filter.clear();
                        context.filter.setValue(context.id + "SuspendedStates", context.menuFilterSuspended.get("hpcc_value"));
                        context.refreshGrid();
                    }
                });
                this.menuFilterUnsuspend = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.filter.clear();
                        context.filter.setValue(context.id + "SuspendedStates", context.menuFilterUnsuspend.get("hpcc_value"));
                        context.refreshGrid();
                    }
                });
                this.menuFilterActive = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.filter.clear();
                        context.filter.setValue(context.id + "ActiveStates", context.menuFilterActive.get("hpcc_value"));
                        context.refreshGrid();
                    }
                });
                this.menuFilterDeactivate = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.filter.clear();
                        context.filter.setValue(context.id + "ActiveStates", context.menuFilterDeactivate.get("hpcc_value"));
                        context.refreshGrid();
                    }
                });
                pSubMenu.addChild(new MenuSeparator());
                this.menuFilterClearFilter = this.addMenuItem(pSubMenu, {
                    label: this.i18n.Clear,
                    onClick: function () {
                        context.filter.clear();
                        context.refreshGrid();
                    }
                });
                pMenu.addChild(new PopupMenuItem({
                    label: this.i18n.Filter,
                    popup: pSubMenu
                }));
            }
            pMenu.startup();
        },

        /*Not Applicable*/
        _onRowContextMenu: function (item, colField, mystring) {
            this.menuFilterCluster.set("disabled", false);
            this.menuFilterSuspended.set("disabled", false);
            this.menuFilterUnsuspend.set("disabled", false);
            this.menuFilterActive.set("disabled", false);
            this.menuFilterDeactivate.set("disabled", false);

            if (item) {
                this.menuFilterCluster.set("label", "Cluster: " + item.QuerySetId);
                this.menuFilterCluster.set("hpcc_value", item.QuerySetId);
                this.menuFilterSuspended.set("label", this.i18n.Suspended + ":  " + item.Suspended);
                this.menuFilterSuspended.set("hpcc_value", 1);
                this.menuFilterUnsuspend.set("label", this.i18n.Unsuspended + ":  true ");
                this.menuFilterUnsuspend.set("hpcc_value", 0);
                this.menuFilterActive.set("label", this.i18n.Active + ":  " + item.Activated);
                this.menuFilterActive.set("hpcc_value", 1);
                this.menuFilterDeactivate.set("label", this.i18n.Inactive + ":  true");
                this.menuFilterDeactivate.set("hpcc_value", 0);
            }
            if (item.Cluster === "") {
                this.menuFilterCluster.set("disabled", true);
                this.menuFilterCluster.set("label", this.i18n.Cluster + ":  " + this.i18n.NA);
            }
            if (item.Suspended === false) {
                this.menuFilterSuspended.set("disabled", true);
                this.menuFilterSuspended.set("label", this.i18n.Suspended + ":  " + this.i18n.NA);
            }
            if (item.Suspended === true) {
                this.menuFilterUnsuspend.set("disabled", true);
                this.menuFilterUnsuspend.set("label", this.i18n.Unsuspended + ":  " + this.i18n.NA);
            }
            if (item.Activated === false) {
                this.menuFilterActive.set("disabled", true);
                this.menuFilterActive.set("label", this.i18n.Active + ":  " + this.i18n.NA);
            }
            if (item.Activated === true) {
                this.menuFilterDeactivate.set("disabled", true);
                this.menuFilterDeactivate.set("label", this.i18n.Inactive + ":  " + this.i18n.NA);
            }
        },

        initQuerySetGrid: function (params) {
            var context = this;
            var store = this.params.searchResults ? this.params.searchResults : ESPQuery.CreateQueryStore();
            this.querySetGrid = new declare([ESPUtil.Grid(true, true, false, false, "QuerySetQueryWidget")])({
                store: store,
                query: this.getGridQuery(),
                sort: [{ attribute: "Id" }],
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    Suspended: {
                        label: this.i18n.Suspended,
                        renderHeaderCell: function (node) {
                            node.innerHTML = Utility.getImageHTML("suspended.png", context.i18n.Suspended);
                        },
                        width: 25,
                        sortable: false,
                        formatter: function (suspended) {
                            if (suspended === true) {
                                return Utility.getImageHTML("suspended.png");
                            }
                            return "";
                        }
                    },
                    ErrorCount: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = Utility.getImageHTML("errwarn.png", context.i18n.ErrorWarnings);
                        },
                        width: 25,
                        sortable: false,
                        formatter: function (error) {
                            if (error > 0) {
                                return Utility.getImageHTML("errwarn.png");
                            }
                            return "";
                        }
                    },
                    MixedNodeStates: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = Utility.getImageHTML("mixwarn.png", context.i18n.MixedNodeStates);
                        },
                        width: 25,
                        sortable: false,
                        formatter: function (mixed) {
                            if (mixed === true) {
                                return Utility.getImageHTML("mixwarn.png");
                            }
                            return "";
                        }
                    },
                    Activated: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = Utility.getImageHTML("active.png", context.i18n.Active);
                        },
                        width: 25,
                        formatter: function (activated) {
                            if (activated === true) {
                                return Utility.getImageHTML("active.png");
                            }
                            return Utility.getImageHTML("inactive.png");
                        }
                    },
                    Id: {
                        label: this.i18n.ID,
                        width: 380,
                        formatter: function (Id, idx) {
                            return "<a href='#' class='dgrid-row-url'>" + Id + "</a>";
                        }
                    },
                    Name: {
                        label: this.i18n.Name
                    },
                    QuerySetId: {
                        width: 140,
                        label: this.i18n.Target,
                        sortable: true
                    },
                    Wuid: {
                        width: 160,
                        label: this.i18n.WUID,
                        formatter: function (Wuid, idx) {
                            return "<a href='#' class='dgrid-row-url2'>" + Wuid + "</a>";
                        }
                    },
                    Dll: {
                        width: 180,
                        label: this.i18n.Dll
                    },
                    PublishedBy: {
                        width: 100,
                        label: this.i18n.PublishedBy,
                        sortable: false
                    },
                    Status: {
                        width: 100,
                        label: this.i18n.Status,
                        sortable: false
                    }
                }
            }, this.id + "QuerySetGrid");
            this.querySetGrid.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.querySetGrid.row(evt).data;
                    context._onRowDblClick(item);
                }
            });
            this.querySetGrid.on(".dgrid-row-url2:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.querySetGrid.row(evt).data;
                    context._onRowDblClick(item, true);
                }
            });
            this.querySetGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.querySetGrid.row(evt).data;
                    context._onRowDblClick(item);
                }
            });
            this.querySetGrid.on(".dgrid-row:contextmenu", function (evt) {
                if (context._onRowContextMenu) {
                    var item = context.querySetGrid.row(evt).data;
                    var cell = context.querySetGrid.cell(evt);
                    var colField = cell.column.field;
                    var mystring = "item." + colField;
                    context._onRowContextMenu(item, colField, mystring);
                }
            });
            this.querySetGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
                var selection = context.querySetGrid.getSelected();
                if (selection.length > 0) {
                    context.downloadToList.set("disabled", false);
                } else {
                    context.downloadToList.set("disabled", true);
                }
            });
            ESPUtil.goToPageUserPreference(this.querySetGrid, "QuerySetQueryWidget_GridRowsPerPage").then(function () {
                context.querySetGrid.startup();
            });

            this.recreateQueriesGrid.createGrid({
                idProperty: "Name",
                columns: {
                    Name: {
                        label: this.i18n.ID
                    },
                    QuerySetId: {
                        label: this.i18n.Target
                    }
                }
            });

            this.refreshActionState();
        },

        refreshActionState: function () {
            var selection = this.querySetGrid.getSelected();
            var data = [];
            var hasSelection = false;
            var isSuspended = false;
            var isNotSuspended = false;
            var isActive = false;
            var isNotActive = false;
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
                if (selection[i].Suspended !== true) {
                    isSuspended = true;
                } else {
                    isNotSuspended = true;
                }
                if (selection[i].Activated !== true) {
                    isActive = true;
                } else {
                    isNotActive = true;
                }
            }

            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "UnSuspend").set("disabled", !isNotSuspended);
            registry.byId(this.id + "OnSuspend").set("disabled", !isSuspended);
            registry.byId(this.id + "Activate").set("disabled", !isActive);
            registry.byId(this.id + "Deactivate").set("disabled", !isNotActive);
            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "RecreateQueryDropDown").set("disabled", !hasSelection);

            this.menuUnsuspend.set("disabled", !isNotSuspended);
            this.menuSuspend.set("disabled", !isSuspended);
            this.menuActivate.set("disabled", !isActive);
            this.menuDeactivate.set("disabled", !isNotActive);

            if (hasSelection) {
                arrayUtil.forEach(selection, function (item, idx) {
                    data.push(item);
                });
                this.recreateQueriesGrid.setData(data);
            }
        },

        _onRefresh: function (params) {
            this.refreshGrid();
        },

        _onRecreateQueriesSuccess: function (status) {
            var context = this;
            if (status) {
                dojo.publish("hpcc/brToaster", {
                    Severity: "Message",
                    Source: "WsWorkunits.WURecreateQuery",
                    Exceptions: [{ Source: context.i18n.RecreateQuery, Message: context.i18n.SuccessfullySaved }]
                });
            }
        },

        _onRecreateQueries: function () {
            if (this.recreateForm.validate()) {
                var context = this;
                var success = false;
                arrayUtil.forEach(this.recreateQueriesGrid.store.data, function (item, idx) {
                    var request = domForm.toObject(context.id + "RecreateForm");
                    request.Republish === "on" ? request.Republish = 0 : request.Republish = 1;
                    request.AllowForeignFiles === "off" ? request.AllowForeignFiles = 0 : request.AllowForeignFiles = 1;
                    request.UpdateDfs === "off" ? request.UpdateDfs = 0 : request.UpdateDfs = 1;
                    request.UpdateSuperFiles === "off" ? request.UpdateSuperFiles = 0 : request.UpdateSuperFiles = 1;
                    request.QueryId = item.Name;
                    request.Target = item.QuerySetId;
                    request.IncludeFileErrors = 1;
                    WsWorkunits.WURecreateQuery({
                        request: request
                    }).then(function (response) {
                        if (lang.exists("WURecreateQueryResponse.Wuid", response)) {
                            success = true;
                        }
                        context._onRecreateQueriesSuccess(success);
                    });
                });
                registry.byId(this.id + "RecreateQueryDropDown").closeDropDown();
            }
        },

        _onDelete: function () {
            var selection = this.querySetGrid.getSelected();
            var list = this.arrayToList(selection, "Id");
            if (confirm(this.i18n.DeleteSelectedQueries + "\n" + list)) {
                var context = this;
                WsWorkunits.WUQuerysetQueryAction(selection, "Delete").then(function (response) {
                    context.refreshGrid(true);
                });
            }
        },

        refreshGrid: function (clearSelection) {
            this.querySetGrid.set("query", this.getGridQuery());
            if (clearSelection) {
                this.querySetGrid.clearSelection();
            }
        },

        _onSuspend: function () {
            var context = this;
            WsWorkunits.WUQuerysetQueryAction(this.querySetGrid.getSelected(), "Suspend").then(function (response) {
                context.refreshGrid();
            });
        },

        _onUnsuspend: function () {
            var context = this;
            WsWorkunits.WUQuerysetQueryAction(this.querySetGrid.getSelected(), "Unsuspend").then(function (response) {
                context.refreshGrid();
            });
        },

        _onActivate: function () {
            var context = this;
            WsWorkunits.WUQuerysetQueryAction(this.querySetGrid.getSelected(), "Activate").then(function (response) {
                context.refreshGrid();
            });
        },

        _onDeactivate: function () {
            var context = this;
            WsWorkunits.WUQuerysetQueryAction(this.querySetGrid.getSelected(), "Deactivate").then(function (response) {
                context.refreshGrid();
            });
        },

        _onOpen: function () {
            var selections = this.querySetGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePane(selections[i].Id, selections[i]);
                if (i === 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onSetOptions: function (event) {
            if (registry.byId(this.id + "OptionsForm").validate()) {
                this.refreshGrid();
                registry.byId(this.id + "Options").closeDropDown();
            }
        },


        _onRowDblClick: function (item, workunitTab) {
            var tab = null;
            if (workunitTab) {
                tab = this.ensurePane(item.Wuid, item, true);
            } else {
                tab = this.ensurePane(item.Id, item, false);
            }
            this.selectChild(tab);
        },

        getGridQuery: function () {
            if (this.params.searchResults) {
                return {};
            }
            var optionsForm = registry.byId(this.id + "OptionsForm");
            var optionsValues = optionsForm.getValues();
            return lang.mixin(this.filter.toObject(), optionsValues);
        },

        ensurePane: function (id, params, workunitTab) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                if (workunitTab) {
                    retVal = new DelayLoadWidget({
                        id: id,
                        title: params.Wuid,
                        closable: true,
                        delayWidget: "WUDetailsWidget",
                        hpcc: {
                            type: "WUDetailsWidget",
                            params: {
                                Wuid: params.Wuid
                            }
                        }
                    });
                } else {
                    retVal = new DelayLoadWidget({
                        id: id,
                        title: params.Id,
                        closable: true,
                        delayWidget: "QuerySetDetailsWidget",
                        hpcc: {
                            type: "QuerySetDetailsWidget",
                            params: {
                                QuerySetId: params.QuerySetId,
                                Id: params.Id
                            }
                        }
                    });
                }
                this.addChild(retVal, 1);
            }
            return retVal;
        }
    });
});
