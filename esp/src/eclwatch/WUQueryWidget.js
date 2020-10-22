define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/date",
    "dojo/topic",
    "dojo/aspect",
    "dojo/dom-class",

    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "src/WsWorkunits",
    "src/ESPUtil",
    "src/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "src/Utility",
    "src/Clippy",

    "dojo/text!../templates/WUQueryWidget.html",

    "hpcc/TargetSelectWidget",
    "hpcc/FilterDropDownWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/RadioButton",
    "dijit/form/Select",
    "dijit/form/DropDownButton",
    "dijit/Dialog",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog"

], function (declare, lang, nlsHPCCMod, arrayUtil, date, topic, aspect, domClass,
    registry, Menu, MenuItem, MenuSeparator, PopupMenuItem,
    selector,
    _TabContainerWidget, WsWorkunits, ESPUtil, ESPWorkunit, DelayLoadWidget, Utility, Clippy,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("WUQueryWidget", [_TabContainerWidget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "WUQueryWidget",
        i18n: nlsHPCC,

        workunitsTab: null,
        workunitsGrid: null,
        filter: null,
        clusterTargetSelect: null,
        stateSelect: null,
        userName: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.filter = registry.byId(this.id + "Filter");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
            this.stateSelect = registry.byId(this.id + "StateSelect");
            this.logicalFileSearchTypeSelect = registry.byId(this.id + "LogicalFileSearchType");
            this.downloadToList = registry.byId(this.id + "DownloadToList");
            this.downloadToListDialog = registry.byId(this.id + "DownloadToListDialog");
            this.downListForm = registry.byId(this.id + "DownListForm");
            this.fileName = registry.byId(this.id + "FileName");
            this.mineControl = registry.byId(this.id + "Mine");
            this.wuCopyButton = registry.byId(this.id + "Copy");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initContextMenu();
            this._idleWatcher = new ESPUtil.IdleWatcher();
            this._idleWatcher.start();
            var context = this;
            this._idleWatcherHandle = this._idleWatcher.on("idle", function () {
                if (!context.store.busy && !context.filter.exists()) {
                    context._onRefresh();
                }
            });
        },

        _onDownloadToListCancelDialog: function (event) {
            this.downloadToListDialog.hide();
        },

        _onDownloadToList: function (event) {
            this.downloadToListDialog.show();
        },

        _buildCSV: function (event) {
            var selections = this.workunitsGrid.getSelected();
            var row = [];
            var fileName = this.fileName.get("value") + ".csv";

            arrayUtil.forEach(selections, function (cell, idx) {
                var rowData = [cell.Protected, cell.Wuid, cell.Owner, cell.Jobname, cell.Cluster, cell.RoxieCluster, cell.State, cell.TotalClusterTime];
                row.push(rowData);
            });

            Utility.downloadToCSV(this.workunitsGrid, row, fileName);
            this._onDownloadToListCancelDialog();
        },

        destroy: function (args) {
            this._idleWatcherHandle.remove();
            this._idleWatcher.stop();
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_WUQuery;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _onOpen: function (event) {
            var selections = this.workunitsGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePane(selections[i].Wuid, {
                    Wuid: selections[i].Wuid
                });
                if (i === 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onDelete: function (event) {
            var selection = this.workunitsGrid.getSelected();
            var list = this.arrayToList(selection, "Wuid");
            if (confirm(this.i18n.DeleteSelectedWorkunits + "\n" + list)) {
                var context = this;
                WsWorkunits.WUAction(selection, "Delete", {
                    load: function (response) {
                        context.refreshGrid(true);
                    }
                });
            }
        },

        _onSetToFailed: function (event) {
            WsWorkunits.WUAction(this.workunitsGrid.getSelected(), "SetToFailed");
        },

        _onAbort: function (event) {
            WsWorkunits.WUAction(this.workunitsGrid.getSelected(), "Abort");
        },

        _onProtect: function (event) {
            WsWorkunits.WUAction(this.workunitsGrid.getSelected(), "Protect");
        },

        _onUnprotect: function (event) {
            WsWorkunits.WUAction(this.workunitsGrid.getSelected(), "Unprotect");
        },

        _onReschedule: function (event) {
            WsWorkunits.WUAction(this.workunitsGrid.getSelected(), "Reschedule");
        },

        _onDeschedule: function (event) {
            WsWorkunits.WUAction(this.workunitsGrid.getSelected(), "Deschedule");
        },

        _onRowDblClick: function (wuid) {
            var wuTab = this.ensurePane(wuid, {
                Wuid: wuid
            });
            this.selectChild(wuTab);
        },

        _onRowContextMenu: function (item, colField, mystring) {
            this.menuFilterOwner.set("disabled", false);
            this.menuFilterJobname.set("disabled", false);
            this.menuFilterCluster.set("disabled", false);
            this.menuFilterState.set("disabled", false);

            if (item) {
                this.menuFilterOwner.set("label", "Owner:  " + item.Owner);
                this.menuFilterOwner.set("hpcc_value", item.Owner);
                this.menuFilterJobname.set("label", "Jobname:  " + item.Jobname);
                this.menuFilterJobname.set("hpcc_value", item.Jobname);
                this.menuFilterCluster.set("label", "Cluster:  " + item.Cluster);
                this.menuFilterCluster.set("hpcc_value", item.Cluster);
                this.menuFilterState.set("label", "State:  " + item.State);
                this.menuFilterState.set("hpcc_value", item.State);
            }

            if (item.Owner === "") {
                this.menuFilterOwner.set("disabled", true);
                this.menuFilterOwner.set("label", this.i18n.Owner + ":  " + this.i18n.NA);
            }
            if (item.Jobname === "") {
                this.menuFilterJobname.set("disabled", true);
                this.menuFilterJobname.set("label", this.i18n.JobName + ":  " + this.i18n.NA);
            }
            if (item.Cluster === "") {
                this.menuFilterCluster.set("disabled", true);
                this.menuFilterCluster.set("label", this.i18n.Cluster + ":  " + this.i18n.NA);
            }
            if (item.State === "") {
                this.menuFilterState.set("disabled", true);
                this.menuFilterState.set("label", this.i18n.State + ":  " + this.i18n.NA);
            }
        },

        _onFilterType: function (evt) {
            var filter = this.filter.toObject();
            this.setVisible(this.id + "ArchivedWarning", filter.Type);
            this.setDisabled(this.id + "ECL", filter.Type);
            this.setDisabled(this.id + "LogicalFile", filter.Type);
            this.setDisabled(this.id + "LogicalFileSearchType", filter.Type);
        },

        //  Implementation  ---
        getFilter: function () {
            var retVal = this.filter.toObject();
            if (retVal.StartDate && retVal.FromTime) {
                lang.mixin(retVal, {
                    StartDate: this.getISOString("FromDate", "FromTime")
                });
            } else if (retVal.StartDate && !retVal.FromTime) {
                lang.mixin(retVal, {
                    StartDate: registry.byId(this.id + "FromDate").attr("value").toISOString().replace(/T.*Z/, '') + "T00:00:00Z"
                });
            }
            if (retVal.EndDate && retVal.ToTime) {
                lang.mixin(retVal, {
                    EndDate: this.getISOString("ToDate", "ToTime")
                });
            } else if (retVal.EndDate && !retVal.ToTime) {
                lang.mixin(retVal, {
                    EndDate: registry.byId(this.id + "ToDate").attr("value").toISOString().replace(/T.*Z/, '') + "T23:59:59Z"
                });
            }
            if (retVal.StartDate && retVal.EndDate) {
                retVal["DateRB"] = "0";
            } else if (retVal.LastNDays) {
                retVal["DateRB"] = "0";
                var now = new Date();
                retVal.StartDate = date.add(now, "day", retVal.LastNDays * -1).toISOString();
                retVal.EndDate = now.toISOString();
            }
            if (retVal.Type === "archived workunits") {
                lang.mixin(retVal, {
                    timeOutSeconds: 300
                });
            }
            return retVal;
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (this.params.searchResults) {
                this.filter.disable(true);
                this.mineControl.set("disabled", true);
            }

            this.clusterTargetSelect.init({
                Targets: true,
                includeBlank: true,
                Target: params.Cluster
            });
            this.stateSelect.init({
                WUState: true,
                includeBlank: true,
                Target: ""
            });
            this.logicalFileSearchTypeSelect.init({
                LogicalFileSearchType: true,
                includeBlank: true,
                Target: ""
            });

            this.initWorkunitsGrid();

            var context = this;
            this.filter.init({
                ws_key: "WUQueryRecentFilter",
                widget: this.widget
            });
            this.filter.on("clear", function (evt) {
                context._onFilterType();
                context.refreshHRef();
                context.refreshGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshHRef();
                context.workunitsGrid._currentPage = 0;
                context.refreshGrid(true);
            });

            topic.subscribe("hpcc/session_management_status", function (publishedMessage) {
                if (publishedMessage.status === "Unlocked") {
                    context.refreshGrid();
                    context._idleWatcher.start();
                } else if (publishedMessage.status === "Locked") {
                    context._idleWatcher.stop();
                }
            });

            topic.subscribe("hpcc/ecl_wu_created", function (topic) {
                context.refreshGrid();
            });

            ESPUtil.MonitorVisibility(this.workunitsTab, function (visibility) {
                if (visibility) {
                    context.refreshGrid();
                }
            });

            this.userName = dojoConfig.username;
            if (this.userName === null) {
                this.mineControl.set("disabled", true);
            }

            Clippy.attachDomNode(this.wuCopyButton.domNode, function () {
                var wuids = [];
                arrayUtil.forEach(context.workunitsGrid.getSelected(), function (item, idx) {
                    wuids.push(item.Wuid);
                });
                return wuids.join("\n");
            });
            this.refreshActionState();
        },

        _onMine: function (event) {
            if (event) {
                this.filter.setValue(this.id + "Owner", this.userName);
                this.filter._onFilterApply();
            } else {
                this.filter._onFilterClear();
                this.filter._onFilterApply();
            }
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.workunitsTab.id) {
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel.params);
                    }
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
                targetNodeIds: [this.id + "WorkunitsGrid"]
            });
            this.menuOpen = this.addMenuItem(pMenu, {
                label: this.i18n.Open,
                onClick: function () { context._onOpen(); }
            });
            this.menuDelete = this.addMenuItem(pMenu, {
                label: this.i18n.Delete,
                onClick: function () { context._onDelete(); }
            });
            this.menuSetToFailed = this.addMenuItem(pMenu, {
                label: this.i18n.SetToFailed,
                onClick: function () { context._onSetToFailed(); }
            });
            pMenu.addChild(new MenuSeparator());
            this.menuProtect = this.addMenuItem(pMenu, {
                label: this.i18n.Protect,
                onClick: function () { context._onProtect(); }
            });
            this.menuUnprotect = this.addMenuItem(pMenu, {
                label: this.i18n.Unprotect,
                onClick: function () { context._onUnprotect(); }
            });
            pMenu.addChild(new MenuSeparator());
            {
                var pSubMenu = new Menu();
                this.menuFilterOwner = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.filter.clear();
                        context.filter.setValue(context.id + "Owner", context.menuFilterOwner.get("hpcc_value"));
                        context.refreshGrid();
                    }
                });
                this.menuFilterJobname = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.filter.clear();
                        context.filter.setValue(context.id + "Jobname", context.menuFilterJobname.get("hpcc_value"));
                        context.refreshGrid();
                    }
                });
                this.menuFilterCluster = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.filter.clear();
                        context.filter.setValue(context.id + "ClusterTargetSelect", context.menuFilterCluster.get("hpcc_value"));
                        context.refreshGrid();
                    }
                });
                this.menuFilterState = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.filter.clear();
                        context.filter.setValue(context.id + "StateSelect", context.menuFilterState.get("hpcc_value"));
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

        initWorkunitsGrid: function () {
            var context = this;
            this.store = this.params.searchResults ? this.params.searchResults : new ESPWorkunit.CreateWUQueryStore();
            this.workunitsGrid = new declare([ESPUtil.Grid(true, true, false, false, "WUQueryWidget")])({
                store: this.store,
                query: this.getFilter(),
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    Protected: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = Utility.getImageHTML("locked.png", context.i18n.Protected);
                        },
                        width: 25,
                        sortable: false,
                        formatter: function (_protected) {
                            if (_protected === true) {
                                return Utility.getImageHTML("locked.png");
                            }
                            return "";
                        }
                    },
                    Wuid: {
                        label: this.i18n.WUID, width: 180,
                        formatter: function (Wuid, idx) {
                            var wu = ESPWorkunit.Get(Wuid);
                            return wu.getStateImageHTML() + "&nbsp;<a href='#' class='dgrid-row-url'>" + Wuid + "</a>";
                        }
                    },
                    Owner: { label: this.i18n.Owner, width: 90 },
                    Jobname: { label: this.i18n.JobName, width: 500 },
                    Cluster: { label: this.i18n.Cluster, width: 90 },
                    RoxieCluster: { label: this.i18n.RoxieCluster, width: 99 },
                    State: { label: this.i18n.State, width: 90 },
                    TotalClusterTime: {
                        label: this.i18n.TotalClusterTime, width: 117,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = value;
                        }
                    },
                }
            }, this.id + "WorkunitsGrid");
            this.workunitsGrid.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.workunitsGrid.row(evt).data;
                    context._onRowDblClick(item.Wuid);
                }
            });
            this.workunitsGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.workunitsGrid.row(evt).data;
                    context._onRowDblClick(item.Wuid);
                }
            });
            this.workunitsGrid.on(".dgrid-row:contextmenu", function (evt) {
                if (context._onRowContextMenu) {
                    var item = context.workunitsGrid.row(evt).data;
                    var cell = context.workunitsGrid.cell(evt);
                    var colField = cell.column.field;
                    var mystring = "item." + colField;
                    context._onRowContextMenu(item, colField, mystring);
                }
            });
            this.workunitsGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
                var selection = context.workunitsGrid.getSelected();
                if (selection.length > 0) {
                    context.downloadToList.set("disabled", false);
                } else {
                    context.downloadToList.set("disabled", true);
                }
            });

            aspect.after(context.workunitsGrid, "gotoPage", function (deferred, args) {
                return deferred.then(function () {
                    args[0] > 1 ? context._idleWatcher.stop() : context._idleWatcher.start();
                });
            });

            ESPUtil.goToPageUserPreference(this.workunitsGrid, "WUQueryWidget_GridRowsPerPage").then(function () {
                context.workunitsGrid.startup();
            });
        },

        refreshGrid: function (clearSelection) {
            this.workunitsGrid.set("query", this.getFilter());
            if (clearSelection) {
                this.workunitsGrid.clearSelection();
            }
        },

        refreshActionState: function () {
            var selection = this.workunitsGrid.getSelected();
            var hasSelection = false;
            var hasProtected = false;
            var hasNotProtected = false;
            var hasFailed = false;
            var hasNotFailed = false;
            var hasCompleted = false;
            var hasNotCompleted = false;

            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
                if (selection[i] && selection[i].Protected !== null) {
                    if (selection[i].Protected !== false) {
                        hasProtected = true;
                    } else {
                        hasNotProtected = true;
                    }
                }
                if (selection[i] && selection[i].StateID !== null) {
                    if (selection[i].StateID === 4) {
                        hasFailed = true;
                    } else {
                        hasNotFailed = true;
                    }
                    if (WsWorkunits.isComplete(selection[i].StateID, selection[i].ActionEx)) {
                        hasCompleted = true;
                    } else {
                        hasNotCompleted = true;
                    }
                }
            }

            this.wuCopyButton.set("disabled", !hasSelection)
            this.wuCopyButton.set("iconClass", !hasSelection ? "iconCopyDisabled" : "iconCopy")
            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Abort").set("disabled", !hasNotCompleted);
            registry.byId(this.id + "SetToFailed").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Protect").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Unprotect").set("disabled", !hasProtected);

            this.menuProtect.set("disabled", !hasNotProtected);
            this.menuUnprotect.set("disabled", !hasProtected);
        },

        ensurePane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new DelayLoadWidget({
                    id: id,
                    title: params.Wuid,
                    closable: true,
                    delayWidget: "WUDetailsWidget",
                    params: params
                });
                this.addChild(retVal, 1);
            }
            return retVal;
        }

    });
});
