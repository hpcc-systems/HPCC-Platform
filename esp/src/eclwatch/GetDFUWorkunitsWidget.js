define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom-class",
    "dojo/topic",

    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "src/ESPUtil",
    "src/ESPDFUWorkunit",
    "src/FileSpray",
    "hpcc/DelayLoadWidget",
    "src/Utility",

    "dojo/text!../templates/GetDFUWorkunitsWidget.html",

    "hpcc/TargetSelectWidget",
    "hpcc/FilterDropDownWidget",
    "dijit/Dialog",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/Select",
    "dijit/form/CheckBox",
    "dijit/Dialog",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog"

], function (declare, i18n, nlsHPCC, arrayUtil, domClass, topic,
    registry, Menu, MenuItem, MenuSeparator, PopupMenuItem,
    selector,
    _TabContainerWidget, ESPUtil, ESPDFUWorkunit, FileSpray, DelayLoadWidget, Utility,
    template) {
    return declare("GetDFUWorkunitsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "GetDFUWorkunitsWidget",
        i18n: nlsHPCC,

        workunitsTab: null,
        workunitsGrid: null,
        filter: null,
        clusterTargetSelect: null,
        stateTargetSelect: null,
        username: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.filter = registry.byId(this.id + "Filter");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
            this.stateSelect = registry.byId(this.id + "StateSelect");
            this.downloadToList = registry.byId(this.id + "DownloadToList");
            this.downloadToListDialog = registry.byId(this.id + "DownloadToListDialog");
            this.downListForm = registry.byId(this.id + "DownListForm");
            this.fileName = registry.byId(this.id + "FileName");
            this.mineControl = registry.byId(this.id + "Mine");
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

        startup: function (args) {
            this.inherited(arguments);
            this.initContextMenu();
            this._idleWatcher = new ESPUtil.IdleWatcher();
            this._idleWatcher.start();
            var context = this;
            this._idleWatcherHandle = this._idleWatcher.on("idle", function () {
                context._onRefresh();
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
                var rowData = [cell.isProtected, cell.ID, cell.CommandMessage, cell.User, cell.JobName, cell.ClusterName, cell.StateMessage, cell.PercentDone];
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
            return this.i18n.title_GetDFUWorkunits;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _onOpen: function (event) {
            var selections = this.workunitsGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePane(selections[i].ID, {
                    Wuid: selections[i].ID
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
            var list = this.arrayToList(selection, "ID");
            if (confirm(this.i18n.DeleteSelectedWorkunits + "\n" + list)) {
                var context = this;
                FileSpray.DFUWorkunitsAction(selection, this.i18n.Delete, {
                    load: function (response) {
                        context.refreshGrid(true);
                    }
                });
            }
        },

        _onSetToFailed: function (event) {
            FileSpray.DFUWorkunitsAction(this.workunitsGrid.getSelected(), "SetToFailed");
        },

        _onProtect: function (event) {
            FileSpray.DFUWorkunitsAction(this.workunitsGrid.getSelected(), "Protect");
        },

        _onUnprotect: function (event) {
            FileSpray.DFUWorkunitsAction(this.workunitsGrid.getSelected(), "Unprotect");
        },

        _onRowDblClick: function (id) {
            var wuTab = this.ensurePane(id, {
                Wuid: id
            });
            this.selectChild(wuTab);
        },

        _onRowContextMenu: function (item, colField, mystring) {
            this.menuFilterJobname.set("disabled", false);
            this.menuFilterCluster.set("disabled", false);
            this.menuFilterState.set("disabled", false);

            if (item) {
                this.menuFilterJobname.set("label", this.i18n.Jobname + ":  " + item.JobName);
                this.menuFilterJobname.set("hpcc_value", item.JobName);
                this.menuFilterCluster.set("label", this.i18n.Cluster + ":  " + item.ClusterName);
                this.menuFilterCluster.set("hpcc_value", item.ClusterName);
                this.menuFilterState.set("label", this.i18n.State + ":  " + item.StateMessage);
                this.menuFilterState.set("hpcc_value", item.StateMessage);
            }
            if (item.Owner === "") {
                this.menuFilterOwner.set("disabled", true);
                this.menuFilterOwner.set("label", this.i18n.Owner + ":  " + this.i18n.NA);
            }
            if (item.JobName === "") {
                this.menuFilterJobname.set("disabled", true);
                this.menuFilterJobname.set("label", this.i18n.Jobname + ":  " + this.i18n.NA);
            }
            if (item.ClusterName === "") {
                this.menuFilterCluster.set("disabled", true);
                this.menuFilterCluster.set("label", this.i18n.Cluster + ":  " + this.i18n.NA);
            }
            if (item.StateMessage === "") {
                this.menuFilterState.set("disabled", true);
                this.menuFilterState.set("label", this.i18n.State + ":  " + this.i18n.NA);
            }
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (this.params.searchResults) {
                this.filter.disable(true);
            }
            if (params.ClusterName) {
                registry.byId(this.id + "Cluster").set("value", params.ClusterName);
            }
            this.initContextMenu();
            this.initWorkunitsGrid();
            this.clusterTargetSelect.init({
                Groups: true,
                includeBlank: true
            });
            this.stateSelect.init({
                DFUState: true,
                includeBlank: true
            });

            var context = this;
            this.filter.on("clear", function (evt) {
                context.refreshHRef();
                context.refreshGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshHRef();
                context.workunitsGrid._currentPage = 0;
                context.refreshGrid();
            });
            topic.subscribe("hpcc/dfu_wu_created", function (topic) {
                context.refreshGrid();
            });
            ESPUtil.MonitorVisibility(this.workunitsTab, function (visibility) {
                if (visibility) {
                    context.refreshGrid();
                }
            });

            topic.subscribe("hpcc/session_management_status", function (publishedMessage) {
                if (publishedMessage.status === "Unlocked") {
                    context.refreshGrid();
                    context._idleWatcher.start();
                } else if (publishedMessage.status === "Locked") {
                    context._idleWatcher.stop();
                }
            });

            this.userName = dojoConfig.username;
            if (this.userName === null) {
                this.mineControl.set("disabled", true);
            }
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.workunitsTab.id) {
                } else {
                    currSel.init(currSel.params);
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
            pMenu.addChild(new MenuItem({
                label: this.i18n.Open,
                onClick: function () { context._onOpen(); }
            }));
            pMenu.addChild(new MenuItem({
                label: this.i18n.Delete,
                onClick: function () { context._onDelete(); }
            }));
            pMenu.addChild(new MenuItem({
                label: this.i18n.SetToFailed,
                onClick: function () { context._onRename(); }
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: this.i18n.Protect,
                onClick: function () { context._onProtect(); }
            }));
            pMenu.addChild(new MenuItem({
                label: this.i18n.Unprotect,
                onClick: function () { context._onUnprotect(); }
            }));
            pMenu.addChild(new MenuSeparator());
            {
                var pSubMenu = new Menu();
                /*this.menuFilterType = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context._onFilterClear(null, true);
                        registry.byId(context.id + "Type").set("value", context.menuFilterType.get("hpcc_value"));
                        context.applyFilter();
                    }
                });
                this.menuFilterOwner = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context._onFilterClear(null, true);
                        registry.byId(context.id + "Owner").set("value", context.menuFilterOwner.get("hpcc_value"));
                        context.applyFilter();
                    }
                });*/
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
            var store = this.params.searchResults ? this.params.searchResults : new ESPDFUWorkunit.CreateWUQueryStore();
            this.workunitsGrid = new declare([ESPUtil.Grid(true, true, false, false, "GetDFUWorkunitsWidget")])({
                store: store,
                query: this.filter.toObject(),
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    isProtected: {
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
                    ID: {
                        label: this.i18n.ID,
                        width: 180,
                        formatter: function (ID, idx) {
                            var wu = ESPDFUWorkunit.Get(ID);
                            return "<img src='" + wu.getStateImage() + "'>&nbsp;<a href='#' class='dgrid-row-url'>" + ID + "</a>";
                        }
                    },
                    Command: {
                        label: this.i18n.Type,
                        width: 117,
                        formatter: function (command) {
                            if (command in FileSpray.CommandMessages) {
                                return FileSpray.CommandMessages[command];
                            }
                            return "Unknown";
                        }
                    },
                    User: { label: this.i18n.Owner, width: 90 },
                    JobName: { label: this.i18n.JobName, width: 500 },
                    ClusterName: { label: this.i18n.Cluster, width: 126 },
                    StateMessage: { label: this.i18n.State, width: 72 },
                    PercentDone: {
                        label: this.i18n.PctComplete, width: 90, sortable: false,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = Utility.valueCleanUp(value);
                        }
                    }
                }
            }, this.id + "WorkunitsGrid");
            this.workunitsGrid.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.workunitsGrid.row(evt).data;
                    context._onRowDblClick(item.ID);
                }
            });
            this.workunitsGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.workunitsGrid.row(evt).data;
                    context._onRowDblClick(item.ID);
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
            ESPUtil.goToPageUserPreference(this.workunitsGrid, "GetDFUWorkunitsWidget_GridRowsPerPage").then(function () {
                context.workunitsGrid.startup();
            });
        },

        refreshGrid: function (clearSelection) {
            this.workunitsGrid.set("query", this.filter.toObject());
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
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
                if (selection[i] && selection[i].isProtected && selection[i].isProtected !== false) {
                    hasProtected = true;
                } else {
                    hasNotProtected = true;
                }
                if (selection[i] && selection[i].State && selection[i].State === 5) {
                    hasFailed = true;
                } else {
                    hasNotFailed = true;
                }
            }

            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasNotProtected);
            registry.byId(this.id + "SetToFailed").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Protect").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Unprotect").set("disabled", !hasProtected);
        },

        ensurePane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                retVal = new DelayLoadWidget({
                    id: id,
                    title: params.Wuid,
                    closable: true,
                    delayWidget: "DFUWUDetailsWidget",
                    params: params
                });
                this.addChild(retVal, 1);
            }
            return retVal;
        }

    });
});
