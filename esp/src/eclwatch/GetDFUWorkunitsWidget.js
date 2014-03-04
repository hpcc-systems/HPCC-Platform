/*##############################################################################
#	HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#	Licensed under the Apache License, Version 2.0 (the "License");
#	you may not use this file except in compliance with the License.
#	You may obtain a copy of the License at
#
#	   http://www.apache.org/licenses/LICENSE-2.0
#
#	Unless required by applicable law or agreed to in writing, software
#	distributed under the License is distributed on an "AS IS" BASIS,
#	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#	See the License for the specific language governing permissions and
#	limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/date",
    "dojo/on",

    "dijit/registry",
    "dijit/Dialog",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "dgrid/Grid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPUtil",
    "hpcc/ESPDFUWorkunit",
    "hpcc/FileSpray",
    "hpcc/DFUWUDetailsWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/FilterDropDownWidget",

    "dojo/text!../templates/GetDFUWorkunitsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog"
], function (declare, lang, i18n, nlsHPCC, arrayUtil,dom, domClass, domForm, date, on,
                registry, Dialog, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, ESPUtil, ESPDFUWorkunit, FileSpray, DFUWUDetailsWidget, TargetSelectWidget, FilterDropDownWidget,
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

        postCreate: function (args) {
            this.inherited(arguments);
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.filter = registry.byId(this.id + "Filter");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
            this.stateSelect = registry.byId(this.id + "StateSelect");
        },

        startup: function (args) {
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
                var tab = this.ensurePane(this.id + "_" + selections[i].ID, {
                    Wuid: selections[i].ID
                });
                if (i == 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onDelete: function (event) {
            if (confirm(this.i18n.DeleteSelectedWorkunits)) {
                var context = this;
                FileSpray.DFUWorkunitsAction(this.workunitsGrid.getSelected(), this.i18n.Delete, {
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
            var wuTab = this.ensurePane(this.id + "_" + id, {
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
            if (item.Owner == "") {
                this.menuFilterOwner.set("disabled", true);
                this.menuFilterOwner.set("label", this.i18n.Owner + ":  " + this.i18n.NA);
            }
            if (item.JobName == "") {
                this.menuFilterJobname.set("disabled", true);
                this.menuFilterJobname.set("label", this.i18n.Jobname + ":  " + this.i18n.NA);
            }
            if (item.ClusterName == "") {
                this.menuFilterCluster.set("disabled", true);
                this.menuFilterCluster.set("label", this.i18n.Cluster + ":  " + this.i18n.NA);
            }
            if (item.StateMessage == "") {
                this.menuFilterState.set("disabled", true);
                this.menuFilterState.set("label", this.i18n.State + ":  " + this.i18n.NA);
            }
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

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
            this.selectChild(this.workunitsTab, true);

            var context = this;
            this.filter.on("clear", function (evt) {
                context.refreshGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshGrid();
            });
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id == this.workunitsTab.id) {
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

        initWorkunitsGrid: function() {
            var store = new ESPDFUWorkunit.CreateWUQueryStore();
            this.workunitsGrid = new declare([Grid, Pagination, Selection, ColumnResizer, Keyboard, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: store,
                rowsPerPage: 50,
                pagingLinks: 1,
                pagingTextBox: true,
                firstLastArrows: true,
                pageSizeOptions: [25, 50, 100],
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    isProtected: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = "<img src='/esp/files/img/locked.png'>";
                        },
                        width: 25,
                        sortable: false,
                        formatter: function (_protected) {
                            if (_protected == true) {
                                return ("<img src='/esp/files/img/locked.png'>");
                            }
                            return "";
                        }
                    },
                    ID: {
                        label: this.i18n.ID,
                        width: 180,
                        formatter: function (ID, idx) {
                            var wu = ESPDFUWorkunit.Get(ID);
                            return "<img src='" + wu.getStateImage() + "'>&nbsp;<a href='#' rowIndex=" + idx + " class='" + context.id + "IDClick'>" + ID + "</a>";
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
                    Owner: { label: this.i18n.Owner, width: 90 },
                    JobName: { label: this.i18n.JobName },
                    ClusterName: { label: this.i18n.Cluster, width: 126 },
                    StateMessage: { label: this.i18n.State, width: 72 },
                    PercentDone: { label: this.i18n.PctComplete, width: 90, sortable: false }
                }
            }, this.id + "WorkunitsGrid");
            this.workunitsGrid.noDataMessage = "<span class='dojoxGridNoData'>" + this.i18n.noDataMessage + "</span>";

            var context = this;
            on(document, "." + context.id + "IDClick:click", function (evt) {
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
            });
            this.workunitsGrid.onContentChanged(function (object, removedFrom, insertedInto) {
                context.refreshActionState();
            });
            this.workunitsGrid.startup();
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
                if (selection[i] && selection[i].isProtected && selection[i].isProtected != "0") {
                    hasProtected = true;
                } else {
                    hasNotProtected = true;
                }
                if (selection[i] && selection[i].State && selection[i].State == "5") {
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
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new DFUWUDetailsWidget.fixCircularDependency({
                    id: id,
                    title: params.Wuid,
                    closable: true,
                    params: params
                });
                this.addChild(retVal, 1);
            }
            return retVal;
        }

    });
});
