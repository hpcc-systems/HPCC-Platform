/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/common",
    "dojo/i18n!./nls/WUQueryWidget",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/date",
    "dojo/on",

    "dijit/registry",
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
    "hpcc/WsWorkunits",
    "hpcc/ESPUtil",
    "hpcc/ESPWorkunit",
    "hpcc/WUDetailsWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/FilterDropDownWidget",

    "dojo/text!../templates/WUQueryWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/RadioButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog"

], function (declare, lang, i18n, nlsCommon, nlsSpecific, arrayUtil, dom, domClass, domForm, date, on,
                registry, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, WsWorkunits, ESPUtil, ESPWorkunit, WUDetailsWidget, TargetSelectWidget, FilterDropDownWidget,
                template) {
    return declare("WUQueryWidget", [_TabContainerWidget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "WUQueryWidget",
        i18n: lang.mixin(nlsCommon, nlsSpecific),

        workunitsTab: null,
        workunitsGrid: null,
        filter: null,
        clusterTargetSelect: null,
        stateSelect: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.filter = registry.byId(this.id + "Filter");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
            this.stateSelect = registry.byId(this.id + "StateSelect");
            this.logicalFileSearchTypeSelect = registry.byId(this.id + "LogicalFileSearchType");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initContextMenu();
        },

        getTitle: function () {
            return this.i18n.title;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _onOpen: function (event) {
            var selections = this.workunitsGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePane(this.id + "_" + selections[i].Wuid, {
                    Wuid: selections[i].Wuid
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
                var selection = this.workunitsGrid.getSelected();
                WsWorkunits.WUAction(selection, "Delete", {
                    load: function (response) {
                        context.refreshGrid(response);
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
        },

        _onDeschedule: function (event) {
        },

        _onRowDblClick: function (wuid) {
            var wuTab = this.ensurePane(this.id + "_" + wuid, {
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

            if (item.Owner == "") {
                this.menuFilterOwner.set("disabled", true);
                this.menuFilterOwner.set("label", this.i18n.Owner + ":  " + this.i18n.NA);
            }
            if (item.Jobname == "") {
                this.menuFilterJobname.set("disabled", true);
                this.menuFilterJobname.set("label", this.i18n.JobName + ":  " + this.i18n.NA);
            }
            if (item.Cluster == "") {
                this.menuFilterCluster.set("disabled", true);
                this.menuFilterCluster.set("label", this.i18n.Cluster + ":  " + this.i18n.NA);
            }
            if (item.State == "") {
                this.menuFilterState.set("disabled", true);
                this.menuFilterState.set("label", this.i18n.State + ":  " + this.i18n.NA);
            }
        },

        //  Implementation  ---
        getFilter: function () {
            var retVal = this.filter.toObject();
            lang.mixin(retVal, {
                StartDate: this.getISOString("FromDate", "FromTime"),
                EndDate: this.getISOString("ToDate", "ToTime")
            });
            if (retVal.StartDate != "" && retVal.EndDate != "") {
                retVal["DateRB"] = "0";
            } else if (retVal.LastNDays != "") {
                retVal["DateRB"] = "0";
                var now = new Date();
                retVal.StartDate = date.add(now, "day", retVal.LastNDays * -1).toISOString();
                retVal.EndDate = now.toISOString();
            }
            return retVal;
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

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
            this.menuReschedule = this.addMenuItem(pMenu, {
                label: this.i18n.Reschedule,
                onClick: function () { context._onReschedule(); }
            });
            this.menuDeschedule = this.addMenuItem(pMenu, {
                label: this.i18n.Deschedule,
                onClick: function () { context._onDeschedule(); }
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
            var store = new ESPWorkunit.CreateWUQueryStore();
            this.workunitsGrid = new declare([Grid, Pagination, Selection, ColumnResizer, Keyboard, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: store,
                query: this.getFilter(),
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
                    Protected: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = "<img src='/esp/files/img/locked.png'>";
                        },
                        width: 25,
                        sortable: false,
                        formatter: function (protected) {
                            if (protected == true) {
                                return ("<img src='/esp/files/img/locked.png'>");
                            }
                            return "";
                        }
                    },
                    Wuid: {
                        label: this.i18n.WUID, width: 180,
                        formatter: function (Wuid, idx) {
                            var wu = ESPWorkunit.Get(Wuid);
                            return "<img src='" + wu.getStateImage() + "'>&nbsp;<a href='#' rowIndex=" + idx + " class='" + context.id + "WuidClick'>" + Wuid + "</a>";
                        }
                    },
                    Owner: { label: this.i18n.Owner, width: 90 },
                    Jobname: { label: this.i18n.JobName},
                    Cluster: { label: this.i18n.Cluster, width: 90 },
                    RoxieCluster: { label: this.i18n.RoxieCluster, width: 99 },
                    State: { label: this.i18n.State, width: 90 },
                    TotalThorTime: { label: this.i18n.TotalThorTime, width: 117 }
                }
            }, this.id + "WorkunitsGrid");
            this.workunitsGrid.noDataMessage = "<span class='dojoxGridNoData'>" + this.i18n.noDataMessage + "</span>";

            var context = this;
            on(document, "." + context.id + "WuidClick:click", function (evt) {
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
            });
            this.workunitsGrid.onContentChanged(function (object, removedFrom, insertedInto) {
                context.refreshActionState();
            });
            this.workunitsGrid.startup();
        },

        refreshGrid: function (args) {
            this.workunitsGrid.set("query", this.getFilter());
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
                    if (selection[i].Protected != "0") {
                        hasProtected = true;
                    } else {
                        hasNotProtected = true;
                    }
                }
                if (selection[i] && selection[i].StateID !== null) {
                    if (selection[i].StateID == "4") {
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

            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Abort").set("disabled", !hasNotCompleted);
            registry.byId(this.id + "SetToFailed").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Protect").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Unprotect").set("disabled", !hasProtected);
            registry.byId(this.id + "Reschedule").set("disabled", true);    //TODO
            registry.byId(this.id + "Deschedule").set("disabled", true);    //TODO

            this.menuProtect.set("disabled", !hasNotProtected);
            this.menuUnprotect.set("disabled", !hasProtected);
        },

        ensurePane: function (id, params) {
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new WUDetailsWidget({
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
