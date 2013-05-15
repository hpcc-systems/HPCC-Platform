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
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/date",
    "dojo/on",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
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
    "hpcc/WsWorkunits",
    "hpcc/ESPUtil",
    "hpcc/ESPWorkunit",
    "hpcc/WUDetailsWidget",
    "hpcc/TargetSelectWidget",

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
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer"

], function (declare, lang, arrayUtil, dom, domClass, domForm, date, on,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry, Dialog, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, WsWorkunits, ESPUtil, ESPWorkunit, WUDetailsWidget, TargetSelectWidget,
                template) {
    return declare("WUQueryWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "WUQueryWidget",

        workunitsTab: null,
        workunitsGrid: null,

        validateDialog: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initContextMenu();
            this.initFilter();
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
            if (confirm('Delete selected workunits?')) {
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

        _onFilterApply: function (event) {
            registry.byId(this.id + "FilterDropDown").closeDropDown();
            if (this.hasFilter()) {
                this.applyFilter();
            } else {
                this.validateDialog.show();
            }
        },

        _onFilterClear: function (event) {
            this.clearFilter();
            this.applyFilter();
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
                this.menuFilterOwner.set("label", "Owner:  " + "N/A");
            }
            if (item.Jobname == "") {
                this.menuFilterJobname.set("disabled", true);
                this.menuFilterJobname.set("label", "Jobname:  " + "N/A");
            }
            if (item.Cluster == "") {
                this.menuFilterCluster.set("disabled", true);
                this.menuFilterCluster.set("label", "Cluster:  " + "N/A");
            }
            if (item.State == "") {
                this.menuFilterState.set("disabled", true);
                this.menuFilterState.set("label", "State:  " + "N/A");
            }
        },

        //  Implementation  ---
        clearFilter: function () {
            arrayUtil.forEach(registry.byId(this.id + "FilterForm").getDescendants(), function (item, idx) {
                item.set('value', null);
            });
        },

        hasFilter: function () {
            var filter = domForm.toObject(this.id + "FilterForm");
            for (var key in filter) {
                if (filter[key] != "") {
                    return true;
                }
            }
            return false;
        },

        getFilter: function () {
            var retVal = domForm.toObject(this.id + "FilterForm");
            lang.mixin(retVal, {
                Cluster: this.clusterTargetSelect.get("value"),
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

        applyFilter: function () {
            this.refreshGrid();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;

            this.clusterTargetSelect.init({
                Targets: true,
                includeBlank: true,
                Target: params.Cluster
            });

            this.initWorkunitsGrid();
            this.selectChild(this.workunitsTab, true);
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
                label: "Open",
                onClick: function () { context._onOpen(); }
            });
            this.menuDelete = this.addMenuItem(pMenu, {
                label: "Delete",
                onClick: function () { context._onDelete(); }
            });
            this.menuSetToFailed = this.addMenuItem(pMenu, {
                label: "Set To Failed",
                onClick: function () { context._onSetToFailed(); }
            });
            pMenu.addChild(new MenuSeparator());
            this.menuProtect = this.addMenuItem(pMenu, {
                label: "Protect",
                onClick: function () { context._onProtect(); }
            });
            this.menuUnprotect = this.addMenuItem(pMenu, {
                label: "Unprotect",
                onClick: function () { context._onUnprotect(); }
            });
            pMenu.addChild(new MenuSeparator());
            this.menuReschedule = this.addMenuItem(pMenu, {
                label: "Reschedule",
                onClick: function () { context._onReschedule(); }
            });
            this.menuDeschedule = this.addMenuItem(pMenu, {
                label: "Deschedule",
                onClick: function () { context._onDeschedule(); }
            });
            pMenu.addChild(new MenuSeparator());
            {
                var pSubMenu = new Menu();
                this.menuFilterOwner = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.clearFilter();
                        registry.byId(context.id + "Owner").set("value", context.menuFilterOwner.get("hpcc_value"));
                        context.applyFilter();
                    }
                });
                this.menuFilterJobname = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.clearFilter();
                        registry.byId(context.id + "Jobname").set("value", context.menuFilterJobname.get("hpcc_value"));
                        context.applyFilter();
                    }
                });
                this.menuFilterCluster = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.clearFilter();
                        registry.byId(context.id + "ClusterTargetSelect").set("value", context.menuFilterCluster.get("hpcc_value"));
                        context.applyFilter();
                    }
                });
                this.menuFilterState = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.clearFilter();
                        registry.byId(context.id + "State").set("value", context.menuFilterState.get("hpcc_value"));
                        context.applyFilter();
                    }
                });
                pSubMenu.addChild(new MenuSeparator());
                this.menuFilterClearFilter = this.addMenuItem(pSubMenu, {
                    label: "Clear",
                    onClick: function () {
                        context._onFilterClear();
                    }
                });
                pMenu.addChild(new PopupMenuItem({
                    label: "Filter",
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
                rowsPerPage: 25,
                firstLastArrows: true,
                pageSizeOptions: [25, 50, 100],
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    Protected: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = "<img src='../files/img/locked.png'>";
                        },
                        width: 25,
                        sortable: false,
                        formatter: function (protected) {
                            if (protected == true) {
                                return ("<img src='../files/img/locked.png'>");
                            }
                            return "";
                        }
                    },
                    Wuid: {
                        label: "Wuid", width: 162,
                        formatter: function (Wuid, idx) {
                            var wu = ESPWorkunit.Get(Wuid);
                            return "<img src='../files/" + wu.getStateImage() + "'>&nbsp<a href=# rowIndex=" + idx + " class='WuidClick' Wuid=>" + Wuid + "</a>";
                        }
                    },
                    Owner: { label: "Owner", width: 90 },
                    Jobname: { label: "Job Name"},
                    Cluster: { label: "Cluster", width: 90 },
                    RoxieCluster: { label: "Roxie Cluster", width: 99 },
                    State: { label: "State", width: 90 },
                    TotalThorTime: { label: "Total Thor Time", width: 117 }
                }
            }, this.id + "WorkunitsGrid");
            this.workunitsGrid.noDataMessage = "<span class='dojoxGridNoData'>Zero Workunits (check filter).</span>";

            var context = this;
            on(document, ".WuidClick:click", function (evt) {
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

        initFilter: function () {
            this.validateDialog = new Dialog({
                title: "Filter",
                content: "No filter criteria specified."
            });
            var stateOptions = [{
                label: "any",
                value: ""
            }];
            for (var key in WsWorkunits.States) {
                stateOptions.push({
                    label: WsWorkunits.States[key],
                    value: WsWorkunits.States[key]
                });
            }
            var stateSelect = registry.byId(this.id + "State");
            stateSelect.addOption(stateOptions);
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
                    if (WsWorkunits.isComplete(selection[i].StateID)) {
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

            this.refreshFilterState();
        },

        refreshFilterState: function () {
            var hasFilter = this.hasFilter();
            dom.byId(this.id + "IconFilter").src = hasFilter ? "img/filter.png" : "img/noFilter.png";
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
