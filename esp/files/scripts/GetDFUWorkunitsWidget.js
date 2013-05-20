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
    "hpcc/ESPUtil",
    "hpcc/ESPDFUWorkunit",
    "hpcc/FileSpray",
    "hpcc/DFUWUDetailsWidget",
    "hpcc/TargetSelectWidget",

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
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer"

], function (declare, arrayUtil,dom, domClass, domForm, date, on,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry, Dialog, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, ESPUtil, ESPDFUWorkunit, FileSpray, DFUWUDetailsWidget, TargetSelectWidget,
                template) {
    return declare("GetDFUWorkunitsWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "GetDFUWorkunitsWidget",

        workunitsTab: null,
        workunitsGrid: null,
        clusterTargetSelect: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
        },

        startup: function (args) {
            this.inherited(arguments);
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
            if (confirm('Delete selected workunits?')) {
                var context = this;
                FileSpray.DFUWorkunitsAction(this.workunitsGrid.getSelected(), "Delete", {
                    load: function (response) {
                        context.refreshGrid(response);
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

        _onFilterApply: function (event) {
            registry.byId(this.id + "FilterDropDown").closeDropDown();
            if (this.hasFilter()) {
                this.applyFilter();
            } else {
                this.validateDialog.show();
            }
        },

        _onFilterClear: function(event) {
            this.clearFilter();
            this.applyFilter();
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
                this.menuFilterJobname.set("label", "Jobname:  " + item.JobName);
                this.menuFilterJobname.set("hpcc_value", item.JobName);
                this.menuFilterCluster.set("label", "Cluster:  " + item.ClusterName);
                this.menuFilterCluster.set("hpcc_value", item.ClusterName);
                this.menuFilterState.set("label", "State:  " + item.StateMessage);
                this.menuFilterState.set("hpcc_value", item.StateMessage);
            }
            if (item.Owner == "") {
                this.menuFilterOwner.set("disabled", true);
                this.menuFilterOwner.set("label", "Owner:  " + "N/A");
            }
            if (item.JobName == "") {
                this.menuFilterJobname.set("disabled", true);
                this.menuFilterJobname.set("label", "Jobname:  " + "N/A");
            }
            if (item.ClusterName == "") {
                this.menuFilterCluster.set("disabled", true);
                this.menuFilterCluster.set("label", "Cluster:  " + "N/A");
            }
            if (item.StateMessage == "") {
                this.menuFilterState.set("disabled", true);
                this.menuFilterState.set("label", "State:  " + "N/A");
            }
        },

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

            if (params.ClusterName) {
                registry.byId(this.id + "Cluster").set("value", params.ClusterName);
            }
            this.initContextMenu();
            this.initWorkunitsGrid();
            this.clusterTargetSelect.init({
                Groups: true,
                includeBlank: true
            });
            this.selectChild(this.workunitsTab, true);
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
                label: "Open",
                onClick: function () { context._onOpen(); }
            }));
            pMenu.addChild(new MenuItem({
                label: "Delete",
                onClick: function () { context._onDelete(); }
            }));
            pMenu.addChild(new MenuItem({
                label: "Set To Failed",
                onClick: function () { context._onRename(); }
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Protect",
                onClick: function () { context._onProtect(); }
            }));
            pMenu.addChild(new MenuItem({
                label: "Unprotect",
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

        initWorkunitsGrid: function() {
            var store = new ESPDFUWorkunit.CreateWUQueryStore();
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
                    isProtected: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = "<img src='../files/img/locked.png'>";
                        },
                        width: 25,
                        sortable: false,
                        formatter: function (_protected) {
                            if (_protected == true) {
                                return ("<img src='../files/img/locked.png'>");
                            }
                            return "";
                        }
                    },
                    ID: {
                        label: "ID",
                        width: 162,
                        formatter: function (ID, idx) {
                            var wu = ESPDFUWorkunit.Get(ID);
                            return "<img src='../files/" + wu.getStateImage() + "'>&nbsp<a href=# rowIndex=" + idx + " class='IDClick'>" + ID + "</a>";
                        }
                    },
                    Command: {
                        label: "Type",
                        width: 117,
                        formatter: function (command) {
                            if (command in FileSpray.CommandMessages) {
                                return FileSpray.CommandMessages[command];
                            }
                            return "Unknown";
                        }
                    },
                    Owner: { label: "Owner", width: 90 },
                    JobName: { label: "Job Name" },
                    ClusterName: { label: "Cluster", width: 126 },
                    StateMessage: { label: "State", width: 72 },
                    PercentDone: { label: "% Complete", width: 90, sortable: false}
                }
            }, this.id + "WorkunitsGrid");
            this.workunitsGrid.noDataMessage = "<span class='dojoxGridNoData'>Zero Workunits (check filter).</span>";

            var context = this;
            on(document, ".IDClick:click", function (evt) {
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

        initFilter: function () {
            this.validateDialog = new Dialog({
                title: "Filter",
                content: "No filter criteria specified."
            });
            var stateOptions = [{
                label: "any",
                value: ""
            }];
            for (var key in FileSpray.States) {
                stateOptions.push({
                    label: FileSpray.States[key],
                    value: FileSpray.States[key]
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
            var hasFilter = this.hasFilter();
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
