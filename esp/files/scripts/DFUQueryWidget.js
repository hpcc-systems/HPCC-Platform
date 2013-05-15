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
    "hpcc/WsDfu",
    "hpcc/ESPUtil",
    "hpcc/ESPLogicalFile",
    "hpcc/LFDetailsWidget",
    "hpcc/SFDetailsWidget",
    "hpcc/TargetSelectWidget",

    "dojo/text!../templates/DFUQueryWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer"

], function (declare, lang, arrayUtil, dom, domClass, domForm, date, on,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry, Dialog, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, WsDfu, ESPUtil, ESPLogicalFile, LFDetailsWidget, SFDetailsWidget, TargetSelectWidget,
                template) {
    return declare("DFUQueryWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "DFUQueryWidget",

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
            this.initWorkunitsGrid();
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
                var tab = this.ensurePane(this.id + "_" + selections[i].Name, selections[i]);
                if (i == 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab, true);
            }
        },

        _onDelete: function (event) {
            if (confirm('Delete selected files?')) {
                var context = this;
                WsDfu.DFUArrayAction(this.workunitsGrid.getSelected(), "Delete", {
                    load: function (response) {
                        context.refreshGrid(response);
                    }
                });
            }
        },

        _onAddToSuperfileOk: function (event) {
            var context = this;
            var formData = domForm.toObject(this.id + "AddToSuperfileForm");
            WsDfu.AddtoSuperfile(this.workunitsGrid.getSelected(), formData.Superfile, formData.ExistingFile, {
                load: function (response) {
                    context.workunitsGrid.rowSelectCell.toggleAllSelection(false);
                    context.refreshGrid(response);
                }
            });
            var d = registry.byId(this.id + "AddtoDropDown");
            registry.byId(this.id + "AddtoDropDown").closeDropDown();
        },

        _onAddToSuperfileCancel: function (event) {
            var d = registry.byId(this.id + "AddtoDropDown");
            registry.byId(this.id + "AddtoDropDown").closeDropDown();
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

        _onRowDblClick: function (item) {
            var wuTab = this.ensurePane(this.id + "_" + item.Name, item);
            this.selectChild(wuTab);
        },

        _onRowContextMenu: function (item, colField, mystring) {
            this.menuFilterOwner.set("disabled", false);
            this.menuFilterCluster.set("disabled", false);

            if (item) {
                this.menuFilterOwner.set("label", "Owner:  " + item.Owner);
                this.menuFilterOwner.set("hpcc_value", item.Owner);
                this.menuFilterCluster.set("label", "Cluster:  " + item.ClusterName);
                this.menuFilterCluster.set("hpcc_value", item.ClusterName);
            }
            if (item.Owner == "") {
                this.menuFilterOwner.set("disabled", true);
                this.menuFilterOwner.set("label", "Owner:  " + "N/A");
            }
            if (item.ClusterName == "") {
                this.menuFilterCluster.set("disabled", true);
                this.menuFilterCluster.set("label", "Cluster:  " + "N/A");
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
                StartDate: this.getISOString("FromDate", "FromTime"),
                EndDate: this.getISOString("ToDate", "ToTime")
            });
            if (retVal.StartDate != "" && retVal.EndDate != "") {
            } else if (retVal.FirstN) {
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
                Groups: true,
                includeBlank: true
            });
            this.selectChild(this.workunitsTab, true);
        },

        initTab: function() {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id == this.workunitsTab.id) {
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel._hpccParams);
                    }
                }
            }
        },

        addMenuItem: function (menu, details) {
            var menuItem = new MenuItem(details);
            menu.addChild(menuItem);
            return menuItem;
        },

        initContextMenu: function() {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "WorkunitsGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: "Refresh",
                onClick: function(args){context._onRefresh();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Open",
                onClick: function(args){context._onOpen();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Delete",
                onClick: function(args){context._onDelete();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Add To Superfile",
                onClick: function(args){dijit.byId(context.id+"AddtoDropDown").openDropDown()}
            }));
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
                this.menuFilterCluster = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.clearFilter();
                        registry.byId(context.id + "ClusterTargetSelect").set("value", context.menuFilterCluster.get("hpcc_value"));
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
            var store = new ESPLogicalFile.CreateLFQueryStore();
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
                    isZipfile: {
                        label: "C", width: 16, sortable: false,
                        formatter: function (compressed) {
                            if (compressed == true) {
                                return "C";
                            }
                            return "";
                        }
                    },
                    IsKeyFile: {
                        label: "K", width: 16, sortable: false,
                        formatter: function (keyfile) {
                            if (keyfile == true) {
                                return "K";
                            }
                            return "";
                        }
                    },
                    isSuperfile: {
                        label: "S", width: 16, sortable: false,
                        formatter: function (superfile) {
                            if (superfile == true) {
                                return "S";
                            }
                            return "";
                        }
                    },
                    Name: { label: "Logical Name",
                        formatter: function (name, idx) {
                            return "<a href=# rowIndex=" + idx + " class='LogicalNameClick'>" + name + "</a>";
                        }
                    },
                    Owner: { label: "Owner", width: 72 },
                    Description: { label: "Description", width: 153 },
                    ClusterName: { label: "Cluster", width: 108 },
                    RecordCount: { label: "Records", width: 72, sortable: false },
                    Totalsize: { label: "Size", width: 72, sortable: false },
                    Parts: { label: "Parts", width: 45, sortable: false },
                    Modified: { label: "Modified (UTC/GMT)", width: 155, sortable: false }
                }
            }, this.id + "WorkunitsGrid");
            this.workunitsGrid.noDataMessage = "<span class='dojoxGridNoData'>Zero Logical Files(check filter).</span>";

            var context = this;
            on(document, ".LogicalNameClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.workunitsGrid.row(evt).data;
                    context._onRowDblClick(item);
                }
            });
            this.workunitsGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.workunitsGrid.row(evt).data;
                    context._onRowDblClick(item);
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
        },

        refreshGrid: function (args) {
            this.workunitsGrid.set("query", this.getFilter());
        },

        refreshActionState: function () {
            var selection = this.workunitsGrid.getSelected();
            var hasSelection = false;
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
            }

            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "AddtoDropDown").set("disabled", !hasSelection);

            this.refreshFilterState();
        },

        refreshFilterState: function () {
            var hasFilter = this.hasFilter();
            dom.byId(this.id + "IconFilter").src = hasFilter ? "img/filter.png" : "img/noFilter.png";
        },

        ensurePane: function (id, params) {
            var obj = id.split("::");
            id = obj.join("");
            obj = id.split(".");
            id = obj.join("");
            var retVal = registry.byId(id);
            if (!retVal) {
                if (params.isSuperfile) {
                    retVal = new SFDetailsWidget.fixCircularDependency({
                        id: id,
                        title: params.Name,
                        closable: true,
                        _hpccParams: params
                    });
                } else {
                    retVal = new LFDetailsWidget.fixCircularDependency({
                        id: id,
                        title: params.Name,
                        closable: true,
                        _hpccParams: params
                    });
                }
                this.addChild(retVal, 1);
            }
            return retVal;
        }

    });
});
