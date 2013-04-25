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
    "dojo/data/ObjectStore",
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

    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/Pagination",
    "dojox/grid/enhanced/plugins/IndirectSelection",

    "hpcc/_TabContainerWidget",
    "hpcc/WsDfu",
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

], function (declare, lang, arrayUtil, dom, domClass, domForm, ObjectStore, date, on,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry, Dialog, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                EnhancedGrid, Pagination, IndirectSelection,
                _TabContainerWidget, WsDfu, ESPLogicalFile, LFDetailsWidget, SFDetailsWidget, TargetSelectWidget,
                template) {
    return declare("DFUQueryWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "DFUQueryWidget",
        workunitsTab: null,
        workunitsGrid: null,

        tabMap: [],

        postCreate: function (args) {
            this.inherited(arguments);
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.workunitsGrid = registry.byId(this.id + "WorkunitsGrid");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initWorkunitsGrid();
            this.initFilter();
            this.refreshActionState();
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _onOpen: function (event) {
            var selections = this.workunitsGrid.selection.getSelected();
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
                WsDfu.DFUArrayAction(this.workunitsGrid.selection.getSelected(), "Delete", {
                    load: function (response) {
                        context.workunitsGrid.rowSelectCell.toggleAllSelection(false);
                        context.refreshGrid(response);
                    }
                });
            }
        },

        _onAddToSuperfileOk: function (event) {
            var context = this;
            var formData = domForm.toObject(this.id + "AddToSuperfileForm");
            WsDfu.AddtoSuperfile(this.workunitsGrid.selection.getSelected(), formData.Superfile, formData.ExistingFile, {
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

        getISOString: function (dateField, timeField) {
            var d = registry.byId(this.id + dateField).attr("value");
            var t = registry.byId(this.id + timeField).attr("value");
            if (d) {
                if (t) {
                    d.setHours(t.getHours());
                    d.setMinutes(t.getMinutes());
                    d.setSeconds(t.getSeconds());
                }
                return d.toISOString();
            }
            return "";
        },

        onRowContextMenu: function (idx, item, colField, mystring) {
            var selection = this.workunitsGrid.selection.getSelected();
            var found = arrayUtil.indexOf(selection, item);
            if (found == -1) {
                this.workunitsGrid.selection.deselectAll();
                this.workunitsGrid.selection.setSelected(idx, true);
            }
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

        clearFilter: function() {
            arrayUtil.forEach(registry.byId(this.id + "FilterForm").getDescendants(), function (item, idx) {
                item.set('value', null);
            });
        },

        hasFilter: function () {
            var filter = domForm.toObject(this.id + "FilterForm")
            for (var key in filter) {
                if (filter[key] != ""){
                    return true
                }
            }
            return false
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
                retVal.StartDate = date.add(now, "day", dom.byId(this.id + "LastNDays").value * -1).toISOString();
                retVal.EndDate = now.toISOString();
            }
            return retVal;
        },

        applyFilter: function () {
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);
            this.refreshGrid();
        },

        getISOString: function (dateField, timeField) {
            var d = registry.byId(this.id + dateField).attr("value");
            var t = registry.byId(this.id + timeField).attr("value");
            if (d) {
                if (t) {
                    d.setHours(t.getHours());
                    d.setMinutes(t.getMinutes());
                    d.setSeconds(t.getSeconds());
                }
                return d.toISOString();
            }
            return "";
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

        initWorkunitsGrid: function() {
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

            this.workunitsGrid.setStructure([
                {
                    name: "C", field: "isZipfile", width: "16px",
                    formatter: function (compressed) {
                        if (compressed == true) {
                            return "C";
                        }
                        return "";
                    }
                },
                {
                    name: "K", field: "IsKeyFile", width: "16px",
                    formatter: function (keyfile) {
                        if (keyfile == true) {
                            return "K";
                        }
                        return "";
                    }
                },
                {
                    name: "S", field: "isSuperfile", width: "16px",
                    formatter: function (superfile) {
                        if (superfile == true) {
                            return "S";
                        }
                        return "";
                    }
                },
                {
                    name: "Logical Name", field: "Name", width: "32",
                    formatter: function (name, idx) {
                        return "<a href=# rowIndex=" + idx + " class='LogicalNameClick'>" + name + "</a>";
                    }
                },
                { name: "Owner", field: "Owner", width: "8" },
                { name: "Description", field: "Description", width: "12" },
                { name: "Cluster", field: "ClusterName", width: "12" },
                { name: "Records", field: "RecordCount", width: "8" },
                { name: "Size", field: "Totalsize", width: "8" },
                { name: "Parts", field: "Parts", width: "4" },
                { name: "Modified (UTC/GMT)", field: "Modified", width: "12" }
            ]);
            var objStore = ESPLogicalFile.CreateLFQueryObjectStore();
            this.workunitsGrid.setStore(objStore, this.getFilter());
            this.workunitsGrid.noDataMessage = "<span class='dojoxGridNoData'>Zero Logical Files(check filter).</span>";

            on(document, ".LogicalNameClick:click", function (evt) {
                if (context.onRowDblClick) {
                    var idx = evt.target.getAttribute("rowIndex");
                    var item = context.workunitsGrid.getItem(idx);
                    context.onRowDblClick(item);
                }
            });

            this.workunitsGrid.on("RowDblClick", function (evt) {
                if (context.onRowDblClick) {
                    var idx = evt.rowIndex;
                    var item = this.getItem(idx);
                    context.onRowDblClick(item);
                }
            }, true);

            this.workunitsGrid.on("RowContextMenu", function (evt) {
                if (context.onRowContextMenu) {
                    var idx = evt.rowIndex;
                    var colField = evt.cell.field;
                    var item = this.getItem(idx);
                    var mystring = "item." + colField;
                    context.onRowContextMenu(idx, item, colField, mystring);
                }
            }, true);

            dojo.connect(this.workunitsGrid.selection, 'onSelected', function (idx) {
                context.refreshActionState();
            });
            dojo.connect(this.workunitsGrid.selection, 'onDeselected', function (idx) {
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
            this.workunitsGrid.setQuery(this.getFilter());
            var context = this;
            setTimeout(function () {
                context.refreshActionState()
            }, 200);
        },

        refreshActionState: function () {
            var selection = this.workunitsGrid.selection.getSelected();
            var hasSelection = false;
            var hasFilter = this.hasFilter();
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
            }

            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "AddtoDropDown").set("disabled", !hasSelection);
            dom.byId(this.id + "IconFilter").src = hasFilter ? "img/filter.png" : "img/noFilter.png";
        },

        ensurePane: function (id, params) {
            var obj = id.split("::");
            id = obj.join("");
            obj = id.split(".");
            id = obj.join("");
            var retVal = this.tabMap[id];
            if (!retVal) {
                retVal = registry.byId(id);
                if (!retVal) {
                    var context = this;
                    if (params.isSuperfile) {
                        retVal = new SFDetailsWidget.fixCircularDependency({
                            id: id,
                            title: params.Name,
                            closable: true,
                            onClose: function () {
                                //  Workaround for http://bugs.dojotoolkit.org/ticket/16475
                                context._tabContainer.removeChild(this);
                                delete context.tabMap[this.id];
                                return false;
                            },
                            _hpccParams: params
                        });
                    } else {
                        retVal = new LFDetailsWidget.fixCircularDependency({
                            id: id,
                            title: params.Name,
                            closable: true,
                            onClose: function () {
                                //  Workaround for http://bugs.dojotoolkit.org/ticket/16475
                                context._tabContainer.removeChild(this);
                                delete context.tabMap[this.id];
                                return false;
                            },
                            _hpccParams: params
                        });
                    }
                }
                this.tabMap[id] = retVal;
                this.addChild(retVal, 1);
            }
            return retVal;
        },

        onRowDblClick: function (item) {
            var wuTab = this.ensurePane(this.id + "_" + item.Name, item);
            this.selectChild(wuTab);
        }
    });
});
