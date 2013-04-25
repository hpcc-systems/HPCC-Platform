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
    "dojo/dom",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/data/ObjectStore",
    "dojo/date",
    "dojo/on",
    "dojo/_base/array",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",
    "dijit/Dialog",

    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/Pagination",
    "dojox/grid/enhanced/plugins/IndirectSelection",

    "hpcc/_TabContainerWidget",
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

], function (declare, dom, domClass, domForm, ObjectStore, date, on, arrayUtil, 
                _TemplatedMixin, _WidgetsInTemplateMixin, registry, Menu, MenuItem, MenuSeparator, PopupMenuItem, Dialog,
                EnhancedGrid, Pagination, IndirectSelection,
                _TabContainerWidget, ESPDFUWorkunit, FileSpray, DFUWUDetailsWidget, TargetSelectWidget,
                template) {
    return declare("GetDFUWorkunitsWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "GetDFUWorkunitsWidget",
        workunitsTab: null,
        workunitsGrid: null,
        clusterTargetSelect: null,

        tabMap: [],

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.workunitsTab = registry.byId(this.id + "_Workunits");
            this.workunitsGrid = registry.byId(this.id + "WorkunitsGrid");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initFilter();
        },

        resize: function (args) {
            this.inherited(arguments);
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },
        _onOpen: function (event) {
            var selections = this.workunitsGrid.selection.getSelected();
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
                FileSpray.DFUWorkunitsAction(this.workunitsGrid.selection.getSelected(), "Delete", {
                    load: function (response) {
                        context.workunitsGrid.rowSelectCell.toggleAllSelection(false);
                        context.refreshGrid(response);
                    }
                });
            }
        },
        _onSetToFailed: function (event) {
            var context = this;
            FileSpray.DFUWorkunitsAction(this.workunitsGrid.selection.getSelected(), "SetToFailed", {
                load: function (response) {
                    context.refreshGrid(response);
                }
            });
        },
        _onProtect: function (event) {
            var context = this;
            FileSpray.DFUWorkunitsAction(this.workunitsGrid.selection.getSelected(), "Protect", {
                load: function (response) {
                    context.refreshGrid(response);
                }
            });
        },
        _onUnprotect: function (event) {
            var context = this;
            FileSpray.DFUWorkunitsAction(this.workunitsGrid.selection.getSelected(), "Unprotect", {
                load: function (response) {
                    context.refreshGrid(response);
                }
            });
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

        _onRowContextMenu: function (idx, item, colField, mystring) {
            var selection = this.workunitsGrid.selection.getSelected();
            var found = arrayUtil.indexOf(selection, item);
            if (found == -1) {
                this.workunitsGrid.selection.deselectAll();
                this.workunitsGrid.selection.setSelected(idx, true);
            }
            this.menuFilterJobname.set("disabled", false);
            this.menuFilterCluster.set("disabled", false);
            this.menuFilterState.set("disabled", false);

            if (item) {
                //this.menuFilterType.set("label", "Type:  " + item.Type);
                //this.menuFilterType.set("hpcc_value", item.Type);
                //this.menuFilterOwner.set("label", "Owner:  " + item.Owner);
                //this.menuFilterOwner.set("hpcc_value", item.Owner);
                this.menuFilterJobname.set("label", "Jobname:  " + item.JobName);
                this.menuFilterJobname.set("hpcc_value", item.JobName);
                this.menuFilterCluster.set("label", "Cluster:  " + item.ClusterName);
                this.menuFilterCluster.set("hpcc_value", item.ClusterName);
                this.menuFilterState.set("label", "State:  " + item.StateMessage);
                this.menuFilterState.set("hpcc_value", item.StateMessage);
            }
            /*if (item.Type == "") {
                this.menuFilterType.set("disabled", true);
                this.menuFilterType.set("label", "Type:  " + "N/A");
            }*/
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

        clearFilter: function() {
            arrayUtil.forEach(registry.byId(this.id + "FilterForm").getDescendants(), function (item, idx) {
                item.set('value', null);
            });
        },

        hasFilter: function () {
            var filter = domForm.toObject(this.id + "FilterForm")
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

            if (params.ClusterName) {
                registry.byId(this.id + "Cluster").set("value", params.ClusterName);
            }
            this.initWorkunitsGrid();
            this.refreshActionState();
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

        initWorkunitsGrid: function() {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "WorkunitsGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: "Open",
                onClick: function(){context._onOpen();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Delete",
                onClick: function(){context._onDelete();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Set To Failed",
                onClick: function(){context._onRename();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Protect",
                onClick: function(){context._onProtect();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Unprotect",
                onClick: function(){context._onUnprotect();}
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
            this.workunitsGrid.setStructure([
                {
                    name: "<img src='../files/img/locked.png'>",
                    field: "isProtected",
                    width: "16px",
                    formatter: function (_protected) {
                        if (_protected == true) {
                            return ("<img src='../files/img/locked.png'>");
                        }
                        return "";
                    }
                },
                { 
                    name: "ID", field: "ID", width: "15", 
                    formatter: function (ID, idx) {
                        var wu = ESPDFUWorkunit.Get(ID);
                        return "<img src='../files/" + wu.getStateImage() + "'>&nbsp<a href=# rowIndex=" + idx + " class='IDClick'>" + ID + "</a>";
                    }
                },
                {
                    name: "Type", field: "Command", width: "8",
                    formatter: function (command) {
                        if (command in FileSpray.CommandMessages) {
                            return FileSpray.CommandMessages[command];
                        }
                        return "Unknown";
                    }
                },
                { name: "Owner", field: "Owner", width: "8" },
                { name: "Job Name", field: "JobName", width: "16" },
                { name: "Cluster", field: "ClusterName", width: "8" },
                { name: "State", field: "StateMessage", width: "8" },
                { name: "% Complete", field: "PercentDone", width: "8" }
            ]);
            var objStore = new ESPDFUWorkunit.CreateWUQueryObjectStore();
            this.workunitsGrid.setStore(objStore, this.getFilter());
            this.workunitsGrid.noDataMessage = "<span class='dojoxGridNoData'>Zero DFU Workunits (check filter).</span>";

            on(document, ".IDClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var idx = evt.target.getAttribute("rowIndex");
                    var item = context.workunitsGrid.getItem(idx);
                    context._onRowDblClick(item.ID);
                }
            });

            this.workunitsGrid.on("RowDblClick", function (evt) {
                if (context._onRowDblClick) {
                    var idx = evt.rowIndex;
                    var item = this.getItem(idx);
                    var Wuid = this.store.getValue(item, "ID");
                    context._onRowDblClick(Wuid);
                }
            }, true);

            this.workunitsGrid.on("RowContextMenu", function (evt) {
                if (context._onRowContextMenu) {
                    var idx = evt.rowIndex;
                    var colField = evt.cell.field;
                    var item = this.getItem(idx);
                    var mystring = "item." + colField;
                    context._onRowContextMenu(idx, item, colField, mystring);
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
            this.workunitsGrid.setQuery(this.getFilter());
            var context = this;
            setTimeout(function () {
                context.refreshActionState()
            }, 200);
        },

        refreshActionState: function () {
            var selection = this.workunitsGrid.selection.getSelected();
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
            var retVal = this.tabMap[id];
            if (!retVal) {
                retVal = registry.byId(id);
                if (!retVal) {
                    var context = this;
                    retVal = new DFUWUDetailsWidget({
                        id: id,
                        title: params.Wuid,
                        closable: true,
                        onClose: function () {
                            //  Workaround for http://bugs.dojotoolkit.org/ticket/16475
                            context._tabContainer.removeChild(this);
                            delete context.tabMap[this.id];
                            return false;
                        },
                        params: params
                    });
                }
                this.tabMap[id] = retVal;
                this.addChild(retVal, 1);
            }
            return retVal;
        }

    });
});
