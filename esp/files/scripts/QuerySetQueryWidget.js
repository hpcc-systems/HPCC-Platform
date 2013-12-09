/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/dom",
    "dojo/dom-form",
    "dojo/request/iframe",
    "dojo/_base/array",
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
    "hpcc/ESPBase",
    "hpcc/ESPWorkunit",
    "hpcc/ESPLogicalFile",
    "hpcc/TargetSelectWidget",
    "hpcc/QuerySetDetailsWidget",
    "hpcc/WsWorkunits",
    "hpcc/ESPQuery",
    "hpcc/ESPUtil",

    "dojo/text!../templates/QuerySetQueryWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/CheckBox",
    "dijit/ToolbarSeparator",
    "dijit/form/TextBox",
    "dijit/Dialog",
    "dijit/form/DropDownButton",
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer"
], function (declare, lang, dom, domForm, iframe, arrayUtil, on,
                registry, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, ESPBase, ESPWorkunit, ESPLogicalFile, TargetSelectWidget, QuerySetDetailsWidget, WsWorkunits, ESPQuery, ESPUtil,
                template) {
    return declare("QuerySetQueryWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "QuerySetQueryWidget",

        borderContainer: null,
        queriesTab: null,
        querySetGrid: null,
        clusterTargetSelect: null,

        initalized: false,
        loaded: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.queriesTab = registry.byId(this.id + "_Queries");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
            this.borderContainer = registry.byId(this.id + "BorderContainer");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initContextMenu();
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

            var context = this;
            var firstCall = true;
            this.clusterTargetSelect.init({
                Targets: true,
                includeBlank: true,
                Target: params.Cluster,
            });
            this.initQuerySetGrid();
            this.selectChild(this.queriesTab, true);
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id == this.queriesTab.id) {
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

        initContextMenu: function ( ) {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "QuerySetGrid"]
            });
             this.menuOpen = this.addMenuItem(pMenu, {
                label: "Open",
                onClick: function () { context._onOpen(); }
            });
            this.menuDelete = this.addMenuItem(pMenu, {
                label: "Delete",
                onClick: function () { context._onDelete(); }
            });
             pMenu.addChild(new MenuSeparator());
            this.menuUnsuspend = this.addMenuItem(pMenu, {
                label: "Unsuspend",
                onClick: function () { context._onUnsuspend(); }
            });
            this.menuSuspend= this.addMenuItem(pMenu, {
                label: "Suspend",
                onClick: function () { context._onSuspend(); }
            });
             pMenu.addChild(new MenuSeparator());
            this.menuActivate = this.addMenuItem(pMenu, {
                label: "Activate",
                onClick: function () { context._onActivate(); }
            });
            this.menuDeactivate = this.addMenuItem(pMenu, {
                label: "Deactivate",
                onClick: function () { context._onDeactivate(); }
            });
            pMenu.addChild(new MenuSeparator());
            {
                var pSubMenu = new Menu();
                /*
                this.menuFilterCluster = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.clearFilter();
                        registry.byId(context.id + "ClusterTargetSelect").set("value", context.menuFilterCluster.get("hpcc_value"));
                        context._onFilterApply();
                    }
                });*/
                this.menuFilterSuspend = this.addMenuItem(pSubMenu, {
                    onClick: function (args) {
                        context.clearFilter();
                        registry.byId(context.id + "Suspended").set("value", context.menuFilterSuspend.get("hpcc_value"));
                        context._onFilterApply();
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

        /*Not Applicable*/
        _onRowContextMenu: function (item, colField, mystring) {
            this.menuFilterSuspend.set("disabled", false);
            //this.menuFilterCluster.set("disabled", false);
            //this.menuFilterUnsuspend.set("disabled", false);
            //this.menuFilterActive.set("disabled", false);
            //this.menuFilterDeactive.set("disabled", false);

            if (item) {
                /*this.menuFilterCluster.set("label", "Cluster: " + item.QuerySetName);
                this.menuFilterCluster.set("hpcc_value", item.QuerySetName);*/
                this.menuFilterSuspend.set("label", "Suspend:  " + item.Suspended);
                this.menuFilterSuspend.set("hpcc_value", item.Suspended);
                /*
                this.menuFilterUnsuspend.set("label", "Unsuspend:  " + item.Suspend);
                this.menuFilterUnsuspend.set("hpcc_value", item.Suspend);
                this.menuFilterActive.set("label", "Active:  " + item.Active);
                this.menuFilterActive.set("hpcc_value", item.Active);
                this.menuFilterDeactivate.set("label", "Deactivate:  " + item.Active);
                this.menuFilterDeactivate.set("hpcc_value", item.Active);
            }
            if (item.Cluster == "") {
                this.menuFilterCluster.set("disabled", true);
                this.menuFilterCluster.set("label", "Cluster: " + "N/A");
            }*/

            if (item.Suspend == "") {
                this.menuFilterSuspend.set("disabled", true);
                this.menuFilterSuspend.set("label", "Suspend: " + "N/A");
            }
            /*
            if (item.Suspend == true) {
                this.menuFilterUnsuspend.set("disabled", false);
                this.menuFilterUnsuspend.set("label", "Unsuspend:  " + "N/A");
            }
            if (item.Active == false) {
                this.menuFilterActive.set("disabled", true);
                this.menuFilterActive.set("label", "Active:  " + "N/A");
            }
            if (item.Active == true) {
                this.menuFilterState.set("disabled", false);
                this.menuFilterState.set("label", "Deactivate:  " + "N/A");*/
            }
        },

        initQuerySetGrid: function (params) {
            var context = this;
            var store = ESPQuery.CreateQueryStore();
            this.querySetGrid = new declare([Grid, Pagination, Selection, ColumnResizer, Keyboard, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: store,
                query: this.getFilter(),
                sort: [{ attribute: "Id" }],
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
                    Suspended: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = "<img src='../files/img/suspended.png'>";
                        },
                        width: 20,
                        sortable: false,
                        formatter: function (suspended) {
                            if (suspended == true) {
                                return ("<img src='../files/img/suspended.png'>");
                            }
                            return "";
                        }
                    },
                    Activated: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = "<img src='../files/img/active.png'>";
                        },
                        width: 20,
                        sortable: false,
                        formatter: function (activated) {
                            if (activated == true) {
                                return ("<img src='../files/img/active.png'>");
                            }
                            return "";
                        }
                    },
                    ErrorCount: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = "<img src='../files/img/error.png'>";
                        },
                        width: 20,
                        sortable: false,
                        formatter: function (error) {
                            if (error > 0) {
                                return ("<img src='../files/img/error.png'>");
                            }
                            return "";
                        }
                    },
                    Id: {
                        width: 220,
                        label: "Id",
                        formatter: function (Id, idx) {
                            return "<a href='#' rowIndex=" + idx + " class='" + context.id + "WuidClick'>" + Id + "</a>";
                        }
                    },
                    Name: {
                        width: 180,
                        label: "Name"
                    },
                    QuerySetId:{
                        width: 180,
                        label: "Target",
                        sortable: false
                    },
                    Wuid: {
                        width: 180,
                        label: "Wuid"
                    },
                     Dll: {
                        width: 180,
                        label: "Dll"
                    },
                    Priority: {
                        width: 100,
                        label: "Priority",
                        sortable: false
                    },
                    IsLibrary: {
                        width: 100,
                        label: "Is Library",
                        sortable: false
                    },
                    PublishedBy: {
                        width: 180,
                        label: "Published By"
                    },
                },
            },
            this.id + "QuerySetGrid");
            //this.querySetGrid.set("noDataMessage", "<span class='dojoxGridNoData'>Zero Workunits (check filter).</span>");
            on(document, "." + context.id + "WuidClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.querySetGrid.row(evt).data;
                    context._onRowDblClick(item);
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
            });
            this.querySetGrid.onContentChanged(function (event) {
                context.refreshActionState();
            });
            this.querySetGrid.startup();
            this.refreshActionState();
        },

        refreshActionState: function () {
            var selection = this.querySetGrid.getSelected();
            var hasSelection = false;
            var isSuspended = false;
            var isNotSuspended = false;
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
                if (selection[i].Suspended != true) {
                    isSuspended = true;
                } else {
                    isNotSuspended = true;
                }
            }

            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "unSuspend").set("disabled", !isNotSuspended);
            registry.byId(this.id + "onSuspend").set("disabled", !isSuspended);
            registry.byId(this.id + "Activate").set("disabled", !hasSelection);
            registry.byId(this.id + "Deactivate").set("disabled", !hasSelection);
            registry.byId(this.id + "Open").set("disabled", !hasSelection);
        },

        _onRefresh: function (params) {
           this.refreshGrid();
        },

        _onFilterClear: function(event) {
            this.clearFilter();
            this.refreshGrid();
        },

        clearFilter: function () {
            arrayUtil.forEach(registry.byId(this.id + "FilterForm").getDescendants(), function (item, idx) {
                item.set('value', null);
            });
        },

        _onFilterApply: function(){
            this.querySetGrid.set("query", this.getFilter());
        },

        _onDelete:function(){
            if (confirm('Delete Selected Queries?')) {
                var context = this;
                WsWorkunits.WUQuerysetQueryAction(this.querySetGrid.getSelected(), "Delete").then(function (response) {
                    context.refreshGrid();
                });
            }
        },

        refreshGrid: function (args) {
            this.querySetGrid.set("query", this.getFilter());
        },

        _onSuspend: function(){
           var context = this;
           WsWorkunits.WUQuerysetQueryAction(this.querySetGrid.getSelected(), "Suspend").then(function (response) {
               context.refreshGrid();
           });
        },

        _onUnsuspend: function (){
            var context = this;
            WsWorkunits.WUQuerysetQueryAction(this.querySetGrid.getSelected(), "Unsuspend").then(function (response) {
                context.refreshGrid();
            });
        },

        _onActivate: function(){
            var context = this;
            WsWorkunits.WUQuerysetQueryAction(this.querySetGrid.getSelected(), "Activate").then(function (response) {
                context.refreshGrid();
            });
        },

         _onDeactivate: function(){
            var context = this;
            WsWorkunits.WUQuerysetQueryAction(this.querySetGrid.getSelected(), "Deactivate").then(function (response) {
                context.refreshGrid();
            });
        },

        _onOpen: function(){
            var selections = this.querySetGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePane(this.id + "_" + selections[i].Id, selections[i]);
                if (i == 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onRowDblClick: function (item) {
            var wuTab = this.ensurePane(this.id + "_" + item.Id, item);
            this.selectChild(wuTab);
        },

        getFilter: function(){
            var context = this;
            var retVal = domForm.toObject(this.id + "FilterForm");
            return retVal;
        },

        ensurePane: function (id, params) {
            id = id.split(".").join("x");
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new QuerySetDetailsWidget({
                    id: id,
                    title: params.Id,
                    closable: true,
                    hpcc: {
                        type: "QuerySetDetailsWidget",
                        params: params
                    }
                });
                this.addChild(retVal, 1);
            }
            return retVal;
        }
    });
});