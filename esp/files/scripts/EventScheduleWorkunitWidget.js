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
    "hpcc/WUDetailsWidget",
    "hpcc/WsWorkunits",
    "hpcc/ESPUtil",

    "dojo/text!../templates/EventScheduleWorkunitWidget.html",

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
                _TabContainerWidget, ESPBase, ESPWorkunit, ESPLogicalFile, TargetSelectWidget, WUDetailsWidget, WsWorkunits, ESPUtil,
                template) {
    return declare("EventScheduleWorkunitWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "EventScheduleWorkunitWidget",

        borderContainer: null,
        eventTab: null,
        eventGrid: null,
        clusterTargetSelect: null,

        initalized: false,
        loaded: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.eventTab = registry.byId(this.id + "_EventScheduledWorkunits");
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
            this.initEventGrid();
            this.selectChild(this.eventTab, true);
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id == this.eventTab.id) {
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

        initContextMenu: function ( ) {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "EventGrid"]
            });
             this.menuOpen = this.addMenuItem(pMenu, {
                label: "Open",
                onClick: function () { context._onOpen(); }
            });
            this.menuDeschedule = this.addMenuItem(pMenu, {
                label: "Deschedule",
                onClick: function () { context._onDeschedule(); }
            });
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

        initEventGrid: function (params) {
            var context = this;
            var store = WsWorkunits.CreateEventScheduleStore();
            this.eventGrid = new declare([Grid, Pagination, Selection, ColumnResizer, Keyboard, DijitRegistry, ESPUtil.GridHelper])({
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
                    col1: selector({ width: 27, selectorType: 'checkbox' }),
                    Wuid: {
                        label: "Workunit", width: 180, sortable: true,
                        formatter: function (Wuid, row) {
                            var wu = row.Server === "DFUserver" ? ESPDFUWorkunit.Get(Wuid) : ESPWorkunit.Get(Wuid);
                            return "<a href='#' class='" + context.id + "WuidClick'>" + Wuid + "</a>";
                        }

                    },
                    Cluster: { label: "Cluster", width: 108, sortable: true },
                    JobName: { label: "Jobname", sortable: true },
                    EventName: { label: "Event Name", width: 90, sortable: true },
                    EventText: { label: "Event Text", sortable: true }
                }
            },
            this.id + "EventGrid");
            this.eventGrid.set("noDataMessage", "<span class='dojoxGridNoData'>No Scheduled Events.</span>");

            var context = this;
            on(document, "." + context.id + "WuidClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.eventGrid.row(evt).data;
                    context._onRowDblClick(item);
                }
            });
            this.eventGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.eventGrid.row(evt).data;
                    context._onRowDblClick(item);
                }
            });
            this.eventGrid.on(".dgrid-row:contextmenu", function (evt) {
                if (context._onRowContextMenu) {
                    var item = context.eventGrid.row(evt).data;
                    var cell = context.eventGrid.cell(evt);
                    var colField = cell.column.field;
                    var mystring = "item." + colField;
                    context._onRowContextMenu(item, colField, mystring);
                }
            });
            this.eventGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
            });
            this.eventGrid.onContentChanged(function (object, removedFrom, insertedInto) {
                context.refreshActionState();
            });
            this.eventGrid.startup();
            this.refreshActionState();
        },

        refreshActionState: function () {
            var selection = this.eventGrid.getSelected();
            var hasSelection = false;
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
            }
            registry.byId(this.id + "Deschedule").set("disabled", !hasSelection);
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
            this.eventGrid.set("query", this.getFilter());
        },

        _onOpen: function (event) {
            var selections = this.eventGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePane(this.id + "_" + selections[i].Wuid, selections[i]);
                if (i == 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab, true);
            }
        },

        _onDeschedule: function (event) {
            if (confirm('Deschedule selected workunits?')) {
                var context = this;
                var selection = this.eventGrid.getSelected();
                WsWorkunits.WUAction(selection, "Deschedule", {
                    load: function (response) {
                        context.refreshGrid(response);
                    }
                });
            }
        },

        refreshGrid: function (args) {
            this.eventGrid.set("query", this.getFilter());
        },


        _onRowDblClick: function (item) {
            var wuTab = this.ensurePane(this.id + "_" + item.Wuid, item);
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
                retVal = new WUDetailsWidget({
                    id: id,
                    title: params.Wuid,
                    closable: true,
                    params: {
                        Wuid: params.Wuid
                    }
                });
                this.addChild(retVal, 1);
            }
            return retVal;
        }
    });
});