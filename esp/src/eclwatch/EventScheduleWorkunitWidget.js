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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/dom-form",
    "dojo/_base/array",
    "dojo/on",

    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",

    "dgrid/Grid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/_TabContainerWidget",
    "hpcc/TargetSelectWidget",
    "hpcc/WUDetailsWidget",
    "hpcc/WsWorkunits",
    "hpcc/ESPUtil",
    "hpcc/FilterDropDownWidget",

    "dojo/text!../templates/EventScheduleWorkunitWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/form/TextBox",
    "dijit/form/DropDownButton",
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer"
], function (declare, lang, i18n, nlsHPCC, dom, domForm, arrayUtil, on,
                registry, Menu, MenuItem,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, TargetSelectWidget, WUDetailsWidget, WsWorkunits, ESPUtil, FilterDropDownWidget,
                template) {
    return declare("EventScheduleWorkunitWidget", [_TabContainerWidget], {
        i18n: nlsHPCC,
        templateString: template,
        baseClass: "EventScheduleWorkunitWidget",

        eventTab: null,
        eventGrid: null,
        filter: null,
        clusterTargetSelect: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.filter = registry.byId(this.id + "Filter");
            this.eventTab = registry.byId(this.id + "_EventScheduledWorkunits");
            this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initContextMenu();
        },

        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;

            this.clusterTargetSelect.init({
                Targets: true,
                includeBlank: true,
                Target: params.Cluster
            });
            this.initEventGrid();
            this.selectChild(this.eventTab, true);

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
                label: this.i18n.Open,
                onClick: function () { context._onOpen(); }
            });
            this.menuDeschedule = this.addMenuItem(pMenu, {
                label: this.i18n.Deschedule,
                onClick: function () { context._onDeschedule(); }
            });
            pMenu.startup();
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
                        label: this.i18n.Workunit, width: 100, sortable: true,
                        formatter: function (Wuid) {
                            return "<a href='#' class='" + context.id + "WuidClick'>" + Wuid + "</a>";
                        }
                    },
                    Cluster: { label: this.i18n.Cluster, width: 100, sortable: true },
                    JobName: { label: this.i18n.JobName, width: 108, sortable: true },
                    EventName: { label: this.i18n.EventName, width: 180, sortable: true },
                    EventText: { label: this.i18n.EventText, width: 180, sortable: true }
                }
            },
            this.id + "EventGrid");
            this.eventGrid.set("noDataMessage", "<span class='dojoxGridNoData'>" + this.i18n.NoScheduledEvents + "</span>");

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
            var hasSelection = selection.length > 0;
            registry.byId(this.id + "Deschedule").set("disabled", !hasSelection);
            registry.byId(this.id + "Open").set("disabled", !hasSelection);
        },

        _onRefresh: function (params) {
            this.refreshGrid();
        },

        _onEventClear: function(event) {
            arrayUtil.forEach(registry.byId(this.id + "FilterForm").getDescendants(), function (item, idx) {
                item.set('value', null);
            });
        },

        _onEventApply: function (event){
            var filterInfo = domForm.toObject(this.id + "FilterForm");
            WsWorkunits.WUPushEvent({
                request:{
                    EventName: filterInfo.EventName,
                    EventText: filterInfo.EventText
                }
            });
            registry.byId(this.id + "FilterDropDown").closeDropDown();
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
            if (confirm(this.i18n.DescheduleSelectedWorkunits)) {
                var context = this;
                var selection = this.eventGrid.getSelected();
                WsWorkunits.WUAction(selection, "Deschedule").then(function (response) {
                    context.refreshGrid(response);
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
            return this.filter.toObject();
        },

        ensurePane: function (id, params) {
            id = id.split(".").join("x");
            var retVal = registry.byId(id);
            if (!retVal) {
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