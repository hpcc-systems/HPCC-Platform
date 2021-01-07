define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/dom-form",
    "dojo/_base/array",

    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",

    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "hpcc/WUDetailsWidget",
    "src/WsWorkunits",
    "src/ESPUtil",

    "dojo/text!../templates/EventScheduleWorkunitWidget.html",

    "hpcc/TargetSelectWidget",
    "hpcc/FilterDropDownWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/ToolbarSeparator",
    "dijit/form/TextBox",
    "dijit/form/DropDownButton",
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer"
], function (declare, nlsHPCCMod, domForm, arrayUtil,
    registry, Menu, MenuItem,
    selector,
    _TabContainerWidget, WUDetailsWidget, WsWorkunits, ESPUtil,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
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

            this.filter.init({
                ws_key: "EventScheduleWorkunitRecentFilter",
                widget: this.widget
            });
            this.filter.on("clear", function (evt) {
                context.refreshHRef();
                context.refreshGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshHRef();
                context.eventGrid._currentPage = 0;
                context.refreshGrid();
            });
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.eventTab.id) {
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
            this.eventGrid = new declare([ESPUtil.Grid(true, true)])({
                store: store,
                query: this.getFilter(),
                columns: {
                    col1: selector({ width: 27, selectorType: "checkbox" }),
                    Wuid: {
                        label: this.i18n.Workunit, width: 180, sortable: false,
                        formatter: function (Wuid) {
                            return "<a href='#' onClick='return false;' class='dgrid-row-url'>" + Wuid + "</a>";
                        }
                    },
                    Cluster: { label: this.i18n.Cluster, width: 100, sortable: false },
                    JobName: { label: this.i18n.JobName, sortable: false },
                    EventName: { label: this.i18n.EventName, width: 180, sortable: false },
                    EventText: { label: this.i18n.EventText, width: 180, sortable: false },
                    Owner: { label: this.i18n.Owner, width: 180, sortable: false },
                    State: { label: this.i18n.State, width: 180, sortable: false }
                }
            }, this.id + "EventGrid");

            this.eventGrid.on(".dgrid-row-url:click", function (evt) {
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
            ESPUtil.goToPageUserPreference(this.eventGrid, "EventScheduleWorkunitWidget_GridRowsPerPage").then(function () {
                context.eventGrid.startup();
            });
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

        _onEventClear: function (event) {
            arrayUtil.forEach(registry.byId(this.id + "FilterForm").getDescendants(), function (item, idx) {
                item.set("value", null);
            });
        },

        _onEventApply: function (event) {
            var filterInfo = domForm.toObject(this.id + "FilterForm");
            WsWorkunits.WUPushEvent({
                request: {
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
                var tab = this.ensurePane(selections[i].Wuid, selections[i]);
                if (i === 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab, true);
            }
        },

        _onDeschedule: function (event) {
            var context = this;
            var selection = this.eventGrid.getSelected();
            var list = this.arrayToList(selection, "Wuid");
            if (confirm(this.i18n.DescheduleSelectedWorkunits + "\n" + list)) {
                WsWorkunits.WUAction(selection, "Deschedule").then(function (response) {
                    context.refreshGrid(response);
                });
            }
        },

        refreshGrid: function (args) {
            this.eventGrid.set("query", this.getFilter());
        },

        _onRowDblClick: function (item) {
            var wuTab = this.ensurePane(item.Wuid, item);
            this.selectChild(wuTab);
        },

        getFilter: function () {
            return this.filter.toObject();
        },

        ensurePane: function (id, params) {
            id = this.createChildTabID(id);
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
