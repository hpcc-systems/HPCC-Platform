define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/dom-class",

    "dijit/registry",

    "hpcc/_Widget",
    "src/WsDfu",
    "src/ESPUtil",

    "dojo/text!../templates/DiskUsageWidget.html",

    "hpcc/FilterDropDownWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/Select",
    "dijit/form/TextBox",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox"

], function (declare, lang, nlsHPCCMod, domClass,
    registry,
    _Widget, WsDfu, ESPUtil,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("DiskUsageWidget", [_Widget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "DiskUsageWidget",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
            this.filter = registry.byId(this.id + "Filter");
        },

        resize: function (args) {
            this.inherited(arguments);
            this.widget.BorderContainer.resize();
        },

        getTitle: function () {
            return this.i18n.title_DiskUsage;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.initDiskUsageGrid();

            this.filter.refreshState();
            var context = this;
            this.filter.on("clear", function (evt) {
                context.refreshHRef();
                context.refreshGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshHRef();
                context.diskUsageGrid._currentPage = 0;
                context.refreshGrid();
            });
            this.refreshGrid();
        },

        initDiskUsageGrid: function () {
            var store = new WsDfu.CreateDiskUsageStore();
            this.diskUsageGrid = new declare([ESPUtil.Grid(false, true)])({
                store: store,
                query: this.getFilter(),
                columns: {
                    Name: { label: this.i18n.Grouping, width: 90, sortable: true },
                    NumOfFiles: {
                        label: this.i18n.FileCounts, width: 90, sortable: true,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = value;
                        },
                    },
                    TotalSize: {
                        label: this.i18n.TotalSize, width: 125, sortable: true,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = value;
                        },
                    },
                    LargestFile: { label: this.i18n.LargestFile, sortable: true },
                    LargestSize: {
                        label: this.i18n.LargestSize, width: 125, sortable: true,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = value;
                        },
                    },
                    SmallestFile: { label: this.i18n.SmallestFile, sortable: true },
                    SmallestSize: {
                        label: this.i18n.SmallestSize, width: 125, sortable: true,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = value;
                        },
                    },
                    NumOfFilesUnknown: {
                        label: this.i18n.FilesWithUnknownSize, width: 160, sortable: true,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = value;
                        },
                    }
                }
            }, this.id + "DiskUsageGrid");
        },

        getFilter: function () {
            var retVal = this.filter.toObject();
            lang.mixin(retVal, {
                StartDate: this.getISOString("FromDate", "FromTime"),
                EndDate: this.getISOString("ToDate", "ToTime")
            });
            return retVal;
        },

        refreshGrid: function (clearSelection) {
            this.diskUsageGrid.set("query", this.getFilter());
            if (clearSelection) {
                this.diskUsageGrid.clearSelection();
            }
        }
    });
});
