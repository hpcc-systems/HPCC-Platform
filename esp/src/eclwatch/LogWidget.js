define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",

    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "src/ESPBase",
    "src/ESPUtil",
    "src/WsTopology",

    "dojo/text!../templates/LogWidget.html",

    "hpcc/TargetSelectWidget",
    "hpcc/FilterDropDownWidget",
    "hpcc/ECLSourceWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/CheckBox",
    "dijit/form/RadioButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog"

], function (declare, lang, nlsHPCCMod,
    registry,
    _TabContainerWidget, ESPBaseMod, ESPUtil, WsTopology,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("LogWidget", [_TabContainerWidget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "LogWidget",
        i18n: nlsHPCC,

        logTab: null,
        logGrid: null,
        filter: null,
        clusterTargetSelect: null,
        stateSelect: null,

        postCreate: function (args) {
            this.inherited(arguments);
            var context = this;

            this.logTab = registry.byId(this.id + "_Log");
            this.logTargetSelect = registry.byId(this.id + "logTargetSelect");
            this.logTargetSelect.on("change", function (evt) {
                context.refreshGrid();
            });
            this.reverseButton = registry.byId(this.id + "Reverse");
            this.filter = registry.byId(this.id + "Filter");
            this.filter.on("clear", function (evt) {
                context._onFilterType();
                context.refreshHRef();
                context.refreshGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshHRef();
                context.logGrid._currentPage = 0;
                context.refreshGrid();
            });
            this.rawText = registry.byId(this.id + "LogText");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_Log;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _doDownload: function (zip) {
            var base = new ESPBaseMod.ESPBase();
            var name = "//" + this.params.getNetaddress() + this.params.getLogDirectory() + "/" + this.logTargetSelect.get("value");
            var type = "tpcomp_log";
            window.open(base.getBaseURL("WsTopology") + "/SystemLog?Name=" + name + "&Type=" + type + "&Zip=" + zip, "_blank");
        },

        _onDownloadText: function (args) {
            this._doDownload(1);
        },

        _onDownloadZip: function (args) {
            this._doDownload(2);
        },

        _onDownloadGZip: function (args) {
            this._doDownload(3);
        },

        //  Implementation  ---
        getFilter: function () {
            var retVal = this.filter.toObject();
            if (retVal.FirstRows) {
                retVal.FilterType = 1;
            } else if (retVal.LastRows) {
                retVal.FilterType = 5;
            } else if (retVal.LastHours) {
                retVal.FilterType = 2;
            } else if (retVal.StartDate) {
                if (retVal.StartDate[0] === "T") {
                    retVal.StartDate = retVal.StartDate.substring(1);
                }
                if (retVal.EndDate[0] === "T") {
                    retVal.EndDate = retVal.EndDate.substring(1);
                }
                retVal.FilterType = 6;
            } else {
                retVal.FilterType = 4;
            }
            return retVal;
        },

        //  Implementation  ---
        init: function (params) {

            if (this.inherited(arguments))
                return;

            this.logTargetSelect.reset();

            if (params.newPreflight) {
                this.logTargetSelect.init({
                    Logs: true,
                    includeBlank: false,
                    treeNode: this.params
                });
            } else if (this.params.__hpcc_id === params.__hpcc_id) {
                this.initalized = false;
                this.inherited(arguments);

                this.logTargetSelect.init({
                    Logs: true,
                    includeBlank: false,
                    treeNode: this.params
                });
            }

            this.initLogGrid();
            if (!this.rawText.initialized) {
                this.rawText.initialized = true;
                this.rawText.init({
                    mode: "text"
                });
            }
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.logTab.id) {
                } else {
                    if (!currSel.initalized) {
                    }
                }
            }
        },

        initLogGrid: function () {
            var context = this;
            var store = new WsTopology.CreateTpLogFileStore();
            store.on("pageLoaded", function (page) {
                context.rawText.setText(page);
            });
            if (!this.logGrid) {
                this.logGrid = new declare([ESPUtil.Grid(true, true, { rowsPerPage: 500, pageSizeOptions: [500, 1000] })])({
                    store: store,
                    query: this.getFilter(),
                    columns: {
                        MsgID: { width: 80 },
                        Audience: { label: this.i18n.Audience, width: 60 },
                        Date: { label: this.i18n.Date, width: 100 },
                        Timemilli: { label: this.i18n.Time, width: 100 },
                        PID: { label: "PID", width: 60 },
                        TID: { label: "TID", width: 60 },
                        Details: { label: this.i18n.Details, width: 1024 },
                        dummy: { label: "", width: 0 }
                    }
                }, this.id + "LogGrid");

                var context = this;
                this.logGrid.on(".dgrid-row-url:click", function (evt) {
                    if (context._onRowDblClick) {
                        var item = context.logGrid.row(evt).data;
                        context._onRowDblClick(item.Wuid);
                    }
                });
                this.logGrid.on(".dgrid-row:dblclick", function (evt) {
                    if (context._onRowDblClick) {
                        var item = context.logGrid.row(evt).data;
                        context._onRowDblClick(item.Wuid);
                    }
                });
                this.logGrid.onSelectionChanged(function (event) {
                    context.refreshActionState();
                });
                this.logGrid.startup();
            }
        },

        refreshGrid: function (clearSelection) {
            this.rawText.setText(this.i18n.loadingMessage);
            var filter = lang.mixin(this.getFilter(), {
                Name: this.params.getNetaddress ? "//" + this.params.getNetaddress() + this.params.getLogDirectory() + "/" + this.logTargetSelect.get("value") : "//" + this.params.params.Netaddress + this.params.LogDirectory + "/" + this.logTargetSelect.get("value"),
                Type: "tpcomp_log",
                LoadData: 1
            });
            this.logGrid.set("query", filter);
            if (clearSelection) {
                this.logGrid.clearSelection();
            }
        },

        refreshActionState: function () {
            var selection = this.logGrid.getSelected();
        }
    });
});
