define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/io-query",

    "dijit/registry",
    "dijit/form/TextBox",

    "dgrid/Grid",
    "dgrid/Keyboard",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/CompoundColumns",
    "dgrid/extensions/DijitRegistry",
    "src/Pagination",

    "hpcc/_Widget",
    "src/ESPBase",
    "src/ESPWorkunit",
    "src/ESPLogicalFile",
    "hpcc/TableContainer",
    "src/DataPatterns/DGridHeaderHook",

    "dojo/text!../templates/ResultWidget.html",

    "hpcc/FilterDropDownWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/ToolbarSeparator"
], function (declare, lang, arrayUtil, i18n, nlsHPCC, ioQuery,
    registry, TextBox,
    Grid, Keyboard, ColumnResizer, CompoundColumns, DijitRegistry, PaginationModule,
    _Widget, ESPBaseMod, ESPWorkunit, ESPLogicalFile, TableContainer, DGridHeaderHookMod,
    template) {
    return declare("ResultWidget", [_Widget], {
        templateString: template,
        baseClass: "ResultWidget",
        i18n: nlsHPCC,

        borderContainer: null,
        grid: null,

        loaded: false,

        dataPatternsButton: null,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.filter = registry.byId(this.id + "Filter");
            this.grid = registry.byId(this.id + "Grid");
            this.dataPatternsButton = registry.byId(this.id + "DataPatterns");
        },

        startup: function (args) {
            this.inherited(arguments);
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

        _onRefresh: function () {
            this.refresh(true);
        },

        _doDownload: function (type) {
            var base = new ESPBaseMod.ESPBase();
            if (lang.exists("params.Sequence", this)) {
                window.open(base.getBaseURL() + "/WUResultBin?Format=" + type + "&Wuid=" + this.params.Wuid + "&Sequence=" + this.params.Sequence, "_blank");
            } else if (lang.exists("params.LogicalName", this)) {
                window.open(base.getBaseURL() + "/WUResultBin?Format=" + type + "&LogicalName=" + this.params.LogicalName, "_blank");
            }
        },

        _onDownloadZip: function (args) {
            this._doDownload("zip");
        },

        _onDownloadGZip: function (args) {
            this._doDownload("gzip");
        },

        _onDownloadXLS: function (args) {
            this._doDownload("xls");
        },

        _onDownloadCSV: function (args) {
            this._doDownload("csv");
        },

        _onFileDetails: function (args) {
            alert("todo");
        },

        _onDataPatterns: function (args) {
            var context = this;
            if (this._logicalFile) {
                var wuPromise = this.dataPatternsButton.get("checked") ? this._logicalFile.fetchDataPatternsWU() : Promise.resolve(null);
                wuPromise.then(function (wu) {
                    return context.gridDPHook.render(wu);
                }).then(function () {
                    context.grid.resize();
                });
            }
        },

        //  Implementation  ---
        onErrorClick: function (line, col) {
        },

        init: function (params) {
            this.__filter = params.__filter;
            if (this.inherited(arguments))
                return;

            this.result = params.result;
            //TODO:  Encapsulate this IF into ESPResult.js
            var context = this;
            if (params.result && params.result.canShowResults()) {
                this.initResult(params.result);
            } else if (params.Wuid && (lang.exists("Sequence", params) || params.Name)) {
                var wu = ESPWorkunit.Get(params.Wuid);
                wu.fetchSequenceResults(function (results) {
                    if (lang.exists("Sequence", params)) {
                        context.initResult(results[params.Sequence]);
                    } else {
                        context.initResult(wu.namedResults[params.Name]);
                    }
                });
            } else if (params.LogicalName) {
                this._logicalFile = ESPLogicalFile.Get(params.NodeGroup, params.LogicalName);
                this._logicalFile.getInfo({
                    onAfterSend: function (response) {
                        context.initResult(context._logicalFile.result);
                    }
                });
            } else if (params.result && params.result.Name) {
                this._logicalFile = ESPLogicalFile.Get(params.result.NodeGroup, params.result.Name);
                this._logicalFile.getInfo({
                    onAfterSend: function (response) {
                        context.initResult(context.logicalFile.result);
                    }
                });
            } else {
                this.initResult(null);
            }
            if (!this._logicalFile) {
                registry.byId(this.id + "DataPatterns").destroyRecursive();
                registry.byId(this.id + "DataPatternsSep").destroyRecursive();
            }
            this.refreshDataPatterns();
        },

        initResult: function (result) {
            if (result) {
                var context = this;
                result.fetchStructure(function (structure) {
                    var origTableContainer = registry.byId(context.filter.id + "TableContainer");
                    var tableContainer = new TableContainer({
                    });
                    var filterObj = {};
                    if (lang.exists("__filter", context) && lang.exists("filter.toObject", context)) {
                        filterObj = ioQuery.queryToObject(context.__filter);
                    }
                    arrayUtil.forEach(structure, function (item, idx) {
                        if (item.label !== "##") {
                            var textBox = new TextBox({
                                title: item.label,
                                label: item.label + (item.__hpcc_keyed ? " (i)" : ""),
                                name: item.field,
                                value: filterObj[item.field],
                                colSpan: 2
                            });
                            tableContainer.addChild(textBox);
                        }
                    });
                    tableContainer.placeAt(origTableContainer.domNode, "replace");
                    origTableContainer.destroyRecursive();
                    context.filter.on("clear", function (evt) {
                        context.refreshHRef();
                        context.refresh();
                    });
                    context.filter.on("apply", function (evt) {
                        context.refreshHRef();
                        context.grid._currentPage = 0;
                        context.refresh();
                    });
                    context.filter.refreshState();

                    context.grid = new declare([Grid, PaginationModule.Pagination, Keyboard, ColumnResizer, CompoundColumns, DijitRegistry])({
                        columns: structure,
                        rowsPerPage: 50,
                        pagingLinks: 1,
                        pagingTextBox: true,
                        firstLastArrows: true,
                        pageSizeOptions: [25, 50, 100, 1000],
                        store: result.getStore(),
                        query: {
                            FilterBy: context.getFilter()
                        }
                    }, context.id + "Grid");
                    context.grid.startup();
                    context.gridDPHook = new DGridHeaderHookMod.DGridHeaderHook(context.grid, context.id + "Grid");
                    context.grid.on("dgrid-columnresize", function (evt) {
                        setTimeout(function () {
                            context.gridDPHook.resize(evt.columnId);
                        }, 20);
                    });

                });
            } else {
                this.grid = new declare([Grid, DijitRegistry])({
                    columns: [
                        {
                            label: "##",
                            width: 54
                        }
                    ]
                }, this.id + "Grid");
                this.grid.set("noDataMessage", "<span class='dojoxGridNoData'>[" + this.i18n.undefined + "]</span>");
                this.grid.startup();
            }
        },

        getFilter: function () {
            return this.filter.toObject();
        },

        refresh: function (bypassCachedResult) {
            bypassCachedResult = bypassCachedResult || false;
            if (this.result && !this.result.isComplete()) {
                this.grid.showMessage(this.result.getLoadingMessage());
            } else if (this.loaded !== this.getFilter() || bypassCachedResult) {
                this.loaded = this.getFilter();
                this.grid.set("query", {
                    FilterBy: this.getFilter(),
                    BypassCachedResult: bypassCachedResult
                });
            }
            this.refreshDataPatterns();
        },

        _wu: null, //  Null needed for initial test  ---
        refreshDataPatterns: function () {
            if (this._logicalFile) {
                var context = this;
                this._logicalFile.fetchDataPatternsWU().then(function (wu) {
                    if (context._wu !== wu) {
                        context._wu = wu;
                        if (context._wu) {
                            context._wu.watchUntilComplete(function (changes) {
                                context.refreshActionState();
                            });
                        } else {
                            context.refreshActionState();
                        }
                    }
                });
            }
        },

        refreshActionState: function () {
            if (this._logicalFile) {
                var isComplete = this._wu && this._wu.isComplete();
                this.setDisabled(this.id + "DataPatterns", !isComplete);
            }
        }
    });
});
