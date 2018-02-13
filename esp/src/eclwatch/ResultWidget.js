/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/_base/array",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/io-query",
    "dojo/dom",

    "dijit/registry",
    "dijit/form/TextBox",

    "dgrid/Grid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/ColumnHider",
    "dgrid/extensions/CompoundColumns",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/_Widget",
    "src/ESPBase",
    "src/ESPWorkunit",
    "src/ESPLogicalFile",
    "hpcc/FilterDropDownWidget",
    "hpcc/TableContainer",

    "dojo/text!../templates/ResultWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Button",
    "dijit/ToolbarSeparator"
], function (declare, lang, arrayUtil, i18n, nlsHPCC, ioQuery, dom,
                registry, TextBox,
                Grid, Keyboard, Selection, selector, ColumnResizer, ColumnHider, CompoundColumns, DijitRegistry, Pagination,
                _Widget, ESPBase, ESPWorkunit, ESPLogicalFile, FilterDropDownWidget, TableContainer,
                template) {
    return declare("ResultWidget", [_Widget], {
        templateString: template,
        baseClass: "ResultWidget",
        i18n: nlsHPCC,

        borderContainer: null,
        grid: null,

        loaded: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.filter = registry.byId(this.id + "Filter");
            this.grid = registry.byId(this.id + "Grid");
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
            var base = new ESPBase.default();
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
                var logicalFile = ESPLogicalFile.Get(params.NodeGroup, params.LogicalName);
                logicalFile.getInfo({
                    onAfterSend: function (response) {
                        context.initResult(logicalFile.result);
                    }
                });
            } else if (params.result && params.result.Name) {
                var logicalFile = ESPLogicalFile.Get(params.result.NodeGroup, params.result.Name);
                logicalFile.getInfo({
                    onAfterSend: function (response) {
                        context.initResult(logicalFile.result);
                    }
                });
            } else {
                this.initResult(null);
            }
        },

        initResult: function (result) {
            if (result) {
                var context = this;
                result.fetchStructure(function (structure) {
                    var filterForm = registry.byId(context.filter.id + "FilterForm");
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
                        context.refresh();
                    });
                    context.filter.on("apply", function (evt) {
                        context.refresh();
                    });
                    context.filter.refreshState();

                    context.grid = new declare([Grid, Pagination, Keyboard, ColumnResizer, ColumnHider, CompoundColumns, DijitRegistry])({
                        columns: structure,
                        rowsPerPage: 50,
                        pagingLinks: 1,
                        pagingTextBox: true,
                        firstLastArrows: true,
                        pageSizeOptions: [25, 50, 100, 1000],
                        store: result.getStore()
                    }, context.id + "Grid");
                    context.grid.set("query", {
                        FilterBy: context.getFilter()
                    });
                    context.grid.startup();
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
        }
    });
});
