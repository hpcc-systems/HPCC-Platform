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
    "dojo/request/iframe",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "dgrid/Grid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/ESPBase",
    "hpcc/ESPWorkunit",
    "hpcc/ESPLogicalFile",

    "dojo/text!../templates/ResultWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/Toolbar",
    "dijit/form/Button",
    "dijit/ToolbarSeparator"
], function (declare, lang, dom, iframe,
                _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                ESPBase, ESPWorkunit, ESPLogicalFile,
                template) {
    return declare("ResultWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "ResultWidget",

        borderContainer: null,
        grid: null,

        initalized: false,
        loaded: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
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

        _doDownload: function (type) {
            //TODO Fix
            var base = new ESPBase();
            if (lang.exists("result.Sequence", this)) {
                var sequence = this.result.Sequence;
                var downloadPdfIframeName = "downloadIframe_" + sequence;
                var frame = iframe.create(downloadPdfIframeName);
                var url = base.getBaseURL() + "/WUResultBin?Format=" + type + "&Wuid=" + this.result.Wuid + "&Sequence=" + sequence;
                iframe.setSrc(frame, url, true);
            } else if (lang.exists("result.Name", this)) {
                var logicalName = this.result.Name;
                var downloadPdfIframeName = "downloadIframe_" + logicalName;
                var frame = iframe.create(downloadPdfIframeName);
                var url = base.getBaseURL() + "/WUResultBin?Format=" + type + "&Wuid=" + this.result.Wuid + "&LogicalName=" + logicalName;
                iframe.setSrc(frame, url, true);
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

        _onFileDetails: function (args) {
            alert("todo");
        },

        //  Implementation  ---
        onErrorClick: function (line, col) {
        },

        init: function (params) {
            if (this.initalized) {
                return;
            }
            this.initalized = true;

            this.result = params.result;
            //TODO:  Encapsulate this IF into ESPResult.js
            if (params.result && params.result.canShowResults()) {
                this.initResult(params.result);
            } else if (params.Wuid && params.Sequence) {
                var wu = ESPWorkunit.Get(params.Wuid);
                var context = this;
                wu.fetchResults(function (results) {
                    context.initResult(results[params.Sequence]);
                });
            } else if (params.LogicalName) {
                var logicalFile = ESPLogicalFile.Get(params.LogicalName);
                var context = this;
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
                    context.grid = new declare([Grid, Pagination, Keyboard, ColumnResizer, DijitRegistry])({
                        columns: structure,
                        rowsPerPage: 50,
                        pagingLinks: 1,
                        pagingTextBox: true,
                        firstLastArrows: true,
                        pageSizeOptions: [25, 50, 100],
                        store: result.getStore()
                    }, context.id + "Grid");
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
                this.grid.set("noDataMessage", "[undefined]");
                this.grid.startup();
            }
        },

        refresh: function () {
            if (this.result && !this.result.isComplete()) {
                this.grid.showMessage(this.result.getLoadingMessage());
            } else if (!this.loaded) {
                this.loaded = true;
                this.grid.set("query", {
                    id: "*"
                });
            }
        }
    });
});
