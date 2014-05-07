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

    "dijit/registry",

    "dgrid/Grid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/_Widget",
    "hpcc/ESPBase",
    "hpcc/ESPWorkunit",
    "hpcc/ESPLogicalFile",

    "dojo/text!../templates/ResultWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/layout/StackController",
    "dijit/layout/StackContainer",
    "dijit/form/Button",
    "dijit/form/NumberSpinner",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",

    "hpcc/DelayLoadWidget"

], function (declare, lang, i18n, nlsHPCC, dom,
                registry,
                Grid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _Widget, ESPBase, ESPWorkunit, ESPLogicalFile,
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
            this.grid = registry.byId(this.id + "Grid");

            var context = this;
            this.widget.TabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                if (nval && !nval.initalized) {
                    if (nval.init) {
                        nval.init(context.params);
                    }
                }
            });
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
            var base = new ESPBase();
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

        _onFileDetails: function (args) {
            alert("todo");
        },

        //  Implementation  ---
        onErrorClick: function (line, col) {
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.result = params.result;
            //TODO:  Encapsulate this IF into ESPResult.js
            if (params.result && params.result.canShowResults()) {
                this.initResult(params.result);
            } else if (params.Wuid && lang.exists("Sequence", params)) {
                var wu = ESPWorkunit.Get(params.Wuid);
                var context = this;
                wu.fetchSequenceResults(function (results) {
                    context.initResult(results[params.Sequence]);
                });
            } else if (params.LogicalName) {
                var logicalFile = ESPLogicalFile.Get(params.ClusterName, params.LogicalName);
                var context = this;
                logicalFile.getInfo({
                    onAfterSend: function (response) {
                        context.initResult(logicalFile.result);
                    }
                });
            } else if (params.result && params.result.Name) {
                var logicalFile = ESPLogicalFile.Get(params.result.ClusterName, params.result.Name);
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
                this.grid.set("noDataMessage", "<span class='dojoxGridNoData'>[" + this.i18n.undefined + "]</span>");
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
