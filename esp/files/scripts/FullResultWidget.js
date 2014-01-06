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
    "dojo/i18n!./nls/common",
    "dojo/i18n!./nls/FullResultWidget",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/request/iframe",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/_Widget",
    "hpcc/ESPBase",
    "hpcc/ESPWorkunit",
    "hpcc/ESPLogicalFile",
    "hpcc/ESPUtil",

    "dojo/text!../templates/FullResultWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Button",
    "dijit/ToolbarSeparator"
], function (declare, lang, i18n, nlsCommon, nlsSpecific, arrayUtil, dom, iframe, Memory, Observable,
                registry,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                _Widget, ESPBase, ESPWorkunit, ESPLogicalFile, ESPUtil,
                template) {
    return declare("FullResultWidget", [_Widget], {
        templateString: template,
        baseClass: "FullResultWidget",
        i18n: lang.mixin(nlsCommon, nlsSpecific),

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
            if (this.inherited(arguments))
                return;

            if (params.FullResult) {
                this.initResult(params.FullResult);
            } else {
                this.initResult(null);
            }
        },

        initResult: function (result) {
            if (result && result.length) {
                var columns = [];
                for (var key in result[0]) {
                    if (key.indexOf("__") != 0) {
                        columns.push({
                            field: key,
                            label: key
                        });
                    }
                }
                arrayUtil.forEach(result, function (item, idx) {
                    item["__hpcc_id"] = idx;
                });
                var store = new Memory({
                    idProperty: "__hpcc_id",
                    data: result
                });
                this.store = Observable(store);
                this.grid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                    columns: columns,
                    store: this.store
                }, this.id + "Grid");
                this.grid.startup();
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
