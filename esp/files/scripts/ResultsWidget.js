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
    "dojo/_base/array",
    "dojo/on",

    "dijit/layout/ContentPane",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPRequest",
    "hpcc/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on,
                ContentPane,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, ESPRequest, ESPWorkunit, DelayLoadWidget, ESPUtil) {
    return declare("ResultsWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_Results,
        idProperty: "Sequence",

        wu: null,

        _onRowDblClickFile: function (row) {
            var tab = this.ensurePane(row, {
                logicalFile: true
            });
            this.selectChild(tab);
        },

        _onRowDblClickView: function (row, viewName) {
            var tab = this.ensurePane(row, {
                resultView: true,
                viewName: viewName
            });
            this.selectChild(tab);
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 == 0) {
                        context.refreshGrid();
                    }
                });
            }
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var retVal = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    Name: {
                        label: this.i18n.Name, width: 180, sortable: true,
                        formatter: function (Name, idx) {
                            return "<a href='#' rowIndex=" + idx + " class='" + context.id + "ResultClick'>" + Name + "</a>";
                        }
                    },
                    FileName: {
                        label: this.i18n.FileName, sortable: true,
                        formatter: function (FileName, idx) {
                            return "<a href='#' rowIndex=" + idx + " class='" + context.id + "FileClick'>" + FileName + "</a>";
                        }
                    },
                    Value: {
                        label: this.i18n.Value,
                        width: 360,
                        sortable: true
                    },
                    ResultViews: {
                        label: this.i18n.Views, sortable: true,
                        formatter: function (ResultViews, idx) {
                            var retVal = "";
                            arrayUtil.forEach(ResultViews, function (item, idx) {
                                retVal += "<a href='#' viewName=" + encodeURIComponent(item) + " class='" + context.id + "ViewClick'>" + item + "</a>&nbsp;";
                            });
                            return retVal;
                        }
                    }
                }
            }, domID);

            var context = this;
            on(document, "." + this.id + "ResultClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = context.grid.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            on(document, "." + this.id + "FileClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = context.grid.row(evt).data;
                    context._onRowDblClickFile(row);
                }
            });
            on(document, "." + this.id + "ViewClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = context.grid.row(evt).data;
                    context._onRowDblClickView(row, evt.srcElement.getAttribute("viewName"));
                }
            });
            return retVal;
        },

        getDetailID: function (row, params) {
            if (row.FileName && params && params.logicalFile) {
                return "File" + row[this.idProperty];
            } else if (params && params.resultView && params.viewName) {
                return params.viewName + row[this.idProperty];
            }
            return this.inherited(arguments);
        },

        createDetail: function (id, row, params) {
            if (row.FileName && params && params.logicalFile) {
                return new DelayLoadWidget({
                    id: id,
                    title: "[F] " + row.Name,
                    closable: true,
                    delayWidget: "LFDetailsWidget",
                    hpcc: {
                        type: "LFDetailsWidget",
                        params: {
                            Name: row.FileName
                        }
                    }
                });
            } else if (params && params.resultView && params.viewName) {
                return new ContentPane({
                    id: id,
                    title: row.Name + " [" + decodeURIComponent(params.viewName) + "]",
                    closable: true,
                    content: dojo.create("iframe", {
                        src: ESPRequest.getBaseURL("WsWorkunits") + "/WUResultView?Wuid=" + row.Wuid + "&ResultName=" + row.Name + "&ViewName=" + params.viewName,
                        style: "border: 0; width: 100%; height: 100%"
                    }),
                    hpcc: {
                        type: "ContentPane",
                        params: {
                            Name: row.Name,
                            viewName: params.viewName
                        }
                    },
                    noRefresh: true
                });
            } else {
                return new DelayLoadWidget({
                    id: id,
                    title: row.Name,
                    closable: true,
                    style: "padding: 0px; overflow: hidden",
                    delayWidget: "ResultWidget",
                    hpcc: {
                        type: "ResultWidget",
                        params: {
                            Wuid: row.Wuid,
                            Sequence: row.Sequence
                        }
                    }
                });
            }
        },

        refreshGrid: function (args) {
            var context = this;
            this.wu.getInfo({
                onGetResults: function (results) {
                    context.store.setData(results);
                    context.grid.refresh();
                }
            });
        }

    });
});
