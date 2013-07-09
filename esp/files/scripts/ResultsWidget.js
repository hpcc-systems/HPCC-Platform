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
    "dojo/on",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPWorkunit",
    "hpcc/ResultWidget",
    "hpcc/LFDetailsWidget",
    "hpcc/SFDetailsWidget",
    "hpcc/ESPUtil"

], function (declare, lang, on,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, ESPWorkunit, ResultWidget, LFDetailsWidget, SFDetailsWidget, ESPUtil) {
    return declare("ResultsWidget", [GridDetailsWidget], {
        gridTitle: "Outputs",
        idProperty: "Sequence",

        wu: null,

        _onRowDblClickFile: function (row) {
            var tab = this.ensurePane(row, {
                logicalFile: true
            });
            this.selectChild(tab);
        },

        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;

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
                        label: "Name", width: 180, sortable: true,
                        formatter: function (Name, idx) {
                            return "<a href='#' rowIndex=" + idx + " class='" + context.id + "ResultClick'>" + Name + "</a>";
                        }
                    },
                    FileName: {
                        label: "FileName", sortable: true,
                        formatter: function (FileName, idx) {
                            return "<a href='#' rowIndex=" + idx + " class='" + context.id + "FileClick'>" + FileName + "</a>";
                        }
                    },
                    Value: { label: "Value", width: 360, sortable: true }
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
            return retVal;
        },

        getDetailID: function (row, params) {
            if (row.FileName && params && params.logicalFile) {
                return this.id + "_" + "File" + row[this.idProperty];
            }
            return this.inherited(arguments);
        },

        createDetail: function (id, row, params) {
            if (row.FileName && params && params.logicalFile) {
                return new LFDetailsWidget.fixCircularDependency({
                    id: id,
                    title: "[F] " + row.Name,
                    closable: true,
                    hpcc: {
                        type: "LFDetailsWidget",
                        params: {
                            Name: row.FileName
                        }
                    }
                });
            } else {
                return new ResultWidget({
                    id: id,
                    title: row.Name,
                    closable: true,
                    style: "padding: 0px; overflow: hidden",
                    hpcc: {
                        type: "ResultWidget",
                        params: row
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
