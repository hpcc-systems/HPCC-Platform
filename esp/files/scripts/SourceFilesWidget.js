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
    "dojo/_base/array",
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

], function (declare, lang, arrayUtil, on,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, ESPWorkunit, ResultWidget, LFDetailsWidget, SFDetailsWidget, ESPUtil) {
    return declare("SourceFilesWidget", [GridDetailsWidget], {

        gridTitle: "Inputs",
        idProperty: "sequence",

        wu: null,

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
                        label: "Name", sortable: true,
                        formatter: function (Name, row) {
                            return "<img src='../files/img/" + (row.IsSuperFile ? "folder_table.png" : "file.png") + "'>&nbsp;<a href='#' rowIndex=" + row + " class='" + context.id + "SourceFileClick'>" + Name + "</a>";
                        }
                    },
                    Count: { label: "Usage", width: 72, sortable: true }
                }
            }, domID);

            var context = this;
            on(document, "." + this.id + "SourceFileClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = context.grid.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        getDetailTitle: function (row, params) {
            return row.Name;
        },

        createDetail: function (id, row) {
            if (lang.exists("IsSuperFile", row) && row.IsSuperFile) {
                return new SFDetailsWidget.fixCircularDependency({
                    id : id,
                    title: row.Name,
                    closable: true,
                    hpcc: {
                        type: "SFDetailsWidget",
                        params: row
                    }
                });
            } else {
                return new LFDetailsWidget.fixCircularDependency({
                    id: id,
                    title: row.Name,
                    closable: true,
                    hpcc: {
                        type: "LFDetailsWidget",
                        params: row
                    }
                });
            }
        },

        refreshGrid: function (args) {
            var context = this;
            this.wu.getInfo({
                onGetSourceFiles: function (sourceFiles) {
                    arrayUtil.forEach(sourceFiles, function (row, idx) {
                        row.sequence = idx;
                    });
                    context.store.setData(sourceFiles);
                    context.grid.refresh();
                }
            });
        }

    });
});
