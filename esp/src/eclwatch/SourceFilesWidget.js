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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/on",

    "dgrid/tree",
    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil",
    "src/Utility",

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on,
                tree, selector,
                GridDetailsWidget, ESPWorkunit, DelayLoadWidget, ESPUtil, Utility) {
    return declare("SourceFilesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_Inputs,
        idProperty: "sequence",

        wu: null,

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 === 0) {
                        context.refreshGrid();
                    }
                });
            }
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var context = this;
            this.store.mayHaveChildren = function (item) {
                return item.IsSuperFile;
            };
            this.store.getChildren = function (parent, options) {
                return context.store.query({__hpcc_parentName: parent.Name});
            };
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    Name: tree({
                        label: "Name", sortable: true,
                        formatter: function (Name, row) {
                            return Utility.getImageHTML(row.IsSuperFile ? "folder_table.png" : "file.png") + "&nbsp;<a href='#' class='dgrid-row-url'>" + Name + "</a>";
                        }
                    }),
                    FileCluster: { label: this.i18n.FileCluster, width: 300, sortable: false },
                    Count: { label: this.i18n.Usage, width: 72, sortable: true }
                }
            }, domID);

            retVal.on(".dgrid-row-url:click", function (evt) {
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
                return new DelayLoadWidget({
                    id : id,
                    title: row.Name,
                    closable: true,
                    delayWidget: "SFDetailsWidget",
                    hpcc: {
                        type: "SFDetailsWidget",
                        params: {
                            Name: row.Name
                        }
                    }
                });
            } else {
                return new DelayLoadWidget({
                    id: id,
                    title: row.Name,
                    closable: true,
                    delayWidget: "LFDetailsWidget",
                    hpcc: {
                        type: "LFDetailsWidget",
                        params: {
                            NodeGroup: row.FileCluster,
                            Name: row.Name
                        }
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
                    context.grid.set("query", { __hpcc_parentName: "" });
                }
            });
        }

    });
});
