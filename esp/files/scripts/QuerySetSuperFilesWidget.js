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

    "dijit/form/Button",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPWorkunit",
    "hpcc/ESPQuery",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on,
                Button,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, ESPWorkunit, ESPQuery, ESPUtil) {
    return declare("QuerySetSuperFilesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_QuerySetLogicalFiles,
        idProperty: "Name",

        wu: null,
        query: null,


        init: function (params) {
           if (this.inherited(arguments))
                return;

            this._refreshActionState();
        },

         createGrid: function (domID) {
            var context = this;
            var retVal = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: ESPQuery.CreateQueryStore(),
                columns: {
                    col1: selector({ width: 27, selectorType: 'checkbox' }),
                    /*Item: {
                        label: "File", width: 180, sortable: true,
                        formatter: function (Wuid, row) {
                            var wu = row.Server === "DFUserver" ? ESPDFUWorkunit.Get(Wuid) : ESPWorkunit.Get(Wuid);
                            return "<img src='" + wu.getStateImage() + "'>&nbsp;<a href='#' class='" + context.id + "WuidClick'>" + Wuid + "</a>";
                        }

                    },*/
                    LogicalFiles: { label: this.i18n.LogicalFiles, width: 108, sortable: false },
                    /*State: {
                        label: "State", width: 180, sortable: true, formatter: function (state, row) {
                            return state + (row.Duration ? " (" + row.Duration + ")" : "");
                        }
                    },*/
                    /*Owner: { label: "Owner", width: 90, sortable: true },
                    Jobname: { label: "Job Name", sortable: true }*/
                }
            }, domID);

            var context = this;
            on(document, "." + this.id + "WuidClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        createDetail: function (id, row, params) {
            if (row.Server === "DFUserver") {
                return new DFUWUDetailsWidget.fixCircularDependency({
                    id: id,
                    title: row.ID,
                    closable: true,
                    hpcc: {
                        params: {
                            Wuid: row.ID
                        }
                    }
                });
            } 
            return new WUDetailsWidget({
                id: id,
                title: row.Wuid,
                closable: true,
                hpcc: {
                    params: {
                        Wuid: row.Wuid
                    }
                }
            });
        },

        

        refreshGrid: function (args) {
            var context = this;
            this.wu.getInfo({
                onGetTimers: function (timers) {
                    //  Required to calculate Graphs Total Time  ---
                },
                onGetGraphs: function (graphs) {
                    context.store.setData(graphs);
                    context.grid.refresh();
                }
            });
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);

            this.openSafeMode.set("disabled", !selection.length);
        }
    });
});
