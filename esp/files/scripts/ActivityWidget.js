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
    "dojo/i18n!./nls/ActivityWidget",
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
    "hpcc/ESPDFUWorkunit",
    "hpcc/WsSMC",
    "hpcc/WUDetailsWidget",
    "hpcc/DFUWUDetailsWidget",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsCommon, nlsSpecific, arrayUtil, on,
                Button,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, ESPWorkunit, ESPDFUWorkunit, WsSMC, WUDetailsWidget, DFUWUDetailsWidget, ESPUtil) {
    return declare("ActivityWidget", [GridDetailsWidget], {

        i18n: lang.mixin(nlsCommon, nlsSpecific),
        gridTitle: nlsSpecific.title,
        idProperty: "Wuid",

        doSearch: function (searchText) {
            this.searchText = searchText;
            this.selectChild(this.gridTab);
            this.refreshGrid();
        },

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
                store: WsSMC.CreateActivityStore(),
                columns: {
                    col1: selector({ width: 27, selectorType: 'checkbox' }),
                    Wuid: {
                        label: this.i18n.ActiveWorkunit, width: 180, sortable: true,
                        formatter: function (Wuid, row) {
                            var wu = row.Server === "DFUserver" ? ESPDFUWorkunit.Get(Wuid) : ESPWorkunit.Get(Wuid);
                            return "<img src='../files/" + wu.getStateImage() + "'>&nbsp;<a href='#' class='" + context.id + "WuidClick'>" + Wuid + "</a>";
                        }

                    },
                    ClusterName: { label: this.i18n.Target, width: 108, sortable: true },
                    State: {
                        label: this.i18n.State, width: 180, sortable: true, formatter: function (state, row) {
                            return state + (row.Duration ? " (" + row.Duration + ")" : "");
                        }
                    },
                    Owner: { label: this.i18n.Owner, width: 90, sortable: true },
                    Jobname: { label: this.i18n.JobName, sortable: true }
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

        loadRunning: function (response) {
            var items = lang.getObject("ActivityResponse.Running", false, response)
            if (items) {
                var context = this;
                arrayUtil.forEach(items, function (item, idx) {
                    context.store.add({
                        id: "ActivityRunning" + idx,
                        ClusterName: item.ClusterName,
                        Wuid: item.Wuid,
                        Owner: item.Owner,
                        Jobname: item.Owner,
                        Summary: item.Name + " (" + prefix + ")",
                        _type: "LogicalFile",
                        _name: item.Name
                    });
                });
                return items.length;
            }
            return 0;
        },

        refreshGrid: function (args) {
            var context = this;
            this.grid.set("query", {
            });
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);
        }
    });
});
