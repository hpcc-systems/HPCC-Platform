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

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPQuery",
    "hpcc/ESPUtil"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, on,
                selector,
                GridDetailsWidget, ESPQuery, ESPUtil) {
    return declare("QuerySetErrorsWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_QuerySetErrors,
        idProperty: "Name",

        queryId: null,
        querySet: null,

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.query = ESPQuery.Get(params.QuerySetId, params.Id);

            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    col1: selector({ width: 27, selectorType: 'checkbox' }),
                    Cluster: { label: this.i18n.Cluster, width: 108, sortable: false },
                    Errors: { label: this.i18n.Error, width: 108, sortable: false },
                    State: { label: this.i18n.State, width: 108, sortable: false }
                }
            }, domID);

            on(document, "." + this.id + "WuidClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        refreshGrid: function (args) {
            var context = this;
            this.query.refresh().then(function (response) {
                var errors = [];
                if (lang.exists("query.Clusters.ClusterQueryState", context)) {
                    arrayUtil.forEach(context.query.Clusters.ClusterQueryState, function (item, idx) {
                        var error = {
                            Cluster: item.Cluster,
                            Errors: item.Errors,
                            State: item.State
                        }
                        errors.push(error);
                    });
                }
                context.store.setData(errors);
                context.grid.refresh();
            });
        }
    });
});
