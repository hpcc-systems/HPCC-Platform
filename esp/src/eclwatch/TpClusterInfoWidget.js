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

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/tree",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/WsTopology",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsHPCC,
                OnDemandGrid, Keyboard, Selection, selector, tree, ColumnResizer, DijitRegistry,
                GridDetailsWidget, WsTopology, DelayLoadWidget, ESPUtil) {
    return declare("TpClusterInfoWidget", [GridDetailsWidget], {

        i18n: nlsHPCC,
        gridTitle: nlsHPCC.title_ClusterInfo,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;

            var retVal = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox',
                        sortable: false
                    }),
                    Name: { label: this.i18n.Name, width: 180, sortable: true },
                    WorkUnit: { label: this.i18n.WUID, sortable: true }
                }
            }, domID);
            return retVal;
        },

        createDetail: function (id, row, params) {
            return new DelayLoadWidget({
                id: id,
                title: row.Name,
                closable: true,
                delayWidget: "TpThorStatusWidget",
                hpcc: {
                    params: {
                        ClusterName: this.params.ClusterName,
                        Name: row.Name
                    }
                }
            });
        },

        refreshGrid: function () {
            var context = this;
            WsTopology.TpClusterInfo({
                request: {
                    Name: this.params.ClusterName
                }
            }).then(function (response) {
                var results = [];
                if (lang.exists("TpClusterInfoResponse.TpQueues.TpQueue", response)) {
                    results = response.TpClusterInfoResponse.TpQueues.TpQueue;
                }
                context.store.setData(results);
                context.grid.refresh();
            });
        }
    });
});
