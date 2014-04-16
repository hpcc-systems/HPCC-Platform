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

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/tree",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/SFDetailsWidget",
    "hpcc/ESPUtil",
    "hpcc/ESPWorkunit"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, on,
                OnDemandGrid, Keyboard, Selection, tree, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, SFDetailsWidget, ESPUtil, ESPWorkunit) {
    return declare("WorksflowsWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        wu: null,

        gridTitle: nlsHPCC.title_Workflows,
        idProperty: "wuid",

        init: function (params) {
            this.wu = ESPWorkunit.Get(params.Wuid);
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
                    }),
                    EventName: { label: this.i18n.EventName, width: 117 },
                    EventText: { label: this.i18n.EventText, width: 117 },
                    Count: { label: this.i18n.Count, width: 117 },
                    CountRemaining: { label: this.i18n.CountRemaining, width: 117 }
                }
            }, domID);
            return retVal;
        },

        refreshGrid: function (args) {
            if (this.wu) {
                var workflows = [];
                if (lang.exists("ECLWorkflow", this.wu.Workflows)) {
                    arrayUtil.forEach(this.wu.Workflows.ECLWorkflow, function (item, idx) {
                        if(item.Count == -1){
                            item.Count = 0;
                        }
                        if(item.CountRemaining == -1){
                            item.CountRemaining = 0;
                        }
                        workflows.push(lang.mixin({
                            EventName: item.EventName,
                            EventText:  item.EventText,
                            Count: item.Count,
                            CountRemaining: item.CountRemaining
                        }, item));
                    });
                }
            this.store.setData(workflows);
            this.grid.refresh();
            }
        }
    });
});