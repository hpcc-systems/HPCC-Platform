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

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsHPCC, arrayUtil,
                OnDemandGrid, Keyboard, ColumnResizer, DijitRegistry,
                GridDetailsWidget, ESPWorkunit, DelayLoadWidget, ESPUtil) {
    return declare("VariablesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.Variables,
        idProperty: "__hpcc_id",

        wu: null,

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
            var retVal = new declare([OnDemandGrid, Keyboard, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: this.store,
                columns: {
                    Type: { label: this.i18n.Type, width: 180 },
                    Name: { label: this.i18n.Name, width: 360 },
                    Value: { label: this.i18n.Value }
                }
            }, domID);
            return retVal;
        },

        refreshGrid: function (args) {
            var context = this;
            var variables = [];
            this.wu.getInfo({
                onGetVariables: function (vars) {
                    arrayUtil.forEach(vars, function (item, idx) {
                        variables.push(lang.mixin({
                            Type: context.i18n.ECL
                        }, item));
                    })
                },
                onGetApplicationValues: function (values) {
                    arrayUtil.forEach(values, function (item, idx) {
                        variables.push(lang.mixin({
                            Type: item.Application
                        }, item));
                    })
                },
                onGetDebugValues: function (values) {
                    arrayUtil.forEach(values, function (item, idx) {
                        variables.push(lang.mixin({
                            Type: context.i18n.Debug
                        }, item));
                    })
                },
                onAfterSend: function (wu) {
                    context.store.setData(variables);
                    context.grid.refresh();
                }
            });
        }
    });
});
