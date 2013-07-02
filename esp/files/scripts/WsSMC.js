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
    "dojo/store/Observable",

    "hpcc/ESPRequest",
    "hpcc/ESPWorkunit",
    "hpcc/ESPDFUWorkunit"
], function (declare, Observable,
    ESPRequest, ESPWorkunit, ESPDFUWorkunit) {

    var Store = declare([ESPRequest.Store], {
        service: "WsSMC",
        action: "Activity",
        responseQualifier: "ActivityResponse.Running.ActiveWorkunit",
        idProperty: "Wuid",

        _watched: [],
        create: function (id, item) {
            if (item.Server === "DFUserver") {
                return ESPDFUWorkunit.Get(id);
            }
            return ESPWorkunit.Get(id);
        },
        update: function (id, item) {
            var storeItem = this.get(id);
            storeItem.updateData(item);
            if (!this._watched[id]) {
                var context = this;
                this._watched[id] = storeItem.watch("changedCount", function (name, oldValue, newValue) {
                    if (context.notify && oldValue !== newValue) {
                        context.notify(storeItem, id);
                    }
                });
            }
        },
        postProcessResults: function (items) {
            return items;
        }
    });

    return {
        CreateActivityStore: function (options) {
            var store = new Store(options);
            return store;//Observable(store);
        },

        Activity: function (params) {
            return ESPRequest.send("WsSMC", "Activity", params);
        },

    };
});

