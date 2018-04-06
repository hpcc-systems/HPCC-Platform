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

    "hpcc/GridDetailsWidget",
    "src/ESPQuery",
    "src/ESPUtil"
], function (declare, lang, i18n, nlsHPCC, arrayUtil,
                GridDetailsWidget, ESPQuery, ESPUtil) {
    return declare("LibrariesUsedWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_LibrariesUsed,
        idProperty: "Name",

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
                    Name: {label: this.i18n.LibrariesUsed}
                }
            }, domID);
            return retVal;
        },

        refreshGrid: function (args) {
            var context = this;
            this.query.refresh().then(function (response) {
                var librariesUsed = [];
                if (lang.exists("LibrariesUsed.Item", context.query)) {
                    arrayUtil.forEach(context.query.LibrariesUsed.Item, function (item, idx) {
                        var file = {
                            Name: item
                        }
                        librariesUsed.push(file);
                    });
                }
                context.store.setData(librariesUsed);
                context.grid.refresh();
            });
        }
    });
});
