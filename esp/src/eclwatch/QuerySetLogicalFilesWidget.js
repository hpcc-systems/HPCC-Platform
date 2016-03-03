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

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPQuery",
    "hpcc/ESPUtil"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, on,
                selector,
                GridDetailsWidget, DelayLoadWidget, ESPQuery, ESPUtil) {
    return declare("QuerySetLogicalFilesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_QuerySetLogicalFiles,
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
                    File: {label: this.i18n.LogicalFiles}
                }
            }, domID);
            return retVal;
        },

        createDetail: function (id, row, params) {
            return new DelayLoadWidget({
                id: id,
                title: row.File,
                closable: true,
                delayWidget: "LFDetailsWidget",
                hpcc: {
                    type: "LFDetailsWidget",
                    params: {
                        Name: row.File
                    }
                }
            });
        },

        refreshGrid: function (args) {
            var context = this;
            this.query.refresh().then(function (response) {
                var logicalFiles = [];
                if (lang.exists("SuperFiles.SuperFile", context.query)) {
                    arrayUtil.forEach(context.query.SuperFiles.SuperFile, function (item, idx) {
                        arrayUtil.forEach(item.SubFiles.File, function (item, idx) {
                            var file = {
                                File: item
                            }
                            logicalFiles.push(file);
                        });
                    });
                }
                context.store.setData(logicalFiles);
                context.grid.refresh();
            });
        }
    });
});
