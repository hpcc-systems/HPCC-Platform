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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPLogicalFile",
    "hpcc/ESPUtil"

], function (declare, i18n, nlsHPCC,
                GridDetailsWidget, ESPLogicalFile, ESPUtil) {
    return declare("FileBelongsToWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        logicalFile: null,

        gridTitle: nlsHPCC.SuperFilesBelongsTo,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this.logicalFile = ESPLogicalFile.Get(params.NodeGroup, params.Name);
            this.refreshGrid();
        },

        createGrid: function (domID) {
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    Name: { label: this.i18n.Name }
                }
            }, domID);
            return retVal;
        },

        refreshGrid: function (args) {
            var context = this;
            if (this.logicalFile.Superfiles.DFULogicalFile) {
                context.store.setData(this.logicalFile.Superfiles.DFULogicalFile);
                context.grid.refresh();
            }
        }
    });
});
