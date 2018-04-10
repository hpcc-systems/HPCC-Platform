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
    "dojo/dom-class",
    "dojo/topic",

    "dijit/registry",
    "dijit/form/ToggleButton",
    "dijit/ToolbarSeparator",

    "dgrid/tree",
    "dgrid/extensions/ColumnHider",

    "hpcc/GridDetailsWidget",
    "hpcc/ws_machine",
    "hpcc/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil",
    "hpcc/Utility",
    "hpcc/WsTopology",
    "hpcc/ESPQuery",

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, domClass, topic,
                registry, ToggleButton, ToolbarSeparator,
                tree, ColumnHider,
    GridDetailsWidget, WsMachine, ESPWorkunit, DelayLoadWidget, ESPUtil, Utility, WsTopology, ESPQuery) {
    return declare("FilesInUseWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.FilesInUse,
        idProperty: "__hpcc_id",

        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;
            this._refreshActionState();
            this.refreshGrid();
        },

        createGrid: function (domID) {
            this.FilesInUseStore = new ESPQuery.CreateFilesInUseStore();
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.FilesInUseStore,
                columns: {
                    displayName: tree({
                        formatter: function (_name, row) {
                            var img = "";
                            var name = _name;
                            if (row.type === "cluster") {
                                img = Utility.getImageHTML("server.png");
                            } else if (row.type === "file") {
                                img = Utility.getImageHTML("file.png");
                            } else if (row.type === "query") {
                                img = Utility.getImageHTML("folder.png");
                            } else {
                                img = Utility.getImageHTML("file.png");
                            }
                            return img + "&nbsp;" + name;
                        },
                        label: this.i18n.Cluster, sortable: true, width:100
                    })
                }
            }, domID);

            return retVal;
        },

        refreshGrid: function () {
            this.grid.set("query", { ClusterType: "RoxieCluster" });
        }
    });
});
