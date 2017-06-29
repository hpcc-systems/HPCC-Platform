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
    "hpcc/WsESDLConfig",
    "hpcc/ESPUtil",
    "hpcc/DynamicESDLDefinitionDetailsWidget"

], function (declare, lang, i18n, nlsHPCC, arrayUtil,
                GridDetailsWidget, WsESDLConfig, ESPUtil, DynamicESDLDefinitionDetailsWidget) {
    return declare("DynamicESDLWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_DefinitionExplorer,
        idProperty: "Name",

        init: function (params) {
            var context = this;
            if (this.inherited(arguments))

            this._refreshActionState();
            this.refreshGrid();
            this.initTab();
        },

        postCreate: function (args) {
            var context = this;
            this.inherited(arguments);
            this.definitionWidget = new DynamicESDLDefinitionDetailsWidget({
                id: this.id + "_DefinitionDetails",
                region: "right",
                splitter: true,
                style: "width: 80%",
                minSize: 240
            });
            this.definitionWidget.placeAt(this.gridTab, "last");
            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    Name: {label: this.i18n.Name, sortable: true, width:200}
                }
            }, domID);

            retVal.on("dgrid-select", function (evt) {
                var selection = context.grid.getSelected();
                if (selection) {
                    context.definitionWidget.init({
                        Id: selection[0].Name
                    });
                }
            });
            return retVal;
        },

        _onRefresh: function () {
            this.refreshGrid();
        },

         refreshGrid: function (args) {
            var context = this;
            WsESDLConfig.ListESDLDefinitions({
                request: {}
            }).then(function (response) {
                var results = [];
                if (lang.exists("ListESDLDefinitionsResponse.Definitions.Definition", response)) {
                    arrayUtil.forEach(response.ListESDLDefinitionsResponse.Definitions.Definition, function (item, idx) {
                        var Def = {
                            Id: idx,
                            Name: item.Id
                        }
                        results.push(Def);
                    });
                }
                context.store.setData(results);
                context.grid.refresh();
                var firstRowSelection = context.store.query({Id: 0});
                context.grid.select({Name:firstRowSelection[0].Name});
            });
        }
    });
});
