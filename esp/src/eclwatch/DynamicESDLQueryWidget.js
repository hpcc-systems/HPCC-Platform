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

    "dijit/registry",

    "dgrid/tree",
    "dgrid/extensions/ColumnHider",

    "hpcc/GridDetailsWidget",
    "hpcc/WsTopology",
    "hpcc/WsESDLConfig",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil",
    "hpcc/DynamicESDLDetailsWidget",
    "hpcc/TargetSelectWidget"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, domClass,
                registry,
                tree, ColumnHider,
                GridDetailsWidget, WsTopology, WsESDLConfig, DelayLoadWidget, ESPUtil, DynamicESDLDetailsWidget, TargetSelectWidget) {
    return declare("DynamicESDLWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_DESDL,
        idProperty: "__hpcc_id",

        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;

            this._refreshActionState();
            this.refreshGrid();
            this.detailsWidget.widget._Binding.set("disabled", true);
        },

        postCreate: function (args) {
            var context = this;
            this.inherited(arguments);
            this.detailsWidget = new DynamicESDLDetailsWidget({
                id: this.id + "Details",
                region: "right",
                splitter: true,
                style: "width: 80%",
                minSize: 240
            });
            this.detailsWidget.placeAt(this.gridTab, "last");
        },

        createGrid: function (domID) {
            var context = this;

           this.store.mayHaveChildren = function (item) {
                if (item.__hpcc_parentName) {
                    return false;
                }
                return true;
            };

            this.store.getChildren = function (parent, options) {
               return this.query({__hpcc_parentName: parent.__hpcc_id}, options);
            };
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    Name: tree({
                        collapseOnRefresh: false, label: this.i18n.Name, sortable: true, width:200,
                    })
                }
            }, domID);

            retVal.on("dgrid-select", function (event) {
                var selection = context.grid.getSelected();
                if (selection.length) {
                    lang.mixin(selection[0],{
                        Owner: context
                    });
                    context.detailsWidget.init(selection[0]);
                }
            });
            return retVal;
        },

        _onRefresh: function () {
            this.refreshGrid();
        },

        refreshGrid: function () {
            var context = this;

            WsESDLConfig.ListDESDLEspBindings({
                request: {
                    IncludeESDLBindingInfo: true
                }
            }).then(function (response) {
                var results = [];
                var newRows = [];
                if (lang.exists("ListDESDLEspBindingsResp.ESPServers.ESPServer", response)) {
                    results = response.ListDESDLEspBindingsResp.ESPServers.ESPServer;
                }
                arrayUtil.forEach(results, function (row, idx) {
                    lang.mixin(row, {
                        __hpcc_parentName: null,
                        __hpcc_id: row.Name
                    });

                    arrayUtil.forEach(row.TpBindingEx.TpBindingEx, function (TpBinding, idx) {
                        newRows.push({
                            __hpcc_parentName: row.Name,
                            __hpcc_id: row.Name + idx,
                            __binding_info: TpBinding.ESDLBinding,
                            Name: TpBinding.Name,
                            Port: TpBinding.Port,
                            DefinitionID: TpBinding.ESDLBinding.Definition.Id,
                            Service: TpBinding.ESDLBinding.Definition.Name,
                            Protocol: TpBinding.Protocol
                        });
                    });
                });

                arrayUtil.forEach(newRows, function (newRow) {
                    results.push(newRow);
                });

                context.store.setData(results);
                context.grid.set("query", {__hpcc_parentName: null });
            });
        }
    });
});
