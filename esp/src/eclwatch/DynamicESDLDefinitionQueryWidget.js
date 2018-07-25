define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",

    "dijit/registry",
    "dijit/form/CheckBox",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/WsESDLConfig",
    "src/ESPUtil",
    "hpcc/DynamicESDLDefinitionDetailsWidget",
    "src/Utility"

], function (declare, lang, i18n, nlsHPCC, arrayUtil,
    registry, Checkbox,
    selector,
    GridDetailsWidget, WsESDLConfig, ESPUtil, DynamicESDLDefinitionDetailsWidget, Utility) {
    return declare("DynamicESDLWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_DefinitionExplorer,
        idProperty: "Name",

        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;

            this._refreshActionState();
            this.refreshGrid();
        },

        postCreate: function (args) {
            var context = this;
            this.inherited(arguments);
            this.definitionWidget = new DynamicESDLDefinitionDetailsWidget({
                id: this.id + "_DefinitionDetails",
                region: "right",
                splitter: true,
                style: "width: 60%",
                minSize: 240
            });
            this.definitionWidget.placeAt(this.gridTab, "last");
            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                selectionMode: "single",
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox',
                        unhidable: true
                    }),
                    Name: {
                        label: this.i18n.Name,
                        sortable: true,
                        width: 200,
                        unhidable: true
                    },
                    PublishBy: {
                        label: this.i18n.PublishedBy,
                        sortable: false,
                        width: 200
                    },
                    CreatedTime: {
                        label: this.i18n.CreatedTime,
                        sortable: false,
                        width: 200
                    },
                    LastEditBy: {
                        label: this.i18n.LastEditedBy,
                        sortable: false,
                        width: 200,
                        hidden: true

                    },
                    LastEditTime: {
                        label: this.i18n.LastEditTime,
                        sortable: false,
                        width: 200,
                        hidden: true
                    }
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
                            Name: item.Id,
                            PublishBy: item.History.PublishBy,
                            CreatedTime: item.History.CreatedTime,
                            LastEditBy: item.History.LastEditBy,
                            LastEditTime: item.History.LastEditTime
                        }
                        results.push(Def);
                    });
                    Utility.alphanumSort(results, "Name");
                }
                context.store.setData(results);
                context.grid.refresh();
                if (context.params.firstLoad && results.length) {
                    var firstRowSelection = context.store.query({
                        Id: results[0].Id
                    });
                    if (firstRowSelection.length) {
                        context.grid.select({
                            Name: firstRowSelection[0].Name
                        });
                    }
                }
            });
        }
    });
});