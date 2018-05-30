define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom-construct",

    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",
    "dijit/Dialog",
    "dijit/form/CheckBox",
    "dijit/form/Button",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/WsESDLConfig",
    "src/ESPUtil",
    "hpcc/DynamicESDLDefinitionDetailsWidget",
    "src/Utility"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, domConstruct,
    registry, Menu, MenuItem, MenuSeparator, PopupMenuItem, Dialog, Checkbox, Button,
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

                this.addContextMenuItems();
                this._refreshActionState();
                this.refreshGrid();
            },

            addContextMenuItems: function () {
                var context = this;
                this.appendContextMenuItem(this.i18n.Delete, function () {
                    context._onDelete()
                });
                this.contextMenu.addChild(new MenuSeparator());
                this.appendContextMenuItem(this.i18n.Bind, function () {
                    context._onBind()
                });
            },

            _onDelete: function () {
                var context = this;
                var selection = this.grid.getSelected();
                var list = this.arrayToList(selection, "Name");
                if (confirm(this.i18n.YouAreAboutToDeleteDefinition + "\n" + list)) {
                    WsESDLConfig.DeleteESDLDefinition({
                        request: {
                            Id: selection[0].Name
                        }
                    }).then(function () {
                        this.refreshGrid();
                    })
                }
            },

            _onBind: function () {
                var context = this;
                var selection = this.grid.getSelected();
                this.dialog = new Dialog({
                    title: this.i18n.PleaseSelectAServiceToBind,
                    style: "width: 500px;"
                });
                this.dialog.show();

                if (this.bindingDropDown) {
                    this.bindingDropDown.destroyRecursive();
                }
                this.bindingDropDown = new TargetSelectWidget({
                    style: "width: 480px;"
                });
                this.dialog.addChild(this.bindingDropDown);
                this.bindingDropDown.init({
                    loadDESDLBindings: true
                });
                domConstruct.create("Label", {
                    title: "Overwrite Binding?",
                    innerHTML: "Overwrite Binding?",
                    style: "float:left; margin-top: 10px; padding-bottom:20px;"
                }, this.bindingDropDown.domNode, "after");

                this.confirmBindCheckbox = new Checkbox({
                    checked: true,
                    title: this.i18n.Overwrite,
                    style: "margin:10px 10px 0 0; float:left;"
                }).placeAt(this.bindingDropDown.domNode, "after");

                this.applyBindCheckbox = new Button({
                    onClick: function (evt) {
                        context._saveBinding(selection[0].Name);
                    },
                    label: this.i18n.Apply,
                    title: this.i18n.Apply,
                    style: "float:right; margin-top:5px;"
                }).placeAt(this.bindingDropDown.domNode, "after");

                this.dialog.show();
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
                    selectionMode: "single",
                    columns: {
                        col1: selector({
                            width: 27,
                            selectorType: 'checkbox'
                        }),
                        Name: { label: this.i18n.Name, sortable: true, width: 200 }
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

            _saveBinding: function (selection) {
                WsESDLConfig.PublishESDLBinding({
                    EspProcName: "myesp", //"myesp"
                    EspBindingName: "EspBinding",
                    EsdlDefinitionID: this.bindingDropDown.get("value"),
                    Overwrite: this.confirmBindCheckbox.get("value"),
                    EsdlServiceName: "MathService",
                    Config: "<Methods><Method name=" + +"/></Methods>"
                })
            },

            refreshGrid: function (args) {
                var context = this;
                WsESDLConfig.ListESDLDefinitions({
                    request: {
                    }
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
                        Utility.alphanumSort(results, "Name");
                    }
                    context.store.setData(results);
                    context.grid.refresh();
                    if (context.params.firstLoad && results.length) {
                        var firstRowSelection = context.store.query({ Id: results[0].Id });
                        if (firstRowSelection.length) {
                            context.grid.select({ Name: firstRowSelection[0].Name });
                        }
                    }
                });
            }
        });
    });
