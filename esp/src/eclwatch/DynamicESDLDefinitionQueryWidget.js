define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom",

    "dijit/registry",
    "dijit/MenuSeparator",
    "dijit/Dialog",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",
    "dijit/form/TextBox",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "hpcc/TargetSelectWidget",
    "src/WsESDLConfig",
    "src/ESPUtil",
    "hpcc/DynamicESDLDefinitionDetailsWidget",
    "src/Utility"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom,
    registry, MenuSeparator, Dialog, ToolbarSeparator, Button, TextBox,
    selector,
    GridDetailsWidget, TargetSelectWidget, WsESDLConfig, ESPUtil, DynamicESDLDefinitionDetailsWidget, Utility) {
    return declare("DynamicESDLWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_DefinitionExplorer,
        idProperty: "Name",

        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;

            if (params.Id) {
                this.grid.select(params.Id);
                this.definitionWidget.init({
                    Id: params.Id
                });
            }

            this._refreshActionState();
            this.refreshGrid();
            this.addContextMenuItems();
        },

        addContextMenuItems: function () {
            var context = this;
            this.appendContextMenuItem(this.i18n.Delete, function () { context._onDelete() });
            this.contextMenu.addChild(new MenuSeparator());
            this.appendContextMenuItem(this.i18n.Bind, function () { context._onBind() });
        },

        _onBind: function () {
            var context = this;
            var selection = this.grid.getSelected();

            this.dialog = new Dialog({
                title: this.i18n.AddBinding,
                style: "width: 480px;"
            });

            this.dialogButton = new Button({
                style: "float:right; padding: 0 10px 10px 20px;",
                innerHTML: context.i18n.Apply,
                onClick: function () {
                    context._saveBinding();
                }
            }).placeAt(this.dialog.domNode, "last");

            if (this.esdlEspDropDown) {
                this.esdlEspDropDown.destroyRecursive();
            }

            this.esdlEspProcessesDropDown = new TargetSelectWidget({
                style: "float:left; width:100%;"
            });

            this.esdlEspProcessesDropDown.init({
                LoadESDLESPProcesses: true
            });

            var dialogDynamicForm = {
                ESProcess: {
                    label: this.i18n.ESPProcessName,
                    widget: this.esdlEspProcessesDropDown,
                },
                Port: {
                    label: this.i18n.Port,
                    widget: new TextBox({
                        placeholder: this.i18n.Port,
                        id: "PortNB",
                        type: "text",
                        required: true,
                        style: "width:100%;",
                        maxLength: 5
                    })
                },
                DefinitionID: {
                    label: this.i18n.DefinitionID,
                    widget: new TextBox({
                        id: "DefId",
                        type: "text",
                        style: "width:100%;",
                        readOnly: true,
                        value: selection[0].Name
                    })
                },
                ServiceName: {
                    label: this.i18n.ServiceName,
                    widget: new TextBox({
                        placeholder: this.i18n.ServiceName,
                        id: "ServiceNameTB",
                        required: true,
                        style: "width:100%;"
                    })
                }
            };

            var table = Utility.DynamicDialogForm(dialogDynamicForm);

            this.dialog.set("content", table);
            this.dialog.show();
            this.dialog.on("cancel", function () {
                context.dialog.destroyRecursive();
            });
            this.dialog.on("hide", function () {
                context.dialog.destroyRecursive();
            });
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
            this.openButton = registry.byId(this.id + "Open");
            this.deleteDefinitionButton = new Button({
                id: this.id + "DeleteDefinition",
                label: this.i18n.Delete,
                onClick: function (event) {
                    context._onDelete(event);
                }
            }).placeAt(this.openButton.domNode, "after");
            dojo.destroy(this.id + "Open");
            this.addBindingButton = new Button({
                id: this.id + "AddBinding",
                label: this.i18n.AddBinding,
                onClick: function (event) {
                    context._onBind(event);
                }
            }).placeAt(this.deleteDefinitionButton.domNode, "after");
            var tmpSplitter = new ToolbarSeparator().placeAt(this.addBindingButton.domNode, "before");

            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                selectionMode: "single",
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'radio',
                        unhidable: true,
                        label: ""
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

        _onDelete: function () {
            var selections = this.grid.getSelected();
            var name = selections[0].Name.split(".");
            var list = this.arrayToList(selections, "Name");
            if (confirm(this.i18n.DeleteSelectedDefinitions + "\n" + list)) {
                var context = this;
                WsESDLConfig.DeleteESDLDefinition({
                    request: {
                        Id: selections[0].Name,
                        Name: name[0],
                        Version: name[1]
                    }
                }).then(function (response) {
                    if (lang.exists("DeleteESDLRegistryEntryResponse.status", response)) {
                        dojo.publish("hpcc/brToaster", {
                            Severity: "Message",
                            Source: "WsESDLConfig/DeleteESDLDefinition",
                            Exceptions: [{
                                Source: context.i18n.DefinitionDeleted,
                                Message: response.DeleteESDLRegistryEntryResponse.status.Description,
                                duration: 1
                            }]
                        });
                    }
                    context.refreshGrid();
                })
            }
        },

        _saveBinding: function (selection) {
            var context = this;

            WsESDLConfig.PublishESDLBinding({
                request: {
                    EspProcName: this.esdlEspProcessesDropDown.get("value"),
                    EspPort: dom.byId("PortNB").value,
                    EsdlDefinitionID: dom.byId("DefId").value,
                    EsdlServiceName: dom.byId("ServiceNameTB").value,
                    Overwrite: true
                }
            }).then(function (response) {
                if (lang.exists("PublishESDLBindingResponse.status", response)) {
                    if (response.PublishESDLBindingResponse.status.Code === 0) {
                        dojo.publish("hpcc/brToaster", {
                            Severity: "Message",
                            Source: "WsESDLConfig.PublishESDLBinding",
                            Exceptions: [{
                                Source: context.i18n.SuccessfullySaved,
                                Message: response.PublishESDLBindingResponse.status.Description,
                                duration: 1
                            }]
                        });
                    } else {
                        dojo.publish("hpcc/brToaster", {
                            Severity: "Error",
                            Source: "WsESDLConfig.PublishESDLBinding",
                            Exceptions: [{
                                Source: context.i18n.Error,
                                Message: response.PublishESDLBindingResponse.status.Description,
                                duration: 1
                            }]
                        });
                    }
                }
                context.dialog.hide();
            });
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
