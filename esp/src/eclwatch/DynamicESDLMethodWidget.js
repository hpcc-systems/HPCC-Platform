define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",

    "dijit/registry",
    "dijit/form/Button",

    "hpcc/GridDetailsWidget",
    "src/ESPUtil",
    "src/WsESDLConfig",

    "dgrid/editor",
    "dgrid/tree"

], function (declare, lang, nlsHPCCMod, arrayUtil,
    registry, Button,
    GridDetailsWidget, ESPUtil, WsESDLConfig,
    editor, tree
) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("DynamicESDLMethodWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_Methods,
        idProperty: "__hpcc_id",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this.refresh(params);
        },

        refresh: function (params) {
            this._params = params.Binding;
            this.refreshGrid();
            this._refreshActionState();
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        createGrid: function (domID) {
            var context = this;
            this.openButton = registry.byId(this.id + "Open");
            dojo.destroy(this.id + "Open");

            this.store.mayHaveChildren = function (item) {
                if (!item.__hpcc_parentName) {
                    return true;
                }
                return false;
            };

            this.store.getChildren = function (parent, options) {
                return this.query({ __hpcc_parentName: parent.__hpcc_id }, options);
            };

            this.store.appendChild = function (child) {
                this.__hpcc_parentName.push(child);
            };

            this.saveButton = new Button({
                onClick: function (evt) {
                    context.saveMethod();
                },
                label: context.i18n.Save
            }).placeAt(this.openButton, "after");

            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                sort: [{ attribute: "Name", descending: false }],
                columns: {
                    Name: tree({
                        label: context.i18n.Methods,
                        width: 500
                    }),
                    Value: editor({
                        label: this.i18n.MethodConfiguration,
                        autoSave: true,
                        canEdit: function (object, value) {
                            if (object.Attributes || !object.__hpcc_parentName) {
                                return false;
                            }
                            return true;
                        },
                        editor: "textarea",
                        editorArgs: {
                            rows: 10
                        }
                    })
                }
            }, domID);
            return retVal;
        },

        saveMethod: function () {
            var context = this;
            var userXML = "";
            var results = this.store.query();

            arrayUtil.forEach(results, function (row, idx) {
                if (row.__hpcc_parentName !== null && row.Value !== "") {
                    userXML += row.Value;
                }
            });

            var xmlBuilder = "<Methods>" + userXML + "</Methods>";
            WsESDLConfig.PublishESDLBinding({
                request: {
                    EspProcName: this.params.Binding.ESPProcessName,
                    EspBindingName: this.params.Binding.Name,
                    EspPort: this.params.Binding.Port,
                    EsdlDefinitionID: this.params.Definition,
                    Overwrite: true,
                    Config: xmlBuilder
                }
            }).then(function (response) {
                if (lang.exists("PublishESDLBindingResponse.status", response)) {
                    if (response.PublishESDLBindingResponse.status.Code === 0) {
                        dojo.publish("hpcc/brToaster", {
                            Severity: "Message",
                            Source: "WsESDLConfig.PublishESDLBinding",
                            Exceptions: [{ Source: context.i18n.SuccessfullySaved, Message: response.PublishESDLBindingResponse.status.Description }]
                        });
                    } else {
                        dojo.publish("hpcc/brToaster", {
                            Severity: "Error",
                            Source: "WsESDLConfig.PublishESDLBinding",
                            Exceptions: [{
                                Source: context.i18n.Error,
                                Message: response.PublishESDLBindingResponse.status.Description
                            }]
                        });
                    }
                    context.refreshGrid();
                }
            });
        },

        refreshGrid: function () {
            var context = this;
            var results = [];
            var newRows = [];

            WsESDLConfig.GetESDLBinding({
                request: {
                    EsdlBindingId: this._params.Name,
                    IncludeInterfaceDefinition: true,
                    ReportMethodsAvailable: true
                }
            }).then(function (response) {
                if (lang.exists("GetESDLBindingResponse.ESDLBinding.Configuration.Methods.Method", response)) {
                    results = response.GetESDLBindingResponse.ESDLBinding.Configuration.Methods.Method;
                }

                arrayUtil.forEach(results, function (row, idx) {
                    lang.mixin(row, {
                        __hpcc_parentName: null,
                        __hpcc_id: row.Name
                    });
                    if (row.XML) {
                        newRows.push({
                            __hpcc_parentName: row.Name,
                            __hpcc_id: row.Name + idx,
                            Value: row.XML
                        });
                    } else {
                        newRows.push({
                            __hpcc_parentName: row.Name,
                            __hpcc_id: row.Name + idx,
                            Value: "<Method name=\"" + row.Name + "\"/>"
                        });
                    }
                });

                arrayUtil.forEach(newRows, function (newRow) {
                    results.push(newRow);
                });

                context.store.setData(results);
                context.grid.set("query", { __hpcc_parentName: null });
            });
        }
    });
});
