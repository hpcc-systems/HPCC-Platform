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
    "dojo/dom-construct",

    "dijit/registry",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/Dialog",
    "dijit/form/TextBox",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPQuery",
    "hpcc/ESPUtil",
    "hpcc/WsESDLConfig",

    "dgrid/selector",
    "dgrid/editor",
    "dgrid/tree"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, domConstruct,
                registry, Button, ToolbarSeparator, Dialog, TextBox,
                GridDetailsWidget, ESPQuery, ESPUtil, WsESDLConfig,
                selector, editor, tree) {
    return declare("DynamicESDLMethodWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: "Methods",
        idProperty: "__hpcc_id",

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.refresh(params);
            this._refreshActionState();
        },

        refresh: function (params) {
            this.params = params;

            if (params.Methods && !params.Configuration) {
                this._populatedByUser(params)
            } else {
                this.refreshGrid(params);
            }
       },

        startup: function (args) {
            this.inherited(arguments);
        },

        createGrid: function (domID) {
            var context = this;
            this.dialog = new Dialog({
                title: context.i18n.AddAttributes,
                style: "width: 300px;"
            });
            this.openButton = registry.byId(this.id + "Open");
            this.saveAttributeButton = new Button({
                onClick: function (val) {
                    var att = context.attributeTextBox.get("value");
                    var val = context.valueTextBox.get("value");
                    context._addAttribute(att,val);
                    context.attributeTextBox.reset();
                    context.valueTextBox.reset();
                    context.dialog.hide();
                },
                label: context.i18n.Save,
                style: "float: right;"
            }).placeAt(this.dialog);
            this.attributeTextBox = new TextBox({
                label: this.i18n.Attribute,
                placeholder: this.i18n.Attribute,
                style: "margin-bottom: 10px;"
            }).placeAt(this.dialog);
            this.valueTextBox = new TextBox({
                label: this.i18n.Value,
                placeholder: this.i18n.Value
            }).placeAt(this.dialog);
            this.addAddAttritbuteButton = new Button({
                disabled: true,
                onClick: function (val) {
                    context.dialog.show();
                },
                label: this.i18n.AddAttributes2
            }).placeAt(this.openButton.domNode, "after");
            this.addRemoveAttritbuteButton = new Button({
                disabled: true,
                onClick: function (val) {
                    if (confirm(context.i18n.RemoveAttributeQ)) {
                        var selection = context.grid.getSelected()
                        context._removeAttribute(selection);
                    }
                },
                label: context.i18n.RemoveAtttributes
            }).placeAt(this.addAddAttritbuteButton.domNode, "after");
            dojo.destroy(this.id + "Open");

            this.store.mayHaveChildren = function (item) {
                if (item.Attributes && item.Attributes.Attribute.length) {
                    return true;
                }
                return false;
            };

            this.store.getChildren = function (parent, options) {
               return this.query({__hpcc_parentName: parent.__hpcc_id}, options);
            };

            this.store.appendChild = function (child) {
                this.__hpcc_parentName.push(child);
            };

            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                sort: [{ attribute: "Name", descending: false }],
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox',
                        sortable: false
                    }),
                    Name: tree({ label: context.i18n.Methods }),
                    Attribute: editor({
                        label: context.i18n.Attribute,
                        autoSave: true,
                        canEdit: function (object, value) {
                            if (object.Attributes || !object.__hpcc_parentName) {
                                return false;
                            }
                            return true
                        },
                        editor: dijit.form.ValidationTextBox,
                        editorArgs: {
                            required: true,
                            placeholder: context.i18n.AttributesAreRequired,
                            promptMessage: context.i18n.AttributesAreRequired
                        }
                    }),
                    Value: editor({ label: context.i18n.Value, editor: "text",
                        autoSave: true,
                        canEdit: function (object, value) {
                            if (object.Attributes || !object.__hpcc_parentName) {
                                return false;
                            }
                            return true
                        }
                    })
                }
            }, domID);

            retVal.on("dgrid-select, dgrid-deselect", function (event) {
                context.refreshActionState(event);
            });

            retVal.on("dgrid-datachange", function (event) {
                context.refreshActionState(event);

            });
            return retVal;
        },

        _removeAttribute: function (selection) {
            var context = this;
            //building out xml to pass into the service until the service fully complies to a add/remove/update type of mechanism
            var removedQuery;
            var passInXML;
            var dynamicAttributes = "";
            arrayUtil.forEach(selection, function (row, idx) {
                if (row.__hpcc_parentName) {
                    context.store.remove(row.__hpcc_id);
                    removedQuery = context.store.query({__hpcc_parentName:row.__hpcc_parentName});
                    if (removedQuery.length === 0) {
                        passInXML = "<Methods><Method name='" + row.__hpcc_parentName + "'"+ "/></Methods>"
                    }
                }
            });
            arrayUtil.forEach(removedQuery, function (row, idx) {
                dynamicAttributes += " " + row.Attribute + "=\'" + row.Value + "\'";
                passInXML = "<Methods><Method name='" + row.__hpcc_parentName + "\'" + dynamicAttributes + "/></Methods>"
            });

            WsESDLConfig.ConfigureESDLBindingMethod({
                request: {
                    EspProcName: context.params.Configuration.__hpcc_parentName,
                    EspBindingName: context.params.Configuration.Name,
                    EsdlDefinitionID: context.params.Configuration.DefinitionID ? context.params.Configuration.DefinitionID : context.params.Methods.Id,
                    EsdlServiceName: context.params.Configuration.Service,
                    Overwrite: true,
                    Config: passInXML
                }
            }).then(function (response) {
                if (lang.exists("ConfigureESDLBindingMethodResponse.status", response)) {
                    if (response.ConfigureESDLBindingMethodResponse.status.Code === 0) {
                        //  MV Is this needed (idx is invalid)?:  context.store.remove(selection[idx].__hpcc_id);
                    }
                    context.refresh(context.params);
                }
            });
        },

        _addAttribute: function (attr, val) {
            //building out xml to pass into the service until the service fully complies to a add/remove/update type of mechanism
            var context = this;
            var selection = this.grid.getSelected();
            var AddedQuery;
            var passInXML;
            var dynamicAttributes = "";

            if (selection[0].Attributes) {
                arrayUtil.forEach(selection, function (row, idx) {
                    AddedQuery = context.store.query({__hpcc_parentName:row.Name});
                    arrayUtil.forEach(AddedQuery, function (row, idx) {
                        dynamicAttributes += " " + row.Attribute + "=\'" + row.Value +"\'";
                        dynamicAttributes += " " + attr + "=\'" + val + "\'";
                    });
                    passInXML = "<Methods><Method name='" + row.Name + "\'" + dynamicAttributes + "/></Methods>"
                });
            }  else {
                var passInXML = "<Methods><Method name='" + selection[0].__hpcc_id + "' " + attr + "='" + val +  "\'" + "/></Methods>";
            }
            WsESDLConfig.ConfigureESDLBindingMethod({
                request: {
                    EspProcName: context.params.Configuration.__hpcc_parentName,
                    EspBindingName: context.params.Configuration.Name,
                    EsdlDefinitionID: context.params.Configuration.DefinitionID ? context.params.Configuration.DefinitionID : context.params.Methods.Id ,
                    EsdlServiceName: context.params.Configuration.Service,
                    Overwrite: true,
                    Config: passInXML
                }
            }).then(function (response) {
                if (lang.exists("ConfigureESDLBindingMethodResponse.status", response)) {
                    context.refresh(context.params);
                }
            });
        },

        refreshActionState: function (event) {
            var context = this;
            var selection = this.grid.getSelected();
            var isParent = false;
            var hasParent = false;

            for (var i = 0; i < selection.length; ++i) {
                if (selection[i] && selection[i].__hpcc_parentName !== null) {
                        hasParent = true;
                    } else {
                        hasParent = false;
                    }
                if (selection[i].__hpcc_parentName === null && selection.length < 2) {
                    isParent = true;
                } else {
                    isParent = false;
                }
            }

            this.addAddAttritbuteButton.set("disabled", !isParent);
            this.addRemoveAttritbuteButton.set("disabled", !hasParent);
        },

        _populatedByUser: function (params) {
            var context = this;
            var results = [];
            var newRows = [];

            results = params.Methods.Methods.Method;

            arrayUtil.forEach(results, function (row, idx) {
                lang.mixin(row, {
                    __hpcc_parentName: null,
                    __hpcc_id: row.Name
                });

                if (row.Attributes) {
                    arrayUtil.forEach(row.Attributes.Attribute, function (attr, idx) {
                        newRows.push({
                            __hpcc_parentName: row.Name,
                            __hpcc_id: row.Name + idx,
                            Attribute: attr.Name,
                            Value: attr.Value
                        });
                    });
                }
            });

            arrayUtil.forEach(newRows, function (newRow) {
                results.push(newRow);
            });

            context.store.setData(results);
            context.grid.set("query", {__hpcc_parentName: null });
        },

        refreshGrid: function (params) {
            var context = this;
            var results = [];
            var newRows = [];

            WsESDLConfig.GetESDLBinding({
                request: {
                    EspProcName: context.params.Configuration.__hpcc_parentName ? context.params.Configuration.__hpcc_parentName : params.Configuration.__hpcc_parentName ,
                    EspBindingName: context.params.Configuration.Name ? context.params.Configuration.Name : params.Configuration.__hpcc_parentName ,
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

                    if (row.Attributes) {
                        arrayUtil.forEach(row.Attributes.Attribute, function (attr, idx) {
                            newRows.push({
                                __hpcc_parentName: row.Name,
                                __hpcc_id: row.Name + idx,
                                Attribute: attr.Name,
                                Value: attr.Value
                            });
                        });
                    }

                });

                arrayUtil.forEach(newRows, function (newRow) {
                    results.push(newRow);
                });

                context.store.setData(results);
                context.grid.set("query", {__hpcc_parentName: null });
            })
        }
    });
});
