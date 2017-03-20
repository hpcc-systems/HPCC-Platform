/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-form",
    "dojo/dom-attr",
    "dojo/request/iframe",
    "dojo/dom-class",
    "dojo/query",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/_base/array",
    "dojo/topic",

    "dijit/registry",
    "dijit/form/ToggleButton",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/Dialog",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    'dgrid/extensions/CompoundColumns',
    'dgrid/editor',

    "hpcc/_TabContainerWidget",
    "hpcc/ESPWorkunit",
    "hpcc/ESPRequest",
    "hpcc/ESPUtil",
    "hpcc/TargetSelectWidget",
    "hpcc/ECLSourceWidget",
    "hpcc/WsTopology",
    "hpcc/GridDetailsWidget",
    "hpcc/WsESDLConfig",
    "hpcc/DynamicESDLMethodWidget",

    "dojo/text!../templates/DynamicESDLDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Form",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/ValidationTextBox",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/form/TextBox",
    "dijit/Dialog",
    "dijit/form/SimpleTextarea",

    "hpcc/TableContainer"
], function (declare, lang, i18n, nlsHPCC, dom, domConstruct, domForm, domAttr, iframe, domClass, query, Memory, Observable, arrayUtil, topic,
                registry, ToggleButton, Button, ToolbarSeparator, Dialog,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, CompoundColumns, editor,
                _TabContainerWidget, ESPWorkunit, ESPRequest, ESPUtil, TargetSelectWidget, ECLSourceWidget, WsTopology, GridDetailsWidget, WsESDLConfig, DynamicESDLMethodWidget,
                template) {
    return declare("DynamicESDLDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "DynamicESDLDetailsWidget",
        i18n: nlsHPCC,

        summaryWidget: null,
        bindingWidget: null,
        bindingWidgetLoaded: false,
        definitionWidget: null,
        definitionWidgetLoaded: false,
        configurationWidget: null,
        configurationWidgetLoaded: false,
        configurationGrid: null,

        postCreate: function (args) {
            this.inherited(arguments);
            var context = this;
            this.details = registry.byId(this.id + "_Details");
            this.definitionWidget = registry.byId(this.id + "Definition");
            this.configurationWidget = registry.byId(this.id + "Configuration");
            this.definitionID = registry.byId(this.id + "DefinitionID");
            this.bindingRefresh = registry.byId(this.id + "BindingRefresh");
            this.refreshButton = registry.byId(this.id + "Refresh");

            this.addBindingButton = new Button({
                id: this.id + "AddBinding",
                disabled: true,
                onClick: function (val) {
                    context.dialog.show();
                },
                label: context.i18n.AddBinding
            }).placeAt(this.refreshButton.domNode, "after");
            var tmpSplitter = new ToolbarSeparator().placeAt(this.addBindingButton.domNode, "before");
            this.deleteBindingButton = new Button({
                id: this.id + "DeleteBinding",
                disabled: true,
                onClick: function (val) {
                    if (confirm(context.i18n.YouAreAboutToDeleteBinding)) {
                        WsESDLConfig.DeleteESDLBinding({
                        request: {
                            Id:  context.params.__hpcc_parentName + "." + context.params.Name
                        }
                        }).then(function (response) {
                            if (lang.exists("DeleteESDLRegistryEntryResponse.status.Code", response)) {
                                if (response.DeleteESDLRegistryEntryResponse.status.Code === 0) {
                                    dojo.publish("hpcc/brToaster", {
                                        Severity: "Message",
                                        Source: "WsESDLConfig.DeleteESDLBinding",
                                        Exceptions: [{ Source: context.i18n.DeletedBinding, Message: context.i18n.BindingDeleted }]
                                    });
                                    context.addBindingButton.set("disabled", false);
                                    context.deleteBindingButton.set("disabled", true);
                                    context.widget._Binding.set("disabled", true);
                                    context.params.Owner._onRefresh({});
                                    context.params.Owner.grid.deselect(context.params.__hpcc_id);
                                    context.params.Owner.grid.select(context.params.__hpcc_id);
                                }
                            }
                        });
                    }
                },
                label: context.i18n.DeleteBinding
            }).placeAt(this.addBindingButton.domNode, "after");
            this.definitionDropDown = new TargetSelectWidget({});
            this.definitionDropDown.init({
                LoadDESDLDefinitions: true
            });
            this.dialog = new Dialog({
                title: context.i18n.PleasePickADefinition,
                style: "width: 300px;"
            });
            this.dialog.addChild(this.definitionDropDown);
            this.addDefinitionButton = new Button({
                disabled: false,
                style: "float:right;",
                label: this.i18n.Apply,
                onClick: function (val) {
                    WsESDLConfig.GetESDLDefinition({
                        request: {
                            Id: context.definitionDropDown.get("value"),
                            ReportMethodsAvailable: true
                        }
                    }).then(function (response) {
                        if (response.GetESDLDefinitionResponse.status.Code === 0) {
                             WsESDLConfig.PublishESDLBinding({
                                request: {
                                    EspProcName: context.params.__hpcc_parentName,
                                    EspBindingName: context.params.Name,
                                    EsdlDefinitionID: response.GetESDLDefinitionResponse.Id,
                                    Overwrite: true,
                                    EsdlServiceName: response.GetESDLDefinitionResponse.ESDLServices.Name,
                                    Config: "<Methods><Method name='" + response.GetESDLDefinitionResponse.Methods.Method[0].Name + "'/></Methods>"
                                }
                            }).then(function (publishresponse) {
                                if (lang.exists("PublishESDLBindingResponse.status.Code", response)) {
                                    if (publishresponse.PublishESDLBindingResponse.status.Code === 0) {
                                        context.deleteBindingButton.set("disabled", false);
                                        context.addBindingButton.set("disabled", true);
                                        context.widget._Binding.set("disabled", false);
                                        context.widget._Binding.__hpcc_initalized = true;
                                        var xml = context.formatXml(response.GetESDLDefinitionResponse.XMLDefinition);
                                        context.widget.Definition.init({
                                            sourceMode: "xml"
                                        });
                                        context.widget.Definition.setText(xml);

                                        if (!context.widget.Configuration.initalized) {
                                            context.widget.Configuration.init({
                                                Configuration: context.params,
                                                Methods: response.GetESDLDefinitionResponse
                                            });
                                        } else if (context.widget.Configuration.refresh) {
                                            context.widget.Configuration.refresh({
                                                Configuration: context.params,
                                                Methods: response.GetESDLDefinitionResponse
                                            });
                                        }
                                        context.params.Owner._onRefresh({});
                                        context.params.Owner.grid.deselect(context.params.__hpcc_id);
                                        context.params.Owner.grid.select(context.params.__hpcc_id);
                                    }
                                }
                            });
                        }
                    });

                    context.widget._Binding.set("disabled", false);
                    context.dialog.hide();
                }
            });
            this.dialog.addChild(this.addDefinitionButton)
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_TopologyDetails;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
        },

        //  Implementation  ---
        init: function (params) {
            if (this.params.__hpcc_id === params.__hpcc_id)
                return;
            var context = this;
            this.selectChild(this.widget._Summary.id);
            this.initalized = false;
            this.widget._Summary.__hpcc_initalized = false;
            this.widget._Binding.__hpcc_initalized = false;

            if (params.__hpcc_parentName) {
                if (params.__binding_info.Definition.Id !== null) {
                    context.widget._Binding.set("disabled", false);
                    context.addBindingButton.set("disabled", true);
                    context.deleteBindingButton.set("disabled", false);
                } else {
                    context.widget._Binding.set("disabled", true);
                    context.addBindingButton.set("disabled", false);
                    context.deleteBindingButton.set("disabled", true);
                }
            }  else {
                domConstruct.destroy(this.id + "serviceInformation");
                domConstruct.create("p", {});
                context.details.setContent(context.i18n.PleaseSelectADynamicESDLService);
                context.widget._Binding.set("disabled", true);
                context.deleteBindingButton.set("disabled", true);
                context.addBindingButton.set("disabled", true);
            }

            this.inherited(arguments);
            this.initTab();
        },

        initTab: function () {
            var context = this;
            var currSel = this.getSelectedChild();
            if (currSel.id === this.widget._Summary.id && !this.widget._Summary.__hpcc_initalized) {
                this.widget._Summary.__hpcc_initalized = true;
                var table = domConstruct.create("table", {id: this.id + "serviceInformation"});
                if (this.params.__hpcc_parentName) {
                    for (var key in this.params) {
                        if (this.params.hasOwnProperty(key) && !(this.params[key] instanceof Object)) {
                            if (key.indexOf("__") !== 0) {
                                switch (key) {
                                    case "Path":
                                    case "ProcessNumber":
                                    case "Widget":
                                    break;
                                default:
                                    var tr = domConstruct.create("tr", {}, table);
                                    domConstruct.create("td", {
                                        innerHTML: "<b>" + key + ":&nbsp;&nbsp;</b>"
                                    }, tr);
                                    domConstruct.create("td", {
                                        innerHTML: this.params[key]
                                    }, tr);
                                }
                            }
                        }
                    }
                    this.details.setContent(table);
                }
            } else if (currSel.id === this.widget._Binding.id && !this.widget._Binding.__hpcc_initalized) {
                this.widget._Binding.__hpcc_initalized = true;

                var xml = context.formatXml(context.params.__binding_info.Definition.Interface)
                context.widget.Definition.init({
                    sourceMode: "xml"
                });
                context.widget.Definition.setText(xml);

                if (!context.widget.Configuration.initalized) {
                    context.widget.Configuration.init({
                        Configuration: context.params
                    });
                } else if (context.widget.Configuration.refresh) {
                    context.widget.Configuration.refresh({
                        Configuration: context.params
                    });
                }
            }
        },

        updateInput: function (name, oldValue, newValue) {
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            }
        }
    });
});