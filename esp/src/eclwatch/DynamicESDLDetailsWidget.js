define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/dom",
    "dojo/dom-attr",

    "dijit/registry",

    "src/Clippy",

    "hpcc/_TabContainerWidget",
    "src/WsESDLConfig",

    "dojo/text!../templates/DynamicESDLDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Form",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/NumberTextBox",
    "dijit/form/ValidationTextBox",
    "dijit/form/Select",
    "dijit/form/ToggleButton",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/form/TextBox",
    "dijit/Dialog",
    "dijit/form/SimpleTextarea",

    "hpcc/TableContainer"
],
    function (declare, lang, nlsHPCCMod, dom, domAttr,
        registry,
        Clippy,
        _TabContainerWidget, WsESDLConfig,
        template) {

        var nlsHPCC = nlsHPCCMod.default;
        return declare("DynamicESDLDetailsWidget", [_TabContainerWidget], {
            templateString: template,
            baseClass: "DynamicESDLDetailsWidget",
            i18n: nlsHPCC,
            definitionWidget: null,
            definitionWidgetLoaded: false,
            configurationWidget: null,
            configurationLoaded: false,
            bound: false,
            binding: null,
            configuration: null,
            definition: null,
            definitionID: null,

            postCreate: function (args) {
                this.inherited(arguments);
                Clippy.attach(this.id + "ClippyButton");
                this.definitionWidget = registry.byId(this.id + "_Definition");
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            destroy: function (args) {
                this.inherited(arguments);
            },

            getTitle: function () {
                return this.i18n.title_DESDL;
            },

            //  Implementation  ---
            init: function (params) {
                var context = this;
                if (this.inherited(arguments))
                    return;
                if (params.Name) {
                    dom.byId(context.id + "Id").textContent = params.Name;
                    WsESDLConfig.GetESDLBinding({
                        request: {
                            EsdlBindingId: params.Name,
                            IncludeInterfaceDefinition: true,
                            ReportMethodsAvailable: true
                        }
                    }).then(function (response) {
                        if (response.GetESDLBindingResponse.ConfigXML) {
                            context.bound = true;
                            context.definition = response.GetESDLBindingResponse.ESDLBinding.Definition.Interface;
                            context.configuration = response.GetESDLBindingResponse.ESDLBinding.Configuration.Methods.Method;
                            context.definitionID = response.GetESDLBindingResponse.ESDLBinding.Definition.Id;
                            context.refreshActionState();
                        }
                        for (var key in response.GetESDLBindingResponse) {
                            context.updateInput(key, null, response.GetESDLBindingResponse[key]);
                        }
                    });
                }
            },

            initTab: function () {
                var context = this;
                var currSel = this.getSelectedChild();
                if (currSel.id === this.widget._Configuration.id && !this.widget._Configuration.__hpcc_initalized) {
                    this.widget._Configuration.init({
                        Binding: this.params,
                        Definition: this.definitionID
                    });
                } else if (currSel.id === this.definitionWidget.id && !this.definitionWidgetLoaded) {
                    this.definitionWidgetLoaded = true;
                    var xml = context.formatXml(this.definition);
                    this.definitionWidget.init({
                        sourceMode: "xml",
                        Usergenerated: xml
                    });
                }
            },

            _onDeleteBinding: function () {
                var context = this;
                if (confirm(context.i18n.YouAreAboutToDeleteBinding)) {
                    WsESDLConfig.DeleteESDLBinding({
                        request: {
                            Id: context.params.Name
                        }
                    }).then(function (response) {
                        if (lang.exists("DeleteESDLRegistryEntryResponse.status.Code", response)) {
                            if (response.DeleteESDLRegistryEntryResponse.status.Code === 0) {
                                dojo.publish("hpcc/brToaster", {
                                    Severity: "Message",
                                    Source: "WsESDLConfig.DeleteESDLBinding",
                                    Exceptions: [{
                                        Source: context.i18n.DeletedBinding,
                                        Message: response.DeleteESDLRegistryEntryResponse.status.Description
                                    }]
                                });
                                context.bound = false;
                                context.refreshActionState();
                            }
                        }
                    });
                }
            },

            updateInput: function (name, oldValue, newValue) {
                var registryNode = registry.byId(this.id + name);
                if (registryNode) {
                    registryNode.set("value", newValue);
                } else {
                    var domElem = dom.byId(this.id + name);
                    if (domElem) {
                        switch (domElem.tagName) {
                            case "SPAN":
                            case "DIV":
                                dom.byId(this.id + name).textContent = newValue;
                                break;
                            case "INPUT":
                            case "TEXTAREA":
                                domAttr.set(this.id + name, "value", newValue);
                                break;
                            default:
                                alert(domElem.tagName);
                        }
                    }
                }
            },

            refreshActionState: function () {
                var hasBinding = this.bound;
                registry.byId(this.id + "DeleteBinding").set("disabled", !this.bound);
                registry.byId(this.id + "_Configuration").set("disabled", !this.bound);
                registry.byId(this.id + "_Definition").set("disabled", !this.bound);
            }
        });
    });
