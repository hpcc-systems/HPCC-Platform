define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "src/nlsHPCC",
    "dojo/dom",
    "dojo/topic",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "src/CodeMirror",

    "src/WsPackageMaps",

    "dojo/text!../templates/PackageSourceWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane"
],
    function (declare, lang, i18n, nlsHPCCMod, dom, topic,
        _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin,
        registry,
        CodeMirror,
        WsPackageMaps, template) {

        var nlsHPCC = nlsHPCCMod.default;
        return declare("PackageSourceWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "PackageSourceWidget",
            i18n: nlsHPCC,
            borderContainer: null,

            editor: null,
            readOnly: true,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            resize: function (args) {
                this.inherited(arguments);
                this.borderContainer.resize();
                if (this.editor) {
                    this.editor.setSize("100%", "100%");
                }
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            init: function (params) {
                if (this.initalized)
                    return;

                this.initalized = true;
                this.editor = CodeMirror.fromTextArea(document.getElementById(this.id + "XMLCode"), {
                    tabMode: "indent",
                    matchBrackets: true,
                    lineNumbers: true,
                    mode: this.isXmlContent ? "xml" : "ecl",
                    readOnly: this.readOnly,
                    foldGutter: this.isXmlContent ? true : false,
                    gutters: this.isXmlContent ? ["CodeMirror-linenumbers", "CodeMirror-foldgutter"] : ["CodeMirror-linenumbers"]
                });
                dom.byId(this.id + "XMLContent").style.backgroundColor = this.readOnly ? 0xd0d0d0 : 0xffffff;
                this.editor.setSize("100%", "100%");

                var context = this;
                if (this.isXmlContent) {
                    WsPackageMaps.getPackageMapById(params).then(function (response) {
                        if (!lang.exists("GetPackageMapByIdResponse.Info", response))
                            context.editor.setValue(i18n.NoContent);
                        else
                            context.editor.setValue(response.GetPackageMapByIdResponse.Info);
                    }, function (err) {
                        context.showErrors(err);
                    });
                }
                else {
                    WsPackageMaps.validatePackage(params).then(function (response) {
                        var responseText = context.validateResponseToText(response.ValidatePackageResponse);
                        if (responseText === '')
                            context.editor.setValue("(Empty)");
                        else
                            context.editor.setValue(responseText);
                    }, function (err) {
                        context.showErrors(err);
                    });
                }
            },

            showErrors: function (err) {
                topic.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: err.message,
                    Exceptions: [{ Message: err.stack }]
                });
            },

            addArrayToText: function (arrayTitle, arrayItems, text) {
                if ((arrayItems.Item !== undefined) && (arrayItems.Item.length > 0)) {
                    text += arrayTitle + ":\n";
                    for (var i = 0; i < arrayItems.Item.length; i++)
                        text += "  " + arrayItems.Item[i] + "\n";
                    text += "\n";
                }
                return text;
            },

            validateResponseToText: function (response) {
                var text = "";
                if (!lang.exists("Errors", response) || (response.Errors.length < 1))
                    text += this.i18n.NoErrorFound;
                else
                    text = this.addArrayToText(this.i18n.Errors, response.Errors, text);
                if (!lang.exists("Warnings", response) || (response.Warnings.length < 1))
                    text += this.i18n.NoWarningFound;
                else
                    text = this.addArrayToText(this.i18n.Warnings, response.Warnings, text);

                text += "\n";
                text = this.addArrayToText(this.i18n.QueriesNoPackage, response.queries.Unmatched, text);
                text = this.addArrayToText(this.i18n.PackagesNoQuery, response.packages.Unmatched, text);
                text = this.addArrayToText(this.i18n.FilesNoPackage, response.files.Unmatched, text);
                return text;
            }
        });
    });
