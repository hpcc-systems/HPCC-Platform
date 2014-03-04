/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.
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
    "dojo/dom",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/registry",

    "hpcc/WsPackageMaps",

    "dojo/text!../templates/PackageSourceWidget.html"
],
    function (declare, lang, i18n, nlsHPCC, dom,
            _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin,
            BorderContainer, ContentPane, registry,
            WsPackageMaps, template) {
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
                    gutter: true,
                    lineNumbers: true,
                    mode: this.isXmlContent ? "xml" : "ecl",
                    readOnly: this.readOnly,
                    gutter: this.isXmlContent ? true : false,
                    onGutterClick: CodeMirror.newFoldFunction(CodeMirror.tagRangeFinder)
                });
                dom.byId(this.id + "XMLContent").style.backgroundColor = this.readOnly ? 0xd0d0d0 : 0xffffff;

                var context = this;
                if (this.isXmlContent) {
                    WsPackageMaps.getPackageMapById(params, {
                        load: function (response) {
                            context.editor.setValue(response);
                        },
                        error: function (errMsg, errStack) {
                            context.showErrors(errMsg, errStack);
                        }
                    });
                }
                else {
                    WsPackageMaps.validatePackage(params, {
                        load: function (response) {
                            var responseText = context.validateResponseToText(response);
                            //console.log(responseText);
                            if (responseText == '')
                                context.editor.setValue("(Empty)");
                            else
                                context.editor.setValue(responseText);
                        },
                        error: function (errMsg, errStack) {
                            context.showErrors(errMsg, errStack);
                        }
                    });
                }
            },

            showErrors: function (errMsg, errStack) {
                dojo.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: errMsg,
                    Exceptions: [{ Message: errStack }]
                });
            },

            addArrayToText: function (arrayTitle, arrayItems, text) {
                if ((arrayItems.Item != undefined) && (arrayItems.Item.length > 0)) {
                    text += arrayTitle + ":\n";
                    for (i=0;i<arrayItems.Item.length;i++)
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
