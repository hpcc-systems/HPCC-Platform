/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
    "dojo/request/xhr",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/registry",

    "hpcc/_Widget",
    "hpcc/ESPWorkunit",

    "dojo/text!../templates/ECLSourceWidget.html",

    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/form/Button"

], function (declare, lang, i18n, nlsHPCC, dom, xhr,
            BorderContainer, ContentPane, registry,
            _Widget, ESPWorkunit,
            template) {
        return declare("ECLSourceWidget", [_Widget], {
            templateString: template,
            baseClass: "ECLSourceWidget",
            i18n: nlsHPCC,

            borderContainer: null,
            eclSourceContentPane: null,
            wu: null,
            editor: null,
            markers: [],
            highlightLines: [],
            readOnly: false,

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

            //  Plugin wrapper  ---
            init: function (params) {
                if (this.inherited(arguments))
                    return;

                var mode = "ecl";
                if (params.sourceMode !== undefined) {
                    mode = params.sourceMode;
                } else if (this.WUXml) {
                    mode = "xml";
                }

                this.editor = CodeMirror.fromTextArea(document.getElementById(this.id + "EclCode"), {
                    tabMode: "indent",
                    matchBrackets: true,
                    gutter: true,
                    lineNumbers: true,
                    mode: mode,
                    readOnly: this.readOnly,
                    gutter: mode === "xml" ? true : false,
                    onGutterClick: CodeMirror.newFoldFunction(CodeMirror.tagRangeFinder)
                });
                dom.byId(this.id + "EclContent").style.backgroundColor = this.readOnly ? 0xd0d0d0 : 0xffffff;

                var context = this;
                if (params.Wuid) {
                    this.wu = ESPWorkunit.Get(params.Wuid);
                    if (this.WUXml) {
                        this.wu.fetchXML(function (xml) {
                            context.editor.setValue(xml);
                        });
                    } else {
                        this.wu.fetchText(function (text) {
                            context.editor.setValue(text);
                        });
                    }
                } else if (lang.exists("ECL", params)) {
                    context.editor.setValue(params.ECL ? params.ECL : "");
                } else if (lang.exists("sourceURL", params)) {
                    xhr(params.sourceURL, {
                        handleAs: "text"
                    }).then(function (data) {
                        context.editor.setValue(data);
                    });
                }
            },

            clearErrors: function (errWarnings) {
                for (var i = 0; i < this.markers.length; ++i) {
                    this.editor.clearMarker(this.markers[i]);
                }
                this.markers = [];
            },

            setErrors: function (errWarnings) {
                for (var i = 0; i < errWarnings.length; ++i) {
                    this.markers.push(this.editor.setMarker(parseInt(
                            errWarnings[i].LineNo, 10) - 1, "",
                            errWarnings[i].Severity + "Line"));
                }
            },

            setCursor: function (line, col) {
                this.editor.setCursor(line - 1, col - 1);
                this.editor.focus();
            },

            clearHighlightLines: function () {
                for (var i = 0; i < this.highlightLines.length; ++i) {
                    this.editor.setLineClass(this.highlightLines[i], null, null);
                }
            },

            highlightLine: function (line) {
                this.highlightLines.push(this.editor.setLineClass(line - 1, "highlightline"));
            },

            setText: function (ecl) {
                this.editor.setValue(ecl);
            },

            setReadOnly: function (readonly) {
                this.editor.readOnly(readonly);
            },

            getText: function () {
                return this.editor.getValue();
            }

        });
    });
