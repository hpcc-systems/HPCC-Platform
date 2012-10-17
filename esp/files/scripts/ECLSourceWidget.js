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
    "dojo/aspect",
    "dojo/has",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-class",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/registry",

    "hpcc/ESPWorkunit",

    "dojo/text!../templates/ECLSourceWidget.html",

    "dijit/Toolbar", "dijit/ToolbarSeparator", "dijit/form/Button"
],
    function (declare, aspect, has, dom, domConstruct, domClass,
            _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, ContentPane, registry,
            ESPWorkunit,
            template) {
        return declare("ECLSourceWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "ECLSourceWidget",
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
                this.initEditor();
            },

            resize: function (args) {
                this.inherited(arguments);
                this.borderContainer.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            //  Plugin wrapper  ---
            initEditor: function () {
                this.editor = CodeMirror.fromTextArea(document.getElementById(this.id + "EclCode"), {
                    tabMode: "indent",
                    matchBrackets: true,
                    gutter: true,
                    lineNumbers: true,
                    readOnly: this.readOnly
                });
            },

            init: function (params) {
                var context = this;
                if (params.Wuid) {
                    this.wu = new ESPWorkunit({
                        wuid: params.Wuid
                    });
                    this.wu.fetchText(function (text) {
                        context.editor.setValue(text);
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
