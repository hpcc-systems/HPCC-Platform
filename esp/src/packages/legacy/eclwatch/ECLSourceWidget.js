define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/dom",
    "dojo/request/xhr",
    "dojo/topic",

    "dijit/registry",

    "@hpcc-js/codemirror",

    "hpcc/_Widget",
    "src/ESPWorkunit",
    "src/Utility",

    "dojo/text!../templates/ECLSourceWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/form/Button"

], function (declare, lang, nlsHPCCMod, dom, xhr, topic,
    registry,
    CodeMirror,
    _Widget, ESPWorkunit, Utility,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
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

            if (params.readOnly !== undefined)
                this.readOnly = params.readOnly;

            var eclContent = dom.byId(this.id + "EclContent");
            Utility.onDomMutate(eclContent, () => {
                this.editor.target(this.id + "EclContent").lazyRender();
                dom.byId(this.id + "EclCode").style.display = "none";
            });

            this.editor = new CodeMirror.ECLEditor();
            eclContent.style.backgroundColor = this.readOnly ? 0xd0d0d0 : 0xffffff;
            // force a style "change" to trigger the mutation observer
            eclContent.style.cssText += " ";

            var context = this;
            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                if (this.WUXml) {
                    this.wu.fetchXML(function (xml) {
                        context.setText(xml);
                    });
                } else {
                    this.wu.fetchText(function (text) {
                        context.setText(text);
                    });
                }
            } else if (lang.exists("ECL", params)) {
                this.setText(params.ECL ? params.ECL : "");
            } else if (lang.exists("Usergenerated", params)) {
                this.setText(params.Usergenerated);
            } else if (lang.exists("sourceURL", params)) {
                xhr(params.sourceURL, {
                    handleAs: "text"
                }).then(function (data) {
                    context.setText(data);
                });
            }
        },

        clearErrors: function (errWarnings) {
            for (var i = 0; i < this.markers.length; ++i) {
                this.markers[i].clear();
            }
            this.markers = [];
        },

        setErrors: function (errWarnings) {
            for (var i = 0; i < errWarnings.length; ++i) {
                var line = parseInt(errWarnings[i].LineNo, 10);
                this.markers.push(this.editor.doc.markText({
                    line: line - 1,
                    ch: 0
                }, {
                    line: line,
                    ch: 0
                }, {
                    className: errWarnings[i].Severity + "Line"
                }));
            }
        },

        setCursor: function (line, col) {
            this.editor.setCursor(line - 1, col - 1);
            this.editor.focus();
        },

        clearHighlightLines: function () {
            for (var i = 0; i < this.highlightLines.length; ++i) {
                this.highlightLines[i].clear();
            }
        },

        highlightLine: function (line) {
            this.highlightLines.push(this.editor.doc.markText({
                line: line - 1,
                ch: 0
            }, {
                line: line,
                ch: 0
            }, {
                className: "highlightline"
            }));
        },

        setText: function (text) {
            try {
                this.editor.text(text);
            } catch (e) {
                topic.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: "ECLSourceWidget.setText",
                    Exceptions: [
                        { Message: this.i18n.SetTextError },
                        { Message: e.toString ? (this.i18n.Details + ":\n" + e.toString()) : e }
                    ]
                });
            }
        },

        setReadOnly: function (readonly) {
            this.editor.readOnly(readonly);
        },

        getText: function () {
            return this.editor.text();
        }

    });
});
