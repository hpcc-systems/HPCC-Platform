define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/_base/array",

    "dijit/registry",

    "hpcc/_Widget",
    "@hpcc-js/comms",

    "dojo/text!../templates/HexViewWidget.html",

    "hpcc/ECLSourceWidget",
    "dijit/form/NumberSpinner",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/CheckBox"
],
    function (declare, nlsHPCCMod, arrayUtil,
        registry,
        _Widget, hpccComms,
        template) {

        var nlsHPCC = nlsHPCCMod.default;
        return declare("HexViewWidget", [_Widget], {
            templateString: template,
            baseClass: "HexViewWidget",
            i18n: nlsHPCC,

            borderContainer: null,
            widthField: null,
            hexView: null,
            wu: null,
            unknownChar: String.fromCharCode(8226),
            lineLength: 16,
            showEbcdic: false,
            bufferLength: 16 * 1024,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.widthField = registry.byId(this.id + "Width");
                this.hexView = registry.byId(this.id + "HexView");
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

            _onWidthChange: function (event) {
                if (this.lineLength !== event) {
                    this.lineLength = event;
                    this.displayHex();
                }
            },

            _onEbcdicChange: function (event) {
                if (this.showEbcdic !== event) {
                    this.showEbcdic = event;
                    this.displayHex();
                }
            },

            init: function (params) {
                if (this.inherited(arguments))
                    return;

                this.logicalFile = params.logicalFile;

                this.hexView.init({
                    sourceMode: ""
                });
                var context = this;
                this.wu = hpccComms.Workunit.submit({ baseUrl: "" }, "hthor", context.getQuery()).then(function (wu) {
                    wu.on("changed", function () {
                        if (!wu.isComplete()) {
                            context.hexView.setText("..." + wu.State + "...");
                        }
                    });
                    return wu.watchUntilComplete();
                }).then(function (wu) {
                    return wu.fetchECLExceptions().then(function () { return wu; });
                }).then(function (wu) {
                    var exceptions = wu.Exceptions && wu.Exceptions.ECLException ? wu.Exceptions.ECLException : [];
                    if (exceptions.length) {
                        var msg = "";
                        arrayUtil.forEach(exceptions, function (exception) {
                            if (exception.Severity === "Error") {
                                if (msg) {
                                    msg += "\n";
                                }
                                msg += exception.Message;
                            }
                        });
                        if (msg) {
                            dojo.publish("hpcc/brToaster", {
                                Severity: "Error",
                                Source: "HexViewWidget.remoteRead",
                                Exceptions: [{ Source: context.wu.Wuid, Message: msg }]
                            });
                        }
                    }
                    return wu.fetchResults().then(function (results) {
                        return results.length ? results[0].fetchRows() : [];
                    }).then(function (rows) {
                        context.cachedResponse = rows;
                        context.displayHex();
                        return wu;
                    });
                }).then(function (wu) {
                    return wu.delete();
                });
            },

            displayHex: function () {
                var context = this;
                var formatRow = function (row, strRow, hexRow, length) {
                    if (row) {
                        for (var i = row.length; i < 4; ++i) {
                            row = "0" + row;
                        }
                        for (var i = strRow.length; i < length; ++i) {
                            strRow += context.unknownChar;
                        }
                        return row + "  " + strRow + "  " + hexRow + "\n";
                    }
                    return "";
                };

                var doc = "";
                var row = "";
                var hexRow = "";
                var strRow = "";
                var charIdx = 0;
                arrayUtil.some(this.cachedResponse, function (item, idx) {
                    if (idx >= context.lineLength * 100) {
                        return false;
                    }
                    if (idx % context.lineLength === 0) {
                        doc += formatRow(row, strRow, hexRow, context.lineLength);
                        row = "";
                        hexRow = "";
                        strRow = "";
                        charIdx = 0;
                        row = idx.toString(16);
                    }
                    if (charIdx % 8 === 0) {
                        if (hexRow)
                            hexRow += " ";
                    }
                    if (hexRow)
                        hexRow += " ";
                    hexRow += item["char"];

                    if (context.showEbcdic) {
                        strRow += context.isCharPrintable(item.estr1) ? item.estr1 : context.unknownChar;
                    } else {
                        strRow += context.isCharPrintable(item.str1) ? item.str1 : context.unknownChar;
                    }
                    ++charIdx;
                });
                doc += formatRow(row, strRow, hexRow, context.lineLength);
                this.hexView.setText(doc);
            },

            getQuery: function () {
                return "data_layout := record\n" +
                    "    data1 char;\n" +
                    "end;\n" +
                    "data_dataset := dataset('" + this.logicalFile + "', data_layout, thor);\n" +
                    "analysis_layout := record\n" +
                    "    data1 char;\n" +
                    "    string1 str1;\n" +
                    "    ebcdic string1 estr1;\n" +
                    "end;\n" +
                    "analysis_layout calcAnalysis(data_layout l) := transform\n" +
                    "    self.char := l.char;\n" +
                    "    self.str1 := transfer(l.char, string1);\n" +
                    "    self.estr1 := transfer(l.char, string1);\n" +
                    "end;\n" +
                    "analysis_dataset := project(data_dataset, calcAnalysis(left));\n" +
                    "choosen(analysis_dataset, " + this.bufferLength + ");\n";
            }
        });
    });
