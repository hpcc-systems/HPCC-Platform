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
    "dojo/_base/array",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/request/iframe",

    "dijit/registry",

    "hpcc/_Widget",
    "hpcc/ESPWorkunit",
    "hpcc/ECLSourceWidget",

    "dojo/text!../templates/HexViewWidget.html",

    "dijit/form/NumberSpinner",
    "dijit/form/CheckBox"
],
    function (declare, lang, i18n, nlsHPCC, arrayUtil, Memory, Observable, iframe,
            registry,
            _Widget, ESPWorkunit, ECLSourceWidget,
            template) {
        return declare("HexViewWidget", [_Widget], {
            templateString: template,
            baseClass: "HexViewWidget",
            i18n: nlsHPCC,

            borderContainer: null,
            widthField: null,
            hexView: null,
            wu: null,
            unknownChar:  String.fromCharCode(8226),
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
                if (this.lineLength != event) {
                    this.lineLength = event;
                    this.displayHex();
                }
            },

            _onEbcdicChange: function (event) {
                if (this.showEbcdic != event) {
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
                this.wu = ESPWorkunit.Create({
                    onCreate: function () {
                        context.wu.update({
                            QueryText: context.getQuery()
                        });
                        context.watchWU();
                    },
                    onUpdate: function () {
                        context.wu.submit();
                    },
                    onSubmit: function () {
                    }
                });
            },

            watchWU: function () {
                if (this.watchHandle) {
                    this.watchHandle.unwatch();
                }
                var context = this;
                this.watchHandle = this.wu.watch(function (name, oldValue, newValue) {
                    switch (name) {
                        case "hasCompleted": 
                            if (newValue === true) {
                                context.wu.fetchResults(function (results) {
                                    context.cachedResponse = "";
                                    arrayUtil.forEach(results, function (result, idx) {
                                        var store = result.getStore();
                                        var result = store.query({
                                        }, {
                                            start: 0,
                                            count: context.bufferLength
                                        }).then(function (response) {
                                            context.watchHandle.unwatch();
                                            context.cachedResponse = response;
                                            context.displayHex();
                                            context.wu.doDelete();
                                        });
                                    });
                                });
                            }
                            break;
                        case "State":
                            context.hexView.setText("..." + (context.wu.isComplete() ? context.i18n.fetchingresults : newValue) + "...");
                            break;
                    }
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
                return  "data_layout := record\n" + 
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
