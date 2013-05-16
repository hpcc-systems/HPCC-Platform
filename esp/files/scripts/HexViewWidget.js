/*##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/array",
    "dojo/_base/lang",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/request/iframe",

    "dijit/registry",
    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",

    "hpcc/ESPWorkunit",
    "hpcc/ECLSourceWidget",

    "dojo/text!../templates/HexViewWidget.html",

    "dijit/form/NumberSpinner"
],
    function (declare, arrayUtil, lang, Memory, Observable, iframe,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin,
            ESPWorkunit, ECLSourceWidget,
            template) {
        return declare("HexViewWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "HexViewWidget",

            borderContainer: null,
            widthField: null,
            hexView: null,
            wu: null,
            unknownChar:  String.fromCharCode(0xb7),
            lineLength: 16,
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

            init: function (params) {
                if (this.initalized)
                    return;
                this.initalized = true;

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
                        context.wu.submit("hthor");
                    },
                    onSubmit: function () {
                    }
                });
            },

            watchWU: function () {
                if (this.watching) {
                    this.watching.unwatch();
                }
                var context = this;
                this.watching = this.wu.watch(function (name, oldValue, newValue) {
                    switch (name) {
                        case "hasCompleted": 
                            if (newValue === true) {
                                context.wu.fetchResults(function (results) {
                                    context.cachedResponse = [];
                                    arrayUtil.forEach(results, function (result, idx) {
                                        var store = result.getStore();
                                        var result = store.query({
                                        }, {
                                            start: 0,
                                            count: context.bufferLength
                                        }).then(function (response) {
                                            context.cachedResponse[idx] = response;
                                            if (context.cachedResponse.length === 1)
                                                context.displayHex();
                                        });
                                    });
                                });
                            }
                            break;
                        case "State":
                            if (!context.wu.isComplete()) {
                                context.hexView.setText("..." + newValue + "...");
                            }
                            break;
                        default:
                            break;
                    }
                });
                this.wu.monitor();
            },

            displayHex: function() {
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
                arrayUtil.some(this.cachedResponse[0], function (item, idx) {
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
                    hexRow += item.char;
                    //strRow += context.cachedResponse[1][idx].char;
                    var charCode = parseInt(item.char, 16);
                    if (charCode < 32 || charCode >= 127) {
                        strRow += context.unknownChar;
                    } else {
                        var char = String.fromCharCode(charCode);
                        if (char.length === 0) {
                            strRow += context.unknownChar;
                        } else {
                            strRow += String.fromCharCode(charCode);
                        }
                    }
                    ++charIdx;
                });
                doc += formatRow(row, strRow, hexRow, context.lineLength);
                this.hexView.setText(doc);
            },

            getQuery: function () {
                return "data_layout := record\n" + 
                    " data1 char;\n" +
                    "end;\n" +
                    "data_dataset := dataset('" + this.logicalFile + "', data_layout, thor);\n" +
                    "choosen(data_dataset, " + this.bufferLength + ");\n" +
                    "ascii_layout := record\n" +
                    " string1 char;\n" +
                    "end;\n" +
                    "ascii_dataset := dataset('" + this.logicalFile + "', ascii_layout, thor);\n" +
                    "//choosen(ascii_dataset, " + this.bufferLength + ");\n" +
                    "ebcdic_layout := record\n" +
                    " ebcdic string1 char;\n" +
                    "end;\n" +
                    "ebcdic_dataset := dataset('" + this.logicalFile + "', ebcdic_layout, thor);\n" +
                    "//choosen(ebcdic_dataset, " + this.bufferLength + ");\n"
            }
        });
    });
