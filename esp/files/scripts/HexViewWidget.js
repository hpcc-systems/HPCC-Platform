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

    "dojo/text!../templates/HexViewWidget.html"
],
    function (declare, array, lang, Memory, Observable, iframe,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin,
            ESPWorkunit, ECLSourceWidget,
            template) {
        return declare("HexViewWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "HexViewWidget",

            borderContainer: null,
            hexView: null,
            wu: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
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

            init: function (params) {
                if (this.initalized)
                    return;
                this.initalized = true;

                this.logicalFile = params.logicalFile;

                this.hexView.init({
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
                    if (name === "hasCompleted" && newValue === true) {
                        context.wu.fetchResults(function (results) {
                            if (results.length) {
                                var store = results[0].getStore();
                                var result = store.query({
                                }, {
                                    start: 0,
                                    count: 1024
                                }).then(function (response) {
                                    var doc = "";
                                    var row = "";
                                    array.forEach(response, function (item, idx) {
                                        if (idx % 16 === 0) {
                                            if (row) {
                                                doc += row + "\n";
                                            }
                                            row = "";
                                            row = idx.toString(16);
                                            for (var i = row.length; i < 8; ++i) {
                                                row = "0" + row;
                                            }
                                            row += "  ";
                                        }
                                    });
                                    if (row) {
                                        doc += row + "\n";
                                    }
                                    context.hexView.setText(doc);
                                });
                            }
                        });
                    }
                });
                this.wu.monitor();
            },

            getQuery: function () {
                return "sample_layout := record\n" + 
                    " string1 s1;\n" +
                    "end;\n" +
                    "d := dataset('" + this.logicalFile + "', sample_layout, thor);\n" +
                    "choosen(d, 1024);\n"
            }
        });
    });
