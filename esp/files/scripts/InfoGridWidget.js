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

    "dijit/registry",
    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",

    "dojox/data/AndOrReadStore",

    "hpcc/ESPWorkunit",

    "dojo/text!../templates/InfoGridWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/form/CheckBox",
    "dojox/grid/DataGrid"
],
    function (declare, array, 
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, 
            AndOrReadStore,
            ESPWorkunit,
            template) {
        return declare("InfoGridWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "InfoGridWidget",
            borderContainer: null,
            infoGrid: null,
            errorsCheck: null,
            warningsCheck: null,
            infoCheck: null,

            dataStore: null,

            lastSelection: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            test: function (value, rowIdx, cell) {
                switch (value) {
                    case "Error":
                        cell.customClasses.push("ErrorCell");
                        break;

                    case "Warning":
                        cell.customClasses.push("WarningCell");
                        break;
                }
                return value;
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.infoGrid = registry.byId(this.id + "InfoGrid");
                this.errorsCheck = registry.byId(this.id + "Errors");
                this.warningsCheck = registry.byId(this.id + "Warnings");
                this.infoCheck = registry.byId(this.id + "Info");

                var context = this;
                this.infoGrid.setStructure([
                    { name: "Severity", field: "Severity", width: 8, formatter: context.test },
                    { name: "Source", field: "Source", width: 8 },
                    { name: "Code", field: "Code", width: 4 },
                    { name: "Message", field: "Message", width: "40" },
                    { name: "Col", field: "Column", width: 3 },
                    { name: "Line", field: "LineNo", width: 3 },
                    { name: "FileName", field: "FileName", width: "40" }
                ]);
                
                this.infoGrid.on("RowClick", function (evt) {
                    var idx = evt.rowIndex;
                    var item = this.getItem(idx);
                    var line = parseInt(this.store.getValue(item, "LineNo"), 10);
                    var col = parseInt(this.store.getValue(item, "Column"), 10);
                    context.onErrorClick(line, col);
                }, true);
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

            onErrorClick: function(line, col) {
            },

            _onErrors: function (args) {
                this.refreshFilter();
            },

            _onWarnings: function (args) {
                this.refreshFilter();
            },

            _onInfo: function (args) {
                this.refreshFilter();
            },

            //  Plugin wrapper  ---
            _onStyleRow: function (row) {
                var item = this.infoGrid.getItem(row.index);
                if (item) {
                    var severity = this.store.getValue(item, "Severity", null);
                    if (severity == "Error") {
                        row.customStyles += "background-color: red;";
                    } else if (severity == "Warning") {
                        row.customStyles += "background-color: yellow;";
                    }
                }
                this.infoGrid.focus.styleRow(row);
                this.infoGrid.edit.styleRow(row);
            },

            init: function (params) {
                if (params.onErrorClick) {
                    this.onErrorClick = params.onErrorClick;
                }
                
                this.wu = new ESPWorkunit({
                    Wuid: params.Wuid
                });

                var context = this;
                this.wu.monitor(function () {
                    context.wu.getInfo({
                        onGetWUExceptions: function (exceptions) {
                            context.loadExceptions(exceptions);
                        }
                    });
                });
            },

            refreshFilter: function (graphName) {
                var filter = "";
                if (this.errorsCheck.get("checked")) {
                    filter = "Severity: 'Error'";
                }
                if (this.warningsCheck.get("checked")) {
                    if (filter.length) {
                        filter += " OR ";
                    }
                    filter += "Severity: 'Warning'";
                }
                if (this.infoCheck.get("checked")) {
                    if (filter.length) {
                        filter += " OR ";
                    }
                    filter += "Severity: 'Info'";
                }
                this.infoGrid.setQuery({
                    complexQuery: filter
                });
            },

            getSelected: function () {
                return this.infoGrid.selection.getSelected();
            },

            setSelected: function (selItems) {
                for (var i = 0; i < this.infoGrid.rowCount; ++i) {
                    var row = this.infoGrid.getItem(i);
                    this.infoGrid.selection.setSelected(i, (row.SubGraphId && array.indexOf(selItems, row.SubGraphId) != -1));
                }
            },

            loadExceptions: function (exceptions) {
                var data = {
                    'items': exceptions
                };
                this.store = new AndOrReadStore({
                    data: data
                });
                this.infoGrid.setStore(this.store);
                this.refreshFilter();
            }
        });
    });
