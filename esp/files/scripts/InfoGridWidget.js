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
    "dojo/store/Memory",
    "dojo/data/ObjectStore",

    "dijit/registry",
    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",

    "dojox/grid/DataGrid",

    "hpcc/ESPWorkunit",

    "dojo/text!../templates/InfoGridWidget.html"
],
    function (declare, array, Memory, ObjectStore,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin,
            DataGrid,
            ESPWorkunit,
            template) {
        return declare("InfoGridWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "InfoGridWidget",
            infoGrid: null,

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
                this.infoGrid = registry.byId(this.id + "InfoGrid");

                var context = this;
                this.infoGrid.setStructure([
                    { name: "Severity", field: "Severity", width: 8, formatter: context.test },
                    { name: "Source", field: "Source", width: 10 },
                    { name: "Code", field: "Code", width: 4 },
                    { name: "Message", field: "Message", width: "100%" }
                ]);

                this.infoGrid.on("RowClick", function (evt) {
                });

                this.infoGrid.on("RowDblClick", function (evt) {
                });
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            resize: function (args) {
                this.inherited(arguments);
                this.infoGrid.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
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

            onClick: function (items) {
            },

            onDblClick: function (item) {
            },

            init: function (params) {
                this.wu = new ESPWorkunit({
                    wuid: params.Wuid
                });

                var context = this;
                this.wu.monitor(function () {
                    context.wu.getInfo({
                        onGetExceptions: function (exceptions) {
                            context.loadExceptions(exceptions);
                        }
                    });
                });
            },

            setQuery: function (graphName) {
                if (!graphName || graphName == "*") {
                    this.infoGrid.setQuery({
                        GraphName: "*"
                    });
                } else {
                    this.infoGrid.setQuery({
                        GraphName: graphName,
                        HasSubGraphId: true
                    });
                }
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
                var memory = new Memory({ data: exceptions });
                this.store = new ObjectStore({ objectStore: memory });
                this.infoGrid.setStore(this.store);
                this.setQuery("*");
            }
        });
    });
