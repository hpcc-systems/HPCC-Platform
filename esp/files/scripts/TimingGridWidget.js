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

    "dojo/text!../templates/TimingGridWidget.html"
],
    function (declare, array, Memory, ObjectStore,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin,
            DataGrid,
            ESPWorkunit,
            template) {
        return declare("TimingGridWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "TimingGridWidget",
            timingGrid: null,

            dataStore: null,

            lastSelection: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.timingGrid = registry.byId(this.id + "TimingGrid");

                var context = this;
                this.timingGrid.on("RowClick", function (evt) {
                    var items = context.timingGrid.selection.getSelected();
                    context.onClick(items);
                });

                this.timingGrid.on("RowDblClick", function (evt) {
                    var item = context.timingGrid.getItem(evt.rowIndex);
                    context.onDblClick(item);
                });
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            resize: function (args) {
                this.inherited(arguments);
                this.timingGrid.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            //  Plugin wrapper  ---
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
                        onGetTimers: function (timers) {
                            context.loadTimings(timers);
                        }
                    });
                });
            },

            setQuery: function (graphName) {
                if (!graphName || graphName == "*") {
                    this.timingGrid.setQuery({
                        GraphName: "*"
                    });
                } else {
                    this.timingGrid.setQuery({
                        GraphName: graphName,
                        HasSubGraphId: true
                    });
                }
            },

            getSelected: function () {
                return this.timingGrid.selection.getSelected();
            },

            setSelected: function (selItems) {
                for (var i = 0; i < this.timingGrid.rowCount; ++i) {
                    var row = this.timingGrid.getItem(i);
                    this.timingGrid.selection.setSelected(i, (row.SubGraphId && array.indexOf(selItems, row.SubGraphId) != -1));
                }
            },

            loadTimings: function (timers) {
                var store = new Memory({ data: timers });
                var dataStore = new ObjectStore({ objectStore: store });
                this.timingGrid.setStore(dataStore);
                this.setQuery("*");
            }
        });
    });
