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
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",
    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/ESPUtil",
    "hpcc/ESPWorkunit",

    "dojo/text!../templates/TimingGridWidget.html"
],
    function (declare, lang, arrayUtil, Memory, Observable,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin,
            OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
            ESPUtil, ESPWorkunit,
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
            },

            startup: function (args) {
                this.inherited(arguments);
                var store = new Memory({
                    idProperty: "id",
                    data: []
                });
                this.timingStore = Observable(store);

                this.timingGrid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                    columns: {
                        id: { label: "##", width: 45 },
                        Name: { label: "Component" },
                        Seconds: { label: "Time (Seconds)", width: 54 }
                    },
                    store: this.timingStore
                }, this.id + "TimingGrid");

                var context = this;
                this.timingGrid.on(".dgrid-row:click", function (evt) {
                    var item = context.timingGrid.row(evt).data;
                    context.onClick(item);
                });
                this.timingGrid.on(".dgrid-row:dblclick", function (evt) {
                    var item = context.timingGrid.row(evt).data;
                    context.onDblClick(item);
                });
                this.timingGrid.startup();
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
                if (this.initalized)
                    return;
                this.initalized = true;

                this.defaultQuery = "*";
                if (params.query) {
                    this.defaultQuery = params.query;
                }
                this.wu = ESPWorkunit.Get(params.Wuid);

                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 == 0) {
                        context.wu.getInfo({
                            onGetTimers: function (timers) {
                                context.loadTimings(timers);
                            }
                        });
                    }
                });
            },

            setQuery: function (graphName) {
                if (!graphName || graphName == "*") {
                    this.timingGrid.refresh();
                } else {
                    this.timingGrid.set("query", {
                        GraphName: graphName,
                        HasSubGraphId: true
                    });
                }
            },

            getSelected: function () {
                return this.timingGrid.getSelected();
            },

            setSelectedAsGlobalID: function (selItems) {
                var selectedItems = [];
                arrayUtil.forEach(this.timingStore.data, function (item, idx) {
                    if (item.SubGraphId) {
                        if (item.SubGraphId && arrayUtil.indexOf(selItems, item.SubGraphId) >= 0) {
                            selectedItems.push(item);
                        }
                    }
                });
                this.setSelected(selectedItems);
            },

            setSelected: function (selItems) {
                this.timingGrid.setSelected(selItems);
            },

            loadTimings: function (timers) {
                arrayUtil.forEach(timers, function (item, idx) {
                    lang.mixin(item, {
                        id: idx
                    });
                });
                this.timingStore.setData(timers);
                this.setQuery(this.defaultQuery);
            }
        });
    });
