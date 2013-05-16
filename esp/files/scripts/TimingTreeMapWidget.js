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
    "dojo/store/Memory",

    "dijit/registry",
    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",

    "dojox/treemap/TreeMap",

    "hpcc/ESPWorkunit",

    "dojo/text!../templates/TimingTreeMapWidget.html"
],
    function (declare, lang, Memory,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, 
            TreeMap,
            ESPWorkunit,
            template) {
        return declare("TimingTreeMapWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "TimingTreeMapWidget",
            treeMap: null,

            store: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.treeMap = registry.byId(this.id + "TreeMap");

                var context = this;
                this.treeMap.on("click", function (evt) {
                    context.onClick(context.treeMap.selectedItems);
                });
                this.treeMap.on("dblclick", function (evt) {
                    context.onDblClick(context.treeMap.selectedItem);
                });
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            resize: function (args) {
                this.inherited(arguments);
                this.treeMap._dataChanged = true;
                this.treeMap.resize(args);
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            //  Plugin wrapper  ---
            onClick: function (value) {
            },

            onDblClick: function (value) {
            },

            init: function (params) {
                if (this.initalized)
                    return;
                this.initalized = true;

                this.defaultQuery = "*";
                if (params.query) {
                    this.defaultQuery = params.query;
                }

                var context = this;
                if (params.Wuid) {
                    this.wu = ESPWorkunit.Get(params.Wuid);

                    this.wu.fetchTimers(function (timers) {
                        context.timers = timers;
                        context.loadTimers(timers, context.defaultQuery);
                    });
                }
            },

            setQuery: function (query) {
                this.loadTimers(this.timers, query);
            },

            getSelected: function () {
                return this.treeMap.selectedItems;
            },

            setSelectedAsGlobalID: function (selItems) {
                if (this.store) {
                    var selectedItems = [];
                    for (var i = 0; i < selItems.length; ++i) {
                        var item = this.store.get(selItems[i]);
                        if (item) {
                            selectedItems.push(item);
                        }
                    }
                    this.treeMap.set("selectedItems", selectedItems);
                }
            },

            setSelected: function (selItems) {
                if (this.store) {
                    var selectedItems = [];
                    for (var i = 0; i < selItems.length; ++i) {
                        var item = this.store.get(selItems[i].SubGraphId);
                        if (item) {
                            selectedItems.push(item);
                        }
                    }
                    this.treeMap.set("selectedItems", selectedItems);
                }
            },

            loadTimers: function (timers, query) {
                this.largestValue = 0;
                var timerData = [];
                if (timers) {
                    for (var i = 0; i < timers.length; ++i) {
                        if (timers[i].GraphName && timers[i].SubGraphId && (query == "*" || query == timers[i].GraphName)) {
                            timerData.push(timers[i]);
                            if (this.largestValue < timers[i].Seconds * 1000) {
                                this.largestValue = timers[i].Seconds * 1000;
                            }
                        }
                    }
                }
                this.store = new Memory({
                    idProperty: "SubGraphId",
                    data: timerData
                });

                var context = this;
                this.treeMap.set("store", this.store);
                this.treeMap.set("areaAttr", "Seconds");
                this.treeMap.set("colorFunc", function (item) {
                    var redness = Math.floor(255 * (item.Seconds * 1000 / context.largestValue));
                    return {
                        r: 255,
                        g: 255 - redness,
                        b: 255 - redness
                    };
                });
                this.treeMap.set("groupAttrs", ["GraphName"]);
                this.treeMap.set("labelAttr", "Name");
                this.treeMap.set("tooltipFunc", function (item) {
                    return item.Name + " " + item.Seconds;
                });
            }
        });
    });
