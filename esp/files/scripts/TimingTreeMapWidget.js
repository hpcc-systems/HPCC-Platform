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
    "dojo/store/Memory",

    "dijit/registry",
    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/ContentPane",

    "dojox/treemap/TreeMap",

    "hpcc/ESPWorkunit",

    "dojo/text!../templates/TimingTreeMapWidget.html"
],
    function (declare, Memory,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, ContentPane,
            TreeMap, 
            ESPWorkunit,
            template) {
        return declare("TimingTreeMapWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "TimingTreeMapWidget",
            contentPane: null,
            treeMap: null,

            dataStore: null,

            lastSelection: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.contentPane = registry.byId(this.id + "ContentPane");
                this.treeMap = registry.byId(this.id + "TreeMap");

                var context = this;
                this.treeMap.on("change", function (e) {
                    context.lastSelection = e.newValue;
                });
                this.treeMap.on("click", function (evt) {
                    context.onClick(context.lastSelection);
                });
                this.treeMap.on("dblclick", function (evt) {
                    context.onDblClick(context.lastSelection);
                });
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            resize: function (args) {
                this.inherited(arguments);
                this.contentPane.resize();
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
                var context = this;
                if (params.Wuid) {
                    this.wu = new ESPWorkunit({
                        wuid: params.Wuid
                    });
                    this.wu.fetchTimers(function (timers) {
                        context.timers = timers;
                        context.loadTimers(timers, "*");
                    });
                }
            },

            setQuery: function(query) {
                this.loadTimers(this.timers, query);
            },

            getSelected: function () {
                return [{
                    SubGraphId: this.lastSelection.subGraphId
                }];
            },

            setSelected: function (selItems) {
                //  TODO:  Not sure this possible...
            },

            loadTimers: function (timers, query) {
                this.largestValue = 0;
                var timerData = [];
                if (timers) {
                    for (var i = 0; i < timers.length; ++i) {
                        if (timers[i].GraphName && timers[i].SubGraphId && (query == "*" || query == timers[i].GraphName)) {
                            var value = timers[i].Seconds;
                            timerData.push({
                                graphName: timers[i].GraphName,
                                subGraphId: timers[i].SubGraphId,
                                label: timers[i].Name,
                                value: value
                            });
                            if (this.largestValue < value * 1000) {
                                this.largestValue = value * 1000;
                            }
                        }
                    }
                }
                var dataStore = new Memory({
                    idProperty: "sequenceNumber",
                    data: timerData
                });

                var context = this;
                this.treeMap.set("colorFunc", function (item) {
                    var redness = 255 * (item.value * 1000 / context.largestValue);
                    return {
                        r: 255,
                        g: 255 - redness,
                        b: 255 - redness
                    };
                });
                this.treeMap.set("areaAttr", "value");
                this.treeMap.set("tooltipFunc", function (item) {
                    return item.label + " " + item.value;
                });
                this.treeMap.set("groupAttrs", ["graphName"]);

                this.treeMap.set("store", dataStore);
            }
        });
    });
