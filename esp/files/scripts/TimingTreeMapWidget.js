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
    "dojo/aspect",
    "dojo/has",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-class",
    "dojo/store/Memory",
    "dojo/_base/Color",

    "dijit/registry",
    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",

    "dojox/treemap/TreeMap",
    "dojox/color/MeanColorModel",

    "hpcc/ESPWorkunit",

    "dojo/text!../templates/TimingTreeMapWidget.html"
],
    function (declare, aspect, has, dom, domConstruct, domClass, Memory, Color,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, ContentPane,
            TreeMap, MeanColorModel,
            ESPWorkunit,
            template) {
        return declare("TimingTreeMapWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "TimingTreeMapWidget",
            borderContainer: null,
            timingContentPane: null,

            dataStore: null,

            lastSelection: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.timingContentPane = registry.byId(this.id + "TimingContentPane");

                var context = this;
                this.timingContentPane.on("change", function (e) {
                    context.lastSelection = e.newValue;
                });
                this.timingContentPane.on("click", function (evt) {
                    context.onClick(context.lastSelection);
                });
                this.timingContentPane.on("dblclick", function (evt) {
                    context.onDblClick(context.lastSelection);
                });
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

            //  Plugin wrapper  ---
            onClick: function (value) {
            },

            onDblClick: function (value) {
            },

            initTreeMap: function () {
            },

            init: function (params) {
                var context = this;
                if (params.Wuid) {
                    this.wu = new ESPWorkunit({
                        wuid: params.Wuid
                    });
                    this.wu.fetchTimers(function (timers) {
                        context.loadTimers(timers, "*");
                    });
                }
            },

            loadTimers: function (timers, query) {
                this.largestValue = 0;
                var timerData = [];
                for (var i = 0; i < timers.length; ++i) {
                    if (timers[i].GraphName && (query == "*" || query == timers[i].GraphName)) {
                        var value = timers[i].Value;
                        timerData.push({
                            graphName: timers[i].GraphName,
                            subGraphId: timers[i].SubGraphId,
                            name: timers[i].Name,
                            value: value
                        });
                        if (this.largestValue < value * 1000) {
                            this.largestValue = value * 1000;
                        }
                    }
                }
                var dataStore = new Memory({
                    idProperty: "name",
                    data: timerData
                });

                var context = this;
                this.timingContentPane.set("colorFunc", function (item) {
                    var redness = 255 * (item.value * 1000 / context.largestValue);
                    return {
                        r: 255,
                        g: 255 - redness,
                        b: 255 - redness
                    };
                });
                this.timingContentPane.set("areaAttr", "value");
                this.timingContentPane.set("tooltipFunc", function (item) {
                    return item.name + " " + item.value;
                });
                this.timingContentPane.set("groupAttrs", ["graphName"]);

                this.timingContentPane.set("store", dataStore);
            },

            select: function (graphID, subGraphID) {
                //this.timingContentPane.setItemSelected(this.timingContentPane.store.data[2]);

            }

        });
    });
