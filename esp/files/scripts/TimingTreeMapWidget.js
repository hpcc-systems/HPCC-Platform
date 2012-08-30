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
require([
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

    "dojo/text!./templates/TimingTreeMapWidget.html"
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

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.timingContentPane = registry.byId(this.id + "TimingContentPane");
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
            initTreeMap: function () {
            },

            setWuid: function (wuid) {
            	this.wuid = wuid;
            	var context = this;
            	if (wuid) {
            		this.wu = new ESPWorkunit({
            			wuid: wuid
            		});
            		this.wu.fetchTimers(function (timers) {
            			context.largestValue = 0;
            			var timerData = [];
            			for (var i = 0; i < timers.length; ++i) {
            				if (timers[i].GraphName) {
            					var value = timers[i].Value;
            					timerData.push({
            						name: timers[i].Name,
            						graphName: timers[i].GraphName,
            						value: value
            					});
            					if (context.largestValue < value * 1000) {
            						context.largestValue = value * 1000;
            					}
            				}
            			}
            			var dataStore = new Memory({
            				idProperty: "name",
            				data: timerData
            			});

            			context.timingContentPane.set("colorFunc", function (item) {
            				var redness = 255 * (item.value * 1000 / context.largestValue);
            				return {
            					r: 255,
            					g: 255 - redness,
            					b: 255 - redness
            				};
            			});
            			context.timingContentPane.set("areaAttr", "value");
            			context.timingContentPane.set("tooltipFunc", function (item) {
            				return item.name + " " + item.value;
            			});
            			context.timingContentPane.set("groupAttrs", ["graphName"]);

            			context.timingContentPane.set("store", dataStore);
            		});
            	}
        	}
        });
    });
