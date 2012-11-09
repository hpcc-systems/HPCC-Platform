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

    "dijit/registry",
    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",

    "hpcc/TimingGridWidget",
    "hpcc/TimingTreeMapWidget",

    "dojo/text!../templates/TimingPageWidget.html"
],
    function (declare,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer,
            TimingGridWidget, TimingTreeMapWidget,
            template) {
        return declare("TimingPageWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "TimingPageWidget",
            borderContainer: null,
            timingGrid: null,
            timingTreeMap: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.timingGrid = registry.byId(this.id + "Grid");
                this.timingTreeMap = registry.byId(this.id + "TreeMap");
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
            init: function (params) {
                this.timingGrid.init(params);
                this.timingTreeMap.init(params);
            }

        });
    });
