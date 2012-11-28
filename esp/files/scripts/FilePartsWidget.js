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

    "dojo/text!../templates/FilePartsWidget.html",

    "dojox/grid/DataGrid"

],
    function (declare, array, Memory, ObjectStore,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin,
            template) {
        return declare("FilePartsWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "FilePartsWidget",
            filePartsGrid: null,

            dataStore: null,

            lastSelection: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.filePartsGrid = registry.byId(this.id + "FilePartsGrid");

                var context = this;
                this.filePartsGrid.setStructure([
                    { name: "Number", field: "Id", width: 4 },
                    { name: "IP", field: "Ip", width: 15 },
                    { name: "Size", field: "Partsize", width: 12 },
                    { name: "Actual Size", field: "ActualSize", width: 12 }
                ]);
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            resize: function (args) {
                this.inherited(arguments);
                this.filePartsGrid.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            //  Plugin wrapper  ---
            init: function (params) {
                var memory = new Memory({ data: params.fileParts });
                this.store = new ObjectStore({ objectStore: memory });
                this.filePartsGrid.setStore(this.store);
                this.filePartsGrid.setQuery({
                    Copy: "1"
                });
            }
        });
    });
