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

    "dojo/text!../templates/FilePartsWidget.html"

],
    function (declare, array, Memory, Observable,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin,
            OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
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
            },

            startup: function (args) {
                this.inherited(arguments);
                var store = new Memory({
                    idProperty: "Id",
                    data: []
                });
                this.filePartsStore = Observable(store);

                this.filePartsGrid = new declare([OnDemandGrid, Keyboard, ColumnResizer, DijitRegistry])({
                    allowSelectAll: true,
                    columns: {
                        Id: { label: "Part", width: 40 },
                        Ip: { label: "IP" },
                        Partsize: { label: "Size", width: 120 },
                        ActualSize: { label: "Actual Size", width: 120 }
                    },
                    store: this.filePartsStore
                }, this.id + "FilePartsGrid");
                this.filePartsGrid.startup();
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
                if (this.initalized)
                    return;
                this.initalized = true;

                this.filePartsStore.setData(params.fileParts);
                this.filePartsGrid.set("query", {
                    Copy: "1"
                });
            }
        });
    });
