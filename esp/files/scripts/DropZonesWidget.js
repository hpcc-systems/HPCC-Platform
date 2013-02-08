/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/dom",
    "dojo/on",
    "dojo/data/ObjectStore",
    "dojo/date",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/form/ComboBox",

    "dijit/PopupMenuItem",
    "dijit/Dialog",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/Pagination",
    "dojox/grid/enhanced/plugins/IndirectSelection",
    "dojox/form/Uploader",

    "hpcc/FileSpray",

    "dojo/text!../templates/DropZonesWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/RadioButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/form/DateTextBox"

], function (declare, dom, on, ObjectStore, date, Menu, MenuItem, MenuSeparator, ComboBox, PopupMenuItem, Dialog,
                _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry, EnhancedGrid, Pagination, IndirectSelection, Uploader,
                FileSpray,
                template) {
    return declare("DropZonesWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "DropZonesWidget",
        borderContainer: null,
        dropzonesGrid: null,

        buildRendering: function (args) {
            this.inherited(arguments);
        },
        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.dropzonesGrid = registry.byId(this.id + "DropZonesGrid");
        },
        query: function (query, options) {
        },
        startup: function (args) {
            this.inherited(arguments);
            this.refreshActionState();
            this.initDropZonesGrid();
        },
        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },
        layout: function (args) {
            this.inherited(arguments);
        },
        destroy: function (args) {
            this.inherited(arguments);
        },
        //  Hitched actions  ---
        _onUpload: function (event) {
        },
        _onDelete: function (event) {
            if (confirm('Delete selected workunits?')) {
                var context = this;
                FileSpray.GetDropZones(this.dropzonesGrid.selection.getSelected(), "Delete", {
                    load: function (response) {
                        context.dropzonesGrid.rowSelectCell.toggleAllSelection(false);
                        context.refreshGrid(response);
                    }
                });
            }
        },
        getValues: function () {
        },
        //  Implementation  ---
        init: function (params) {
            if (this.initalized) {
                return;
            }
            this.initalized = true;
            this.dropzonesGrid.setQuery({
                NetAddress: params.netAddress
            });
        },
        initDropZonesGrid: function() {
            this.dropzonesGrid.setStructure([
                { name: "Name", field: "name", width: "25" },
                { name: "Size", field: "filesize", width: "25" },
                { name: "Date", field: "modifiedtime", width: "25" }
            ]);

            var store = new FileSpray.DropZoneFiles();
            var objStore = new ObjectStore({ objectStore: store });
            this.dropzonesGrid.setStore(objStore);
            //this.dropzonesGrid.noDataMessage = "<span class='dojoxGridNoData'>No Results.</span>";
            this.dropzonesGrid.startup();
        },

        refreshGrid: function (args) {
        },

        refreshActionState: function () {
        }
    });
});