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
    "dojo/dom-class",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",
    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",

    "dojox/data/AndOrReadStore",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/ESPUtil",
    "hpcc/ESPWorkunit",

    "dojo/text!../templates/InfoGridWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/form/CheckBox"
],
    function (declare, lang, arrayUtil, domClass, Memory, Observable,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, 
            AndOrReadStore,
            OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry,
            ESPUtil, ESPWorkunit,
            template) {
        return declare("InfoGridWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "InfoGridWidget",
            borderContainer: null,
            infoGrid: null,
            errorsCheck: null,
            warningsCheck: null,
            infoCheck: null,

            dataStore: null,

            lastSelection: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            test: function (value, rowIdx, cell) {
                switch (value) {
                    case "Error":
                        cell.customClasses.push("ErrorCell");
                        break;

                    case "Warning":
                        cell.customClasses.push("WarningCell");
                        break;
                }
                return value;
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.errorsCheck = registry.byId(this.id + "Errors");
                this.warningsCheck = registry.byId(this.id + "Warnings");
                this.infoCheck = registry.byId(this.id + "Info");
            },

            startup: function (args) {
                this.inherited(arguments);
                var context = this;
                var store = new Memory({
                    idProperty: "id",
                    data: []
                });
                this.infoStore = Observable(store);

                this.infoGrid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                    selectionMode: "single",
                    columns: {
                        Severity: {
                            label: "Severity", field: "", width: 72, sortable: false,
                            renderCell: function (object, value, node, options) {
                                switch (value) {
                                    case "Error":
                                        domClass.add(node, "ErrorCell");
                                        break;

                                    case "Warning":
                                        domClass.add(node, "WarningCell");
                                        break;
                                }
                                node.innerText = value;
                            }
                        }, 
                        Source: { label: "Source", field: "", width: 72, sortable: false },
                        Code: { label: "Code", field: "", width: 45, sortable: false },
                        Message: { label: "Message", field: "", sortable: false },
                        Column: { label: "Col", field: "", width: 36, sortable: false },
                        LineNo: { label: "Line", field: "", width: 36, sortable: false },
                        FileName: { label: "FileName", field: "", width: 360, sortable: false }
                    },
                    store: this.infoStore
                }, this.id + "InfoGrid");

                this.infoGrid.on(".dgrid-row:click", function (evt) {
                    var item = context.infoGrid.row(evt).data;
                    var line = parseInt(item.LineNo, 10);
                    var col = parseInt(item.Column, 10);
                    context.onErrorClick(line, col);
                });
                this.infoGrid.startup();
            },

            resize: function (args) {
                this.inherited(arguments);
                this.borderContainer.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            onErrorClick: function(line, col) {
            },

            _onErrors: function (args) {
                this.refreshFilter();
            },

            _onWarnings: function (args) {
                this.refreshFilter();
            },

            _onInfo: function (args) {
                this.refreshFilter();
            },

            //  Plugin wrapper  ---
            _onStyleRow: function (row) {
                var item = this.infoGrid.getItem(row.index);
                if (item) {
                    var severity = this.store.getValue(item, "Severity", null);
                    if (severity == "Error") {
                        row.customStyles += "background-color: red;";
                    } else if (severity == "Warning") {
                        row.customStyles += "background-color: yellow;";
                    }
                }
                this.infoGrid.focus.styleRow(row);
                this.infoGrid.edit.styleRow(row);
            },

            init: function (params) {
                if (this.initalized)
                    return;
                this.initalized = true;

                if (params.onErrorClick) {
                    this.onErrorClick = params.onErrorClick;
                }
                
                this.wu = ESPWorkunit.Get(params.Wuid);

                var context = this;
                this.wu.monitor(function () {
                    context.wu.getInfo({
                        onGetWUExceptions: function (exceptions) {
                            context.loadExceptions(exceptions);
                        }
                    });
                });
            },

            refreshFilter: function (graphName) {
                var data = [];
                var filter = "";
                var errorChecked = this.errorsCheck.get("checked");
                var warningChecked = this.warningsCheck.get("checked");
                var infoChecked = this.infoCheck.get("checked");
                arrayUtil.forEach(this.infoData, function (item, idx) {
                    lang.mixin(item, {
                        id: idx
                    });
                    switch(item.Severity) {
                        case "Error":
                            if (errorChecked) {
                                data.push(item);
                            }
                            break;
                        case "Warning":
                            if (warningChecked) {
                                data.push(item);
                            }
                            break;
                        case "Info":
                            if (infoChecked) {
                                data.push(item);
                            }
                            break;
                    }
                });
                this.infoStore.setData(data);
                this.infoGrid.refresh();
            },

            getSelected: function () {
                return this.infoGrid.selection.getSelected();
            },

            setSelected: function (selItems) {
                for (var i = 0; i < this.infoGrid.rowCount; ++i) {
                    var row = this.infoGrid.getItem(i);
                    this.infoGrid.selection.setSelected(i, (row.SubGraphId && arrayUtil.indexOf(selItems, row.SubGraphId) != -1));
                }
            },

            loadExceptions: function (exceptions) {
                exceptions.sort(function (l, r) {
                    if (l.Severity === r.Severity) {
                        return 0;
                    } else if (l.Severity === "Error") {
                        return -1;
                    } else if (r.Severity === "Error") {
                        return 1;
                    } else if (l.Severity === "Warning") {
                        return -1;
                    } else if (r.Severity === "Warning") {
                        return 1;
                    }
                    return l.Severity > r.Severity;
                });
                this.infoData = exceptions;
                this.refreshFilter();
            }
        });
    });
