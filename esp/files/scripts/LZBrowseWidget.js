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
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/date",
    "dojo/on",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",
    "dijit/Dialog",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "dgrid/OnDemandGrid",
    "dgrid/tree",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/_TabContainerWidget",
    "hpcc/FileSpray",
    "hpcc/ESPUtil",
    "hpcc/HexViewWidget",

    "dojo/text!../templates/LZBrowseWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/RadioButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer"

], function (declare, lang, arrayUtil, dom, domClass, domForm, date, on,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry, Dialog, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                OnDemandGrid, tree, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, FileSpray, ESPUtil, HexViewWidget,
                template) {
    return declare("LZBrowseWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "LZBrowseWidget",

        landingZonesTab: null,
        landingZonesGrid: null,

        tabMap: [],

        validateDialog: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.landingZonesTab = registry.byId(this.id + "_LandingZones");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _onHexPreview: function (event) {
            var selections = this.landingZonesGrid.getSelected();
            var firstTab = null;
            var context = this;
            arrayUtil.forEach(selections, function (item, idx) {
                var tab = context.ensurePane(context.id + "_" + item.calculatedID, item.displayName, {
                    logicalFile: item.getLogicalFile()
                });
                if (firstTab === null) {
                    firstTab = tab;
                }
            });
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onRowDblClick: function (wuid) {
            var wuTab = this.ensurePane(this.id + "_" + wuid, {
                Wuid: wuid
            });
            this.selectChild(wuTab);
        },

        _onRowContextMenu: function (item, colField, mystring) {
        },

        //  Implementation  ---
        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;
            this.initLandingZonesGrid();
            this.selectChild(this.landingZonesTab, true);
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id == this.landingZonesTab.id) {
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel.params);
                    }
                }
            }
        },

        addMenuItem: function (menu, details) {
            var menuItem = new MenuItem(details);
            menu.addChild(menuItem);
            return menuItem;
        },

        initLandingZonesGrid: function () {
            var store = new FileSpray.CreateLandingZonesStore();
            this.landingZonesGrid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox',
                        disabled: function (item) {
                            if (item.type) {
                                switch (item.type) {
                                    case "dropzone":
                                    case "folder":
                                        return true;
                                }
                            }
                            return false;
                        }
                    }),
                    displayName: tree({
                        label: "Name",
                        collapseOnRefresh: true,
                        formatter: function (name, row) {
                            var img = "../files/img/";
                            if (row.isDir === undefined) {
                                img += "server.png";
                            } else if (row.isDir) {
                                img += "folder.png";
                            } else {
                                img += "file.png";
                            }
                            return "<img src='" + img + "'/>&nbsp" + name;
                        }
                    }),
                    filesize: { label: "Size", width: 108 },
                    modifiedtime: { label: "Date", width: 180 }
                },
                getSelected: function () {
                    var retVal = [];
                    var store = FileSpray.CreateFileListStore();
                    for (key in this.selection) {
                        retVal.push(store.get(key));
                    }
                    return retVal;
                }
            }, this.id + "LandingZonesGrid");
            this.landingZonesGrid.set("noDataMessage", "<span class='dojoxGridNoData'>Zero Workunits (check filter).</span>");

            var context = this;
            on(document, ".WuidClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.landingZonesGrid.row(evt).data;
                    context._onRowDblClick(item.Wuid);
                }
            });
            this.landingZonesGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.landingZonesGrid.row(evt).data;
                    context._onRowDblClick(item.Wuid);
                }
            });
            this.landingZonesGrid.on(".dgrid-row:contextmenu", function (evt) {
                if (context._onRowContextMenu) {
                    var item = context.landingZonesGrid.row(evt).data;
                    var cell = context.landingZonesGrid.cell(evt);
                    var colField = cell.column.field;
                    var mystring = "item." + colField;
                    context._onRowContextMenu(item, colField, mystring);
                }
            });
            this.landingZonesGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
            });
            this.landingZonesGrid.onContentChanged(function (object, removedFrom, insertedInto) {
                context.refreshActionState();
            });
            this.landingZonesGrid.startup();
            this.refreshActionState();
        },

        refreshGrid: function (args) {
            this.landingZonesGrid.set("query", {
                id: "*"
            });
        },

        refreshActionState: function () {
            var selection = this.landingZonesGrid.getSelected();
            var hasSelection = selection.length;
            registry.byId(this.id + "HexPreview").set("disabled", !hasSelection);
            /*
            var hasSelection = false;
            var hasProtected = false;
            var hasNotProtected = false;
            var hasFailed = false;
            var hasNotFailed = false;
            var hasCompleted = false;
            var hasNotCompleted = false;
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
                if (selection[i] && selection[i].Protected !== null) {
                    if (selection[i].Protected != "0") {
                        hasProtected = true;
                    } else {
                        hasNotProtected = true;
                    }
                }
                if (selection[i] && selection[i].StateID !== null) {
                    if (selection[i].StateID == "4") {
                        hasFailed = true;
                    } else {
                        hasNotFailed = true;
                    }
                    if (WsWorkunits.isComplete(selection[i].StateID)) {
                        hasCompleted = true;
                    } else {
                        hasNotCompleted = true;
                    }
                }
            }

            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Abort").set("disabled", !hasNotCompleted);
            registry.byId(this.id + "SetToFailed").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Protect").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Unprotect").set("disabled", !hasProtected);
            registry.byId(this.id + "Reschedule").set("disabled", true);    //TODO
            registry.byId(this.id + "Deschedule").set("disabled", true);    //TODO

            this.menuProtect.set("disabled", !hasNotProtected);
            this.menuUnprotect.set("disabled", !hasProtected);

            this.refreshFilterState();
            */
        },

        ensurePane: function (id, title, params) {
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new HexViewWidget({
                    id: id,
                    title: title,
                    closable: true,
                    params: params
                });
                this.addChild(retVal);
            }
            return retVal;
        }

    });
});
