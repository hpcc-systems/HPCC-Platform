/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "hpcc/_TabContainerWidget",

    "dojo/text!../templates/GridDetailsWidget.html",

    "dijit/layout/TabContainer",
    "dijit/layout/BorderContainer",
    "dijit/Toolbar",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/layout/ContentPane"

], function (declare, lang, Memory, Observable,
                registry, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                _TabContainerWidget,
                template) {
    return declare("GridDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "GridDetailsWidget",

        gridTitle: "Change Me",
        idProperty: "Change Me",

        store: null,
        toolbar: null,
        gridTab: null,
        grid: null,
        contextMenu: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.toolbar = registry.byId(this.id + "Toolbar");
            this.gridTab = registry.byId(this.id + "_Grid");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initGrid();
            this.initContextMenu();
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        _onOpen: function (event, params) {
            var selections = this.grid.getSelected();
            var firstTab = null;
            for (var i = 0; i < selections.length; ++i) {
                var tab = this.ensurePane(selections[i], params);
                if (i == 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onRowDblClick: function (row) {
            var tab = this.ensurePane(row);
            this.selectChild(tab);
        },

        //  Implementation  ---
        initGrid: function() {
            var context = this;
            var store = new Memory({
                idProperty: this.idProperty,
                data: []
            });
            this.store = Observable(store);

            this.grid = this.createGrid(this.id + "Grid");
            this.grid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var row = context.grid.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            this.grid.on(".dgrid-row:contextmenu", function (evt) {
                if (context._onRowContextMenu) {
                }
            });
            this.grid.onSelectionChanged(function (event) {
                context._refreshActionState();
            });
            this.grid.onContentChanged(function (object, removedFrom, insertedInto) {
                context._refreshActionState();
            });
            if (!this.grid.get("noDataMessage")) {
                this.grid.set("noDataMessage", "<span class='dgridInfo'>Zero Rows...</span>");
            }
            if (!this.grid.get("loadingMessage")) {
                this.grid.set("loadingMessage", "<span class='dgridInfo'>Loading...</span>");
            }
            this.grid.startup();
        },

        appendMenuItem: function (menu, label, onClick) {
            var menuItem = new MenuItem({
                label: label,
                onClick: onClick
            });
            menu.addChild(menuItem);
            return menuItem;
        },

        appendContextMenuItem: function (label, onClick) {
            return this.appendMenuItem(this.contextMenu, label, onClick);
        },

        initContextMenu: function () {
            var context = this;
            this.contextMenu = new Menu({
                targetNodeIds: [this.id + "Grid"]
            });
            this.appendContextMenuItem("Refresh", function () {
                context._onRefresh();
            });
            this.contextMenu.addChild(new MenuSeparator());
            this.appendContextMenuItem("Open", function () {
                context._onOpen();
            });
            if (this.appendContextMenu) {
                this.appendContextMenu();
            }
            this.contextMenu.startup();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel) {
                if (!currSel.initalized) {
                    if (currSel.init && currSel.hpcc) {
                        currSel.init(currSel.hpcc.params);
                    }
                    currSel.initalized = true;
                } else if (currSel.refresh && !currSel.noRefresh) {
                    currSel.refresh(currSel.hpcc.refreshParams);
                }
            }
        },

        getDetailID: function (row, params) {
            return this.id + "_" + "Detail" + row[this.idProperty];
        },

        ensurePane: function (row, params) {
            var id = this.getDetailID(row, params);
            var retVal = registry.byId(id);
            if (!retVal) {
                retVal = this.createDetail(id, row, params);
                this.addChild(retVal);
            } else {
                lang.mixin(retVal.hpcc, {
                    refreshParams: params
                });
            }
            return retVal;
        },

        _refreshActionState: function () {
            var selection = this.grid.getSelected();
            this.refreshActionState(selection);
        },

        refreshActionState: function (selection) {
            registry.byId(this.id + "Open").set("disabled", !selection.length);
        }
    });
});
