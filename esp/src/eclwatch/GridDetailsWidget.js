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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
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

], function (declare, lang, i18n, nlsHPCC, Memory, Observable,
                registry, Menu, MenuItem, MenuSeparator, PopupMenuItem,
                _TabContainerWidget,
                template) {
    return declare("GridDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "GridDetailsWidget",
        i18n: nlsHPCC,

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
                if (!firstTab && tab) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onRowDblClick: function (row, params) {
            var tab = this.ensurePane(row, params);
            if (tab) {
                this.selectChild(tab);
            }
        },

        //  Implementation  ---
        setGridNoDataMessage: function(msg) {
            if (this.grid && this.grid.store === this.store) {
                this.grid.noDataMessage = "<span class='dojoxGridNoData'>" + msg + "</span>";
                if (this.grid.noDataNode) {
                    this.grid.noDataNode.innerHTML = "<span class='dojoxGridNoData'>" + msg + "</span>";
                }
            }
        },

        initGrid: function() {
            var context = this;
            var MyMemory = declare("MyMemory", [Memory], {
                idProperty: this.idProperty,
                data: [],
                setData: function (data) {
                    var retVal = this.inherited(arguments);
                    context.setGridNoDataMessage(context.i18n.noDataMessage);
                    return retVal;
                }
            });
            var store = new MyMemory();
            this.store = Observable(store);
            this.grid = this.createGrid(this.id + "Grid");
            this.setGridNoDataMessage(this.i18n.loadingMessage);

            this.grid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var row = context.grid.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            this.grid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var row = context.grid.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            this.grid.onSelectionChanged(function (event) {
                context._refreshActionState();
            });
            this.grid.startup();
        },

        getTitle: function () {
            return this.gridTitle;
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
            this.appendContextMenuItem(this.i18n.Refresh, function () {
                context._onRefresh();
            });
            this.contextMenu.addChild(new MenuSeparator());
            this.appendContextMenuItem(this.i18n.Open, function () {
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
                } else if (currSel.refresh && !currSel.noRefresh && lang.exists("hpcc.refreshParams", currSel)) {
                    currSel.refresh(currSel.hpcc.refreshParams);
                }
            }
        },

        createDetail: function (id, row, params) {
            return null;
        },

        getDetailID: function (row, params) {
            return "Detail" + row[this.idProperty];
        },

        ensurePane: function (row, params) {
            var id = this.createChildTabID(this.getDetailID(row, params));
            var retVal = registry.byId(id);
            if (!retVal) {
                retVal = this.createDetail(id, row, params);
                if (retVal) {
                    this.addChild(retVal);
                }
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
