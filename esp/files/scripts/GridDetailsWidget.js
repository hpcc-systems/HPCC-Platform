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
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",

    "hpcc/_TabContainerWidget",

    "dojo/text!../templates/GridDetailsWidget.html",

    "dijit/layout/TabContainer",
    "dijit/layout/BorderContainer",
    "dijit/Toolbar",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/layout/ContentPane"

], function (declare, Memory, Observable,
                registry,
                _TabContainerWidget,
                template) {
    return declare("GridDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "GridDetailsWidget",

        gridTitle: "Change Me",
        idProperty: "Change Me",

        store: null,
        grid: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.gridTab = registry.byId(this.id + "_Grid");
        },

        startup: function (args) {
            this.inherited(arguments);
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
            this.grid.startup();
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

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.hpcc) {
                    currSel.init(currSel.hpcc.params);
                }
                currSel.initalized = true;
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
