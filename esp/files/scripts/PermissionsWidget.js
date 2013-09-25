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
    "dojo/_base/array",
    "dojo/on",

    "dijit/form/Button",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",
    "dijit/form/DropDownButton",
    "dijit/DropDownMenu",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/WsAccess",
    "hpcc/ESPUtil",

], function (declare, lang, arrayUtil, on,
                Button, Menu, MenuItem, MenuSeparator, PopupMenuItem, DropDownButton, DropDownMenu,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, WsAccess, ESPUtil) {
    return declare("PermissionsWidget", [GridDetailsWidget], {

        gridTitle: "Permissions",
        idProperty: "name",
        user:null,

        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;
            this.refreshGrid();
            this._refreshActionState();
            this.initContextMenu();
            
            if (params.username) {
                this.user = params.username;
                //this.summaryWidget.set("title", "User Summary");
                //dom.byId(this.id + "User").innerHTML = this.user;
                this.grid.set("query", {
                    username: this.user
                 });
            }
        },

        getTitle: function () {
            return "Permissions";
        },

        addMenuItem: function (menu, details) {
            var menuItem = new MenuItem(details);
            menu.addChild(menuItem);
            return menuItem;
        },

        initContextMenu:function(){
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.grid.id ]
            });
            this.menuAdd = this.addMenuItem(pMenu, {
                label: "Open",
                onClick: function () { context._onOpen(); }
            });
             /*this.menuAdd = this.addMenuItem(pMenu, {
                label: "Add",
                onClick: function () { context._onAdd(); }
            });
            this.menuDelete = this.addMenuItem(pMenu, {
                label: "Delete",
                onClick: function () { context._onDelete(); }
            });*/
        },

        createGrid: function (domID) {
            var context = this;
            /*this.add = new Button({
                label: "Add",
                onClick: function (event) {
                    context._onAdd();
                }
            }, this.id + "ContainerNode");

            this.remove = new Button({
                label: "Delete",
                onClick: function (event) {
                    context._onDelete();
                }
            }, this.id + "DeleteNode");*/

            var retVal = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: false,
                deselectOnRefresh: false,
                store: WsAccess.PermissionsStore(),
                columns: {
                    col1: selector({ width: 27, selectorType: 'checkbox', label: " " }),
                    name: {
                        width: 27,
                        label: "Name"
                    },
                    basedn: {
                        width: 27,
                        label: "BaseDN"
                    }
                }
            }, domID);

            var context = this;
            on(document, "." + this.id + "WuidClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        /*createDetail: function (id, row, params) {
            if (row.Server === "DFUserver") {
                return new DFUWUDetailsWidget.fixCircularDependency({
                    id: id,
                    title: row.ID,
                    closable: true,
                    hpcc: {
                        params: {
                            Wuid: row.ID
                        }
                    }
                });
            } 
            return new MemberOfWidget({
                id: id,
                title: row.Wuid,
                closable: true,
                hpcc: {
                    params: {
                        Wuid: row.Wuid
                    }
                }
            });
        },*/

        buildFilter: function(){
            var menu = new DropDownMenu({ style: "display: none;"});
            var menuItem1 = new MenuItem({
                label: "Save",
                onClick: function(){ alert('save'); }
            });
            menu.addChild(menuItem1);

            var menuItem2 = new MenuItem({
                label: "Cut",
                onClick: function(){ alert('cut'); }
            });
            menu.addChild(menuItem2);

            var button = new DropDownButton({
                label: "Filter",
                name: "programmatic2",
                dropDown: menu,
                id: "progButton"
            }, this.id + "FilterNode");
        },

        _onAdd: function (event) {
                var context = this;
                var selection = this.grid.getSelected();
                WsWorkunits.WUAction(selection, "Deschedule", {
                    load: function (response) {
                        context.refreshGrid(response);
                    }
                });
        },

        _onDelete: function (params){
            var selections = this.grid.getSelected();
            if (confirm('Delete this user from selected group?')) {
                var context = this;
                for (var i = selections.length - 1; i >= 0; --i) {
                    WsAccess.UserGroupEdit({
                        request:{
                            action: "delete",
                            groupnames: selections[i].name,
                            username: this.user
                        }
                    }).then(function (response) {
                    if(lang.exists("UserGroupEditResponse.retmsg", response)){
                        dojo.publish("hpcc/brToaster", {
                            message: "<p>" + response.UserGroupEditResponse.retmsg + "</p>",
                            type: "error",
                            duration: -1
                        });
                    }
                    });
                }
            }
            setTimeout(this.refreshGrid(), 2000);
        },


        /*_onDelete: function (event) {
            if (confirm('Delete this user from selected group?')) {
                var context = this;
                var selection = this.grid.getSelected();
                WsWorkunits.WUAction(selection, "Deschedule", {
                    load: function (response) {
                        context.refreshGrid(response);
                    }
                });
            }
        },
*/
        refreshGrid: function (args) {
            var context = this;
            this.grid.set("query",{
               username: this.user
            });
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);
            //this.add.set("disabled", !selection.length);
        }
    });
});