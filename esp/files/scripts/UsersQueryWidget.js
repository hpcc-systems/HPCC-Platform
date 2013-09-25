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
    "dojo/dom",
    "dojo/dom-form",
    "dojo/request/iframe",
    "dojo/_base/array",
    "dojo/on",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/_TabContainerWidget",
    "hpcc/WsAccess",
    "hpcc/ESPUtil",
    "hpcc/UserDetailsWidget",

    "dojo/text!../templates/UsersQueryWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/form/TextBox",
    "dijit/Dialog",

    "dojox/layout/TableContainer",
    "dojox/form/PasswordValidator"
], function (declare, lang, dom, domForm, iframe, arrayUtil, on,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry, Menu, MenuItem, MenuSeparator,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                _TabContainerWidget, WsAccess, ESPUtil, UserDetailsWidget,
                template) {
    return declare("UsersQueryWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "UsersQueryWidget",

        borderContainer: null,
        usersTab: null,
        usersGrid: null,

        initalized: false,
        loaded: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.usersTab = registry.byId(this.id + "_Users");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initContextMenu();
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize(); //is needed
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;
            this.initUsersGrid();
            this.selectChild(this.usersTab, true);
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id == this.usersTab.id) {
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel.params);
                    }
                }
            }
        },

        initContextMenu: function() {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "UsersGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: "Open",
                onClick: function(args){context._onOpen();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Add",
                onClick: function(args){context._onAdd();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Delete",
                onClick: function(args){context._onDelete();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Edit",
                onClick: function(args){context._onEdit();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Change Password",
                onClick: function(args){context._onPassword();}
            }));
        },

        initUsersGrid: function () {
            var store = new WsAccess.CreateUsersStore();
            this.usersGrid = declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                selectionMode: "single",
                allowSelectAll: false,
                deselectOnRefresh: false,
                store: store,
                columns: {
                    check: selector({
                        width: 1,
                        label: " "
                    },"checkbox"),
                    username: {
                        width: 27,
                        label: "Username"
                    },
                    fullname: {
                        width: 27,
                        label: "Full Name"
                    },
                    passwordexpiration: {
                        width: 27,
                        label: "Password Expiration"
                    },
                },
            },
            this.id + "UsersGrid");
            var context = this;
            this.usersGrid.set("noDataMessage", "<span class='dojoxGridNoData'>No User Information Available.</span>");
            on(document, ".WuidClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.usersGrid.row(evt).data;
                    context._onRowDblClick(item.username,item.fullname,item.passwordexpiration);
                }
            });
            this.usersGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var item = context.usersGrid.row(evt).data;
                    context._onRowDblClick(item.username,item.fullname,item.passwordexpiration);
                }
            });
            this.usersGrid.on(".dgrid-row:contextmenu", function (evt) {
                if (context._onRowContextMenu) {
                    var item = context.usersGrid.row(evt).data;
                    var cell = context.usersGrid.cell(evt);
                    var colField = cell.column.field;
                    var mystring = "item." + colField;
                    context._onRowContextMenu(item, colField, mystring);
                }
            });
            this.usersGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
            });
            this.usersGrid.onContentChanged(function (event) {
                context.refreshActionState();
            });
            this.usersGrid.startup();
            this.refreshActionState();
        },

        //  Hitched actions  ---
        _onRefresh:function(){
            this.refreshGrid();
        },

        _onOpen: function (event) {
            var selections = this.usersGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePane(this.id + "_" + selections[i].username, {
                    Username: selections[i].username,
                    Fullname: selections[i].fullname,
                    Passwordexpiration: selections[i].passwordexpiration
                });
                if (i == 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onCancelDialog: function (nodeName) {
            registry.byId(this.id + "EditDialog").hide();
            registry.byId(this.id + "PasswordDialog").hide();
            registry.byId(this.id + "AddUserDialog").hide();
        },

        _onEdit: function (event) {
            var context = this;
            var selections = this.usersGrid.getSelected();
            registry.byId(this.id + "EditDialog").show();
            arrayUtil.forEach(selections, function (item, idx) {
                context.updateInput("UsersUsername", null, item.username);
            });
             WsAccess.UserInfoEditInput({
               request: domForm.toObject(this.id + "EditForm")
             }).then(function (response) {
                if(lang.exists("UserInfoEditInputResponse.firstname", response)){
                    context.updateInput("UsersFirstName", null, response.UserInfoEditInputResponse.firstname);
                }
                if(lang.exists("UserInfoEditInputResponse.lastname", response)){
                    context.updateInput("UsersLastName", null, response.UserInfoEditInputResponse.lastname);
                }
            });
        },

        _onAdd: function (event){
            registry.byId(this.id + "AddUserDialog").show();
        },

        _onDelete: function (params){
            var selections = this.usersGrid.getSelected();
            if (confirm('Delete selected user(s)?')) {
                var context = this;
                for (var i = selections.length - 1; i >= 0; --i) {
                    WsAccess.UserAction({
                        request:{
                            action: "delete",
                            ActionType: "delete",
                            usernames: selections[i].username
                        }
                    }).then(function (response) {
                    if(lang.exists("UserActionResponse.retcode", response)){
                        dojo.publish("hpcc/brToaster", {
                            message: "<p>" + response.UserResetPassResponse.retmsg + "</p>",
                            type: "error",
                            duration: -1
                        });
                    }
                    });
                }
            }
            setTimeout(this.refreshGrid(), 2000);
        },

        _onPassword: function (event) {
            var context = this;
            var selections = this.usersGrid.getSelected();
            registry.byId(this.id + "PasswordDialog").show();
             arrayUtil.forEach(selections, function (item, idx) {
                context.updateInput("PasswordUsername", null, item.username);
            });
        },

        _onEditSubmit: function (event) {
            var context = this;
            var selections = this.usersGrid.getSelected();
             arrayUtil.forEach(selections, function (item, idx) {
                context.updateInput("UsersUsername", null, item.username);
            });
             WsAccess.UserInfoEdit({
               request: domForm.toObject(this.id + "EditForm")
            }).then(function (response) {
                if(lang.exists("UserInfoEditResponse.retmsg" == "", response)){
                    dojo.publish("hpcc/brToaster", {
                            message: "<p>User password updated successfully</p>",
                            type: "error",
                            duration: -1
                    });
                }
            });
            registry.byId(this.id + "EditDialog").hide();
            setTimeout(this.refreshGrid(), 2000);
        },

         _onAddSubmit: function (event) {
            var context = this;
            var selections = this.usersGrid.getSelected();
             WsAccess.AddUser({
                request: domForm.toObject(this.id + "AddUserForm")
            }).then(function (response) {
                if(lang.exists("AddUserResponse.retcode", response)){
                    dojo.publish("hpcc/brToaster", {
                        message: "<p>" + response.AddUserResponse.retmsg + "</p>",
                        type: "error",
                        duration: -1
                    });
                }
            });
            registry.byId(this.id + "AddUserDialog").hide();
            setTimeout(this.refreshGrid(), 2000);
        },

        _onPasswordSubmit: function (event) {
            var context = this;
            var selections = this.usersGrid.getSelected();
             arrayUtil.forEach(selections, function (item, idx) {
                context.updateInput("PasswordUsername", null, item.username);
            });
            WsAccess.UserResetPass({
               request: domForm.toObject(this.id + "PasswordForm")
            }).then(function (response) {
                if(lang.exists("UserResetPassResponse.retcode", response)){
                    dojo.publish("hpcc/brToaster", {
                            message: "<p>" + response.UserResetPassResponse.retmsg + "</p>",
                            type: "error",
                            duration: -1
                    });
                }
            });
            registry.byId(this.id + "PasswordDialog").hide();
            setTimeout(this.refreshGrid(), 2000);
        },

        _onRowDblClick: function (username,fullname, passwordexpiration) {
            var wuTab = this.ensurePane(this.id + "_" + username, {
                Username: username,
                Fullname: fullname,
                Passwordexpiration: passwordexpiration
            });
            this.selectChild(wuTab);
        },

        refreshGrid: function (args) {
            this.usersGrid.set("query",{
               id: "*"
            });
        },

        refreshActionState: function () {
            var selection = this.usersGrid.getSelected();
            var hasSelection = selection.length;
            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Password").set("disabled", !hasSelection);
            registry.byId(this.id + "DeleteUsers").set("disabled", !hasSelection);
            registry.byId(this.id + "EditUsers").set("disabled", !hasSelection);
        },

        updateInput: function (name, oldValue, newValue) {
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            } else {
                var domElem = dom.byId(this.id + name);
                if (domElem) {
                    switch (domElem.tagName) {
                        case "SPAN":
                        case "DIV":
                            domAttr.set(this.id + name, "innerHTML", newValue);
                            break;
                        case "INPUT":
                        case "TEXTAREA":
                            domAttr.set(this.id + name, "value", newValue);
                            break;
                        default:
                            alert(domElem.tagName);
                    }
                }
            }
        },

        ensurePane: function (id, params) {
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new UserDetailsWidget({
                    id: id,
                    title: params.Username,
                    closable: true,
                    params: params
                });
                this.addChild(retVal, 1);
            }
            return retVal;
        }

    });
});
