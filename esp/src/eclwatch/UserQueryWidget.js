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
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-form",
    "dojo/on",

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

    "hpcc/_TabContainerWidget",
    "hpcc/ws_access",
    "hpcc/ESPUtil",
    "hpcc/UserDetailsWidget",
    "hpcc/GroupDetailsWidget",

    "dojo/text!../templates/UserQueryWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/ValidationTextBox",
    "dijit/ToolbarSeparator",
    "dijit/form/TextBox",
    "dijit/Dialog",

    "dojox/form/PasswordValidator",

    "hpcc/TableContainer"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domForm, on,
                registry, Menu, MenuItem, MenuSeparator,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                _TabContainerWidget, WsAccess, ESPUtil, UserDetailsWidget, GroupDetailsWidget,
                template) {
    return declare("UserQueryWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "UserQueryWidget",
        i18n: nlsHPCC,

        usersTab: null,
        usersGrid: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.addGroupForm = registry.byId(this.id + "AddGroupForm");
            this.groupsTab = registry.byId(this.id + "_Groups");
            this.addUserForm = registry.byId(this.id + "AddUserForm");
            this.usersTab = registry.byId(this.id + "_Users");
        },

        //  Hitched actions  ---
        //  Groups  ---
        _onRefreshGroups: function () {
            this.refreshGroupsGrid();
        },

        _onEditGroup: function (event) {
            var selections = this.groupsGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensureGroupPane("Group" + selections[i].name, {
                    Name: selections[i].name
                });
                if (i == 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onDeleteGroup: function (params) {
            if (confirm(this.i18n.DeleteSelectedGroups)) {
                var selections = this.groupsGrid.getSelected();
                var request = {
                    ActionType: "delete"
                };
                arrayUtil.forEach(selections, function (item, idx) {
                    request["groupnames_i" + idx] = item.name;
                }, this);

                var context = this;
                WsAccess.GroupAction({
                    request: request
                }).then(function (response) {
                    context.refreshGroupsGrid(true);
                });
            }
        },

        _onGroupsRowDblClick: function (name) {
            var groupTab = this.ensureGroupPane("Group" + name, {
                Name: name
            });
            this.selectChild(groupTab);
        },

        _onAddGroupSubmit: function () {
            if (this.addGroupForm.validate()) {
                var context = this;
                var request = domForm.toObject(this.addGroupForm.id);
                WsAccess.GroupAdd({
                    request: request
                }).then(function (response) {
                    context.refreshGroupsGrid();
                });
                registry.byId(this.id + "AddGroupsDropDown").closeDropDown();
            }
        },

        //  Users  ---
        _onRefreshUsers: function () {
            this.refreshUsersGrid();
        },

        _onEditUser: function (event) {
            var selections = this.usersGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensureUserPane(selections[i].username, {
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

        _onDeleteUser: function (params) {
            var selections = this.usersGrid.getSelected();
            if (confirm(this.i18n.DeleteSelectedUsers)) {
                request = {
                    ActionType: "delete"
                };
                arrayUtil.forEach(selections, function (item, idx) {
                    request["usernames_i" + idx] = item.username;
                }, this);
                var context = this;
                WsAccess.UserAction({
                    request: request
                }).then(function (response) {
                    context.refreshUsersGrid(true);
                });
            }
        },

        _onUsersRowDblClick: function (username, fullname, passwordexpiration) {
            var userTab = this.ensureUserPane(username, {
                Username: username,
                Fullname: fullname,
                Passwordexpiration: passwordexpiration
            });
            this.selectChild(userTab);
        },

        _onSubmitAddUserDialog: function (event) {
            if (this.addUserForm.validate()) {
                var context = this;
                var request = domForm.toObject(this.addUserForm.id);
                lang.mixin(request, {
                    password1: request.password,
                    password2: request.password
                })
                WsAccess.AddUser({
                    request: request
                }).then(function (response) {
                    context.refreshUsersGrid();
                });
                registry.byId(this.id + "AddUsersDropDown").closeDropDown();
            }
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.initGroupsGrid();
            this.initUsersGrid();
            this.refreshActionState();
        },

        //  Groups  ---
        initGroupsGrid: function () {
            this.initGroupsContextMenu();
            var store = WsAccess.CreateGroupsStore();
            this.groupsGrid = declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: store,
                columns: {
                    check: selector({
                        width: 27,
                        label: " "
                    }, "checkbox"),
                    name: {
                        label: this.i18n.GroupName
                    }
                }
            }, this.id + "GroupsGrid");
            var context = this;
            on(document, ".WuidClick:click", function (evt) {
                if (context._onGroupsRowDblClick) {
                    var item = context.groupsGrid.row(evt).data;
                    context._onGroupsRowDblClick(item.name);
                }
            });
            this.groupsGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onGroupsRowDblClick) {
                    var item = context.groupsGrid.row(evt).data;
                    context._onGroupsRowDblClick(item.name);
                }
            });
            this.groupsGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
            });
            this.groupsGrid.onContentChanged(function (event) {
                context.refreshActionState();
            });
            this.groupsGrid.startup();
        },

        initGroupsContextMenu: function () {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "GroupsGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: this.i18n.Add,
                onClick: function (args) {
                    registry.byId(context.id + "AddGroupsDropDown").openDropDown();
                }
            }));
            pMenu.addChild(new MenuItem({
                label: this.i18n.Edit,
                onClick: function (args) { context._onEditGroup(); }
            }));
            pMenu.addChild(new MenuItem({
                label: this.i18n.Delete,
                onClick: function (args) { context._onDeleteGroup(); }
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: this.i18n.Refresh,
                onClick: function (args) { context._onRefreshGroups(); }
            }));
        },

        refreshGroupsGrid: function (clearSelection) {
            this.groupsGrid.set("query", {
                id: "*"
            });
            if (clearSelection) {
                this.groupsGrid.clearSelection();
            }
        },

        ensureGroupPane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                retVal = new GroupDetailsWidget({
                    id: id,
                    title: params.Name,
                    closable: true,
                    params: params
                });
                this.addChild(retVal, 2);
            }
            return retVal;
        },
        //  Users  ---
        initUsersGrid: function () {
            this.initUsersContextMenu();
            var store = WsAccess.CreateUsersStore();
            this.usersGrid = declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: store,
                columns: {
                    check: selector({
                        width: 27,
                        label: " "
                    },"checkbox"),
                    username: {
                        width: 180,
                        label: this.i18n.Username
                    },
                    fullname: {
                        label: this.i18n.FullName
                    },
                    passwordexpiration: {
                        width: 180,
                        label: this.i18n.PasswordExpiration
                    }
                }
            }, this.id + "UsersGrid");
            var context = this;
            on(document, ".WuidClick:click", function (evt) {
                if (context._onUsersRowDblClick) {
                    var item = context.usersGrid.row(evt).data;
                    context._onUsersRowDblClick(item.username,item.fullname,item.passwordexpiration);
                }
            });
            this.usersGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onUsersRowDblClick) {
                    var item = context.usersGrid.row(evt).data;
                    context._onUsersRowDblClick(item.username,item.fullname,item.passwordexpiration);
                }
            });
            this.usersGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
            });
            this.usersGrid.onContentChanged(function (event) {
                context.refreshActionState();
            });
            this.usersGrid.startup();
        },

        initUsersContextMenu: function () {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "UsersGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: this.i18n.Add,
                onClick: function (args) {
                    registry.byId(context.id + "AddUsersDropDown").openDropDown();
                }
            }));
            pMenu.addChild(new MenuItem({
                label: this.i18n.Edit,
                onClick: function (args) { context._onEditUser(); }
            }));
            pMenu.addChild(new MenuItem({
                label: this.i18n.Delete,
                onClick: function (args) { context._onDeleteUser(); }
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: this.i18n.Refresh,
                onClick: function (args) { context._onRefreshUsers(); }
            }));
        },

        ensureUserPane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                retVal = new UserDetailsWidget({
                    id: id,
                    title: params.Username,
                    closable: true,
                    params: params
                });
                this.addChild(retVal, 2);
            }
            return retVal;
        },

        refreshUsersGrid: function (clearSelection) {
            this.usersGrid.set("query",{
               id: "*"
            });
            if (clearSelection) {
                this.usersGrid.clearSelection();
            }
        },

        //  ---  ---
        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.groupsTab.id) {
                } else if (currSel.id === this.usersTab.id) {
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel.params);
                    }
                }
            }
        },

        refreshActionState: function () {
            var userSelection = this.usersGrid.getSelected();
            var hasUserSelection = userSelection.length;
            registry.byId(this.id + "EditUsers").set("disabled", !hasUserSelection);
            registry.byId(this.id + "DeleteUsers").set("disabled", !hasUserSelection);

            var groupSelection = this.groupsGrid.getSelected();
            var hasGroupSelection = groupSelection.length;
            registry.byId(this.id + "EditGroups").set("disabled", !hasGroupSelection);
            registry.byId(this.id + "DeleteGroups").set("disabled", !hasGroupSelection);
        }
    });
});
