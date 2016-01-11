/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/promise/all",

    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/form/Select",

    "dgrid/tree",
    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "hpcc/ws_access",
    "hpcc/ESPBase",
    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/UserDetailsWidget",
    "hpcc/GroupDetailsWidget",
    "hpcc/FilterDropDownWidget",

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

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domForm, on, all,
                registry, Menu, MenuItem, MenuSeparator, Select,
                tree, selector,
                _TabContainerWidget, WsAccess, ESPBase, ESPUtil, ESPRequest, UserDetailsWidget, GroupDetailsWidget, FilterDropDownWidget,
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
            this.addPermissionForm = registry.byId(this.id + "AddPermissionForm");
            this.addPermissionType = registry.byId(this.id + "AddPermissionType");
            this.permissionsTab = registry.byId(this.id + "_Permissions");
            this.filter = registry.byId(this.id + "Filter");
        },

        //  Hitched actions  ---

        _onClearPermissionsCache: function () {
            if (confirm(this.i18n.ClearPermissionsCacheConfirm)) {
                WsAccess.ClearPermissionsCache();
            }
        },

        _onEnableScopeScans: function () {
            if (confirm(this.i18n.EnableScopeScansConfirm)) {
                WsAccess.EnableScopeScans();
            }
        },

        _onDisableScopeScans: function () {
            if (confirm(this.i18n.DisableScopeScanConfirm)) {
                WsAccess.DisableScopeScans();
            }
        },

        getRow: function (rtitle) {
            for (var i = 0; i < this.permissionsStore.data.length; ++i) {
                if (this.permissionsStore.data[i].rtitle === rtitle) {
                    return this.permissionsStore.data[i];
                }
            }
            return null;
        },

        _onCodeGenerator: function () {
            var row = this.getRow("Module");
            if (row) {
                WsAccess.Resources({
                    request: {
                        basedn: row.basedn,
                        rtype: "service",
                        rtitle: "CodeGenerator Permission",
                        prefix: "codegenerator.",
                        action: "Code Generator"
                    }
                });
            }
        },

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
            var selection = this.groupsGrid.getSelected();
            var list = this.arrayToList(selection, "name");
            if (confirm(this.i18n.DeleteSelectedGroups + "\n" + list)) {
                var request = {
                    ActionType: "delete"
                };
                arrayUtil.forEach(selection, function (item, idx) {
                    request["groupnames_i" + idx] = item.name;
                }, this);

                var context = this;
                WsAccess.GroupAction({
                    request: request
                }).then(function (response) {
                    context.refreshGroupsGrid(true);
                    return response;
                });
            }
        },

        _onExportGroup: function (params) {
            var selections = this.groupsGrid.getSelected();
            var groupnames = "";
            arrayUtil.forEach(selections, function (item, idx) {
                if (groupnames.length) {
                    groupnames += "&";
                }
                groupnames += "groupnames_i" + idx + "=" + item.name;
            }, this);
            var base = new ESPBase();
            window.open(base.getBaseURL("ws_access") + "/UserAccountExport?" + groupnames);
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
                    if (lang.exists("GroupAddResponse.retcode", response) && response.GroupAddResponse.retcode === 0) {
                        context.refreshGroupsGrid();
                        context._onGroupsRowDblClick(response.GroupAddResponse.groupname);
                    }
                    return response;
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
            var selection = this.usersGrid.getSelected();
            var list = this.arrayToList(selection, "username");
            if (confirm(this.i18n.DeleteSelectedUsers + "\n" + list)) {
                request = {
                    ActionType: "delete"
                };
                arrayUtil.forEach(selection, function (item, idx) {
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

        _onExportUser: function (params) {
            var selections = this.usersGrid.getSelected();
            var usernames = "";
            arrayUtil.forEach(selections, function (item, idx) {
                if (usernames.length) {
                    usernames += "&";
                }
                usernames += "usernames_i" + idx + "=" + item.username;
            }, this);
            var base = new ESPBase();
            window.open(base.getBaseURL("ws_access") + "/UserAccountExport?" + usernames);
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
                    if (lang.exists("AddUserResponse.retcode", response) && response.AddUserResponse.retcode === 0) {
                        context.refreshUsersGrid();
                        context._onUsersRowDblClick(request.username);
                    }
                    return response;
                });
                registry.byId(this.id + "AddUsersDropDown").closeDropDown();
            }
        },

        //  Groups  ---
        _onRefreshPermissions: function () {
            this.refreshPermissionsGrid();
        },

        _onAddPermissionSubmit: function (event) {
            var selRow = this.addPermissionType.__hpcc_data[this.addPermissionType.get("value")];
            if (selRow) {
                var request = lang.mixin(selRow, domForm.toObject(this.id + "AddPermissionForm"));
                var context = this;
                WsAccess.ResourceAdd({
                    request: request
                }).then(function (response) {
                    context.refreshPermissionsGrid();
                    return response;
                });
                registry.byId(this.id + "AddPermissionsDropDown").closeDropDown();
            }
        },

        _onDeletePermission: function (params) {
            var selection = this.permissionsGrid.getSelected();
            var list = this.arrayToList(selection, "DisplayName");
            if (confirm(this.i18n.DeleteSelectedPermissions + "\n" + list)) {
                var deleteRequests = {};
                arrayUtil.forEach(selection, function (item, idx) {
                    if (!deleteRequests[item.__hpcc_id]) {
                        deleteRequests[item.__hpcc_id] = {
                            action: "Delete",
                            basedn: item.__hpcc_parent.basedn,
                            rtype: item.__hpcc_parent.rtype,
                            rtitle: item.__hpcc_parent.rtitle
                        }
                    }
                    deleteRequests[item.__hpcc_id]["names_i" + idx] = item.name;
                }, this);
                var context = this;
                var requests = [];
                for (var key in deleteRequests) {
                    requests.push(WsAccess.ResourceDelete({
                        request: deleteRequests[key]
                    }));
                }
                all(requests).then(function () {
                    context.refreshPermissionsGrid(true);
                });
            }
        },

        _onPermissionsRowDblClick: function (username, fullname, passwordexpiration) {
        },

        _onSubmitAddPermissionDialog: function (event) {
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.initGroupsGrid();
            this.initUsersGrid();
            this.initPermissionsGrid();

            var context = this;
            this.usersGrid.on("dgrid-refresh-complete", function (evt) {
                if (context.usersStore.ldapTooMany) {
                    context.setVisible(context.id + "LDAPWarning", true);
                    context.filter.open();
                } else {
                    context.setVisible(context.id + "LDAPWarning", false);
                }
            });
            this.filter.on("clear", function (evt) {
                context.refreshUsersGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshUsersGrid();
            });

            this.refreshActionState();
        },

        //  Groups  ---
        initGroupsGrid: function () {
            this.initGroupsContextMenu();
            var store = WsAccess.CreateGroupsStore(null, true);
            this.groupsGrid = declare([ESPUtil.Grid(false, true)])({
                store: store,
                columns: {
                    check: selector({
                        width: 27,
                        label: " "
                    }, "checkbox"),
                    name: {
                        label: this.i18n.GroupName
                    },
                    groupOwner: {
                        label: this.i18n.ManagedBy
                    },
                    groupDesc: {
                        label: this.i18n.Description
                    }
                }
            }, this.id + "GroupsGrid");
            var context = this;
            this.groupsGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onGroupsRowDblClick) {
                    var item = context.groupsGrid.row(evt).data;
                    context._onGroupsRowDblClick(item.name);
                }
            });
            this.groupsGrid.onSelectionChanged(function (event) {
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
                    iconClass: 'iconPeople',
                    closable: true,
                    params: params
                });
                this.addChild(retVal, 3);
            }
            return retVal;
        },

        //  Users  ---
        initUsersGrid: function () {
            this.initUsersContextMenu();
            this.usersStore = WsAccess.CreateUsersStore(null, true);
            this.usersGrid = declare([ESPUtil.Grid(false, true)])({
                store: this.usersStore,
                query: this.filter.toObject(),
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
            this.usersGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onUsersRowDblClick) {
                    var item = context.usersGrid.row(evt).data;
                    context._onUsersRowDblClick(item.username,item.fullname,item.passwordexpiration);
                }
            });
            this.usersGrid.onSelectionChanged(function (event) {
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
                    iconClass: 'iconPerson',
                    closable: true,
                    params: params
                });
                this.addChild(retVal, 3);
            }
            return retVal;
        },

        refreshUsersGrid: function (clearSelection) {
            this.usersGrid.set("query", this.filter.toObject());
            if (clearSelection) {
                this.usersGrid.clearSelection();
            }
        },

        //  Permissions  ---
        initPermissionsGrid: function () {
            WsAccess.Permissions().then(lang.hitch(this, function (response) {
                var options = [];
                var optionMap = {};
                if (lang.exists("BasednsResponse.Basedns.Basedn", response)) {
                    arrayUtil.forEach(response.BasednsResponse.Basedns.Basedn, function (item, idx) {
                        options.push({
                            label: item.name,
                            value: item.basedn
                        });
                        optionMap[item.basedn] = item;
                    }, this);
                }
                this.addPermissionType.set("options", options);
                if (options.length) {
                    this.addPermissionType.set("value", options[0].value);
                }
                this.addPermissionType.set("__hpcc_data", optionMap);
                return response;
            }));

            this.initPermissionsContextMenu();
            this.permissionsStore = WsAccess.CreatePermissionsStore();
            this.permissionsGrid = declare([ESPUtil.Grid(false, true)])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: this.permissionsStore,
                columns: {
                    check: selector({
                        width: 27,
                        disabled: function (row) {
                            return row.children ? true : false;
                        }
                    }, "checkbox"),
                    DisplayName: tree({
                        width: 360,
                        sortable: false,
                        label: this.i18n.Name
                    }),
                    basedn: {
                        sortable: false,
                        label: "basedn"
                    }
                }
            }, this.id + "PermissionsGrid");
            var context = this;
            this.permissionsGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onPermissionsRowDblClick) {
                    var item = context.permissionsGrid.row(evt).data;
                    context._onPermissionsRowDblClick(item.username, item.fullname, item.passwordexpiration);
                }
            });
            this.permissionsGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
            });
            this.permissionsGrid.startup();
        },

        initPermissionsContextMenu: function () {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "PermissionsGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: this.i18n.Add,
                onClick: function (args) {
                    registry.byId(context.id + "AddPermissionsDropDown").openDropDown();
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
                onClick: function (args) { context._onRefreshPermissions(); }
            }));
        },

        refreshPermissionsGrid: function (clearSelection) {
            this.permissionsGrid.set("query", {
                id: "*"
            });
            if (clearSelection) {
                this.permissionsGrid.clearSelection();
            }
        },

        //  ---  ---
        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.groupsTab.id) {
                } else if (currSel.id === this.usersTab.id) {
                } else if (currSel.id === this.permissionsTab.id) {
                } else if (currSel.id === this.id + "_FileScopes") {
                    currSel.set("content", dojo.create("iframe", {
                        src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src=" + encodeURIComponent(ESPRequest.getBaseURL("ws_access") + "/Resources?rtype=file&rtitle=FileScope"),
                        style: "border: 0; width: 100%; height: 100%"
                    }));
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
            registry.byId(this.id + "ExportUsers").set("disabled", !hasUserSelection);

            var groupSelection = this.groupsGrid.getSelected();
            var hasGroupSelection = groupSelection.length;
            registry.byId(this.id + "EditGroups").set("disabled", !hasGroupSelection);
            registry.byId(this.id + "DeleteGroups").set("disabled", !hasGroupSelection);
            registry.byId(this.id + "ExportGroups").set("disabled", !hasGroupSelection);

            var permissionSelection = this.permissionsGrid.getSelected();
            var hasPermissionSelection = permissionSelection.length;
            registry.byId(this.id + "DeletePermissions").set("disabled", !hasPermissionSelection);
        }
    });
});
