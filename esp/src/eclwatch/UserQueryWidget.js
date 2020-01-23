define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom-form",
    "dojo/promise/all",

    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/form/Select",

    "dgrid/tree",
    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "src/ws_access",
    "src/ws_account",
    "src/ESPBase",
    "src/ESPUtil",
    "hpcc/UserDetailsWidget",
    "hpcc/GroupDetailsWidget",
    "hpcc/ShowIndividualPermissionsWidget",

    "dojo/text!../templates/UserQueryWidget.html",

    "hpcc/FilterDropDownWidget",
    "hpcc/TargetSelectWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/DropDownButton",
    "dijit/form/ValidationTextBox",
    "dijit/ToolbarSeparator",
    "dijit/form/TextBox",
    "dijit/Dialog",

    "dojox/form/PasswordValidator",

    "hpcc/TableContainer"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, domForm, all,
    registry, Menu, MenuItem, MenuSeparator, Select,
    tree, selector,
    _TabContainerWidget, WsAccess, WsAccount, ESPBaseMod, ESPUtil, UserDetailsWidget, GroupDetailsWidget, ShowIndividualPermissionsWidget,
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
            this.filePermissionsForm = registry.byId(this.id + "FilePermissionForm");
            this.usersTab = registry.byId(this.id + "_Users");
            this.addPermissionForm = registry.byId(this.id + "AddPermissionForm");
            this.addPermissionType = registry.byId(this.id + "AddPermissionType");
            this.permissionsTab = registry.byId(this.id + "_Permissions");
            this.filter = registry.byId(this.id + "Filter");
            this.filePermissionDialog = registry.byId(this.id + "FilePermissionDialog");
            this.showPermissionDialog = registry.byId(this.id + "ShowPermissionDialog");
            this.usersSelect = registry.byId(this.id + "UsersSelect");
            this.groupsSelect = registry.byId(this.id + "GroupsSelect");
            this.showPermissionsGrid = registry.byId(this.id + "ShowPermissionsGrid");
            this.checkFileSubmit = registry.byId(this.id + "CheckFileSubmit");
            this.nameSelect = registry.byId(this.id + "NameSelect");
            this.addGroupOwner = registry.byId(this.id + "AddGroupOwner");
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
                this.refreshPermissionsGrid();
            }
        },

        _onDisableScopeScans: function () {
            if (confirm(this.i18n.DisableScopeScanConfirm)) {
                WsAccess.DisableScopeScans();
                this.refreshPermissionsGrid();
            }
        },

        _onFileScopeDefaultPermissions: function () {
            var row = this.getRow("FileScope");
            if (row) {
                var fileScopeDefaultPermissionsTab = this.ensurePermissionsPane(row.Basedn + "FileScope", {
                    Basedn: row.Basedn,
                    TabName: this.i18n.title_FileScopeDefaultPermissions,
                    DefaultPermissions: true
                });
                this.selectChild(fileScopeDefaultPermissionsTab);
            }
        },

        _onWorkunitScopeDefaultPermissions: function () {
            var row = this.getRow("WorkunitScope");
            if (row) {
                var workunitScopeDefaultPermissionsTab = this.ensurePermissionsPane(row.Basedn + "WUScope", {
                    Basedn: row.Basedn,
                    TabName: this.i18n.title_WorkunitScopeDefaultPermissions,
                    DefaultPermissions: true
                });
                this.selectChild(workunitScopeDefaultPermissionsTab);
            }
        },

        _onPhysicalFiles: function () {
            var row = this.getRow("FileScope");
            if (row) {
                var physicalPermissionsTab = this.ensurePermissionsPane(row.Basedn + "PhysicalFiles", {
                    Basedn: row.Basedn,
                    Name: "file",
                    TabName: "Physical Files"
                });
                this.selectChild(physicalPermissionsTab);
            }
        },

        _onCloseFilePermissions: function () {
            this.filePermissionDialog.hide();
            this.nameSelect.reset();
            this.usersSelect.set("value", "");
            this.groupsSelect.set("value", "");
        },
        _onCheckFilePermissions: function () {
            this.filePermissionDialog.show();
        },
        _onCheckFileSubmit: function () {
            var context = this;
            if (this.filePermissionsForm.validate()) {
                WsAccess.FilePermission({
                    request: {
                        FileName: this.nameSelect.get("value"),
                        UserName: this.usersSelect.get("value"),
                        GroupName: this.groupsSelect.get("value")
                    }
                }).then(function (response) {
                    dojo.byId("PermissionResponse").innerHTML = context.i18n.FilePermission + ": " + response.FilePermissionResponse.UserPermission;
                });
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
                var codeGeneratorPermissionsTab = this.ensurePermissionsPane(row.basedn, {
                    Basedn: row.basedn,
                    Rtype: "service",
                    Rtitle: "CodeGenerator Permission",
                    Name: "",
                    prefix: "codegenerator.",
                    action: "Code Generator",
                    TabName: this.i18n.title_CodeGeneratorPermissions,

                    DefaultPermissions: true
                });
                this.selectChild(codeGeneratorPermissionsTab);
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
                if (i === 0) {
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
            var base = new ESPBaseMod.ESPBase();
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
                    EmployeeID: selections[i].employeeID,
                    EmployeeNumber: selections[i].employeeNumber,
                    Fullname: selections[i].fullname,
                    Passwordexpiration: selections[i].passwordexpiration
                });
                if (i === 0) {
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
                var request = {
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
            var base = new ESPBaseMod.ESPBase();
            window.open(base.getBaseURL("ws_access") + "/UserAccountExport?" + usernames);
        },

        _onUsersRowDblClick: function (username, employeeID, employeeNumber, fullname, passwordexpiration) {
            var userTab = this.ensureUserPane(username, {
                Username: username,
                EmployeeID: employeeID,
                EmployeeNumber: employeeNumber,
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
            var selRow = {}
            selRow["BasednName"] = this.addPermissionType.get("value");
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

        _onEditPermission: function (event) {
            var selections = this.permissionsGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePermissionsPane("Permissions" + selections[i].name, {
                    Name: selections[i].name
                });
                if (i === 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
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

        _onPermissionsRowDblClick: function (basedn, name, description) {
            var permissionsTab = this.ensurePermissionsPane(name, {
                Basedn: basedn,
                Name: name,
                Description: description
            });
            this.selectChild(permissionsTab);
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

            this.usersSelect.init({
                Users: true,
                includeBlank: true
            });

            this.groupsSelect.init({
                UserGroups: true,
                includeBlank: true
            });

            this.filter.on("clear", function (evt) {
                context.refreshHRef();
                context.refreshUsersGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshHRef();
                context.usersGrid._currentPage = 0;
                context.refreshUsersGrid();
            });

            this.filePermissionDialog.on("cancel", function (evt) {
                context._onCloseFilePermissions();
            });

            this.groupsSelect.on("click", function (evt) {
                context.usersSelect.set("value", "");
            });

            this.usersSelect.on("click", function (evt) {
                context.groupsSelect.set("value", "");
            });

            WsAccount.MyAccount({
            }).then(function (response) {
                if (lang.exists("MyAccountResponse.distinguishedName", response)) {
                    context.addGroupOwner.set("value", response.MyAccountResponse.distinguishedName);
                }
            });

            this.refreshActionState();
        },

        //  Groups  ---
        initGroupsGrid: function () {
            this.initGroupsContextMenu();
            var store = WsAccess.CreateGroupsStore(null, true);
            this.groupsGrid = declare([ESPUtil.Grid(true, true)])({
                sort: [{ attribute: "name" }],
                store: store,
                columns: {
                    check: selector({
                        width: 27,
                        label: " "
                    }, "checkbox"),
                    name: {
                        label: this.i18n.GroupName,
                        formatter: function (_name, idx) {
                            return "<a href='#' class='dgrid-row-url'>" + _name + "</a>"
                        }
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
            this.groupsGrid.on(".dgrid-row-url:click", function (evt) {
                if (context._onGroupsRowDblClick) {
                    var item = context.groupsGrid.row(evt).data;
                    context._onGroupsRowDblClick(item.name);
                }
            });
            this.groupsGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
            });
            ESPUtil.goToPageUserPreference(this.groupsGrid, "UsersQueryWidget_GroupsGrid_GridRowsPerPage").then(function () {
                context.groupsGrid.startup();
            });
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
            this.usersGrid = declare([ESPUtil.Grid(true, true)])({
                store: this.usersStore,
                query: this.filter.toObject(),
                sort: [{ attribute: "username" }],
                columns: {
                    check: selector({
                        width: 27,
                        label: " "
                    }, "checkbox"),
                    username: {
                        width: 180,
                        label: this.i18n.Username,
                        sortable: true,
                        formatter: function (_name, idx) {
                            return "<a href='#' class='dgrid-row-url'>" + _name + "</a>"
                        }
                    },
                    employeeID: {
                        width: 180,
                        sortable: true,
                        label: this.i18n.EmployeeID
                    },
                    employeeNumber: {
                        width: 180,
                        sortable: true,
                        label: this.i18n.EmployeeNumber
                    },
                    fullname: {
                        label: this.i18n.FullName,
                        sortable: true
                    },
                    passwordexpiration: {
                        width: 180,
                        label: this.i18n.PasswordExpiration,
                        sortable: true
                    }
                }
            }, this.id + "UsersGrid");
            var context = this;

            this.usersGrid.on(".dgrid-row-url:click", function (evt) {
                if (context._onUsersRowDblClick) {
                    var item = context.usersGrid.row(evt).data;
                    context._onUsersRowDblClick(item.username, item.employeeID, item.employeeNumber, item.fullname, item.passwordexpiration);
                }
            });
            this.usersGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onUsersRowDblClick) {
                    var item = context.usersGrid.row(evt).data;
                    context._onUsersRowDblClick(item.username, item.employeeID, item.employeeNumber, item.fullname, item.passwordexpiration);
                }
            });
            this.usersGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
            });
            ESPUtil.goToPageUserPreference(this.usersGrid, "UsersQueryWidget_UsersGrid_GridRowsPerPage").then(function () {
                context.usersGrid.startup();
            });
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
                this.addChild(retVal, "last");
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
                            value: item.name
                        });
                        optionMap[item.name] = item;
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
                deselectOnRefresh: true,
                sort: [{ attribute: "DisplayName" }],
                store: this.permissionsStore,
                columns: {
                    check: selector({
                        width: 27,
                        disabled: function (row) {
                            if (row.name === "File Scopes" || row.name === "Workunit Scopes" || row.name === "Repository Modules") {
                                return false;
                            }
                            return row.children ? true : false;
                        }
                    }, "checkbox"),
                    name: tree({
                        width: 360,
                        sortable: false,
                        label: this.i18n.Name,
                        formatter: function (_name, idx) {
                            if (idx.__hpcc_parent) {
                                return "<a href='#' class='dgrid-row-url'>" + _name + "</a>"
                            } else {
                                return _name;
                            }
                        }
                    }),
                    description: {
                        width: 360,
                        sortable: false,
                        label: this.i18n.Description
                    },
                    basedn: {
                        sortable: false,
                        label: "basedn"
                    }
                }
            }, this.id + "PermissionsGrid");
            var context = this;
            this.permissionsGrid.on(".dgrid-row-url:click", function (evt) {
                if (context._onPermissionsRowDblClick) {
                    var item = context.permissionsGrid.row(evt).data;
                    context._onPermissionsRowDblClick(item.__hpcc_parent.name, item.name, item.DisplayName);
                }
            });
            this.permissionsGrid.on(".dgrid-row:dblclick", function (evt) {
                if (context._onPermissionsRowDblClick) {
                    var item = context.permissionsGrid.row(evt).data;
                    context._onPermissionsRowDblClick(item.__hpcc_parent.name, item.name, item.DisplayName);
                }
            });
            this.permissionsGrid.onSelectionChanged(function (event) {
                context.refreshActionState(event);
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
                onClick: function (args) { context._onEditPermission(); }
            }));
            pMenu.addChild(new MenuItem({
                label: this.i18n.Delete,
                onClick: function (args) { context._onDeletePermission(); }
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

        ensurePermissionsPane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                retVal = new ShowIndividualPermissionsWidget({
                    id: id,
                    title: params.TabName ? params.TabName : params.Name,
                    iconClass: 'iconPeople',
                    closable: true,
                    params: params
                });
                this.addChild(retVal, "last");
            }
            return retVal;
        },

        //  ---  ---
        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.groupsTab.id) {
                } else if (currSel.id === this.usersTab.id) {
                    this.refreshUsersGrid();
                } else if (currSel.id === this.permissionsTab.id) {
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel.params);
                    }
                }
            }
        },

        refreshActionState: function (event) {
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

            if (hasPermissionSelection && permissionSelection[0].name === "File Scopes") {
                var context = this;
                WsAccess.Resources({
                    request: {
                        name: event.rows[0].data.Basedn
                    }
                }).then(function (response) {
                    if (lang.exists("ResourcesResponse.scopeScansStatus", response)) {
                        var scopeScansEnabled;
                        scopeScansEnabled = response.ResourcesResponse.scopeScansStatus.isEnabled;
                        registry.byId(context.id + "EnableScopeScans").set("disabled", scopeScansEnabled);
                        registry.byId(context.id + "DisableScopeScans").set("disabled", !scopeScansEnabled);
                    }
                });
            }

            registry.byId(this.id + "FileScopeDefaultPermissions").set("disabled", true);
            registry.byId(this.id + "WorkUnitScopeDefaultPermissions").set("disabled", true);
            registry.byId(this.id + "PhysicalFiles").set("disabled", true);
            registry.byId(this.id + "CheckFilePermissions").set("disabled", true);
            registry.byId(this.id + "CodeGenerator").set("disabled", true);
            registry.byId(this.id + "AdvancedPermissions").set("disabled", true);
            registry.byId(this.id + "DeletePermissions").set("disabled", !hasPermissionSelection);

            for (var i = 0; i < permissionSelection.length; ++i) {
                if (permissionSelection[i].children) {
                    registry.byId(this.id + "DeletePermissions").set("disabled", true);
                }
                switch (permissionSelection[i].name) {
                    case "File Scopes":
                        registry.byId(this.id + "PhysicalFiles").set("disabled", !hasPermissionSelection);
                        registry.byId(this.id + "FileScopeDefaultPermissions").set("disabled", !hasPermissionSelection);
                        registry.byId(this.id + "CheckFilePermissions").set("disabled", !hasPermissionSelection);
                        registry.byId(this.id + "AdvancedPermissions").set("disabled", !hasPermissionSelection);
                        break;
                    case "Workunit Scopes":
                        registry.byId(this.id + "WorkUnitScopeDefaultPermissions").set("disabled", !hasPermissionSelection);
                        registry.byId(this.id + "AdvancedPermissions").set("disabled", !hasPermissionSelection);
                        break;
                    case "Repository Modules":
                        registry.byId(this.id + "CodeGenerator").set("disabled", !hasPermissionSelection);
                        registry.byId(this.id + "AdvancedPermissions").set("disabled", !hasPermissionSelection);
                        break;
                }
            }
        }
    });
});
