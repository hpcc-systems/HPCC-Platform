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
    "dojo/Deferred",
    "dojo/promise/all",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/store/util/QueryResults",

    "hpcc/ESPRequest",
    "hpcc/ESPUtil"

], function (declare, lang, arrayUtil, Deferred, all, Memory, Observable, QueryResults,
    ESPRequest, ESPUtil) {

    var UsersStore = declare([Memory], {

        constructor: function () {
            this.idProperty = "__hpcc_id";
        },

        put: function (object, options) {
            var retVal = this.inherited(arguments);
            self.UserGroupEdit({
                request: {
                    username: object.username,
                    action: object.isMember ? "add" : "delete",
                    groupnames_i1: object.__hpcc_groupname
                }
            });
            return retVal;
        },

        query: function (query, options) {
            var results = all([
                this.refreshUsers(),
                this.refreshGroupUsers()
            ]).then(lang.hitch(this, function (response) {
                var groupUsers = {};
                arrayUtil.forEach(response[1], function (item, idx) {
                    groupUsers[item.username] = true;
                }, this);

                var data = [];
                arrayUtil.forEach(response[0], function (item, idx) {
                    data.push(lang.mixin(item, {
                        __hpcc_groupname: this.groupname,
                        __hpcc_id: item.username,
                        isMember: groupUsers[item.username] ? true : false
                    }));
                }, this);
                this.setData(data);
                return this.data;
            }));
            return QueryResults(results);
        },

        refreshUsers: function () {
            return self.Users().then(function (response) {
                if (lang.exists("UserResponse.Users.User", response)) {
                    return response.UserResponse.Users.User;
                }
                return [];
            });
        },

        refreshGroupUsers: function (query) {
            if (!this.groupname) {
                var deferred = new Deferred;
                deferred.resolve([]);
                return deferred.promise;
            };
            return self.GroupEdit({
                request: {
                    groupname: this.groupname
                }
            }).then(function (response) {
                if (lang.exists("GroupEditResponse.Users.User", response)) {
                    return response.GroupEditResponse.Users.User;
                }
                return [];
            });
        }
    });

    var GroupsStore = declare([Memory], {

        constructor: function () {
            this.idProperty = "__hpcc_id";
        },

        put: function (object, options) {
            var retVal = this.inherited(arguments);
            self.UserGroupEdit({
                request: {
                    username: object.__hpcc_username,
                    action: object.isMember ? "add" : "delete",
                    groupnames_i1: object.name
                }
            });
            return retVal;
        },

        query: function (query, options) {
            var results = all([
                this.refreshGroups(),
                this.refreshUserGroups()
            ]).then(lang.hitch(this, function (response) {
                var userGroups = {};
                arrayUtil.forEach(response[1], function (item, idx) {
                    userGroups[item.name] = true;
                }, this);

                var data = [];
                arrayUtil.forEach(response[0], function (item, idx) {
                    if (item.name !== "Authenticated Users") {
                        data.push(lang.mixin(item, {
                            __hpcc_id: item.name,
                            __hpcc_username: this.username,
                            isMember: userGroups[item.name] ? true : false
                        }));
                    }
                }, this);
                this.setData(data);
                return this.data;
            }));
            return QueryResults(results);
        },

        refreshGroups: function () {
            return self.Groups().then(function (response) {
                if (lang.exists("GroupResponse.Groups.Group", response)) {
                    return response.GroupResponse.Groups.Group;
                }
                return [];
            });
        },

        refreshUserGroups: function (query) {
            return self.UserEdit({
                request: {
                    username: this.username
                }
            }).then(function (response) {
                if (lang.exists("UserEditResponse.Groups.Group", response)) {
                    return response.UserEditResponse.Groups.Group;
                }
                return [];
            });
        }
    });

    var CONCAT_SYMBOL = ":";
    var ResourcesStore = declare([Memory], {

        constructor: function () {
            this.idProperty = "__hpcc_id";
        },

        put: function (row) {
            var item = this.get(row.__hpcc_id);
            var retVal = this.inherited(arguments);
            var request = {
                account_name: this.groupname ? this.groupname : this.username,
                account_type: this.groupname ? 1 : 0,
                basedn: row.__hpcc_parent.basedn,
                rname: row.name,
                action: "update"
            };
            lang.mixin(request, row);
            self.PermissionAction({
                request: request
            });
            return retVal;
        },

        query: function (query, options) {
            var results = all([
                this.refreshResources(query),
                this.refreshAccountPermissions(query)
            ]).then(lang.hitch(this, function (response) {
                var accountPermissions = {};
                arrayUtil.forEach(response[1], function (item, idx) {
                    accountPermissions[item.PermissionName] = item;
                }, this);

                var data = [];
                arrayUtil.forEach(response[0], function (item, idx) {
                    var accountPermission = accountPermissions[item.name];
                    data.push(lang.mixin(item, {
                        __hpcc_type: "Resources",
                        __hpcc_id: this.parentRow.__hpcc_id + CONCAT_SYMBOL + item.name,
                        __hpcc_parent: this.parentRow,
                        DisplayName: item.description ? item.description : item.name,
                        allow_access: accountPermission ? accountPermission.allow_access : false,
                        allow_read: accountPermission ? accountPermission.allow_read : false,
                        allow_write: accountPermission ? accountPermission.allow_write : false,
                        allow_full: accountPermission ? accountPermission.allow_full : false,
                        deny_access: accountPermission ? accountPermission.deny_access : false,
                        deny_read: accountPermission ? accountPermission.deny_read : false,
                        deny_write: accountPermission ? accountPermission.deny_write : false,
                        deny_full: accountPermission ? accountPermission.deny_full : false
                    }));
                }, this);
                this.setData(data);
                return data;
            }));
            return QueryResults(results);
        },

        refreshResources: function (query) {
            return self.Resources({
                request: {
                    basedn: this.basedn
                }
            }).then(lang.hitch(this, function (response) {
                if (lang.exists("ResourcesResponse.Resources.Resource", response)) {
                    return response.ResourcesResponse.Resources.Resource;
                }
                return [];
            }));
        },

        refreshAccountPermissions: function () {
            return self.AccountPermissions({
                request: {
                    AccountName: this.groupname ? this.groupname : this.username,
                    IsGroup: this.groupname ? true : false,
                    IncludeGroup: false
                }
            }).then(lang.hitch(this, function (response) {
                if (lang.exists("AccountPermissionsResponse.Permissions.Permission", response)) {
                    return response.AccountPermissionsResponse.Permissions.Permission;
                }
                return [];
            }));
        }
    });

    var PermissionsStore = declare([Memory], {
        service: "ws_access",
        action: "Permissions",
        responseQualifier: "BasednsResponse.Basedns.Basedn",
        idProperty: "__hpcc_id",

        constructor: function () {
            this.idProperty = "__hpcc_id";
        },

        get: function (id) {
            var tmp = id.split(CONCAT_SYMBOL);
            var retVal = null;
            if (tmp.length > 0) {
                var parentID = tmp[0];
                var parent = this.inherited(arguments, [parentID]);
                if (tmp.length === 1) {
                    return parent;
                }
                var child = parent.children.get(id);
                if (child) {
                    return child;
                }
                return parent;
            }
            return null;
        },

        putChild: function (row) {
            var parent = row.__hpcc_parent;
            return parent.children.put(row);
        },

        getChildren: function (parent, options) {
            return parent.children.query();
        },

        mayHaveChildren: function (object) {
            return object.__hpcc_type === "Permission";
        },

        query: function (query, options) {
            var results = self.Permissions().then(lang.hitch(this, function (response) {
                var data = [];
                if (lang.exists("BasednsResponse.Basedns.Basedn", response)) {
                    arrayUtil.forEach(response.BasednsResponse.Basedns.Basedn, function (item, idx) {
                        data.push(lang.mixin(item, {
                            __hpcc_type: "Permission",
                            __hpcc_id: item.basedn,
                            DisplayName: item.name,
                            children: lang.mixin(self.CreateResourcesStore(this.groupname, this.username, item.basedn), {
                                parent: this,
                                parentRow: item
                            })
                        }));
                    }, this);
                }
                this.setData(data);
                return data;
            }));
            return QueryResults(results);
        }
    });

    var self = {
        checkError: function (response, sourceMethod) {
            var retCode = lang.getObject(sourceMethod + "Response.retcode", false, response);
            var retMsg = lang.getObject(sourceMethod + "Response.retmsg", false, response);
            if (retCode) {
                dojo.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: "WsAccess." + sourceMethod,
                    Exceptions: [{ Message: retMsg }]
                });
            }
        },

        _doCall: function (action, params) {
            var context = this;
            return ESPRequest.send("ws_access", action, params).then(function (response) {
                context.checkError(response, action);
                return response;
            });
        },

        Users: function (params) {
            return this._doCall("Users", params);
        },

        UserAction: function (params) {
            return this._doCall("UserAction", params);
        },

        AddUser: function (params) {
            return this._doCall("AddUser", params);
        },

        UserEdit: function (params) {
            return this._doCall("UserEdit", params);
        },

        UserInfoEditInput: function (params) {
            return this._doCall("UserInfoEditInput", params);
        },

        UserInfoEdit: function (params) {
            return this._doCall("UserInfoEdit", params);
        },

        UserResetPass: function (params) {
            return this._doCall("UserResetPass", params);
        },

        UserGroupEdit: function (params) {
            return this._doCall("UserGroupEdit", params);
        },

        GroupAdd: function (params) {
            return this._doCall("GroupAdd", params);
        },

        GroupAction: function (params) {
            return this._doCall("GroupAction", params);
        },

        GroupEdit: function (params) {
            return this._doCall("GroupEdit", params);
        },

        Groups: function (params) {
            return this._doCall("Groups", params);
        },

        Permissions: function (params) {
            return this._doCall("Permissions", params);
        },

        Resources: function (params) {
            return this._doCall("Resources", params);
        },

        AccountPermissions: function (params) {
            return this._doCall("AccountPermissions", params);
        },

        PermissionAction: function (params) {
            return this._doCall("PermissionAction", params);
        },

        CreateUsersStore: function (groupname) {
            var store = new UsersStore();
            store.groupname = groupname;
            return Observable(store);
        },

        CreateGroupsStore: function (username) {
            var store = new GroupsStore();
            store.username = username;
            return Observable(store);
        },

        CreatePermissionsStore: function (groupname, username) {
            var store = new PermissionsStore();
            store.groupname = groupname;
            store.username = username;
            return Observable(store);
        },

        CreateResourcesStore: function (groupname, username, basedn) {
            var store = new ResourcesStore();
            store.groupname = groupname;
            store.username = username;
            store.basedn = basedn;
            return Observable(store);
        }
    };

    return self;
});
