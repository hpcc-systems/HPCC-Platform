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
    "dojo/_base/array",
    "dojo/Deferred",
    "dojo/promise/all",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/store/util/QueryResults",
    "dojo/store/util/SimpleQueryEngine",
    "dojo/topic",

    "hpcc/ESPRequest",
    "hpcc/ESPUtil"

], function (declare, lang, arrayUtil, Deferred, all, Memory, Observable, QueryResults, SimpleQueryEngine, topic,
    ESPRequest, ESPUtil) {

    var UsersStore = declare([ESPRequest.Store], {
        service: "ws_access",
        action: "UserQuery",
        responseQualifier: "UserQueryResponse.Users.User",
        responseTotalQualifier: "UserQueryResponse.TotalUsers",
        idProperty: "username",
        startProperty: "PageStartFrom",
        countProperty: "PageSize",
        SortbyProperty: 'SortBy'
    });

    var GroupsStore = declare([ESPRequest.Store], {
        service: "ws_access",
        action: "GroupQuery",
        responseQualifier: "GroupQueryResponse.Groups.Group",
        responseTotalQualifier: "GroupQueryResponse.TotalGroups",
        idProperty: "name",
        startProperty: "PageStartFrom",
        countProperty: "PageSize",
        SortbyProperty: 'SortBy',

        preRequest: function (request) {
            switch (request.SortBy) {
                case "name":
                    request.SortBy = "Name";
                    break;
                case "groupOwner":
                    request.SortBy = "ManagedBy";
                    break;
            }
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
                rtitle: row.__hpcc_parent.rtitle,
                rtype: row.__hpcc_parent.rtype,
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
                options = options || {};
                this.setData(SimpleQueryEngine({}, { sort: options.sort })(data));
                return this.data;
            }));
            return QueryResults(results);
        },

        refreshResources: function (query) {
            return self.Resources({
                request: {
                    basedn: this.parentRow.basedn,
                    rtype: this.parentRow.rtype,
                    rtitle: this.parentRow.rtitle
                }
            }).then(lang.hitch(this, function (response) {
                if (lang.exists("ResourcesResponse.Resources.Resource", response)) {
                    return response.ResourcesResponse.Resources.Resource;
                }
                return [];
            }));
        },

        refreshAccountPermissions: function () {
            if (!this.groupname && !this.username) {
                return [];
            }
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

    var InheritedPermissionStore = declare([Memory], {

        constructor: function () {
            this.idProperty = "__hpcc_id";
        },

        put: function (row) {
            var item = this.get(row.__hpcc_id);
            var retVal = this.inherited(arguments);
            var request = {
                basedn: row.basedn,
                rtype: row.rtype,
                rname: row.rname,
                rtitle: row.rtitle,
                account_name: row.account_name,
                account_type: 0,
                action: "update"
            };
            lang.mixin(request, row);
            self.PermissionAction({
                request: request
            });
            return retVal;
        },

        query: function (query, options) {
            var data = [];
            var results = all([
                this.refreshAccountPermissions(query)
            ]).then(lang.hitch(this, function (response) {
                var accountPermissions = {};
                arrayUtil.forEach(response[0], function (item, idx) {
                    accountPermissions[item.PermissionName] = item;
                    data.push(lang.mixin(item, {
                        __hpcc_type: "InheritedPermissions",
                        __hpcc_id: this.TabName + CONCAT_SYMBOL + this.AccountName + CONCAT_SYMBOL + item.PermissionName + CONCAT_SYMBOL + idx,
                        rname: item.PermissionName,
                        rtype: item.RType,
                        rtitle: item.ResourceName,
                        account_name: this.TabName,
                        allow_access: item ? item.allow_access : false,
                        allow_read: item ? item.allow_read : false,
                        allow_write: item ? item.allow_write : false,
                        allow_full: item ? item.allow_full : false,
                        deny_access: item ? item.deny_access : false,
                        deny_read: item ? item.deny_read : false,
                        deny_write: item ? item.deny_write : false,
                        deny_full: item ? item.deny_full : false
                    }));
                }, this);
                options = options || {};
                this.setData(SimpleQueryEngine({}, { sort: options.sort })(data));
                return this.data;
            }));
            return QueryResults(results);
        },

        refreshAccountPermissions: function () {
            if (!this.AccountName) {
                return [];
            }
            return self.AccountPermissions({
                request: {
                    AccountName: this.AccountName,
                    IsGroup: false,
                    IncludeGroup: true,
                    TabName: this.TabName
                }
            }).then(lang.hitch(this, function (response) {
                if (lang.exists("AccountPermissionsResponse.GroupPermissions.GroupPermission", response)) {
                    var arr = response.AccountPermissionsResponse.GroupPermissions.GroupPermission;
                    for (var index in arr) {
                        if (arr[index].GroupName === this.TabName) {
                            return response.AccountPermissionsResponse.GroupPermissions.GroupPermission[index].Permissions.Permission;
                        }
                    }
                }
                return [];
            }));
        }
    });

    var AccountResourcesStore = declare([Memory], {

        constructor: function () {
            this.idProperty = "__hpcc_id";
        },

        put: function (row) {
            var item = this.get(row.__hpcc_id);
            var retVal = this.inherited(arguments);
            var request = {
                basedn: row.basedn,
                rtype: row.rtype,
                rname: row.rname,
                rtitle: row.rtitle,
                account_name: row.account_name,
                account_type: 0,
                action: "update"
            };
            lang.mixin(request, row);
            self.PermissionAction({
                request: request
            });
            return retVal;
        },

        query: function (query, options) {
            var data = [];
            var results = all([
                this.refreshAccountPermissions(query)
            ]).then(lang.hitch(this, function (response) {
                var accountPermissions = {};
                arrayUtil.forEach(response[0], function (item, idx) {
                    accountPermissions[item.PermissionName] = item;
                    data.push(lang.mixin(item, {
                        __hpcc_type: "AccountPermissions",
                        __hpcc_id: this.AccountName + CONCAT_SYMBOL + item.PermissionName + CONCAT_SYMBOL + idx,
                        rname: item.PermissionName,
                        rtype: item.RType,
                        rtitle: item.ResourceName,
                        account_name: this.AccountName,
                        allow_access: item ? item.allow_access : false,
                        allow_read: item ? item.allow_read : false,
                        allow_write: item ? item.allow_write : false,
                        allow_full: item ? item.allow_full : false,
                        deny_access: item ? item.deny_access : false,
                        deny_read: item ? item.deny_read : false,
                        deny_write: item ? item.deny_write : false,
                        deny_full: item ? item.deny_full : false
                    }));
                }, this);
                options = options || {};
                this.setData(SimpleQueryEngine({}, { sort: options.sort })(data));
                return this.data;
            }));
            return QueryResults(results);
        },

        refreshAccountPermissions: function () {
            if (!this.AccountName) {
                return [];
            }
            return self.AccountPermissions({
                request: {
                    AccountName: this.AccountName,
                    IsGroup: this.IsGroup ? true : false,
                    IncludeGroup: this.IsGroup ? true : false
                }
            }).then(lang.hitch(this, function (response) {
                if (lang.exists("AccountPermissionsResponse.Permissions.Permission", response)) {
                    return response.AccountPermissionsResponse.Permissions.Permission;
                }
                return [];
            }));
        }
    });

    var IndividualPermissionsStore = declare([Memory], {

        constructor: function () {
            this.idProperty = "__hpcc_id";
        },

        put: function (row) {
            var item = this.get(row.__hpcc_id);
            var retVal = this.inherited(arguments);
            var request = {
                basedn: row.basedn,
                rtype: row.rtype,
                rtitle: row.rtitle,
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
            var data = [];
            var results = all([
                this.refreshAccountPermissions(query)
            ]).then(lang.hitch(this, function (response) {
                var accountPermissions = {};
                arrayUtil.forEach(response[0], function (item, idx) {
                    accountPermissions[item.account_name] = item;
                    data.push(lang.mixin(item, {
                        __hpcc_type: "IndividualPermissions",
                        __hpcc_id: this.name + CONCAT_SYMBOL + idx,
                        basedn: this.basedn,
                        rtype: this.rtype,
                        rtitle: this.rtitle,
                        rname: this.name,
                        account_name: item.account_name,
                        allow_access: item ? item.allow_access : false,
                        allow_read: item ? item.allow_read : false,
                        allow_write: item ? item.allow_write : false,
                        allow_full: item ? item.allow_full : false,
                        deny_access: item ? item.deny_access : false,
                        deny_read: item ? item.deny_read : false,
                        deny_write: item ? item.deny_write : false,
                        deny_full: item ? item.deny_full : false
                    }));
                }, this);
                options = options || {};
                this.setData(SimpleQueryEngine({}, { sort: options.sort })(data));
                return this.data;
            }));
            return QueryResults(results);
        },

        refreshAccountPermissions: function () {
            if (!this.name) {
                return [];
            }
            return self.ResourcePermissions({
                request: {
                    basedn: this.basedn,
                    rtype: this.rtype,
                    rtitle: this.rtitle,
                    name: this.name

                }
            }).then(lang.hitch(this, function (response) {
                if (lang.exists("ResourcePermissionsResponse.Permissions.Permission", response)) {
                    return response.ResourcePermissionsResponse.Permissions.Permission;
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
                options = options || {};
                this.setData(SimpleQueryEngine({}, { sort: options.sort })(data));
                return this.data;
            }));
            return QueryResults(results);
        }
    });

    var self = {    // jshint ignore:line
        checkError: function (response, sourceMethod, showOkMsg) {
            var retCode = lang.getObject(sourceMethod + "Response.retcode", false, response);
            var retMsg = lang.getObject(sourceMethod + "Response.retmsg", false, response);
            if (retCode) {
                topic.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: "WsAccess." + sourceMethod,
                    Exceptions: [{ Message: retMsg }]
                });
            } else if (showOkMsg && retMsg) {
                topic.publish("hpcc/brToaster", {
                    Severity: "Message",
                    Source: "WsAccess." + sourceMethod,
                    Exceptions: [{ Message: retMsg }]
                });

            }
        },

        _doCall: function (action, params) {
            var context = this;
            return ESPRequest.send("ws_access", action, params).then(function (response) {
                context.checkError(response, action, params ? params.showOkMsg : false);
                return response;
            });
        },

        Users: function (params) {
            return this._doCall("UserQuery", params);
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

        UserGroupEditInput: function (params) {
            return this._doCall("UserGroupEditInput", params);
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

        GroupMemberEdit: function (params) {
            return this._doCall("GroupMemberEdit", params);
        },

        Groups: function (params) {
            return this._doCall("GroupQuery", params);
        },

        Members: function (params) {
            return this._doCall("GroupEdit", params);
        },

        GroupMemberEditInput: function (params) {
            return this._doCall("GroupMemberEditInput", params);
        },

        Permissions: function (params) {
            return this._doCall("Permissions", params);
        },

        AccountPermissions: function (params) {
            return this._doCall("AccountPermissions", params);
        },

        ResourcePermissions: function (params) {
            return this._doCall("ResourcePermissions", params);
        },

        Resources: function (params) {
            return this._doCall("Resources", params);
        },

        ResourceAdd: function (params) {
            return this._doCall("ResourceAdd", params);
        },

        ResourceDelete: function(params) {
            return this._doCall("ResourceDelete", params);
        },

        PermissionAction: function (params) {
            return this._doCall("PermissionAction", params);
        },

        FilePermission: function (params) {
            return this._doCall("FilePermission", params);
        },

        ClearPermissionsCache: function() {
            return this._doCall("ClearPermissionsCache", {
                request: {
                    action :"Clear Permissions Cache"
                }
            });
        },

        EnableScopeScans: function () {
            return this._doCall("EnableScopeScans", {
                request: {
                    action: "Enable Scope Scans"
                }
            });
        },

        DisableScopeScans: function () {
            return this._doCall("DisableScopeScans", {
                request: {
                    action: "Disable Scope Scans"
                }
            });
        },

        DefaultPermissions: function () {
            return this._doCall("ResourcePermissions", {
                request: {
                    basedn: "ou=ecl,dc=hpccdev,dc=local",
                    rtype: "file",
                    name: "files",
                    action: "Default Permissions"
                }
            });
        },

        PhysicalFiles: function () {
            return this._doCall("ResourcePermissions", {
                request: {
                    basedn: "ou=files,ou=ecl,dc=hpccdev,dc=local",
                    rtype: "file",
                    rtitle: "FileScope",
                    name: "file",
                    action: "Physical Files"
                }
            });
        },

        CheckFilePermissions: function () {
            return this._doCall("FilePermission", {
                request: {
                    action: "Check File Permission"
                }
            });
        },

        CreateUsersStore: function (groupname, observable) {
            var store = new UsersStore();
            store.groupname = groupname;
            if (observable) {
                return Observable(store);
            }
            return store;
        },

        CreateGroupsStore: function (username, observable) {
            var store = new GroupsStore();
            store.username = username;
            if (observable) {
                return Observable(store);
            }
            return store;
        },

        CreatePermissionsStore: function (groupname, username) {
            var store = new PermissionsStore();
            store.groupname = groupname;
            store.username = username;
            return Observable(store);
        },

        CreateAccountPermissionsStore: function (IsGroup, IncludeGroup, AccountName) {
            var store = new AccountResourcesStore();
            store.IsGroup = IsGroup;
            store.IncludeGroup = IncludeGroup;
            store.AccountName = AccountName;
            return Observable(store);
        },

        CreateInheritedPermissionsStore: function (IsGroup, IncludeGroup, AccountName, TabName) {
            var store = new InheritedPermissionStore();
            store.IsGroup = IsGroup;
            store.IncludeGroup = IncludeGroup;
            store.AccountName = AccountName;
            store.TabName = TabName;
            return Observable(store);
        },

        CreateIndividualPermissionsStore: function (basedn, rtype, rtitle, name) {
            var store = new IndividualPermissionsStore();
            store.basedn = basedn;
            store.rtype = rtype;
            store.rtitle = rtitle;
            store.name = name;
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