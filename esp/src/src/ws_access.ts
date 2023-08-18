import * as arrayUtil from "dojo/_base/array";
import * as Deferred from "dojo/_base/Deferred";
import * as lang from "dojo/_base/lang";
import * as all from "dojo/promise/all";
import * as Observable from "dojo/store/Observable";
import * as QueryResults from "dojo/store/util/QueryResults";
import * as SimpleQueryEngine from "dojo/store/util/SimpleQueryEngine";
import * as topic from "dojo/topic";

import { AccessService, WsAccess } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";

import * as ESPRequest from "./ESPRequest";
import { Memory } from "./store/Memory";
import { Paged } from "./store/Paged";
import { BaseStore } from "./store/Store";

const logger = scopedLogger("src/ws_access.ts");

class UsersStore extends ESPRequest.Store {

    service = "ws_access";
    action = "UserQuery";
    responseQualifier = "UserQueryResponse.Users.User";
    responseTotalQualifier = "UserQueryResponse.TotalUsers";
    idProperty = "username";

    startProperty = "PageStartFrom";
    countProperty = "PageSize";

    SortbyProperty = "SortBy";

    groupname: string;

}

class GroupsStore extends ESPRequest.Store {

    service = "ws_access";
    action = "GroupQuery";
    responseQualifier = "GroupQueryResponse.Groups.Group";
    responseTotalQualifier = "GroupQueryResponse.TotalGroups";
    idProperty = "name";

    startProperty = "PageStartFrom";
    countProperty = "PageSize";

    SortbyProperty = "SortBy";

    username: string;

    preRequest(request) {
        switch (request.SortBy) {
            case "name":
                request.SortBy = "Name";
                break;
            case "groupOwner":
                request.SortBy = "ManagedBy";
                break;
        }
    }
}

const CONCAT_SYMBOL = ":";
class ResourcesStore extends Memory {

    groupname: string;
    username: string;
    parentRow: any;
    basedn: string;
    name: string;
    scopeScansEnabled: boolean;

    constructor() {
        super("__hpcc_id");
    }

    put(row, options) {
        this.get(row.__hpcc_id);
        const retVal = super.put(row, options);
        const request = {
            account_name: this.groupname ? this.groupname : this.username,
            account_type: this.groupname ? 1 : 0,
            BasednName: row.__hpcc_parent.name,
            rname: row.name,
            action: "update"
        };
        lang.mixin(request, row);
        delete request["__hpcc_parent"];
        PermissionAction({
            request
        });
        return retVal;
    }

    query(query, options) {
        const results = all([
            this.refreshResources(query),
            this.refreshAccountPermissions()
        ]).then(lang.hitch(this, function (response) {
            const accountPermissions = {};
            arrayUtil.forEach(response[1], function (item, idx) {
                accountPermissions[item.ResourceName] = item;
            }, this);

            const data = [];
            arrayUtil.forEach(response[0], function (item, idx) {
                const accountPermission = accountPermissions[item.name];
                data.push(lang.mixin(item, {
                    __hpcc_type: "Resources",
                    __hpcc_id: this.parentRow.__hpcc_id + CONCAT_SYMBOL + item.name,
                    __hpcc_parent: this.parentRow,
                    DisplayName: item.description ? item.description : item.name,
                    AccountName: this.groupname,
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
    }

    refreshResources(query) {
        return Resources({
            request: {
                name: this.parentRow.name
            }
        }).then(lang.hitch(this, function (response) {
            this.checkScopesEnabled(response);
            if (lang.exists("ResourcesResponse.Resources.Resource", response)) {
                return response.ResourcesResponse.Resources.Resource;
            }
            return [];
        }));
    }

    checkScopesEnabled(response) {
        this.scopeScansEnabled = response.ResourcesResponse?.scopeScansStatus?.isEnabled ?? false;
    }

    refreshAccountPermissions() {
        if (!this.groupname && !this.username) {
            return [];
        }
        return AccountPermissionsV2({
            request: {
                AccountName: this.groupname ? this.groupname : this.username,
                IsGroup: this.groupname ? true : false,
                IncludeGroup: false
            }
        }).then(lang.hitch(this, function (response) {
            if (lang.exists("AccountPermissionsV2Response.Permissions.Permission", response)) {
                return response.AccountPermissionsV2Response.Permissions.Permission;
            }
            return [];
        }));
    }
}

class InheritedPermissionStore extends Memory {

    AccountName: string;
    TabName: string;
    IsGroup: boolean;
    IncludeGroup: boolean;

    constructor() {
        super("__hpcc_id");
    }

    put(row, options) {
        this.get(row.__hpcc_id);
        const retVal = super.put(row, options);
        const request = {
            BasednName: row.BasednName,
            rname: row.ResourceName,
            account_name: row.account_name,
            account_type: 0,
            action: "update"
        };
        lang.mixin(request, row);
        PermissionAction({
            request
        });
        return retVal;
    }

    query(query, options) {
        const data = [];
        const results = all([
            this.refreshAccountPermissions()
        ]).then(lang.hitch(this, function (response) {
            const accountPermissions = {};
            arrayUtil.forEach(response[0], function (item, idx) {
                accountPermissions[item.ResourceName] = item;
                data.push(lang.mixin(item, {
                    __hpcc_type: "InheritedPermissions",
                    __hpcc_id: this.TabName + CONCAT_SYMBOL + this.AccountName + CONCAT_SYMBOL + item.ResourceName + CONCAT_SYMBOL + idx,
                    rname: item.ResourceName,
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
    }

    refreshAccountPermissions() {
        if (!this.AccountName) {
            return [];
        }
        return AccountPermissionsV2({
            request: {
                AccountName: this.AccountName,
                IsGroup: false,
                IncludeGroup: true,
                TabName: this.TabName
            }
        }).then(lang.hitch(this, function (response) {
            if (lang.exists("AccountPermissionsV2Response.GroupPermissions.GroupPermission", response)) {
                const arr = response.AccountPermissionsV2Response.GroupPermissions.GroupPermission;
                for (const index in arr) {
                    if (arr[index].GroupName === this.TabName) {
                        return response.AccountPermissionsV2Response.GroupPermissions.GroupPermission[index].Permissions.Permission;
                    }
                }
            }
            return [];
        }));
    }
}

class AccountResourcesStore extends Memory {

    AccountName: string;
    IsGroup: boolean;
    IncludeGroup: boolean;

    constructor() {
        super("__hpcc_id");
    }

    put(row, options) {
        this.get(row.__hpcc_id);
        const retVal = super.put(row, options);
        const request = {
            BasednName: row.BasednName,
            rname: row.ResourceName,
            account_name: row.account_name,
            account_type: this.IsGroup ? 1 : 0,
            action: "update"
        };
        lang.mixin(request, row);
        PermissionAction({
            request
        });
        return retVal;
    }

    query(query, options) {
        const data = [];
        const results = all([
            this.refreshAccountPermissions()
        ]).then(lang.hitch(this, function (response) {
            const accountPermissions = {};
            arrayUtil.forEach(response[0], function (item, idx) {
                accountPermissions[item.ResourceName] = item;
                data.push(lang.mixin(item, {
                    __hpcc_type: "AccountPermissions",
                    __hpcc_id: this.AccountName + CONCAT_SYMBOL + item.ResourceName + CONCAT_SYMBOL + idx,
                    rname: item.ResourceName,
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
    }

    refreshAccountPermissions() {
        if (!this.AccountName) {
            return [];
        }
        return AccountPermissions({
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
}

class IndividualPermissionsStore extends Memory {

    name: string;
    basedn: string;

    constructor() {
        super("__hpcc_id");
    }

    put(row, options) {
        this.get(row.__hpcc_id);
        const retVal = super.put(row, options);
        const request = {
            BasednName: row.BasednName,
            rname: row.rname,
            account_name: row.account_name,
            action: "update"
        };
        lang.mixin(request, row);
        PermissionAction({
            request
        });
        return retVal;
    }

    query(query, options) {
        const data = [];
        const results = all([
            this.refreshAccountPermissions()
        ]).then(lang.hitch(this, function (response) {
            const accountPermissions = {};
            arrayUtil.forEach(response[0], function (item, idx) {
                accountPermissions[item.account_name] = item;
                data.push(lang.mixin(item, {
                    __hpcc_type: "IndividualPermissions",
                    __hpcc_id: this.name + CONCAT_SYMBOL + idx,
                    BasednName: this.basedn,
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
    }

    refreshAccountPermissions() {
        return ResourcePermissions({
            request: {
                name: this.name ? this.name : "",
                BasednName: this.basedn
            }
        }).then(lang.hitch(this, function (response) {
            if (lang.exists("ResourcePermissionsResponse.Permissions.Permission", response)) {
                return response.ResourcePermissionsResponse.Permissions.Permission;
            }
            return [];
        }));
    }
}

class PermissionsStore extends Memory {

    service = "ws_access";
    action = "Permissions";
    responseQualifier = "BasednsResponse.Basedns.Basedn";
    groupname: string;
    username: string;

    constructor() {
        super("__hpcc_id");
    }

    get(id) {
        const tmp = id.split(CONCAT_SYMBOL);
        if (tmp.length > 0) {
            const parentID = tmp[0];
            const parent = super.get(parentID);
            if (tmp.length === 1) {
                return parent;
            }
            const child = parent.children.get(id);
            if (child) {
                return child;
            }
            return parent;
        }
        return null;
    }

    putChild(row) {
        const parent = row.__hpcc_parent;
        return parent.children.put(row);
    }

    getChildren(parent, options) {
        return parent.children.query();
    }

    mayHaveChildren(object) {
        return object.__hpcc_type === "Permission";
    }

    query(query, options) {
        const deferredResults = new Deferred();
        deferredResults.total = new Deferred();
        Permissions().then(lang.hitch(this, function (response) {
            const data = [];
            if (lang.exists("BasednsResponse.Basedns.Basedn", response)) {
                arrayUtil.forEach(response.BasednsResponse.Basedns.Basedn, function (item, idx) {
                    data.push(lang.mixin(item, {
                        __hpcc_type: "Permission",
                        __hpcc_id: item.basedn,
                        DisplayName: item.name,
                        Basedn: item.name,
                        children: lang.mixin(CreateResourcesStore(this.groupname, this.username, item.name, item.rname), {
                            parent: this,
                            parentRow: item,
                            Basedn: item.name
                        })
                    }));
                }, this);
            }
            options = options || {};
            this.setData(SimpleQueryEngine({}, { sort: options.sort })(data));
            deferredResults.resolve(this.data);
            deferredResults.total.resolve(this.data.length);
        }));
        return QueryResults(deferredResults);
    }
}

export function checkError(response, sourceMethod, showOkMsg) {
    const retCode = lang.getObject(sourceMethod + "Response.retcode", false, response);
    const retMsg = lang.getObject(sourceMethod + "Response.retmsg", false, response);
    if (retCode) {
        topic.publish("hpcc/brToaster", {
            Severity: "Error",
            Source: "WsAccess." + sourceMethod,
            Exceptions: [{ Message: retMsg }]
        });
        logger.error(retMsg);
    } else if (showOkMsg && retMsg) {
        topic.publish("hpcc/brToaster", {
            Severity: "Message",
            Source: "WsAccess." + sourceMethod,
            Exceptions: [{ Message: retMsg }]
        });
        logger.info(retMsg);
    }
}

export function _doCall(action, params?) {
    return ESPRequest.send("ws_access", action, params).then(function (response) {
        checkError(response, action, params ? params.showOkMsg : false);
        return response;
    });
}

export function Users(params) {
    return _doCall("UserQuery", params);
}

export function UserAction(params) {
    return _doCall("UserAction", params);
}

export function AddUser(params) {
    return _doCall("AddUser", params);
}

export function UserEdit(params) {
    return _doCall("UserEdit", params);
}

export function UserInfoEditInput(params) {
    return _doCall("UserInfoEditInput", params);
}

export function UserInfoEdit(params) {
    return _doCall("UserInfoEdit", params);
}

export function UserResetPass(params) {
    return _doCall("UserResetPass", params);
}

export function UserGroupEdit(params) {
    return _doCall("UserGroupEdit", params);
}

export function UserGroupEditInput(params) {
    return _doCall("UserGroupEditInput", params);
}

export function GroupAdd(params) {
    return _doCall("GroupAdd", params);
}

export function GroupAction(params) {
    return _doCall("GroupAction", params);
}

export function GroupEdit(params) {
    return _doCall("GroupEdit", params);
}

export function GroupMemberEdit(params) {
    return _doCall("GroupMemberEdit", params);
}

export function Groups(params) {
    return _doCall("GroupQuery", params);
}

export function Members(params) {
    return _doCall("GroupEdit", params);
}

export function GroupMemberEditInput(params) {
    return _doCall("GroupMemberEditInput", params);
}

export function GroupMemberQuery(params) {
    return _doCall("GroupMemberQuery", params);
}

export function Permissions(params?) {
    return _doCall("Permissions", params);
}

export function AccountPermissions(params) {
    return _doCall("AccountPermissions", params);
}

export function AccountPermissionsV2(params) {
    return _doCall("AccountPermissionsV2", params);
}

export function ResourcePermissions(params) {
    return _doCall("ResourcePermissions", params);
}

export function Resources(params) {
    return _doCall("Resources", {
        request: {
            BasednName: params.request.name
        }
    });
}

export function ResourceAdd(params) {
    return _doCall("ResourceAdd", params);
}

export function ResourceDelete(params) {
    return _doCall("ResourceDelete", params);
}

export function PermissionAction(params) {
    return _doCall("PermissionAction", params);
}

export function FilePermission(params) {
    return _doCall("FilePermission", params);
}

export function ClearPermissionsCache() {
    return _doCall("ClearPermissionsCache", {
        request: {
            action: "Clear Permissions Cache"
        }
    });
}

export function EnableScopeScans() {
    return _doCall("EnableScopeScans", {
        request: {
            action: "Enable Scope Scans"
        }
    });
}

export function DisableScopeScans() {
    return _doCall("DisableScopeScans", {
        request: {
            action: "Disable Scope Scans"
        }
    });
}

export function CheckFilePermissions() {
    return _doCall("FilePermission", {
        request: {
            action: "Check File Permission"
        }
    });
}

export function CreateUsersStore(groupname, observable) {
    const store = new UsersStore();
    store.groupname = groupname;
    if (observable) {
        return new Observable(store);
    }
    return store;
}

export function CreateGroupsStore(username, observable) {
    const store = new GroupsStore();
    store.username = username;
    if (observable) {
        return new Observable(store);
    }
    return store;
}

export function CreatePermissionsStore(groupname, username) {
    const store = new PermissionsStore();
    store.groupname = groupname;
    store.username = username;
    return new Observable(store);
}

export function CreateAccountPermissionsStore(IsGroup, IncludeGroup, AccountName) {
    const store = new AccountResourcesStore();
    store.IsGroup = IsGroup;
    store.IncludeGroup = IncludeGroup;
    store.AccountName = AccountName;
    return new Observable(store);
}

export function CreateInheritedPermissionsStore(IsGroup, IncludeGroup, AccountName, TabName) {
    const store = new InheritedPermissionStore();
    store.IsGroup = IsGroup;
    store.IncludeGroup = IncludeGroup;
    store.AccountName = AccountName;
    store.TabName = TabName;
    return new Observable(store);
}

export function CreateIndividualPermissionsStore(basedn, name) {
    const store = new IndividualPermissionsStore();
    store.basedn = basedn;
    store.name = name;
    return new Observable(store);
}

export function CreateResourcesStore(groupname, username, basedn, name) {
    const store = new ResourcesStore();
    store.groupname = groupname;
    store.username = username;
    store.basedn = basedn;
    store.name = name;
    return new Observable(store);
}

const service = new AccessService({ baseUrl: "" });
const emptyStore = { data: [], total: 0 };

export type GroupStore = BaseStore<WsAccess.GroupQueryRequest, WsAccess.Group>;

export function CreateGroupStore(): BaseStore<WsAccess.GroupQueryRequest, WsAccess.Group> {
    const store = new Paged<WsAccess.GroupQueryRequest, WsAccess.Group>({
        start: "PageStartFrom",
        count: "PageSize",
        sortBy: "SortBy",
        descending: "Descending"
    }, "Name", request => {
        try {
            return service.GroupQuery(request).then(response => {
                return {
                    data: response?.Groups?.Group ?? [],
                    total: response?.TotalGroups ?? 0
                };
            });
        } catch (err) {
            logger.error(err);
            return Promise.resolve(emptyStore);
        }
    });
    return store;
}

export type UserStore = BaseStore<WsAccess.GroupRequest, WsAccess.User>;

export function CreateUserStore(): BaseStore<WsAccess.UserQueryRequest, WsAccess.User> {
    const store = new Paged<WsAccess.UserQueryRequest, WsAccess.User>({
        start: "PageStartFrom",
        count: "PageSize",
        sortBy: "SortBy",
        descending: "Descending"
    }, "username", request => {
        try {
            return service.UserQuery(request).then(response => {
                return {
                    data: response?.Users?.User ?? [],
                    total: response?.TotalUsers ?? 0
                };
            });
        } catch (err) {
            logger.error(err);
            return Promise.resolve(emptyStore);
        }
    });
    return store;
}

export type GroupMemberStore = BaseStore<WsAccess.GroupRequest, WsAccess.User>;

export function CreateGroupMemberStore(): BaseStore<WsAccess.GroupMemberQueryRequest, WsAccess.User> {
    const store = new Paged<WsAccess.GroupMemberQueryRequest, WsAccess.User>({
        start: "PageStartFrom",
        count: "PageSize",
        sortBy: "SortBy",
        descending: "Descending"
    }, "username", request => {
        try {
            return service.GroupMemberQuery(request).then(response => {
                return {
                    data: response?.Users?.User ?? [],
                    total: response?.TotalUsers ?? 0
                };
            });
        } catch (err) {
            logger.error(err);
            return Promise.resolve(emptyStore);
        }
    });
    return store;
}
