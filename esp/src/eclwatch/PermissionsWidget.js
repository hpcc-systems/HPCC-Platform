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

    "dijit/registry",
    "dijit/form/CheckBox",

    "dgrid/tree",
    "dgrid/editor",

    "hpcc/GridDetailsWidget",
    "src/ws_access",
    "src/ESPUtil"

], function (declare, lang, i18n, nlsHPCC,
                registry, CheckBox,
                tree, editor,
                GridDetailsWidget, WsAccess, ESPUtil) {
    return declare("PermissionsWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_Permissions,
        idProperty: "__hpcc_id",

        //  Hitched Actions  ---
        _onRefresh: function (args) {
            this.grid.refresh();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.store = WsAccess.CreatePermissionsStore(params.groupname, params.username);
            this.grid.setStore(this.store);
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    DisplayName: tree({
                        label: this.i18n.Resource,
                        formatter: function (_name, row) {
                            return _name;
                        }
                    }),
                    allow_access: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) { return object.__hpcc_type !== "Permission"; },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.AllowAccess;
                        }
                    }, CheckBox),
                    allow_read: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) { return object.__hpcc_type !== "Permission"; },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.AllowRead;
                        }
                    }, CheckBox),
                    allow_write: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) { return object.__hpcc_type !== "Permission"; },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.AllowWrite;
                        }
                    }, CheckBox),
                    allow_full: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) { return object.__hpcc_type !== "Permission"; },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.AllowFull;
                        }
                    }, CheckBox),
                    padding: {
                        width:20,
                        label: " "
                    },
                    deny_access: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) {
                            if (object.__hpcc_type === "Permission" || object.AccountName === "Administrators") {
                                return false
                            }
                            return true;
                        },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.DenyAccess
                        }
                    }, CheckBox),
                    deny_read: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) {
                            if (object.__hpcc_type === "Permission" || object.AccountName === "Administrators") {
                                return false
                            }
                            return true;
                        },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.DenyRead
                        }
                    }, CheckBox),
                    deny_write: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) {
                            if (object.__hpcc_type === "Permission" || object.AccountName === "Administrators") {
                                return false
                            }
                            return true;
                        },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.DenyWrite
                        }
                    }, CheckBox),
                    deny_full: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) {
                            if (object.__hpcc_type === "Permission" || object.AccountName === "Administrators") {
                                return false
                            }
                            return true;
                        },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.DenyFull
                        }
                    }, CheckBox)
                }
            }, domID);

            retVal.on("dgrid-datachange", function (evt) {
                evt.preventDefault();
                context.calcPermissionState(evt.cell.column.field, evt.value, evt.cell.row.data);
                evt.grid.store.putChild(evt.cell.row.data);
            });
            return retVal;
        },

        calcPermissionState: function(field, value, row) {
            switch (field) {
                case "allow_access":
                    row.allow_full = value && row.allow_read && row.allow_write;
                    if (value)
                        this.calcPermissionState("deny_access", false, row);
                    break;
                case "allow_read":
                    row.allow_full = row.allow_access && value && row.allow_write;
                    if (value)
                        this.calcPermissionState("deny_read", false, row);
                    break;
                case "allow_write":
                    row.allow_full = row.allow_access && row.allow_read && value;
                    if (value)
                        this.calcPermissionState("deny_write", false, row);
                    break;
                case "allow_full":
                    row.allow_access = value;
                    row.allow_read = value;
                    row.allow_write = value;
                    if (value)
                        this.calcPermissionState("deny_full", false, row);
                    break;
                case "deny_access":
                    row.deny_full = value && row.deny_read && row.deny_write;
                    if (value)
                        this.calcPermissionState("allow_access", false, row);
                    break;
                case "deny_read":
                    row.deny_full = row.deny_access && value && row.deny_write;
                    if (value)
                        this.calcPermissionState("allow_read", false, row);
                    break;
                case "deny_write":
                    row.deny_full = row.deny_access && row.deny_read && value;
                    if (value)
                        this.calcPermissionState("allow_write", false, row);
                    break;
                case "deny_full":
                    row.deny_access = value;
                    row.deny_read = value;
                    row.deny_write = value;
                    if (value)
                        this.calcPermissionState("allow_full", false, row);
                    break;
            }
            row[field] = value;
        },

        refreshActionState: function (selection) {
            registry.byId(this.id + "Open").set("disabled", true);
        }
    });
});