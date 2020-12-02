define([
    "dojo/_base/declare",
    "src/nlsHPCC",

    "dijit/registry",
    "dijit/form/CheckBox",

    "dgrid/editor",

    "hpcc/GridDetailsWidget",
    "src/ws_access",
    "src/ESPUtil"

], function (declare, nlsHPCCMod,
    registry, CheckBox,
    editor,
    GridDetailsWidget, WsAccess, ESPUtil) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("ShowIndividualPermissionsWidget", [GridDetailsWidget], {
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
            this.store = WsAccess.CreateIndividualPermissionsStore(params.Basedn, params.Name);
            this.grid.setStore(this.store);
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                sort: [{ attribute: "account_name" }],
                columns: {
                    account_name: {
                        label: this.i18n.Account,
                        formatter: function (_name, row) {
                            return _name;
                        }
                    },
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
                        width: 20,
                        label: " "
                    },
                    deny_access: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) { return object.__hpcc_type !== "Permission"; },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.DenyAccess;
                        }
                    }, CheckBox),
                    deny_read: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) { return object.__hpcc_type !== "Permission"; },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.DenyRead;
                        }
                    }, CheckBox),
                    deny_write: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) { return object.__hpcc_type !== "Permission"; },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.DenyWrite;
                        }
                    }, CheckBox),
                    deny_full: editor({
                        width: 54,
                        editor: "checkbox",
                        editorArgs: { value: true },
                        className: "hpccCentered",
                        autoSave: true,
                        canEdit: function (object, value) { return object.__hpcc_type !== "Permission"; },
                        renderHeaderCell: function (node) {
                            node.innerHTML = context.i18n.DenyFull;
                        }
                    }, CheckBox)
                }
            }, domID);

            retVal.on("dgrid-datachange", function (evt) {
                evt.preventDefault();
                context.calcPermissionState(evt.cell.column.field, evt.value, evt.cell.row.data);
                evt.grid.store.put(evt.cell.row.data);
            });
            return retVal;
        },

        calcPermissionState: function (field, value, row) {
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
