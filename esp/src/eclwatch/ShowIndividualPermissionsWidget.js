define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/query",
    "src/nlsHPCC",
    "dojo/dom-form",

    "dijit/registry",
    "dijit/form/CheckBox",

    "dgrid/editor",
    "dgrid/selector",

    "hpcc/_TabContainerWidget",
    "src/ws_access",
    "src/ESPUtil",
    "src/Utility",

    "dojo/text!../templates/ShowIndividualPermissionsWidget.html",

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
    "dijit/TooltipDialog",

    "hpcc/TableContainer"

], function (declare, lang, query, nlsHPCCMod, domForm,
    registry, CheckBox,
    editor, selector,
    _TabContainerWidget, WsAccess, ESPUtil, Utility, template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("ShowIndividualPermissionsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "ShowIndividualPermissionsWidget",
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_Permissions,
        idProperty: "__hpcc_id",

        postCreate: function (args) {
            this.inherited(arguments);
            this.addGroupForm = registry.byId(this.id + "AddGroupForm");
        },

        //  Hitched Actions  ---
        _onRefresh: function (args) {
            this.grid.refresh();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;
            this.store = WsAccess.CreateIndividualPermissionsStore(params.Basedn, params.Name);
            this.grid = this.createGrid(this.id + "Grid");
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                sort: [{ attribute: "account_name" }],
                columns: {
                    check: selector({
                        width: 27,
                        label: " "
                    }, "radio"),
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
                            node.innerHTML = Utility.wrapStringWithTag(context.i18n.AllowAccess, "center").split(" ").join("<br />");
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
                            node.innerHTML = Utility.wrapStringWithTag(context.i18n.AllowRead, "center").split(" ").join("<br />");
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
                            node.innerHTML = Utility.wrapStringWithTag(context.i18n.AllowWrite, "center").split(" ").join("<br />");
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
                            node.innerHTML = Utility.wrapStringWithTag(context.i18n.AllowFull, "center").split(" ").join("<br />");
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
                            node.innerHTML = Utility.wrapStringWithTag(context.i18n.DenyAccess, "center").split(" ").join("<br />");
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
                            node.innerHTML = Utility.wrapStringWithTag(context.i18n.DenyRead, "center").split(" ").join("<br />");
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
                            node.innerHTML = Utility.wrapStringWithTag(context.i18n.DenyWrite, "center").split(" ").join("<br />");
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
                            node.innerHTML = Utility.wrapStringWithTag(context.i18n.DenyFull, "center").split(" ").join("<br />");
                        }
                    }, CheckBox)
                }
            }, domID);

            retVal.on("dgrid-datachange", function (evt) {
                evt.preventDefault();
                context.calcPermissionState(evt.cell.column.field, evt.value, evt.cell.row.data);
                evt.grid.store.put(evt.cell.row.data);
                const t = window.setTimeout(() => { context.grid.refresh(); window.clearTimeout(t); }, 100);
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

        _onSubmitAddGroupDialog: function (event) {
            if (this.addGroupForm.validate()) {
                var context = this;
                var request = domForm.toObject(this.addGroupForm.id);
                lang.mixin(request, {
                    action: "update",
                    account_type: "1",
                    rname: this.params.Name,
                    BasednName: this.params.Basedn
                });
                WsAccess.PermissionAction({
                    request: request
                }).then(function (response) {
                    if (lang.exists("PermissionActionResponse.retcode", response) && response.PermissionActionResponse.retcode === 0) {
                        context.grid.refresh();
                    }
                    return response;
                });
                registry.byId(this.id + "AddGroupsDropDown").closeDropDown();
            }
        },

        _onDelete: function (event) {
            var context = this;
            var selection = this.grid.getSelected();
            var list = this.arrayToList(selection, "account_name");
            if (confirm(this.i18n.DeleteSelectedGroups + "\n" + list)) {
                var request = {
                    ActionType: "delete",
                    BasednName: this.params.Basedn,
                    rname: this.params.Name,
                    "account_name": selection[0].account_name
                };
                WsAccess.PermissionAction({
                    request: request
                }).then(function (response) {
                    context.grid.refresh();
                });
            }
        }
    });
});
