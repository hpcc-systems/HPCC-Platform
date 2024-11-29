define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/promise/all",

    "dijit/registry",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/Dialog",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/ws_access",
    "src/ESPUtil",
    "hpcc/TargetSelectWidget"

], function (declare, lang, nlsHPCCMod, arrayUtil, all,
    registry, Button, ToolbarSeparator, Dialog,
    selector,
    GridDetailsWidget, WsAccess, ESPUtil, TargetSelectWidget) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("MemberOfWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_MemberOf,
        idProperty: "name",
        currentUser: null,

        //  Hitched Actions  ---
        _onRefresh: function (args) {
            this.refreshGrid();
        },

        //  Implementation  ---
        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;

            this.refreshGrid();
            this.memberDropDown = new TargetSelectWidget({});
            this.memberDropDown.init({
                loadUsersNotAMemberOfAGroup: true,
                username: params.username
            });
            this.dialog.addChild(this.memberDropDown);

        },

        createGrid: function (domID) {
            var context = this;
            this.openButton = registry.byId(this.id + "Open");
            this.refreshButton = registry.byId(this.id + "Refresh");
            this.addButton = new Button({
                id: this.id + "Add",
                label: this.i18n.Add,
                onClick: function (event) {
                    context._onAddMember(event);
                }
            }).placeAt(this.openButton.domNode, "after");
            this.deleteButton = new Button({
                id: this.id + "Delete",
                label: this.i18n.Delete,
                disabled: true,
                onClick: function (event) {
                    context._onDeleteMember(event);
                }
            }).placeAt(this.addButton.domNode, "after");
            var tmpSplitter = new ToolbarSeparator().placeAt(this.addButton.domNode, "before");
            this.dialog = new Dialog({
                title: this.i18n.PleaseSelectAGroupToAddUser,
                style: "width: 320px;"
            });
            this.dialogBtn = new Button({
                label: this.i18n.Add,
                style: "float:right;",
                onClick: function (event) {
                    context._onSubmitAddMember(event);
                }
            }).placeAt(this.dialog.domNode);

            var retVal = new declare([ESPUtil.Grid(true, true)])({
                sort: [{ attribute: "name" }],
                store: this.store,
                columns: {
                    check: selector({
                        width: 27,
                        label: " "
                    }, "checkbox"),
                    name: {
                        label: this.i18n.GroupName,
                        sortable: true
                    }
                }
            }, domID);

            retVal.onSelectionChanged(function (event) {
                context.refreshActionState();
            });

            return retVal;
        },

        _onAddMember: function (event) {
            this.dialog.show();
        },

        _onSubmitAddMember: function (event) {
            var context = this;
            WsAccess.UserGroupEdit({
                request: {
                    groupnames_i1: context.memberDropDown.get("value"),
                    username: context.params.username,
                    action: "Add"
                }
            }).then(function (response) {
                context.dialog.hide();
                context._onRefresh();
            });
        },

        _onDeleteMember: function (event) {
            var context = this;
            var selections = this.grid.getSelected();
            if (confirm(this.i18n.YouAreAboutToRemoveUserFrom)) {
                var promises = [];
                arrayUtil.forEach(selections, function (row, idx) {
                    promises.push(WsAccess.UserGroupEdit({
                        request: {
                            groupnames_i6: row.name,
                            username: context.params.username,
                            action: "Delete"
                        }
                    }));
                });
                all(promises).then(function () {
                    context._onRefresh();
                });
            }
        },

        refreshGrid: function () {
            var context = this;
            WsAccess.UserEdit({
                request: {
                    username: context.params.username
                }
            }).then(function (response) {
                var results = [];

                if (lang.exists("UserEditResponse.Groups.Group", response)) {
                    results = response.UserEditResponse.Groups.Group;
                    arrayUtil.forEach(results, function (row, idx) {
                        lang.mixin(row, {
                            name: row.name
                        });
                    });
                }

                context.store.setData(results);
                context.grid.set("query", {});
            });
        },

        refreshActionState: function (selection) {
            registry.byId(this.id + "Open").set("disabled", true);
            var selection = this.grid.getSelected();
            var hasUserSelection = selection.length;

            this.deleteButton.set("disabled", !hasUserSelection);
        }
    });
});
