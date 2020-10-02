define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/promise/all",
    "dojo/on",

    "dijit/registry",
    "dijit/form/Button",
    "dijit/form/ValidationTextBox",
    "dijit/ToolbarSeparator",
    "dijit/Dialog",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/ws_access",
    "src/ESPUtil",
    "src/Utility",
    "hpcc/TargetSelectWidget"

], function (declare, lang, nlsHPCCMod, arrayUtil, all, on,
    registry, Button, ValidationTextBox, ToolbarSeparator, Dialog,
    selector,
    GridDetailsWidget, WsAccess, ESPUtil, Utility, TargetSelectWidget) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("MembersWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_Members,
        idProperty: "username",

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.refreshGrid();
            this.memberDropDown = new TargetSelectWidget({});
            this.memberDropDown.init({
                loadUsersNotInGroup: true,
                groupname: params.groupname
            });
            this.dialog.addChild(this.memberDropDown);
        },

        createGrid: function (domID) {
            var context = this;
            this.openButton = registry.byId(this.id + "Open");
            this.newPage = registry.byId(this.id + "NewPage")
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
            this.downloadButton = new Button({
                id: this.id + "DownloadtoCSV",
                style: "float:right;",
                label: this.i18n.DownloadToCSV,
                disabled: true,
                onClick: function (event) {
                    context._onAddDownloadMember();
                }
            }).placeAt(this.newPage.domNode, "after");
            var tmpSplitter = new ToolbarSeparator().placeAt(this.addButton.domNode, "before");

            this.dialog = new Dialog({
                title: this.i18n.PleaseSelectAUserToAdd,
                style: "width: 300px;"
            });
            this.dialogBtn = new Button({
                label: this.i18n.Add,
                style: "float:right;",
                onClick: function (event) {
                    context._onSubmitAddMember(event);
                }
            }).placeAt(this.dialog.domNode);

            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                sort: [{ attribute: "fullname" }],
                columns: {
                    check: selector({
                        width: 27,
                        label: " "
                    }, "checkbox"),
                    username: {
                        width: 180,
                        label: this.i18n.Username,
                        sortable: true
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
            WsAccess.GroupMemberEdit({
                request: {
                    groupname: context.params.groupname,
                    usernames_i7: context.memberDropDown.get("value"),
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
                    promises.push(WsAccess.GroupMemberEdit({
                        request: {
                            groupname: context.params.groupname,
                            usernames_i1: row.username,
                            action: "Delete"
                        }
                    }));
                });
                all(promises).then(function () {
                    context._onRefresh();
                });
            }
        },

        _onAddDownloadMember: function (event) {
            var context = this;

            this.dialog = new Dialog({
                title: this.i18n.Members,
                style: "width: 480px; height: 100px"
            });
            this.dialog.show();

            this.dialogValidationTextBox = new ValidationTextBox({
                placeHolder: context.i18n.FileName,
                required: true,
                style: "float:left; margin-left: 10px; width: 70%"
            }).placeAt(this.dialog.domNode, "second");

            this.dialogButton = new Button({
                style: "float:right; padding: 0 10px 10px 20px;",
                innerHTML: context.i18n.Submit,
                disabled: true,
                onClick: function () {
                    context._buildCSV();
                }
            }).placeAt(this.dialog.domNode, "last");

            on(this.dialogValidationTextBox, "keyup", function (event) {
                if (context.dialogValidationTextBox.get("value") !== "") {
                    context.dialogButton.set("disabled", false);
                } else {
                    context.dialogButton.set("disabled", true);
                }
            });
        },

        _buildCSV: function (event) {
            var selections = this.grid.getSelected();
            var row = [];
            var fileName = this.dialogValidationTextBox.get("value") + ".csv";

            arrayUtil.forEach(selections, function (cell, idx) {
                var rowData = [cell.username, cell.employeeID, cell.employeeNumber, cell.fullname, cell.passwordexpiration];
                row.push(rowData);
            });

            Utility.downloadToCSV(this.grid, row, fileName);
            this.dialog.hide();
        },

        _onRefresh: function () {
            this.refreshGrid();
        },

        refreshActionState: function (selection) {
            registry.byId(this.id + "Open").set("disabled", true);
            var selection = this.grid.getSelected();
            var hasUserSelection = selection.length;

            this.deleteButton.set("disabled", !hasUserSelection);
            this.downloadButton.set("disabled", !hasUserSelection);
        },

        refreshGrid: function () {
            var context = this;
            WsAccess.GroupMemberQuery({
                request: {
                    GroupName: context.params.groupname
                }
            }).then(function (response) {
                var results = [];

                if (lang.exists("GroupMemberQueryResponse.Users.User", response)) {
                    results = response.GroupMemberQueryResponse.Users.User;
                    arrayUtil.forEach(results, function (row, idx) {
                        lang.mixin(row, {
                            username: row.username
                        });
                    });
                }

                context.store.setData(results);
                context.grid.set("query", {});
            });
        }
    });
});
