define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/dom-form",

    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "src/Clippy",
    "src/ws_access",

    "dojo/text!../templates/UserDetailsWidget.html",

    "hpcc/DelayLoadWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/form/Form",
    "dijit/form/Textarea",
    "dijit/form/TextBox",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/Dialog",

    "dojox/form/PasswordValidator"

], function (declare, lang, nlsHPCCMod, domForm,
    registry,
    _TabContainerWidget, Clippy, WsAccess,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("UserDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "UserDetailsWidget",
        i18n: nlsHPCC,

        summaryWidget: null,
        memberOfWidget: null,
        permissionsWidget: null,
        activePermissionsWidget: null,
        user: null,

        getTitle: function () {
            return this.i18n.UserDetails;
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.testWidget = registry.byId(this.id + "_Test");
            this.memberOfWidget = registry.byId(this.id + "_MemberOf");
            this.permissionsWidget = registry.byId(this.id + "_UserPermissions");
            this.activePermissionsWidget = registry.byId(this.id + "_ActivePermissions");
            this.userForm = registry.byId(this.id + "UserForm");

            Clippy.attach(this.id + "ClippyButton");
        },

        //  Hitched actions  ---
        _onSave: function (event) {
            var context = this;
            if (this.userForm.validate()) {
                var formInfo = domForm.toObject(this.id + "UserForm");
                WsAccess.UserInfoEdit({
                    showOkMsg: true,
                    request: {
                        username: this.user,
                        firstname: formInfo.firstname,
                        lastname: formInfo.lastname,
                        employeeID: formInfo.employeeID,
                        employeeNumber: formInfo.employeeNumber
                    }
                });

                if (formInfo.newPassword) {
                    WsAccess.UserResetPass({
                        showOkMsg: true,
                        request: {
                            username: this.user,
                            newPassword: formInfo.newPassword,
                            newPasswordRetype: formInfo.newPassword
                        }
                    }).then(function (response) {
                        if (lang.exists("UserResetPassResponse", response)) {
                            if (response.UserResetPassResponse.retcode === 0) {
                                context.getLatestUserData(context.user);
                            }
                        }
                    });
                }
            }
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.user = params.Username;
            if (this.user) {
                this.updateInput("User", null, this.user);
                this.updateInput("EmployeeID", null, params.EmployeeID);
                this.updateInput("EmployeeNumber", null, params.EmployeeNumber);
                this.updateInput("Username", null, this.user);
                this.updateInput("PasswordExpiration", null, params.Passwordexpiration);

                this.getLatestUserData(this.user);
            }
        },

        initTab: function () {
            var currSel = this.getSelectedChild();

            if (currSel.id === this.memberOfWidget.id) {
                this.memberOfWidget.init({
                    username: this.user
                });
            } else if (currSel.id === this.permissionsWidget.id) {
                this.permissionsWidget.init({
                    username: this.user
                });
            } else if (currSel.id === this.activePermissionsWidget.id) {
                this.activePermissionsWidget.init({
                    IsGroup: false,
                    IncludeGroup: true,
                    AccountName: this.user
                });
            }
        },

        getLatestUserData: function (user) {
            var context = this;
            WsAccess.UserInfoEditInput({
                request: {
                    username: user
                }
            }).then(function (response) {
                if (lang.exists("UserInfoEditInputResponse.firstname", response)) {
                    context.updateInput("FirstName", null, response.UserInfoEditInputResponse.firstname);
                }
                if (lang.exists("UserInfoEditInputResponse.lastname", response)) {
                    context.updateInput("LastName", null, response.UserInfoEditInputResponse.lastname);
                }
                if (lang.exists("UserInfoEditInputResponse.employeeID", response)) {
                    context.updateInput("EmployeeID", null, response.UserInfoEditInputResponse.employeeID);
                }
                if (lang.exists("UserInfoEditInputResponse.employeeNumber", response)) {
                    context.updateInput("EmployeeNumber", null, response.UserInfoEditInputResponse.employeeNumber);
                }
                if (lang.exists("UserInfoEditInputResponse.PasswordExpiration", response)) {
                    context.updateInput("PasswordExpiration", null, response.UserInfoEditInputResponse.PasswordExpiration);
                }
            });
        }
    });
});
