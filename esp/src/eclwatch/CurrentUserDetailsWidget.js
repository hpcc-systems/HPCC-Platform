define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/dom-form",

    "dijit/registry",

    "hpcc/_Widget",
    "src/ws_account",

    "dojo/text!../templates/CurrentUserDetailsWidget.html",

    "dijit/form/Form",
    "dijit/form/Textarea",
    "dijit/form/TextBox",
    "dijit/form/Button",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/Dialog",

    "dojox/form/PasswordValidator"

], function (declare, lang, i18n, nlsHPCC, dom, domForm,
    registry,
    _Widget, WsAccount,
    template) {
    return declare("CurrentUserDetailsWidget", [_Widget], {
        templateString: template,
        baseClass: "CurrentUserDetailsWidget",
        i18n: nlsHPCC,

        user: null,

        getTitle: function () {
            return this.i18n.UserDetails;
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.userForm = registry.byId(this.id + "UserForm");
        },

        resize: function (args) {
            this.inherited(arguments);
            this.widget.BorderContainer.resize();
        },

        //  Hitched actions  ---
        _onSave: function (event) {
            var dialog = dijit.byId("stubUserDialog");
            if (this.userForm.validate()) {
                var formInfo = domForm.toObject(this.id + "UserForm");
                WsAccount.UpdateUser({
                    showOkMsg: true,
                    request: {
                        username: this.user,
                        oldpass: formInfo.oldPassword,
                        newpass1: formInfo.newPassword,
                        newpass2: formInfo.newPassword
                    }
                });
                dialog.hide();
            }
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.user = params.Username;
            this.refresh();
        },

        refresh: function () {
            if (this.user) {
                this.updateInput("User", null, this.user);
                this.updateInput("Username", null, this.user);

                var context = this;
                WsAccount.MyAccount({
                }).then(function (response) {
                    if (lang.exists("MyAccountResponse.firstName", response)) {
                        context.updateInput("FirstName", null, response.MyAccountResponse.firstName);
                    }
                    if (lang.exists("MyAccountResponse.employeeID", response)) {
                        context.updateInput("EmployeeID", null, response.MyAccountResponse.employeeID);
                    }
                    if (lang.exists("MyAccountResponse.lastName", response)) {
                        context.updateInput("LastName", null, response.MyAccountResponse.lastName);
                    }
                    if (lang.exists("MyAccountResponse.passwordExpiration", response)) {
                        context.updateInput("PasswordExpiration", null, response.MyAccountResponse.passwordExpiration);
                    }
                });
            }
        }
    });
});
