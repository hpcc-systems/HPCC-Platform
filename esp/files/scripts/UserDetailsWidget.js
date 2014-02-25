/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/common",
    "dojo/i18n!./nls/UserQueryWidget",
    "dojo/dom",
    "dojo/dom-attr",

    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/_TabContainerWidget",
    "hpcc/WsAccess",
    "hpcc/MemberOfWidget",
    "hpcc/PermissionsWidget",

    "dojo/text!../templates/UserDetailsWidget.html",

    "dijit/form/Textarea",
    "dijit/form/TextBox",
    "dijit/form/Button",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/Dialog"

], function (declare, lang, i18n, nlsCommon, nlsSpecific, dom, domAttr,
                registry,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                _TabContainerWidget, WsAccess, MemberOfWidget, PermissionsWidget,
                template) {
    return declare("UserDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "UserDetailsWidget",
        i18n: lang.mixin(nlsCommon, nlsSpecific),

        summaryWidget: null,
        memberOfWidget: null,
        permissionsWidget: null,
        user: null,

        getTitle: function () {
            return this.i18n.UserDetails;
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.memberOfWidget = registry.byId(this.id + "_MemberOf");
            this.permissionsWidget = registry.byId(this.id + "_UserPermissions");
        },

        //  Hitched actions  ---
        _onSave: function (event) {
            WsAccess.UserInfoEdit({
                request: {
                    username: this.user,
                    firstname: registry.byId(this.id + "FirstName").get("value"),
                    lastname: registry.byId(this.id + "LastName").get("value")
                }
            });

            var newPassword = registry.byId(this.id + "NewPassword").get("value");
            var newPasswordRetype = registry.byId(this.id + "VerifyPassword").get("value");
            if (newPassword || newPasswordRetype) {
                WsAccess.UserResetPass({
                    request: {
                        username: this.user,
                        newPassword: newPassword,
                        newPasswordRetype: newPasswordRetype
                    }
                });
            }
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.user = params.Username;
            if (this.user) {
                this.updateInput("User", null, this.user);
                this.updateInput("Username", null, this.user);
                this.updateInput("PasswordExpiration", null, params.Passwordexpiration);

                var context = this;
                WsAccess.UserInfoEditInput({
                    request: {
                        username: this.user
                    }
                }).then(function (response) {
                    if (lang.exists("UserInfoEditInputResponse.firstname", response)) {
                        context.updateInput("FirstName", null, response.UserInfoEditInputResponse.firstname);
                    }
                    if (lang.exists("UserInfoEditInputResponse.lastname", response)) {
                        context.updateInput("LastName", null, response.UserInfoEditInputResponse.lastname);
                    }
                });
            }
        },

        initTab: function () {
            var currSel = this.getSelectedChild();

            if (currSel.id == this.memberOfWidget.id) {
                this.memberOfWidget.init({
                    username: this.user
                });
            } else if (currSel.id == this.permissionsWidget.id) {
                this.permissionsWidget.init({
                    username: this.user
                });
            }
        }
    });
});
