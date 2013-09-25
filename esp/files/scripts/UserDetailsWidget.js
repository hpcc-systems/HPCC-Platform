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
    "dojo/dom",
    "dojo/dom-form",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/query",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
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
    "hpcc/UserPermissionsWidget",

    "dojo/text!../templates/UserDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/Dialog",

    "dojox/layout/TableContainer",
    "dojox/form/PasswordValidator"
], function (declare, lang, dom, domForm, domAttr, domClass, query, Memory, Observable,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                _TabContainerWidget, WsAccess, MemberOfWidget, UserPermissionsWidget,
                template) {
    return declare("UserDetailsWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "UserDetailsWidget",
        initalized: false,
        summaryWidget: null,
        memberOfWidget:null,
        memberOfWidgetLoaded:false,
        userPermissionsWidget:null,
        userPermissionsWidgetLoaded:false,
        user:null,


        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.memberOfWidget = registry.byId(this.id + "_MemberOf");
            this.userPermissionsWidget = registry.byId(this.id + "_UserPermissions");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return "User Details";
        },

        //  Hitched actions  ---
        _onSave: function (event) {
           
        },

        //  Implementation  ---
        init: function (params) {
            var context = this;
            if (this.initalized)
                return;
            this.initalized = true;
            this.user = params.Username;

            if (params.Username) {
                this.summaryWidget.set("title", "User Summary");
                context.updateInput("User", null, this.user);
                context.updateInput("PasswordExpiration", null, params.Passwordexpiration);
            }

             WsAccess.UserInfoEditInput({
               request: {
                username: this.user
               }
             }).then(function (response) {
                if(lang.exists("UserInfoEditInputResponse.firstname", response)){
                    context.updateInput("FirstName", null, response.UserInfoEditInputResponse.firstname);
                }
                if(lang.exists("UserInfoEditInputResponse.lastname", response)){
                    context.updateInput("LastName", null, response.UserInfoEditInputResponse.lastname);
                }
            });

            this.memberOfWidget.init({
                username: this.user
            });

            this.userPermissionsWidget.init({
                AccountName: this.user
            });
        },

        initTab: function () {
            if (!this.wu) {
                return
            }
        },

        _onCancelDialog: function (nodeName) {
            registry.byId(this.id + "EditDialog").hide();
            registry.byId(this.id + "PasswordDialog").hide();
        },

        _onPassword: function (params) {
            var context = this;
            registry.byId(this.id + "PasswordDialog").show();
            context.updateInput("PasswordUsername", null, this.user);
        },

        _onEdit: function (params) {
            var context = this;
            registry.byId(this.id + "EditDialog").show();
            context.updateInput("UsersUsername", null, this.user);

             WsAccess.UserInfoEditInput({
               request: domForm.toObject(this.id + "EditForm")
             }).then(function (response) {
                if(lang.exists("UserInfoEditInputResponse.firstname", response)){
                    context.updateInput("UsersFirstName", null, response.UserInfoEditInputResponse.firstname);
                }
                if(lang.exists("UserInfoEditInputResponse.lastname", response)){
                    context.updateInput("UsersLastName", null, response.UserInfoEditInputResponse.lastname);
                }
            });
        },

        _onEditSubmit: function (event) {
            var context = this;
            context.updateInput("UsersUsername", null, this.user);
             
             WsAccess.UserInfoEdit({
               request: domForm.toObject(this.id + "EditForm")
            }).then(function (response) {
                if(lang.exists("UserInfoEditResponse.retmsg" == "", response)){
                    dojo.publish("hpcc/brToaster", {
                            message: "<p>User password updated successfully</p>",
                            type: "error",
                            duration: -1
                    });
                }
            });
            context.updateInput("FirstName", null, registry.byId(this.id + "UsersFirstName").value);
            context.updateInput("LastName", null, registry.byId(this.id + "UsersLastName").value);
            registry.byId(this.id + "EditDialog").hide();
        },

        _onPasswordSubmit: function (event) {
            var context = this;
            context.updateInput("PasswordUsername", null, this.user);
            WsAccess.UserResetPass({
               request: domForm.toObject(this.id + "PasswordForm")
            }).then(function (response) {
                if(lang.exists("UserResetPassResponse.retcode", response)){
                    dojo.publish("hpcc/brToaster", {
                            message: "<p>" + response.UserResetPassResponse.retmsg + "</p>",
                            type: "error",
                            duration: -1
                    });
                }
            });
            registry.byId(this.id + "PasswordDialog").hide();

        },

        updateInput: function (name, oldValue, newValue) {
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            } else {
                var domElem = dom.byId(this.id + name);
                if (domElem) {
                    switch (domElem.tagName) {
                        case "SPAN":
                        case "DIV":
                            domAttr.set(this.id + name, "innerHTML", newValue);
                            break;
                        case "INPUT":
                        case "TEXTAREA":
                            domAttr.set(this.id + name, "value", newValue);
                            break;
                        default:
                            alert(domElem.tagName);
                    }
                }
            }
        },

        refreshActionState: function () {

        }
    });
});
