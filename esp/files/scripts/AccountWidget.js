/*##############################################################################
#	HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#	Licensed under the Apache License, Version 2.0 (the "License");
#	you may not use this file except in compliance with the License.
#	You may obtain a copy of the License at
#
#	   http://www.apache.org/licenses/LICENSE-2.0
#
#	Unless required by applicable law or agreed to in writing, software
#	distributed under the License is distributed no an "AS IS" BASIS,
#	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#	See the License for the specific language governing permissions and
#	limitations under the License.
############################################################################## */
define([
    "exports",
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/query",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/form/Form",
    "dijit/form/SimpleTextarea",
    "dijit/form/TextBox",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/TitlePane",
    "dijit/registry",

    "dojox/form/PasswordValidator",

    "hpcc/_TabContainerWidget",
    "hpcc/DFUWUDetailsWidget",
    "hpcc/WsAccount",

    "dojo/text!../templates/AccountWidget.html",

    "dijit/TooltipDialog"
], function (exports, declare, lang, arrayUtil, dom, domAttr, domClass, domForm, query,
                _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, Toolbar, TooltipDialog, Form, SimpleTextarea, TextBox, Button, DropDownButton, TitlePane, registry,
                PasswordValidator,
                _TabContainerWidget, DFUWUDetailsWidget, WsAccount, /*UsersWidget, GroupsWidget, PermissionsWidget,*/
                template) {
    exports.fixCircularDependency = declare("AccountWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "AccountWidget",
        borderContainer: null,
        tabContainer: null,

        logicalFile: null,
        prevState: "",
        initalized: false,

        postCreate: function (args) {
            this.inherited(arguments);
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
        
        },

        _onChangePass: function (name) {
           /*var wuTab = this.ensurePane(this.id + "_Users", {
                name: "Change Password"
            });
            this.selectChild(wuTab);*/
        },

        _onSubmit: function (event) {
            var context = this;
            WsAccount.UpdateUser({
               request: domForm.toObject(this.id + "credentials")
            }).then(function (response) {
                if(lang.exists("UpdateUserResponse.message", response)){
                    dojo.publish("hpcc/brToaster", {
                            message: "<p>" + response.UpdateUserResponse.message + "</p>",
                            type: "error",
                            duration: -1
                        });
                }
            });
        },

        _onClear: function (event) {
            var context = this;
            dojo.forEach(registry.byId(this.id + "credentials").getDescendants(), function(widget) {
            if (widget.id == context.id + "OldPassword" || widget.id == context.id + "NewPassword" || widget.id == context.id + "VerifyPassword"){
                     widget.attr('value', null);
                }
            });
        },

        //  Implementation  ---
        init: function (params) {
            var context = this;
            WsAccount.MyAccount({
                 request: {}
            }).then(function (response) {
                if(lang.exists("MyAccountResponse.username", response)){
                    context.updateInput("UserName", null, response.MyAccountResponse.username);
                    context.updateInput("FirstName", null, response.MyAccountResponse.firstName);
                    context.updateInput("LastName", null, response.MyAccountResponse.lastName);
                    context.updateInput("PasswordExpiration", null, response.MyAccountResponse.passwordExpiration);
                }
            });
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

        ensurePane: function (id, params) {
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                
                this.addChild(retVal);
            }
            return retVal;
        }
    });
});
