/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
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
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-form",
    "dojo/dom-class",
    "dojo/on",
    "dojo/dom-style",
    "dojo/request/xhr",
    "dojo/keys",

    "dijit/registry",
    "dijit/form/Select",
    "dijit/form/CheckBox",

    "hpcc/_Widget",
    "src/Utility",
    "src/ws_account",

    "dojo/text!../templates/LockDialogWidget.html",

    "dijit/Dialog",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/TextBox",
    "dijit/form/ValidationTextBox",

    "hpcc/TableContainer"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domForm, domClass, on, domStyle, xhr, keys,
                registry, Select, CheckBox,
                _Widget, Utility, WsAccount,
                template) {
    return declare("LockDialogWidget", [_Widget], {
        templateString: template,
        baseClass: "LockDialogWidget",
        i18n: nlsHPCC,

        _width:"480px",
        lockDialogWidget: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.unlockDialog = registry.byId(this.id + "UnlockDialog");
            this.tableContainer = registry.byId(this.id + "TableContainer");
            this.unlockUserName = registry.byId(this.id + "UnlockUserName");
            this.unlockPassword = registry.byId(this.id + "UnlockPassword");
            this.unlockBtn = registry.byId(this.id + "UnlockBtn");
            this.unlockForm = registry.byId(this.id + "UnlockForm");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        show: function (event) {
            this.unlockDialog.show();
            domClass.add("SessionLock", "overlay");
            dojo.removeClass(this.id + "UnlockDialog_underlay", "dijitDialogUnderlay _underlay");
            this.unlockUserName.set("value", dojoConfig.username);
            this._onLock();
        },

        hide: function (event) {
            this.unlockDialog.hide();
        },

        _onUnlock: function (event) {
            var context = this;

            if (this.unlockForm.validate()) {
                WsAccount.Unlock({
                    request: {
                        username: this.unlockUserName.get("value"),
                        password: this.unlockPassword.get("value")
                    }
                }).then(function (response) {
                    var status = dom.byId("UnlockStatus");

                    if (response.UnlockResponse.Error === 0) {
                        if (status.innerHTML !== "") {
                            status.innerHTML = "";
                        }
                        context.hide();
                        context.unlockDialog.destroyRecursive();
                        domClass.remove("SessionLock", "overlay");
                    } else {
                        status.innerHTML = response.UnlockResponse.Message
                    }
                });
            }
        },

        _onLock: function (event) {
            xhr("esp/lock", {
                method: "post",
            });
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;
        }
    });
});
