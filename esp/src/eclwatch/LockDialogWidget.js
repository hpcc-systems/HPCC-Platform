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
    "dojo/cookie",
    "dojo/topic",
    "dojo/query",

    "dijit/registry",
    "dijit/form/Select",
    "dijit/form/CheckBox",

    "hpcc/_Widget",
    "src/Utility",
    "src/ws_account",
    "src/ESPUtil",

    "dojo/text!../templates/LockDialogWidget.html",

    "dijit/Dialog",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/TextBox",
    "dijit/form/ValidationTextBox",

    "hpcc/TableContainer"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domForm, domClass, on, domStyle, xhr, keys, cookie, topic, query,
    registry, Select, CheckBox,
    _Widget, Utility, WsAccount, ESPUtil,
    template) {
        return declare("LockDialogWidget", [_Widget], {
            templateString: template,
            baseClass: "LockDialogWidget",
            i18n: nlsHPCC,

            _width: "480px",
            lockDialogWidget: null,
            storage: null,

            postCreate: function (args) {
                this.inherited(arguments);
                this.unlockDialog = registry.byId(this.id + "UnlockDialog");
                this.tableContainer = registry.byId(this.id + "TableContainer");
                this.unlockUserName = registry.byId(this.id + "UnlockUserName");
                this.unlockPassword = registry.byId(this.id + "UnlockPassword");
                this.unlockForm = registry.byId(this.id + "UnlockForm");
                this.unlockStatus = dom.byId(this.id + "UnlockStatus");
                this.storage = new ESPUtil.LocalStorage();
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            show: function (event) {
                var context = this;
                on(this.unlockPassword, "keypress", function (event) {
                    if (event.key === "Enter") {
                        context._onUnlock();
                    }
                });

                this.unlockDialog.show();
                domClass.add("SessionLock", "overlay");
                this.unlockUserName.set("value", dojoConfig.username);

                topic.publish("hpcc/session_management_status", {
                    status: "Locked"
                });
            },

            hide: function (event) {
                domClass.remove("SessionLock", "overlay");
                this.unlockDialog.hide();
                this.unlockDialog.destroyRecursive()
                dojo.query(".dijitDialogUnderlayWrapper").forEach(function(node){
                    dojo.destroy(node.id);
                });
                dojo.query(".unlockDialogToHide").forEach(function(node){
                    dojo.destroy(node.id);
                });
            },

            _onUnlock: function (event) {
                var context = this;

                if (this.unlockForm.validate()) {
                    cookie("Status", "login_attempt");
                    WsAccount.Unlock({
                        request: {
                            username: this.unlockUserName.get("value"),
                            password: this.unlockPassword.get("value")
                        }
                    }).then(function (response) {

                        if (response.UnlockResponse.Error === 0) {
                            if (context.unlockStatus.innerHTML !== "") {
                                context.unlockStatus.innerHTML = "";
                            }
                            domClass.remove("SessionLock", "overlay");
                            context.unlockDialog.hide();
                            context.unlockDialog.destroyRecursive();
                            topic.publish("hpcc/session_management_status", {
                                status: "Unlocked"
                            });
                            cookie("Status", "Unlocked");
                            context.storage.removeItem("Status");
                            context.storage.setItem("Status", "Unlocked");
                        } else {
                            context.unlockStatus.innerHTML = response.UnlockResponse.Message;
                            cookie("Status", "Locked");
                        }
                    });
                }
            },

            _onLock: function (event) {
                var context = this;

                on(this.unlockPassword, "keypress", function (event) {
                    if (event.key === "Enter") {
                        context._onUnlock();
                    }
                });

                if (cookie("Status") === "Unlocked") {
                    xhr("esp/lock", {
                        method: "post",
                    }).then(function(response){
                        if (response) {
                            context.unlockDialog.show();
                            domClass.add("SessionLock", "overlay");
                            context.unlockUserName.set("value", dojoConfig.username);
                            topic.publish("hpcc/session_management_status", {
                                status: "Locked"
                            });
                            cookie("Status", "Locked");
                            context.storage.removeItem("Status");
                            context.storage.setItem("Status", "Locked");
                        }
                    });
                }
            },

            init: function (params) {
                if (this.inherited(arguments))
                    return;
            }
        });
    });
