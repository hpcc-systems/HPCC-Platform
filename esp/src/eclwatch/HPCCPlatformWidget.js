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
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-style",
    "dojo/cookie",

    "dijit/registry",
    "dijit/Tooltip",

    "dojox/widget/UpgradeBar",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPRequest",
    "hpcc/ESPActivity",
    "hpcc/ws_account",
    "hpcc/ws_access",
    "hpcc/WsDfu",
    "hpcc/WsSMC",
    "hpcc/GraphWidget",
    "hpcc/DelayLoadWidget",

    "dojo/text!../templates/HPCCPlatformWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/StackContainer",
    "dijit/layout/StackController",
    "dijit/layout/ContentPane",
    "dijit/form/DropDownButton",
    "dijit/form/TextBox",
    "dijit/form/Textarea",
    "dijit/Dialog",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "hpcc/HPCCPlatformMainWidget",
    "hpcc/TableContainer",
    "hpcc/InfoGridWidget"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domStyle, cookie,
                registry, Tooltip,
                UpgradeBar,
                _TabContainerWidget, ESPRequest, ESPActivity, WsAccount, WsAccess, WsDfu, WsSMC, GraphWidget, DelayLoadWidget,
                template) {
    return declare("HPCCPlatformWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "HPCCPlatformWidget",
        i18n: nlsHPCC,

        banner: "",
        upgradeBar: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.searchText = registry.byId(this.id + "FindText");
            this.aboutDialog = registry.byId(this.id + "AboutDialog");
            this.setBannerDialog = registry.byId(this.id + "SetBannerDialog");
            this.stackContainer = registry.byId(this.id + "TabContainer");
            this.mainPage = registry.byId(this.id + "_Main");
            this.errWarnPage = registry.byId(this.id + "_ErrWarn");

            this.upgradeBar = new UpgradeBar({
                notifications: [],
                noRemindButton: ""
            });
        },

        startup: function (args) {
            this.inherited(arguments);
            domStyle.set(dom.byId(this.id + "StackController_stub_ErrWarn").parentNode.parentNode, {
                visibility: "hidden"
            });
            domStyle.set(dom.byId(this.id + "StackController_stub_Config").parentNode.parentNode, {
                visibility: "hidden"
            });
        },

        //  Implementation  ---
        parseBuildString: function (build) {
            if (!build) {
                return;
            }
            this.build = {};
            this.build.orig = build;
            this.build.prefix = "";
            this.build.postfix = "";
            var verArray = build.split("[");
            if (verArray.length > 1) {
                this.build.postfix = verArray[1].split("]")[0];
            }
            verArray = verArray[0].split("_");
            if (verArray.length > 1) {
                this.build.prefix = verArray[0];
                verArray.splice(0, 1);
            }
            this.build.version = verArray.join("_");
        },

        refreshBanner: function (banner) {
            if (this.banner !== banner) {
                this.banner = banner;
                this.upgradeBar.notify("<div style='text-align:center'><b>" + banner + "</b></div>");
                this.upgradeBar.show();
            }
        },

        refreshUserName: function () {
            var userDisplay = this.userName ? this.userName : "";

            var total = 0;
            var myTotal = 0;
            arrayUtil.forEach(this.spaceUsage, function (item, idx) {
                var itemTotal = item.TotalSize.split(",").join("");
                total += parseInt(itemTotal);
                if (item.Name === this.userName) {
                    myTotal = parseInt(itemTotal);
                }
            }, this);
            this.userUsage = (myTotal / total) * 100;

            if (this.userUsage) {
                userDisplay += " (" + (Math.round(this.userUsage * 100) / 100) + "%)"
            }
            dom.byId(this.id + "UserID").innerHTML = userDisplay;
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            registry.byId(this.id + "SetBanner").set("disabled", true);

            var context = this;
            WsAccount.MyAccount({
            }).then(function (response) {
                if (lang.exists("MyAccountResponse.username", response)) {
                    context.userName = response.MyAccountResponse.username;
                    context.checkIfAdmin(context.username);
                    context.refreshUserName();
                    if (!cookie("PasswordExpiredCheck")) {
                        cookie("PasswordExpiredCheck", "true", { expires: 1 });
                        if (lang.exists("MyAccountResponse.passwordDaysRemaining", response) &&
                            response.MyAccountResponse.passwordDaysRemaining !== null &&
                            response.MyAccountResponse.passwordDaysRemaining >= 0 &&
                            response.MyAccountResponse.passwordDaysRemaining <= 10) {
                            if (confirm(context.i18n.PasswordExpirePrefix + response.MyAccountResponse.passwordDaysRemaining + context.i18n.PasswordExpirePostfix)) {
                                context._onUserID();
                            }
                        }
                    }
                }
            });

            WsDfu.DFUSpace({
                request: {
                    CountBy: "Owner"
                }
            }).then(function (response) {
                if (lang.exists("DFUSpaceResponse.DFUSpaceItems.DFUSpaceItem", response)) {
                    context.spaceUsage = response.DFUSpaceResponse.DFUSpaceItems.DFUSpaceItem;
                    context.refreshUserName();
                }
            });

            this.activity = ESPActivity.Get();
            this.activity.watch("Build", function (name, oldValue, newValue) {
                context.parseBuildString(newValue);
            });
            this.activity.watch("BannerContent", function (name, oldValue, newValue) {
                context.refreshBanner(newValue);
            });

            this.createStackControllerTooltip(this.id + "_Main", this.i18n.Activity);
            this.createStackControllerTooltip(this.id + "_ECL", this.i18n.ECL);
            this.createStackControllerTooltip(this.id + "_Files", this.i18n.Files);
            this.createStackControllerTooltip(this.id + "_RoxieQueries", this.i18n.PublishedQueries);
            this.createStackControllerTooltip(this.id + "_OPS", this.i18n.Operations);
            this.initTab();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.init) {
                    currSel.init({});
                }
            }
        },

        getTitle: function () {
            return "HPCC Platform";
        },

        checkIfAdmin: function (user) {
            var context = this;
            if(user == null){
                registry.byId(context.id + "SetBanner").set("disabled", false);
            }else{
                WsAccess.UserEdit({
                    request: {
                        username: user
                    }
                }).then(function (response) {
                    if (lang.exists("UserEditResponse.Groups.Group", response)) {
                        arrayUtil.forEach(response.UserEditResponse.Groups.Group, function (item, idx) {
                            if(item.name == "Administrators"){
                                registry.byId(context.id + "SetBanner").set("disabled", false);
                                return true;
                            }
                        });
                    }
                });
            }
        },

        //  Hitched actions  ---
        _onUserID: function (evt) {
            var userDialog = registry.byId(this.id + "UserDialog");
            var userInfo = registry.byId(this.id + "UserInfo");
            if (!userInfo.init({ Username: this.userName })) {
                userInfo.refresh();
            }
            userDialog.show();
        },

        _onFind: function (evt) {
            var context = this;
            this.stackContainer.selectChild(this.mainPage);
            this.mainPage.ensureWidget().then(function (mainPage) {
                mainPage.widget.TabContainer.selectChild(mainPage.widget._Search);
                mainPage.widget._Search.ensureWidget().then(function (searchPage) {
                    searchPage.doSearch(context.searchText.get("value"));
                });
            });
        },

        _onOpenLegacy: function (evt) {
            var win = window.open("/?legacy", "_blank");
            if (win && win.focus) {
                win.focus();
            }
        },

        _onOpenResources: function (evt) {
            var win = window.open("http://hpccsystems.com/download", "_blank");
            if (win && win.focus) {
                win.focus();
            }
        },

        _onOpenReleaseNotes: function (evt) {
            var win = window.open("http://hpccsystems.com/download/free-community-edition-known-limitations#" + this.build.version, "_blank");
            if (win && win.focus) {
                win.focus();
            }
        },

        _onOpenConfiguration: function (evt) {
            var context = this;
            if (!this.configText) {
                ESPRequest.send("main", "", {
                    request: {
                        config_: "",
                        PlainText: "yes"
                    },
                    handleAs: "text"
                }).then(function(response) {
                    context.configText = context.formatXml(response);
                    context.configSourceCM = CodeMirror.fromTextArea(dom.byId(context.id + "ConfigTextArea"), {
                        tabMode: "indent",
                        matchBrackets: true,
                        gutter: true,
                        lineNumbers: true,
                        mode: "xml",
                        readOnly: true,
                        onGutterClick: CodeMirror.newFoldFunction(CodeMirror.tagRangeFinder)
                    });
                    context.configSourceCM.setValue(context.configText);
                }); 
            }
            this.stackContainer.selectChild(this.widget._Config);
        },
        
        _onOpenErrWarn: function (evt) {
            this.stackContainer.selectChild(this.errWarnPage);
        },

        _ondebugLanguageFiles: function () {
            var context = this;
            require(["hpcc/nls/hpcc"], function (lang) {
                var languageID = [];
                var languageRequire = [];
                for (var key in lang) {
                    if (key !== "root") {
                        languageID.push(key);
                        languageRequire.push("hpcc/nls/" + key + "/hpcc");
                    }
                }
                require(languageRequire, function () {
                    var errWarnGrid = registry.byId(context.id + "ErrWarnGrid");
                    arrayUtil.forEach(arguments, function (otherLang, idx) {
                        var langID = languageID[idx];
                        for (var key in lang.root) {
                            if (!otherLang[key]) {
                                errWarnGrid.loadTopic({
                                    Severity: "Error",
                                    Source: context.i18n.Missing,
                                    Exceptions: [{
                                        Code: langID,
                                        FileName: languageRequire[idx] + ".js - " + key,
                                        Message: "'" + lang.root[key] + "'",
                                        Javascript: key + ": \"\","
                                    }]
                                }, true);
                            } else if (otherLang[key] === lang.root[key]) {
                                errWarnGrid.loadTopic({
                                    Severity: /[a-z]/.test(otherLang[key]) ? "Warning" : "Info",
                                    Source: context.i18n.EnglishQ,
                                    Exceptions: [{
                                        Code: langID,
                                        FileName: languageRequire[idx] + ".js - " + key,
                                        Message: otherLang[key],
                                        Javascript: key + ": \"\","
                                    }]
                                }, true);
                            }
                        }
                    });
                    errWarnGrid.refreshTopics();
                });
            });
            this.stackContainer.selectChild(this.errWarnPage);
        },

        _onAboutLoaded: false,
        _onAbout: function (evt) {
            if (!this._onAboutLoaded) {
                this._onAboutLoaded = true;
                dom.byId(this.id + "ServerVersion").value = this.build.version;
                var gc = new GraphWidget({
                    id: this.id + "GraphControl"
                }).placeAt(this.aboutDialog);
                var context = this;
                gc.checkPluginLoaded().then(function () {
                    dom.byId(context.id + "GraphControlVersion").value = gc.getVersion();
                    gc.destroyRecursive();
                })
            }
            this.aboutDialog.show();
        },

        _onAboutClose: function (evt) {
            this.aboutDialog.hide();
        },

        _onSetBanner: function (evt) {
            dom.byId(this.id + "BannerText").value = this.banner;
            this.setBannerDialog.show();
        },

        _onSetBannerOk: function (evt) {
            this.activity.setBanner(dom.byId(this.id + "BannerText").value);
            this.setBannerDialog.hide();
        },

        _onSetBannerCancel: function (evt) {
            this.setBannerDialog.hide();
        },

        createStackControllerTooltip: function (widgetID, text) {
            return new Tooltip({
                connectId: [this.id + "StackController_" + widgetID],
                label: text,
                showDelay: 1,
                position: ["below"]
            });
        }
    });
});
