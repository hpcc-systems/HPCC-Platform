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

    "dijit/registry",
    "dijit/Tooltip",

    "dojox/widget/UpgradeBar",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPActivity",
    "hpcc/ws_account",
    "hpcc/ws_access",
    "hpcc/WsSMC",
    "hpcc/GraphWidget",

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

    "hpcc/HPCCPlatformMainWidget",
    "hpcc/HPCCPlatformECLWidget",
    "hpcc/HPCCPlatformFilesWidget",
    "hpcc/HPCCPlatformRoxieWidget",
    "hpcc/HPCCPlatformOpsWidget",
    "hpcc/TableContainer",
    "hpcc/InfoGridWidget"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domStyle,
                registry, Tooltip,
                UpgradeBar,
                _TabContainerWidget, ESPActivity, WsAccount, WsAccess, WsSMC, GraphWidget,
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
            this.searchPage = registry.byId(this.id + "_Main" + "_Search");
            this.stackContainer = registry.byId(this.id + "TabContainer");
            this.mainPage = registry.byId(this.id + "_Main");
            this.errWarnPage = registry.byId(this.id + "_ErrWarn");
            this.mainStackContainer = registry.byId(this.mainPage.id + "TabContainer");
            this.searchPage = registry.byId(this.id + "_Main" + "_Search");

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

        init: function (params) {
            if (this.inherited(arguments))
                return;
            var context = this;
            registry.byId(context.id + "SetBanner").set("disabled", true);

            WsAccount.MyAccount({
            }).then(function (response) {
                if (lang.exists("MyAccountResponse.username", response)) {
                    dom.byId(context.id + "UserID").innerHTML = response.MyAccountResponse.username;
                    context.checkIfAdmin(response.MyAccountResponse.username);
                }
            });

            this.activity = ESPActivity.Get();
            this.activity.watch("Build", function (name, oldValue, newValue) {
                context.parseBuildString(newValue);
            });
            this.activity.watch("BannerContent", function (name, oldValue, newValue) {
                context.refreshBanner(newValue);
            });

            this.createStackControllerTooltip(this.id + "_ECL", this.i18n.ECL);
            this.createStackControllerTooltip(this.id + "_Files", this.i18n.files);
            this.createStackControllerTooltip(this.id + "_RoxieQueries", this.i18n.publishedQueries);
            this.createStackControllerTooltip(this.id + "_OPS", this.i18n.operations);
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
        _onFind: function (evt) {
            this.stackContainer.selectChild(this.mainPage);
            this.mainStackContainer.selectChild(this.searchPage);
            this.searchPage.doSearch(this.searchText.get("value"));
        },

        _onOpenLegacy: function (evt) {
            var win = window.open("/?legacy", "_blank");
            win.focus();
        },

        _onOpenReleaseNotes: function (evt) {
            var win = window.open("http://hpccsystems.com/download/free-community-edition-known-limitations#" + this.build.version, "_blank");
            win.focus();
        },

        _onOpenErrWarn: function (evt) {
            this.stackContainer.selectChild(this.errWarnPage);
        },

        _onAboutLoaded: false,
        _onAbout: function (evt) {
            if (!this._onAboutLoaded) {
                this._onAboutLoaded = true;
                dom.byId(this.id + "ServerVersion").value = this.build.version;
                var gc = registry.byId(this.id + "GraphControl");
                dom.byId(this.id + "GraphControlVersion").value = gc.getVersion();
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
