/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/dom-style",
    "dojo/dom-geometry",
    "dojo/cookie",
    "dojo/topic",

    "dijit/registry",
    "dijit/Tooltip",

    "dojox/widget/UpgradeBar",
    "dojox/widget/ColorPicker",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPRequest",
    "hpcc/ESPActivity",
    "hpcc/ws_account",
    "hpcc/ws_access",
    "hpcc/WsSMC",
    "hpcc/WsTopology",
    "hpcc/GraphWidget",
    "hpcc/DelayLoadWidget",
    "hpcc/ws_machine",

    "dojo/text!../templates/HPCCPlatformWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/StackContainer",
    "dijit/layout/StackController",
    "dijit/layout/ContentPane",
    "dijit/form/Form",
    "dijit/form/DropDownButton",
    "dijit/form/TextBox",
    "dijit/form/Textarea",
    "dijit/form/CheckBox",
    "dijit/Dialog",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "hpcc/HPCCPlatformMainWidget",
    "hpcc/TableContainer",
    "hpcc/InfoGridWidget"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domClass, domForm, domStyle, domGeo, cookie, topic,
                registry, Tooltip,
                UpgradeBar, ColorPicker,
                _TabContainerWidget, ESPRequest, ESPActivity, WsAccount, WsAccess, WsSMC, WsTopology, GraphWidget, DelayLoadWidget, WsMachine,
                template) {
    return declare("HPCCPlatformWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "HPCCPlatformWidget",
        i18n: nlsHPCC,

        bannerContent: "",
        upgradeBar: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.searchText = registry.byId(this.id + "FindText");
            this.aboutDialog = registry.byId(this.id + "AboutDialog");
            this.setBannerDialog = registry.byId(this.id + "SetBannerDialog");
            this.stackContainer = registry.byId(this.id + "TabContainer");
            this.mainPage = registry.byId(this.id + "_Main");
            this.errWarnPage = registry.byId(this.id + "_ErrWarn");
            this.pluginsPage = registry.byId(this.id + "_Plugins");
            this.operationsPage = registry.byId(this.id + "_OPS");
            registry.byId(this.id + "SetBanner").set("disabled", true);

            this.upgradeBar = new UpgradeBar({
                notifications: [],
                noRemindButton: ""
            });
        },

        startup: function (args) {
            this.inherited(arguments);
            domStyle.set(dom.byId(this.id + "StackController_stub_Plugins").parentNode.parentNode, {
                visibility: "hidden"
            });
            domStyle.set(dom.byId(this.id + "StackController_stub_ErrWarn").parentNode.parentNode, {
                visibility: "hidden"
            });
            domStyle.set(dom.byId(this.id + "StackController_stub_Config").parentNode.parentNode, {
                visibility: "hidden"
            });
        },

        //  Implementation  ---
        refreshBanner: function (activity) {
            if (this.showBanner !== activity.ShowBanner ||
                this.bannerContent !== activity.BannerContent ||
                this.bannerScroll !== activity.BannerScroll ||
                this.bannerColor !== activity.BannerColor ||
                this.bannerSize !== activity.BannerSize) {

                this.showBanner = activity.ShowBanner;
                this.bannerContent = activity.BannerContent;
                this.bannerScroll = activity.BannerScroll;
                this.bannerColor = activity.BannerColor;
                this.bannerSize = activity.BannerSize;
                if (this.showBanner) {
                    var msg = "<marquee id='" + this.id + "Marquee' width='100%' direction='left' scrollamount='" + activity.BannerScroll + "' style='color:" + activity.BannerColor + ";font-size:" + ((activity.BannerSize / 2) * 100) + "%'>" + activity.BannerContent + "</marquee>";
                    this.upgradeBar.notify(msg);
                    var marquee = dom.byId(this.id + "Marquee");
                    var height = domGeo.getContentBox(marquee).h;
                    domStyle.set(this.upgradeBar.domNode, "height", height + "px");
                    domStyle.set(marquee.parentNode, { top: "auto", "margin-top": "auto" });
                } else {
                    this.upgradeBar.notify("");
                    domStyle.set(this.upgradeBar.domNode, "height", "0px");
                }
            }
        },

        refreshUserName: function () {
            dom.byId(this.id + "UserID").innerHTML = this.userName ? this.userName : "";
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;

            WsMachine.GetComponentStatus({
                request: {}
            }).then(function (response) {
                if (lang.exists("GetComponentStatusResponse.ComponentStatus", response)) {
                    var status = response.GetComponentStatusResponse.ComponentStatus
                    context.checkMonitoring(status);
                }
            });

            WsAccount.MyAccount({
            }).then(function (response) {
                if (lang.exists("MyAccountResponse.username", response)) {
                    context.userName = response.MyAccountResponse.username;
                    context.checkIfAdmin(context.userName);
                    context.refreshUserName();
                    if (!cookie("PasswordExpiredCheck")) {
                        cookie("PasswordExpiredCheck", "true", { expires: 1 });
                        if (lang.exists("MyAccountResponse.passwordDaysRemaining", response)) {
                            switch (response.MyAccountResponse.passwordDaysRemaining) {
                                case null:
                                    break;
                                case -1:
                                    alert(context.i18n.PasswordExpired);
                                    context._onUserID();
                                    break;
                                case -2:
                                    break;
                                default:
                                    if (response.MyAccountResponse.passwordDaysRemaining <= response.MyAccountResponse.passwordExpirationWarningDays) {
                                        if (confirm(context.i18n.PasswordExpirePrefix + response.MyAccountResponse.passwordDaysRemaining + context.i18n.PasswordExpirePostfix)) {
                                            context._onUserID();
                                        }
                                    }
                                    break;
                            }
                        }
                    }
                }
            });

            WsTopology.TpGetServicePlugins({
                request: {
                }
            }).then(function (response) {
                if (lang.exists("TpGetServicePluginsResponse.Plugins.Plugin", response) && response.TpGetServicePluginsResponse.Plugins.Plugin.length) {
                    domStyle.set(dom.byId(context.id + "StackController_stub_Plugins").parentNode.parentNode, {
                        visibility: "visible"
                    });
                }
            });

            this.activity = ESPActivity.Get();
            this.activity.watch("Build", function (name, oldValue, newValue) {
                context.build = WsSMC.parseBuildString(newValue);
            });
            this.activity.watch("changedCount", function (name, oldValue, newValue) {
                context.refreshBanner(context.activity);
            });

            this.createStackControllerTooltip(this.id + "_Main", this.i18n.Activity);
            this.createStackControllerTooltip(this.id + "_ECL", this.i18n.ECL);
            this.createStackControllerTooltip(this.id + "_Files", this.i18n.Files);
            this.createStackControllerTooltip(this.id + "_RoxieQueries", this.i18n.PublishedQueries);
            this.createStackControllerTooltip(this.id + "_OPS", this.i18n.Operations);
            this.createStackControllerTooltip(this.id + "_Plugins", this.i18n.Plugins);
            this.initTab();

            topic.subscribe("hpcc/monitoring_component_update", function (topic) {
                context.checkMonitoring(topic.status);
            });
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
            return "ECL Watch";
        },

        checkMonitoring: function (status) {
            if (status) {
                domClass.remove("MonitorStatus");
                domClass.add("MonitorStatus", status);
            }
        },

        checkIfAdmin: function (user) {
            var context = this;
            if(user == null){
                registry.byId(context.id + "SetBanner").set("disabled", false);
                dojo.destroy(this.monitorStatus);
            }else{
                WsAccess.UserEdit({
                    suppressExceptionToaster: true,
                    request: {
                        username: user
                    }
                }).then(function (response) {
                    if (lang.exists("UserEditResponse.Groups.Group", response)) {
                        arrayUtil.some(response.UserEditResponse.Groups.Group, function (item, idx) {
                            if(item.name == "Administrators" || "Directory Administrators"){
                                dojoConfig.isAdmin = true;
                                registry.byId(context.id + "SetBanner").set("disabled", false);
                                if (context.widget._OPS.refresh) {
                                    context.widget._OPS.refresh();
                                }
                                return false;
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

        _openNewTab: function(url) {
            var win = window.open(url, "_blank");
            if (win && win.focus) {
                win.focus();
            }
        },

        _onOpenLegacy: function (evt) {
            this._openNewTab("/?legacy");
        },

        _onOpenResources: function (evt) {
            this._openNewTab("http://hpccsystems.com/download");
        },

        _onOpenReleaseNotes: function (evt) {
            this._openNewTab("http://hpccsystems.com/download/free-community-edition-known-limitations#" + this.build.version);
        },

        _onOpenTransitionGuide: function(evt) {
            this._openNewTab("https://wiki.hpccsystems.com/display/hpcc/HPCC+ECL+Watch+5.0+Transition+Guide");
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
                        lineNumbers: true,
                        mode: "xml",
                        readOnly: true,
                        foldGutter: true,
                        gutters: ["CodeMirror-linenumbers", "CodeMirror-foldgutter"]
                    });
                    context.configSourceCM.setSize("100%", "100%");
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

        _onMonitoring: function (evt) {
            this.stackContainer.selectChild(this.operationsPage);
            this.operationsPage.ensureWidget().then(function (operationsPage) {
                operationsPage.widget._Topology.ensureWidget().then(function (topologyPage) {  //  This is needed otherwise topology will steal focus the first time it is delay loaded
                    operationsPage.selectChild(operationsPage.widget._Monitoring);
                });
            });
        },

        _onSetBanner: function (evt) {
            registry.byId(this.id + "ShowBanner").set("value", this.activity.ShowBanner);
            dom.byId(this.id + "BannerContent").value = this.activity.BannerContent;
            dom.byId(this.id + "BannerColor").value = this.activity.BannerColor;
            dom.byId(this.id + "BannerSize").value = this.activity.BannerSize;
            dom.byId(this.id + "BannerScroll").value = this.activity.BannerScroll;
            this.setBannerDialog.show();
        },

        _onSetBannerOk: function (evt) {
            this.activity.setBanner(domForm.toObject(this.id + "SetBannerForm"));
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
