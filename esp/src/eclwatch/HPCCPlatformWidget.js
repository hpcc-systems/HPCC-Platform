define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-class",
    "dojo/dom-form",
    "dojo/dom-style",
    "dojo/dom-geometry",
    "dojo/cookie",
    "dojo/query",
    "dojo/topic",
    "dojo/request/xhr",

    "dijit/registry",
    "dijit/Tooltip",

    "dojox/widget/UpgradeBar",
    "dojox/widget/ColorPicker",

    "@hpcc-js/codemirror",
    "src/react/index",
    "src-react/components/About",
    "src-react/components/controls/ComingSoon",

    "hpcc/_TabContainerWidget",
    "src/ESPRequest",
    "src/ESPActivity",
    "src/ESPUtil",
    "src/ws_account",
    "src/ws_access",
    "src/WsSMC",
    "src/WsTopology",
    "src/ws_machine",
    "hpcc/LockDialogWidget",
    "src/UserPreferences/EnvironmentTheme",
    "src/Utility",

    "dojo/text!../templates/HPCCPlatformWidget.html",

    /* DEBUG_ONLY
    "src/nls/bs/hpcc",
    "src/nls/es/hpcc",
    "src/nls/fr/hpcc",
    "src/nls/hr/hpcc",
    "src/nls/hu/hpcc",
    "src/nls/pt-br/hpcc",
    "src/nls/sr/hpcc",
    "src/nls/zh/hpcc",
    /* */

    "hpcc/DelayLoadWidget",
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
    "dijit/ConfirmDialog",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",

    "hpcc/HPCCPlatformMainWidget",
    "hpcc/TableContainer",
    "hpcc/InfoGridWidget"

], function (declare, lang, nlsHPCCMod, arrayUtil, dom, domConstruct, domClass, domForm, domStyle, domGeo, cookie, query, topic, xhr,
    registry, Tooltip,
    UpgradeBar, ColorPicker,
    CodeMirror, srcReact, AboutModule, ComingSoonModule,
    _TabContainerWidget, ESPRequest, ESPActivity, ESPUtil, WsAccount, WsAccess, WsSMC, WsTopology, WsMachine, LockDialogWidget, EnvironmentTheme, Utility,
    template,

    /* DEBUG_ONLY
    bs_hpcc,
    es_hpcc,
    fr_hpcc,
    hr_hpcc,
    hu_hpcc,
    pt_br_hpcc,
    sr_hpcc,
    zh_hpcc,
    /* */

) {

    declare("HPCCColorPicker", [ColorPicker], {
        _underlay: "/esp/files/eclwatch/img/underlay.png",
        _hueUnderlay: "/esp/files/eclwatch/img/hue.png",
        _pickerPointer: "/esp/files/eclwatch/img/pickerPointer.png",
        _huePickerPointer: "/esp/files/eclwatch/img/hueHandle.png",
        _huePickerPointerAlly: "/esp/files/eclwatch/img/hueHandleA11y.png"
    });

    function encodeHTML(str) {
        if (typeof str === "string") {
            return Utility.encodeHTML(str);
        }
        return str;
    }

    var nlsHPCC = nlsHPCCMod.default;
    return declare("HPCCPlatformWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "HPCCPlatformWidget",
        i18n: nlsHPCC,

        bannerContent: "",
        upgradeBar: null,
        storage: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.searchText = registry.byId(this.id + "FindText");
            this.logoutBtn = registry.byId(this.id + "Logout");
            this.lockBtn = registry.byId(this.id + "Lock");
            this.setBannerDialog = registry.byId(this.id + "SetBannerDialog");
            this.setToolbarDialog = registry.byId(this.id + "SetToolbarDialog");
            this.stackContainer = registry.byId(this.id + "TabContainer");
            this.mainPage = registry.byId(this.id + "_Main");
            this.errWarnPage = registry.byId(this.id + "_ErrWarn");
            this.pluginsPage = registry.byId(this.id + "_Plugins");
            this.operationsPage = registry.byId(this.id + "_OPS");
            registry.byId(this.id + "SetBanner").set("disabled", true);
            registry.byId(this.id + "SetToolbar").set("disabled", true);
            this.sessionBackground = registry.byId(this.id + "SessionBackground");
            this.unlockDialog = registry.byId(this.id + "UnlockDialog");
            this.unlockUserName = registry.byId(this.id + "UnlockUserName");
            this.unlockPassword = registry.byId(this.id + "UnlockPassword");
            this.logoutConfirm = registry.byId(this.id + "LogoutConfirm");
            this.unlockForm = registry.byId(this.id + "UnlockForm");
            this.environmentTextCB = registry.byId(this.id + "EnvironmentTextCB");
            this.environmentText = registry.byId(this.id + "EnvironmentText");
            this.toolbarColor = registry.byId(this.id + "ToolbarColor");

            this.upgradeBar = new UpgradeBar({
                notifications: [],
                noRemindButton: ""
            });
        },

        startup: function (args) {
            this.inherited(arguments);
            domStyle.set(dom.byId(this.id + "StackController_stub_Plugins").parentNode.parentNode, {
                display: "none"
            });
            domStyle.set(dom.byId(this.id + "StackController_stub_ErrWarn").parentNode.parentNode, {
                display: "none"
            });
            domStyle.set(dom.byId(this.id + "StackController_stub_Config").parentNode.parentNode, {
                display: "none"
            });
            /* DEBUG_ONLY
            registry.byId(this.id + "Debug").set("hidden", false);
            /* */
            var teaserNode = dom.byId(this.id + "teaser");
            srcReact.render(ComingSoonModule.ComingSoon, { style: { color: "white" } }, teaserNode);
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
                    var marquee = dom.byId(this.id + "Marquee");
                    var height = activity.BannerSize * 2;
                    var msg = "<marquee id='" + this.id + "Marquee' width='100%' direction='left' scrollamount='" + encodeHTML(activity.BannerScroll) + "' style='color:" + encodeHTML(activity.BannerColor) + ";font-size:" + activity.BannerSize + "px;line-height:" + height + "px'>" + encodeHTML(activity.BannerContent) + "</marquee>";
                    this.upgradeBar.notify(msg);
                    domStyle.set(this.upgradeBar.domNode, "height", height + "px");
                    domStyle.set(marquee.parentNode, { top: "auto", "margin-top": "auto", height: height + "px" });
                } else {
                    this.upgradeBar.notify("");
                    domStyle.set(this.upgradeBar.domNode, "height", "0px");
                }
            }
        },

        refreshUserName: function () {
            if (this.userName) {
                dom.byId(this.id + "UserID").textContent = this.userName;
            } else if (cookie("ESPUserName")) {
                domConstruct.place("<span>" + cookie("ESPUserName") + "</span>", this.id + "UserID", "replace");
                dojoConfig.username = cookie("ESPUserName");
            } else {
                dom.byId(this.id + "UserID").textContent = "";
            }
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;

            WsMachine.GetComponentStatus({
                request: {}
            }).then(function (response) {
                if (lang.exists("GetComponentStatusResponse.ComponentStatus", response)) {
                    dojoConfig.monitoringEnabled = true;
                    var status = response.GetComponentStatusResponse.ComponentStatus;
                    context.checkMonitoring(status);
                } else {
                    dojoConfig.monitoringEnabled = false;
                }
            });

            WsAccount.MyAccount({}).then(function (response) {
                if (lang.exists("MyAccountResponse.username", response)) {
                    context.userName = response.MyAccountResponse.username;
                    dojoConfig.username = response.MyAccountResponse.username;
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
                                    if (response.MyAccountResponse.passwordDaysRemaining && response.MyAccountResponse.passwordDaysRemaining <= response.MyAccountResponse.passwordExpirationWarningDays) {
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
                request: {}
            }).then(function (response) {
                if (lang.exists("TpGetServicePluginsResponse.Plugins.Plugin", response) && response.TpGetServicePluginsResponse.Plugins.Plugin.length) {
                    domStyle.set(dom.byId(context.id + "StackController_stub_Plugins").parentNode.parentNode, {
                        display: "inline"
                    });
                }
            });

            WsTopology.TpGetServerVersion().then(function (buildVersion) {
                context.build = WsSMC.parseBuildString(buildVersion);
            });

            this.activity = ESPActivity.Get();
            this.activity.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                context.refreshBanner(context.activity);
            });

            this.createStackControllerTooltip(this.id + "_Main", this.i18n.Activity);
            this.createStackControllerTooltip(this.id + "_ECL", this.i18n.ECL);
            this.createStackControllerTooltip(this.id + "_Files", this.i18n.Files);
            this.createStackControllerTooltip(this.id + "_RoxieQueries", this.i18n.PublishedQueries);
            this.createStackControllerTooltip(this.id + "_OPS", this.i18n.Operations);
            this.createStackControllerTooltip(this.id + "_Plugins", this.i18n.Plugins);
            this.initTab();
            this.checkIfSessionsAreActive();

            topic.subscribe("hpcc/monitoring_component_update", function (topic) {
                context.checkMonitoring(topic.status);
            });
            this.storage = new ESPUtil.LocalStorage();

            this.storage.on("storageUpdate", function (msg) {
                context._onUpdateFromStorage(msg);
            });
            this.storage.setItem("Status", "Unlocked");

            this._onUpdateToolbarDom();
        },

        _onUpdateFromStorage: function (msg) {
            var context = this;
            if (msg.event.newValue === "logged_out") {
                window.location.reload();
            } else if (msg.event.newValue === "Locked") {
                context._onShowLock();
            } else if (msg.event.newValue === "Unlocked" || msg.event.oldValue === "Locked") {
                context._onHideLock();
            }
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
            if (user == null) {
                registry.byId(context.id + "SetBanner").set("disabled", false);
                registry.byId(context.id + "SetToolbar").set("disabled", false);
                dojo.destroy(this.monitorStatus);
            } else {
                WsAccess.UserEdit({
                    suppressExceptionToaster: true,
                    request: {
                        username: user
                    }
                }).then(function (response) {
                    if (lang.exists("UserEditResponse.isLDAPAdmin", response)) {
                        if (response.UserEditResponse.isLDAPAdmin === true) {
                            dojoConfig.isAdmin = true;
                            registry.byId(context.id + "SetBanner").set("disabled", false);
                            registry.byId(context.id + "SetToolbar").set("disabled", false);
                            if (context.widget._OPS.refresh) {
                                context.widget._OPS.refresh();
                            }
                            return false;
                        }
                    } else {
                        if (lang.exists("UserEditResponse.Groups.Group", response)) {
                            arrayUtil.some(response.UserEditResponse.Groups.Group, function (item, idx) {
                                if (item.name === "Administrators" || item.name === "Directory Administrators") {
                                    dojoConfig.isAdmin = true;
                                    registry.byId(context.id + "SetBanner").set("disabled", false);
                                    registry.byId(context.id + "SetToolbar").set("disabled", false);
                                    if (context.widget._OPS.refresh) {
                                        context.widget._OPS.refresh();
                                    }
                                    return false;
                                }
                            });
                        }
                    }
                });
            }
        },

        checkIfSessionsAreActive: function () {
            if (cookie("ESPSessionTimeoutSeconds")) {
                this.logoutBtn.set("disabled", false);
                this.lockBtn.set("disabled", false);
                dom.byId("UserDivider").textContent = " / ";
                dom.byId("Lock").textContent = this.i18n.Lock;
            }
        },

        //  Hitched actions  ---
        _onUserID: function (evt) {
            var userDialog = registry.byId(this.id + "UserDialog");
            var userInfo = registry.byId(this.id + "UserInfo");
            if (!userInfo.init({
                Username: this.userName,
                Widget: userDialog
            })) {
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

        _openNewTab: function (url) {
            var win = window.open(url, "_blank");
            if (win && win.focus) {
                win.focus();
            }
        },

        _onOpenResources: function (evt) {
            this._openNewTab("https://hpccsystems.com/download");
        },

        _onOpenDocuments: function (evt) {
            this._openNewTab("https://hpccsystems.com/training/documentation");
        },

        _onOpenJira: function (evt) {
            this._openNewTab("https://track.hpccsystems.com/issues");
        },

        _onOpenForums: function (evt) {
            this._openNewTab("https://hpccsystems.com/bb/");
        },

        _onOpenRedBook: function (evt) {
            this._openNewTab("https://wiki.hpccsystems.com/x/fYAb");
        },

        _onOpenReleaseNotes: function (evt) {
            this._openNewTab("https://hpccsystems.com/download/release-notes");
        },

        _onOpenTransitionGuide: function (evt) {
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
                }).then(function (response) {
                    context.configText = context.formatXml(response);
                    context.configSourceCM = new CodeMirror.XMLEditor();

                    var t = window.setTimeout(() => {
                        context.configSourceCM.target(dom.byId(context.id + "_Config")).render();
                        dom.byId(context.id + "ConfigTextArea").style.display = "none";
                        window.clearTimeout(t);
                    }, 50);
                    context.configSourceCM.text(context.configText);
                });
            }
            this.stackContainer.selectChild(this.widget._Config);
        },

        _onOpenErrWarn: function (evt) {
            this.stackContainer.selectChild(this.errWarnPage);
        },

        _ondebugLanguageFiles: function () {
            /* DEBUG_ONLY
            var context = this;
            require(["src/nls/hpcc"], function (lang) {
                var languageID = [];
                var languageRequire = [];
                for (var key in lang) {
                    if (key !== "root") {
                        languageID.push(key);
                        languageRequire.push("src/nls/" + key + "/hpcc");
                    }
                }
                var errWarnGrid = registry.byId(context.id + "ErrWarnGrid");
                arrayUtil.forEach([bs_hpcc, es_hpcc, fr_hpcc, hr_hpcc, hu_hpcc, pt_br_hpcc, sr_hpcc, zh_hpcc], function (otherLang, idx) {
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
            this.stackContainer.selectChild(this.errWarnPage);
            /* */
        },

        _onECLWatchV9: function (evt) {
            var teaserNode = dom.byId(this.id + "teaser");
            srcReact.render(ComingSoonModule.ComingSoon, { style: { color: "white" }, value: true }, teaserNode);
        },

        _onAboutLoaded: false,
        _onAbout: function (evt) {
            var placeholderNode = dom.byId(this.id + "DialogPlaceholder");
            srcReact.render(AboutModule.About, {
                eclwatchVersion: "5",
                show: true,
                onClose: function () {
                    srcReact.unrender(placeholderNode);
                }
            }, placeholderNode);
        },

        _onShowLock: function (evt) {
            var LockDialog = new LockDialogWidget({});
            LockDialog.show();
        },

        _onLock: function (evt) {
            var LockDialog = new LockDialogWidget({});
            LockDialog._onLock();
        },

        _onHideLock: function (evt) {
            var LockDialog = new LockDialogWidget({});
            LockDialog.hide();
        },

        _onLogout: function (evt) {
            var context = this;
            this.logoutConfirm.show();
            query(".dijitDialogUnderlay").style("opacity", "0.5");
            this.logoutConfirm.on("execute", function () {
                xhr("/esp/logout", {
                    method: "post"
                }).then(function (data) {
                    if (data) {
                        Utility.deleteCookie("ECLWatchUser");
                        Utility.deleteCookie("ESPSessionID");
                        Utility.deleteCookie("Status");
                        Utility.deleteCookie("User");
                        Utility.deleteCookie("ESPSessionState");
                        window.location.reload();
                        context.storage.setItem("Status", "logged_out");
                    }
                });
            });
        },

        _onMonitoring: function (evt) {
            this.stackContainer.selectChild(this.operationsPage);
            this.operationsPage.ensureWidget().then(function (operationsPage) {
                operationsPage.widget._Topology.ensureWidget().then(function (topologyPage) { //  This is needed otherwise topology will steal focus the first time it is delay loaded
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

        _onSetToolbar: function (evt) {
            this.setToolbarDialog.show();
        },

        _onSetToolbarOk: function (evt) {
            EnvironmentTheme.setEnvironmentTheme({
                active: this.environmentTextCB.get("checked"),
                text: this.environmentText.value,
                color: this.toolbarColor.value
            });
            this._onUpdateToolbarDom();
        },

        _onSetToolbarCancel: function (evt) {
            this.setToolbarDialog.hide();
        },

        _onUpdateToolbarDom: function (evt) {
            EnvironmentTheme.getEnvironmentTheme().then(theme => {
                if (theme === undefined) return;
                this.environmentTextCB.set("checked", theme.active);
                this.environmentText.set("value", theme.text);
                this.toolbarColor.set("value", theme.color);

                domConstruct.destroy(this.id + "BannerInnerText");

                if (theme.active && theme.text !== "") {
                    var searchUserMoreComponents = dom.byId(this.id + "searchUserMoreComponents");
                    var parent = domConstruct.create("div", { id: this.id + "BannerInnerText", class: "environmentText" }, searchUserMoreComponents, "before");
                    domConstruct.create("span", { id: this.id + "BannerContent", style: { color: Utility.textColor(theme.color) }, innerHTML: theme.text }, parent);
                    document.title = theme.text;
                } else {
                    document.title = dojoConfig.pageTitle;
                }

                domStyle.set(this.id + "Titlebar", { backgroundColor: theme.active && theme.color ? theme.color : EnvironmentTheme.defaults.color });
            });
        },

        _onSetToolbarReset: function (evt) {
            if (confirm(this.i18n.AreYouSureYouWantToResetTheme)) {
                EnvironmentTheme._onResetDefaultTheme();
                this._onUpdateToolbarDom();
                this._onSetToolbarCancel();
            }
        },

        createStackControllerTooltip: function (widgetID, text) {
            return new Tooltip({
                connectId: [this.id + "StackController_" + widgetID],
                label: text,
                position: ["above"]
            });
        }
    });
});