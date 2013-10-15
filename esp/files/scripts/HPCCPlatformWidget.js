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
    "dojo/dom-style",

    "dijit/registry",
    "dijit/Tooltip",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPRequest",
    "hpcc/WsAccount",
    "hpcc/WsSMC",
    "hpcc/GraphWidget",

    "dojo/text!../templates/HPCCPlatformWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/StackContainer",
    "dijit/layout/StackController",
    "dijit/layout/ContentPane",
    "dijit/form/DropDownButton",
    "dijit/form/ComboButton",
    "dijit/form/TextBox",
    "dijit/Menu",
    "dijit/MenuSeparator",
    "dijit/Toolbar",
    "dijit/TooltipDialog",

    "hpcc/HPCCPlatformMainWidget",
    "hpcc/HPCCPlatformECLWidget",
    "hpcc/HPCCPlatformFilesWidget",
    "hpcc/HPCCPlatformRoxieWidget",
    "hpcc/HPCCPlatformOpsWidget"

], function (declare, lang, dom, domStyle,
                registry, Tooltip,
                _TabContainerWidget, ESPRequest, WsAccount, WsSMC, GraphWidget,
                template) {
    return declare("HPCCPlatformWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "HPCCPlatformWidget",
        BuildVer: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.searchText = registry.byId(this.id + "FindText");
            this.aboutDialog = registry.byId(this.id + "AboutDialog");
            this.searchPage = registry.byId(this.id + "_Main" + "_Search");
            this.stackContainer = registry.byId(this.id + "TabContainer");
            this.mainPage = registry.byId(this.id + "_Main");
            this.errWarnPage = registry.byId(this.id + "_ErrWarn");
            this.mainStackContainer = registry.byId(this.mainPage.id + "TabContainer");
            this.searchPage = registry.byId(this.id + "_Main" + "_Search");
        },

        startup: function (args) {
            this.inherited(arguments);
            domStyle.set(dom.byId(this.id + "StackController_stub_ErrWarn").parentNode.parentNode, {
                visibility: "hidden"
            });
        },

        //  Implementation  ---
        init: function (params) {
             if (this.inherited(arguments))
                return;

            var context = this;
            WsAccount.MyAccount({
            }).then(function (response) {
                if (lang.exists("MyAccountResponse.username", response)) {
                    dom.byId(context.id + "UserID").innerHTML = response.MyAccountResponse.username;
                }
            });

            WsSMC.Activity({
            }).then(function (response) {
                if (lang.exists("ActivityResponse.Build", response)) {
                    context.BuildVer = response.ActivityResponse.Build;
                    context.BuildVer = context.BuildVer.substring(0, context.BuildVer.indexOf("["));
                    context.BuildVer = context.BuildVer.replace("community_","");
                }
            });

            this.createStackControllerTooltip(this.id + "_ECL", "ECL");
            this.createStackControllerTooltip(this.id + "_Files", "Files");
            this.createStackControllerTooltip(this.id + "_Queries", "Published Queries");
            this.createStackControllerTooltip(this.id + "_OPS", "Operations");
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

        //  Hitched actions  ---
        _onFind: function (evt) {
            this.stackContainer.selectChild(this.mainPage);
            this.mainStackContainer.selectChild(this.searchPage);
            this.searchPage.doSearch(this.searchText.get("value"));
        },

        _onOpenLegacy: function (evt) {
            var win = window.open("\\", "_blank");
            win.focus();
        },

        _onOpenReleaseNotes: function (evt) {
            //this.BuildVer = BuildVer
            var win = window.open("http://hpccsystems.com/download/free-community-edition-known-limitations#" + this.BuildVer, "_blank");
            win.focus();
        },

        _onOpenErrWarn: function (evt) {
            this.stackContainer.selectChild(this.errWarnPage);
        },

        _onAboutLoaded: false,
        _onAbout: function (evt) {
            if (!this._onAboutLoaded) {
                this._onAboutLoaded = true;
                var context = this;
                WsSMC.Activity({
                }).then(function (response) {
                    if (lang.exists("ActivityResponse.Build", response)) {
                        dom.byId(context.id + "ServerVersion").value = response.ActivityResponse.Build;
                    }
                    var gc = registry.byId(context.id + "GraphControl");
                    dom.byId(context.id + "GraphControlVersion").value = gc.getVersion();
                });
            }
            this.aboutDialog.show();
        },

        _onAboutClose: function (evt) {
            this.aboutDialog.hide();
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
