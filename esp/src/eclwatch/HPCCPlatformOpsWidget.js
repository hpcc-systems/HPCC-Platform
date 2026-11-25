define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",

    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "src/ESPRequest",

    "dojo/text!../templates/HPCCPlatformOpsWidget.html",

    "hpcc/UserQueryWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",

    "hpcc/DelayLoadWidget"

], function (declare, lang, nlsHPCCMod,
    registry,
    _TabContainerWidget, ESPRequest,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("HPCCPlatformOpsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "HPCCPlatformOpsWidget",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
            registry.byId(this.id + "_Permissions").set("disabled", true);
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_HPCCPlatformOps;
        },

        //  Hitched actions  ---

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.refresh();
            this.initTab();
        },

        refresh: function (params) {
            if (dojoConfig.isAdmin) {
                registry.byId(this.id + "_Permissions").set("disabled", false);
            }
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.id + "_SystemServers") {
                    currSel.set("content", dojo.create("iframe", {
                        src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src=" + encodeURIComponent(ESPRequest.getBaseURL("WsTopology") + "/TpServiceQuery?Type=ALLSERVICES"),
                        style: "border: 0; width: 100%; height: 100%"
                    }));
                } else if (currSel.init) {
                    currSel.init({});
                }
                currSel.initalized = true;
            }
        }
    });
});
