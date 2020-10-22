define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",

    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "src/ESPRequest",
    "src/ws_elk",

    "dojo/text!../templates/HPCCPlatformOpsWidget.html",

    "hpcc/UserQueryWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",

    "hpcc/DelayLoadWidget"

], function (declare, lang, nlsHPCCMod,
    registry,
    _TabContainerWidget, ESPRequest, WsELK,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("HPCCPlatformOpsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "HPCCPlatformOpsWidget",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
            registry.byId(this.id + "_Permissions").set("disabled", true);
            registry.byId(this.id + "_Monitoring").set("disabled", !dojoConfig.monitoringEnabled);
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
                } else if (currSel.id === this.id + "_LogVisualization") {
                    var context = this;
                    WsELK.GetConfigDetails({
                        request: {}
                    }).then(function (response) {
                        if (lang.exists("GetConfigDetailsResponse.IntegrateKibana", response) && response.GetConfigDetailsResponse.IntegrateKibana === true) {
                            var elk = response.GetConfigDetailsResponse;
                            currSel.set("content", dojo.create("iframe", {
                                src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src=" + encodeURIComponent(elk.KibanaAddress + ":" + elk.KibanaPort + elk.KibanaEntryPointURI),
                                style: "border: 0; width: 100%; height: 100%"
                            }));
                        } else {
                            currSel.set("content", dojo.create("div", {
                                innerHTML: "<p>" + context.i18n.LogVisualizationUnconfigured + "</p> <br> <a href = 'https://hpccsystems.com/blog/ELK_visualizations'>" + context.i18n.LearnMore + "</a>",
                                style: "margin: 0; position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); font-size: medium; color: #c91312"
                            }));
                        }
                    });
                } else if (currSel.init) {
                    currSel.init({});
                }
                currSel.initalized = true;
            }
        }
    });
});
