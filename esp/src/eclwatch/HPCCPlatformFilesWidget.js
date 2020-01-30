define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",

    "hpcc/_TabContainerWidget",
    "src/ESPRequest",

    "dojo/text!../templates/HPCCPlatformFilesWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",

    "hpcc/DelayLoadWidget"

], function (declare, i18n, nlsHPCC,
    _TabContainerWidget, ESPRequest,
    template) {
    return declare("HPCCPlatformFilesWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "HPCCPlatformFilesWidget",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_HPCCPlatformFiles;
        },

        //  Hitched actions  ---

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.initTab();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.id + "_XRef") {
                    currSel.set("content", dojo.create("iframe", {
                        src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src=" + encodeURIComponent(ESPRequest.getBaseURL("WsDFUXRef") + "/DFUXRefList"),
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
