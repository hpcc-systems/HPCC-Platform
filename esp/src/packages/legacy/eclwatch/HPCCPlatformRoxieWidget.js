define([
    "dojo/_base/declare",
    "src/nlsHPCC",

    "hpcc/_TabContainerWidget",

    "dojo/text!../templates/HPCCPlatformRoxieWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",

    "hpcc/DelayLoadWidget"

], function (declare, nlsHPCCMod,
    _TabContainerWidget,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("HPCCPlatformRoxieWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "HPCCPlatformRoxieWidget",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_HPCCPlatformRoxie;
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
                if (currSel.init) {
                    currSel.init({});
                }
            }
        }
    });
});
