define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",

    "hpcc/_TabContainerWidget",

    "dojo/text!../templates/HPCCPlatformMainWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",

    "hpcc/DelayLoadWidget"

], function (declare, lang, i18n, nlsHPCC,
    _TabContainerWidget,
    template) {
        return declare("HPCCPlatformMainWidget", [_TabContainerWidget], {
            templateString: template,
            baseClass: "HPCCPlatformMainWidget",
            i18n: nlsHPCC,

            postCreate: function (args) {
                this.inherited(arguments);
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            getTitle: function () {
                return this.i18n.title_HPCCPlatformMain;
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
