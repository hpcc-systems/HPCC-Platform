define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",

    "hpcc/_TabContainerWidget",
    "hpcc/DelayLoadWidget",
    "src/WsTopology",

    "dojo/text!../templates/HPCCPlatformServicesPluginWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane"

], function (declare, lang, i18n, nlsHPCC, arrayUtil,
    _TabContainerWidget, DelayLoadWidget, WsTopology,
    template) {
    return declare("HPCCPlatformServicesPluginWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "HPCCPlatformServicesPluginWidget",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_HPCCPlatformServicesPlugin;
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;
            WsTopology.TpGetServicePlugins({
                request: {
                }
            }).then(function (response) {
                if (lang.exists("TpGetServicePluginsResponse.Plugins.Plugin", response) && response.TpGetServicePluginsResponse.Plugins.Plugin.length) {
                    arrayUtil.forEach(response.TpGetServicePluginsResponse.Plugins.Plugin, function (item) {
                        var pluginWidget = new DelayLoadWidget({
                            id: context.createChildTabID(item.ShortName),
                            title: item.ShortName,
                            delayFolder: item.FolderName,
                            delayWidget: item.WidgetName,
                            hpcc: {
                                params: {
                                }
                            }
                        });
                        context.addChild(pluginWidget);
                        context.resize();
                    });
                }
            });
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
