define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom-construct",

    "dijit/registry",

    "hpcc/_TabContainerWidget",

    "dojo/text!../templates/RequestInformationWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/ToolbarSeparator",
    "dijit/form/TextBox",
    "dijit/form/DropDownButton",
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer"
], function (declare, i18n, nlsHPCC, domConstruct,
    registry,
    _TabContainerWidget,
    template) {
    return declare("RequestInformationWidget", [_TabContainerWidget], {
        i18n: nlsHPCC,
        templateString: template,
        baseClass: "RequestInformationWidget",

        requestInfoTab: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.requestInfoTab = registry.byId(this.id + "_RequestInfo");
            this.requestDetails = registry.byId(this.id + "_RequestDetails");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        init: function (params) {
            this.generateRequestInfo(params.RequestInfo);
        },

        _onRefresh: function (params) {

        },

        generateRequestInfo: function (params) {
            var table = domConstruct.create("table", {});
            for (var key in params) {
                if (params[key] === true) {
                    params[key] = "enabled";
                }
                if (params[key] === false) {
                    params[key] = "disabled";
                }

                switch (key) {
                    case "SecurityString":
                    case "UserName":
                    case "Password":
                    case "Addresses":
                    case "EnableSNMP":
                    case "SortBy":
                    case "OldIP":
                    case "Path":
                    case "ClusterType":
                    case "Cluster":
                        break;
                    default:
                        var tr = domConstruct.create("tr", {}, table);
                        domConstruct.create("td", {
                            innerHTML: "<b>" + key + ":&nbsp;&nbsp;</b>"
                        }, tr);
                        domConstruct.create("td", {
                            innerHTML: params[key]
                        }, tr);
                }
            }
            this.requestDetails.setContent(table);
        }
    });
});
