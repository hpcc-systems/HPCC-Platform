/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
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
    "dijit/ToolbarSeparator",
    "dijit/form/TextBox",
    "dijit/form/DropDownButton",
    "dijit/TooltipDialog",

    "dojox/layout/TableContainer"
], function (declare, lang, i18n, nlsHPCC, dom, domConstruct,
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
                    switch (key) {
                        case "SecurityString":
                        case "UserName":
                        case "Password":
                        case "Addresses":
                        case "EnableSNMP":
                        case "SortBy":
                        case "OldIP":
                        case "Path":
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