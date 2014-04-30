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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",

    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "hpcc/WsTopology",
    "hpcc/ESPQuery",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",

    "dojo/text!../templates/QueryTestWidget.html"

], function (declare, lang, i18n, nlsHPCC, arrayUtil,
            registry,
            _TabContainerWidget, WsTopology, ESPQuery,
            BorderContainer, TabContainer, ContentPane,
            template) {
    return declare("QueryTestWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "QueryTestWidget",
        i18n: nlsHPCC,

        initalized: false,
        soapTab: null,
        jsonTab: null,
        requestTab: null,
        responseTab: null,
        requestSchemaTab: null,
        responseSchemaTab: null,
        wsdlTab: null,
        paramXmlTab: null,
        formTab: null,
        linksTab: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.soapTab = registry.byId(this.id + "_SOAP");
            this.jsonTab = registry.byId(this.id + "_JSON");
            this.wsdlTab = registry.byId(this.id + "_WSDL");
            this.requestSchemaTab = registry.byId(this.id + "_RequestSchema");
            this.responseSchemaTab = registry.byId(this.id + "ResponseSchema");
            this.requestTab = registry.byId(this.id + "_Request");
            this.responseTab = registry.byId(this.id + "_Response");
            this.paramXmlTab = registry.byId(this.id + "_ParamXML");
            this.formTab = registry.byId(this.id + "_Form");
            this.linksTab = registry.byId(this.id + "_Links");
        },

        //  Hitched actions  ---
        _onRefresh: function () {
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.query = ESPQuery.Get(params.QuerySet, params.QueryId);
        },

        setContent: function (target, type, postfix) {
            var context = this;
            WsTopology.GetWsEclIFrameURL(type).then(function (response) {
                var src = response + encodeURIComponent(context.params.QuerySet + "/" + context.params.QueryId + (postfix ? postfix : ""));
                target.set("content", dojo.create("iframe", {
                    src: src,
                    style: "border: 0; width: 100%; height: 100%"
                }));
            });
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (!currSel.initalized) {
                if (currSel.id === this.id + "_SOAP") {
                    //  .../WsEcl/forms/soap/query/roxie/countydeeds.1
                    this.setContent(currSel, "forms/soap");
                } else if (currSel.id === this.id + "_JSON") {
                    //  .../WsEcl/forms/json/query/roxie/countydeeds.1
                    this.setContent(currSel, "forms/json");
                } else if (currSel.id === this.id + "_WSDL") {
                    //  .../WsEcl/definitions/query/roxie/countydeeds.1/main/countydeeds.1.wsdl?display
                    this.setContent(currSel, "definitions", "/main/" + this.params.QueryId + ".wsdl?display");
                } else if (currSel.id === this.id + "_RequestSchema") {
                    //  .../WsEcl/definitions/query/roxie/countydeeds.1/main/countydeeds.1.xsd?display
                    this.setContent(currSel, "definitions", "/main/" + this.params.QueryId + ".xsd?display");
                } else if (currSel.id === this.id + "_ResponseSchemaBorder") {
                    var wu = this.query.getWorkunit();
                    var context = this;
                    wu.fetchResults(function (response) {
                        arrayUtil.forEach(response, function (item, idx) {
                            var responseSchema = new ContentPane({
                                id: context.id + "ResponseSchema" + item.Name,
                                title: item.Name,
                                closable: false
                            });
                            context.responseSchemaTab.addChild(responseSchema);
                            //  .../WsEcl/definitions/query/roxie/countydeeds.1/result/jo_orig.xsd?display
                            context.setContent(responseSchema, "definitions", "/result/" + item.Name + ".xsd?display");
                        });
                    });
                } else if (currSel.id === this.id + "_Request") {
                    // .../WsEcl/example/request/query/roxie/countydeeds.1?display
                    this.setContent(currSel, "example/request", "?display");
                } else if (currSel.id === this.id + "_Response") {
                    //  .../WsEcl/example/response/query/roxie/countydeeds.1?display
                    this.setContent(currSel, "example/response", "?display");
                } else if (currSel.id === this.id + "_ParamXML") {
                    //  .../WsEcl/definitions/query/roxie/countydeeds.1/resource/soap/countydeeds.1.xml?display
                    this.setContent(currSel, "definitions", "/resource/soap/" + this.params.QueryId + ".xml?display");
                } else if (currSel.id === this.id + "_Form") {
                    //  .../WsEcl/forms/ecl/query/roxie/countydeeds.1
                    this.setContent(currSel, "forms/ecl");
                } else if (currSel.id === this.id + "_Links") {
                    //  .../WsEcl/links/query/roxie/countydeeds.1
                    this.setContent(currSel, "links");
                } else if (currSel.init) {
                    currSel.init(this.params);
                }
                currSel.initalized = true;
            }
        }
    });
});
