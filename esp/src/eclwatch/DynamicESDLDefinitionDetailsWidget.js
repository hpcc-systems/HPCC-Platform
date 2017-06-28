/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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

    "hpcc/_TabContainerWidget",
    "hpcc/ECLSourceWidget",
    "hpcc/WsESDLConfig",

    "dojo/text!../templates/DynamicESDLDefinitionDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane"
], function (declare, lang, i18n, nlsHPCC,
                _TabContainerWidget, ECLSourceWidget, WsESDLConfig,
                template) {
    return declare("DynamicESDLDefinitionDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "DynamicESDLDefinitionDetailsWidget",
        i18n: nlsHPCC,

        //  Implementation  ---
        init: function (params) {
            var context = this;
            this.inherited(arguments);

            if (params.Id) {
                this.widget._Summary.set("title", params.Id);
            }
            WsESDLConfig.GetESDLDefinition({
                request: {
                    Id: params.Id
                }
            }).then(function (response) {
                var xml = context.formatXml(response.GetESDLDefinitionResponse.XMLDefinition);
                context.widget._XML.init({
                    sourceMode: "xml",
                    readOnly: true
                });
                context.widget._XML.setText(xml);
            });
        }
    });
});