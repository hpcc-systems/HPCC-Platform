define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",

    "hpcc/_TabContainerWidget",
    "src/WsESDLConfig",

    "dojo/text!../templates/DynamicESDLDefinitionDetailsWidget.html",

    "hpcc/ECLSourceWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane"
], function (declare, i18n, nlsHPCC,
    _TabContainerWidget, WsESDLConfig,
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
                var xml = context.formatXml(response.GetESDLDefinitionResponse.Definition.Interface);
                context.widget._XML.init({
                    sourceMode: "xml",
                    readOnly: true
                });
                context.widget._XML.setText(xml);
            });
        }
    });
});
