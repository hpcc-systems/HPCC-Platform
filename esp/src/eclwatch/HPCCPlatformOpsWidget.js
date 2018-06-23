/*##############################################################################
#	HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#	Licensed under the Apache License, Version 2.0 (the "License");
#	you may not use this file except in compliance with the License.
#	You may obtain a copy of the License at
#
#	   http://www.apache.org/licenses/LICENSE-2.0
#
#	Unless required by applicable law or agreed to in writing, software
#	distributed under the License is distributed on an "AS IS" BASIS,
#	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#	See the License for the specific language governing permissions and
#	limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",

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

], function (declare, lang, i18n, nlsHPCC,
                registry,
                _TabContainerWidget, ESPRequest, WsELK,
                template) {
    return declare("HPCCPlatformOpsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "HPCCPlatformOpsWidget",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
            registry.byId(this.id + "_Permissions").set("disabled", true);
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
                if (currSel.id === this.id + "_TargetClusters") {
                    currSel.set("content", dojo.create("iframe", {
                        src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src=" + encodeURIComponent(ESPRequest.getBaseURL("WsTopology") + "/TpTargetClusterQuery?Type=ROOT"),
                        style: "border: 0; width: 100%; height: 100%"
                    }));
                } else if (currSel.id === this.id + "_ClusterProcesses") {
                    currSel.set("content", dojo.create("iframe", {
                        src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src=" + encodeURIComponent(ESPRequest.getBaseURL("WsTopology") + "/TpClusterQuery?Type=ROOT"),
                        style: "border: 0; width: 100%; height: 100%"
                    }));
                } else if (currSel.id === this.id + "_SystemServers") {
                    currSel.set("content", dojo.create("iframe", {
                        src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src=" + encodeURIComponent(ESPRequest.getBaseURL("WsTopology") + "/TpServiceQuery?Type=ALLSERVICES"),
                        style: "border: 0; width: 100%; height: 100%"
                    }));
                } else if (currSel.id === this.id + "_LogVisualization") {
                    WsELK.GetConfigDetails({
                        request: {}
                    }).then(function (response) {
                        if (lang.exists("GetConfigDetailsResponse", response)) {
                            var elk = response.GetConfigDetailsResponse;
                            currSel.set("content", dojo.create("iframe", {
                                src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src="+ encodeURIComponent(elk.KibanaAddress + ":" + elk.KibanaPort + elk.KibanaEntryPointURI),
                                style: "border: 0; width: 100%; height: 100%"
                            }));
                        } else {
                            //load LogVisualizationWidget and show status there
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
