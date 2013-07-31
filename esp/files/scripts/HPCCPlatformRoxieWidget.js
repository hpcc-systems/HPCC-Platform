/*##############################################################################
#	HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPRequest",

    "dojo/text!../templates/HPCCPlatformRoxieWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane"

], function (declare,
                _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                _TabContainerWidget, ESPRequest,
                template) {
    return declare("HPCCPlatformRoxieWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "HPCCPlatformRoxieWidget",

        postCreate: function (args) {
            this.inherited(arguments);
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return "HPCC Platform - Roxie";
        },

        //  Hitched actions  ---

        //  Implementation  ---
        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;
            this.initTab();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.id + "_Queries") {
                    currSel.set("content", dojo.create("iframe", {
                        src: ESPRequest.getBaseURL() + "/WUQuerySets",
                        style: "border: 0; width: 100%; height: 100%"
                    }));
                    currSel.initalized = true;
                } else if (currSel.init) {
                    currSel.init(currSel.params);
                }
            }
        }
    });
});
