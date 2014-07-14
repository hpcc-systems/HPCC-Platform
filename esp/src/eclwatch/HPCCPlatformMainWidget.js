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
