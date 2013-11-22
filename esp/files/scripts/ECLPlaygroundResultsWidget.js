/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
    "dojo/dom",

    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPWorkunit",
    "hpcc/ResultWidget",
    "hpcc/LFDetailsWidget",
    "hpcc/VizWidget",

    "dojo/text!../templates/ECLPlaygroundResultsWidget.html",

    "dijit/layout/TabContainer"
], function (declare, lang, dom, 
                registry,
                _TabContainerWidget, ESPWorkunit, ResultWidget, LFDetailsWidget, VizWidget,
                template) {
    return declare("ECLPlaygroundResultsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "ECLPlaygroundResultsWidget",

        selectedTab: null,
        TabPosition: "bottom",

        onErrorClick: function (line, col) {
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                currSel.init(currSel.params);
            }
        },

        ensurePane: function (id, title, params) {
            var retVal = registry.byId(id);
            if (!retVal) {
                if (lang.exists("Wuid", params) && lang.exists("Sequence", params)) {
                    retVal = new ResultWidget({
                        id: id,
                        title: title,
                        params: params
                    });
                }
                this.addChild(retVal);
            }
            return retVal;
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);

                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 == 0) {
                        context.wu.getInfo({
                            onGetResults: function (results) {
                                if (!params.SourceFiles) {
                                    for (var i = 0; i < results.length; ++i) {
                                        var tab = context.ensurePane(context.id + "_result" + i, results[i].Name, {
                                            Wuid: results[i].Wuid,
                                            Sequence: results[i].Sequence
                                        });
                                        if (i == 0) {
                                            context.initTab();
                                        }
                                    }
                                }
                            }
                        });
                        var currSel = context.getSelectedChild();
                        if (currSel && currSel.refresh) {
                            currSel.refresh();
                        }
                    }
                });
            }
        },

        clear: function () {
            this.removeAllChildren();
            this.selectedTab = null;
            this.initalized = false;
        },

        refresh: function (wu) {
            if (this.workunit != wu) {
                this.clear();
                this.workunit = wu;
                this.init({
                    Wuid: wu.Wuid
                });
            }
        }
    });
});
