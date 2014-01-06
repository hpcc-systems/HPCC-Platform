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
    "hpcc/ESPQuery",
    "hpcc/ResultWidget",
    "hpcc/FullResultWidget",
    "hpcc/LFDetailsWidget",
    "hpcc/VizWidget",

    "dojo/text!../templates/ECLPlaygroundResultsWidget.html",

    "dijit/layout/TabContainer"
], function (declare, lang, dom, 
                registry,
                _TabContainerWidget, ESPWorkunit, ESPQuery, ResultWidget, FullResultWidget, LFDetailsWidget, VizWidget,
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
                } else if (lang.exists("QuerySetId", params) && lang.exists("Id", params)) {
                    retVal = new FullResultWidget({
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

            var context = this;
            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);

                var monitorCount = 4;
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
            } else if (params.QuerySetId && params.Id) {
                this.query = ESPQuery.Get(params.QuerySetId, params.Id);
                this.query.SubmitXML(params.RequestXml).then(function (response) {
                    var firstTab = true;
                    for (var key in response) {
                        var tab = context.ensurePane(context.id + "_result" + key, key, {
                            QuerySetId: params.QuerySetId,
                            Id: params.Id,
                            FullResult: response[key]
                        });
                        if (firstTab) {
                            context.initTab();
                        } else {
                            firstTab = false;
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

        refresh: function (params) {
            if (params.Wuid) {
                if (!this.wu || (this.wu.Wuid != params.Wuid)) {
                    this.clear();
                    this.init(params);
                }
            } else if (params.QuerySetId && params.Id) {
                this.clear();
                this.init(params);
            }
        }
    });
});
